package manager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
)

type EventAction int

const (
	RELEASE EventAction = iota
	CONFIRM
)

func (a EventAction) String() string {
	switch a {
	case RELEASE:
		return "release"
	case CONFIRM:
		return "confirm"
	default:
		return "unknown"
	}
}

type failedConfirmationEvent struct {
	CacheCleared    bool   `json:"cache_cleared"`
	PaymentCaptured bool   `json:"payment_captured"`
	PaymentID       string `json:"payment_id"`
	ConsignmentID   string `json:"consignment_id"`
	ReservationID   string `json:"reservation_id"`
	VesselID        string `json:"vessel_id"`
	Weight          int    `json:"weight"`
	Containers      int    `json:"containers"`
}

type CapacityEvent struct {
	Action          EventAction             `json:"action"`
	ReservationInfo storage.ReservationInfo `json:"reservation_info"`
	CacheCleared    bool                    `json:"cache_cleared"`
	RetryCount      int                     `json:"retry_count"`

	// Only used for confirm events that end up going to DLQ
	ConsignmentID string `json:"consignment_id"` // Marked as cancelled
	PaymentID     string `json:"payment_id"`     // Refund payment
}

func (m *Manager) processEvents(ctx context.Context) error {
	if err := m.consumer.StartConsuming(ctx, m.eventHandlers); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

func (m *Manager) handleCapacityEvent(ctx context.Context, key, value []byte) error {
	var event CapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	reservationID := event.ReservationInfo.Id.String()
	vesselID := event.ReservationInfo.VesselID.String()

	log.Debug().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Int("retry_count", event.RetryCount).
		Str("action", event.Action.String()).
		Msg("handling capacity event")

	if !event.CacheCleared {
		deleted, deleteErr := m.cache.DeleteData(ctx, reservationID)
		if deleteErr != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("action", event.Action.String()).
				Err(deleteErr).
				Int("retry_count", event.RetryCount).
				Msg("failed to delete reservation data — scheduling retry")

			if err := m.scheduleEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule event retry: %w", err)
			}
			return fmt.Errorf("failed to delete reservation %s: %w", reservationID, deleteErr)
		}

		if !deleted {
			log.Info().
				Str("reservation_id", reservationID).
				Msg("reservation data already deleted — skipping (duplicate event)")
			return nil
		}

		event.CacheCleared = true
	}

	req := &vesselpb.CapacityRequest{
		VesselId:           vesselID,
		Weight:             int32(event.ReservationInfo.Weight),
		NumberOfContainers: int32(event.ReservationInfo.NumberOfContainers),
		ReservationId:      reservationID,
	}

	var vesselErr error
	switch event.Action {
	case RELEASE:
		// TODO - add backoff - does grpc support natively?
		_, vesselErr = m.vesselCli.ReleaseCapacity(ctx, req)
	case CONFIRM:
		// TODO - add backoff - does grpc support natively?

		_, vesselErr = m.vesselCli.ConfirmCapacity(ctx, req)
	default:
		return fmt.Errorf("unknown event action: %d", event.Action)
	}

	if vesselErr != nil {
		log.Error().
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Str("action", event.Action.String()).
			Err(vesselErr).
			Int("retry_count", event.RetryCount).
			Msg("vessel call failed — scheduling retry")

		if err := m.scheduleEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule event retry: %w", err)
		}
		return fmt.Errorf("vessel %s failed: %w", event.Action.String(), vesselErr)
	}

	log.Info().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Str("action", event.Action.String()).
		Msg("vessel call succeeded")

	if _, err := m.cache.DeleteID(ctx, reservationID); err != nil {
		log.Warn().
			Str("reservation_id", reservationID).
			Err(err).
			Msg("failed to delete reservation id key — will expire naturally")
	}

	return nil
}

func (m *Manager) handleCapacityDLQEvent(ctx context.Context, key, value []byte) error {
	var event CapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal DLQ capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	reservationID := event.ReservationInfo.Id.String()
	vesselID := event.ReservationInfo.VesselID.String()

	switch event.Action {
	case RELEASE:
		log.Error().
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Int("retry_count", event.RetryCount).
			Msg("ALERT: release capacity exhausted retries — vessel capacity may be understated, manual reconciliation required")

	case CONFIRM:
		// Vessel capacity was not confirmed after repeated failures. Payment has already
		// been captured — notify the consignment service so it can refund and cancel.
		if err := notifyConfirmConsignmentDLQ(ctx, m.producer, &event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentID).
				Str("payment_id", event.PaymentID).
				Err(err).
				Msg("ALERT: failed to notify consignment DLQ — manual refund and cancellation required")
			return err
		}
		log.Info().
			Str("reservation_id", reservationID).
			Str("consignment_id", event.ConsignmentID).
			Str("payment_id", event.PaymentID).
			Msg("published failed confirmation event to consignment DLQ")

	default:
		log.Error().
			Str("key", string(key)).
			Int("action", int(event.Action)).
			Msg("ALERT: unknown action on DLQ event — manual intervention required")
	}

	return nil
}

func notifyConfirmConsignmentDLQ(ctx context.Context, producer kafka.IProducer, event *CapacityEvent) error {
	payload := failedConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       event.PaymentID,
		ConsignmentID:   event.ConsignmentID,
		ReservationID:   event.ReservationInfo.Id.String(),
		VesselID:        event.ReservationInfo.VesselID.String(),
		Weight:          event.ReservationInfo.Weight,
		Containers:      event.ReservationInfo.NumberOfContainers,
		CacheCleared:    true,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal failed confirmation event: %w", err)
	}
	if err := producer.Produce(ctx, ConfirmConsignmentDLQTopic, []byte(event.ConsignmentID), payloadJSON); err != nil {
		// TODO - alert
		return fmt.Errorf("failed to publish to consignment DLQ: %w", err)
	}
	return nil
}

func (m *Manager) scheduleEvent(ctx context.Context, event *CapacityEvent) error {
	event.RetryCount++

	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("ALERT: failed to marshal capacity event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	var topic string
	switch event.Action {
	case RELEASE:
		topic = ReleaseCapacityTopic
	case CONFIRM:
		topic = ConfirmCapacityTopic
	}

	// Send to DLQ if retries exhausted
	if event.RetryCount > maxRetries {
		topic = CapacityDLQTopic
	}

	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     event.ReservationInfo.Id.String(),
		Payload: eventJSON,
	}); err != nil {
		log.Warn().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("failed to create outbox event")
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}
