package manager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
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

type FailedConfirmationEvent struct {
	CacheCleared    bool   `json:"cache_cleared"`
	PaymentCaptured bool   `json:"payment_captured"`
	PaymentID       string `json:"payment_id"`
	ConsignmentID   string `json:"consignment_id"`
	ReservationID   string `json:"reservation_id"`
	VesselID        string `json:"vessel_id"`
	Weight          int    `json:"weight"`
	Containers      int    `json:"containers"`
}

// CapacityEvent is RS-internal — used only for retry scheduling via the outbox.
type CapacityEvent struct {
	Action          EventAction             `json:"action"`
	ReservationInfo storage.ReservationInfo `json:"reservation_info"`
	CacheCleared    bool                    `json:"cache_cleared"`
	RetryCount      int                     `json:"retry_count"`

	// Only populated for confirm events that reach the DLQ.
	ConsignmentID string `json:"consignment_id"`
	PaymentID     string `json:"payment_id"`
}

type reservationConfirmedPayload struct {
	ReservationID string `json:"reservation_id"`
	ConsignmentID string `json:"consignment_id"`
}

// reservationExpiredPayload is the wire format for reservation.expired.
// Both RS and CS consume this topic via separate consumer groups.
type reservationExpiredPayload struct {
	ReservationID string `json:"reservation_id"`
	VesselID      string `json:"vessel_id"`
	ConsignmentID string `json:"consignment_id"`
	Weight        int    `json:"weight"`
	Containers    int    `json:"containers"`
}

func (m *Manager) scheduleReservationExpired(ctx context.Context, r *storage.ReservationInfo) error {
	payload, err := json.Marshal(reservationExpiredPayload{
		ReservationID: r.Id.String(),
		VesselID:      r.VesselID.String(),
		ConsignmentID: r.ConsignmentID.String(),
		Weight:        r.Weight,
		Containers:    r.NumberOfContainers,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal reservation expired event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ReservationExpiredTopic,
		Key:     r.Id.String(),
		Payload: payload,
	})
}

func (m *Manager) scheduleReservationConfirmed(ctx context.Context, event *CapacityEvent) error {
	payload, err := json.Marshal(reservationConfirmedPayload{
		ReservationID: event.ReservationInfo.Id.String(),
		ConsignmentID: event.ConsignmentID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal reservation confirmed event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ReservationConfirmedTopic,
		Key:     event.ConsignmentID,
		Payload: payload,
	})
}

func (m *Manager) handleReservationExpiredEvent(ctx context.Context, key, value []byte) error {
	var e reservationExpiredPayload
	if err := json.Unmarshal(value, &e); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal reservation expired event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	rID, err := uuid.Parse(e.ReservationID)
	if err != nil {
		return fmt.Errorf("invalid reservation_id %q: %w", e.ReservationID, err)
	}
	vID, err := uuid.Parse(e.VesselID)
	if err != nil {
		return fmt.Errorf("invalid vessel_id %q: %w", e.VesselID, err)
	}
	cID, err := uuid.Parse(e.ConsignmentID)
	if err != nil {
		return fmt.Errorf("invalid consignment_id %q: %w", e.ConsignmentID, err)
	}

	event := CapacityEvent{
		Action: RELEASE,
		ReservationInfo: storage.ReservationInfo{
			Id:                 rID,
			VesselID:           vID,
			ConsignmentID:      cID,
			Weight:             e.Weight,
			NumberOfContainers: e.Containers,
		},
	}
	return m.processCapacityEvent(ctx, event)
}

func (m *Manager) processEvents(ctx context.Context) error {
	if err := m.consumer.StartConsuming(ctx, m.eventHandlers); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

// Inbound handlers — unmarshal and delegate to process functions.

func (m *Manager) handlePaymentCapturedEvent(ctx context.Context, key, value []byte) error {
	var event CapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal payment captured event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}
	event.Action = CONFIRM
	return m.processCapacityEvent(ctx, event)
}

// TODO: review why we needed the retry topic
func (m *Manager) handleCapacityEvent(ctx context.Context, key, value []byte) error {
	var event CapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal release capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}
	return m.processCapacityEvent(ctx, event)
}

func (m *Manager) processCapacityEvent(ctx context.Context, event CapacityEvent) error {
	reservationID := event.ReservationInfo.Id.String()
	vesselID := event.ReservationInfo.VesselID.String()

	log.Debug().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Str("action", event.Action.String()).
		Int("retry_count", event.RetryCount).
		Msg("processing capacity event")

	if !event.CacheCleared {
		deleted, deleteErr := m.cache.DeleteData(ctx, reservationID)
		if deleteErr != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Err(deleteErr).
				Int("retry_count", event.RetryCount).
				Msg("failed to delete reservation data — scheduling retry")
			if err := m.publishRetryEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule capacity retry: %w", err)
			}

			// Don't return error - we publish a new retry event
			return nil
		}
		if !deleted {
			log.Info().Str("reservation_id", reservationID).Msg("reservation data already deleted — skipping (duplicate event)")
			return nil
		}
		event.CacheCleared = true
	}

	var vesselErr error
	switch event.Action {
	case CONFIRM:
		_, vesselErr = m.vesselCli.ConfirmCapacity(ctx, &vesselpb.CapacityRequest{
			VesselId:           vesselID,
			Weight:             int32(event.ReservationInfo.Weight),
			NumberOfContainers: int32(event.ReservationInfo.NumberOfContainers),
			ReservationId:      reservationID,
		})
	case RELEASE:
		_, vesselErr = m.vesselCli.ReleaseCapacity(ctx, &vesselpb.CapacityRequest{
			VesselId:           vesselID,
			Weight:             int32(event.ReservationInfo.Weight),
			NumberOfContainers: int32(event.ReservationInfo.NumberOfContainers),
			ReservationId:      reservationID,
		})
	}

	if vesselErr != nil {
		log.Error().
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Str("action", event.Action.String()).
			Err(vesselErr).
			Int("retry_count", event.RetryCount).
			Msg("Vessel Capacity call failed — scheduling retry")
		if err := m.publishRetryEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule release retry: %w", err)
		}

		// Don't return error - we publish a new retry event
		return nil
	}

	log.Info().Str("reservation_id", reservationID).
		Str("action", event.Action.String()).
		Str("vessel_id", vesselID).
		Msg("Capacity event succeeded")

	if _, err := m.cache.DeleteID(ctx, reservationID); err != nil {
		log.Warn().Str("reservation_id", reservationID).Err(err).Msg("failed to delete reservation id key — will expire naturally")
	}

	if event.Action == CONFIRM {
		// TODO: add retry
		if err := m.scheduleReservationConfirmed(ctx, &event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentID).
				Err(err).
				Msg("ALERT: failed to schedule reservation confirmed event — manual intervention required")
		}
	}

	return nil
}

func (m *Manager) handleFailedCapacityEvent(ctx context.Context, key, value []byte) error {
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
		// ConfirmCapacity exhausted retries — release capacity and notify CS to refund and cancel.
		releaseEvent := CapacityEvent{
			Action:          RELEASE,
			ReservationInfo: event.ReservationInfo,
			CacheCleared:    event.CacheCleared,
		}
		if err := m.processCapacityEvent(ctx, releaseEvent); err != nil {
			log.Error().Str("reservation_id", reservationID).Err(err).Msg("ALERT: failed to release capacity after confirm exhaustion — manual intervention required")
		}

		if err := m.notifyConfirmationFailed(ctx, &event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentID).
				Str("payment_id", event.PaymentID).
				Err(err).
				Msg("ALERT: failed to notify consignment of confirmation failure — manual refund and cancellation required")

			return fmt.Errorf("failed to publish capacity confirmation failure event: %w", err)
		}

	default:
		log.Error().
			Str("key", string(key)).
			Int("action", int(event.Action)).
			Msg("ALERT: unknown action on DLQ event — manual intervention required")
	}

	return nil
}

func (m *Manager) notifyConfirmationFailed(ctx context.Context, event *CapacityEvent) error {
	payload := FailedConfirmationEvent{
		PaymentCaptured: true,
		CacheCleared:    true,
		PaymentID:       event.PaymentID,
		ConsignmentID:   event.ConsignmentID,
		ReservationID:   event.ReservationInfo.Id.String(),
		VesselID:        event.ReservationInfo.VesselID.String(),
		Weight:          event.ReservationInfo.Weight,
		Containers:      event.ReservationInfo.NumberOfContainers,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal failed confirmation event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ConsignmentConfirmationFailedTopic, // TODO: maybe this should be reservation.confirmation.failed (that is the event that happened)
		Key:     event.ConsignmentID,
		Payload: payloadJSON,
	})
}

func (m *Manager) publishRetryEvent(ctx context.Context, event *CapacityEvent) error {
	event.RetryCount++

	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("ALERT: failed to marshal capacity event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := CapacityFailedTopic
	if event.RetryCount < maxRetries {
		switch event.Action {
		case CONFIRM:
			topic = ConfirmCapacityTopic
		case RELEASE:
			topic = ReleaseCapacityTopic
		}
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
