package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/proto/payment"
	"github.com/rs/zerolog/log"
)

const backoffAttempts = 3

func newPaymentBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
	), backoffAttempts)
}

type EventAction int
type EventType int

const (
	RELEASE EventAction = iota
	CONFIRM
	CANCEL
)

func (a EventAction) String() string {
	switch a {
	case RELEASE:
		return "release"
	case CONFIRM:
		return "confirm"
	case CANCEL:
		return "cancel"
	default:
		return "unknown"
	}
}

type reservationInfo struct {
	Id                 string `json:"id"`
	VesselID           string `json:"vessel_id"`
	NumberOfContainers int    `json:"number_of_containers"`
	Weight             int    `json:"weight"`
}

type CapacityEvent struct {
	Action          EventAction     `json:"action"`
	ReservationInfo reservationInfo `json:"reservation_info"`

	// Only used for confirm events that end up in DLQ
	CacheCleared  bool   `json:"cache_cleared"`
	ConsignmentID string `json:"consignment_id"` // Marked as cancelled
	PaymentID     string `json:"payment_id"`     // Refund payment
}

type ConsignmentEvent struct {
	Action        EventAction `json:"action"`
	ConsignmentID string      `json:"consignment_id"`
	RetryCount    int         `json:"retry_count"`
}

type ConfirmationEvent struct {
	CacheCleared    bool   `json:"cache_cleared"`
	PaymentCaptured bool   `json:"payment_captured"`
	PaymentID       string `json:"payment_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	PaymentAuthID   string `json:"payment_auth_id"`
	ReservationID   string `json:"reservation_id"`
	ConsignmentID   string `json:"consignment_id"`
	VesselID        string `json:"vessel_id"`
	Weight          int    `json:"weight"`
	Containers      int    `json:"containers"`
	RetryCount      int    `json:"retry_count"`
}

func (m *Manager) processEvents(ctx context.Context) error {
	if err := m.consumer.StartConsuming(ctx, m.eventHandlers); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

func (m *Manager) handlePaymentAuthorisedEvent(ctx context.Context, key, value []byte) error {
	var event ConfirmationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		// TODO - Alert
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	if !event.PaymentCaptured {
		// capture payment
		event.IdempotencyKey = event.ConsignmentID

		var capResponse *payment.CaptureResponse
		captureErr := backoff.Retry(func() error {
			var err error
			capResponse, err = m.paymentCli.Capture(ctx, &payment.CaptureRequest{
				AuthId:         event.PaymentAuthID,
				IdempotencyKey: event.IdempotencyKey,
			})
			return err
		}, newPaymentBackoff())
		if captureErr != nil {
			event.RetryCount++
			if err := m.schedulePaymentAuthorisedEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule event retry: %w", err)
			}
			return fmt.Errorf("failed to capture payment: %w", captureErr)
		}

		event.PaymentID = capResponse.PaymentId
	}

	event.PaymentCaptured = true

	capacityEvent := &CapacityEvent{
		Action: CONFIRM,
		ReservationInfo: reservationInfo{
			Id:                 event.ReservationID,
			VesselID:           event.VesselID,
			NumberOfContainers: event.Containers,
			Weight:             event.Weight,
		},
		ConsignmentID: event.ConsignmentID,
		PaymentID:     event.PaymentID,
	}

	if err := m.scheduleCapacityEvent(ctx, ConfirmCapacityTopic, event.ConsignmentID, capacityEvent); err != nil {
		event.RetryCount++
		if err := m.schedulePaymentAuthorisedEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule event retry: %w", err)
		}
		return fmt.Errorf("failed to write confirm capacity event to outbox: %w", err)
	}

	confirmedEvent := ConsignmentEvent{Action: CONFIRM, ConsignmentID: event.ConsignmentID}
	if err := m.scheduleConsignmentStatusEvent(ctx, &confirmedEvent); err != nil {
		log.Error().Str("consignment_id", event.ConsignmentID).Err(err).Msg("ALERT: failed to schedule confirmed status event")
	}

	return nil
}

func (m *Manager) handleFailedConfirmationEvent(ctx context.Context, key, value []byte) error {
	var event ConfirmationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal failed confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Undo payment — void if not yet captured, refund if captured
	if !event.PaymentCaptured {
		voidErr := backoff.Retry(func() error {
			_, err := m.paymentCli.Void(ctx, &payment.VoidRequest{AuthId: event.PaymentAuthID})
			return err
		}, newPaymentBackoff())
		if voidErr != nil {
			log.Error().Str("payment_auth_id", event.PaymentAuthID).Err(voidErr).Msg("ALERT: failed to void payment — authorisation will expire naturally")
		}
	} else {
		refundErr := backoff.Retry(func() error {
			_, err := m.paymentCli.Refund(ctx, &payment.RefundRequest{
				PaymentId:      event.PaymentID,
				IdempotencyKey: event.ConsignmentID,
			})
			return err
		}, newPaymentBackoff())
		if refundErr != nil {
			log.Error().Str("payment_id", event.PaymentID).Err(refundErr).Msg("ALERT: failed to refund payment — manual intervention required")
		}
	}

	// Release reservation
	releaseEvent := &CapacityEvent{
		CacheCleared: event.CacheCleared,
		Action:       RELEASE,
		ReservationInfo: reservationInfo{
			Id:                 event.ReservationID,
			VesselID:           event.VesselID,
			NumberOfContainers: event.Containers,
			Weight:             event.Weight,
		},
	}
	if err := m.scheduleCapacityEvent(ctx, ReleaseCapacityTopic, event.ReservationID, releaseEvent); err != nil {
		log.Error().Str("consignment_id", event.ConsignmentID).Err(err).Msg("ALERT: failed to schedule release event — manual intervention required")
	}

	cancelEvent := ConsignmentEvent{Action: CANCEL, ConsignmentID: event.ConsignmentID}
	if err := m.scheduleConsignmentStatusEvent(ctx, &cancelEvent); err != nil {
		log.Error().Str("consignment_id", event.ConsignmentID).Err(err).Msg("ALERT: failed to schedule cancel event — consignment status requires manual update")
	}

	return nil
}

func (m *Manager) handleConsignmentConfirmedEvent(ctx context.Context, key, value []byte) error {
	return m.handleConsignmentStatusEvent(ctx, key, value)
}

func (m *Manager) handleConsignmentCancelledEvent(ctx context.Context, key, value []byte) error {
	return m.handleConsignmentStatusEvent(ctx, key, value)
}

func (m *Manager) handleConsignmentStatusEvent(ctx context.Context, key, value []byte) error {
	var event ConsignmentEvent
	if err := json.Unmarshal(value, &event); err != nil {
		// TODO - Alert
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	var targetStatus storage.ConsignmentStatus
	switch event.Action {
	case CONFIRM:
		targetStatus = storage.StatusConfirmed
	case CANCEL:
		targetStatus = storage.StatusCancelled
	default:
		return fmt.Errorf("unknown event action: %d", event.Action)
	}

	if err := m.repository.UpdateStatus(ctx, event.ConsignmentID, targetStatus); err != nil {
		log.Error().
			Str("consignment_id", event.ConsignmentID).
			Str("action", event.Action.String()).
			Err(err).
			Msg("failed to update consignment status — scheduling retry")

		event.RetryCount++
		if err := m.scheduleConsignmentStatusEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule retry: %w", err)
		}
	}

	return nil
}

func (m *Manager) schedulePaymentAuthorisedEvent(ctx context.Context, event *ConfirmationEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("consignment_id", event.ConsignmentID).
			Err(err).
			Msg("ALERT: failed to marshal capacity event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := ConsignmentPaymentAuthorisedTopic
	if event.RetryCount > maxRetries {
		// TODO: do we have a dedicated DLQ or do we just alert here? the DLQ handler would
		// only alert. might not be worth it
		topic = ConsignmentConfirmationFailedTopic
	}

	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     event.ConsignmentID,
		Payload: eventJSON,
	}); err != nil {
		log.Warn().
			Str("consignment_id", event.ConsignmentID).
			Err(err).
			Msg("failed to create outbox event")
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}

func (m *Manager) scheduleCapacityEvent(ctx context.Context, topic, key string, event *CapacityEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal capacity event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     key,
		Payload: eventJSON,
	})
}

func (m *Manager) scheduleConsignmentStatusEvent(ctx context.Context, event *ConsignmentEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("consignment_id", event.ConsignmentID).
			Str("action", event.Action.String()).
			Err(err).
			Msg("ALERT: failed to marshal consignment event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := ConsignmentStatusFailedTopic
	if event.RetryCount <= maxRetries {
		switch event.Action {
		case CONFIRM:
			topic = ConsignmentConfirmedTopic
		case CANCEL:
			topic = ConsignmentCancelledTopic
		}
	}

	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     event.ConsignmentID,
		Payload: eventJSON,
	}); err != nil {
		log.Warn().
			Str("consignment_id", event.ConsignmentID).
			Str("action", event.Action.String()).
			Err(err).
			Msg("failed to create outbox event")
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}
