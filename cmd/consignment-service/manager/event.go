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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	backoffAttempts = 3
	tracerName      = "consignment-service"
)

func newPaymentBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
	), backoffAttempts)
}

type reservationInfo struct {
	Id                 string `json:"id"`
	VesselID           string `json:"vessel_id"`
	NumberOfContainers int    `json:"number_of_containers"`
	Weight             int    `json:"weight"`
}

type PaymentCapturedEvent struct {
	ReservationInfo reservationInfo `json:"reservation_info"`

	// Only used for confirm events that end up in DLQ
	CacheCleared  bool   `json:"cache_cleared"`
	ConsignmentID string `json:"consignment_id"` // Marks as cancelled
	PaymentID     string `json:"payment_id"`     // Refund payment
}

type ReservationEvent struct {
	ConsignmentID string `json:"consignment_id"`
	RetryCount    int    `json:"retry_count"`
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
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handlePaymentAuthorisedEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event ConfirmationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		// TODO - Alert
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentID),
		attribute.String("reservation_id", event.ReservationID),
	)

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
			span.RecordError(captureErr)
			span.SetStatus(codes.Error, "payment capture failed — retrying")
			event.RetryCount++
			if err := m.publishPaymentAuthorised(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule event retry: %w", err)
			}
			log.Error().Err(captureErr).Str("consignment_id", event.ConsignmentID).Msg("payment capture failed — retry scheduled")

			// Don't return error - we publish a new retry event
			return nil
		}

		event.PaymentID = capResponse.PaymentId
	}

	event.PaymentCaptured = true

	paymentEvent := &PaymentCapturedEvent{
		ReservationInfo: reservationInfo{
			Id:                 event.ReservationID,
			VesselID:           event.VesselID,
			NumberOfContainers: event.Containers,
			Weight:             event.Weight,
		},
		ConsignmentID: event.ConsignmentID,
		PaymentID:     event.PaymentID,
	}

	paymentID := event.PaymentID

	// TODO: refactor into helper
	status := storage.StatusConfirmationPending
	if txErr := m.transactor.WithTransaction(ctx, func(txCtx context.Context) error {
		if err := m.publishPaymentCaptured(txCtx, PaymentCapturedTopic, event.ConsignmentID, paymentEvent); err != nil {
			return err
		}
		return m.repository.Update(txCtx, event.ConsignmentID, storage.ConsignmentUpdate{
			Status:    &status,
			PaymentID: &paymentID,
		})
	}); txErr != nil {
		span.RecordError(txErr)
		span.SetStatus(codes.Error, "payment captured tx failed — retrying")
		event.RetryCount++
		if scheduleErr := m.publishPaymentAuthorised(ctx, &event); scheduleErr != nil {
			return fmt.Errorf("failed to schedule event retry: %w", scheduleErr)
		}
		log.Error().Err(txErr).Str("consignment_id", event.ConsignmentID).Msg("payment captured tx failed — retry scheduled")
		return nil
	}

	return nil
}

func (m *Manager) handleFailedConfirmationEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleFailedConfirmationEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event ConfirmationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal failed confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentID),
		attribute.String("payment_id", event.PaymentID),
	)

	// Undo payment — void if not yet captured, refund if captured
	if !event.PaymentCaptured {
		voidErr := backoff.Retry(func() error {
			_, err := m.paymentCli.Void(ctx, &payment.VoidRequest{AuthId: event.PaymentAuthID})
			return err
		}, newPaymentBackoff())
		if voidErr != nil {
			span.RecordError(voidErr)
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
			span.RecordError(refundErr)
			log.Error().Str("payment_id", event.PaymentID).Err(refundErr).Msg("ALERT: failed to refund payment — manual intervention required")
		}
	}

	if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentID, storage.StatusCancelled); updateErr != nil {
		span.RecordError(updateErr)
		span.SetStatus(codes.Error, updateErr.Error())
		log.Error().
			Str("consignment_id", event.ConsignmentID).
			Err(updateErr).
			Msg("failed to cancel consignment")

		return fmt.Errorf("failed to cancel consignment %s: %w", event.ConsignmentID, updateErr)
	}

	return nil
}

func (m *Manager) handleExpiredReservationEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleExpiredReservationEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event ReservationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal expired reservation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(attribute.String("consignment_id", event.ConsignmentID))

	consignment, err := m.repository.GetByID(ctx, event.ConsignmentID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to get consignment %s: %w", event.ConsignmentID, err)
	}

	// If consignment still pending cancel and refund if neccessary
	if consignment.Status == storage.StatusPending {
		if consignment.PaymentID != "" {
			// REFUND
			refundErr := backoff.Retry(func() error {
				_, err := m.paymentCli.Refund(ctx, &payment.RefundRequest{
					PaymentId:      consignment.PaymentID,
					IdempotencyKey: consignment.ID,
				})
				return err
			}, newPaymentBackoff())
			if refundErr != nil {
				span.RecordError(refundErr)
				log.Error().Str("payment_id", consignment.PaymentID).Err(refundErr).Msg("ALERT: failed to refund payment — manual intervention required")
			}
		}

		// CANCEL
		if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentID, storage.StatusCancelled); updateErr != nil {
			span.RecordError(updateErr)
			span.SetStatus(codes.Error, updateErr.Error())
			log.Error().Str("consignment_id", event.ConsignmentID).Err(updateErr).Msg("failed to cancel consignment")
			return fmt.Errorf("failed to cancel consignment %s: %w", event.ConsignmentID, updateErr)
		}
	}

	return nil
}

// TODO: merge with function above
func (m *Manager) handleReservationConfirmedEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleReservationConfirmedEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event ReservationEvent
	if err := json.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal expired reservation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(attribute.String("consignment_id", event.ConsignmentID))

	if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentID, storage.StatusConfirmed); updateErr != nil {
		span.RecordError(updateErr)
		span.SetStatus(codes.Error, updateErr.Error())
		log.Error().Str("consignment_id", event.ConsignmentID).Err(updateErr).Msg("failed to cancel consignment")
		return fmt.Errorf("failed to confirm consignment %s: %w", event.ConsignmentID, updateErr)
	}

	return nil
}

func (m *Manager) publishPaymentAuthorised(ctx context.Context, event *ConfirmationEvent) error {
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

func (m *Manager) publishPaymentCaptured(ctx context.Context, topic, key string, event *PaymentCapturedEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal payment captured event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     key,
		Payload: eventJSON,
	})
}
