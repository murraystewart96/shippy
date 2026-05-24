package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	eventspb "github.com/murraystewart96/shippy/proto/events"
	"github.com/murraystewart96/shippy/proto/payment"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	backoffAttempts = 3
	tracerName      = "consignment-service"
)

func backoffFn() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
	), backoffAttempts)
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

	var event eventspb.PaymentAuthorisedEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentId),
		attribute.String("reservation_id", event.ReservationId),
	)

	if !event.PaymentCaptured {
		event.IdempotencyKey = event.ConsignmentId

		var capResponse *payment.CaptureResponse
		captureErr := backoff.Retry(func() error {
			var err error
			capResponse, err = m.paymentCli.Capture(ctx, &payment.CaptureRequest{
				AuthId:         event.PaymentAuthId,
				IdempotencyKey: event.IdempotencyKey,
			})
			return err
		}, backoffFn())
		if captureErr != nil {
			span.RecordError(captureErr)
			span.SetStatus(codes.Error, "payment capture failed — retrying")
			event.RetryCount++
			if err := m.publishPaymentAuthorised(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule event retry: %w", err)
			}
			log.Error().Err(captureErr).Str("consignment_id", event.ConsignmentId).Msg("payment capture failed — retry scheduled")
			return nil
		}

		event.PaymentId = capResponse.PaymentId
	}

	event.PaymentCaptured = true

	paymentEvent := &eventspb.PaymentCapturedEvent{
		ReservationInfo: &eventspb.ReservationInfo{
			Id:                 event.ReservationId,
			VesselId:           event.VesselId,
			NumberOfContainers: event.Containers,
			Weight:             event.Weight,
		},
		ConsignmentId: event.ConsignmentId,
		PaymentId:     event.PaymentId,
		SagaStartedAt: event.SagaStartedAt,
	}

	paymentID := event.PaymentId

	status := storage.StatusConfirmationPending
	if txErr := m.transactor.WithTransaction(ctx, func(txCtx context.Context) error {
		if err := m.publishPaymentCaptured(txCtx, PaymentCapturedTopic, event.ConsignmentId, paymentEvent); err != nil {
			return err
		}
		return m.repository.Update(txCtx, event.ConsignmentId, storage.ConsignmentUpdate{
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
		log.Error().Err(txErr).Str("consignment_id", event.ConsignmentId).Msg("payment captured tx failed — retry scheduled")
		return nil
	}

	return nil
}

func (m *Manager) handleFailedConfirmationEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleFailedConfirmationEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event eventspb.ConsignmentConfirmationFailedEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal failed confirmation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentId),
		attribute.String("payment_id", event.PaymentId),
	)

	if !event.PaymentCaptured {
		voidErr := backoff.Retry(func() error {
			_, err := m.paymentCli.Void(ctx, &payment.VoidRequest{AuthId: event.PaymentAuthId})
			return err
		}, backoffFn())
		if voidErr != nil {
			span.RecordError(voidErr)
			log.Error().Str("payment_auth_id", event.PaymentAuthId).Err(voidErr).Msg("ALERT: failed to void payment — authorisation will expire naturally")
		}
	} else {
		refundErr := backoff.Retry(func() error {
			_, err := m.paymentCli.Refund(ctx, &payment.RefundRequest{
				PaymentId:      event.PaymentId,
				IdempotencyKey: event.ConsignmentId,
			})
			return err
		}, backoffFn())
		if refundErr != nil {
			span.RecordError(refundErr)
			log.Error().Str("payment_id", event.PaymentId).Err(refundErr).Msg("ALERT: failed to refund payment — manual intervention required")
		}
	}

	if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentId, storage.StatusCancelled); updateErr != nil {
		span.RecordError(updateErr)
		span.SetStatus(codes.Error, updateErr.Error())
		log.Error().
			Str("consignment_id", event.ConsignmentId).
			Err(updateErr).
			Msg("failed to cancel consignment")
		return fmt.Errorf("failed to cancel consignment %s: %w", event.ConsignmentId, updateErr)
	}

	if event.SagaStartedAt != nil && !event.SagaStartedAt.AsTime().IsZero() {
		m.metrics.ObserveSagaDuration(time.Since(event.SagaStartedAt.AsTime()).Seconds(), "cancelled")
	}
	m.metrics.IncSagaTotal("cancelled")

	return nil
}

func (m *Manager) handleExpiredReservationEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleExpiredReservationEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event eventspb.ReservationExpiredEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal expired reservation event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(attribute.String("consignment_id", event.ConsignmentId))

	consignment, err := m.repository.GetByID(ctx, event.ConsignmentId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to get consignment %s: %w", event.ConsignmentId, err)
	}

	if consignment.Status == storage.StatusPending {
		if consignment.PaymentID != "" {
			refundErr := backoff.Retry(func() error {
				_, err := m.paymentCli.Refund(ctx, &payment.RefundRequest{
					PaymentId:      consignment.PaymentID,
					IdempotencyKey: consignment.ID,
				})
				return err
			}, backoffFn())
			if refundErr != nil {
				span.RecordError(refundErr)
				log.Error().Str("payment_id", consignment.PaymentID).Err(refundErr).Msg("ALERT: failed to refund payment — manual intervention required")
			}
		}

		if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentId, storage.StatusCancelled); updateErr != nil {
			span.RecordError(updateErr)
			span.SetStatus(codes.Error, updateErr.Error())
			log.Error().Str("consignment_id", event.ConsignmentId).Err(updateErr).Msg("failed to cancel consignment")
			return fmt.Errorf("failed to cancel consignment %s: %w", event.ConsignmentId, updateErr)
		}
	}

	return nil
}

func (m *Manager) handleReservationConfirmedEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleReservationConfirmedEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event eventspb.ReservationConfirmedEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal reservation confirmed event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(attribute.String("consignment_id", event.ConsignmentId))

	if updateErr := m.repository.UpdateStatus(ctx, event.ConsignmentId, storage.StatusConfirmed); updateErr != nil {
		span.RecordError(updateErr)
		span.SetStatus(codes.Error, updateErr.Error())
		log.Error().Str("consignment_id", event.ConsignmentId).Err(updateErr).Msg("failed to confirm consignment")
		return fmt.Errorf("failed to confirm consignment %s: %w", event.ConsignmentId, updateErr)
	}

	if event.SagaStartedAt != nil && !event.SagaStartedAt.AsTime().IsZero() {
		m.metrics.ObserveSagaDuration(time.Since(event.SagaStartedAt.AsTime()).Seconds(), "confirmed")
	}
	m.metrics.IncSagaTotal("confirmed")

	return nil
}

func (m *Manager) publishPaymentAuthorised(ctx context.Context, event *eventspb.PaymentAuthorisedEvent) error {
	if event.RetryCount > maxRetries {
		return m.publishCaptureFailedToDLQ(ctx, event)
	}

	payload, err := proto.Marshal(event)
	if err != nil {
		log.Error().
			Str("consignment_id", event.ConsignmentId).
			Err(err).
			Msg("ALERT: failed to marshal payment authorised event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	key := fmt.Sprintf("%s-%d", event.ConsignmentId, event.RetryCount)
	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ConsignmentPaymentAuthorisedTopic,
		Key:     key,
		Payload: payload,
	}); err != nil {
		log.Warn().
			Str("consignment_id", event.ConsignmentId).
			Err(err).
			Msg("failed to create outbox event")
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}

// publishCaptureFailedToDLQ converts a PaymentAuthorisedEvent to ConsignmentConfirmationFailedEvent
// and routes it to the confirmation failed topic when capture retries are exhausted.
func (m *Manager) publishCaptureFailedToDLQ(ctx context.Context, event *eventspb.PaymentAuthorisedEvent) error {
	failed := &eventspb.ConsignmentConfirmationFailedEvent{
		ConsignmentId:   event.ConsignmentId,
		PaymentId:       event.PaymentId,
		PaymentAuthId:   event.PaymentAuthId,
		PaymentCaptured: event.PaymentCaptured,
		SagaStartedAt:   event.SagaStartedAt,
	}

	payload, err := proto.Marshal(failed)
	if err != nil {
		return fmt.Errorf("failed to marshal confirmation failed event: %w", err)
	}

	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ConsignmentConfirmationFailedTopic,
		Key:     event.ConsignmentId,
		Payload: payload,
	})
}

func (m *Manager) publishPaymentCaptured(ctx context.Context, topic, key string, event *eventspb.PaymentCapturedEvent) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal payment captured event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     key,
		Payload: payload,
	})
}
