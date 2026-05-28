package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	eventspb "github.com/murraystewart96/shippy/proto/events"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const tracerName = "reservation-service"

func backoffFn() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
	), MaxRetries)
}

// --- Event handlers ---

func (m *Manager) processEvents(ctx context.Context) error {
	if err := m.consumer.StartConsuming(ctx, m.eventHandlers); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

func (m *Manager) handleReservationExpiredEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleReservationExpiredEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var pb eventspb.ReservationExpiredEvent
	if err := proto.Unmarshal(value, &pb); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal reservation expired event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("reservation_id", pb.ReservationId),
		attribute.String("vessel_id", pb.VesselId),
	)

	event := &eventspb.CapacityEvent{
		Action: eventspb.CapacityAction_CAPACITY_ACTION_RELEASE,
		ReservationInfo: &eventspb.ReservationInfo{
			Id:                 pb.ReservationId,
			VesselId:           pb.VesselId,
			ConsignmentId:      pb.ConsignmentId,
			Weight:             pb.Weight,
			NumberOfContainers: pb.Containers,
		},
	}
	if err := m.processCapacityEvent(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (m *Manager) handlePaymentCapturedEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handlePaymentCapturedEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var pb eventspb.PaymentCapturedEvent
	if err := proto.Unmarshal(value, &pb); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal payment captured event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	if pb.ReservationInfo == nil {
		return fmt.Errorf("payment captured event missing reservation_info")
	}

	event := &eventspb.CapacityEvent{
		Action:          eventspb.CapacityAction_CAPACITY_ACTION_CONFIRM,
		ReservationInfo: pb.ReservationInfo,
		ConsignmentId:   pb.ConsignmentId,
		PaymentId:       pb.PaymentId,
		SagaStartedAt:   pb.SagaStartedAt,
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentId),
		attribute.String("reservation_id", event.ReservationInfo.GetId()),
		attribute.String("vessel_id", event.ReservationInfo.GetVesselId()),
	)

	if err := m.processCapacityEvent(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (m *Manager) handleCapacityEvent(ctx context.Context, key, value []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "handleCapacityEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var event eventspb.CapacityEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal event")
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal release capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	span.SetAttributes(
		attribute.String("consignment_id", event.ConsignmentId),
		attribute.String("reservation_id", event.ReservationInfo.GetId()),
		attribute.String("vessel_id", event.ReservationInfo.GetVesselId()),
	)

	if err := m.processCapacityEvent(ctx, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (m *Manager) processCapacityEvent(ctx context.Context, event *eventspb.CapacityEvent) error {
	reservationID := event.ReservationInfo.GetId()
	vesselID := event.ReservationInfo.GetVesselId()

	log.Debug().
		Str("consignment_id", event.ConsignmentId).
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Str("action", event.Action.String()).
		Int32("retry_count", event.RetryCount).
		Msg("processing capacity event")

	if !event.CacheCleared {
		deleted, deleteErr := m.cache.DeleteData(ctx, reservationID)
		if deleteErr != nil {
			log.Error().
				Str("consignment_id", event.ConsignmentId).
				Str("reservation_id", reservationID).
				Err(deleteErr).
				Int32("retry_count", event.RetryCount).
				Msg("failed to delete reservation data — scheduling retry")
			if err := m.publishRetryEvent(ctx, event); err != nil {
				return fmt.Errorf("failed to schedule capacity retry: %w", err)
			}
			return nil
		}
		if !deleted {
			log.Info().Str("consignment_id", event.ConsignmentId).Str("reservation_id", reservationID).Msg("reservation data already deleted — skipping (duplicate event)")
			return nil
		}
		event.CacheCleared = true
	}

	req := &vesselpb.CapacityRequest{
		VesselId:           vesselID,
		Weight:             event.ReservationInfo.GetWeight(),
		NumberOfContainers: event.ReservationInfo.GetNumberOfContainers(),
		ReservationId:      reservationID,
	}
	var vesselErr error
	switch event.Action {
	case eventspb.CapacityAction_CAPACITY_ACTION_CONFIRM:
		vesselErr = backoff.Retry(func() error {
			_, err := m.vesselCli.ConfirmCapacity(ctx, req)
			return err
		}, backoffFn())
	case eventspb.CapacityAction_CAPACITY_ACTION_RELEASE:
		vesselErr = backoff.Retry(func() error {
			_, err := m.vesselCli.ReleaseCapacity(ctx, req)
			return err
		}, backoffFn())
	}

	if vesselErr != nil {
		log.Error().
			Str("consignment_id", event.ConsignmentId).
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Str("action", event.Action.String()).
			Err(vesselErr).
			Int32("retry_count", event.RetryCount).
			Msg("Vessel Capacity call failed — scheduling retry")
		if err := m.publishRetryEvent(ctx, event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentId).
				Err(err).
				Msg("ALERT: failed to publish retry event — manual intervention required")
			return fmt.Errorf("failed to schedule release retry: %w", err)
		}
		return nil
	}

	log.Info().
		Str("consignment_id", event.ConsignmentId).
		Str("reservation_id", reservationID).
		Str("action", event.Action.String()).
		Str("vessel_id", vesselID).
		Msg("Capacity event succeeded")

	if _, err := m.cache.DeleteID(ctx, reservationID); err != nil {
		log.Warn().Str("reservation_id", reservationID).Err(err).Msg("failed to delete reservation id key — will expire naturally")
	}

	if event.Action == eventspb.CapacityAction_CAPACITY_ACTION_CONFIRM {
		if err := m.publishReservationConfirmed(ctx, event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentId).
				Err(err).
				Msg("ALERT: failed to schedule reservation confirmed event — manual intervention required")
		}
	}

	return nil
}

func (m *Manager) handleFailedCapacityEvent(ctx context.Context, key, value []byte) error {
	var event eventspb.CapacityEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal DLQ capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	reservationID := event.ReservationInfo.GetId()
	vesselID := event.ReservationInfo.GetVesselId()

	switch event.Action {
	case eventspb.CapacityAction_CAPACITY_ACTION_RELEASE:
		log.Error().
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Int32("retry_count", event.RetryCount).
			Msg("ALERT: release capacity exhausted retries — vessel capacity may be understated, manual reconciliation required")

	case eventspb.CapacityAction_CAPACITY_ACTION_CONFIRM:
		releaseEvent := &eventspb.CapacityEvent{
			Action:          eventspb.CapacityAction_CAPACITY_ACTION_RELEASE,
			ReservationInfo: event.ReservationInfo,
			CacheCleared:    event.CacheCleared,
		}
		if err := m.processCapacityEvent(ctx, releaseEvent); err != nil {
			log.Error().Str("reservation_id", reservationID).Err(err).Msg("ALERT: failed to release capacity after confirm exhaustion — manual intervention required")
		}

		if err := m.publishConfirmationFailed(ctx, &event); err != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Str("consignment_id", event.ConsignmentId).
				Str("payment_id", event.PaymentId).
				Err(err).
				Msg("ALERT: failed to notify consignment of confirmation failure — manual refund and cancellation required")
			return fmt.Errorf("failed to publish capacity confirmation failure event: %w", err)
		}

	default:
		log.Error().
			Str("key", string(key)).
			Int32("action", int32(event.Action)).
			Msg("ALERT: unknown action")
	}

	return nil
}

// --- Publish functions ---

func (m *Manager) publishReservationExpired(ctx context.Context, r *storage.ReservationInfo) error {
	payload, err := proto.Marshal(&eventspb.ReservationExpiredEvent{
		ReservationId: r.Id.String(),
		VesselId:      r.VesselID.String(),
		ConsignmentId: r.ConsignmentID.String(),
		Weight:        int32(r.Weight),
		Containers:    int32(r.NumberOfContainers),
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

func (m *Manager) publishReservationConfirmed(ctx context.Context, event *eventspb.CapacityEvent) error {
	payload, err := proto.Marshal(&eventspb.ReservationConfirmedEvent{
		ReservationId: event.ReservationInfo.GetId(),
		ConsignmentId: event.ConsignmentId,
		SagaStartedAt: event.SagaStartedAt,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal reservation confirmed event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ReservationConfirmedTopic,
		Key:     event.ConsignmentId,
		Payload: payload,
	})
}

func (m *Manager) publishConfirmationFailed(ctx context.Context, event *eventspb.CapacityEvent) error {
	payload, err := proto.Marshal(&eventspb.ConsignmentConfirmationFailedEvent{
		PaymentCaptured: true,
		CacheCleared:    true,
		PaymentId:       event.PaymentId,
		ConsignmentId:   event.ConsignmentId,
		ReservationId:   event.ReservationInfo.GetId(),
		VesselId:        event.ReservationInfo.GetVesselId(),
		Weight:          event.ReservationInfo.GetWeight(),
		Containers:      event.ReservationInfo.GetNumberOfContainers(),
		SagaStartedAt:   event.SagaStartedAt,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal failed confirmation event: %w", err)
	}
	return m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ConsignmentConfirmationFailedTopic,
		Key:     event.ConsignmentId,
		Payload: payload,
	})
}

func (m *Manager) publishRetryEvent(ctx context.Context, event *eventspb.CapacityEvent) error {
	event.RetryCount++

	payload, err := proto.Marshal(event)
	if err != nil {
		log.Error().
			Str("reservation_id", event.ReservationInfo.GetId()).
			Err(err).
			Msg("ALERT: failed to marshal capacity event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := CapacityFailedTopic
	if event.RetryCount < int32(MaxRetries) {
		switch event.Action {
		case eventspb.CapacityAction_CAPACITY_ACTION_CONFIRM:
			topic = ConfirmCapacityTopic
		case eventspb.CapacityAction_CAPACITY_ACTION_RELEASE:
			topic = ReleaseCapacityTopic
		}
	}

	key := fmt.Sprintf("%s-%d", event.ReservationInfo.GetId(), event.RetryCount)
	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   topic,
		Key:     key,
		Payload: payload,
	}); err != nil {
		log.Warn().
			Str("reservation_id", event.ReservationInfo.GetId()).
			Err(err).
			Msg("failed to create outbox event")
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}
