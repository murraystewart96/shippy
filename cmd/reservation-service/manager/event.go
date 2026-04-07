package manager

import (
	"context"
	"encoding/json"
	"fmt"

	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
)

func (m *Manager) processEvents(ctx context.Context) error {
	if err := m.consumer.StartConsuming(ctx, m.eventHandlers); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

func (m *Manager) handleReleaseReservationEvent(ctx context.Context, key, value []byte) error {
	return m.handleCapacityEvent(ctx, key, value)
}

func (m *Manager) handleConfirmReservationEvent(ctx context.Context, key, value []byte) error {
	return m.handleCapacityEvent(ctx, key, value)
}

func (m *Manager) handleCapacityEvent(ctx context.Context, key, value []byte) error {
	var event CapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		// TODO - publish to DLQ
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal capacity event — manual intervention required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	reservationID := event.ReservationInfo.Id.String()
	vesselID := event.ReservationInfo.VesselID.String()

	log.Info().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Int("retry_count", event.RetryCount).
		Bool("cache_cleared", event.CacheCleared).
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

			event.RetryCount++
			if err := m.scheduleEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule event retry: %w", err)
			}
			// TODO: publish to DLQ when event.RetryCount >= maxRetries
			return fmt.Errorf("failed to delete reservation %s: %w", reservationID, deleteErr)
		}

		if !deleted {
			log.Info().
				Str("reservation_id", reservationID).
				Msg("reservation data already deleted — skipping (duplicate event)")
			return nil
		}

		log.Info().Str("reservation_id", reservationID).Msg("reservation data deleted from cache")
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
		log.Info().Str("reservation_id", reservationID).Str("vessel_id", vesselID).Msg("calling vessel ReleaseCapacity")
		_, vesselErr = m.vesselCli.ReleaseCapacity(ctx, req)
	case CONFIRM:
		log.Info().Str("reservation_id", reservationID).Str("vessel_id", vesselID).Msg("calling vessel ConfirmCapacity")
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

		event.RetryCount++
		if err := m.scheduleEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule event retry: %w", err)
		}
		// TODO: publish to DLQ when event.RetryCount >= maxRetries
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

func (m *Manager) scheduleEvent(ctx context.Context, event *CapacityEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("ALERT: failed to marshal capacity event — manual intervention required")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ReleaseCapacityTopic,
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
