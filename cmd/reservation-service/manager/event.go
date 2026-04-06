package manager

import (
	"context"
	"encoding/json"
	"fmt"

	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
)

// ConsumeReleaseEvents consumes release reservation events
func (m *Manager) processReleaseEvents(ctx context.Context) error {
	err := m.consumer.StartConsuming(ctx, m.eventHandlers)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	return nil
}

func (m *Manager) handleReleaseReservationEvent(ctx context.Context, key, value []byte) error {
	var event ReleaseCapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		// TODO - publish to DLQ
		log.Error().Err(err).Str("key", string(key)).Msg("ALERT: failed to unmarshal release event — manual capacity release required")
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	reservationID := event.ReservationInfo.Id.String()
	vesselID := event.ReservationInfo.VesselID.String()

	log.Info().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Int("retry_count", event.RetryCount).
		Bool("cache_cleared", event.CacheCleared).
		Msg("handling release reservation event")

	if !event.CacheCleared {
		deleted, deleteErr := m.cache.DeleteData(ctx, reservationID)
		if deleteErr != nil {
			log.Error().
				Str("reservation_id", reservationID).
				Err(deleteErr).
				Int("retry_count", event.RetryCount).
				Msg("failed to delete reservation data — scheduling retry")

			event.RetryCount++
			if err := m.scheduleReleaseEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule release event retry: %w", err)
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

	log.Info().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Msg("calling vessel ReleaseCapacity")

	_, releaseErr := m.vesselCli.ReleaseCapacity(ctx, req)
	if releaseErr != nil {
		log.Error().
			Str("reservation_id", reservationID).
			Str("vessel_id", vesselID).
			Err(releaseErr).
			Int("retry_count", event.RetryCount).
			Msg("vessel ReleaseCapacity failed — scheduling retry")

		event.RetryCount++
		if err := m.scheduleReleaseEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule release event retry: %w", err)
		}
		// TODO: publish to DLQ when event.RetryCount >= maxRetries
		return fmt.Errorf("vessel ReleaseCapacity failed: %w", releaseErr)
	}

	log.Info().
		Str("reservation_id", reservationID).
		Str("vessel_id", vesselID).
		Msg("vessel capacity released successfully")

	if _, err := m.cache.DeleteID(ctx, reservationID); err != nil {
		log.Warn().
			Str("reservation_id", reservationID).
			Err(err).
			Msg("failed to delete reservation id key — will expire naturally")
	}

	return nil
}

func (m *Manager) scheduleReleaseEvent(ctx context.Context, event *ReleaseCapacityEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("ALERT: failed to marshal release event — manual capacity release required")

		return fmt.Errorf("failed to marshal release event: %w", err)
	}

	if err := m.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Topic:   ReleaseCapacityTopic,
		Key:     event.ReservationInfo.Id.String(),
		Payload: eventJSON,
	}); err != nil {
		log.Warn().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("failed to create outbox event for reservation release")

		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return nil
}
