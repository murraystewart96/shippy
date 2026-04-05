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
		// TODO - republish event?
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// If data entry was cleared on the last event attempt skip the delete check
	// The delete check acts as a lock so that the first routine to delete the data gets to process the event
	if !event.CacheCleared {
		// Delete the reservation entry from cache.
		deleted, deleteErr := m.cache.DeleteData(ctx, event.ReservationInfo.Id.String())
		if deleteErr != nil {
			// TODO - log errors when they happen
			event.RetryCount++
			if err := m.scheduleReleaseEvent(ctx, &event); err != nil {
				return fmt.Errorf("failed to schedule release event retry: %w", err)
			}
			return fmt.Errorf("failed to delete reservation %s: %w", event.ReservationInfo.Id.String(), deleteErr)
		}

		// Deletes are atomic so if there was nothing to delete another routine has already
		// handled the event
		if !deleted {
			return nil
		}

		// Reservation data has been cleared from the cache
		// This is set so event retries skip the delete check
		event.CacheCleared = true
	}

	req := &vesselpb.CapacityRequest{
		VesselId:           event.ReservationInfo.VesselID.String(),
		Weight:             int32(event.ReservationInfo.Weight),
		NumberOfContainers: int32(event.ReservationInfo.NumberOfContainers),
	}

	// Make call to vessel service to release the capacity
	_, releaseErr := m.vesselCli.ReleaseCapacity(ctx, req)
	if releaseErr != nil {
		event.RetryCount++
		if err := m.scheduleReleaseEvent(ctx, &event); err != nil {
			return fmt.Errorf("failed to schedule release event retry: %w", err)
		}
		return fmt.Errorf("vessel ReleaseCapacity failed: %w", releaseErr)
	}

	// Delete reservation ID from cache
	if _, err := m.cache.DeleteID(ctx, event.ReservationInfo.Id.String()); err != nil {
		// Not critical as cache will clean it up eventually
		log.Warn().
			Str("reservation_id", event.ReservationInfo.Id.String()).
			Err(err).
			Msg("failed to delete reservation id key")
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
		Topic:   releaseCapacityTopic,
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
