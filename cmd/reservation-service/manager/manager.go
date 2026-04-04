package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
)

const (
	releaseCapacityTopic    = "reservation.capacity.release"
	releaseCapacityDLQTopic = "reservation.capacity.release.dlq"

	maxRetries = 3
)

type ReleaseCapacityEvent struct {
	ReservationInfo storage.ReservationInfo
	CacheCleared    bool // Used for retries to know if the data entry was cleared on the last attempt
	RetryCount      int
}

type Manager struct {
	cache           storage.ReservationCache
	outbox          storage.OutboxRepository
	vesselCli       vesselpb.VesselServiceClient
	consumer        kafka.IConsumer
	producer        kafka.IProducer
	eventHandlers   kafka.EventHandlers
	cleanupInterval int
}

func New(
	vesselCli vesselpb.VesselServiceClient,
	producer kafka.IProducer,
	consumer kafka.IConsumer,
	topics []string,
	cache storage.ReservationCache,
	outbox storage.OutboxRepository,
) (*Manager, error) {
	manager := &Manager{
		cache:     cache,
		consumer:  consumer,
		producer:  producer,
		vesselCli: vesselCli,
		outbox:    outbox,
	}

	eventHandlers := kafka.EventHandlers{
		releaseCapacityTopic: manager.handleReleaseReservationEvent,
	}

	// Assign configured topic handlers
	activeHandlers := make(kafka.EventHandlers)
	for _, topic := range topics {
		handler, found := eventHandlers[topic]
		if !found {
			return nil, fmt.Errorf("no event handler for topic: %s", topic)
		}
		activeHandlers[topic] = handler
	}

	manager.eventHandlers = activeHandlers

	return manager, nil
}

// The managers responsibilites
// 1.) cleanup job -> pushes to kafka
// 2.) kafka consumer handler for restoring capacity

// when creating consignment the vessel service confirms with the reservation service
// also i think the consignment service publishes to reservation restore if it gets a failure from the payment service (after certain number of retries)

func (m *Manager) Cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cleanupInterval) * time.Second)

	for {
		select {
		case <-ticker.C:
			expired, err := m.cache.GetExpired(ctx)
			if err != nil {
				log.Error().Err(err).Msg("failed to get expired reservations")
				continue

				// TODO - alert if this fails
			}

			for _, expiredReservation := range expired {
				event := ReleaseCapacityEvent{
					ReservationInfo: *expiredReservation,
					CacheCleared:    false,
					RetryCount:      0,
				}

				eventJSON, err := json.Marshal(event)
				if err != nil {
					log.Error().
						Str("reservation_id", expiredReservation.Id.String()).
						Err(err).
						Msg("ALERT: failed to marshal release event — manual capacity release required")
					continue
				}

				if err := m.producer.Produce(ctx, releaseCapacityTopic, []byte(expiredReservation.Id.String()), eventJSON); err != nil {
					// Not critical - will be picked up by the next cleanup job
					log.Warn().
						Str("reservation_id", expiredReservation.Id.String()).
						Err(err).
						Msg("failed to publish release event")
				}
			}

		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// ReleaseReservation consumes release reservation events
func (m *Manager) ReleaseReservations(ctx context.Context) error {
	err := m.consumer.StartConsuming(ctx, m.eventHandlers)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	return nil
}

func (m *Manager) handleReleaseReservationEvent(ctx context.Context, key, value []byte) error {
	var event ReleaseCapacityEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// If the event isn't aware of the data entry being cleared
	// If it was cleared this is a retry and data entry was cleared on the last attempt
	// Hence we should just process the event immediately
	if !event.CacheCleared {
		// Delete the reservation entry from cache.
		deleted, deleteErr := m.cache.DeleteData(ctx, event.ReservationInfo.Id.String())
		if deleteErr != nil {
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
		// This is set to event retries skip the delete check
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
