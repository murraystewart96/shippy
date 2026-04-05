package manager

import (
	"context"
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

	maxRetries      = 3
	cleanupInterval = 60
	outboxInterval  = 15
)

type ReleaseCapacityEvent struct {
	ReservationInfo storage.ReservationInfo
	CacheCleared    bool // Used for retries to know if the data entry was cleared on the last attempt
	RetryCount      int
}

type Manager struct {
	cache         storage.ReservationCache
	outbox        storage.OutboxRepository
	vesselCli     vesselpb.VesselServiceClient
	consumer      kafka.IConsumer
	producer      kafka.IProducer
	eventHandlers kafka.EventHandlers
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

func (m *Manager) Start(ctx context.Context) error {
	go m.processOutbox(ctx)

	go m.processReservations(ctx)

	err := m.processReleaseEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to consume release events: %w", err)
	}

	return nil
}

// The managers responsibilites
// 1.) cleanup job -> pushes to kafka
// 2.) kafka consumer handler for restoring capacity

// when creating consignment the vessel service confirms with the reservation service
// also i think the consignment service publishes to reservation restore if it gets a failure from the payment service (after certain number of retries)

func (m *Manager) processReservations(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cleanupInterval) * time.Second)

	for {
		select {
		case <-ticker.C:
			err := m.releaseReservations(ctx)
			if err != nil {
				log.Error().Err(err).Msg("failed to release reservations")

				// TODO - alert if this fails
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (m *Manager) releaseReservations(ctx context.Context) error {
	expired, err := m.cache.GetExpired(ctx)
	if err != nil {
		return fmt.Errorf("failed to get expired reservations: %w", err)
	}

	for _, expiredReservation := range expired {
		event := ReleaseCapacityEvent{
			ReservationInfo: *expiredReservation,
			CacheCleared:    false,
			RetryCount:      0,
		}

		if err := m.scheduleReleaseEvent(ctx, &event); err != nil {
			log.Warn().
				Str("reservation_id", expiredReservation.Id.String()).
				Err(err).
				Msg("failed to schedule release event")
		}
	}

	return nil
}
