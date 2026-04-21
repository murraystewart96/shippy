package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/rs/zerolog/log"
)

const (
	ReleaseCapacityTopic = "reservation.capacity.release"
	ConfirmCapacityTopic = "reservation.capacity.confirm"
	CapacityDLQTopic     = "reservation.capacity.dlq"

	ConfirmConsignmentDLQTopic = "consignment.confirm.dlq"

	maxRetries = 3
)

type Manager struct {
	cache           storage.ReservationCache
	outbox          storage.OutboxRepository
	vesselCli       vesselpb.VesselServiceClient
	consumer        kafka.IConsumer
	producer        kafka.IProducer
	eventHandlers   kafka.EventHandlers
	cleanupInterval int
	outboxInterval  int
}

func New(
	vesselCli vesselpb.VesselServiceClient,
	producer kafka.IProducer,
	consumer kafka.IConsumer,
	topics []string,
	cache storage.ReservationCache,
	outbox storage.OutboxRepository,
	cfg config.Manager,
) (*Manager, error) {
	manager := &Manager{
		cache:           cache,
		consumer:        consumer,
		producer:        producer,
		vesselCli:       vesselCli,
		outbox:          outbox,
		cleanupInterval: cfg.CleanupInterval,
		outboxInterval:  cfg.OutboxInterval,
	}

	eventHandlers := kafka.EventHandlers{
		ReleaseCapacityTopic: manager.handleCapacityEvent,
		ConfirmCapacityTopic: manager.handleCapacityEvent,
		CapacityDLQTopic:     manager.handleCapacityDLQEvent,
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

func (m *Manager) Start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.processOutbox(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.processReservations(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.processEvents(ctx); err != nil {
			errCh <- err
		}
	}()

	return errCh
}

// The managers responsibilites
// 1.) cleanup job -> pushes to kafka
// 2.) kafka consumer handler for restoring capacity

// when creating consignment the vessel service confirms with the reservation service
// also i think the consignment service publishes to reservation restore if it gets a failure from the payment service (after certain number of retries)

func (m *Manager) processReservations(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cleanupInterval) * time.Second)
	log.Info().Int("interval_seconds", m.cleanupInterval).Msg("reservation cleanup job started")

	for {
		select {
		case <-ticker.C:
			log.Debug().Msg("running reservation cleanup")
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

	if len(expired) == 0 {
		log.Debug().Msg("no expired reservations found")
		return nil
	}

	log.Info().Int("count", len(expired)).Msg("found expired reservations — scheduling release events")

	for _, expiredReservation := range expired {
		event := CapacityEvent{
			Action:          RELEASE,
			ReservationInfo: *expiredReservation,
			CacheCleared:    false,
			RetryCount:      0,
		}

		if err := m.scheduleEvent(ctx, &event); err != nil {
			log.Warn().
				Str("reservation_id", expiredReservation.Id.String()).
				Err(err).
				Msg("failed to schedule release event")
		} else {
			log.Info().
				Str("reservation_id", expiredReservation.Id.String()).
				Str("vessel_id", expiredReservation.VesselID.String()).
				Msg("release event scheduled")
		}

		// TODO: check if this is the right level to publish
		// We should also schedule event to cancel the consignment
		// i think we could also use the outbox pattern here.
		// we would either need to update the schedule event function to accept
		// a consignment event also or we coud publish here directly

		// we need the reservation info to also store the consignment ID
	}

	return nil
}
