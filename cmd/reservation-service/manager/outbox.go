package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

func (m *Manager) processOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.outboxInterval) * time.Second)
	log.Info().Int("interval_seconds", m.outboxInterval).Msg("outbox publisher started")

	for {
		select {
		case <-ticker.C:
			log.Debug().Msg("running outbox publish")
			err := m.publishOutbox(ctx)
			if err != nil {
				log.Warn().Err(err).Msg("failed to publish outbox")
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (m *Manager) publishOutbox(ctx context.Context) error {
	events, err := m.outbox.GetPendingEvents(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to get pending outbox events")
		// TODO - alert if this fails
		return fmt.Errorf("failed to get pending outbox events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	log.Info().Int("count", len(events)).Msg("publishing outbox events")

	for _, event := range events {
		if err := m.producer.Produce(ctx, event.Topic, []byte(event.Key), event.Payload); err != nil {
			log.Warn().
				Str("reservation_id", event.Key).
				Str("topic", event.Topic).
				Err(err).
				Msg("failed to publish outbox event")
			continue
		}

		log.Info().
			Str("event_id", event.Id.String()).
			Str("reservation_id", event.Key).
			Str("topic", event.Topic).
			Msg("outbox event published")

		if err = m.outbox.MarkPublished(ctx, event.Id); err != nil {
			log.Warn().
				Str("event_id", event.Id.String()).
				Err(err).
				Msg("failed to mark outbox event as published")
		}
	}

	return nil
}
