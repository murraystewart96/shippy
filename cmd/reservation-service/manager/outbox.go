package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// publishOutbox published all event retries in the outbox table
func (m *Manager) processOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(outboxInterval) * time.Second)

	for {
		select {
		case <-ticker.C:
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
	// Get unpublished events
	events, err := m.outbox.GetPendingEvents(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to get pending outbox events")

		return fmt.Errorf("failed to get pending outbox events: %w", err)

		// TODO - alert if this fails
	}

	for _, event := range events {
		if err := m.producer.Produce(ctx, event.Topic, []byte(event.Key), event.Payload); err != nil {
			log.Warn().
				Str("reservation_id", event.Key).
				Err(err).
				Msg("failed to publish release event retry from outbox")

			continue
		}

		err = m.outbox.MarkPublished(ctx, event.Id)
		if err != nil {
			log.Warn().
				Str("event id", event.Id.String()).
				Err(err).
				Msg("failed to mark event as published")
		}
	}

	return nil
}
