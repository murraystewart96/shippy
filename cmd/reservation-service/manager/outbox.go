package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/murraystewart96/shippy/pkg/kafka"
	"github.com/rs/zerolog/log"
)

const (
	outboxLeaseDuration   = 30 * time.Second
	outboxPublishDeadline = 20 * time.Second
)

func (m *Manager) processOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.outboxInterval) * time.Second)
	log.Info().Int("interval_seconds", m.outboxInterval).Msg("outbox publisher started")

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
	publishCtx, cancel := context.WithTimeout(ctx, outboxPublishDeadline)
	defer cancel()

	events, err := m.outbox.GetPendingEvents(ctx, outboxLeaseDuration)
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
		if err := publishCtx.Err(); err != nil {
			log.Info().Msg("publish deadline exceeded — remaining events will retry on next tick")
			return nil
		}

		headers := kafka.HeadersFromTraceContext(event.TraceContext)
		if err := m.producer.Produce(publishCtx, event.Topic, []byte(event.Key), event.Payload, headers); err != nil {
			log.Warn().
				Str("reservation_id", event.Key).
				Str("topic", event.Topic).
				Err(err).
				Msg("failed to publish outbox event")

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Info().Msg("context cancelled — stopping outbox publish")
				return nil
			}

			if errors.Is(err, kafka.ErrBrokerUnreachable) {
				log.Error().Err(err).Msg("kafka unreachable — aborting outbox publish")
				return err // break out entirely, all rows stay unpublished
			}

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
