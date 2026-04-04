package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/reservation-service/storage"
)


func (db *DB) CreateEvent(ctx context.Context, event *storage.OutboxEvent) error {
	_, err := db.pool.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload)
		 VALUES ($1, $2, $3)`,
		event.Topic, event.Key, event.Payload,
	)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}
	return nil
}

func (db *DB) MarkPublished(ctx context.Context, id uuid.UUID) error {
	result, err := db.pool.Exec(ctx,
		`UPDATE outbox SET published_at = $1 WHERE id = $2`,
		time.Now(), id,
	)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as published: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox event %s not found", id)
	}
	return nil
}

func (db *DB) GetPendingEvents(ctx context.Context) ([]*storage.OutboxEvent, error) {
	rows, err := db.pool.Query(ctx,
		`SELECT id, topic, key, payload, created_at, published_at
		 FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*storage.OutboxEvent
	for rows.Next() {
		event := &storage.OutboxEvent{}
		if err := rows.Scan(
			&event.Id,
			&event.Topic,
			&event.Key,
			&event.Payload,
			&event.CreatedAt,
			&event.PublishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, event)
	}

	return events, nil
}
