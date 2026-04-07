package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/murraystewart96/shippy/reservation-service/storage"
)


func (db *DB) CreateEvent(ctx context.Context, event *storage.OutboxEvent) error {
	_, err := db.pool.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload)
		 VALUES ($1, $2, $3)`,
		event.Topic, event.Key, event.Payload,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Unique violation — another instance already scheduled this event
			return nil
		}
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

func (db *DB) GetPendingEvents(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id, topic, key, payload, created_at, published_at
		 FROM outbox
		 WHERE published_at IS NULL
		   AND (processing_until IS NULL OR processing_until < NOW())
		 ORDER BY created_at ASC
		 FOR UPDATE SKIP LOCKED`,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending outbox events: %w", err)
	}

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
			rows.Close()
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	rows.Close()

	if len(events) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(events))
	for i, e := range events {
		ids[i] = e.Id
	}

	_, err = tx.Exec(ctx,
		`UPDATE outbox SET processing_until = $1 WHERE id = ANY($2)`,
		time.Now().Add(lease), ids,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox lease: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit outbox lease: %w", err)
	}

	return events, nil
}
