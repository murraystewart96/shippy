package storage

import (
	"context"

	"github.com/google/uuid"
)

type ReservationCache interface {
	Store(ctx context.Context, id string, reservation ReservationInfo) error
	GetData(ctx context.Context, id string) (ReservationInfo, error)
	GetExpired(ctx context.Context) ([]*ReservationInfo, error)
	DeleteData(ctx context.Context, id string) (bool, error)
	DeleteID(ctx context.Context, id string) (bool, error)
}

type OutboxRepository interface {
	CreateEvent(ctx context.Context, event *OutboxEvent) error
	MarkPublished(ctx context.Context, id uuid.UUID) error
	GetPendingEvents(ctx context.Context) ([]*OutboxEvent, error)
}
