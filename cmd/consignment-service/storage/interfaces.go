package storage

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type ConsignmentRepository interface {
	Create(ctx context.Context, consignment *Consignment) error
	GetByID(ctx context.Context, id string) (*Consignment, error)
	GetAll(ctx context.Context) ([]*Consignment, error)
	UpdateStatus(ctx context.Context, id string, status ConsignmentStatus) error
}

type OutboxRepository interface {
	CreateEvent(ctx context.Context, event *OutboxEvent) error
	MarkPublished(ctx context.Context, id uuid.UUID) error
	GetPendingEvents(ctx context.Context, lease time.Duration) ([]*OutboxEvent, error)
}
