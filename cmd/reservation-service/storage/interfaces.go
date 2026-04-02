package storage

import "context"

type ReservationCache interface {
	Store(ctx context.Context, id string, reservation ReservationInfo) error
	Get(ctx context.Context, id string) (ReservationInfo, error)
	Delete(ctx context.Context, id string) (bool, error)
}
