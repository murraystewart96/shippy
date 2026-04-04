package storage

import (
	"time"

	"github.com/google/uuid"
)

type ReservationInfo struct {
	Id                 uuid.UUID
	VesselID           uuid.UUID
	NumberOfContainers int
	Weight             int
}

type OutboxEvent struct {
	Id          uuid.UUID
	Topic       string
	Key         string
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
}
