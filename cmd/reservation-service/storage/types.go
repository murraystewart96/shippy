package storage

import (
	"time"

	"github.com/google/uuid"
)

type ReservationInfo struct {
	Id                 uuid.UUID `json:"id"`
	VesselID           uuid.UUID `json:"vessel_id"`
	ConsignmentID      uuid.UUID `json:"consignment_id"`
	NumberOfContainers int       `json:"number_of_containers"`
	Weight             int       `json:"weight"`
}

type OutboxEvent struct {
	Id          uuid.UUID
	Topic       string
	Key         string
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
}
