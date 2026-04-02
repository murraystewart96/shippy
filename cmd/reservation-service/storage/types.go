package storage

import "github.com/google/uuid"

type ReservationInfo struct {
	VesselID           uuid.UUID
	NumberOfContainers int
	Weight             int
}
