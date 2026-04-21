package storage

import (
	"time"

	"github.com/google/uuid"
)

type ConsignmentStatus string

const (
	StatusPending   ConsignmentStatus = "pending"
	StatusConfirmed ConsignmentStatus = "confirmed"
	StatusCancelled ConsignmentStatus = "cancelled"
)

type Consignment struct {
	ID             string            `bson:"_id"            json:"id"`
	Weight         int32             `bson:"weight"         json:"weight"`
	Description    string            `bson:"description"    json:"description"`
	Containers     Containers        `bson:"containers"     json:"containers"`
	VesselID       string            `bson:"vessel_id"      json:"vessel_id"`
	ReservationID  string            `bson:"reservation_id" json:"reservation_id"`
	Status         ConsignmentStatus `bson:"status"         json:"status"`
}

type Container struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	UserID     string `json:"user_id"`
}

type Containers []*Container

type OutboxEvent struct {
	Id              uuid.UUID
	Topic           string
	Key             string
	Payload         []byte
	CreatedAt       time.Time
	PublishedAt     *time.Time
	ProcessingUntil *time.Time
	ClaimID         string
}
