package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/murraystewart96/shippy/proto/vessel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	opReserve = "reserve"
	opRelease = "release"
	opConfirm = "confirm"
)

type repository interface {
	ReserveCapacity(ctx context.Context, spec *Specification) (*Vessel, error)
	ReleaseCapacity(ctx context.Context, req *CapacityRequest) error
	ConfirmCapacity(ctx context.Context, req *CapacityRequest) error
	Create(ctx context.Context, vessel *Vessel) error
}

type Specification struct {
	ReservationID      string
	Weight             int
	NumberOfContainers int
}

type CapacityRequest struct {
	ReservationID      string
	VesselID           string
	Weight             int
	NumberOfContainers int
}

type Vessel struct {
	ID               string `json:"id" bson:"id"`
	Capacity         int    `json:"capacity" bson:"capacity"`
	MaxWeight        int    `json:"max_weight" bson:"max_weight"`
	ReservedWeight   int    `json:"reserved_weight" bson:"reserved_weight"`
	UsedWeight       int    `json:"used_weight" bson:"used_weight"`
	ReservedCapacity int    `json:"reserved_capacity" bson:"reserved_capacity"`
	UsedCapacity     int    `json:"used_capacity" bson:"used_capacity"`
	Name             string `json:"name" bson:"name"`
	Available        bool   `json:"available" bson:"available"`
}

// CapacityOperation records each capacity change against a reservation ID.
// A unique compound index on (reservation_id, operation) ensures each operation
// is applied exactly once, even if retried.
type CapacityOperation struct {
	ReservationID string    `bson:"reservation_id"`
	Operation     string    `bson:"operation"`
	VesselID      string    `bson:"vessel_id"`
	CreatedAt     time.Time `bson:"created_at"`
}

type MongoRepository struct {
	client      *mongo.Client
	vessels     *mongo.Collection
	capacityOps *mongo.Collection
}

func (repo *MongoRepository) SetupIndexes(ctx context.Context) error {
	_, err := repo.capacityOps.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "reservation_id", Value: 1},
				{Key: "operation", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "created_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(30 * 24 * 60 * 60), // 30 days
		},
	})
	return err
}

// ReserveCapacity atomically finds a vessel with sufficient available capacity and reserves it.
// available_weight = max_weight - reserved_weight - used_weight
// available_capacity = capacity - reserved_capacity - used_capacity
func (repo *MongoRepository) ReserveCapacity(ctx context.Context, spec *Specification) (*Vessel, error) {
	session, err := repo.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	var vessel Vessel
	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return err
		}

		filter := bson.M{
			"$expr": bson.M{
				"$and": bson.A{
					bson.M{"$gte": bson.A{
						bson.M{"$subtract": bson.A{
							bson.M{"$subtract": bson.A{"$capacity", "$reserved_capacity"}},
							"$used_capacity",
						}},
						spec.NumberOfContainers,
					}},
					bson.M{"$gte": bson.A{
						bson.M{"$subtract": bson.A{
							bson.M{"$subtract": bson.A{"$max_weight", "$reserved_weight"}},
							"$used_weight",
						}},
						spec.Weight,
					}},
				},
			},
		}

		update := bson.M{
			"$inc": bson.M{
				"reserved_weight":   spec.Weight,
				"reserved_capacity": spec.NumberOfContainers,
			},
		}

		opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
		result := repo.vessels.FindOneAndUpdate(sc, filter, update, opts)
		if result.Err() != nil {
			session.AbortTransaction(sc)
			if errors.Is(result.Err(), mongo.ErrNoDocuments) {
				return fmt.Errorf("no vessel available for spec: %v", spec)
			}
			return result.Err()
		}

		if err := result.Decode(&vessel); err != nil {
			session.AbortTransaction(sc)
			return err
		}

		_, insertErr := repo.capacityOps.InsertOne(sc, CapacityOperation{
			ReservationID: spec.ReservationID,
			Operation:     opReserve,
			VesselID:      vessel.ID,
			CreatedAt:     time.Now(),
		})
		if insertErr != nil {
			session.AbortTransaction(sc)
			if mongo.IsDuplicateKeyError(insertErr) {
				// A concurrent or retried call already processed this — return the original vessel
				var op CapacityOperation
				if err := repo.capacityOps.FindOne(ctx, bson.M{
					"reservation_id": spec.ReservationID,
					"operation":      opReserve,
				}).Decode(&op); err != nil {
					return fmt.Errorf("failed to fetch existing operation: %w", err)
				}
				return repo.vessels.FindOne(ctx, bson.M{"id": op.VesselID}).Decode(&vessel)
			}
			return fmt.Errorf("failed to record capacity operation: %w", insertErr)
		}

		return session.CommitTransaction(sc)
	})

	if err != nil {
		return nil, err
	}
	return &vessel, nil
}

// ReleaseCapacity releases previously reserved capacity back to the vessel.
func (repo *MongoRepository) ReleaseCapacity(ctx context.Context, req *CapacityRequest) error {
	session, err := repo.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	return mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return err
		}

		_, insertErr := repo.capacityOps.InsertOne(sc, CapacityOperation{
			ReservationID: req.ReservationID,
			Operation:     opRelease,
			VesselID:      req.VesselID,
			CreatedAt:     time.Now(),
		})
		if insertErr != nil {
			session.AbortTransaction(sc)
			if mongo.IsDuplicateKeyError(insertErr) {
				return nil
			}
			return fmt.Errorf("failed to record capacity operation: %w", insertErr)
		}

		filter := bson.M{"id": req.VesselID}
		update := bson.M{
			"$inc": bson.M{
				"reserved_weight":   -req.Weight,
				"reserved_capacity": -req.NumberOfContainers,
			},
		}

		result, err := repo.vessels.UpdateOne(sc, filter, update)
		if err != nil {
			session.AbortTransaction(sc)
			return fmt.Errorf("failed to release capacity: %w", err)
		}
		if result.MatchedCount == 0 {
			session.AbortTransaction(sc)
			return fmt.Errorf("vessel %s not found", req.VesselID)
		}

		return session.CommitTransaction(sc)
	})
}

// ConfirmCapacity moves reserved capacity to used, confirming a consignment.
func (repo *MongoRepository) ConfirmCapacity(ctx context.Context, req *CapacityRequest) error {
	session, err := repo.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	return mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := session.StartTransaction(); err != nil {
			return err
		}

		_, insertErr := repo.capacityOps.InsertOne(sc, CapacityOperation{
			ReservationID: req.ReservationID,
			Operation:     opConfirm,
			VesselID:      req.VesselID,
			CreatedAt:     time.Now(),
		})
		if insertErr != nil {
			session.AbortTransaction(sc)
			if mongo.IsDuplicateKeyError(insertErr) {
				return nil
			}
			return fmt.Errorf("failed to record capacity operation: %w", insertErr)
		}

		filter := bson.M{"id": req.VesselID}
		update := bson.M{
			"$inc": bson.M{
				"reserved_weight":   -req.Weight,
				"reserved_capacity": -req.NumberOfContainers,
				"used_weight":       req.Weight,
				"used_capacity":     req.NumberOfContainers,
			},
		}

		result, err := repo.vessels.UpdateOne(sc, filter, update)
		if err != nil {
			session.AbortTransaction(sc)
			return fmt.Errorf("failed to confirm capacity: %w", err)
		}
		if result.MatchedCount == 0 {
			session.AbortTransaction(sc)
			return fmt.Errorf("vessel %s not found", req.VesselID)
		}

		return session.CommitTransaction(sc)
	})
}

func (repo *MongoRepository) Create(ctx context.Context, vessel *Vessel) error {
	_, err := repo.vessels.InsertOne(ctx, vessel)
	return err
}

func UnmarshalVessel(vessel *Vessel) *pb.Vessel {
	return &pb.Vessel{
		Id:               vessel.ID,
		Capacity:         int32(vessel.Capacity),
		MaxWeight:        int32(vessel.MaxWeight),
		ReservedWeight:   int32(vessel.ReservedWeight),
		UsedWeight:       int32(vessel.UsedWeight),
		ReservedCapacity: int32(vessel.ReservedCapacity),
		UsedCapacity:     int32(vessel.UsedCapacity),
		Name:             vessel.Name,
		Available:        vessel.Available,
	}
}

func MarshalVessel(vessel *pb.Vessel) *Vessel {
	return &Vessel{
		ID:               vessel.Id,
		Capacity:         int(vessel.Capacity),
		MaxWeight:        int(vessel.MaxWeight),
		ReservedWeight:   int(vessel.ReservedWeight),
		UsedWeight:       int(vessel.UsedWeight),
		ReservedCapacity: int(vessel.ReservedCapacity),
		UsedCapacity:     int(vessel.UsedCapacity),
		Name:             vessel.Name,
		Available:        vessel.Available,
	}
}

func MarshalSpecification(spec *pb.Specification) *Specification {
	return &Specification{
		ReservationID:      spec.ReservationId,
		Weight:             int(spec.Weight),
		NumberOfContainers: int(spec.NumberOfContainers),
	}
}

func MarshalCapacityRequest(req *pb.CapacityRequest) *CapacityRequest {
	return &CapacityRequest{
		ReservationID:      req.ReservationId,
		VesselID:           req.VesselId,
		Weight:             int(req.Weight),
		NumberOfContainers: int(req.NumberOfContainers),
	}
}
