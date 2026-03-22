package main

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/murraystewart96/shippy/shippy-vessel-service/proto/vessel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type repository interface {
	FindAvailable(ctx context.Context, spec *Specification) (*Vessel, error)
	Create(ctx context.Context, vessel *Vessel) error
}

type Specification struct {
	Capacity  int `json:"capacity"`
	MaxWeight int `json:"max_weight"`
}

type Vessel struct {
	ID        string `json:"id" bson:"id"`
	Capacity  int    `json:"capacity" bson:"capacity"`
	MaxWeight int    `json:"max_weight" bson:"max_weight"`
	Name      string `json:"name" bson:"name"`
	Available bool   `json:"available" bson:"available"`
}

type MongoRespository struct {
	collection *mongo.Collection
}

// FindAvailable - checks a specification against a map of vessels,
// if capacity and max weight are below a vessels capacity and max weight,
// then return that vessel.
func (repo *MongoRespository) FindAvailable(ctx context.Context, spec *Specification) (*Vessel, error) {
	filter := bson.M{
		"capacity":   bson.M{"$gte": spec.Capacity},
		"max_weight": bson.M{"$gte": spec.MaxWeight},
	}

	res := repo.collection.FindOne(ctx, filter)
	if res.Err() != nil {
		if errors.Is(res.Err(), mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("no vessel available for spec: %v", spec)
		}
		return nil, res.Err()
	}

	var vessel Vessel
	if err := res.Decode(&vessel); err != nil {
		return nil, err
	}
	return &vessel, nil
}

func (repo *MongoRespository) Create(ctx context.Context, vessel *Vessel) error {
	_, err := repo.collection.InsertOne(ctx, vessel)
	return err
}

//

func UnmarshalVessel(vessel *Vessel) *pb.Vessel {
	return &pb.Vessel{
		Id:        vessel.ID,
		Capacity:  int32(vessel.Capacity),
		MaxWeight: int32(vessel.MaxWeight),
		Name:      vessel.Name,
		Available: vessel.Available,
	}
}

func MarshalVessel(vessel *pb.Vessel) *Vessel {
	return &Vessel{
		ID:        vessel.Id,
		Capacity:  int(vessel.Capacity),
		MaxWeight: int(vessel.MaxWeight),
		Name:      vessel.Name,
		Available: vessel.Available,
	}
}

func MarshalSpecification(spec *pb.Specification) *Specification {
	return &Specification{
		Capacity:  int(spec.Capacity),
		MaxWeight: int(spec.MaxWeight),
	}
}

func UnmarshalSpecification(spec *Specification) *pb.Specification {
	return &pb.Specification{
		Capacity:  int32(spec.Capacity),
		MaxWeight: int32(spec.MaxWeight),
	}
}
