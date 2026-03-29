package main

import (
	"context"
	"errors"

	"github.com/google/uuid"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Consignment struct {
	ID          string     `json:"id"`
	Weight      int32      `json:"weight"`
	Description string     `json:"description"`
	Containers  Containers `json:"containers"`
	VesselID    string     `json:"vessel_id"`
}

type Container struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	UserID     string `json:"user_id"`
}

type Containers []*Container

type repository interface {
	Create(ctx context.Context, consignment *Consignment) error
	GetAll(ctx context.Context) ([]*Consignment, error)
}

type MongoRespository struct {
	collection *mongo.Collection
}

func (repo *MongoRespository) Create(ctx context.Context, consignment *Consignment) error {
	consignment.ID = uuid.New().String()
	log.Info().Msgf("Creating consignment: %v", *consignment)
	_, err := repo.collection.InsertOne(ctx, consignment)
	return err
}

func (repo *MongoRespository) GetAll(ctx context.Context) ([]*Consignment, error) {
	cur, err := repo.collection.Find(ctx, bson.M{}, nil)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []*Consignment{}, nil
		}
		return nil, err
	}

	var consignments []*Consignment
	for cur.Next(ctx) {
		var consignment *Consignment
		if err := cur.Decode(&consignment); err != nil {
			return nil, err
		}

		consignments = append(consignments, consignment)
	}

	return consignments, nil
}

// HELPERS

func MarshalContainerCollection(containers []*pb.Container) []*Container {
	collection := make([]*Container, len(containers))
	for i, container := range containers {
		collection[i] = MarshalContainer(container)
	}

	return collection
}

func UnmarshalContainerCollection(containers []*Container) []*pb.Container {
	collection := make([]*pb.Container, 0)
	for _, container := range containers {
		collection = append(collection, UnmarshalContainer(container))
	}
	return collection
}

func UnmarshalConsignmentCollection(consignments []*Consignment) []*pb.Consignment {
	collection := make([]*pb.Consignment, 0)
	for _, consignment := range consignments {
		collection = append(collection, UnmarshalConsignment(consignment))
	}
	return collection
}

func UnmarshalContainer(container *Container) *pb.Container {
	return &pb.Container{
		Id:         container.ID,
		CustomerId: container.CustomerID,
		UserId:     container.UserID,
	}
}

func MarshalContainer(container *pb.Container) *Container {
	return &Container{
		ID:         container.Id,
		CustomerID: container.CustomerId,
		UserID:     container.UserId,
	}
}

// Marshal an input consignment type to a consignment model
func MarshalConsignment(consignment *pb.Consignment) *Consignment {
	containers := MarshalContainerCollection(consignment.Containers)
	return &Consignment{
		ID:          consignment.Id,
		Weight:      consignment.Weight,
		Description: consignment.Description,
		Containers:  containers,
		VesselID:    consignment.VesselId,
	}
}

func UnmarshalConsignment(consignment *Consignment) *pb.Consignment {
	return &pb.Consignment{
		Id:          consignment.ID,
		Weight:      consignment.Weight,
		Description: consignment.Description,
		Containers:  UnmarshalContainerCollection(consignment.Containers),
		VesselId:    consignment.VesselID,
	}
}
