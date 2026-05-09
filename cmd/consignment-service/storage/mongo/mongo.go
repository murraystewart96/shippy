package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/murraystewart96/shippy/consignment-service/storage"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	collection *mongo.Collection
}

func New(collection *mongo.Collection) *Mongo {
	return &Mongo{collection: collection}
}

func CreateClient(ctx context.Context, uri string, retry int32) (*mongo.Client, error) {
	conn, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx, nil); err != nil {
		if retry >= 3 {
			return nil, err
		}
		retry++
		time.Sleep(time.Second * 2)
		return CreateClient(ctx, uri, retry)
	}

	return conn, err
}

func (repo *Mongo) Create(ctx context.Context, consignment *storage.Consignment) error {
	consignment.Status = storage.StatusPending
	log.Info().Msgf("Creating consignment: %v", *consignment)
	_, err := repo.collection.InsertOne(ctx, consignment)
	return err
}

func (repo *Mongo) UpdateStatus(ctx context.Context, id string, status string) error {
	result, err := repo.collection.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": status}},
	)
	if err != nil {
		return fmt.Errorf("failed to update consignment status: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("consignment %s not found", id)
	}
	return nil
}

func (repo *Mongo) GetByID(ctx context.Context, id string) (*storage.Consignment, error) {
	var consignment storage.Consignment
	err := repo.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&consignment)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("consignment %s not found", id)
		}
		return nil, fmt.Errorf("failed to get consignment: %w", err)
	}
	return &consignment, nil
}

func (repo *Mongo) GetAll(ctx context.Context) ([]*storage.Consignment, error) {
	cur, err := repo.collection.Find(ctx, bson.M{}, nil)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []*storage.Consignment{}, nil
		}
		return nil, err
	}

	var consignments []*storage.Consignment
	for cur.Next(ctx) {
		var consignment *storage.Consignment
		if err := cur.Decode(&consignment); err != nil {
			return nil, err
		}
		consignments = append(consignments, consignment)
	}

	return consignments, nil
}

// HELPERS

func MarshalConsignment(consignment *pb.Consignment) *storage.Consignment {
	return &storage.Consignment{
		ID:          consignment.Id,
		Weight:      consignment.Weight,
		Description: consignment.Description,
		Containers:  marshalContainerCollection(consignment.Containers),
		VesselID:    consignment.VesselId,
		Status:      consignment.Status,
	}
}

func UnmarshalConsignment(consignment *storage.Consignment) *pb.Consignment {
	return &pb.Consignment{
		Id:          consignment.ID,
		Weight:      consignment.Weight,
		Description: consignment.Description,
		Containers:  unmarshalContainerCollection(consignment.Containers),
		VesselId:    consignment.VesselID,
		Status:      consignment.Status,
	}
}

func UnmarshalConsignmentCollection(consignments []*storage.Consignment) []*pb.Consignment {
	collection := make([]*pb.Consignment, len(consignments))
	for i, consignment := range consignments {
		collection[i] = UnmarshalConsignment(consignment)
	}
	return collection
}

func marshalContainerCollection(containers []*pb.Container) storage.Containers {
	collection := make(storage.Containers, len(containers))
	for i, container := range containers {
		collection[i] = &storage.Container{
			ID:         container.Id,
			CustomerID: container.CustomerId,
			UserID:     container.UserId,
		}
	}
	return collection
}

func unmarshalContainerCollection(containers storage.Containers) []*pb.Container {
	collection := make([]*pb.Container, len(containers))
	for i, container := range containers {
		collection[i] = &pb.Container{
			Id:         container.ID,
			CustomerId: container.CustomerID,
			UserId:     container.UserID,
		}
	}
	return collection
}
