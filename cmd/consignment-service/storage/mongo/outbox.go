package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type OutboxMongo struct {
	collection *mongo.Collection
}

func NewOutbox(collection *mongo.Collection) *OutboxMongo {
	return &OutboxMongo{collection: collection}
}

type outboxDocument struct {
	Id              string            `bson:"_id"`
	Topic           string            `bson:"topic"`
	Key             string            `bson:"key"`
	Payload         []byte            `bson:"payload"`
	CreatedAt       time.Time         `bson:"created_at"`
	PublishedAt     *time.Time        `bson:"published_at"`
	ProcessingUntil *time.Time        `bson:"processing_until"`
	ClaimID         string            `bson:"claim_id"`
	TraceContext    map[string]string `bson:"trace_context"`
}

func (o *OutboxMongo) CreateEvent(ctx context.Context, event *storage.OutboxEvent) error {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	doc := &outboxDocument{
		Id:           uuid.New().String(),
		Topic:        event.Topic,
		Key:          event.Key,
		Payload:      event.Payload,
		CreatedAt:    time.Now(),
		TraceContext: carrier,
	}

	_, err := o.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Another instance already scheduled this event
			return nil
		}
		return fmt.Errorf("failed to create outbox event: %w", err)
	}
	return nil
}

func (o *OutboxMongo) MarkPublished(ctx context.Context, id uuid.UUID) error {
	now := time.Now()
	result, err := o.collection.UpdateOne(ctx,
		bson.M{"_id": id.String()},
		bson.M{"$set": bson.M{"published_at": now}},
	)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as published: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("outbox event %s not found", id)
	}
	return nil
}

func (o *OutboxMongo) GetPendingEvents(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error) {
	claimID := uuid.New().String()
	now := time.Now()
	processingUntil := now.Add(lease)

	// Atomically claim all unclaimed/expired pending events with our claimID
	_, err := o.collection.UpdateMany(ctx,
		bson.M{
			"published_at": nil,
			"$or": bson.A{
				bson.M{"processing_until": nil},
				bson.M{"processing_until": bson.M{"$lt": now}},
			},
		},
		bson.M{"$set": bson.M{
			"claim_id":         claimID,
			"processing_until": processingUntil,
		}},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox events: %w", err)
	}

	// Fetch the events we just claimed
	cursor, err := o.collection.Find(ctx,
		bson.M{"claim_id": claimID, "published_at": nil},
		options.Find().SetSort(bson.M{"created_at": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch claimed outbox events: %w", err)
	}
	defer cursor.Close(ctx)

	var events []*storage.OutboxEvent
	for cursor.Next(ctx) {
		var doc outboxDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode outbox event: %w", err)
		}
		id, err := uuid.Parse(doc.Id)
		if err != nil {
			return nil, fmt.Errorf("failed to parse outbox event id: %w", err)
		}
		events = append(events, &storage.OutboxEvent{
			Id:              id,
			Topic:           doc.Topic,
			Key:             doc.Key,
			Payload:         doc.Payload,
			CreatedAt:       doc.CreatedAt,
			PublishedAt:     doc.PublishedAt,
			ProcessingUntil: doc.ProcessingUntil,
			ClaimID:         doc.ClaimID,
			TraceContext:    doc.TraceContext,
		})
	}

	return events, nil
}
