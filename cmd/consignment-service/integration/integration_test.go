package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/pkg/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestHandleConfirmationEvent_HappyPath(t *testing.T) {
	s.cleanState(t)
	s.paymentSvc.reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consignmentID := "test-consignment-id"
	col := s.mongoCli.Database("shippy_test").Collection("consignments")
	_, err := col.InsertOne(ctx, bson.M{
		"_id":            consignmentID,
		"vessel_id":      "vessel-id",
		"reservation_id": "reservation-id",
		"weight":         100,
		"containers":     bson.A{},
		"status":         storage.StatusPending,
	})
	require.NoError(t, err)

	event := manager.ConfirmationEvent{
		PaymentAuthID: "auth-id",
		ConsignmentID: consignmentID,
		ReservationID: "reservation-id",
		VesselID:      "vessel-id",
		Weight:        100,
		Containers:    1,
	}
	s.publish(t, manager.ConsignmentPaymentAuthorisedTopic, consignmentID, event)

	capacityEventReceived := make(chan struct{})
	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: s.kafkaAddr,
		GroupID:          "test-capacity-consumer",
		OffsetReset:      "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.StartConsuming(ctx, kafka.EventHandlers{
			manager.ConfirmCapacityTopic: func(ctx context.Context, key, value []byte) error {
				close(capacityEventReceived)
				return nil
			},
		})
	}()

	mgr := s.newManager(t, []string{manager.ConsignmentPaymentAuthorisedTopic, manager.ConsignmentConfirmedTopic})
	mgr.Start(ctx, &wg)

	select {
	case <-capacityEventReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for confirm capacity event")
	}

	assert.Equal(t, 1, s.paymentSvc.captureCalls)

	assert.Eventually(t, func() bool {
		var result bson.M
		err := col.FindOne(ctx, bson.M{"_id": consignmentID}).Decode(&result)
		if err != nil {
			return false
		}
		return result["status"] == string(storage.StatusConfirmed)
	}, 15*time.Second, 500*time.Millisecond, "consignment status should be confirmed")

	cancel()
	wg.Wait()
}

func TestHandleFailedConfirmationEvent_RefundAndCancel(t *testing.T) {
	s.cleanState(t)
	s.paymentSvc.reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consignmentID := "test-consignment-dlq"
	col := s.mongoCli.Database("shippy_test").Collection("consignments")
	_, err := col.InsertOne(ctx, bson.M{
		"_id":            consignmentID,
		"vessel_id":      "vessel-id",
		"reservation_id": "reservation-id",
		"weight":         100,
		"containers":     bson.A{},
		"status":         storage.StatusConfirmed,
	})
	require.NoError(t, err)

	event := manager.ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       "test-payment-id",
		ConsignmentID:   consignmentID,
		ReservationID:   "reservation-id",
		VesselID:        "vessel-id",
		Weight:          100,
		Containers:      1,
	}
	s.publish(t, manager.ConsignmentConfirmationFailedTopic, consignmentID, event)

	releaseEventReceived := make(chan struct{})
	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: s.kafkaAddr,
		GroupID:          "test-release-consumer",
		OffsetReset:      "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	var wg sync.WaitGroup

	// Consume release event triggered by "consignment.confirmation.failed"
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.StartConsuming(ctx, kafka.EventHandlers{
			manager.ReleaseCapacityTopic: func(ctx context.Context, key, value []byte) error {
				close(releaseEventReceived)
				return nil
			},
		})
	}()

	mgr := s.newManager(t, []string{manager.ConsignmentConfirmationFailedTopic, manager.ConsignmentCancelledTopic})
	mgr.Start(ctx, &wg)

	select {
	case <-releaseEventReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for release capacity event")
	}

	assert.Equal(t, 1, s.paymentSvc.refundCalls)

	assert.Eventually(t, func() bool {
		var result bson.M
		err := col.FindOne(ctx, bson.M{"_id": consignmentID}).Decode(&result)
		if err != nil {
			return false
		}
		return result["status"] == string(storage.StatusCancelled)
	}, 15*time.Second, 500*time.Millisecond, "consignment status should be cancelled")

	cancel()
	wg.Wait()
}
