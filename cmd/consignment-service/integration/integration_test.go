package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/pkg/kafka"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestConsignmentCreateAndConfirmation(t *testing.T) {
	s.cleanState(t)
	s.paymentSvc.reset()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	col := s.mongoCli.Database("shippy_test").Collection("consignments")

	consignmentCli := s.newConsignmentClient(t)

	mgr := s.newManager(t, []string{
		manager.ConsignmentPaymentAuthorisedTopic,
		manager.ConsignmentConfirmationFailedTopic,
		manager.ReservationExpiredTopic,
	})

	var wg sync.WaitGroup

	// TODO: handle err
	mgr.Start(ctx, &wg)

	consignment := &pb.Consignment{
		Description: "This is a test consignment",
		Weight:      550,
		Containers: []*pb.Container{
			{
				CustomerId: "cust001",
				UserId:     "user001",
				Origin:     "Manchester, United Kingdom",
			},
		},
	}

	createRes, err := consignmentCli.CreateConsignment(ctx, consignment)
	require.NoError(t, err)

	confirmRes, err := consignmentCli.ConfirmConsignment(ctx, &pb.ConfirmRequest{Id: createRes.Consignment.Id})
	require.NoError(t, err)
	assert.Equal(t, true, confirmRes.Confirmed)

	paymentCapturedEvent := make(chan struct{})
	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: s.kafkaAddr,
		GroupID:          "test-capacity-consumer",
		OffsetReset:      "earliest",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.StartConsuming(ctx, kafka.EventHandlers{
			manager.PaymentCapturedTopic: func(ctx context.Context, key, value []byte) error {
				close(paymentCapturedEvent)
				return nil
			},
		})
	}()

	// Payment Captured event was sent and received
	select {
	case <-paymentCapturedEvent:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for payment captured event")
	}

	// Consignment status should update to confirm
	assert.Eventually(t, func() bool {
		var result bson.M
		err := col.FindOne(ctx, bson.M{"_id": createRes.Consignment.Id}).Decode(&result)
		if err != nil {
			return false
		}

		return result["status"] == string(storage.StatusConfirmed)
	}, 15*time.Second, 500*time.Millisecond, "consignment status should be confirmed")
}

func TestHandleConfirmationEvent_HappyPath(t *testing.T) {
	s.cleanState(t)
	s.paymentSvc.reset()

	ctx, cancel := context.WithCancel(t.Context())
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

	paymentCapturedEvent := make(chan struct{})
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
			manager.PaymentCapturedTopic: func(ctx context.Context, key, value []byte) error {
				close(paymentCapturedEvent)
				return nil
			},
		})
	}()

	mgr := s.newManager(t, []string{manager.ConsignmentPaymentAuthorisedTopic})

	// TODO: handle err
	mgr.Start(ctx, &wg)

	select {
	case <-paymentCapturedEvent:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for payment captured event")
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

	var wg sync.WaitGroup

	mgr := s.newManager(t, []string{manager.ConsignmentConfirmationFailedTopic})
	mgr.Start(ctx, &wg)

	assert.Eventually(t, func() bool {
		var result bson.M
		err := col.FindOne(ctx, bson.M{"_id": consignmentID}).Decode(&result)
		if err != nil {
			return false
		}
		return result["status"] == string(storage.StatusCancelled) && s.paymentSvc.refundCalls == 1
	}, 15*time.Second, 500*time.Millisecond, "consignment status should be cancelled")

	cancel()
	wg.Wait()
}
