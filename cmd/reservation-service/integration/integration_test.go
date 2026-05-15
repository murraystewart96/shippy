package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/pkg/kafka"
	eventspb "github.com/murraystewart96/shippy/proto/events"
	"github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/manager"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestHandlePaymentCapturedEvent_HappyPath(t *testing.T) {
	s.cleanState(t)
	s.vesselSvc.reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Seed redis with reservation
	reservation := storage.ReservationInfo{
		Id:                 uuid.New(),
		VesselID:           uuid.New(),
		NumberOfContainers: 10,
		Weight:             500,
	}

	err := s.cache.Store(ctx, reservation.Id.String(), reservation)
	require.NoError(t, err)

	event := &eventspb.PaymentCapturedEvent{
		ReservationInfo: &eventspb.ReservationInfo{
			Id:                 reservation.Id.String(),
			VesselId:           reservation.VesselID.String(),
			NumberOfContainers: int32(reservation.NumberOfContainers),
			Weight:             int32(reservation.Weight),
		},
	}

	s.publish(t, manager.PaymentCapturedTopic, reservation.Id.String(), event)

	var wg sync.WaitGroup

	mgr := s.newManager(t, []string{manager.PaymentCapturedTopic})
	mgr.Start(ctx, &wg)

	assert.Eventually(t, func() bool {
		s.vesselSvc.mu.Lock()
		calls := s.vesselSvc.confirmCapacityCalls
		s.vesselSvc.mu.Unlock()
		return calls == 1
	}, 15*time.Second, 500*time.Millisecond)

	// After Eventually, assert cache was cleared
	_, err = s.cache.GetData(ctx, reservation.Id.String())
	assert.Error(t, err, "reservation data should have been deleted")

	cancel()
	wg.Wait()
}

func TestHandleFailedPaymentCapturedEvent_RefundAndCancel(t *testing.T) {
	s.cleanState(t)
	s.vesselSvc.reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Seed redis with reservation
	reservation := storage.ReservationInfo{
		Id:                 uuid.New(),
		VesselID:           uuid.New(),
		NumberOfContainers: 10,
		Weight:             500,
	}

	err := s.cache.Store(ctx, reservation.Id.String(), reservation)
	require.NoError(t, err)

	event := &eventspb.PaymentCapturedEvent{
		ReservationInfo: &eventspb.ReservationInfo{
			Id:                 reservation.Id.String(),
			VesselId:           reservation.VesselID.String(),
			NumberOfContainers: int32(reservation.NumberOfContainers),
			Weight:             int32(reservation.Weight),
		},
		ConsignmentId: "test-consignment-id",
		PaymentId:     "test-payment-id",
	}

	s.publish(t, manager.PaymentCapturedTopic, reservation.Id.String(), event)

	var receivedEvent eventspb.ConsignmentConfirmationFailedEvent
	consignmentConfirmationFailedReceived := make(chan struct{})

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
			manager.ConsignmentConfirmationFailedTopic: func(ctx context.Context, key, value []byte) error {
				_ = proto.Unmarshal(value, &receivedEvent)
				close(consignmentConfirmationFailedReceived)
				return nil
			},
		})
	}()

	s.vesselSvc.confirmFunc = func(ctx context.Context, req *vessel.CapacityRequest) (*vessel.Empty, error) {
		return nil, fmt.Errorf("failed to confirm capacity")
	}

	mgr := s.newManager(t, []string{
		manager.PaymentCapturedTopic,
		manager.ConfirmCapacityTopic,
		manager.CapacityFailedTopic,
	})
	mgr.Start(ctx, &wg)

	// Consignment confirmation failure event published
	select {
	case <-consignmentConfirmationFailedReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for confirm consignment DLQ event")
	}

	assert.True(t, receivedEvent.PaymentCaptured)
	assert.True(t, receivedEvent.CacheCleared)
	assert.Equal(t, "test-payment-id", receivedEvent.PaymentId)
	assert.Equal(t, "test-consignment-id", receivedEvent.ConsignmentId)

	assert.Eventually(t, func() bool {
		s.vesselSvc.mu.Lock()
		confirmCalls := s.vesselSvc.confirmCapacityCalls
		releaseCalls := s.vesselSvc.releaseCapacityCalls
		s.vesselSvc.mu.Unlock()
		return confirmCalls == 3*(manager.MaxRetries+1) && releaseCalls == 1
	}, 15*time.Second, 500*time.Millisecond)

	// After Eventually, assert cache was cleared
	_, err = s.cache.GetData(ctx, reservation.Id.String())
	assert.Error(t, err, "reservation data should have been deleted")

	cancel()
	wg.Wait()
}
