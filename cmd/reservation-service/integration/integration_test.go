package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/pkg/kafka"
	"github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/manager"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleConfirmationEvent_HappyPath(t *testing.T) {
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

	event := manager.CapacityEvent{
		Action:          manager.CONFIRM,
		ReservationInfo: reservation,
	}

	s.publish(t, manager.ConfirmCapacityTopic, reservation.Id.String(), event)

	var wg sync.WaitGroup

	mgr := s.newManager(t, []string{manager.ConfirmCapacityTopic})
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

func TestHandleFailedConfirmationEvent_RefundAndCancel(t *testing.T) {
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

	event := manager.CapacityEvent{
		Action:          manager.CONFIRM,
		ReservationInfo: reservation,
		ConsignmentID:   "test-consignment-id",
		PaymentID:       "test-payment-id",
	}

	s.publish(t, manager.ConfirmCapacityTopic, reservation.Id.String(), event)

	var receivedEvent manager.FailedConfirmationEvent
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
				_ = json.Unmarshal(value, &receivedEvent)
				close(consignmentConfirmationFailedReceived)
				return nil
			},
		})
	}()

	s.vesselSvc.confirmFunc = func(ctx context.Context, req *vessel.CapacityRequest) (*vessel.Empty, error) {
		return nil, fmt.Errorf("failed to confirm capacity")
	}

	mgr := s.newManager(t, []string{manager.ConfirmCapacityTopic, manager.CapacityDLQTopic})
	mgr.Start(ctx, &wg)

	// Refund and cancel are handled by the consignment service
	select {
	case <-consignmentConfirmationFailedReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for confirm consignment DLQ event")
	}

	assert.True(t, receivedEvent.PaymentCaptured)
	assert.True(t, receivedEvent.CacheCleared)
	assert.Equal(t, "test-payment-id", receivedEvent.PaymentID)
	assert.Equal(t, "test-consignment-id", receivedEvent.ConsignmentID)

	assert.Eventually(t, func() bool {
		s.vesselSvc.mu.Lock()
		calls := s.vesselSvc.confirmCapacityCalls
		s.vesselSvc.mu.Unlock()
		return calls == 4
	}, 15*time.Second, 500*time.Millisecond)

	// After Eventually, assert cache was cleared
	_, err = s.cache.GetData(ctx, reservation.Id.String())
	assert.Error(t, err, "reservation data should have been deleted")

	cancel()
	wg.Wait()
}
