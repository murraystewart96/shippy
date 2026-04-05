package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// Test case

// handleReleaseReservationEvent

// Happy flow
// we have a reservation in the cache to release
// create multiple events for the same entry to ensure idempotency

// Retry flows
// 1.) the interesting case is when you do delete the data entry
// then fail to release capacity
// when you try again you should skip the delete path

func TestHandleReleaseReservationEvent(t *testing.T) {
	reservationID1 := uuid.New()
	vesselID1 := uuid.New()

	numberOfContainers := 2
	weight := 100

	event := &ReleaseCapacityEvent{
		ReservationInfo: storage.ReservationInfo{
			Id:                 reservationID1,
			VesselID:           vesselID1,
			NumberOfContainers: numberOfContainers,
			Weight:             weight,
		},
	}

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}

	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return true, nil
	}

	vesselCli := &mockVesselClient{}

	vesselCli.releaseCapacity = func(ctx context.Context, in *vessel.CapacityRequest, opts ...grpc.CallOption) (*vessel.Empty, error) {
		vesselCli.releaseCalls++
		return nil, nil
	}

	topics := []string{releaseCapacityTopic}

	mgr, err := New(vesselCli, nil, nil, topics, cache, nil)
	assert.NoError(t, err)

	eventJSON, err := json.Marshal(event)
	assert.NoError(t, err)

	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
	assert.NoError(t, err)

	// Reservation is deleted from cache and capacity is released on vessel
	assert.Equal(t, cache.deleteDataCalls, 1)
	assert.Equal(t, vesselCli.releaseCalls, 1)
}

func TestHandleReleaseReservationEvent_DuplicateEvent(t *testing.T) {
	reservationID1 := uuid.New()
	vesselID1 := uuid.New()

	numberOfContainers := 2
	weight := 100

	event := &ReleaseCapacityEvent{
		CacheCleared: false,
		ReservationInfo: storage.ReservationInfo{
			Id:                 reservationID1,
			VesselID:           vesselID1,
			NumberOfContainers: numberOfContainers,
			Weight:             weight,
		},
	}

	// Cache returns false for data delete signalling that is the event has already been processed
	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}

	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return false, nil
	}

	vesselCli := &mockVesselClient{}

	topics := []string{releaseCapacityTopic}

	mgr, err := New(vesselCli, nil, nil, topics, cache, nil)
	assert.NoError(t, err)

	eventJSON, err := json.Marshal(event)
	assert.NoError(t, err)

	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
	assert.NoError(t, err)

	// Delete data was called but there was nothing to delete
	// handler quit processing event and returned nil
	assert.Equal(t, cache.deleteDataCalls, 1)
	assert.Equal(t, vesselCli.releaseCalls, 0)
}

func TestHandleReleaseReservationEvent_RetryAfterClearingCache(t *testing.T) {
	reservationID1 := uuid.New()
	vesselID1 := uuid.New()
	eventID1 := "e1"

	numberOfContainers := 2
	weight := 100

	event := &ReleaseCapacityEvent{
		ReservationInfo: storage.ReservationInfo{
			Id:                 reservationID1,
			VesselID:           vesselID1,
			NumberOfContainers: numberOfContainers,
			Weight:             weight,
		},
	}

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}

	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return true, nil
	}

	outbox := &mockOutbox{
		data: make(map[string]*storage.OutboxEvent),
	}

	outbox.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		event.PublishedAt = nil
		outbox.data[eventID1] = event
		return nil
	}

	outbox.getPendingEvents = func(ctx context.Context) ([]*storage.OutboxEvent, error) {
		pendingEvents := make([]*storage.OutboxEvent, 0)
		for _, event := range outbox.data {
			if event.PublishedAt == nil {
				pendingEvents = append(pendingEvents, event)
			}
		}

		return pendingEvents, nil
	}

	vesselCli := &mockVesselClient{}

	vesselCli.releaseCapacity = func(ctx context.Context, in *vessel.CapacityRequest, opts ...grpc.CallOption) (*vessel.Empty, error) {
		vesselCli.releaseCalls++

		if vesselCli.releaseCalls == 1 {
			return nil, fmt.Errorf("Release Capacity failed")
		}

		return nil, nil
	}

	topics := []string{releaseCapacityTopic}

	mgr, err := New(vesselCli, nil, nil, topics, cache, outbox)
	assert.NoError(t, err)

	eventJSON, err := json.Marshal(event)
	assert.NoError(t, err)

	// Handle first attempt at event - should fail at vessel release call and be rescheduled in outbox
	// also reservation data should be deleted
	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vessel ReleaseCapacity failed")
	assert.Equal(t, cache.deleteDataCalls, 1)

	// Simulate outbox publish
	events, err := outbox.GetPendingEvents(t.Context())
	assert.NoError(t, err)

	// Retry should skip reservation data delete check and complete success flow
	retryEvent := events[0]
	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(retryEvent.Key), retryEvent.Payload)
	assert.NoError(t, err)
	assert.Equal(t, cache.deleteDataCalls, 1)
	assert.Equal(t, vesselCli.releaseCalls, 2)
}

func TestHandleReleaseReservationEvent_RetryAfterNotClearingCache(t *testing.T) {
	reservationID1 := uuid.New()
	vesselID1 := uuid.New()
	eventID1 := "e1"

	numberOfContainers := 2
	weight := 100

	event := &ReleaseCapacityEvent{
		ReservationInfo: storage.ReservationInfo{
			Id:                 reservationID1,
			VesselID:           vesselID1,
			NumberOfContainers: numberOfContainers,
			Weight:             weight,
		},
	}

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}

	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		if cache.deleteDataCalls == 1 {
			return false, fmt.Errorf("failed to delete reservation")
		}
		return true, nil
	}

	outbox := &mockOutbox{
		data: make(map[string]*storage.OutboxEvent),
	}

	outbox.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		event.PublishedAt = nil
		outbox.data[eventID1] = event
		return nil
	}

	outbox.getPendingEvents = func(ctx context.Context) ([]*storage.OutboxEvent, error) {
		pendingEvents := make([]*storage.OutboxEvent, 0)
		for _, event := range outbox.data {
			if event.PublishedAt == nil {
				pendingEvents = append(pendingEvents, event)
			}
		}

		return pendingEvents, nil
	}

	vesselCli := &mockVesselClient{}

	vesselCli.releaseCapacity = func(ctx context.Context, in *vessel.CapacityRequest, opts ...grpc.CallOption) (*vessel.Empty, error) {
		vesselCli.releaseCalls++
		return nil, nil
	}

	topics := []string{releaseCapacityTopic}

	mgr, err := New(vesselCli, nil, nil, topics, cache, outbox)
	assert.NoError(t, err)

	eventJSON, err := json.Marshal(event)
	assert.NoError(t, err)

	// Handle first attempt at event - should fail at vessel release call and be rescheduled in outbox
	// also reservation data should be deleted
	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete reservation")
	assert.Equal(t, cache.deleteDataCalls, 1)

	// Simulate outbox publish
	events, err := outbox.GetPendingEvents(t.Context())
	assert.NoError(t, err)

	// Retry should successfully delete reservation from cache and complete success flow
	retryEvent := events[0]
	err = mgr.handleReleaseReservationEvent(t.Context(), []byte(retryEvent.Key), retryEvent.Payload)
	assert.NoError(t, err)
	assert.Equal(t, cache.deleteDataCalls, 2)
	assert.Equal(t, vesselCli.releaseCalls, 1)
}

func TestHandleReleaseReservationEvent_IsIdempotent(t *testing.T) {
	reservationID1 := uuid.New()
	vesselID1 := uuid.New()
	eventID1 := "e1"

	numberOfContainers := 2
	weight := 100

	event := &ReleaseCapacityEvent{
		ReservationInfo: storage.ReservationInfo{
			Id:                 reservationID1,
			VesselID:           vesselID1,
			NumberOfContainers: numberOfContainers,
			Weight:             weight,
		},
	}

	cache := &mockCache{

		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}

	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.mu.Lock()
		defer cache.mu.Unlock()

		cache.deleteDataCalls++
		if cache.deleteDataCalls == 1 {
			return true, nil
		}
		return false, nil
	}

	outbox := &mockOutbox{
		data: make(map[string]*storage.OutboxEvent),
	}

	outbox.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		event.PublishedAt = nil
		outbox.data[eventID1] = event
		return nil
	}

	outbox.getPendingEvents = func(ctx context.Context) ([]*storage.OutboxEvent, error) {
		pendingEvents := make([]*storage.OutboxEvent, 0)
		for _, event := range outbox.data {
			if event.PublishedAt == nil {
				pendingEvents = append(pendingEvents, event)
			}
		}

		return pendingEvents, nil
	}

	vesselCli := &mockVesselClient{}

	vesselCli.releaseCapacity = func(ctx context.Context, in *vessel.CapacityRequest, opts ...grpc.CallOption) (*vessel.Empty, error) {
		vesselCli.releaseCalls++
		return nil, nil
	}

	topics := []string{releaseCapacityTopic}

	mgr, err := New(vesselCli, nil, nil, topics, cache, outbox)
	assert.NoError(t, err)

	eventJSON, err := json.Marshal(event)
	assert.NoError(t, err)

	// Fire of 3 identical events
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := mgr.handleReleaseReservationEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// All 3 events attempted to delete the reservation data
	// Only 1 succeeded and continued to process the event
	assert.Equal(t, cache.deleteDataCalls, 3)
	assert.Equal(t, vesselCli.releaseCalls, 1)
}
