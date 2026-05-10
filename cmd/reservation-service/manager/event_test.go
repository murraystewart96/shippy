package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// *** HELPERS ***

func newTestEvent(action EventAction) *CapacityEvent {
	return &CapacityEvent{
		Action: action,
		ReservationInfo: storage.ReservationInfo{
			Id:                 uuid.New(),
			VesselID:           uuid.New(),
			NumberOfContainers: 2,
			Weight:             100,
		},
	}
}

func newOutboxWithStore() *mockOutbox {
	o := &mockOutbox{data: make(map[string]*storage.OutboxEvent)}
	o.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.data[event.Key] = event
		return nil
	}
	o.getPendingEvents = func(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		pending := make([]*storage.OutboxEvent, 0)
		for _, e := range o.data {
			if e.PublishedAt == nil {
				pending = append(pending, e)
			}
		}
		return pending, nil
	}
	o.markPublished = func(ctx context.Context, id uuid.UUID) error {
		return nil
	}
	return o
}

func mustMarshalEvent(t *testing.T, event *CapacityEvent) []byte {
	t.Helper()
	b, err := json.Marshal(event)
	require.NoError(t, err)
	return b
}

func newManager(t *testing.T, vesselCli *mockVesselClient, cache *mockCache, outbox *mockOutbox, producer kafka.IProducer) *Manager {
	t.Helper()
	mgr, err := New(vesselCli, producer, nil, []string{ReleaseCapacityTopic}, cache, outbox, config.Manager{})
	require.NoError(t, err)
	return mgr
}

// *** TESTS ***

func TestHandleReleaseReservationEvent_HappyPath(t *testing.T) {
	event := newTestEvent(RELEASE)

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
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, nil, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_DuplicateEvent(t *testing.T) {
	event := newTestEvent(RELEASE)

	cache := &mockCache{}

	// A duplicate event would return false because cache entry has already been deleted
	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return false, nil
	}

	vesselCli := &mockVesselClient{}
	mgr := newManager(t, vesselCli, cache, nil, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)
	assert.Equal(t, 0, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_RetryAfterClearingCache(t *testing.T) {
	event := newTestEvent(RELEASE)

	outbox := newOutboxWithStore()

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
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		if vesselCli.releaseCalls == 1 {
			return nil, fmt.Errorf("release capacity failed")
		}
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, outbox, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	require.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.releaseCalls)

	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	retryEvent := pending[0]
	err = mgr.handleCapacityEvent(t.Context(), []byte(retryEvent.Key), retryEvent.Payload)
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)
	assert.Equal(t, 2, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_RetryAfterNotClearingCache(t *testing.T) {
	event := newTestEvent(RELEASE)

	outbox := newOutboxWithStore()

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

	vesselCli := &mockVesselClient{}
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, outbox, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)

	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	retryEvent := pending[0]
	err = mgr.handleCapacityEvent(t.Context(), []byte(retryEvent.Key), retryEvent.Payload)
	assert.NoError(t, err)
	assert.Equal(t, 2, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_IsIdempotent(t *testing.T) {
	event := newTestEvent(RELEASE)

	outbox := newOutboxWithStore()

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

	vesselCli := &mockVesselClient{}
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		vesselCli.mu.Lock()
		defer vesselCli.mu.Unlock()
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, outbox, nil)
	eventJSON := mustMarshalEvent(t, event)

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), eventJSON)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Duplicate events should only be processed once
	// This means one release call to the vessel service
	assert.Equal(t, 3, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_InvalidJSON(t *testing.T) {
	vesselCli := &mockVesselClient{}
	cache := &mockCache{}
	mgr := newManager(t, vesselCli, cache, nil, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte("key"), []byte("not valid json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal event")
	assert.Equal(t, 0, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_CacheClearedSkipsDelete(t *testing.T) {
	event := newTestEvent(RELEASE)

	event.CacheCleared = true

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
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, nil, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 0, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_DeleteIDFailureIsNonFatal(t *testing.T) {
	event := newTestEvent(RELEASE)

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return false, fmt.Errorf("redis unavailable")
		},
	}
	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return true, nil
	}

	vesselCli := &mockVesselClient{}
	vesselCli.releaseCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, nil
	}

	mgr := newManager(t, vesselCli, cache, nil, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_ScheduleRetryFailsAfterDeleteError(t *testing.T) {
	event := newTestEvent(RELEASE)

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}
	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return false, fmt.Errorf("redis unavailable")
	}

	outbox := &mockOutbox{
		createEvent: func(ctx context.Context, event *storage.OutboxEvent) error {
			return fmt.Errorf("postgres unavailable")
		},
	}

	mgr := newManager(t, nil, cache, outbox, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to schedule capacity retry")
}

func TestHandleConfirmReservationEvent(t *testing.T) {
	event := newTestEvent(CONFIRM)

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}
	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return true, nil
	}
	outbox := newOutboxWithStore()

	vesselCli := &mockVesselClient{}
	vesselCli.confirmCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return &vesselpb.Empty{}, nil
	}

	mgr := newManager(t, vesselCli, cache, outbox, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.deleteDataCalls)
	assert.Equal(t, 1, vesselCli.confirmCalls)
	assert.Equal(t, 0, vesselCli.releaseCalls)
}

func TestHandleCapacityEvent_ConfirmVesselFailureSchedulesRetry(t *testing.T) {
	event := newTestEvent(CONFIRM)

	outbox := newOutboxWithStore()

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
	vesselCli.confirmCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, fmt.Errorf("confirm capacity failed")
	}

	mgr := newManager(t, vesselCli, cache, outbox, nil)

	err := mgr.handleCapacityEvent(t.Context(), []byte(event.ReservationInfo.Id.String()), mustMarshalEvent(t, event))
	assert.NoError(t, err)
	assert.Equal(t, 1, vesselCli.confirmCalls)
	assert.Equal(t, 0, vesselCli.releaseCalls)

	// A retry event was scheduled with CacheCleared=true and RetryCount=1
	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	var retryEvent CapacityEvent
	require.NoError(t, json.Unmarshal(pending[0].Payload, &retryEvent))
	assert.Equal(t, CONFIRM, retryEvent.Action)
	assert.Equal(t, 1, retryEvent.RetryCount)
	assert.True(t, retryEvent.CacheCleared)
}

func TestHandleCapacityEvent_ExhaustRetries_Release_And_NotifyConfirmConsignmentDLQ(t *testing.T) {
	event := newTestEvent(CONFIRM)

	cache := &mockCache{
		deleteID: func(ctx context.Context, id string) (bool, error) {
			return true, nil
		},
	}
	cache.deleteData = func(ctx context.Context, id string) (bool, error) {
		cache.deleteDataCalls++
		return true, nil
	}

	outbox := newOutboxWithStore()

	producer := &mockProducer{}

	producer.produce = func(ctx context.Context, topic string, key, value []byte, headers kafka.Headers) error {
		return nil
	}

	vesselCli := &mockVesselClient{
		releaseCapacity: func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
			return nil, nil
		},
	}
	vesselCli.confirmCapacity = func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
		return nil, fmt.Errorf("confirm capacity failed")
	}

	mgr := newManager(t, vesselCli, cache, outbox, producer)

	eventJSON := mustMarshalEvent(t, event)

	// Retry event over max retries
	for i := 0; i <= maxRetries; i++ {
		err := mgr.handleCapacityEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
		require.NoError(t, err)

		assert.Equal(t, 1, cache.deleteDataCalls)
		assert.Equal(t, i+1, vesselCli.confirmCalls)

		// Get retry event
		events, err := outbox.GetPendingEvents(t.Context(), 10)
		require.NoError(t, err)
		require.Len(t, events, 1)

		eventJSON = events[0].Payload
	}

	// Process pending capacity failed event
	events, err := outbox.GetPendingEvents(t.Context(), 10)
	require.NoError(t, err)
	require.Len(t, events, 1)

	pendingDLQEvent := events[0]
	assert.Equal(t, CapacityFailedTopic, pendingDLQEvent.Topic)

	err = mgr.handleFailedCapacityEvent(t.Context(), []byte(pendingDLQEvent.Key), pendingDLQEvent.Payload)
	require.NoError(t, err)

	events, err = outbox.GetPendingEvents(t.Context(), 10)
	require.NoError(t, err)

	var confirmFailed *storage.OutboxEvent
	for _, e := range events {
		if e.Topic == ConsignmentConfirmationFailedTopic {
			confirmFailed = e
			break
		}
	}
	require.NotNil(t, confirmFailed, "expected a ConsignmentConfirmationFailed outbox event")
	assert.Equal(t, 1, vesselCli.releaseCalls)
}
