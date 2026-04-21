package manager

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"google.golang.org/grpc"
)

// mockCache

type mockCache struct {
	store      func(ctx context.Context, id string, reservation storage.ReservationInfo) error
	getData    func(ctx context.Context, id string) (storage.ReservationInfo, error)
	getExpired func(ctx context.Context) ([]*storage.ReservationInfo, error)
	deleteData func(ctx context.Context, id string) (bool, error)
	deleteID   func(ctx context.Context, id string) (bool, error)
	refresh    func(ctx context.Context, id string) (bool, error)

	mu              sync.Mutex
	deleteDataCalls int
}

func (m *mockCache) Store(ctx context.Context, id string, reservation storage.ReservationInfo) error {
	return m.store(ctx, id, reservation)
}

func (m *mockCache) GetData(ctx context.Context, id string) (storage.ReservationInfo, error) {
	return m.getData(ctx, id)
}

func (m *mockCache) GetExpired(ctx context.Context) ([]*storage.ReservationInfo, error) {
	return m.getExpired(ctx)
}

func (m *mockCache) DeleteData(ctx context.Context, id string) (bool, error) {
	return m.deleteData(ctx, id)
}

func (m *mockCache) DeleteID(ctx context.Context, id string) (bool, error) {
	return m.deleteID(ctx, id)
}

func (m *mockCache) Refresh(ctx context.Context, id string) (bool, error) {
	return m.refresh(ctx, id)
}

// mockOutbox

type mockOutbox struct {
	createEvent      func(ctx context.Context, event *storage.OutboxEvent) error
	markPublished    func(ctx context.Context, id uuid.UUID) error
	getPendingEvents func(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error)

	// In memory data store
	data map[string]*storage.OutboxEvent
}

func (m *mockOutbox) CreateEvent(ctx context.Context, event *storage.OutboxEvent) error {
	return m.createEvent(ctx, event)
}

func (m *mockOutbox) MarkPublished(ctx context.Context, id uuid.UUID) error {
	return m.markPublished(ctx, id)
}

func (m *mockOutbox) GetPendingEvents(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error) {
	return m.getPendingEvents(ctx, lease)
}

// mockVesselClient

type mockVesselClient struct {
	reserveCapacity func(ctx context.Context, in *vesselpb.Specification, opts ...grpc.CallOption) (*vesselpb.Response, error)
	releaseCapacity func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error)
	confirmCapacity func(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error)
	create          func(ctx context.Context, in *vesselpb.Vessel, opts ...grpc.CallOption) (*vesselpb.Response, error)

	mu           sync.Mutex
	releaseCalls int
	confirmCalls int
}

func (m *mockVesselClient) ReserveCapacity(ctx context.Context, in *vesselpb.Specification, opts ...grpc.CallOption) (*vesselpb.Response, error) {
	return m.reserveCapacity(ctx, in, opts...)
}

func (m *mockVesselClient) ReleaseCapacity(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
	return m.releaseCapacity(ctx, in, opts...)
}

func (m *mockVesselClient) ConfirmCapacity(ctx context.Context, in *vesselpb.CapacityRequest, opts ...grpc.CallOption) (*vesselpb.Empty, error) {
	return m.confirmCapacity(ctx, in, opts...)
}

func (m *mockVesselClient) Create(ctx context.Context, in *vesselpb.Vessel, opts ...grpc.CallOption) (*vesselpb.Response, error) {
	return m.create(ctx, in, opts...)
}
