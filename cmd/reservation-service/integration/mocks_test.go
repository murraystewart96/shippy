package integration

import (
	"context"
	"sync"

	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
)

type mockVesselService struct {
	vesselpb.UnimplementedVesselServiceServer

	mu                   sync.Mutex
	releaseCapacityCalls int
	confirmCapacityCalls int

	releaseFunc func(ctx context.Context, req *vesselpb.CapacityRequest) (*vesselpb.Empty, error)
	confirmFunc func(ctx context.Context, req *vesselpb.CapacityRequest) (*vesselpb.Empty, error)
}

func (m *mockVesselService) ReleaseCapacity(ctx context.Context, req *vesselpb.CapacityRequest) (*vesselpb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCapacityCalls++
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, req)
	}
	return &vesselpb.Empty{}, nil
}

func (m *mockVesselService) ConfirmCapacity(ctx context.Context, req *vesselpb.CapacityRequest) (*vesselpb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.confirmCapacityCalls++
	if m.confirmFunc != nil {
		return m.confirmFunc(ctx, req)
	}
	return &vesselpb.Empty{}, nil
}

func (m *mockVesselService) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCapacityCalls = 0
	m.confirmCapacityCalls = 0
	m.releaseFunc = nil
	m.confirmFunc = nil
}