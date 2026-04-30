package integration

import (
	"context"
	"sync"

	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"
)

type mockPaymentService struct {
	paymentpb.UnimplementedPaymentServiceServer

	mu           sync.Mutex
	captureCalls int
	refundCalls  int
	voidCalls    int

	captureFunc func(ctx context.Context, req *paymentpb.CaptureRequest) (*paymentpb.CaptureResponse, error)
	refundFunc  func(ctx context.Context, req *paymentpb.RefundRequest) (*paymentpb.RefundResponse, error)
	voidFunc    func(ctx context.Context, req *paymentpb.VoidRequest) (*paymentpb.VoidResponse, error)
}

func (m *mockPaymentService) Capture(ctx context.Context, req *paymentpb.CaptureRequest) (*paymentpb.CaptureResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.captureCalls++
	if m.captureFunc != nil {
		return m.captureFunc(ctx, req)
	}
	return &paymentpb.CaptureResponse{PaymentId: "test-payment-id"}, nil
}

func (m *mockPaymentService) Refund(ctx context.Context, req *paymentpb.RefundRequest) (*paymentpb.RefundResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refundCalls++
	if m.refundFunc != nil {
		return m.refundFunc(ctx, req)
	}
	return &paymentpb.RefundResponse{}, nil
}

func (m *mockPaymentService) Void(ctx context.Context, req *paymentpb.VoidRequest) (*paymentpb.VoidResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.voidCalls++
	if m.voidFunc != nil {
		return m.voidFunc(ctx, req)
	}
	return &paymentpb.VoidResponse{}, nil
}

func (m *mockPaymentService) Authorise(ctx context.Context, req *paymentpb.AuthoriseRequest) (*paymentpb.AuthoriseResponse, error) {
	return &paymentpb.AuthoriseResponse{AuthId: "test-auth-id"}, nil
}

func (m *mockPaymentService) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.captureCalls = 0
	m.refundCalls = 0
	m.voidCalls = 0
	m.captureFunc = nil
	m.refundFunc = nil
	m.voidFunc = nil
}

type mockReservationService struct {
	reservepb.UnimplementedReservationServiceServer

	mu                  sync.Mutex
	reserveCapacityCalls int
	refreshCalls        int

	reserveFunc func(ctx context.Context, req *reservepb.ReserveCapacityRequest) (*reservepb.ReservationResponse, error)
	refreshFunc func(ctx context.Context, req *reservepb.CapacityActionRequest) (*reservepb.Empty, error)
}

func (m *mockReservationService) ReserveCapacity(ctx context.Context, req *reservepb.ReserveCapacityRequest) (*reservepb.ReservationResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveCapacityCalls++
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, req)
	}
	return &reservepb.ReservationResponse{Id: "test-reservation-id", VesselId: "test-vessel-id"}, nil
}

func (m *mockReservationService) RefreshReservation(ctx context.Context, req *reservepb.CapacityActionRequest) (*reservepb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshCalls++
	if m.refreshFunc != nil {
		return m.refreshFunc(ctx, req)
	}
	return &reservepb.Empty{}, nil
}

func (m *mockReservationService) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveCapacityCalls = 0
	m.refreshCalls = 0
	m.reserveFunc = nil
	m.refreshFunc = nil
}
