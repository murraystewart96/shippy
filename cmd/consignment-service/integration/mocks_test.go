package integration

import (
	"context"
	"sync"

	paymentpb "github.com/murraystewart96/shippy/proto/payment"
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
