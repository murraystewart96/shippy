package manager

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	"google.golang.org/grpc"
)

// mockRepository

type mockRepository struct {
	create       func(ctx context.Context, consignment *storage.Consignment) error
	getByID      func(ctx context.Context, id string) (*storage.Consignment, error)
	getAll       func(ctx context.Context) ([]*storage.Consignment, error)
	updateStatus func(ctx context.Context, id string, status storage.ConsignmentStatus) error

	mu                sync.Mutex
	updateStatusCalls int
	lastStatus        storage.ConsignmentStatus
}

func (m *mockRepository) Create(ctx context.Context, consignment *storage.Consignment) error {
	return m.create(ctx, consignment)
}

func (m *mockRepository) GetByID(ctx context.Context, id string) (*storage.Consignment, error) {
	return m.getByID(ctx, id)
}

func (m *mockRepository) GetAll(ctx context.Context) ([]*storage.Consignment, error) {
	return m.getAll(ctx)
}

func (m *mockRepository) UpdateStatus(ctx context.Context, id string, status storage.ConsignmentStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateStatusCalls++
	m.lastStatus = status
	return m.updateStatus(ctx, id, status)
}

// mockOutbox

type mockOutbox struct {
	createEvent      func(ctx context.Context, event *storage.OutboxEvent) error
	markPublished    func(ctx context.Context, id uuid.UUID) error
	getPendingEvents func(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error)

	mu   sync.Mutex
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
		pending := make([]*storage.OutboxEvent, 0, len(o.data))
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

// mockPaymentClient

type mockPaymentClient struct {
	authorise func(ctx context.Context, in *paymentpb.AuthoriseRequest, opts ...grpc.CallOption) (*paymentpb.AuthoriseResponse, error)
	capture   func(ctx context.Context, in *paymentpb.CaptureRequest, opts ...grpc.CallOption) (*paymentpb.CaptureResponse, error)
	refund    func(ctx context.Context, in *paymentpb.RefundRequest, opts ...grpc.CallOption) (*paymentpb.RefundResponse, error)
	void      func(ctx context.Context, in *paymentpb.VoidRequest, opts ...grpc.CallOption) (*paymentpb.VoidResponse, error)

	mu           sync.Mutex
	captureCalls int
	refundCalls  int
	voidCalls    int
}

func (m *mockPaymentClient) Authorise(ctx context.Context, in *paymentpb.AuthoriseRequest, opts ...grpc.CallOption) (*paymentpb.AuthoriseResponse, error) {
	return m.authorise(ctx, in, opts...)
}

func (m *mockPaymentClient) Capture(ctx context.Context, in *paymentpb.CaptureRequest, opts ...grpc.CallOption) (*paymentpb.CaptureResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.captureCalls++
	return m.capture(ctx, in, opts...)
}

func (m *mockPaymentClient) Refund(ctx context.Context, in *paymentpb.RefundRequest, opts ...grpc.CallOption) (*paymentpb.RefundResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refundCalls++
	return m.refund(ctx, in, opts...)
}

func (m *mockPaymentClient) Void(ctx context.Context, in *paymentpb.VoidRequest, opts ...grpc.CallOption) (*paymentpb.VoidResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.voidCalls++
	return m.void(ctx, in, opts...)
}

// mockProducer

type mockProducer struct {
	produce func(ctx context.Context, topic string, key, value []byte) error

	mu           sync.Mutex
	produceCalls int
	lastTopic    string
}

func (m *mockProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.produceCalls++
	m.lastTopic = topic
	return m.produce(ctx, topic, key, value)
}

func (m *mockProducer) Close() error { return nil }
