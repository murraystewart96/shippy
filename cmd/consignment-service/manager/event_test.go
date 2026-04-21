package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/proto/payment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestHandleConfirmationEvent(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: "payment id"}, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	mgr, err := New(producer, nil, nil, nil, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: "payment id",
		ConsignmentID: "consignment id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.NoError(t, err)

	assert.Equal(t, paymentCli.captureCalls, 1)
	assert.Equal(t, producer.produceCalls, 1)
	assert.Equal(t, repo.updateStatusCalls, 1)
}

func TestHandleConfirmationEvent_PaymentFail(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return nil, fmt.Errorf("payment failed")
		},
	}

	outbox := newOutboxWithStore()

	mgr, err := New(nil, nil, nil, outbox, paymentCli, nil, Config{OutboxInterval: 10})
	require.NoError(t, err)

	consignmentID := "consignment id"

	event := &ConfirmationEvent{
		PaymentAuthID: "payment id",
		ConsignmentID: consignmentID,
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payment failed")

	assert.NotNil(t, outbox.data[consignmentID])
	assert.Equal(t, ConsignmentPaymentAuthorisedTopic, outbox.data[consignmentID].Topic)
}

func TestHandleConfirmationEvent_ProduceFailsWithSuccessfulRetry(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: "payment id"}, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return fmt.Errorf("produce failed")
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	outbox := newOutboxWithStore()

	mgr, err := New(producer, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: "payment id",
		ConsignmentID: "consignment id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "produce failed")

	assert.Equal(t, 1, paymentCli.captureCalls)
	assert.Equal(t, 1, producer.produceCalls)
	assert.Equal(t, 0, repo.updateStatusCalls)

	producer.produce = func(ctx context.Context, topic string, key, value []byte) error {
		return nil
	}

	pendingEvents, err := outbox.GetPendingEvents(t.Context(), 10)
	require.NoError(t, err)
	require.Len(t, pendingEvents, 1)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), pendingEvents[0].Payload)
	require.NoError(t, err)

	// Payment capture should be skipped
	assert.Equal(t, 1, paymentCli.captureCalls)
	assert.Equal(t, 2, producer.produceCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
}

// TestHandleConfirmationEvent_ExhaustRetries exhausts retries and publishes
// event to DLQ.
func TestHandleConfirmationEvent_ExhaustRetries_RefundPayment(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: "payment id"}, nil
		},
		refund: func(ctx context.Context, in *payment.RefundRequest, opts ...grpc.CallOption) (*payment.RefundResponse, error) {
			return &payment.RefundResponse{}, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return fmt.Errorf("produce failed")
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	outbox := newOutboxWithStore()

	mgr, err := New(producer, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: "payment id",
		ConsignmentID: "consignment id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	// Retry event over max retries
	for i := 0; i <= maxRetries; i++ {
		err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "produce failed")

		assert.Equal(t, 1, paymentCli.captureCalls)
		assert.Equal(t, i+1, producer.produceCalls)
		assert.Equal(t, 0, repo.updateStatusCalls)

		// Get retry event
		eventJSON = outbox.data[event.ConsignmentID].Payload
	}

	// Update mock functions for next event
	producer.produce = func(ctx context.Context, topic string, key, value []byte) error {
		return nil
	}

	pendingEvents, err := outbox.GetPendingEvents(t.Context(), 10)
	require.NoError(t, err)
	require.Len(t, pendingEvents, 1)

	assert.Equal(t, ConsignmentConfirmationFailedTopic, pendingEvents[0].Topic)

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), pendingEvents[0].Payload)
	require.NoError(t, err)

	// Payment capture should be skipped
	assert.Equal(t, 1, paymentCli.refundCalls)
	assert.Equal(t, maxRetries+2, producer.produceCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
}

func TestHandleFailedConfirmationEvent_RefundPayment(t *testing.T) {
	paymentCli := &mockPaymentClient{
		refund: func(ctx context.Context, in *payment.RefundRequest, opts ...grpc.CallOption) (*payment.RefundResponse, error) {
			return &payment.RefundResponse{}, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	mgr, err := New(producer, nil, nil, nil, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       "payment-id",
		ConsignmentID:   "consignment-id",
		ReservationID:   "reservation-id",
		VesselID:        "vessel-id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.NoError(t, err)

	assert.Equal(t, 1, paymentCli.refundCalls)
	assert.Equal(t, 0, paymentCli.voidCalls)
	assert.Equal(t, 1, producer.produceCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleFailedConfirmationEvent_VoidPayment(t *testing.T) {
	paymentCli := &mockPaymentClient{
		void: func(ctx context.Context, in *payment.VoidRequest, opts ...grpc.CallOption) (*payment.VoidResponse, error) {
			return nil, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	outbox := newOutboxWithStore()

	mgr, err := New(producer, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: false,
		PaymentAuthID:   "payment id",
		ConsignmentID:   "consignment id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.NoError(t, err)

	// Payment capture should be skipped
	assert.Equal(t, 1, paymentCli.voidCalls)
	assert.Equal(t, 1, producer.produceCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
}

func TestHandleFailedConfirmationEvent_VoidFails_StillReleasesAndCancels(t *testing.T) {
	paymentCli := &mockPaymentClient{
		void: func(ctx context.Context, in *payment.VoidRequest, opts ...grpc.CallOption) (*payment.VoidResponse, error) {
			return nil, fmt.Errorf("void failed")
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	mgr, err := New(producer, nil, nil, nil, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: false,
		PaymentAuthID:   "auth-id",
		ConsignmentID:   "consignment-id",
		ReservationID:   "reservation-id",
		VesselID:        "vessel-id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.NoError(t, err)

	assert.Equal(t, backoffAttempts+1, paymentCli.voidCalls)
	assert.Equal(t, 1, producer.produceCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleFailedConfirmationEvent_RefundFails_StillReleasesAndCancels(t *testing.T) {
	paymentCli := &mockPaymentClient{
		refund: func(ctx context.Context, in *payment.RefundRequest, opts ...grpc.CallOption) (*payment.RefundResponse, error) {
			return nil, fmt.Errorf("refund failed")
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return nil
		},
	}

	mgr, err := New(producer, nil, nil, nil, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       "payment-id",
		ConsignmentID:   "consignment-id",
		ReservationID:   "reservation-id",
		VesselID:        "vessel-id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	require.NoError(t, err)

	assert.Equal(t, backoffAttempts+1, paymentCli.refundCalls)
	assert.Equal(t, 1, producer.produceCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleConfirmationEvent_UpdateStatusFails_IsNonFatal(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: "payment-id"}, nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte) error {
			return nil
		},
	}

	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status storage.ConsignmentStatus) error {
			return fmt.Errorf("db unavailable")
		},
	}

	mgr, err := New(producer, nil, nil, nil, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: "auth-id",
		ConsignmentID: "consignment-id",
	}

	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), eventJSON)
	assert.NoError(t, err)

	assert.Equal(t, 1, paymentCli.captureCalls)
	assert.Equal(t, 1, producer.produceCalls)
}

func TestHandleConfirmationEvent_InvalidJSON(t *testing.T) {
	mgr, err := New(nil, nil, nil, nil, nil, nil, Config{OutboxInterval: 10})
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte("key"), []byte("not valid json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal event")
}
