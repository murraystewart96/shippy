package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/proto/payment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	testConsignmentID = "consignment-id"
	testReservationID = "reservation-id"
	testVesselID      = "vessel-id"
	testPaymentID     = "payment-id"
	testAuthID        = "auth-id"
)

func mustMarshalEvent(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func pendingTopics(t *testing.T, outbox *mockOutbox) []string {
	t.Helper()
	events, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	topics := make([]string, len(events))
	for i, e := range events {
		topics[i] = e.Topic
	}
	return topics
}

// *** handlePaymentAuthorisedEvent ***

func TestHandlePaymentAuthorisedEvent(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: testPaymentID}, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: testAuthID,
		ConsignmentID: testConsignmentID,
		ReservationID: testReservationID,
		VesselID:      testVesselID,
		Weight:        100,
		Containers:    1,
	}

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, 1, paymentCli.captureCalls)

	// PaymentCaptured event only exists in happy path
	topics := pendingTopics(t, outbox)
	assert.Contains(t, topics, PaymentCapturedTopic)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusConfirmationPending, repo.lastStatus)
}

func TestHandlePaymentAuthorisedEvent_SkipsCapture_WhenAlreadyCaptured(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: testPaymentID}, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       testPaymentID,
		ConsignmentID:   testConsignmentID,
		ReservationID:   testReservationID,
		VesselID:        testVesselID,
	}

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, 0, paymentCli.captureCalls)

	// PaymentCaptured event only exists in happy path
	topics := pendingTopics(t, outbox)
	assert.Contains(t, topics, PaymentCapturedTopic)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusConfirmationPending, repo.lastStatus)
}

func TestHandlePaymentAuthorisedEvent_PaymentFail(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return nil, fmt.Errorf("payment failed")
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: testAuthID,
		ConsignmentID: testConsignmentID,
	}

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)

	// PaymentAuthorisedEvent should be rescheduled
	require.Len(t, pending, 1)
	assert.Equal(t, ConsignmentPaymentAuthorisedTopic, pending[0].Topic)
}

func TestHandlePaymentAuthorisedEvent_PaymentCapturedEventFails_SchedulesRetry(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: testPaymentID}, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	outbox.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		if event.Topic == PaymentCapturedTopic {
			return fmt.Errorf("outbox write failed")
		}
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		outbox.data[event.Key] = event
		return nil
	}

	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentAuthID: testAuthID,
		ConsignmentID: testConsignmentID,
	}

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	assert.NoError(t, err)

	assert.Equal(t, 1, paymentCli.captureCalls)

	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, ConsignmentPaymentAuthorisedTopic, pending[0].Topic)

	// Retry event carries PaymentCaptured=true so capture is skipped on replay
	var retryEvent ConfirmationEvent
	require.NoError(t, json.Unmarshal(pending[0].Payload, &retryEvent))
	assert.True(t, retryEvent.PaymentCaptured)
}

func TestHandlePaymentAuthorisedEvent_ExhaustRetries(t *testing.T) {
	paymentCli := &mockPaymentClient{
		capture: func(ctx context.Context, in *payment.CaptureRequest, opts ...grpc.CallOption) (*payment.CaptureResponse, error) {
			return &payment.CaptureResponse{PaymentId: testPaymentID}, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	outbox.createEvent = func(ctx context.Context, event *storage.OutboxEvent) error {
		if event.Topic == PaymentCapturedTopic {
			return fmt.Errorf("outbox write failed")
		}
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		outbox.data[event.Key] = event
		return nil
	}

	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	eventJSON := mustMarshalEvent(t, &ConfirmationEvent{
		PaymentAuthID: testAuthID,
		ConsignmentID: testConsignmentID,
	})

	for i := 0; i <= maxRetries; i++ {
		err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte(testConsignmentID), eventJSON)
		require.NoError(t, err)

		events, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
		require.NoError(t, err)
		require.Len(t, events, 1)

		eventJSON = events[0].Payload
	}

	assert.Equal(t, 1, paymentCli.captureCalls)

	pending, err := outbox.GetPendingEvents(t.Context(), 30*time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, ConsignmentConfirmationFailedTopic, pending[0].Topic)
}

func TestHandlePaymentAuthorisedEvent_InvalidJSON(t *testing.T) {
	mgr, err := New(nil, nil, nil, nil, nil, nil, Config{OutboxInterval: 10})
	require.NoError(t, err)

	err = mgr.handlePaymentAuthorisedEvent(t.Context(), []byte("key"), []byte("not valid json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal event")
}

// *** handleFailedConfirmationEvent ***

func TestHandleFailedConfirmationEvent_RefundPayment(t *testing.T) {
	paymentCli := &mockPaymentClient{
		refund: func(ctx context.Context, in *payment.RefundRequest, opts ...grpc.CallOption) (*payment.RefundResponse, error) {
			return &payment.RefundResponse{}, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       testPaymentID,
		ConsignmentID:   testConsignmentID,
		ReservationID:   testReservationID,
		VesselID:        testVesselID,
	}

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, 1, paymentCli.refundCalls)
	assert.Equal(t, 0, paymentCli.voidCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleFailedConfirmationEvent_VoidPayment(t *testing.T) {
	paymentCli := &mockPaymentClient{
		void: func(ctx context.Context, in *payment.VoidRequest, opts ...grpc.CallOption) (*payment.VoidResponse, error) {
			return nil, nil
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: false,
		PaymentAuthID:   testAuthID,
		ConsignmentID:   testConsignmentID,
		ReservationID:   testReservationID,
		VesselID:        testVesselID,
	}

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, 1, paymentCli.voidCalls)
	assert.Equal(t, 0, paymentCli.refundCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleFailedConfirmationEvent_VoidFails_StillCancels(t *testing.T) {
	paymentCli := &mockPaymentClient{
		void: func(ctx context.Context, in *payment.VoidRequest, opts ...grpc.CallOption) (*payment.VoidResponse, error) {
			return nil, fmt.Errorf("void failed")
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: false,
		PaymentAuthID:   testAuthID,
		ConsignmentID:   testConsignmentID,
		ReservationID:   testReservationID,
		VesselID:        testVesselID,
	}

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, backoffAttempts+1, paymentCli.voidCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}

func TestHandleFailedConfirmationEvent_RefundFails_StillCancels(t *testing.T) {
	paymentCli := &mockPaymentClient{
		refund: func(ctx context.Context, in *payment.RefundRequest, opts ...grpc.CallOption) (*payment.RefundResponse, error) {
			return nil, fmt.Errorf("refund failed")
		},
	}
	repo := &mockRepository{
		updateStatus: func(ctx context.Context, id string, status string) error {
			return nil
		},
	}
	outbox := newOutboxWithStore()
	mgr, err := New(nil, nil, nil, outbox, paymentCli, repo, Config{OutboxInterval: 10})
	require.NoError(t, err)

	event := &ConfirmationEvent{
		PaymentCaptured: true,
		PaymentID:       testPaymentID,
		ConsignmentID:   testConsignmentID,
		ReservationID:   testReservationID,
		VesselID:        testVesselID,
	}

	err = mgr.handleFailedConfirmationEvent(t.Context(), []byte(event.ConsignmentID), mustMarshalEvent(t, event))
	require.NoError(t, err)

	assert.Equal(t, backoffAttempts+1, paymentCli.refundCalls)
	assert.Equal(t, 1, repo.updateStatusCalls)
	assert.Equal(t, storage.StatusCancelled, repo.lastStatus)
}
