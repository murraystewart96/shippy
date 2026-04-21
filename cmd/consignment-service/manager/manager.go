package manager

import (
	"context"
	"fmt"
	"sync"

	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/pkg/kafka"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
)

type Config struct {
	OutboxInterval int
}

const (
	ConsignmentPaymentAuthorisedTopic = "consignment.payment.authorised"

	// ConsignmentConfirmationFailedTopic receives failed confirmation events in two shapes,
	// discriminated by PaymentCaptured:
	//
	//   false — payment capture exhausted retries; PaymentAuthID is set for voiding.
	//   true  — vessel confirm exhausted retries after payment was taken;
	//            PaymentID is set for refunding. CacheCleared will be true.
	//
	// Both shapes trigger: undo payment, release reservation, cancel consignment.
	ConsignmentConfirmationFailedTopic = "consignment.confirmation.failed"

	ConsignmentConfirmedTopic    = "consignment.confirmed"
	ConsignmentCancelledTopic    = "consignment.cancelled"
	ConsignmentStatusFailedTopic = "consignment.status.failed"

	ConfirmCapacityTopic = "reservation.capacity.confirm"
	ReleaseCapacityTopic = "reservation.capacity.release"

	maxRetries = 3
)

type Manager struct {
	repository storage.ConsignmentRepository
	outbox     storage.OutboxRepository
	paymentCli paymentpb.PaymentServiceClient

	consumer       kafka.IConsumer
	producer       kafka.IProducer
	eventHandlers  kafka.EventHandlers
	outboxInterval int
}

func New(
	producer kafka.IProducer,
	consumer kafka.IConsumer,
	topics []string,
	outbox storage.OutboxRepository,
	paymentCli paymentpb.PaymentServiceClient,
	repo storage.ConsignmentRepository,
	cfg Config,
) (*Manager, error) {
	manager := &Manager{
		consumer:       consumer,
		producer:       producer,
		paymentCli:     paymentCli,
		repository:     repo,
		outbox:         outbox,
		outboxInterval: cfg.OutboxInterval,
	}

	eventHandlers := kafka.EventHandlers{
		ConsignmentPaymentAuthorisedTopic: manager.handlePaymentAuthorisedEvent,
		ConsignmentConfirmationFailedTopic: manager.handleFailedConfirmationEvent,
		ConsignmentCancelledTopic:         manager.handleConsignmentCancelledEvent,
		ConsignmentConfirmedTopic:         manager.handleConsignmentConfirmedEvent,
	}

	// Assign configured topic handlers
	activeHandlers := make(kafka.EventHandlers)
	for _, topic := range topics {
		handler, found := eventHandlers[topic]
		if !found {
			return nil, fmt.Errorf("no event handler for topic: %s", topic)
		}
		activeHandlers[topic] = handler
	}

	manager.eventHandlers = activeHandlers

	return manager, nil
}

func (m *Manager) Start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		m.processOutbox(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.processEvents(ctx); err != nil {
			errCh <- err
		}
	}()

	return errCh
}
