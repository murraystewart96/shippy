package manager

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/pkg/kafka"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
)

type mockProducer struct {
	produce      func(ctx context.Context, topic string, key, value []byte, headers kafka.Headers) error
	produceCalls int
}

func (m *mockProducer) Produce(ctx context.Context, topic string, key, value []byte, headers kafka.Headers) error {
	m.produceCalls++
	return m.produce(ctx, topic, key, value, headers)
}

func (m *mockProducer) Close() error { return nil }

var _ kafka.IProducer = (*mockProducer)(nil)

func TestPublishOutbox(t *testing.T) {
	now := time.Now()
	events := []*storage.OutboxEvent{
		{Id: uuid.New(), Topic: ReleaseCapacityTopic, Key: uuid.NewString(), Payload: []byte(`{}`)},
		{Id: uuid.New(), Topic: ReleaseCapacityTopic, Key: uuid.NewString(), Payload: []byte(`{}`)},
		{Id: uuid.New(), Topic: ReleaseCapacityTopic, Key: uuid.NewString(), Payload: []byte(`{}`)},
	}

	publishedIDs := make([]uuid.UUID, 0)

	outbox := &mockOutbox{
		getPendingEvents: func(ctx context.Context, lease time.Duration) ([]*storage.OutboxEvent, error) {
			return events, nil
		},
		markPublished: func(ctx context.Context, id uuid.UUID) error {
			publishedIDs = append(publishedIDs, id)
			for _, e := range events {
				if e.Id == id {
					e.PublishedAt = &now
				}
			}
			return nil
		},
	}

	producer := &mockProducer{
		produce: func(ctx context.Context, topic string, key, value []byte, headers kafka.Headers) error {
			return nil
		},
	}

	mgr, err := New(nil, producer, nil, []string{}, nil, outbox, config.Manager{})
	assert.NoError(t, err)

	err = mgr.publishOutbox(t.Context())
	assert.NoError(t, err)

	// All 3 events were produced
	assert.Equal(t, 3, producer.produceCalls)

	// All 3 events were marked as published
	assert.Len(t, publishedIDs, 3)
	for _, e := range events {
		assert.NotNil(t, e.PublishedAt)
	}
}
