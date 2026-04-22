package manager

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/stretchr/testify/assert"
)

func TestReleaseReservations(t *testing.T) {
	reservations := []*storage.ReservationInfo{
		{Id: uuid.New(), VesselID: uuid.New(), NumberOfContainers: 2, Weight: 100},
		{Id: uuid.New(), VesselID: uuid.New(), NumberOfContainers: 3, Weight: 200},
		{Id: uuid.New(), VesselID: uuid.New(), NumberOfContainers: 1, Weight: 50},
	}

	cache := &mockCache{
		getExpired: func(ctx context.Context) ([]*storage.ReservationInfo, error) {
			return reservations, nil
		},
	}

	createdEvents := make([]*storage.OutboxEvent, 0)
	outbox := &mockOutbox{
		data: make(map[string]*storage.OutboxEvent),
		createEvent: func(ctx context.Context, event *storage.OutboxEvent) error {
			createdEvents = append(createdEvents, event)
			return nil
		},
	}

	mgr, err := New(nil, nil, nil, []string{}, cache, outbox, config.Manager{})
	assert.NoError(t, err)

	err = mgr.releaseReservations(t.Context())
	assert.NoError(t, err)

	// Two outbox events scheduled per expired reservation
	assert.Len(t, createdEvents, len(reservations)*2)

	// Each event key matches a reservation ID
	reservationIDs := make(map[string]bool)
	for _, r := range reservations {
		reservationIDs[r.Id.String()] = true
	}

	expectedTopic := []string{ReleaseCapacityTopic, ConsignmentCancelledTopic}
	for i, e := range createdEvents {
		if e.Topic == ReleaseCapacityTopic {
			assert.True(t, reservationIDs[e.Key], "unexpected reservation ID in outbox event: %s", e.Key)
		}
		assert.Equal(t, expectedTopic[i%2], e.Topic)
	}
}
