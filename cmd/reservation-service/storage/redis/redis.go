package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/redis/go-redis/v9"
)

const (
	reservationTTL     = 10 * time.Minute
	reservationDataTTL = 30 * time.Minute

	reservationKeyNameSpaceFmt = "reservation:%s"

	reservationDataKeyNameSpaceFmt = "reservation_data:%s"
)

type Cache struct {
	client *redis.Client
}

func NewCache(cfg *config.Redis) *Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return &Cache{
		client: client,
	}
}

// Store caches the reservation ID and the reservation data seperately. The reservation ID is what defines the TTL for the
// reservation. When this expires the reservation is considered void. The reservation data is stored with a longer TTL
// so it can be accessed after the fact to restore the resources claimed by the reservation.
func (c *Cache) Store(ctx context.Context, id string, reservation storage.ReservationInfo) error {
	// Store reservation ID
	key := fmt.Sprintf(reservationKeyNameSpaceFmt, id)
	err := c.client.Set(ctx, key, "", reservationTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to set price: %w", err)
	}

	// Store reservation data
	dataKey := fmt.Sprintf(reservationDataKeyNameSpaceFmt, id)
	data, err := json.Marshal(reservation)
	if err != nil {
		return fmt.Errorf("failed to marshal reservation: %w", err)
	}
	err = c.client.Set(ctx, dataKey, data, reservationDataTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to set reservation data: %w", err)
	}

	return nil
}

func (c *Cache) Get(ctx context.Context, id string) (storage.ReservationInfo, error) {
	key := fmt.Sprintf(reservationDataKeyNameSpaceFmt, id)

	value, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return storage.ReservationInfo{}, fmt.Errorf("reservation %s not found: %w", id, err)
		}
		return storage.ReservationInfo{}, fmt.Errorf("failed to get reservation: %w", err)
	}

	var info storage.ReservationInfo
	if err := json.Unmarshal(value, &info); err != nil {
		return storage.ReservationInfo{}, fmt.Errorf("failed to unmarshal reservation: %w", err)
	}

	return info, nil
}

// Delete deletes the reservation ID. This signals that a reservation has already confirmed.
// We leave redis to automatically remove the associated data entry.
func (c *Cache) Delete(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf(reservationKeyNameSpaceFmt, id)

	deleted, err := c.client.Del(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to delete reservation: %w", err)
	}

	return (deleted == 1), nil
}
