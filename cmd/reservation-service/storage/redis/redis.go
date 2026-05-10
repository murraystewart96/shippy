package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	reservationKeyNameSpaceFmt     = "reservation:%s"
	reservationDataKeyNameSpaceFmt = "reservation_data:%s"
)

type Cache struct {
	client             *redis.Client
	reservationTTL     time.Duration
	reservationDataTTL time.Duration
}

// TODO - review ttl settings
func NewCache(cfg *config.Redis) *Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: "",
		DB:       0,
	})

	if err := redisotel.InstrumentTracing(client); err != nil {
		log.Warn().Err(err).Msg("failed to instrument redis tracing")
	}

	return &Cache{
		client:             client,
		reservationTTL:     time.Duration(cfg.ReservationTTL) * time.Second,
		reservationDataTTL: time.Duration(cfg.ReservationDataTTL) * time.Second,
	}
}

// Store caches the reservation ID and the reservation data seperately. The reservation ID is what defines the TTL for the
// reservation. When this expires the reservation is considered void. The reservation data is stored with a longer TTL
// so it can be accessed after the fact to restore the resources claimed by the reservation.
func (c *Cache) Store(ctx context.Context, id string, reservation storage.ReservationInfo) error {
	// Store reservation ID
	key := fmt.Sprintf(reservationKeyNameSpaceFmt, id)
	err := c.client.Set(ctx, key, "", c.reservationTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to set price: %w", err)
	}

	// Store reservation data
	dataKey := fmt.Sprintf(reservationDataKeyNameSpaceFmt, id)
	data, err := json.Marshal(reservation)
	if err != nil {
		return fmt.Errorf("failed to marshal reservation: %w", err)
	}
	err = c.client.Set(ctx, dataKey, data, c.reservationDataTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to set reservation data: %w", err)
	}

	return nil
}

func (c *Cache) GetData(ctx context.Context, id string) (storage.ReservationInfo, error) {
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

// GetExpired gets all data entries whose corresponding id entry has expired. These are reservations
// that need to be cancelled.
func (c *Cache) GetExpired(ctx context.Context) ([]*storage.ReservationInfo, error) {
	var cursor uint64
	var idKeys []string
	var dataKeys []string

	// Scan all active reservation ID keys
	for {
		var batch []string
		var err error
		batch, cursor, err = c.client.Scan(ctx, cursor, "reservation:*", 100).Result()
		if err != nil {
			return nil, err
		}
		idKeys = append(idKeys, batch...)
		if cursor == 0 {
			break
		}
	}

	// Scan all reservation data keys
	for {
		var batch []string
		var err error
		batch, cursor, err = c.client.Scan(ctx, cursor, "reservation_data:*", 100).Result()
		if err != nil {
			return nil, err
		}
		dataKeys = append(dataKeys, batch...)
		if cursor == 0 {
			break
		}
	}

	if len(dataKeys) == 0 {
		return nil, nil
	}

	// Build set of active reservation IDs
	activeIDs := make(map[string]struct{}, len(idKeys))
	for _, key := range idKeys {
		activeIDs[strings.TrimPrefix(key, "reservation:")] = struct{}{}
	}

	// Fetch all reservation data values
	values, err := c.client.MGet(ctx, dataKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to mget reservation data: %w", err)
	}

	var expired []*storage.ReservationInfo
	for _, val := range values {
		if val == nil {
			// Entry was deleted after fetching the keys
			continue
		}

		var reservation storage.ReservationInfo
		if err := json.Unmarshal([]byte(val.(string)), &reservation); err != nil {
			log.Warn().Err(err).Msg("failed to unmarshal reservation")
			continue
		}

		// Check if reservation is still active
		if _, active := activeIDs[reservation.Id.String()]; !active {
			expired = append(expired, &reservation)
		}
	}

	return expired, nil
}

func (c *Cache) Refresh(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf(reservationKeyNameSpaceFmt, id)
	refreshed, err := c.client.Expire(ctx, key, c.reservationTTL).Result()
	if err != nil {
		return false, fmt.Errorf("failed to refresh reservation TTL: %w", err)
	}
	return refreshed, nil
}

func (c *Cache) DeleteData(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf(reservationDataKeyNameSpaceFmt, id)

	deleted, err := c.client.Del(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to delete reservation data: %w", err)
	}

	return deleted == 1, nil
}

func (c *Cache) DeleteID(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf(reservationKeyNameSpaceFmt, id)

	deleted, err := c.client.Del(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to delete reservation id: %w", err)
	}

	return deleted == 1, nil
}
