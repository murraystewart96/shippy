package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

type IConsumer interface {
	StartConsuming(ctx context.Context, handlers EventHandlers) error
	Close() error
}

type Consumer struct {
	client *kafka.Consumer
}

type ConsumerConfig struct {
	BootstrapServers string
	GroupID          string
	OffsetReset      string
}

type EventHandler func(ctx context.Context, key, value []byte) error
type EventHandlers map[string]EventHandler

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	client, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"group.id":          cfg.GroupID,
		"auto.offset.reset": cfg.OffsetReset,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		client: client,
	}, nil
}

func (c *Consumer) StartConsuming(ctx context.Context, topicHandlers EventHandlers) error {
	topics := make([]string, 0, len(topicHandlers))
	for topic := range topicHandlers {
		topics = append(topics, topic)
	}

	log.Info().Msgf("subscribing to topics: %v", topics)

	if err := c.client.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics (%v): %w", topics, err)
	}

	defer c.client.Close()

	log.Info().Msg("consuming...")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			message, err := c.client.ReadMessage(300 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Error().Err(err).Msg("failed to read from topic")
				}
				continue
			}

			log.Info().Msg("consuming event...")

			if handler, ok := topicHandlers[*message.TopicPartition.Topic]; ok {
				if err := handler(ctx, message.Key, message.Value); err != nil {
					log.Error().Err(err).Msg("message handler failed")
				} else {
					if _, err := c.client.CommitMessage(message); err != nil {
						log.Error().Err(err).Msg("failed to commit message offset")
					}
				}
			} else {
				log.Warn().Msgf("no handler for topic: %s", *message.TopicPartition.Topic)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.client.Close()
}
