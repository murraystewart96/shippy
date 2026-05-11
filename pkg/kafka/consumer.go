package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"go.opentelemetry.io/otel/trace"
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
			return ctx.Err()
		default:
			message, err := c.client.ReadMessage(300 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() != kafka.ErrTimedOut {
					log.Error().Err(err).Msg("failed to read from topic")
				}
				continue
			}

			log.Info().Str("topic", *message.TopicPartition.Topic).Msg("consuming event...")

			if handler, ok := topicHandlers[*message.TopicPartition.Topic]; ok {
				c.handleMessage(message, handler)
			} else {
				log.Warn().Msgf("no handler for topic: %s", *message.TopicPartition.Topic)
			}
		}
	}
}

func (c *Consumer) handleMessage(message *kafka.Message, handler EventHandler) {
	remoteCtx := otel.GetTextMapPropagator().Extract(context.Background(), NewHeaderCarrier(&message.Headers))

	topic := *message.TopicPartition.Topic
	msgCtx, span := otel.Tracer("kafka/consumer").Start(remoteCtx, topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystem("kafka"),
			semconv.MessagingDestinationName(topic),
			attribute.Int("messaging.kafka.partition", int(message.TopicPartition.Partition)),
		),
	)
	defer span.End()

	if err := handler(msgCtx, message.Key, message.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Error().Err(err).Str("topic", topic).Msg("handler failed")
	}
}

func (c *Consumer) Close() error {
	return c.client.Close()
}
