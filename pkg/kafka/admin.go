package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TopicConfig struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

// EnsureTopics creates each topic if it does not already exist.
// Idempotent: calling with an existing topic is a no-op.
func EnsureTopics(ctx context.Context, bootstrapServers string, topics []TopicConfig) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	specs := make([]kafka.TopicSpecification, len(topics))
	for i, t := range topics {
		specs[i] = kafka.TopicSpecification{
			Topic:             t.Name,
			NumPartitions:     t.NumPartitions,
			ReplicationFactor: t.ReplicationFactor,
		}
	}

	results, err := admin.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("failed to create topics: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic %s: %w", result.Topic, result.Error)
		}
	}

	return nil
}
