package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type IProducer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

type Producer struct {
	client *kafka.Producer
}

type ProducerConfig struct {
	BootstrapServers string
	Acks             string
}

func NewProducer(cfg *ProducerConfig) (*Producer, error) {
	client, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
		"acks":              cfg.Acks,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		client: client,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	err := p.client.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: value,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce event: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
