package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ErrBrokerUnreachable is returned by Produce when the broker cannot be reached.
// Callers that loop over multiple messages should abort on this error rather than continuing.
var ErrBrokerUnreachable = errors.New("kafka broker unreachable")

type Headers []kafka.Header

type IProducer interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers Headers) error
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

func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte, headers Headers) error {
	deliveryCh := make(chan kafka.Event, 1)

	err := p.client.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:     key,
		Value:   value,
		Headers: headers,
	}, deliveryCh)
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	select {
	case e := <-deliveryCh:
		msg := e.(*kafka.Message)
		if msg.TopicPartition.Error != nil {
			if isBrokerUnreachable(msg.TopicPartition.Error) {
				return fmt.Errorf("%w: %w", ErrBrokerUnreachable, msg.TopicPartition.Error)
			}
			return fmt.Errorf("failed to deliver message: %w", msg.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isBrokerUnreachable(err error) bool {
	var kafkaErr kafka.Error
	return errors.As(err, &kafkaErr) &&
		(kafkaErr.Code() == kafka.ErrMsgTimedOut || kafkaErr.Code() == kafka.ErrTransport)
}

func HeadersFromTraceContext(tc map[string]string) Headers {
	headers := make(Headers, 0, len(tc))
	for k, v := range tc {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return headers
}

func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
