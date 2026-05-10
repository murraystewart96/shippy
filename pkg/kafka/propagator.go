package kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// HeaderCarrier implements propagation.TextMapCarrier for Kafka headers
type HeaderCarrier struct {
	headers *[]kafka.Header
}

func (c *HeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *HeaderCarrier) Set(key string, value string) {
	if c.headers == nil {
		return
	}
	// Remove existing header with the same key
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i] = kafka.Header{Key: key, Value: []byte(value)}
			return
		}
	}
	// Add new header
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c *HeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

func NewHeaderCarrier(headers *[]kafka.Header) *HeaderCarrier {
	return &HeaderCarrier{headers: headers}
}
