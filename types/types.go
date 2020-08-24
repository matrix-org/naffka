package types

import (
	"time"

	"github.com/Shopify/sarama"
)

// A Message is used internally within naffka to store messages.
// It is converted to a sarama.ConsumerMessage when exposed to the
// public APIs to maintain API compatibility with sarama.
type Message struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   []sarama.RecordHeader
}

func (m *Message) ConsumerMessage(topic string) *sarama.ConsumerMessage {
	var headers []*sarama.RecordHeader
	for i := range m.Headers {
		headers = append(headers, &m.Headers[i])
	}

	return &sarama.ConsumerMessage{
		Topic:     topic,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Headers:   headers,
	}
}
