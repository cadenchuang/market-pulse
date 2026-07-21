// Package kafka wraps the Kafka client behind a small interface so the concrete
// implementation is swappable. The default is pure-Go segmentio/kafka-go
// (segmentio.go), which builds everywhere with no CGo/librdkafka. A
// confluent-kafka-go implementation can be dropped in behind this same interface
// for environments that prefer it — callers never change.
//
// Kafka is the language boundary in Market Pulse: Go only ever *produces* bytes
// to a topic here; it never calls Python directly.
package kafka

import "context"

// Producer sends messages to a single Kafka topic. Implementations may batch or
// send asynchronously internally, but must respect ctx cancellation.
type Producer interface {
	// Produce sends one message with the given key and value.
	Produce(ctx context.Context, key, value []byte) error
	// Close flushes any buffered messages and releases resources.
	Close() error
}
