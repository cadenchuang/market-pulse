package kafka

import (
	"context"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// SegmentioProducer is the pure-Go default Producer implementation, backed by
// segmentio/kafka-go. It builds with CGO_ENABLED=0 (no librdkafka), which keeps
// the Docker image small and cross-compilation painless.
type SegmentioProducer struct {
	writer *kgo.Writer
}

// NewSegmentioProducer returns a Producer that writes to topic on the given
// brokers. It uses hash partitioning on the message key so the same item id
// always lands on the same partition.
func NewSegmentioProducer(brokers []string, topic string) *SegmentioProducer {
	return &SegmentioProducer{
		writer: &kgo.Writer{
			Addr:         kgo.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kgo.Hash{},
			RequiredAcks: kgo.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Produce sends one message, blocking until it is acknowledged or ctx is done.
func (p *SegmentioProducer) Produce(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kgo.Message{Key: key, Value: value})
}

// Close flushes and closes the underlying writer.
func (p *SegmentioProducer) Close() error { return p.writer.Close() }
