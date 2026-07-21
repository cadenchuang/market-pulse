package kafka

import (
	"context"
	"errors"

	kgo "github.com/segmentio/kafka-go"
)

// SegmentioConsumer is the pure-Go default Consumer implementation, backed by a
// segmentio/kafka-go group reader. Offsets are committed manually
// (CommitInterval defaults to 0) so we get at-least-once semantics: commit only
// after the item's processed output has been produced.
type SegmentioConsumer struct {
	reader *kgo.Reader
}

// NewSegmentioConsumer joins groupID on topic across the given brokers.
func NewSegmentioConsumer(brokers []string, groupID, topic string) *SegmentioConsumer {
	return &SegmentioConsumer{
		reader: kgo.NewReader(kgo.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10 << 20, // 10 MiB
		}),
	}
}

// Fetch reads the next message without committing it.
func (c *SegmentioConsumer) Fetch(ctx context.Context) (Record, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Key:       m.Key,
		Value:     m.Value,
		Partition: m.Partition,
		Offset:    m.Offset,
		handle:    m,
	}, nil
}

// Commit commits the given record's offset.
func (c *SegmentioConsumer) Commit(ctx context.Context, r Record) error {
	m, ok := r.handle.(kgo.Message)
	if !ok {
		return errors.New("segmentio consumer: record has no commit handle")
	}
	return c.reader.CommitMessages(ctx, m)
}

// Lag returns the reader's current lag.
func (c *SegmentioConsumer) Lag() int64 { return c.reader.Lag() }

// Close closes the underlying reader.
func (c *SegmentioConsumer) Close() error { return c.reader.Close() }
