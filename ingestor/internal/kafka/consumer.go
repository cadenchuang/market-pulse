package kafka

import "context"

// Record is one consumed message. The unexported handle carries impl-specific
// commit information so callers can Commit without depending on a concrete
// client type.
type Record struct {
	Key       []byte
	Value     []byte
	Partition int
	Offset    int64
	handle    any
}

// Consumer reads from a topic as part of a consumer group. Offsets are committed
// explicitly (at-least-once): the caller commits only after a message has been
// fully processed and its output produced.
type Consumer interface {
	// Fetch returns the next message. It blocks until a message is available or
	// ctx is cancelled. When the consumer is closed it returns io.EOF.
	Fetch(ctx context.Context) (Record, error)
	// Commit marks a record's offset as processed.
	Commit(ctx context.Context, r Record) error
	// Lag returns the current consumer lag (the backpressure signal exported as
	// a metric).
	Lag() int64
	// Close stops the consumer.
	Close() error
}
