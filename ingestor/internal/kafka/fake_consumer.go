package kafka

import (
	"context"
	"io"
)

// FakeConsumer is an in-memory Consumer for tests. It replays a fixed slice of
// records, then returns io.EOF (the same signal a closed real reader gives),
// which lets the processor's run loop terminate deterministically in tests.
// The processor consumes from a single goroutine, so this needs no locking.
type FakeConsumer struct {
	records   []Record
	idx       int
	Committed []int64 // offsets committed, in order
	lag       int64
}

// NewFakeConsumer builds a consumer that will hand out records in order.
func NewFakeConsumer(records []Record) *FakeConsumer {
	return &FakeConsumer{records: records, lag: int64(len(records))}
}

// Fetch returns the next record, or io.EOF once exhausted.
func (f *FakeConsumer) Fetch(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if f.idx >= len(f.records) {
		return Record{}, io.EOF
	}
	r := f.records[f.idx]
	r.Offset = int64(f.idx)
	f.idx++
	f.lag = int64(len(f.records) - f.idx)
	return r, nil
}

// Commit records the offset as committed.
func (f *FakeConsumer) Commit(_ context.Context, r Record) error {
	f.Committed = append(f.Committed, r.Offset)
	return nil
}

// Lag returns remaining unread records.
func (f *FakeConsumer) Lag() int64 { return f.lag }

// Close is a no-op for the fake.
func (f *FakeConsumer) Close() error { return nil }
