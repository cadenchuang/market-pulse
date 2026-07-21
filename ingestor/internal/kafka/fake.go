package kafka

import (
	"context"
	"errors"
	"sync"
)

// ErrFakeProduce is returned by FakeProducer once its FailAfter threshold is hit.
var ErrFakeProduce = errors.New("fake producer: forced failure")

// CapturedMessage is a key/value pair recorded by FakeProducer.
type CapturedMessage struct {
	Key   []byte
	Value []byte
}

// FakeProducer is an in-memory Producer for tests. It records every message and
// is safe for concurrent use by the ingestor's worker pool. This lets us verify
// that messages "land" end-to-end without standing up a real broker.
type FakeProducer struct {
	mu       sync.Mutex
	messages []CapturedMessage

	// FailAfter, if > 0, makes Produce return ErrFakeProduce after that many
	// successful calls (useful for exercising error handling).
	FailAfter int
	calls     int
}

// NewFakeProducer returns an empty FakeProducer.
func NewFakeProducer() *FakeProducer { return &FakeProducer{} }

// Produce records the message unless ctx is done or FailAfter has been reached.
func (f *FakeProducer) Produce(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.FailAfter > 0 && f.calls > f.FailAfter {
		return ErrFakeProduce
	}
	f.messages = append(f.messages, CapturedMessage{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	return nil
}

// Close is a no-op for the fake.
func (f *FakeProducer) Close() error { return nil }

// Messages returns a copy of everything captured so far.
func (f *FakeProducer) Messages() []CapturedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]CapturedMessage, len(f.messages))
	copy(out, f.messages)
	return out
}
