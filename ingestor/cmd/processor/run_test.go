package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/cadenchuang/market-pulse/ingestor/internal/process"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func rawRecord(t *testing.T, id, title, body, lang string) kafka.Record {
	t.Helper()
	item := contract.NewsRaw{
		SchemaVersion: contract.SchemaVersion, ID: id, Source: contract.SourceReplay,
		Title: title, Body: body, Language: lang,
		PublishedAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		IngestedAt:  time.Date(2026, 7, 20, 12, 0, 1, 0, time.UTC),
	}
	v, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Record{Key: item.Key(), Value: v}
}

func counterVal(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}

func testConfig() config {
	return config{
		rawTopic:       "news.raw",
		processedTopic: "news.processed",
		groupID:        "test",
		dedupSize:      1000,
		filter:         process.FilterConfig{MinBodyLen: 20, Language: "en"},
	}
}

// TestRun_DedupNormalizeFilter drives the full processing loop against fake
// Kafka and asserts each decision path, that only clean unique items are
// produced (as valid contract JSON), and that every consumed offset is committed.
func TestRun_DedupNormalizeFilter(t *testing.T) {
	longBody := "this is a sufficiently long synthetic body for the processor test"
	records := []kafka.Record{
		rawRecord(t, "1", "Alpha Corp beats estimates", longBody, "en"),
		rawRecord(t, "2", "ALPHA CORP   beats   estimates", longBody, "en"), // dup after normalize
		rawRecord(t, "3", "Beta Inc update", "short", "en"),                 // too_short -> filtered
		rawRecord(t, "4", "Gamma news in french", longBody, "fr"),           // language -> filtered
		rawRecord(t, "5", "Delta Ltd names new CFO", longBody+" delta", "en"),
	}
	badJSON := kafka.Record{Value: []byte("{not json")}
	records = append(records, badJSON) // error path

	consumer := kafka.NewFakeConsumer(records)
	producer := kafka.NewFakeProducer()
	m := metrics.NewProcessor(prometheus.NewRegistry())

	s, err := run(context.Background(), testConfig(), consumer, producer, m)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if s.consumed != 6 {
		t.Errorf("consumed = %d, want 6", s.consumed)
	}
	if s.processed != 2 {
		t.Errorf("processed = %d, want 2 (ids 1 and 5)", s.processed)
	}
	if s.duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", s.duplicates)
	}
	if s.filtered != 2 {
		t.Errorf("filtered = %d, want 2", s.filtered)
	}
	if s.errors != 1 {
		t.Errorf("errors = %d, want 1", s.errors)
	}

	// Every consumed record must be committed (at-least-once, no stuck offsets).
	if len(consumer.Committed) != int(s.consumed) {
		t.Errorf("committed %d offsets, want %d", len(consumer.Committed), s.consumed)
	}

	// Produced messages must be valid news.processed contract JSON.
	msgs := producer.Messages()
	if len(msgs) != 2 {
		t.Fatalf("produced %d messages, want 2", len(msgs))
	}
	for _, msg := range msgs {
		var p contract.NewsProcessed
		if err := json.Unmarshal(msg.Value, &p); err != nil {
			t.Fatalf("produced message not valid JSON: %v", err)
		}
		if err := p.Validate(); err != nil {
			t.Fatalf("produced message fails contract validation: %v", err)
		}
		if string(msg.Key) != p.ID {
			t.Errorf("key %q != id %q", msg.Key, p.ID)
		}
	}

	// Metrics reflect the same counts.
	if got := counterVal(t, m.Processed); got != 2 {
		t.Errorf("processed metric = %v, want 2", got)
	}
	if got := counterVal(t, m.Consumed); got != 6 {
		t.Errorf("consumed metric = %v, want 6", got)
	}
}

// TestRun_ProducePropagatesError verifies at-least-once safety: if producing to
// news.processed fails, run returns the error and the source offset is NOT
// committed, so the item is redelivered.
func TestRun_ProducePropagatesError(t *testing.T) {
	longBody := "this is a sufficiently long synthetic body for the processor test"
	records := []kafka.Record{rawRecord(t, "1", "Alpha", longBody, "en")}
	consumer := kafka.NewFakeConsumer(records)
	m := metrics.NewProcessor(prometheus.NewRegistry())

	_, err := run(context.Background(), testConfig(), consumer, failProducer{}, m)
	if err == nil {
		t.Fatal("expected run to return produce error so the offset is retried")
	}
	if len(consumer.Committed) != 0 {
		t.Errorf("offset must not be committed on produce failure, committed=%d", len(consumer.Committed))
	}
}

type failProducer struct{}

func (failProducer) Produce(context.Context, []byte, []byte) error { return kafka.ErrFakeProduce }
func (failProducer) Close() error                                  { return nil }
