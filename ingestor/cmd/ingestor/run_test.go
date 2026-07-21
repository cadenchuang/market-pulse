package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// testMetrics returns an Ingestor recorder backed by a throwaway registry.
func testMetrics() *metrics.Ingestor {
	return metrics.NewIngestor(prometheus.NewRegistry())
}

func writeDataset(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "news.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for i := 0; i < n; i++ {
		item := contract.NewsRaw{
			SchemaVersion: contract.SchemaVersion,
			ID:            "replay-" + itoa(i),
			Source:        contract.SourceReplay,
			Title:         "Synthetic headline " + itoa(i),
			Body:          "Synthetic body " + itoa(i),
			PublishedAt:   mustTime("2026-07-20T12:00:00Z"),
			IngestedAt:    mustTime("2026-07-20T12:00:00Z"),
		}
		if err := enc.Encode(item); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	return path
}

// TestRun_MessagesLand verifies end-to-end that every dataset item is produced
// to the (fake) broker as valid contract JSON — the code-level equivalent of
// Phase 2's "verify messages land", without needing a real Kafka.
func TestRun_MessagesLand(t *testing.T) {
	const n = 200
	path := writeDataset(t, n)
	fake := kafka.NewFakeProducer()

	cfg := config{
		mode:       "replay",
		file:       path,
		rate:       0,
		workers:    4,
		bufferSize: 64,
	}
	produced, err := run(context.Background(), cfg, fake, testMetrics())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if produced != n {
		t.Fatalf("produced = %d, want %d", produced, n)
	}

	msgs := fake.Messages()
	if len(msgs) != n {
		t.Fatalf("captured %d messages, want %d", len(msgs), n)
	}

	seen := map[string]bool{}
	for _, m := range msgs {
		var item contract.NewsRaw
		if err := json.Unmarshal(m.Value, &item); err != nil {
			t.Fatalf("message not valid contract JSON: %v", err)
		}
		if err := item.Validate(); err != nil {
			t.Fatalf("produced message fails contract validation: %v", err)
		}
		if string(m.Key) != item.ID {
			t.Fatalf("message key %q != id %q", m.Key, item.ID)
		}
		seen[item.ID] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique ids, got %d", n, len(seen))
	}
}

func TestRun_UnsupportedModeErrors(t *testing.T) {
	_, err := run(context.Background(), config{mode: "bogus"}, kafka.NewFakeProducer(), testMetrics())
	if err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}

func TestBuildSources(t *testing.T) {
	// replay is always available.
	if srcs, err := buildSources(config{mode: "replay", file: "x"}); err != nil || len(srcs) != 1 {
		t.Fatalf("replay: got %d srcs, err=%v", len(srcs), err)
	}
	// gdelt requires a query.
	if _, err := buildSources(config{mode: "gdelt"}); err == nil {
		t.Fatal("gdelt without query should error")
	}
	if srcs, err := buildSources(config{mode: "gdelt", gdeltQuery: "stocks"}); err != nil || len(srcs) != 1 {
		t.Fatalf("gdelt: got %d srcs, err=%v", len(srcs), err)
	}
	// rss requires feeds.
	if _, err := buildSources(config{mode: "rss"}); err == nil {
		t.Fatal("rss without feeds should error")
	}
	// live fans in both configured sources.
	srcs, err := buildSources(config{mode: "live", gdeltQuery: "stocks", rssFeeds: []string{"http://f"}})
	if err != nil || len(srcs) != 2 {
		t.Fatalf("live: got %d srcs, err=%v", len(srcs), err)
	}
	if _, err := buildSources(config{mode: "live"}); err == nil {
		t.Fatal("live with nothing configured should error")
	}
	if _, err := buildSources(config{mode: "bogus"}); err == nil {
		t.Fatal("bogus mode should error")
	}
}

func TestRun_PropagatesProduceError(t *testing.T) {
	path := writeDataset(t, 10)
	fake := kafka.NewFakeProducer()
	fake.FailAfter = 3 // fail some produces; run should still finish without hanging

	cfg := config{mode: "replay", file: path, workers: 2, bufferSize: 8}
	produced, err := run(context.Background(), cfg, fake, testMetrics())
	if err != nil {
		t.Fatalf("run should not fatal on per-message produce errors: %v", err)
	}
	if produced == 0 {
		t.Fatal("expected some messages to be produced before failures")
	}
}
