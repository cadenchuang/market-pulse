package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/process"
	"github.com/prometheus/client_golang/prometheus"
)

func counters() (prometheus.Counter, prometheus.Counter) {
	reg := prometheus.NewRegistry()
	p := prometheus.NewCounter(prometheus.CounterOpts{Name: "p"})
	e := prometheus.NewCounter(prometheus.CounterOpts{Name: "e"})
	reg.MustRegister(p, e)
	return p, e
}

// TestRun_FloodsUniqueValidItems verifies the generator produces the requested
// count of items that (a) validate against the contract and (b) survive the
// processor's dedup+filter, so stress reaches the whole pipeline.
func TestRun_FloodsUniqueValidItems(t *testing.T) {
	const n = 500
	fake := kafka.NewFakeProducer()
	p, e := counters()

	cfg := config{workers: 4, count: n, duration: 0}
	total := run(context.Background(), cfg, fake, p, e)
	if total != n {
		t.Fatalf("total = %d, want %d", total, n)
	}

	msgs := fake.Messages()
	if len(msgs) != n {
		t.Fatalf("captured %d messages, want %d", len(msgs), n)
	}

	filterCfg := process.DefaultFilterConfig()
	ids := map[string]bool{}
	hashes := map[string]bool{}
	for _, m := range msgs {
		var item contract.NewsRaw
		if err := json.Unmarshal(m.Value, &item); err != nil {
			t.Fatalf("invalid contract JSON: %v", err)
		}
		if err := item.Validate(); err != nil {
			t.Fatalf("item fails contract validation: %v", err)
		}
		if ids[item.ID] {
			t.Fatalf("duplicate id produced: %s", item.ID)
		}
		ids[item.ID] = true

		normTitle := process.Normalize(item.Title)
		normBody := process.Normalize(item.Body)
		if keep, reason := process.Filter(normTitle, normBody, item.Language, filterCfg); !keep {
			t.Fatalf("synthetic item dropped by filter: %s", reason)
		}
		hash := process.ContentHash(normTitle, normBody)
		if hashes[hash] {
			t.Fatalf("duplicate content hash would be deduped away: %s", hash)
		}
		hashes[hash] = true
	}
}

// TestRun_RespectsDeadline ensures duration-based shutdown works without count.
func TestRun_RespectsDeadline(t *testing.T) {
	fake := kafka.NewFakeProducer()
	p, e := counters()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	cfg := config{workers: 2, rate: 200}
	total := run(ctx, cfg, fake, p, e)
	if total == 0 {
		t.Fatal("expected some messages before deadline")
	}
}
