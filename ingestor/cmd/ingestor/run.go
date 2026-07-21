package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/cadenchuang/market-pulse/ingestor/internal/sources"
)

// config holds the resolved ingestor configuration (flags with env fallbacks).
type config struct {
	brokers     []string
	topic       string
	mode        string
	file        string
	rate        float64
	workers     int
	bufferSize  int
	loop        bool
	metricsAddr string

	// Live-source config (Phase 8).
	gdeltQuery   string
	gdeltMax     int
	rssFeeds     []string
	pollInterval time.Duration
}

// buildSources resolves the configured mode into the set of ingestion sources to
// run. Modes:
//
//	replay        - stream the bundled JSONL dataset (offline default)
//	gdelt         - poll the GDELT Doc 2.0 API
//	rss           - poll the configured RSS/Atom feeds
//	live          - gdelt + rss together (whatever is configured)
//
// Multiple sources fan in onto the same channel in run().
func buildSources(cfg config) ([]sources.Source, error) {
	switch cfg.mode {
	case "replay":
		return []sources.Source{
			&sources.Replay{Path: cfg.file, RatePerSec: cfg.rate, Loop: cfg.loop},
		}, nil
	case "gdelt":
		if cfg.gdeltQuery == "" {
			return nil, fmt.Errorf("mode gdelt requires a query (-gdelt-query / GDELT_QUERY)")
		}
		return []sources.Source{gdeltSource(cfg)}, nil
	case "rss":
		if len(cfg.rssFeeds) == 0 {
			return nil, fmt.Errorf("mode rss requires feeds (-rss-feeds / RSS_FEEDS)")
		}
		return []sources.Source{rssSource(cfg)}, nil
	case "live":
		var srcs []sources.Source
		if cfg.gdeltQuery != "" {
			srcs = append(srcs, gdeltSource(cfg))
		}
		if len(cfg.rssFeeds) > 0 {
			srcs = append(srcs, rssSource(cfg))
		}
		if len(srcs) == 0 {
			return nil, fmt.Errorf("mode live requires a GDELT query and/or RSS feeds")
		}
		return srcs, nil
	default:
		return nil, fmt.Errorf("mode %q not supported (use: replay | gdelt | rss | live)", cfg.mode)
	}
}

func gdeltSource(cfg config) sources.Source {
	return &sources.GDELT{
		Query:        cfg.gdeltQuery,
		MaxRecords:   cfg.gdeltMax,
		PollInterval: cfg.pollInterval,
	}
}

func rssSource(cfg config) sources.Source {
	return &sources.RSS{
		Feeds:        cfg.rssFeeds,
		PollInterval: cfg.pollInterval,
	}
}

// run wires the ingestion pipeline and blocks until the source is exhausted or
// ctx is cancelled. The design mirrors the spec's concurrency model:
//
//	replay source ──(bounded channel = backpressure)──► N producer workers ──► Kafka
//
// The source is one goroutine; a pool of worker goroutines fan out onto the
// Producer. The bounded channel is the backpressure signal: if Kafka slows down,
// workers stop draining, the channel fills, and the source blocks. ctx cancels
// every stage for a clean, leak-free shutdown.
//
// The Producer is injected so tests can substitute a fake broker.
func run(ctx context.Context, cfg config, producer kafka.Producer, m *metrics.Ingestor) (int64, error) {
	srcs, err := buildSources(cfg)
	if err != nil {
		return 0, err
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}

	out := make(chan contract.NewsRaw, cfg.bufferSize)

	// Source stage: N sources fan in onto one channel. The last source to finish
	// closes it, so the producer workers drain and exit cleanly.
	srcErrCh := make(chan error, len(srcs))
	var srcWG sync.WaitGroup
	for _, s := range srcs {
		srcWG.Add(1)
		go func(s sources.Source) {
			defer srcWG.Done()
			if err := s.Run(ctx, out); err != nil {
				srcErrCh <- fmt.Errorf("source %s: %w", s.Name(), err)
			}
		}(s)
	}
	go func() {
		srcWG.Wait()
		close(out)
		close(srcErrCh)
	}()

	// Producer stage: fan out onto Kafka.
	var produced int64
	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range out {
				value, err := json.Marshal(item)
				if err != nil {
					log.Printf("marshal error id=%s: %v", item.ID, err)
					m.Errors.Inc()
					continue
				}
				if err := producer.Produce(ctx, item.Key(), value); err != nil {
					if ctx.Err() != nil {
						return // shutting down
					}
					log.Printf("produce error id=%s: %v", item.ID, err)
					m.Errors.Inc()
					continue
				}
				atomic.AddInt64(&produced, 1)
				m.Produced.Inc()
			}
		}()
	}
	wg.Wait()

	// Producer workers only exit once out is closed, which happens after every
	// source goroutine returns — so srcErrCh is closed here. Surface the first
	// real (non-cancellation) source error, if any.
	for srcErr := range srcErrCh {
		if srcErr != nil && !errors.Is(srcErr, context.Canceled) && !errors.Is(srcErr, context.DeadlineExceeded) {
			return produced, srcErr
		}
	}
	return produced, nil
}
