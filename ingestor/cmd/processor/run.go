package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/cadenchuang/market-pulse/ingestor/internal/process"
)

// config holds the resolved processor configuration.
type config struct {
	brokers        []string
	rawTopic       string
	processedTopic string
	groupID        string
	dedupSize      int
	filter         process.FilterConfig
	metricsAddr    string
}

// stats is a run summary, returned for logging and asserted in tests.
type stats struct {
	consumed   int64
	processed  int64
	duplicates int64
	filtered   int64
	errors     int64
}

// run is the processing loop: consume news.raw as a group, normalize/dedup/
// filter, produce clean unique items to news.processed, and commit offsets
// only after the output is produced (at-least-once). It returns when the
// consumer is closed (io.EOF) or ctx is cancelled.
//
// It is single-goroutine on purpose: one consumer, ordered offset commits.
// Horizontal scaling is by running more processor replicas in the same consumer
// group across the topic's partitions.
func run(ctx context.Context, cfg config, consumer kafka.Consumer, producer kafka.Producer, m *metrics.Processor) (stats, error) {
	proc := process.NewProcessor(cfg.dedupSize, cfg.filter)
	var s stats

	for {
		rec, err := consumer.Fetch(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return s, nil // clean shutdown
			}
			return s, err
		}
		s.consumed++
		m.Consumed.Inc()

		var raw contract.NewsRaw
		if err := json.Unmarshal(rec.Value, &raw); err != nil {
			log.Printf("skip: decode error at offset %d: %v", rec.Offset, err)
			s.errors++
			m.Errors.Inc()
			if err := commit(ctx, consumer, rec, m); err != nil {
				return s, err
			}
			continue
		}
		if err := raw.Validate(); err != nil {
			log.Printf("skip: invalid raw item id=%s: %v", raw.ID, err)
			s.errors++
			m.Errors.Inc()
			if err := commit(ctx, consumer, rec, m); err != nil {
				return s, err
			}
			continue
		}

		out, decision, reason := proc.Process(raw, time.Now().UTC())
		switch decision {
		case process.DecisionEmit:
			value, err := json.Marshal(out)
			if err != nil {
				log.Printf("skip: marshal error id=%s: %v", out.ID, err)
				s.errors++
				m.Errors.Inc()
				break
			}
			if err := producer.Produce(ctx, out.Key(), value); err != nil {
				if ctx.Err() != nil {
					return s, nil
				}
				// Do NOT commit: leave the offset so the item is retried.
				return s, err
			}
			s.processed++
			m.Processed.Inc()
		case process.DecisionDuplicate:
			s.duplicates++
			m.Duplicates.Inc()
		case process.DecisionFiltered:
			s.filtered++
			m.Filtered.WithLabelValues(reason).Inc()
		}

		if err := commit(ctx, consumer, rec, m); err != nil {
			return s, err
		}
	}
}

func commit(ctx context.Context, consumer kafka.Consumer, rec kafka.Record, m *metrics.Processor) error {
	if err := consumer.Commit(ctx, rec); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	m.Lag.Set(float64(consumer.Lag()))
	return nil
}
