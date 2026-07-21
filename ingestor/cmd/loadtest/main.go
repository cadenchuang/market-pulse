// Command loadtest is the Phase 7 --stress load generator. It floods news.raw
// with synthetic, unique news items as fast as the brokers and worker pool
// allow, so you can watch consumer lag, throughput, inference latency, and
// memory climb under sustained load in Grafana.
//
// Every generated item has a unique id and randomized body, so nothing is
// dropped by the processor's dedup/filter — the pressure propagates all the way
// through to the Python inference workers.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type config struct {
	brokers     []string
	topic       string
	workers     int
	rate        float64 // items/sec across all workers; <=0 = unlimited
	duration    time.Duration
	count       int64 // max items; <=0 = unlimited
	metricsAddr string
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	reg := metrics.NewRegistry()
	produced := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: "market_pulse", Subsystem: "loadtest", Name: "produced_total",
		Help: "Synthetic messages flooded into news.raw.",
	})
	errCtr := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: "market_pulse", Subsystem: "loadtest", Name: "errors_total",
		Help: "Failed produce attempts.",
	})
	metricsSrv := metrics.Serve(cfg.metricsAddr, reg)
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
	}()

	producer := kafka.NewSegmentioProducer(cfg.brokers, cfg.topic)
	defer producer.Close()

	log.Printf("loadtest starting: workers=%d rate=%.0f/s duration=%s count=%d brokers=%v topic=%s metrics=%s",
		cfg.workers, cfg.rate, cfg.duration, cfg.count, cfg.brokers, cfg.topic, cfg.metricsAddr)

	total := run(ctx, cfg, producer, produced, errCtr)
	log.Printf("loadtest stopped: flooded %d messages to %q", total, cfg.topic)
}

// run fans out cfg.workers goroutines that each generate and produce synthetic
// items. A shared token ticker enforces an aggregate rate limit when cfg.rate>0.
func run(ctx context.Context, cfg config, producer kafka.Producer, produced, errCtr prometheus.Counter) int64 {
	if cfg.workers < 1 {
		cfg.workers = 1
	}

	var ticker *time.Ticker
	var tick <-chan time.Time
	if cfg.rate > 0 {
		ticker = time.NewTicker(time.Duration(float64(time.Second) / cfg.rate))
		defer ticker.Stop()
		tick = ticker.C
	}

	var total int64
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				if cfg.count > 0 && atomic.LoadInt64(&total) >= cfg.count {
					return
				}
				if tick != nil {
					select {
					case <-ctx.Done():
						return
					case <-tick:
					}
				} else if ctx.Err() != nil {
					return
				}

				n := atomic.AddInt64(&total, 1)
				if cfg.count > 0 && n > cfg.count {
					atomic.AddInt64(&total, -1)
					return
				}
				item := synthItem(rng, n)
				value, err := json.Marshal(item)
				if err != nil {
					errCtr.Inc()
					continue
				}
				if err := producer.Produce(ctx, item.Key(), value); err != nil {
					if ctx.Err() != nil {
						return
					}
					errCtr.Inc()
					continue
				}
				produced.Inc()
			}
		}(time.Now().UnixNano() + int64(w))
	}
	wg.Wait()
	return atomic.LoadInt64(&total)
}

var loremWords = strings.Fields(
	"markets rally slump surge plunge earnings guidance profit loss revenue " +
		"regulator lawsuit acquisition merger dividend buyback downgrade upgrade " +
		"inflation rates fed hike cut outlook forecast quarter growth demand supply")

// synthItem builds a unique, filter-passing NewsRaw. The random body guarantees
// a distinct content hash so the processor never dedups it away.
func synthItem(rng *rand.Rand, n int64) contract.NewsRaw {
	var b strings.Builder
	for i := 0; i < 30; i++ {
		b.WriteString(loremWords[rng.Intn(len(loremWords))])
		b.WriteByte(' ')
	}
	now := time.Now().UTC()
	return contract.NewsRaw{
		SchemaVersion: contract.SchemaVersion,
		ID:            fmt.Sprintf("stress-%d-%d", now.UnixNano(), n),
		Source:        contract.SourceReplay,
		Title:         fmt.Sprintf("Synthetic stress headline %d", n),
		Body:          strings.TrimSpace(b.String()),
		Language:      "en",
		PublishedAt:   now,
		IngestedAt:    now,
	}
}

func parseFlags() config {
	var (
		brokers  = flag.String("brokers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"), "comma-separated Kafka bootstrap servers")
		topic    = flag.String("topic", env("KAFKA_TOPIC_RAW", "news.raw"), "topic to flood")
		workers  = flag.Int("workers", 8, "number of producer goroutines")
		rate     = flag.Float64("rate", 0, "aggregate items/sec (0 = unlimited flood)")
		duration = flag.Duration("duration", 60*time.Second, "how long to run (0 = until interrupted)")
		count    = flag.Int64("count", 0, "max items to produce (0 = unlimited)")
		metrics  = flag.String("metrics-addr", ":"+env("METRICS_PORT_LOADTEST", "9106"), "address for the Prometheus /metrics endpoint")
	)
	flag.Parse()

	return config{
		brokers:     splitBrokers(*brokers),
		topic:       *topic,
		workers:     *workers,
		rate:        *rate,
		duration:    *duration,
		count:       *count,
		metricsAddr: *metrics,
	}
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
