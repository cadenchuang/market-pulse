// Command ingestor is the Go ingestion service. In Phase 2 it streams a bundled
// synthetic JSONL dataset (replay mode) to the Kafka topic news.raw, using
// goroutine fan-out with channel backpressure and context-based graceful
// shutdown. Live GDELT/RSS sources are added in Phase 8; replay stays the
// Docker default.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg := parseFlags()

	// Graceful shutdown: SIGINT/SIGTERM cancels ctx, which drains the pipeline.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	m := metrics.NewIngestor(reg)
	metricsSrv := metrics.Serve(cfg.metricsAddr, reg)
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
	}()

	producer := kafka.NewSegmentioProducer(cfg.brokers, cfg.topic)
	defer producer.Close()

	log.Printf("ingestor starting: mode=%s workers=%d brokers=%v topic=%s metrics=%s",
		cfg.mode, cfg.workers, cfg.brokers, cfg.topic, cfg.metricsAddr)
	switch cfg.mode {
	case "replay":
		log.Printf("  replay: file=%s rate=%.1f/s loop=%t", cfg.file, cfg.rate, cfg.loop)
	default:
		log.Printf("  live: gdelt_query=%q rss_feeds=%d poll_interval=%s", cfg.gdeltQuery, len(cfg.rssFeeds), cfg.pollInterval)
	}

	produced, err := run(ctx, cfg, producer, m)
	if err != nil {
		log.Fatalf("ingestor failed after %d messages: %v", produced, err)
	}
	log.Printf("ingestor stopped cleanly: produced %d messages to %q", produced, cfg.topic)
}

func parseFlags() config {
	var (
		brokers = flag.String("brokers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"), "comma-separated Kafka bootstrap servers")
		topic   = flag.String("topic", env("KAFKA_TOPIC_RAW", "news.raw"), "topic to produce raw items to")
		mode    = flag.String("mode", env("INGEST_MODE", "replay"), "ingestion mode: replay | gdelt | rss | live")
		file    = flag.String("file", env("REPLAY_PATH", "data/sample/news_sample.jsonl"), "replay JSONL dataset path")
		rate    = flag.Float64("rate", envFloat("REPLAY_RATE_PER_SEC", 50), "replay items/sec (<=0 = unlimited)")
		workers = flag.Int("workers", 4, "number of producer goroutines")
		buffer  = flag.Int("buffer", 256, "bounded channel size (backpressure)")
		loop    = flag.Bool("loop", false, "restart the dataset when it ends (replay only)")
		metrics = flag.String("metrics-addr", ":"+env("METRICS_PORT_INGESTOR", "9101"), "address for the Prometheus /metrics endpoint")

		gdeltQuery = flag.String("gdelt-query", env("GDELT_QUERY", ""), "GDELT Doc 2.0 query (e.g. '(stocks OR earnings) sourcelang:eng')")
		gdeltMax   = flag.Int("gdelt-max", envInt("GDELT_MAX_RECORDS", 75), "max GDELT articles per poll")
		rssFeeds   = flag.String("rss-feeds", env("RSS_FEEDS", ""), "comma-separated RSS/Atom feed URLs")
		pollEvery  = flag.Duration("poll-interval", envDuration("POLL_INTERVAL", 60*time.Second), "delay between live-source polls (rate limit)")
	)
	flag.Parse()

	return config{
		brokers:      splitBrokers(*brokers),
		topic:        *topic,
		mode:         *mode,
		file:         *file,
		rate:         *rate,
		workers:      *workers,
		bufferSize:   *buffer,
		loop:         *loop,
		metricsAddr:  *metrics,
		gdeltQuery:   *gdeltQuery,
		gdeltMax:     *gdeltMax,
		rssFeeds:     splitBrokers(*rssFeeds),
		pollInterval: *pollEvery,
	}
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
