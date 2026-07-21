// Command processor is the Go processing consumer group. It consumes news.raw,
// dedups by content hash, normalizes, and filters, then produces clean unique
// items to news.processed. This cheap high-throughput stage exists so the
// expensive Python model never runs on duplicate or irrelevant items. It
// exports Prometheus throughput and consumer-lag metrics.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/kafka"
	"github.com/cadenchuang/market-pulse/ingestor/internal/metrics"
	"github.com/cadenchuang/market-pulse/ingestor/internal/process"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	m := metrics.NewProcessor(reg)
	metricsSrv := metrics.Serve(cfg.metricsAddr, reg)
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
	}()

	consumer := kafka.NewSegmentioConsumer(cfg.brokers, cfg.groupID, cfg.rawTopic)
	defer consumer.Close()
	producer := kafka.NewSegmentioProducer(cfg.brokers, cfg.processedTopic)
	defer producer.Close()

	log.Printf("processor starting: brokers=%v group=%s %s->%s dedup=%d metrics=%s",
		cfg.brokers, cfg.groupID, cfg.rawTopic, cfg.processedTopic, cfg.dedupSize, cfg.metricsAddr)

	s, err := run(ctx, cfg, consumer, producer, m)
	if err != nil {
		log.Fatalf("processor failed: %v (consumed=%d processed=%d dup=%d filtered=%d err=%d)",
			err, s.consumed, s.processed, s.duplicates, s.filtered, s.errors)
	}
	log.Printf("processor stopped cleanly: consumed=%d processed=%d dup=%d filtered=%d err=%d",
		s.consumed, s.processed, s.duplicates, s.filtered, s.errors)
}

func parseFlags() config {
	var (
		brokers     = flag.String("brokers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"), "comma-separated Kafka bootstrap servers")
		rawTopic    = flag.String("raw-topic", env("KAFKA_TOPIC_RAW", "news.raw"), "source topic")
		procTopic   = flag.String("processed-topic", env("KAFKA_TOPIC_PROCESSED", "news.processed"), "destination topic")
		group       = flag.String("group", env("KAFKA_CONSUMER_GROUP_PROCESSOR", "market-pulse-processor"), "consumer group id")
		dedupSize   = flag.Int("dedup-size", 100_000, "max content hashes retained for dedup")
		minBody     = flag.Int("min-body-len", 20, "drop items whose normalized body is shorter than this")
		language    = flag.String("language", "en", "keep only this language (empty = any)")
		metricsAddr = flag.String("metrics-addr", ":"+env("METRICS_PORT_PROCESSOR", "9102"), "address for the Prometheus /metrics endpoint")
	)
	flag.Parse()

	return config{
		brokers:        splitBrokers(*brokers),
		rawTopic:       *rawTopic,
		processedTopic: *procTopic,
		groupID:        *group,
		dedupSize:      *dedupSize,
		filter:         process.FilterConfig{MinBodyLen: *minBody, Language: *language},
		metricsAddr:    *metricsAddr,
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
