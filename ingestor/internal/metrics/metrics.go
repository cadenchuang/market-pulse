// Package metrics exposes Prometheus collectors for the Go stream stages. Phase 3
// wires the processor's throughput and consumer-lag metrics; Phase 7 adds the
// ingestor's metrics, Prometheus scrape config, and Grafana dashboards.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry returns a registry preloaded with the Go runtime and process
// collectors, so every stage exports memory (process_resident_memory_bytes),
// goroutine counts, GC stats, etc. alongside its own metrics.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Ingestor holds the collectors for the Go ingestion stage.
type Ingestor struct {
	Produced prometheus.Counter
	Errors   prometheus.Counter
}

// NewIngestor registers the ingestion-stage collectors on reg.
func NewIngestor(reg prometheus.Registerer) *Ingestor {
	f := promauto.With(reg)
	const ns, sub = "market_pulse", "ingestor"
	return &Ingestor{
		Produced: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "produced_total",
			Help: "Messages produced to news.raw.",
		}),
		Errors: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "produce_errors_total",
			Help: "Failed produce attempts to news.raw.",
		}),
	}
}

// Processor holds the collectors for the Go processing stage. Counters are used
// with rate() in Prometheus to get throughput; consumer_lag is the backpressure
// signal.
type Processor struct {
	Consumed   prometheus.Counter
	Processed  prometheus.Counter
	Duplicates prometheus.Counter
	Filtered   *prometheus.CounterVec
	Errors     prometheus.Counter
	Lag        prometheus.Gauge
}

// NewProcessor registers the processing-stage collectors on reg.
func NewProcessor(reg prometheus.Registerer) *Processor {
	f := promauto.With(reg)
	const ns, sub = "market_pulse", "processor"
	return &Processor{
		Consumed: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "consumed_total",
			Help: "Messages consumed from news.raw.",
		}),
		Processed: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "processed_total",
			Help: "Clean, unique messages produced to news.processed.",
		}),
		Duplicates: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "duplicates_total",
			Help: "Messages dropped as duplicate content.",
		}),
		Filtered: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "filtered_total",
			Help: "Messages dropped by the filter, by reason.",
		}, []string{"reason"}),
		Errors: f.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "errors_total",
			Help: "Messages that failed to decode or validate.",
		}),
		Lag: f.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "consumer_lag",
			Help: "Current consumer lag on news.raw (backpressure signal).",
		}),
	}
}

// Server wraps an HTTP server exposing /metrics for a registry.
type Server struct {
	http *http.Server
}

// Serve starts an HTTP server exposing /metrics on addr in a background
// goroutine. Call Shutdown to stop it.
func Serve(addr string, reg *prometheus.Registry) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Metrics are non-fatal; the pipeline keeps running if the endpoint
			// cannot bind.
			return
		}
	}()
	return &Server{http: srv}
}

// Shutdown gracefully stops the metrics server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}
