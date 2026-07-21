"""Prometheus metrics for the inference workers.

``prometheus_client`` is imported lazily; when it is unavailable the worker uses
a no-op metrics object, so the pipeline (and tests) run without the dependency.
Phase 7 adds the scrape config and Grafana dashboards.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class NoOpMetrics:
    """Metrics interface with zero dependencies; every method is a no-op."""

    def observe_latency(self, seconds: float) -> None:
        ...

    def inc_processed(self, label: str) -> None:
        ...

    def inc_error(self) -> None:
        ...

    def set_lag(self, lag: int) -> None:
        ...

    def serve(self, port: int) -> None:
        ...


class PrometheusMetrics:
    """Real metrics backed by prometheus_client (one registry per process)."""

    def __init__(self, worker_index: int):
        from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

        self._start_http_server = start_http_server
        self.registry = CollectorRegistry()
        labels = {"worker": str(worker_index)}

        # Process + platform + GC collectors give memory
        # (process_resident_memory_bytes), CPU, open FDs, and Python GC stats on
        # this custom registry. They are Linux-populated (containers); on macOS
        # they register harmlessly with no samples.
        self._register_runtime_collectors()

        self._latency = Histogram(
            "market_pulse_inference_latency_seconds",
            "Model inference latency per item.",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
            registry=self.registry,
        )
        self._processed = Counter(
            "market_pulse_inference_processed_total",
            "Items scored, by sentiment label.",
            ["label"],
            registry=self.registry,
        )
        self._errors = Counter(
            "market_pulse_inference_errors_total",
            "Items that failed to decode or score.",
            registry=self.registry,
        )
        self._lag = Gauge(
            "market_pulse_inference_consumer_lag",
            "Consumer lag on news.processed for this worker (backpressure signal).",
            registry=self.registry,
        )
        # Const label to distinguish worker processes when scraped on separate ports.
        self._worker = labels

    def _register_runtime_collectors(self) -> None:
        try:
            from prometheus_client import (
                GCCollector,
                PlatformCollector,
                ProcessCollector,
            )

            ProcessCollector(registry=self.registry)
            PlatformCollector(registry=self.registry)
            GCCollector(registry=self.registry)
        except Exception as exc:  # pragma: no cover - depends on platform
            logger.debug("runtime collectors unavailable: %s", exc)

    def observe_latency(self, seconds: float) -> None:
        self._latency.observe(seconds)

    def inc_processed(self, label: str) -> None:
        self._processed.labels(label=label).inc()

    def inc_error(self) -> None:
        self._errors.inc()

    def set_lag(self, lag: int) -> None:
        self._lag.set(lag)

    def serve(self, port: int) -> None:
        self._start_http_server(port, registry=self.registry)


def build_metrics(worker_index: int):
    """Return a Prometheus metrics object, or a no-op if the dep is missing."""
    try:
        return PrometheusMetrics(worker_index)
    except Exception as exc:  # pragma: no cover - depends on environment
        logger.warning("prometheus_client unavailable (%s); metrics disabled", exc)
        return NoOpMetrics()
