# observability/ (Prometheus + Grafana as code)

Provisioned-as-code monitoring for the whole pipeline. Nothing here needs manual
clicking — `docker compose up` brings up Prometheus and Grafana already wired to
the datasource and dashboard.

```
observability/
├── prometheus.yml                              # scrape configs: both Go stages + every Python worker
└── grafana/
    ├── provisioning/
    │   ├── datasources/datasource.yml          # Prometheus datasource (uid: prometheus)
    │   └── dashboards/dashboards.yml           # dashboard file provider
    └── dashboards/market-pulse.json            # the pipeline dashboard
```

## What is scraped

Every stage exposes a Prometheus `/metrics` endpoint with the standard Go/Python
runtime collectors (memory, CPU, goroutines/GC, open FDs) plus its own metrics:

| Stage      | Target(s)                                  | Key metrics |
|------------|--------------------------------------------|-------------|
| ingestor   | `ingestor:9101`                            | `market_pulse_ingestor_produced_total`, `..._produce_errors_total` |
| processor  | `processor:9102`                           | `market_pulse_processor_{consumed,processed,duplicates,filtered,errors}_total`, `..._consumer_lag` |
| inference  | `inference:9103` … `9105` (one per worker) | `market_pulse_inference_latency_seconds` (histogram), `..._processed_total{label}`, `..._errors_total`, `..._consumer_lag` |
| loadtest   | `loadtest:9106` (only with `--profile stress`) | `market_pulse_loadtest_{produced,errors}_total` |

Memory per instance comes from `process_resident_memory_bytes` (Linux/containers).

## Dashboard

`Market Pulse Pipeline` (folder *Market Pulse*) has panels for produce rate,
processor throughput (consumed vs processed vs dropped), **consumer lag per
stage** (the backpressure signal), inference latency p50/p95/p99, throughput by
sentiment, error rate per stage, resident memory per instance, and Go goroutine
counts.

## Open the UIs

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000> (anonymous viewer; `admin`/`admin` to edit)

## Load test

See [`../loadtest/README.md`](../loadtest/README.md). Run the flood, then watch
lag and memory climb on the dashboard:

```bash
docker compose --profile stress up loadtest
```
