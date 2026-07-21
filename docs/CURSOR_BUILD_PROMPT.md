# Cursor Build Prompt — Market Pulse (Go stream layer + Python inference)

> Paste this into Cursor's Agent (Composer). Build it **phase by phase** — don't let the agent one-shot the whole thing. Review and run each phase before moving on.

---

## Project spec

Build **Market Pulse**, a real-time financial news sentiment stream processor with a **Go stream-processing layer** and a **Python inference layer**, connected through **Kafka**:

- A **Go ingestion service** consumes live news (GDELT + RSS, or a bundled replay dataset) using goroutines/channels and produces raw items to Kafka `news.raw`.
- A **Go processing consumer group** (`confluent-kafka-go`) reads `news.raw`, dedups/normalizes/filters, and republishes clean unique items to `news.processed`.
- A **Python inference consumer group** reads `news.processed` and runs INT8-quantized FinBERT ONNX workers to extract sentiment + entities.
- Results land in a time-series store; a heuristic **"sentiment shock"** signal is computed from rolling sentiment velocity.
- **Prometheus + Grafana** provide observability; a `--stress` load generator floods the pipeline. A **Streamlit** dashboard visualizes sentiment and flagged shocks.

**This is a public portfolio project.** Optimize for clean architecture, reproducibility (runs offline with zero keys via replay mode), correct concurrency, real measured benchmarks, and honest claims. Do **not** invent benchmarks, claim ML "prediction," or claim edge-hardware deployment.

### Language boundary (deliberate — document it)
- **Go** owns all stream plumbing: concurrent ingestion, Kafka producing, and a Kafka consumer group doing cheap high-throughput processing (dedup, normalize, filter). Goroutines/channels are the idiomatic tool for I/O-bound fan-out and backpressure.
- **Python** owns model inference only, because that's where the quantized-ONNX-FinBERT + tokenizer ecosystem lives.
- **Kafka is the language boundary.** The two languages never call each other directly — they hand off through topics. Explain this rationale in the README and code comments.

### Non-negotiable design decisions
- **Two-stage Kafka pipeline.** `news.raw` (Go produces) → Go processing consumer group → `news.processed` (Go produces) → Python inference consumer group → DB. The Go processing stage exists so the expensive model never runs on duplicate/irrelevant items.
- **Go concurrency + backpressure.** Ingestion uses goroutines with channel fan-in and `context` cancellation. Both Go stages are backpressure-aware; Kafka consumer lag is the backpressure signal and must be exported as a metric.
- **Python inference uses processes, not threads.** The GIL prevents thread parallelism on CPU-bound work, so the inference service runs N worker processes, each with its own ONNX session, as a Kafka consumer group scaling with partitions. Comment on this.
- **Replay mode is mandatory and is the Docker default.** Bundle a synthetic ~200-item JSONL dataset (`data/sample/news_sample.jsonl`, clearly synthetic, no copyrighted article text). The Go ingestion service streams it to `news.raw` at a configurable rate so the whole system runs offline with no keys. `docker compose up` must just work.
- **Quantization is real and benchmarked.** Export `ProsusAI/finbert` to ONNX, apply dynamic INT8 quantization via ONNX Runtime. `benchmarks/bench_inference.py` measures p50/p95 latency + throughput for FP32 vs INT8 on CPU and writes results to a file. Never hardcode numbers.
- **"Shock" is a heuristic, not a prediction.** Rolling z-score of per-entity sentiment velocity crossing a configurable threshold. Label it as a heuristic everywhere.
- **Observability + load test.** Go and Python services both expose Prometheus `/metrics` (throughput, consumer lag per stage, inference latency, memory). Grafana dashboard provisioned as code. `--stress` floods the pipeline to show sustained-load and memory behavior.
- **Secrets hygiene.** No keys in code. `.env` + example file, `.env` in `.gitignore`.

### Tech stack
- **Go 1.22+**: `net/http`, `encoding/json`, `github.com/confluentinc/confluent-kafka-go/v2/kafka` (producer + consumer groups; note: CGo/librdkafka — install in the Go Dockerfile), goroutines/channels, `context`, `prometheus/client_golang`.
  - Fallback if CGo/librdkafka is painful in Docker: `github.com/segmentio/kafka-go` (pure Go). Keep the Kafka client behind a small interface so this is swappable.
- **Python 3.11+**: `confluent-kafka` or `aiokafka` (inference consumer), `transformers`, `optimum[onnxruntime]` / `onnxruntime`, `sqlalchemy`, `prometheus_client`, `pytest`.
- **Kafka** in KRaft mode (single broker in compose; partitioned `news.raw` and `news.processed` topics).
- **Storage**: SQLite default via SQLAlchemy; Postgres/TimescaleDB as a config swap.
- **Streamlit** + `plotly`/`altair` dashboard.
- **Prometheus + Grafana**, **Docker + docker-compose**.

### Architecture (target)
```
                 ┌──────────── Go ingestion service ────────────┐
GDELT / RSS ───► │ goroutines fetch concurrently → channel → Kafka│──► news.raw
   / replay      └───────────────────────────────────────────────┘
                                                                    │
                 ┌──────── Go processing consumer group ───────┐    │
       news.raw ─┤ dedup (content hash) · normalize · filter   ├────┘──► news.processed
                 └─────────────────────────────────────────────┘              │
                                                                              ▼
                          ┌──── Python inference consumer group ────┐
        news.processed ──►│ N worker processes, ONNX INT8 FinBERT   │──► SQLite / Postgres
                          └─────────────────────────────────────────┘        │
                                                                              ▼
                                                     shock detector (rolling z-score)
                                                                              ▼
                          Streamlit dashboard          Prometheus ──► Grafana
```

### Suggested layout
```
.
├── ingestor/            # Go: ingestion producers + processing consumer group
│   ├── cmd/ingestor/main.go        (producers → news.raw)
│   ├── cmd/processor/main.go       (news.raw → dedup/normalize → news.processed)
│   ├── internal/sources/ (gdelt.go, rss.go, replay.go)
│   ├── internal/kafka/ (producer.go, consumer.go — behind an interface)
│   ├── internal/process/ (dedup.go, normalize.go, filter.go)
│   ├── internal/metrics/
│   └── go.mod
├── inference/           # Python: inference consumer group
│   ├── consumer.py worker.py model.py quantize.py
│   ├── storage/ signals/ metrics.py config.py
├── loadtest/            # --stress flood generator
├── dashboard/           # streamlit_app.py
├── observability/       # prometheus.yml, grafana/ dashboards
├── data/sample/news_sample.jsonl
├── benchmarks/          # bench_inference.py, results/
├── docker-compose.yml   # ingestor + processor + kafka + inference + db + prometheus + grafana + dashboard
├── .env.example  .gitignore  LICENSE (MIT)  README.md
```

---

## Build phases (in order — each ends runnable + tested)

**Phase 1 — Repo + message contract.** Monorepo layout, `.env.example`, `.gitignore`, MIT `LICENSE`, and a shared JSON message schema for `news.raw` and `news.processed` that Go produces and Python consumes. Define it once, document it.

**Phase 2 — Kafka + Go ingestion (replay).** Stand up Kafka (KRaft) in compose. Build the Go ingestion service's replay source: read `news_sample.jsonl`, stream to `news.raw` at a configurable rate, goroutine-based concurrency, `context` graceful shutdown. Generate the realistic synthetic sample dataset. Verify messages land.

**Phase 3 — Go processing consumer group.** `cmd/processor`: consume `news.raw` as a consumer group, dedup by content hash, normalize, filter, produce to `news.processed`. Export consumer lag + throughput metrics. Keep the Kafka client behind an interface (confluent-kafka-go default, segmentio fallback).

**Phase 4 — Python inference consumer group.** `quantize.py` (FinBERT → ONNX → INT8, cached), `model.py` (sentiment + entities), `worker.py` (a consumer-group process running one ONNX session). Wire N worker processes across partitions. Comment on processes-vs-threads and partition parallelism.

**Phase 5 — Storage + shock signal.** SQLAlchemy models (SQLite default), repository layer, per-entity rolling z-score shock detector. Persist raw results + flags.

**Phase 6 — Benchmarks.** `bench_inference.py`: real p50/p95 latency + throughput, FP32 vs INT8, written to `benchmarks/results/`. Print a markdown table. Run it for real.

**Phase 7 — Observability + load test.** Prometheus `/metrics` in Go (both stages) and Python; consumer lag per stage, throughput, inference latency, memory. Provision Prometheus + Grafana as code. Build `--stress` and verify you can watch lag and memory under sustained load.

**Phase 8 — Live ingestion.** Go GDELT + RSS sources selectable via flag, with rate-limit and failure handling. Replay stays the compose default.

**Phase 9 — Dashboard + compose polish.** Streamlit app (sentiment over time, flagged shocks, live metrics). One `docker compose up` brings up the whole system in replay mode. End-to-end smoke test. Fill README benchmark numbers from real runs.

---

## Standards for the agent
- Go: idiomatic, `context` for cancellation, graceful shutdown, no goroutine leaks, commit offsets correctly. Python: type hints, structured `logging`, graceful drain on SIGINT/SIGTERM.
- Comment the *why* on the language boundary, the two-stage topic design, processes-vs-threads, partitioning, and quantization — reviewers read those.
- Each phase must be runnable with passing tests before the next.
- If unsure about an external API shape (GDELT params, confluent-kafka-go call, ONNX quantization API), say so and stub behind an interface rather than guessing silently.
