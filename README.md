# Market Pulse

A real-time financial-news **sentiment stream processor**: a Go stream-processing
layer and a Python inference layer, connected through Kafka. It ingests news,
dedups/normalizes it, runs **INT8-quantized FinBERT** to extract sentiment +
entities, and computes a heuristic **"sentiment shock"** signal from rolling
sentiment velocity — all observable in Prometheus/Grafana and a Streamlit
dashboard.

> **Portfolio project, honest by design.** It runs **offline with zero API keys**
> via replay mode (`docker compose up` just works). Benchmarks are **measured, not
> invented**. "Shock" is a **heuristic, not a prediction**, and nothing here claims
> ML forecasting or edge-hardware deployment.

## Why two languages, split by Kafka

Kafka is the **language boundary** — the two runtimes never call each other
directly, they hand off through topics.

- **Go** owns all stream plumbing: concurrent ingestion, Kafka producing, and a
  consumer group doing cheap high-throughput processing (dedup, normalize,
  filter). Goroutines + channels + `context` are the idiomatic tool for I/O-bound
  fan-out and backpressure.
- **Python** owns **model inference only**, because that is where the
  quantized-ONNX-FinBERT + tokenizer ecosystem lives. It runs **N worker
  processes** (not threads — the GIL blocks CPU-bound thread parallelism), joined
  as a Kafka consumer group that scales with partitions.

## Architecture

```
                 ┌──────────── Go ingestion service ────────────┐
GDELT / RSS ───► │ goroutines fetch concurrently → channel → Kafka│──► news.raw
   / replay      └───────────────────────────────────────────────┘
                                                                    │
                 ┌──────── Go processing consumer group ───────┐    │
    news.raw  ───┤ dedup (content hash) · normalize · filter   ├────┘──► news.processed
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

**Two-stage pipeline on purpose:** the Go processing stage exists so the expensive
model never runs on duplicate/irrelevant items. Consumer lag per stage is exported
as a metric — it is the backpressure signal.

## Message contract

The shape of every Kafka message is defined **once**, in [`schemas/`](schemas/),
as the single source of truth both languages agree on:

| Topic | Producer | Consumer | Schema |
|-------|----------|----------|--------|
| `news.raw` | Go ingestion | Go processing | [`schemas/news.raw.schema.json`](schemas/news.raw.schema.json) |
| `news.processed` | Go processing | Python inference | [`schemas/news.processed.schema.json`](schemas/news.processed.schema.json) |

`news.processed` is a superset of `news.raw` (adds `content_hash` + `processed_at`,
normalizes `title`/`body`). Model output never rides on the wire. See
[`schemas/README.md`](schemas/README.md) for the full field reference and rules.

## Repository layout

```
.
├── ingestor/            # Go: cmd/ingestor (→ news.raw), cmd/processor (→ news.processed)
├── inference/           # Python: consumer group, ONNX INT8 FinBERT, storage, shock signal
├── loadtest/            # --stress flood generator (Phase 7)
├── dashboard/           # Streamlit app: sentiment, shocks, live metrics (Phase 9)
├── observability/       # prometheus.yml + provisioned Grafana (Phase 7)
├── schemas/             # ← the shared message contract (source of truth) + tests
├── data/sample/         # synthetic replay dataset (Phase 2)
├── benchmarks/          # bench_inference.py + measured results (Phase 6)
├── scripts/             # smoke_test.sh (Docker-level end-to-end)
├── tests/               # offline end-to-end smoke test
├── docker-compose.yml   # whole system in one command, replay mode by default
├── .env.example         # config; copy to .env (gitignored)
└── LICENSE (MIT)
```

## Build phases

Built **phase by phase**; each phase is meant to end runnable + tested.

| # | Phase | Status |
|---|-------|--------|
| 1 | Repo layout + message contract | ✅ done |
| 2 | Kafka (KRaft) + Go ingestion (replay) | ✅ done |
| 3 | Go processing consumer group (dedup/normalize/filter) | ✅ done |
| 4 | Python inference consumer group (ONNX INT8 FinBERT) | ✅ done |
| 5 | Storage + shock signal (rolling z-score) | ✅ done |
| 6 | Benchmarks (FP32 vs INT8, real p50/p95 + throughput) | ✅ done |
| 7 | Observability (Prometheus/Grafana) + `--stress` load test | ✅ done |
| 8 | Live ingestion (GDELT + RSS, selectable via flag) | ✅ done |
| 9 | Streamlit dashboard + `docker compose up` polish + e2e smoke test | ✅ done |

## Getting started

Configure (defaults run offline in replay mode):

```bash
cp .env.example .env
```

### Run the whole system (one command, offline)

```bash
docker compose up --build
```

This brings up the **entire pipeline** in replay mode with zero keys: single-node
Kafka (KRaft) + partitioned topics, the Go ingestor and processor, the Python
inference workers, Prometheus, Grafana, and the Streamlit dashboard.

| UI | URL |
|----|-----|
| Dashboard (sentiment, shocks, live metrics) | <http://localhost:8501> |
| Grafana (pipeline dashboard, provisioned) | <http://localhost:3000> (`admin`/`admin`) |
| Prometheus | <http://localhost:9090> |

Verify messages landed:

```bash
docker exec market-pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic news.raw --from-beginning --max-messages 5
```

### End-to-end smoke test

```bash
# Docker-level: brings the full stack up, checks every stage's metrics + topics + UIs
./scripts/smoke_test.sh

# Dependency-light (no Docker/Kafka): contract → inference → storage → dashboard read
python -m pytest tests/test_end_to_end.py
```

### Live ingestion (Phase 8 — optional, no API keys)

Replay is the offline default. To pull **real** news, select a source mode. GDELT
and RSS both work without keys:

```bash
# GDELT Doc 2.0 (headline metadata only → body = headline)
go run ./ingestor/cmd/ingestor -mode gdelt -gdelt-query '(stocks OR earnings) sourcelang:eng'

# RSS / Atom feeds
go run ./ingestor/cmd/ingestor -mode rss -rss-feeds https://example.com/markets.xml

# both (fan-in)
go run ./ingestor/cmd/ingestor -mode live -gdelt-query 'earnings sourcelang:eng' -rss-feeds https://example.com/markets.xml
```

Live sources poll every `-poll-interval` (rate limit), skip repeats via a bounded
seen-set, and treat transient HTTP/parse errors as non-fatal (log + retry). See
[`ingestor/README.md`](ingestor/README.md#sources-internalsources--mode) for the
full source table. `docker compose up` keeps using replay so it always runs offline.

### Regenerate the synthetic dataset (optional, deterministic)

```bash
python3 data/generate_sample.py
```

### Tests

```bash
# Go ingestion + processing
cd ingestor && go test ./...

# Python inference (fakes + heuristic model; no ML/Kafka deps needed)
cd inference && python -m venv .venv && source .venv/bin/activate \
  && pip install -r requirements-dev.txt && python -m pytest tests

# Message-contract JSON Schemas
cd schemas && python -m venv .venv && source .venv/bin/activate \
  && pip install -r requirements.txt && pytest

# Dashboard data layer + offline end-to-end (contract → inference → storage → dashboard)
pip install pandas sqlalchemy pytest
python -m pytest dashboard/tests tests
```

> **Toolchain:** Go 1.22+ (stream layer) and Python 3.11+ (inference targets
> 3.11 for ML wheels). Docker + Docker Compose to run the full stack.

## Benchmarks

Real p50/p95 latency + throughput for FinBERT **FP32 vs INT8** on CPU, measured by
[`benchmarks/bench_inference.py`](benchmarks/bench_inference.py) (nothing hardcoded).

```bash
python -m inference.quantize --out inference/models/finbert-onnx
python -m benchmarks.bench_inference --variants fp32,int8
```

Measured run (Apple Silicon, 14 logical cores, ONNX Runtime, Python 3.13; per-item
latency at batch size 1, throughput batched):

| Variant | p50 latency (ms) | p95 latency (ms) | mean (ms) | throughput (items/s) |
|---------|------------------|------------------|-----------|----------------------|
| FP32    | 15.17            | 16.48            | 15.26     | 78.2                 |
| INT8    | 13.41            | 16.29            | 13.62     | 67.0                 |

**INT8 vs FP32 on this machine: 1.13× lower p50 latency, 0.86× throughput.**
Honest note: dynamic INT8 quantization improved single-item latency but *reduced*
batched throughput here — expected on an ARM CPU without INT8 VNNI acceleration,
where the extra quant/dequant work isn't offset. On x86 with AVX-512 VNNI the
throughput picture is typically the reverse. Numbers come straight from the
harness (`benchmarks/results/latest.md`); re-run on your hardware to compare. See
[`benchmarks/README.md`](benchmarks/README.md) for method and options.

## Observability + load test

Every stage exposes a Prometheus `/metrics` endpoint with runtime/memory
collectors plus its own counters — throughput, **consumer lag per stage**
(backpressure), inference latency histogram, error rates, and
`process_resident_memory_bytes`. Prometheus and Grafana are **provisioned as
code** in [`observability/`](observability/README.md); `docker compose up` brings
them up already wired to the datasource and the *Market Pulse Pipeline*
dashboard.

- Grafana: <http://localhost:3000> (anonymous viewer; `admin`/`admin` to edit)
- Prometheus: <http://localhost:9090>

Drive sustained load with the `--stress` generator, then watch lag and memory
climb and drain on the dashboard:

```bash
docker compose --profile stress up loadtest
```

It floods `news.raw` with unique synthetic items (so nothing is deduped away),
pushing pressure all the way to the Python workers. See
[`loadtest/README.md`](loadtest/README.md) for flags.

## Dashboard

The Streamlit app at <http://localhost:8501> shows **sentiment over time**,
**flagged sentiment shocks** (the rolling-z-score heuristic — not a prediction),
sentiment/source mix, recent scored items, and best-effort **live pipeline
metrics** from Prometheus. It reads the same results DB the inference workers
write (shared volume), with all data access + transforms in
[`dashboard/data.py`](dashboard/data.py) (no Streamlit import → unit-tested). See
[`dashboard/README.md`](dashboard/README.md).

## License

MIT — see [`LICENSE`](LICENSE).
