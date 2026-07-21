# inference/ (Python inference layer)

Python owns **model inference only** — that is where the quantized-ONNX-FinBERT +
tokenizer ecosystem lives. It consumes `news.processed`, runs INT8-quantized
FinBERT to extract sentiment, heuristically extracts entities, and writes results
to a sink. Kafka is the language boundary: this layer never calls the Go services
directly.

## Why processes, not threads

Inference is CPU-bound. Python's GIL serializes CPU-bound work across threads, so
threads give no real parallelism here. Instead the service runs **N worker
processes** (`consumer.py`), each with its **own ONNX Runtime session**. They
share one Kafka `group.id`, so the broker assigns the partitions of
`news.processed` across them — **parallelism scales with the partition count**.
To use all N workers, the topic needs ≥ N partitions (compose creates 3).

```
news.processed (3 partitions)
        │  consumer group: market-pulse-inference
        ├── worker process 0  ── ONNX session ──► sink   (:9103/metrics)
        ├── worker process 1  ── ONNX session ──► sink   (:9104/metrics)
        └── worker process 2  ── ONNX session ──► sink   (:9105/metrics)
```

## The model (and the honest fallback)

- **`OnnxFinbertModel`** — the real model: INT8-quantized `ProsusAI/finbert` run
  via ONNX Runtime. Heavy imports (onnxruntime, transformers) are lazy.
- **`HeuristicModel`** — a tiny, dependency-free finance-lexicon classifier. It is
  the fallback when no exported model is present, so the pipeline runs **fully
  offline** and tests stay fast. It is clearly labeled a heuristic and is **not**
  presented as FinBERT.

`load_model()` uses ONNX when the model dir + deps are available, else the
heuristic. Entity extraction (`entities.py`) is likewise a **heuristic gazetteer
matcher**, not a trained NER model — labeled as such to keep claims honest.

> Model output (sentiment/entities) is written to the datastore (Phase 5), never
> back onto Kafka. The topic contract forbids it (`additionalProperties: false`).

## Layout

```
inference/
├── consumer.py     # entrypoint: spawns N worker processes (the consumer group)
├── worker.py       # per-process loop: consume -> infer -> entities -> sink -> commit
├── model.py        # SentimentModel protocol + ONNX FinBERT + heuristic fallback
├── quantize.py     # ProsusAI/finbert -> ONNX -> dynamic INT8 (cached)
├── entities.py     # heuristic ticker/company gazetteer matcher
├── messages.py     # Python side of the news.processed contract
├── results.py      # InferenceResult (internal; not a Kafka message)
├── kafka_consumer.py  # Consumer interface + confluent impl + fake (tests)
├── sink.py         # Sink interface + LoggingSink + CollectingSink
├── metrics.py      # Prometheus metrics + runtime/memory collectors (guarded; no-op if dep missing)
├── config.py       # env-driven config
├── storage/        # SQLAlchemy models, engine, repository, SqlAlchemySink
└── signals/        # shock.py — rolling z-score shock detector (heuristic)
```

## Storage + shock signal (Phase 5)

Results are persisted via SQLAlchemy (SQLite by default; swap `DATABASE_URL` for
Postgres/TimescaleDB). Three tables: `sentiment_results` (one row per item),
`entity_sentiments` (one row per item×entity — the shock detector's time series),
and `shocks` (flagged shocks).

The **`SqlAlchemySink`** ties it together: on each result it persists the
sentiment + entity rows, then for each entity evaluates the shock heuristic over
recent samples read back from the DB. Reading from the shared DB means shock
detection sees items scored by **all** worker processes — the cross-process view
in-process state could not provide.

**Shock = heuristic, not a prediction.** It is the rolling **z-score of per-entity
sentiment velocity** (per-event change in sentiment score). When the latest
velocity exceeds `SHOCK_ZSCORE_THRESHOLD` standard deviations from the recent
baseline (and there are at least `SHOCK_MIN_SAMPLES` samples in the
`SHOCK_WINDOW_SECONDS` window), a shock row is written. It does not forecast
prices or markets.

## Observability (Phase 7)

Each worker process serves its own Prometheus `/metrics` endpoint on
`metrics_base_port + worker_index` (defaults `:9103`…`:9105`):

- `market_pulse_inference_latency_seconds` — per-item inference latency histogram
  (drives p50/p95/p99).
- `market_pulse_inference_processed_total{label}` — throughput by sentiment.
- `market_pulse_inference_errors_total` — decode/score failures.
- `market_pulse_inference_consumer_lag` — per-worker lag on `news.processed`
  (refreshed periodically; the backpressure signal for this stage).
- Process + platform + GC collectors: `process_resident_memory_bytes` (memory),
  CPU, open FDs, Python GC stats — registered on the worker's own registry.

Prometheus + Grafana are provisioned as code; see
[`../observability/README.md`](../observability/README.md).

## Run

### Tests (no ML/Kafka deps needed — fakes + heuristic model)

```bash
cd inference
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests -v      # run from repo root: python -m pytest inference/tests
```

### Export the real INT8 FinBERT model (one-time, needs network)

```bash
pip install -r inference/requirements.txt -r inference/requirements-export.txt
python -m inference.quantize --out inference/models/finbert-onnx
```

### Run the consumer group

```bash
pip install -r inference/requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 INFERENCE_WORKERS=3 python -m inference.consumer
```

With Docker (whole stack): `docker compose up --build`. The default build does
**not** export the model (fast, offline) and the service uses the heuristic
fallback. To ship real INT8 FinBERT, build with `--build-arg EXPORT_MODEL=true`
or export into the mounted `inference/models` volume.

## At-least-once

Offsets are committed only after a result is written. Malformed or unscored
messages are committed (logged as errors) so a single bad item cannot wedge the
group.
