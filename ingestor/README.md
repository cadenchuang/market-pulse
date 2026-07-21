# ingestor/ (Go stream-processing layer)

Go owns all stream plumbing. This module contains **two** binaries that share the
Kafka client and metrics code.

| Binary | Role | Reads | Writes | Phase |
|--------|------|-------|--------|-------|
| `cmd/ingestor`  | Concurrent news ingestion (replay / GDELT / RSS) | source feeds / JSONL replay | `news.raw` | 2 ✅, 8 |
| `cmd/processor` | Consumer group: dedup, normalize, filter | `news.raw` | `news.processed` | 3 ✅ |
| `cmd/loadtest`  | `--stress` flood generator (unique synthetic items) | — | `news.raw` | 7 ✅ |

## Why Go here (language boundary)

Ingestion and cheap high-throughput processing are I/O-bound fan-out/backpressure
problems. Goroutines + channels + `context` cancellation are the idiomatic tool.
Go never calls Python directly — the two languages hand off through Kafka topics.
See the root `README.md` for the full rationale.

## Kafka client is behind an interface (swappable)

`internal/kafka.Producer` is a tiny interface. The default implementation is
**pure-Go `segmentio/kafka-go`** (`segmentio.go`), which builds with
`CGO_ENABLED=0` — no librdkafka, tiny distroless image, painless cross-compile.

The spec names `confluent-kafka-go` as the default; that needs CGo + librdkafka,
which is exactly the pain point the spec flags. Because the client sits behind the
`Producer` interface, a confluent implementation can be dropped in without
touching any caller. We use the sanctioned pure-Go fallback as the working
default so every phase builds and tests cleanly everywhere.

## Current layout (Phases 2–3)

```
ingestor/
├── cmd/ingestor/         # replay -> news.raw (main.go + testable run.go)
├── cmd/processor/        # news.raw -> news.processed (main.go + testable run.go)
├── cmd/loadtest/         # --stress flood generator -> news.raw (Phase 7)
├── internal/contract/    # Go side of the shared schema (news.raw + news.processed)
├── internal/kafka/       # Producer + Consumer interfaces, segmentio impls, fakes
├── internal/process/     # normalize, content-hash dedup, filter
├── internal/metrics/     # Prometheus collectors + /metrics server
├── internal/sources/     # Source interface + replay.go, gdelt.go, rss.go
└── go.mod
```

## Stage 1 — ingestion concurrency model (`cmd/ingestor`)

```
source(s) ──(bounded channel = backpressure)──► N producer workers ──► Kafka
```

- Each **`sources.Source`** runs in its own goroutine; multiple sources **fan in**
  onto one channel. The last source to finish closes the channel so workers drain
  and exit cleanly.
- A worker pool fans out onto the `Producer`.
- The **bounded channel is the backpressure signal**: if Kafka slows, workers
  stop draining, the channel fills, and the sources block.
- `SIGINT`/`SIGTERM` cancels a `context` that unwinds every stage — no goroutine
  leaks (verified by tests).

### Sources (`internal/sources`, `-mode`)

All sources satisfy one interface (`Name()` + `Run(ctx, out)`); they never close
the channel (the caller owns it) and never crash the pipeline on a bad poll.

| Mode     | Source | Notes |
|----------|--------|-------|
| `replay` | JSONL dataset (default, offline) | rate-limited by `-rate`, optional `-loop` |
| `gdelt`  | GDELT Doc 2.0 API (`-gdelt-query`) | no key; returns **headline metadata only** (no full text), so `body = title` |
| `rss`    | RSS 2.0 / Atom feeds (`-rss-feeds`) | HTML stripped from descriptions; `body` falls back to title |
| `live`   | gdelt + rss together | whatever is configured |

Live sources poll every `-poll-interval` (the rate limit). Transient
fetch/parse/HTTP errors are logged and retried on the next tick — never fatal. A
small bounded seen-set suppresses re-emitting the same article across polls (the
processor stage still dedups authoritatively by content hash). Live sources only
stop on `context` cancellation; **replay stays the compose default.**
- **Metrics** (Prometheus at `:9101/metrics`):
  `market_pulse_ingestor_produced_total` / `..._produce_errors_total`, plus the
  Go runtime + process collectors (memory, goroutines, GC, FDs).

## Stage 2 — processing consumer group (`cmd/processor`)

```
news.raw ─► [ decode+validate ] ─► normalize ─► filter ─► dedup ─► news.processed
                                                                     (commit offset)
```

This cheap, high-throughput stage is the whole reason the pipeline is two-stage:
**the expensive Python model must never run on duplicate or irrelevant items.**

- **Normalize** (`internal/process`): Unicode NFC, drop control chars, collapse
  whitespace, lowercase. Makes dedup robust and gives the (uncased) FinBERT
  tokenizer consistent input.
- **Filter**: drops empty / too-short / wrong-language items (metric-labeled by
  reason).
- **Dedup**: bounded in-memory set of `content_hash` (sha256 over normalized
  title+body). Per-instance, best-effort — see the note in `dedup.go`.
- **At-least-once**: uses `FetchMessage` + explicit `CommitMessages`; the offset
  is committed only *after* the processed item is produced. A produce failure
  returns an error and leaves the offset uncommitted for redelivery.
- **Single consumer goroutine** keeps offset commits ordered; scale horizontally
  by running more `processor` replicas in the same consumer group across the
  topic's partitions.
- **Metrics** (`internal/metrics`, Prometheus at `:9102/metrics`):
  `market_pulse_processor_{consumed,processed,duplicates,filtered,errors}_total`
  and `market_pulse_processor_consumer_lag` (the backpressure signal), plus the
  Go runtime + process collectors (memory, goroutines, GC, FDs). Both Go stages
  build their registry with `metrics.NewRegistry()` so memory is always exported.

## `--stress` load test (`cmd/loadtest`)

Floods `news.raw` with **unique** synthetic items (unique id + randomized body,
so the processor never dedups them away) to drive sustained load through the
whole pipeline. Metrics at `:9106/metrics`
(`market_pulse_loadtest_{produced,errors}_total`).

```bash
# via compose (off by default, behind the `stress` profile)
docker compose --profile stress up loadtest

# or locally
go run ./ingestor/cmd/loadtest -brokers localhost:29092 -workers 16 -rate 0 -duration 120s
```

Flags: `-brokers -topic -workers -rate -duration -count -metrics-addr`. See
[`../loadtest/README.md`](../loadtest/README.md).

## Run it

### With Docker (recommended — brings up Kafka too)

```bash
docker compose up --build
```

This starts Kafka (KRaft), creates the partitioned topics, and streams the
synthetic dataset to `news.raw` in a loop.

### Locally against a running broker

```bash
# from repo root, with Kafka reachable at localhost:29092
go run ./ingestor/cmd/ingestor \
  -brokers localhost:29092 \
  -file data/sample/news_sample.jsonl \
  -rate 50
```

Flags: `-brokers -topic -mode -file -rate -workers -buffer -loop -metrics-addr`
plus live-source flags `-gdelt-query -gdelt-max -rss-feeds -poll-interval`
(each falls back to the matching env var from `.env.example`).

Live examples (no keys needed):

```bash
# GDELT headlines matching a query
go run ./ingestor/cmd/ingestor -mode gdelt \
  -gdelt-query '(stocks OR earnings) sourcelang:eng' -poll-interval 60s

# RSS/Atom feeds
go run ./ingestor/cmd/ingestor -mode rss \
  -rss-feeds https://feeds.a.com/markets.xml,https://feeds.b.com/atom.xml

# both at once
go run ./ingestor/cmd/ingestor -mode live \
  -gdelt-query 'earnings sourcelang:eng' -rss-feeds https://feeds.a.com/markets.xml
```

### Run the processor locally

```bash
go run ./ingestor/cmd/processor -brokers localhost:29092
# metrics at http://localhost:9102/metrics
```

### Verify messages landed

```bash
# raw items from the ingestor
docker exec market-pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic news.raw --from-beginning --max-messages 5

# clean, deduped items from the processor
docker exec market-pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic news.processed --from-beginning --max-messages 5

# processor throughput + consumer lag
curl -s localhost:9102/metrics | grep market_pulse_processor
```

## Test

```bash
cd ingestor
go test ./...
```

Both stages include end-to-end tests against fake brokers, no Docker required:
`cmd/ingestor` asserts all 200 items are produced as valid contract JSON;
`cmd/processor` drives the decode → normalize → filter → dedup loop and asserts
the right emit/duplicate/filter/error counts, that only clean unique items are
produced (valid `news.processed` JSON), and that every offset is committed.
Tests pass under `-race`.
