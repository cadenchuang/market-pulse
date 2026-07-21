# loadtest/ (`--stress` flood generator)

Floods `news.raw` with **unique** synthetic news items as fast as the brokers and
worker pool allow, so you can watch consumer lag, throughput, inference latency,
and memory climb under sustained load in Prometheus/Grafana.

Every generated item has a unique id and a randomized body, so nothing is dropped
by the processor's dedup/filter — the pressure propagates all the way through to
the Python inference workers.

## Where the code lives

The generator is a Go command inside the ingestor module so it reuses the shared
message contract, the Kafka producer, and the metrics helpers with no
duplication:

```
ingestor/cmd/loadtest/main.go
```

It compiles into the same image as the ingestor/processor (`/loadtest` binary).

## Run it

Via compose (recommended — off by default behind the `stress` profile):

```bash
docker compose --profile stress up loadtest
```

Or directly against a local broker:

```bash
cd ingestor
go run ./cmd/loadtest -brokers localhost:29092 -workers 16 -rate 0 -duration 120s
```

## Flags

| Flag           | Default        | Meaning |
|----------------|----------------|---------|
| `-brokers`     | `localhost:29092` | comma-separated bootstrap servers |
| `-topic`       | `news.raw`     | topic to flood |
| `-workers`     | `8`            | producer goroutines |
| `-rate`        | `0`            | aggregate items/sec; **0 = unlimited flood** |
| `-duration`    | `60s`          | run length; `0` = until interrupted |
| `-count`       | `0`            | max items; `0` = unlimited |
| `-metrics-addr`| `:9106`        | Prometheus `/metrics` endpoint |

The generator exposes `market_pulse_loadtest_produced_total` /
`..._errors_total` (plus runtime/memory collectors) on `:9106`, scraped by
Prometheus. Watch the **Consumer lag per stage** and **Resident memory** panels
on the Grafana dashboard to see backpressure build and drain.
