# schemas/ — the message contract (defined once)

Kafka is the language boundary in Market Pulse: **Go and Python never call each
other directly — they hand off through topics.** For that to work, the shape of
each message has to be agreed on in exactly one place. That place is here.

These JSON Schema files are the **single source of truth** for the two pipeline
topics. Go produces messages that conform to them; Python consumes and validates
against them. Neither language owns the contract — this directory does.

| File | Topic | Producer | Consumer |
|------|-------|----------|----------|
| `news.raw.schema.json` | `news.raw` | Go ingestion (`cmd/ingestor`) | Go processing (`cmd/processor`) |
| `news.processed.schema.json` | `news.processed` | Go processing (`cmd/processor`) | Python inference (`consumer.py`) |

Runnable examples that conform to each schema live in `examples/`.

## Design rules

- **`news.processed` is a superset of `news.raw`.** Identity/content fields carry
  over unchanged; the processing stage only *adds* `content_hash` (the dedup key)
  and `processed_at`, and *normalizes* `title`/`body`.
- **No model output on the wire.** Sentiment and extracted entities are produced
  by the Python inference layer and written to the datastore — they never appear
  on either topic. `additionalProperties: false` enforces this: a stray
  `sentiment` field fails validation.
- **`schema_version` is a `const`.** Both languages fail loudly on a mismatch.
  Bump it (and this doc) on any breaking change.
- **Times are RFC 3339 / ISO 8601 UTC strings.** `id` is also the Kafka message
  key, so the same item lands on the same partition across both topics.

## Field reference

### `news.raw`
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema_version` | string (`"1.0.0"`) | yes | contract version |
| `id` | string | yes | stable unique id; Kafka message key |
| `source` | enum `replay`/`gdelt`/`rss` | yes | `replay` is the offline default |
| `feed` | string | no | feed name/identifier within the source |
| `url` | string (uri) | no | canonical URL |
| `title` | string | yes | raw headline (unnormalized) |
| `body` | string | yes | raw body/summary (unnormalized) |
| `language` | string (`^[a-z]{2}$`) | no | ISO 639-1 code |
| `published_at` | string (date-time) | yes | original publish time |
| `ingested_at` | string (date-time) | yes | time Go produced to `news.raw` |
| `tickers` | string[] | no | optional source hints, **not** model output |

### `news.processed` (adds to the above)
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `content_hash` | string (`^sha256:[0-9a-f]{64}$`) | yes | dedup key over normalized content |
| `processed_at` | string (date-time) | yes | time Go produced to `news.processed` |
| `title` / `body` | string | yes | **normalized** text |

## Validating

The contract is covered by a test that (a) checks each schema against the
JSON Schema 2020-12 meta-schema, (b) validates the example messages, and
(c) asserts that missing-required and extra-field messages are rejected.

```bash
cd schemas
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest
```
