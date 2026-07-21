"""Offline end-to-end smoke test (no Kafka, no ML deps, no Docker).

Exercises the whole Python-side chain the way the running system does, using the
same message contract the Go processor emits on ``news.processed``:

    news.processed JSON  ->  inference worker (heuristic model)
                         ->  SqlAlchemySink (results + entities + shock detection)
                         ->  dashboard.data read layer

This is the code-level equivalent of the Phase 9 smoke test; the Docker-level
smoke test (real Kafka + compose) lives in scripts/smoke_test.sh.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd

from dashboard import data
from inference.kafka_consumer import FakeConsumer, Record
from inference.model import HeuristicModel
from inference.signals.shock import ShockDetector
from inference.storage.db import make_engine, make_session_factory
from inference.storage.repository import Repository
from inference.storage.sink import SqlAlchemySink
from inference.worker import run_worker

POSITIVE_BODIES = [
    "Shares surge as the company beats profit estimates and raises guidance sharply.",
    "Record revenue growth and a strong earnings beat lift the outlook considerably.",
    "Analysts upgrade the stock after a blockbuster quarter with soaring demand.",
]
NEGATIVE_BODIES = [
    "Shares plunge as the company misses estimates and slashes its guidance badly.",
    "Weak demand and a steep revenue decline trigger a downgrade and heavy losses.",
]


def _processed_json(item_id: str, body: str, when: datetime, ticker: str = "NMBS") -> bytes:
    iso = when.isoformat()
    return json.dumps(
        {
            "schema_version": "1.0.0",
            "id": item_id,
            "content_hash": "sha256:" + f"{abs(hash(item_id)):064x}"[:64],
            "source": "replay",
            "title": f"{ticker} update {item_id}",
            "body": body,
            "language": "en",
            "published_at": iso,
            "ingested_at": iso,
            "processed_at": iso,
            "tickers": [ticker],
        }
    ).encode()


def test_end_to_end_offline(tmp_path):
    db_url = f"sqlite:///{tmp_path/'e2e.db'}"

    # Sink identical to production, but with a shock detector tuned to fire on a
    # short synthetic burst (production defaults need a longer warmup).
    engine = make_engine(db_url)
    from inference.storage.db import init_db

    init_db(engine)
    repo = Repository(make_session_factory(engine))
    detector = ShockDetector(window_seconds=3600, threshold=1.0, min_samples=3)
    sink = SqlAlchemySink(repo, detector)

    # Build a stream: a calm negative baseline, then a sharp positive swing for
    # the same entity — the kind of velocity spike the shock heuristic exists for.
    base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    records = []
    n = 0
    for i in range(4):
        records.append(Record(value=_processed_json(f"neg-{i}", NEGATIVE_BODIES[i % len(NEGATIVE_BODIES)], base + timedelta(seconds=n))))
        n += 5
    for i in range(4):
        records.append(Record(value=_processed_json(f"pos-{i}", POSITIVE_BODIES[i % len(POSITIVE_BODIES)], base + timedelta(seconds=n))))
        n += 5

    consumer = FakeConsumer(records)
    written = run_worker(
        consumer,
        HeuristicModel(),
        sink,
        should_stop=lambda: consumer._idx >= len(records),
    )
    sink.close()

    # Every record was consumed and committed (at-least-once, no stuck offsets).
    assert written == len(records)
    assert len(consumer.committed) == len(records)

    # Dashboard read layer sees the persisted results.
    results = data.load_results(engine)
    assert len(results) == len(records)

    kpis = data.compute_kpis(results, data.load_shocks(engine), now=pd.Timestamp(base + timedelta(seconds=n)))
    assert kpis.total_scored == len(records)

    timeline = data.sentiment_timeline(results, rule="1min")
    assert timeline["volume"].sum() == len(records)

    labels = data.label_distribution(results)
    assert labels["count"].sum() == len(records)

    # The negative->positive swing should have flagged at least one shock.
    shocks = data.load_shocks(engine)
    assert len(shocks) >= 1
    assert (shocks["entity"] == "NMBS").any()
