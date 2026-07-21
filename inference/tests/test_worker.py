import json
from datetime import datetime, timezone

from inference.kafka_consumer import FakeConsumer, Record
from inference.model import HeuristicModel
from inference.sink import CollectingSink
from inference.worker import run_worker


def processed_json(id: str, title: str, body: str, tickers=None) -> bytes:
    now = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc).isoformat()
    obj = {
        "schema_version": "1.0.0",
        "id": id,
        "content_hash": "sha256:" + "b" * 64,
        "source": "replay",
        "title": title,
        "body": body,
        "published_at": now,
        "ingested_at": now,
        "processed_at": now,
        "tickers": tickers or [],
    }
    return json.dumps(obj).encode()


def test_worker_processes_scores_and_commits():
    records = [
        Record(value=processed_json("1", "Alpha beats estimates", "profit rises to a record", ["NMBS"])),
        Record(value=processed_json("2", "Beta misses estimates", "shares slide on weak demand")),
        Record(value=b"{ not valid json"),  # error path: committed, not scored
    ]
    consumer = FakeConsumer(records)
    sink = CollectingSink()

    written = run_worker(
        consumer,
        HeuristicModel(),
        sink,
        poll_timeout=0.0,
        should_stop=lambda: consumer._idx >= len(records),
    )

    assert written == 2
    assert len(sink.results) == 2
    # Every consumed record (including the bad one) is committed: no stuck offsets.
    assert len(consumer.committed) == 3

    r1 = sink.results[0]
    assert r1.id == "1"
    assert r1.sentiment_label == "positive"
    assert any(e.text == "NMBS" for e in r1.entities)
    assert r1.model_name == "heuristic-lexicon-v1"

    assert sink.results[1].sentiment_label == "negative"


def test_worker_stops_immediately_when_should_stop_true():
    consumer = FakeConsumer([Record(value=processed_json("1", "x", "profit rises"))])
    sink = CollectingSink()
    written = run_worker(consumer, HeuristicModel(), sink, should_stop=lambda: True)
    assert written == 0
    assert sink.results == []


class _LagConsumer(FakeConsumer):
    """FakeConsumer that also reports a fixed lag, to exercise metrics.set_lag."""

    def lag(self) -> int:
        return 7


class _RecordingMetrics:
    def __init__(self):
        self.lags: list[int] = []

    def observe_latency(self, seconds: float) -> None: ...
    def inc_processed(self, label: str) -> None: ...
    def inc_error(self) -> None: ...
    def set_lag(self, lag: int) -> None:
        self.lags.append(lag)


def test_worker_reports_consumer_lag_periodically():
    # 40 empty polls -> lag refreshed at polls 20 and 40 (interval = 20).
    consumer = _LagConsumer([])
    metrics = _RecordingMetrics()
    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 40

    run_worker(consumer, HeuristicModel(), CollectingSink(), metrics=metrics,
               poll_timeout=0.0, should_stop=should_stop)

    assert metrics.lags == [7, 7]


def test_worker_result_serializes_to_dict():
    consumer = FakeConsumer([Record(value=processed_json("1", "Alpha beats", "profit rises"))])
    sink = CollectingSink()
    run_worker(consumer, HeuristicModel(), sink, should_stop=lambda: consumer._idx >= 1)
    d = sink.results[0].to_dict()
    assert set(["id", "sentiment_label", "sentiment_score", "sentiment_probs", "entities"]) <= set(d)
    assert isinstance(d["sentiment_probs"], dict)
