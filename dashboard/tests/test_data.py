from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from dashboard import data
from inference.results import Entity, InferenceResult
from inference.signals.shock import ShockSignal
from inference.storage.db import make_engine, make_session_factory
from inference.storage.repository import Repository


def _result(id: str, score: float, label: str, when: datetime, source: str = "replay") -> InferenceResult:
    return InferenceResult(
        id=id,
        content_hash="sha256:" + "a" * 64,
        source=source,
        sentiment_label=label,
        sentiment_score=score,
        sentiment_probs={"positive": 0.6, "negative": 0.2, "neutral": 0.2},
        entities=[Entity(text="NMBS", type="ticker")],
        model_name="heuristic-lexicon-v1",
        quantized=False,
        published_at=when,
        inferred_at=when,
    )


@pytest.fixture()
def engine(tmp_path):
    eng = data.get_engine(f"sqlite:///{tmp_path/'dash.db'}")
    return eng


@pytest.fixture()
def repo(engine):
    return Repository(make_session_factory(engine))


def test_empty_db_yields_empty_frames_and_zero_kpis(engine):
    results = data.load_results(engine)
    shocks = data.load_shocks(engine)
    assert results.empty and shocks.empty

    kpis = data.compute_kpis(results, shocks)
    assert kpis.total_scored == 0
    assert kpis.shocks_flagged == 0
    # Transforms must not blow up on empty input.
    assert data.sentiment_timeline(results).empty
    assert data.label_distribution(results).empty
    assert data.source_distribution(results).empty


def test_loads_and_aggregates(engine, repo):
    base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    repo.save_result(_result("1", 0.9, "positive", base, source="replay"))
    repo.save_result(_result("2", -0.8, "negative", base + timedelta(seconds=10), source="gdelt"))
    repo.save_result(_result("3", 0.1, "neutral", base + timedelta(seconds=70), source="replay"))

    results = data.load_results(engine)
    assert len(results) == 3

    kpis = data.compute_kpis(results, data.load_shocks(engine), now=pd.Timestamp(base + timedelta(seconds=80)))
    assert kpis.total_scored == 3
    assert kpis.model_name == "heuristic-lexicon-v1"
    assert kpis.quantized is False
    assert kpis.avg_sentiment == pytest.approx((0.9 - 0.8 + 0.1) / 3)

    # Two 1-minute buckets (12:00 has 2 items, 12:01 has 1).
    timeline = data.sentiment_timeline(results, rule="1min")
    assert list(timeline["volume"]) == [2, 1]

    labels = data.label_distribution(results)
    assert set(labels["label"]) == {"positive", "negative", "neutral"}
    assert labels["count"].sum() == 3

    sources = data.source_distribution(results)
    assert dict(zip(sources["source"], sources["count"]))["replay"] == 2


def test_kpi_last_5m_window(engine, repo):
    now = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    repo.save_result(_result("old", 0.5, "positive", now - timedelta(minutes=30)))
    repo.save_result(_result("new", 0.5, "positive", now - timedelta(minutes=1)))

    results = data.load_results(engine)
    kpis = data.compute_kpis(results, pd.DataFrame(), now=pd.Timestamp(now))
    assert kpis.total_scored == 2
    assert kpis.scored_last_5m == 1


def test_global_stats_uncapped(engine, repo):
    base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(5):
        repo.save_result(_result(f"r{i}", 1.0, "positive", base + timedelta(seconds=i)))

    stats = data.global_stats(engine)
    assert stats.total_scored == 5
    assert stats.total_shocks == 0
    assert stats.avg_sentiment == pytest.approx(1.0)


def test_kpis_use_global_stats_over_capped_window(engine, repo):
    base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(10):
        repo.save_result(_result(f"r{i}", 0.5, "positive", base + timedelta(seconds=i)))

    # Simulate a capped load window (2 rows) but DB holds 10.
    capped = data.load_results(engine, limit=2)
    assert len(capped) == 2
    stats = data.global_stats(engine)

    kpis = data.compute_kpis(capped, data.load_shocks(engine), stats=stats)
    assert kpis.total_scored == 10  # from DB, not the capped frame
    assert kpis.avg_sentiment == pytest.approx(0.5)


def test_global_stats_empty_db(engine):
    stats = data.global_stats(engine)
    assert stats.total_scored == 0
    assert stats.total_shocks == 0
    assert stats.avg_sentiment == 0.0


def test_loads_shocks(engine, repo):
    sig = ShockSignal(
        entity="NMBS",
        zscore=4.2,
        velocity=0.9,
        mean_velocity=0.1,
        std_velocity=0.19,
        window_seconds=300,
        threshold=3.0,
        sentiment_score=0.8,
        triggered_at=datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc),
    )
    repo.save_shock(sig, item_id="item-1")

    shocks = data.load_shocks(engine)
    assert len(shocks) == 1
    assert shocks.iloc[0]["entity"] == "NMBS"
    assert shocks.iloc[0]["zscore"] == pytest.approx(4.2)
