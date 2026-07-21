from datetime import datetime, timedelta, timezone

import pytest

from inference.results import Entity
from inference.signals.shock import ShockSignal

pytest.importorskip("sqlalchemy")

from inference.storage.db import init_db, make_engine, make_session_factory  # noqa: E402
from inference.storage.repository import Repository  # noqa: E402

from .helpers import make_result  # noqa: E402


@pytest.fixture
def repo(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/test.db")
    init_db(engine)
    return Repository(make_session_factory(engine))


def test_save_result_persists_sentiment_and_entities(repo):
    result = make_result(
        entities=[Entity(text="NMBS", type="ticker"), Entity(text="Nimbus Robotics", type="company")]
    )
    repo.save_result(result)

    assert repo.count_results() == 1
    samples = repo.recent_entity_samples("NMBS", 3600, as_of=result.inferred_at)
    assert len(samples) == 1
    assert samples[0].score == result.sentiment_score


def test_recent_samples_respects_window(repo):
    t0 = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    repo.save_result(make_result(id="old", score=0.1, inferred_at=t0))
    repo.save_result(make_result(id="new", score=0.2, inferred_at=t0 + timedelta(seconds=100)))

    as_of = t0 + timedelta(seconds=100)
    # 60s window should exclude the item 100s earlier.
    samples = repo.recent_entity_samples("NMBS", 60, as_of=as_of)
    assert len(samples) == 1
    # 3600s window should include both.
    assert len(repo.recent_entity_samples("NMBS", 3600, as_of=as_of)) == 2


def test_save_and_list_shock(repo):
    sig = ShockSignal(
        entity="NMBS", zscore=4.2, velocity=0.8, mean_velocity=0.0, std_velocity=0.19,
        window_seconds=300, threshold=3.0, sentiment_score=0.9,
        triggered_at=datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc),
    )
    repo.save_shock(sig, item_id="replay-1")
    shocks = repo.list_shocks()
    assert len(shocks) == 1
    assert shocks[0].entity == "NMBS"
    assert shocks[0].zscore == pytest.approx(4.2)
