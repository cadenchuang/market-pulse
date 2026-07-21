from datetime import datetime, timedelta, timezone

import pytest

from inference.results import Entity
from inference.signals.shock import ShockDetector

pytest.importorskip("sqlalchemy")

from inference.storage.db import init_db, make_engine, make_session_factory  # noqa: E402
from inference.storage.repository import Repository  # noqa: E402
from inference.storage.sink import SqlAlchemySink  # noqa: E402

from .helpers import make_result  # noqa: E402


@pytest.fixture
def repo(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/test.db")
    init_db(engine)
    return Repository(make_session_factory(engine))


def test_sink_persists_results_and_flags_shock(repo):
    detector = ShockDetector(window_seconds=3600, threshold=2.5, min_samples=6)
    sink = SqlAlchemySink(repo, detector)

    t0 = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    entity = [Entity(text="NMBS", type="ticker")]

    # A stable baseline of small-amplitude scores for one entity...
    stable = [0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.10, 0.11]
    for i, score in enumerate(stable):
        sink.write(make_result(id=f"s{i}", score=score, entities=entity, inferred_at=t0 + timedelta(seconds=i)))

    assert repo.list_shocks() == []  # nothing dramatic yet

    # ...then a sudden sentiment jump should trip the heuristic.
    sink.write(make_result(id="jump", score=0.95, entities=entity, inferred_at=t0 + timedelta(seconds=len(stable))))

    shocks = repo.list_shocks()
    assert len(shocks) == 1
    assert shocks[0].entity == "NMBS"
    assert shocks[0].item_id == "jump"
    assert repo.count_results() == len(stable) + 1


def test_sink_no_shock_on_flat_stream(repo):
    detector = ShockDetector(window_seconds=3600, threshold=3.0, min_samples=6)
    sink = SqlAlchemySink(repo, detector)
    t0 = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    entity = [Entity(text="HLES", type="ticker")]
    for i in range(10):
        score = 0.10 + (0.01 if i % 2 else -0.01)
        sink.write(make_result(id=f"f{i}", score=score, entities=entity, inferred_at=t0 + timedelta(seconds=i)))
    assert repo.list_shocks() == []
