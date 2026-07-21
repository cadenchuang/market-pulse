"""Repository layer: the only place that talks to the ORM."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ..results import Entity, InferenceResult
from ..signals.shock import Sample, ShockSignal
from .models import EntitySentiment, SentimentResult, Shock


def _naive_utc(dt: datetime) -> datetime:
    """Normalize to naive UTC for consistent SQLite storage/comparison."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _canonical_entity(entity: Entity) -> str:
    return entity.text.upper() if entity.type == "ticker" else entity.text


class Repository:
    def __init__(self, session_factory: sessionmaker[Session]):
        self._sf = session_factory

    def save_result(self, result: InferenceResult) -> None:
        """Persist the sentiment result and one row per entity."""
        published = _naive_utc(result.published_at)
        inferred = _naive_utc(result.inferred_at)
        probs = result.sentiment_probs

        with self._sf() as s, s.begin():
            s.add(
                SentimentResult(
                    item_id=result.id,
                    content_hash=result.content_hash,
                    source=result.source,
                    sentiment_label=result.sentiment_label,
                    sentiment_score=result.sentiment_score,
                    prob_positive=probs.get("positive", 0.0),
                    prob_negative=probs.get("negative", 0.0),
                    prob_neutral=probs.get("neutral", 0.0),
                    model_name=result.model_name,
                    quantized=result.quantized,
                    published_at=published,
                    inferred_at=inferred,
                )
            )
            for entity in result.entities:
                s.add(
                    EntitySentiment(
                        item_id=result.id,
                        entity=_canonical_entity(entity),
                        entity_type=entity.type,
                        sentiment_score=result.sentiment_score,
                        published_at=published,
                        inferred_at=inferred,
                    )
                )

    def recent_entity_samples(
        self, entity: str, window_seconds: int, as_of: datetime
    ) -> list[Sample]:
        """Per-entity sentiment series within the window, oldest-first.

        Windowed on inferred_at (arrival time), which makes the shock signal
        real-time as items stream; for live data this equals event time.
        """
        upper = _naive_utc(as_of)
        lower = upper - timedelta(seconds=window_seconds)
        with self._sf() as s:
            rows = s.execute(
                select(EntitySentiment.inferred_at, EntitySentiment.sentiment_score)
                .where(
                    EntitySentiment.entity == entity,
                    EntitySentiment.inferred_at >= lower,
                    EntitySentiment.inferred_at <= upper,
                )
                .order_by(EntitySentiment.inferred_at.asc(), EntitySentiment.id.asc())
            ).all()
        return [Sample(ts=r[0], score=r[1]) for r in rows]

    def save_shock(self, signal: ShockSignal, item_id: str) -> None:
        with self._sf() as s, s.begin():
            s.add(
                Shock(
                    entity=signal.entity,
                    item_id=item_id,
                    zscore=signal.zscore,
                    velocity=signal.velocity,
                    mean_velocity=signal.mean_velocity,
                    std_velocity=signal.std_velocity,
                    window_seconds=signal.window_seconds,
                    threshold=signal.threshold,
                    sentiment_score=signal.sentiment_score,
                    triggered_at=_naive_utc(signal.triggered_at),
                )
            )

    # --- Read helpers (used by the Streamlit dashboard in Phase 9) ---

    def list_shocks(self, limit: int = 100) -> list[Shock]:
        with self._sf() as s:
            return list(
                s.execute(select(Shock).order_by(Shock.triggered_at.desc()).limit(limit)).scalars()
            )

    def list_results(self, limit: int = 500) -> list[SentimentResult]:
        with self._sf() as s:
            return list(
                s.execute(
                    select(SentimentResult).order_by(SentimentResult.inferred_at.desc()).limit(limit)
                ).scalars()
            )

    def count_results(self) -> int:
        with self._sf() as s:
            return s.query(SentimentResult).count()
