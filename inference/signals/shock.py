"""Sentiment "shock" detector — a HEURISTIC, not a prediction.

The signal is the rolling z-score of per-entity **sentiment velocity**. Velocity
is defined as the per-event change in sentiment score for an entity
(``score_i - score_{i-1}``); using a unit event-step (rather than dividing by a
wall-clock delta) keeps it stable when many items arrive within milliseconds
during replay. When the latest velocity is more than ``threshold`` standard
deviations from the recent baseline of velocities, we flag a shock.

This is deliberately simple and labeled a heuristic everywhere. It does not
predict prices or markets.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Sample:
    """One per-entity sentiment observation within the rolling window."""

    ts: datetime
    score: float


@dataclass
class ShockSignal:
    entity: str
    zscore: float
    velocity: float
    mean_velocity: float
    std_velocity: float
    window_seconds: int
    threshold: float
    sentiment_score: float
    triggered_at: datetime


class ShockDetector:
    def __init__(self, window_seconds: int = 300, threshold: float = 3.0, min_samples: int = 8):
        self.window_seconds = window_seconds
        self.threshold = threshold
        # Minimum number of samples required before we will ever flag a shock, so
        # the baseline is meaningful.
        self.min_samples = max(4, min_samples)

    def evaluate(
        self, entity: str, samples: list[Sample], *, now: datetime | None = None
    ) -> ShockSignal | None:
        """Return a ShockSignal if the latest sample is a shock, else None.

        ``samples`` must be ordered oldest-first and already filtered to the
        window (the repository does this). The newest sample is the candidate.
        """
        if len(samples) < self.min_samples:
            return None

        scores = [s.score for s in samples]
        velocities = [scores[i] - scores[i - 1] for i in range(1, len(scores))]
        if len(velocities) < 3:
            return None

        current = velocities[-1]
        baseline = velocities[:-1]
        mean = statistics.fmean(baseline)
        std = statistics.pstdev(baseline)
        if std == 0.0:
            # No variation in the baseline: z-score is undefined. Don't flag.
            return None

        z = (current - mean) / std
        if abs(z) < self.threshold:
            return None

        return ShockSignal(
            entity=entity,
            zscore=z,
            velocity=current,
            mean_velocity=mean,
            std_velocity=std,
            window_seconds=self.window_seconds,
            threshold=self.threshold,
            sentiment_score=samples[-1].score,
            triggered_at=now or samples[-1].ts,
        )
