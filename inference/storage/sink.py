"""A Sink that persists results and runs shock detection per entity.

Kept in the storage package so importing the base `sink` module stays dependency-
free. The worker loop is unchanged — it just calls `write`.
"""
from __future__ import annotations

import logging

from ..results import InferenceResult
from ..signals.shock import ShockDetector
from .repository import Repository, _canonical_entity

logger = logging.getLogger(__name__)


class SqlAlchemySink:
    """Persist each result, then evaluate the shock heuristic for its entities.

    Shock detection reads recent per-entity samples from the shared DB, so it sees
    items scored by *all* worker processes — the cross-process view a shared store
    provides but in-process state could not.
    """

    def __init__(self, repository: Repository, detector: ShockDetector):
        self._repo = repository
        self._detector = detector

    def write(self, result: InferenceResult) -> None:
        self._repo.save_result(result)

        for entity in result.entities:
            name = _canonical_entity(entity)
            samples = self._repo.recent_entity_samples(
                name, self._detector.window_seconds, as_of=result.inferred_at
            )
            signal = self._detector.evaluate(name, samples, now=result.inferred_at)
            if signal is not None:
                self._repo.save_shock(signal, item_id=result.id)
                logger.info(
                    "SHOCK (heuristic) entity=%s z=%.2f velocity=%.3f",
                    name, signal.zscore, signal.velocity,
                )

    def close(self) -> None:
        return None
