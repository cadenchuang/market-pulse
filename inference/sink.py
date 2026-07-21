"""Result sinks.

Phase 4 ships a ``LoggingSink`` so inference is observable end-to-end. Phase 5
adds a SQLAlchemy-backed sink (persist results + shock flags) behind this same
interface — the worker loop does not change.
"""
from __future__ import annotations

import json
import logging
from typing import Protocol

from .results import InferenceResult

logger = logging.getLogger(__name__)


class Sink(Protocol):
    def write(self, result: InferenceResult) -> None:
        ...

    def close(self) -> None:
        ...


class LoggingSink:
    """Writes each result as a JSON line via the logger (default Phase 4 sink)."""

    def write(self, result: InferenceResult) -> None:
        logger.info("result %s", json.dumps(result.to_dict(), separators=(",", ":")))

    def close(self) -> None:  # nothing to release
        return None


class CollectingSink:
    """Keeps results in memory (used by tests)."""

    def __init__(self) -> None:
        self.results: list[InferenceResult] = []

    def write(self, result: InferenceResult) -> None:
        self.results.append(result)

    def close(self) -> None:
        return None
