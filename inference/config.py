"""Environment-driven configuration for the inference service."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, "") or default)
    except ValueError:
        return default


def _float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, "") or default)
    except ValueError:
        return default


def _bool(key: str, default: bool) -> bool:
    v = os.environ.get(key)
    if v is None or v == "":
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Resolved inference configuration (all fields have offline-friendly defaults)."""

    bootstrap_servers: str = "localhost:29092"
    topic_processed: str = "news.processed"
    group_id: str = "market-pulse-inference"

    # Number of worker PROCESSES (not threads — see README). 0 => auto (cpu_count).
    workers: int = 0

    # Directory holding the exported/quantized ONNX model + tokenizer. If missing
    # or the ML deps are unavailable, the service falls back to the heuristic model
    # so it still runs offline.
    model_dir: str = "inference/models/finbert-onnx"
    model_id: str = "ProsusAI/finbert"
    quantize_int8: bool = True
    # Force the dependency-free heuristic model even if an ONNX model exists.
    force_heuristic: bool = False

    poll_timeout_sec: float = 1.0
    metrics_base_port: int = 9103

    # Storage.
    database_url: str = "sqlite:///market_pulse.db"

    # Shock detector (heuristic — rolling z-score of per-entity sentiment velocity).
    shock_window_seconds: int = 300
    shock_zscore_threshold: float = 3.0
    shock_min_samples: int = 8

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            topic_processed=os.environ.get("KAFKA_TOPIC_PROCESSED", cls.topic_processed),
            group_id=os.environ.get("KAFKA_CONSUMER_GROUP_INFERENCE", cls.group_id),
            workers=_int("INFERENCE_WORKERS", cls.workers),
            model_dir=os.environ.get("ONNX_MODEL_DIR", cls.model_dir),
            model_id=os.environ.get("MODEL_ID", cls.model_id),
            quantize_int8=_bool("QUANTIZE_INT8", cls.quantize_int8),
            force_heuristic=_bool("FORCE_HEURISTIC", cls.force_heuristic),
            metrics_base_port=_int("METRICS_PORT_INFERENCE", cls.metrics_base_port),
            database_url=os.environ.get("DATABASE_URL", cls.database_url),
            shock_window_seconds=_int("SHOCK_WINDOW_SECONDS", cls.shock_window_seconds),
            shock_zscore_threshold=_float("SHOCK_ZSCORE_THRESHOLD", cls.shock_zscore_threshold),
            shock_min_samples=_int("SHOCK_MIN_SAMPLES", cls.shock_min_samples),
        )
