"""Data access + transforms for the Streamlit dashboard.

This module intentionally has **no Streamlit import** so it can be unit-tested
directly. It reads the same tables the inference workers write (via the shared
SQLAlchemy models), and shapes them into small pandas DataFrames the UI renders.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Engine, func, select

from inference.storage.db import init_db, make_engine
from inference.storage.models import SentimentResult, Shock

logger = logging.getLogger(__name__)

# Model sentiment labels -> a signed numeric for averaging/plotting.
LABEL_SIGN = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}


def get_engine(database_url: str) -> Engine:
    """Open the results DB and ensure the tables exist (so a fresh start doesn't
    error before the first item is scored). Safe to call repeatedly."""
    engine = make_engine(database_url)
    try:
        init_db(engine)
    except Exception as exc:  # pragma: no cover - depends on FS perms
        logger.warning("init_db skipped: %s", exc)
    return engine


def load_results(engine: Engine, limit: int = 2000) -> pd.DataFrame:
    """Most recent scored items as a DataFrame (newest first)."""
    stmt = (
        select(
            SentimentResult.item_id,
            SentimentResult.source,
            SentimentResult.sentiment_label,
            SentimentResult.sentiment_score,
            SentimentResult.prob_positive,
            SentimentResult.prob_negative,
            SentimentResult.prob_neutral,
            SentimentResult.model_name,
            SentimentResult.quantized,
            SentimentResult.published_at,
            SentimentResult.inferred_at,
        )
        .order_by(SentimentResult.inferred_at.desc())
        .limit(limit)
    )
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    if not df.empty:
        df["inferred_at"] = pd.to_datetime(df["inferred_at"], utc=True)
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    return df


def load_shocks(engine: Engine, limit: int = 200) -> pd.DataFrame:
    """Most recent flagged shocks (heuristic) as a DataFrame."""
    stmt = (
        select(
            Shock.entity,
            Shock.item_id,
            Shock.zscore,
            Shock.velocity,
            Shock.mean_velocity,
            Shock.std_velocity,
            Shock.sentiment_score,
            Shock.window_seconds,
            Shock.threshold,
            Shock.triggered_at,
        )
        .order_by(Shock.triggered_at.desc())
        .limit(limit)
    )
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    if not df.empty:
        df["triggered_at"] = pd.to_datetime(df["triggered_at"], utc=True)
    return df


@dataclass(frozen=True)
class GlobalStats:
    """DB-wide totals (not limited by the loaded row window)."""

    total_scored: int
    total_shocks: int
    avg_sentiment: float


def global_stats(engine: Engine) -> GlobalStats:
    """Aggregate the full tables server-side, so headline KPIs reflect real
    production state even when the UI only loads a capped recent window."""
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(SentimentResult)).scalar_one()
        shocks = conn.execute(select(func.count()).select_from(Shock)).scalar_one()
        avg = conn.execute(select(func.avg(SentimentResult.sentiment_score))).scalar()
    return GlobalStats(
        total_scored=int(total or 0),
        total_shocks=int(shocks or 0),
        avg_sentiment=float(avg) if avg is not None else 0.0,
    )


@dataclass(frozen=True)
class KPIs:
    total_scored: int
    scored_last_5m: int
    shocks_flagged: int
    avg_sentiment: float
    model_name: str
    quantized: bool


def compute_kpis(
    results: pd.DataFrame,
    shocks: pd.DataFrame,
    now: pd.Timestamp | None = None,
    stats: GlobalStats | None = None,
) -> KPIs:
    """Headline numbers for the top of the dashboard.

    ``total_scored``, ``shocks_flagged``, and ``avg_sentiment`` come from
    ``stats`` (DB-wide aggregates) when provided, so they stay accurate even
    though ``results`` is a capped recent window. Without ``stats`` they fall
    back to the loaded frame (fine for small/local runs and tests).
    """
    total_scored = stats.total_scored if stats else len(results)
    shocks_flagged = stats.total_shocks if stats else len(shocks)

    if results.empty:
        avg = stats.avg_sentiment if stats else 0.0
        return KPIs(total_scored, 0, shocks_flagged, avg, "-", False)

    now = now or pd.Timestamp.now(tz="UTC")
    cutoff = now - pd.Timedelta(minutes=5)
    last_5m = int((results["inferred_at"] >= cutoff).sum())
    avg_sentiment = stats.avg_sentiment if stats else float(results["sentiment_score"].mean())
    return KPIs(
        total_scored=total_scored,
        scored_last_5m=last_5m,
        shocks_flagged=shocks_flagged,
        avg_sentiment=avg_sentiment,
        model_name=str(results["model_name"].iloc[0]),
        quantized=bool(results["quantized"].iloc[0]),
    )


def sentiment_timeline(results: pd.DataFrame, rule: str = "1min") -> pd.DataFrame:
    """Average sentiment score + item volume, resampled into time buckets.

    Returns a DataFrame indexed by bucket start with columns
    ``avg_sentiment`` and ``volume``. Empty input yields an empty frame.
    """
    if results.empty:
        return pd.DataFrame(columns=["avg_sentiment", "volume"])
    s = results.set_index("inferred_at").sort_index()
    agg = s["sentiment_score"].resample(rule).agg(["mean", "count"])
    agg.columns = ["avg_sentiment", "volume"]
    agg["avg_sentiment"] = agg["avg_sentiment"].fillna(0.0)
    return agg


def label_distribution(results: pd.DataFrame) -> pd.DataFrame:
    """Counts per sentiment label, as a tidy DataFrame (label, count)."""
    if results.empty:
        return pd.DataFrame(columns=["label", "count"])
    counts = results["sentiment_label"].value_counts()
    # Stable ordering for the bar chart.
    order = [l for l in ("positive", "neutral", "negative") if l in counts.index]
    order += [l for l in counts.index if l not in order]
    return pd.DataFrame({"label": order, "count": [int(counts[l]) for l in order]})


def source_distribution(results: pd.DataFrame) -> pd.DataFrame:
    """Counts per source (replay/gdelt/rss), as a tidy DataFrame."""
    if results.empty:
        return pd.DataFrame(columns=["source", "count"])
    counts = results["source"].value_counts()
    return pd.DataFrame({"source": counts.index.tolist(), "count": counts.values.astype(int)})


def fetch_prometheus_scalar(prom_url: str, query: str, timeout: float = 2.0) -> float | None:
    """Best-effort instant query against Prometheus, returning a single scalar.

    Returns None if Prometheus is unreachable or the query has no data, so the
    dashboard degrades gracefully when the metrics stack isn't running.
    """
    try:
        import requests  # lazy: dashboard runs without it if Prometheus is absent

        resp = requests.get(
            f"{prom_url.rstrip('/')}/api/v1/query",
            params={"query": query},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("data", {}).get("result", [])
        if not results:
            return None
        return float(results[0]["value"][1])
    except Exception as exc:  # pragma: no cover - network dependent
        logger.debug("prometheus query failed (%s): %s", query, exc)
        return None
