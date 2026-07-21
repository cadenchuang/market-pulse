"""SQLAlchemy ORM models.

Three tables:

* ``sentiment_results`` — one row per scored item.
* ``entity_sentiments`` — one row per (item, entity); this is what the shock
  detector reads to build a per-entity time series.
* ``shocks`` — one row per flagged shock (heuristic).

Times are stored as naive UTC (SQLite has no native tz type); the repository
normalizes on the way in and out.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SentimentResult(Base):
    __tablename__ = "sentiment_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str] = mapped_column(String, index=True)
    content_hash: Mapped[str] = mapped_column(String)
    source: Mapped[str] = mapped_column(String)
    sentiment_label: Mapped[str] = mapped_column(String)
    sentiment_score: Mapped[float] = mapped_column(Float)
    prob_positive: Mapped[float] = mapped_column(Float)
    prob_negative: Mapped[float] = mapped_column(Float)
    prob_neutral: Mapped[float] = mapped_column(Float)
    model_name: Mapped[str] = mapped_column(String)
    quantized: Mapped[bool] = mapped_column(Boolean)
    published_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    inferred_at: Mapped[datetime] = mapped_column(DateTime)


class EntitySentiment(Base):
    __tablename__ = "entity_sentiments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str] = mapped_column(String, index=True)
    entity: Mapped[str] = mapped_column(String, index=True)
    entity_type: Mapped[str] = mapped_column(String)
    sentiment_score: Mapped[float] = mapped_column(Float)
    published_at: Mapped[datetime] = mapped_column(DateTime)
    inferred_at: Mapped[datetime] = mapped_column(DateTime)

    __table_args__ = (
        # The shock detector queries (entity, inferred_at) ranges.
        Index("ix_entity_inferred", "entity", "inferred_at"),
    )


class Shock(Base):
    __tablename__ = "shocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity: Mapped[str] = mapped_column(String, index=True)
    item_id: Mapped[str] = mapped_column(String)
    zscore: Mapped[float] = mapped_column(Float)
    velocity: Mapped[float] = mapped_column(Float)
    mean_velocity: Mapped[float] = mapped_column(Float)
    std_velocity: Mapped[float] = mapped_column(Float)
    window_seconds: Mapped[int] = mapped_column(Integer)
    threshold: Mapped[float] = mapped_column(Float)
    sentiment_score: Mapped[float] = mapped_column(Float)
    triggered_at: Mapped[datetime] = mapped_column(DateTime, index=True)
