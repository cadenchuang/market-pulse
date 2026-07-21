from datetime import datetime, timezone

from inference.messages import ProcessedItem
from inference.results import Entity, InferenceResult


def make_item(
    id: str = "replay-1",
    title: str = "Headline",
    body: str = "Body text.",
    tickers: list[str] | None = None,
    language: str = "en",
) -> ProcessedItem:
    now = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    return ProcessedItem(
        id=id,
        content_hash="sha256:" + "a" * 64,
        source="replay",
        title=title,
        body=body,
        published_at=now,
        ingested_at=now,
        processed_at=now,
        language=language,
        tickers=tickers or [],
    )


def make_result(
    id: str = "replay-1",
    score: float = 0.5,
    label: str = "positive",
    entities: list[Entity] | None = None,
    inferred_at: datetime | None = None,
) -> InferenceResult:
    ts = inferred_at or datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    return InferenceResult(
        id=id,
        content_hash="sha256:" + "c" * 64,
        source="replay",
        sentiment_label=label,
        sentiment_score=score,
        sentiment_probs={"positive": 0.6, "negative": 0.1, "neutral": 0.3},
        entities=entities if entities is not None else [Entity(text="NMBS", type="ticker")],
        model_name="heuristic-lexicon-v1",
        quantized=False,
        published_at=ts,
        inferred_at=ts,
    )
