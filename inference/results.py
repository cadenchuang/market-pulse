"""The inference result produced per item.

Note: results are written to the datastore (Phase 5), NOT back onto Kafka. Model
output never rides on the topic contract — that is enforced on the Go side by
`additionalProperties: false`. This dataclass is therefore internal to Python.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Entity:
    """A heuristically-detected entity (ticker or company mention)."""

    text: str
    type: str  # "ticker" | "company"

    def to_dict(self) -> dict:
        return {"text": self.text, "type": self.type}


@dataclass
class InferenceResult:
    id: str
    content_hash: str
    source: str
    sentiment_label: str  # positive | negative | neutral
    sentiment_score: float  # signed confidence in [-1, 1] (positive - negative)
    sentiment_probs: dict[str, float]
    entities: list[Entity]
    model_name: str
    quantized: bool
    published_at: datetime
    inferred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "content_hash": self.content_hash,
            "source": self.source,
            "sentiment_label": self.sentiment_label,
            "sentiment_score": round(self.sentiment_score, 6),
            "sentiment_probs": {k: round(v, 6) for k, v in self.sentiment_probs.items()},
            "entities": [e.to_dict() for e in self.entities],
            "model_name": self.model_name,
            "quantized": self.quantized,
            "published_at": self.published_at.isoformat(),
            "inferred_at": self.inferred_at.isoformat(),
        }
