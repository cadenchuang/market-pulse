"""Python side of the shared message contract for `news.processed`.

Mirrors schemas/news.processed.schema.json. This is the consumer end of the
Kafka language boundary: Go produces these, we parse and validate them here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

SCHEMA_VERSION = "1.0.0"


def _parse_dt(value: str) -> datetime:
    # RFC 3339 with a trailing 'Z'. fromisoformat handles 'Z' from 3.11+, but we
    # normalize defensively for portability.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class ProcessedItem:
    """A validated `news.processed` message."""

    id: str
    content_hash: str
    source: str
    title: str
    body: str
    published_at: datetime
    ingested_at: datetime
    processed_at: datetime
    schema_version: str = SCHEMA_VERSION
    feed: str | None = None
    url: str | None = None
    language: str | None = None
    tickers: list[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: bytes | str | dict) -> "ProcessedItem":
        obj = data if isinstance(data, dict) else json.loads(data)

        version = obj.get("schema_version")
        if version != SCHEMA_VERSION:
            # Fail loudly on contract drift across the language boundary.
            raise ValueError(
                f"unsupported schema_version {version!r}; expected {SCHEMA_VERSION!r}"
            )

        try:
            return cls(
                id=obj["id"],
                content_hash=obj["content_hash"],
                source=obj["source"],
                title=obj["title"],
                body=obj["body"],
                published_at=_parse_dt(obj["published_at"]),
                ingested_at=_parse_dt(obj["ingested_at"]),
                processed_at=_parse_dt(obj["processed_at"]),
                schema_version=version,
                feed=obj.get("feed"),
                url=obj.get("url"),
                language=obj.get("language"),
                tickers=list(obj.get("tickers", []) or []),
            )
        except KeyError as exc:
            raise ValueError(f"missing required field: {exc.args[0]}") from exc

    def text(self) -> str:
        """Combined text handed to the sentiment model."""
        title = self.title.strip()
        body = self.body.strip()
        if title and body:
            return f"{title}. {body}"
        return title or body
