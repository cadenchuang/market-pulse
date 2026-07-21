import json
from pathlib import Path

import pytest

from inference.messages import SCHEMA_VERSION, ProcessedItem

EXAMPLE = (
    Path(__file__).resolve().parents[2]
    / "schemas"
    / "examples"
    / "news.processed.example.json"
)


def test_parses_committed_schema_example():
    """The Python parser must accept the canonical contract example."""
    item = ProcessedItem.from_json(EXAMPLE.read_text())
    assert item.id == "replay-000042"
    assert item.content_hash.startswith("sha256:")
    assert item.tickers == ["EXMP"]
    assert item.published_at.tzinfo is not None


def test_text_combines_title_and_body():
    item = ProcessedItem.from_json(EXAMPLE.read_text())
    text = item.text()
    assert item.title in text and item.body in text


def test_schema_version_mismatch_fails_loudly():
    obj = json.loads(EXAMPLE.read_text())
    obj["schema_version"] = "9.9.9"
    with pytest.raises(ValueError):
        ProcessedItem.from_json(obj)


def test_missing_required_field_raises():
    obj = json.loads(EXAMPLE.read_text())
    del obj["content_hash"]
    with pytest.raises(ValueError):
        ProcessedItem.from_json(obj)


def test_current_version_constant():
    assert SCHEMA_VERSION == "1.0.0"
