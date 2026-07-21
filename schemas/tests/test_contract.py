"""Contract tests for the shared Kafka message schemas.

These guard the language boundary: the schemas in this directory are the single
source of truth that both the Go stream layer and the Python inference layer
agree on. If these pass, an example message that Go would produce is one Python
can trust, and vice versa.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

SCHEMA_DIR = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = SCHEMA_DIR / "examples"

CASES = {
    "news.raw": (
        SCHEMA_DIR / "news.raw.schema.json",
        EXAMPLES_DIR / "news.raw.example.json",
    ),
    "news.processed": (
        SCHEMA_DIR / "news.processed.schema.json",
        EXAMPLES_DIR / "news.processed.example.json",
    ),
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


@pytest.mark.parametrize("topic", list(CASES))
def test_schema_is_valid_metaschema(topic: str) -> None:
    schema_path, _ = CASES[topic]
    Draft202012Validator.check_schema(_load(schema_path))


@pytest.mark.parametrize("topic", list(CASES))
def test_example_conforms_to_schema(topic: str) -> None:
    schema_path, example_path = CASES[topic]
    Draft202012Validator(_load(schema_path)).validate(_load(example_path))


def test_missing_required_field_is_rejected() -> None:
    schema_path, example_path = CASES["news.raw"]
    validator = Draft202012Validator(_load(schema_path))
    bad = _load(example_path)
    del bad["id"]
    with pytest.raises(ValidationError):
        validator.validate(bad)


def test_model_output_on_the_wire_is_rejected() -> None:
    """Sentiment/entities must never ride on the topic (additionalProperties: false)."""
    schema_path, example_path = CASES["news.processed"]
    validator = Draft202012Validator(_load(schema_path))
    bad = _load(example_path)
    bad["sentiment"] = 0.91
    with pytest.raises(ValidationError):
        validator.validate(bad)


def test_processed_is_superset_of_raw() -> None:
    """Every property defined on news.raw must also exist on news.processed."""
    raw = _load(CASES["news.raw"][0])["properties"]
    processed = _load(CASES["news.processed"][0])["properties"]
    missing = set(raw) - set(processed)
    assert not missing, f"news.processed is missing carried-over fields: {missing}"
