import json

import pytest

from benchmarks.bench_inference import (
    percentile,
    resolve_variants,
    run_benchmark,
    write_results,
    environment,
    load_texts,
)
from inference.model import HeuristicModel


def test_percentile_basic():
    xs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert percentile(xs, 50) == pytest.approx(5.5)
    assert percentile(xs, 0) == 1
    assert percentile(xs, 100) == 10


def test_percentile_single_value():
    assert percentile([42.0], 95) == 42.0


def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        percentile([], 50)


def test_run_benchmark_produces_real_numbers():
    texts = ["profit rises to a record", "loss widens on weak demand", "names new officer"]
    r = run_benchmark("heuristic", HeuristicModel(), texts, iterations=20, batch_size=4, warmup=2)
    assert r.variant == "heuristic"
    assert r.model_name == "heuristic-lexicon-v1"
    assert r.p50_ms > 0
    assert r.p95_ms >= r.p50_ms
    assert r.throughput_items_per_s > 0


def test_resolve_variants_explicit():
    assert resolve_variants("fp32,int8", "nonexistent-dir") == ["fp32", "int8"]


def test_resolve_variants_auto_falls_back_to_heuristic(tmp_path):
    assert resolve_variants("auto", str(tmp_path / "no-model")) == ["heuristic"]


def test_write_results_creates_files(tmp_path):
    texts = ["profit rises", "loss widens"]
    r = run_benchmark("heuristic", HeuristicModel(), texts, iterations=10, batch_size=2, warmup=1)
    json_path, md_path = write_results([r], environment(), tmp_path)
    assert json_path.exists() and md_path.exists()
    payload = json.loads(json_path.read_text())
    assert payload["results"][0]["variant"] == "heuristic"
    assert "p50 latency" in md_path.read_text()


def test_load_texts_fallback_when_missing(tmp_path):
    texts = load_texts(tmp_path / "missing.jsonl", limit=10)
    assert len(texts) >= 1
