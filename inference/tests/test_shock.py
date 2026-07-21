from datetime import datetime, timedelta, timezone

from inference.signals.shock import Sample, ShockDetector


def series(scores: list[float]) -> list[Sample]:
    base = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
    return [Sample(ts=base + timedelta(seconds=i), score=s) for i, s in enumerate(scores)]


def test_flat_series_is_no_shock():
    d = ShockDetector(threshold=3.0, min_samples=8)
    samples = series([0.10, 0.12, 0.09, 0.11, 0.10, 0.12, 0.09, 0.11, 0.10])
    assert d.evaluate("NMBS", samples) is None


def test_sudden_jump_is_a_shock():
    d = ShockDetector(threshold=3.0, min_samples=8)
    # Small stable velocities, then a large jump on the last step.
    samples = series([0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.95])
    sig = d.evaluate("NMBS", samples)
    assert sig is not None
    assert abs(sig.zscore) >= 3.0
    assert sig.velocity > 0
    assert sig.entity == "NMBS"


def test_too_few_samples_returns_none():
    d = ShockDetector(min_samples=8)
    assert d.evaluate("NMBS", series([0.1, 0.9, 0.1])) is None


def test_zero_variation_baseline_returns_none():
    d = ShockDetector(threshold=3.0, min_samples=8)
    # All identical -> velocities all 0 -> std 0 -> undefined z -> no shock.
    assert d.evaluate("NMBS", series([0.5] * 9)) is None


def test_threshold_is_respected():
    samples = series([0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.35])
    lenient = ShockDetector(threshold=2.0, min_samples=8)
    strict = ShockDetector(threshold=50.0, min_samples=8)
    assert lenient.evaluate("X", samples) is not None
    assert strict.evaluate("X", samples) is None


def test_triggered_at_defaults_to_last_sample():
    d = ShockDetector(threshold=3.0, min_samples=8)
    samples = series([0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.10, 0.11, 0.95])
    sig = d.evaluate("NMBS", samples)
    assert sig.triggered_at == samples[-1].ts
