import pytest

from inference.model import HeuristicModel, LABELS, load_model


@pytest.fixture
def model():
    return HeuristicModel()


def test_positive(model):
    (s,) = model.predict(["Company beats estimates as profit rises to a record high"])
    assert s.label == "positive"
    assert s.score > 0


def test_negative(model):
    (s,) = model.predict(["Company misses estimates, cuts outlook; shares slide on weak demand"])
    assert s.label == "negative"
    assert s.score < 0


def test_neutral(model):
    (s,) = model.predict(["Company names a new officer effective next month"])
    assert s.label == "neutral"
    assert s.score == 0.0


def test_probs_are_normalized(model):
    for s in model.predict(["profit rises", "loss widens", "no signal words"]):
        assert set(s.probs) >= set(LABELS)
        assert abs(sum(s.probs.values()) - 1.0) < 1e-9
        assert -1.0 <= s.score <= 1.0


def test_batch_length(model):
    out = model.predict(["a", "b", "c"])
    assert len(out) == 3


def test_load_model_falls_back_to_heuristic_when_no_onnx(tmp_path):
    class Cfg:
        force_heuristic = False
        model_dir = str(tmp_path / "does-not-exist")
        quantize_int8 = True

    m = load_model(Cfg())
    assert m.name == "heuristic-lexicon-v1"
    assert m.quantized is False


def test_force_heuristic():
    class Cfg:
        force_heuristic = True
        model_dir = "whatever"
        quantize_int8 = True

    assert load_model(Cfg()).name == "heuristic-lexicon-v1"
