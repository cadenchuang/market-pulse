"""Sentiment models behind a common interface.

Two implementations:

* ``OnnxFinbertModel`` — the real thing: INT8-quantized ProsusAI/finbert run via
  ONNX Runtime. Heavy deps (onnxruntime, transformers) are imported lazily so the
  rest of the package (and the tests) don't require them.
* ``HeuristicModel`` — a tiny, dependency-free finance lexicon classifier. It is
  the fallback when no exported model is present, which keeps the pipeline
  runnable fully offline and makes tests fast. It is clearly labeled as a
  heuristic; it is NOT presented as FinBERT.

``load_model`` picks ONNX when a model directory and the deps are available,
otherwise falls back to the heuristic.
"""
from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)

LABELS = ("positive", "negative", "neutral")


@dataclass
class Sentiment:
    label: str
    score: float  # signed: P(positive) - P(negative), in [-1, 1]
    probs: dict[str, float]


@runtime_checkable
class SentimentModel(Protocol):
    name: str
    quantized: bool

    def predict(self, texts: list[str]) -> list[Sentiment]:
        ...


# --------------------------------------------------------------------------- #
# Heuristic fallback (no dependencies)
# --------------------------------------------------------------------------- #

_POSITIVE = {
    "beat", "beats", "beating", "rose", "rise", "rises", "jumped", "jump", "jumps",
    "record", "surge", "surged", "gain", "gains", "gained", "strong", "stronger",
    "profit", "profits", "upgrade", "upgraded", "raise", "raised", "growth", "grew",
    "outperform", "bullish", "rally", "rallied", "tops", "topped", "expands",
}
_NEGATIVE = {
    "miss", "misses", "missed", "fell", "fall", "falls", "slide", "slides", "slid",
    "cut", "cuts", "cutting", "recall", "recalls", "weak", "weaker", "loss", "losses",
    "downgrade", "downgraded", "warn", "warns", "warned", "drop", "drops", "dropped",
    "decline", "declined", "bearish", "plunge", "plunged", "lawsuit", "probe",
}
_TOKEN_RE = re.compile(r"[a-z']+")


class HeuristicModel:
    """Lexicon-based finance sentiment. Deterministic, offline, dependency-free."""

    name = "heuristic-lexicon-v1"
    quantized = False

    def predict(self, texts: list[str]) -> list[Sentiment]:
        return [self._one(t) for t in texts]

    def _one(self, text: str) -> Sentiment:
        tokens = _TOKEN_RE.findall(text.lower())
        pos = sum(1 for t in tokens if t in _POSITIVE)
        neg = sum(1 for t in tokens if t in _NEGATIVE)
        total = pos + neg
        if total == 0:
            return Sentiment("neutral", 0.0, {"positive": 0.0, "negative": 0.0, "neutral": 1.0})

        p_pos = pos / total
        p_neg = neg / total
        # Soften with the count so "1 vs 0" isn't as extreme as "5 vs 0".
        confidence = total / (total + 2)
        p_pos *= confidence
        p_neg *= confidence
        p_neu = 1.0 - (p_pos + p_neg)
        probs = {"positive": p_pos, "negative": p_neg, "neutral": p_neu}
        score = p_pos - p_neg
        label = max(probs, key=probs.get)
        return Sentiment(label, score, probs)


# --------------------------------------------------------------------------- #
# ONNX FinBERT (lazy heavy imports)
# --------------------------------------------------------------------------- #


class OnnxFinbertModel:
    """INT8-quantized FinBERT via ONNX Runtime. One session per process."""

    def __init__(self, model_dir: str, quantized: bool = True, max_length: int = 256):
        # Imported here so importing this module never requires the ML stack.
        import numpy as np  # noqa: F401
        import onnxruntime as ort
        from transformers import AutoTokenizer

        self._np = np
        self.quantized = quantized
        self.max_length = max_length
        self.name = "ProsusAI/finbert-onnx" + ("-int8" if quantized else "-fp32")

        model_file = "model_quantized.onnx" if quantized else "model.onnx"
        model_path = os.path.join(model_dir, model_file)
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"ONNX model not found: {model_path} (run quantize.py)")

        self._tokenizer = AutoTokenizer.from_pretrained(model_dir)
        so = ort.SessionOptions()
        so.intra_op_num_threads = 1  # one session per process; avoid oversubscription
        self._session = ort.InferenceSession(model_path, sess_options=so, providers=["CPUExecutionProvider"])
        self._labels = self._resolve_labels(model_dir)

    @staticmethod
    def _resolve_labels(model_dir: str) -> list[str]:
        import json

        cfg_path = os.path.join(model_dir, "config.json")
        try:
            cfg = json.loads(open(cfg_path).read())
            id2label = cfg.get("id2label")
            if id2label:
                return [id2label[str(i)].lower() for i in range(len(id2label))]
        except Exception:  # pragma: no cover - defensive
            pass
        # ProsusAI/finbert default order.
        return ["positive", "negative", "neutral"]

    def predict(self, texts: list[str]) -> list[Sentiment]:
        np = self._np
        enc = self._tokenizer(
            texts, padding=True, truncation=True, max_length=self.max_length, return_tensors="np"
        )
        inputs = {k: v for k, v in enc.items() if k in {i.name for i in self._session.get_inputs()}}
        logits = self._session.run(None, inputs)[0]
        return [self._to_sentiment(row) for row in logits]

    def _to_sentiment(self, logits) -> Sentiment:
        np = self._np
        exps = np.exp(logits - np.max(logits))
        probs_arr = exps / exps.sum()
        probs = {self._labels[i]: float(probs_arr[i]) for i in range(len(self._labels))}
        for lab in LABELS:
            probs.setdefault(lab, 0.0)
        score = probs.get("positive", 0.0) - probs.get("negative", 0.0)
        label = max(probs, key=probs.get)
        return Sentiment(label, score, probs)


def load_model(config) -> SentimentModel:
    """Return the best available model for the given Config."""
    if getattr(config, "force_heuristic", False):
        logger.info("using heuristic model (forced)")
        return HeuristicModel()
    try:
        model = OnnxFinbertModel(config.model_dir, quantized=config.quantize_int8)
        logger.info("loaded ONNX model: %s", model.name)
        return model
    except Exception as exc:  # missing model dir or missing deps
        logger.warning("ONNX model unavailable (%s); falling back to heuristic model", exc)
        return HeuristicModel()


def _softmax_stable(values: list[float]) -> list[float]:
    m = max(values)
    exps = [math.exp(v - m) for v in values]
    s = sum(exps)
    return [e / s for e in exps]
