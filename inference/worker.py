"""The per-process worker loop.

One worker == one process == one ONNX session. The loop consumes news.processed,
runs the model, extracts entities, writes the result to the sink, and commits the
offset (at-least-once). It is deliberately synchronous and single-threaded within
the process: parallelism comes from running many *processes* (see consumer.py).
"""
from __future__ import annotations

import logging
from time import perf_counter
from typing import Callable

from .entities import extract_entities
from .kafka_consumer import Consumer
from .messages import ProcessedItem
from .metrics import NoOpMetrics
from .model import SentimentModel
from .results import InferenceResult
from .sink import Sink

logger = logging.getLogger(__name__)


def run_worker(
    consumer: Consumer,
    model: SentimentModel,
    sink: Sink,
    *,
    metrics=None,
    poll_timeout: float = 1.0,
    should_stop: Callable[[], bool] | None = None,
    gazetteer: dict[str, str] | None = None,
) -> int:
    """Run until ``should_stop()`` is true (or forever if None).

    Returns the number of results written. ``should_stop`` lets tests and signal
    handlers drain the loop cleanly.
    """
    metrics = metrics or NoOpMetrics()
    stop = should_stop or (lambda: False)
    written = 0
    polls = 0
    report_lag = getattr(consumer, "lag", None)

    while not stop():
        record = consumer.poll(poll_timeout)

        # Refresh the consumer-lag gauge periodically (it costs a broker
        # round-trip, so we don't do it on every single message).
        polls += 1
        if report_lag is not None and polls % 20 == 0:
            try:
                metrics.set_lag(report_lag())
            except Exception:  # pragma: no cover - lag is best-effort
                pass

        if record is None:
            continue

        try:
            item = ProcessedItem.from_json(record.value)
        except Exception as exc:
            logger.warning("skip: bad message at offset %s: %s", record.offset, exc)
            metrics.inc_error()
            consumer.commit(record)
            continue

        t0 = perf_counter()
        try:
            (sentiment,) = model.predict([item.text()])
        except Exception as exc:  # model failure should not wedge the group
            logger.error("inference error id=%s: %s", item.id, exc)
            metrics.inc_error()
            consumer.commit(record)
            continue
        metrics.observe_latency(perf_counter() - t0)

        result = InferenceResult(
            id=item.id,
            content_hash=item.content_hash,
            source=item.source,
            sentiment_label=sentiment.label,
            sentiment_score=sentiment.score,
            sentiment_probs=sentiment.probs,
            entities=extract_entities(item, gazetteer),
            model_name=model.name,
            quantized=model.quantized,
            published_at=item.published_at,
        )
        sink.write(result)
        metrics.inc_processed(sentiment.label)
        consumer.commit(record)
        written += 1

    return written
