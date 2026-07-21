"""Inference service entrypoint: a Kafka consumer group of N worker PROCESSES.

Why processes, not threads
--------------------------
Inference is CPU-bound. Python's GIL serializes CPU-bound work across threads, so
threads would not give real parallelism here. Instead we run N independent
*processes*, each with its own ONNX Runtime session. Because they all share one
Kafka ``group.id``, the broker assigns the partitions of ``news.processed`` across
them — so parallelism scales with the partition count. To use all N workers, the
topic needs >= N partitions (compose creates 3).

Each process is isolated: its own model/session, its own metrics endpoint
(``metrics_base_port + worker_index``), and its own offset commits.
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import sys

from .config import Config
from .kafka_consumer import ConfluentConsumer
from .metrics import build_metrics
from .model import load_model
from .sink import LoggingSink, Sink
from .worker import run_worker

logger = logging.getLogger(__name__)


def resolve_worker_count(configured: int, cpu_count: int | None = None) -> int:
    """0 (or negative) means auto = cpu_count; always at least 1."""
    if configured and configured > 0:
        return configured
    cpus = cpu_count if cpu_count is not None else (os.cpu_count() or 1)
    return max(1, cpus)


def build_sink(config: Config) -> Sink:
    """Build the persistence + shock-detection sink.

    Imports SQLAlchemy lazily so the base package stays import-light. Falls back
    to a LoggingSink if storage cannot be initialized, so inference still runs.
    """
    try:
        from .signals.shock import ShockDetector
        from .storage.db import init_db, make_engine, make_session_factory
        from .storage.repository import Repository
        from .storage.sink import SqlAlchemySink

        engine = make_engine(config.database_url)
        init_db(engine)
        repo = Repository(make_session_factory(engine))
        detector = ShockDetector(
            window_seconds=config.shock_window_seconds,
            threshold=config.shock_zscore_threshold,
            min_samples=config.shock_min_samples,
        )
        logger.info("storage ready: %s", config.database_url)
        return SqlAlchemySink(repo, detector)
    except Exception as exc:  # pragma: no cover - depends on environment
        logger.warning("storage unavailable (%s); using LoggingSink", exc)
        return LoggingSink()


def _worker_process(config: Config, worker_index: int) -> None:
    """Runs in a child process: build session + consumer, then loop until SIGTERM."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [w{worker_index}] %(message)s",
    )

    stopping = {"flag": False}

    def _handle(signum, _frame):
        logger.info("signal %s: draining worker %d", signum, worker_index)
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    model = load_model(config)
    metrics = build_metrics(worker_index)
    metrics.serve(config.metrics_base_port + worker_index)
    consumer = ConfluentConsumer(config.bootstrap_servers, config.group_id, config.topic_processed)
    sink = build_sink(config)

    try:
        run_worker(
            consumer,
            model,
            sink,
            metrics=metrics,
            poll_timeout=config.poll_timeout_sec,
            should_stop=lambda: stopping["flag"],
        )
    finally:
        consumer.close()
        sink.close()
        logger.info("worker %d stopped", worker_index)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = Config.from_env()
    n = resolve_worker_count(config.workers)

    logger.info(
        "starting inference group: workers=%d group=%s topic=%s brokers=%s model_dir=%s",
        n, config.group_id, config.topic_processed, config.bootstrap_servers, config.model_dir,
    )

    # 'spawn' is the safe default across platforms (esp. with native ONNX libs).
    ctx = mp.get_context("spawn")
    procs: list[mp.process.BaseProcess] = []
    for i in range(n):
        p = ctx.Process(target=_worker_process, args=(config, i), name=f"inference-worker-{i}")
        p.start()
        procs.append(p)

    def _shutdown(signum, _frame):
        logger.info("signal %s: stopping %d workers", signum, len(procs))
        for p in procs:
            if p.pid:
                os.kill(p.pid, signal.SIGTERM)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    exit_code = 0
    for p in procs:
        p.join()
        if p.exitcode not in (0, None):
            exit_code = p.exitcode
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
