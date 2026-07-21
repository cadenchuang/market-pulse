"""Kafka consumer behind a small interface, mirroring the Go side's design.

The real implementation uses ``confluent-kafka`` (prebuilt wheels bundle
librdkafka, so no system package is needed). It is imported lazily so tests can
run against ``FakeConsumer`` without the dependency.

Offsets are committed explicitly after a message is fully processed and its
result written (at-least-once).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class Record:
    value: bytes
    key: bytes | None = None
    partition: int = 0
    offset: int = 0
    _raw: Any = None


class Consumer(Protocol):
    def poll(self, timeout: float) -> Record | None:
        ...

    def commit(self, record: Record) -> None:
        ...

    def close(self) -> None:
        ...


class ConfluentConsumer:
    """confluent-kafka consumer group member (one per worker process)."""

    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        from confluent_kafka import Consumer as _Consumer  # lazy

        self._c = _Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # explicit commit for at-least-once
            }
        )
        self._c.subscribe([topic])

    def poll(self, timeout: float) -> Record | None:
        from confluent_kafka import KafkaError  # lazy

        msg = self._c.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            err = msg.error()
            # Partition EOF is a benign "caught up" event, not a failure.
            if err.code() != KafkaError._PARTITION_EOF:
                # Surface stalls/broker errors instead of silently idling, which
                # would let consumer lag grow with no signal.
                logger.warning(
                    "kafka poll error (partition=%s): %s",
                    msg.partition() if msg else "?", err,
                )
            return None
        return Record(
            value=msg.value(),
            key=msg.key(),
            partition=msg.partition(),
            offset=msg.offset(),
            _raw=msg,
        )

    def commit(self, record: Record) -> None:
        if record._raw is not None:
            self._c.commit(message=record._raw, asynchronous=False)

    def lag(self) -> int:
        """Total consumer lag across this member's assigned partitions.

        Lag = high watermark - current position, summed over assignments. Best
        effort: returns 0 if no partitions are assigned yet.
        """
        total = 0
        assignment = self._c.assignment()
        if not assignment:
            return 0
        for tp in self._c.position(assignment):
            if tp.offset is None or tp.offset < 0:
                continue
            _low, high = self._c.get_watermark_offsets(tp, timeout=1.0, cached=True)
            if high is not None and high > tp.offset:
                total += high - tp.offset
        return total

    def close(self) -> None:
        self._c.close()


class FakeConsumer:
    """In-memory consumer for tests. Yields queued records, then None forever."""

    def __init__(self, records: list[Record]):
        self._records = list(records)
        self._idx = 0
        self.committed: list[int] = []
        self.closed = False

    def poll(self, timeout: float) -> Record | None:
        if self._idx >= len(self._records):
            return None
        rec = self._records[self._idx]
        rec.offset = self._idx
        self._idx += 1
        return rec

    def commit(self, record: Record) -> None:
        self.committed.append(record.offset)

    def close(self) -> None:
        self.closed = True
