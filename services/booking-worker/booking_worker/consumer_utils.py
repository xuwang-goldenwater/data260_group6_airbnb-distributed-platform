"""Retry / DLQ / decode helpers for Kafka consumers (00_conventions.md "Validation, retry, idempotency, DLQ").

Retry policy comes from Matthew's shared/consumer.py. Booking-specific payload validation,
transactional idempotency and DLQ envelope construction remain owned by Booking.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from booking_core import envelope as env
from booking_core.events import T_DLQ
from booking_core.kafka_producer import EventProducer
from shared.consumer import run_with_retry, RetryExhausted

log = logging.getLogger("booking.consumer")
T = TypeVar("T")

BACKOFF_S: tuple[float, ...] = (0.2, 1.0, 5.0)   # 3 in-place retries: 200 ms, 1 s, 5 s


@dataclass(frozen=True)
class RecordMeta:
    topic: str
    partition: int | None
    offset: int | None
    key: str | None


class PermanentError(Exception):
    """Business data that can never be processed (goes to the DLQ without retry)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def decode(value: bytes | None, meta: RecordMeta, schema_path, *, expected_type: str) -> dict[str, Any]:
    """JSON-decode and validate envelope + payload + key. Raises EnvelopeError (non-retryable)."""
    if value is None:
        raise env.EnvelopeError("INVALID_JSON", "empty record value")
    try:
        event = json.loads(value)
    except ValueError as exc:
        raise env.EnvelopeError("INVALID_JSON", str(exc)) from None
    return env.validate(event, schema_path, expected_type=expected_type, key=meta.key, check_key=True)


def raw_event(value: bytes | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except ValueError:
        return value.decode(errors="replace")


def send_to_dlq(producer: EventProducer, meta: RecordMeta, original: Any, *, error_code: str, error_message: str,
                retry_count: int, consumer_group: str, producer_name: str) -> dict[str, Any]:
    record = env.dlq_record(original_topic=meta.topic, original_partition=meta.partition,
                            original_offset=meta.offset, original_key=meta.key, original_event=original,
                            error_code=error_code, error_message=error_message, retry_count=retry_count,
                            consumer_group=consumer_group, producer=producer_name)
    producer.publish(T_DLQ, meta.key, record)   # raises if Kafka is down → offset is NOT committed
    log.error("sent to %s error_code=%s retry_count=%d topic=%s partition=%s offset=%s trace_id=%s msg=%s",
              T_DLQ, error_code, retry_count, meta.topic, meta.partition, meta.offset,
              record["payload"]["trace_id"], error_message)
    return record
