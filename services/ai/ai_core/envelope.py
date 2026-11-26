"""Build and validate the shared Kafka event envelope (shared/envelope.schema.json).

Every AI event is validated before it is produced and after it is consumed, like the Booking service does.
Payload checks here are deliberately light: the detailed agent payload rules live in harness.py.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMA_VERSION = "1.0"
PRODUCER = "ai-service"


class EnvelopeError(Exception):
    """A non-retryable problem with an event. `code` is used as the DLQ error_code."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:20]}"


@lru_cache(maxsize=4)
def _validator(schema_path: str) -> Draft202012Validator:
    schema = json.loads(Path(schema_path).read_text())
    return Draft202012Validator(schema)


def build(*, event_type: str, trace_id: str, actor_id: str, entity_type: str, entity_id: str,
          payload: dict[str, Any], idempotency_key: str, attempt: int = 1) -> dict[str, Any]:
    """Return a complete envelope. trace_id is also used as the Kafka key for every AI event."""
    return {
        "event_type": event_type,
        "trace_id": trace_id,
        "timestamp": utc_now_iso(),
        "actor_id": actor_id,
        "entity": {"entity_type": entity_type, "entity_id": entity_id},
        "payload": payload,
        "idempotency_key": idempotency_key,
        "attempt": attempt,
        "event_id": new_id("ev"),
        "producer": PRODUCER,
        "schema_version": SCHEMA_VERSION,
    }


def validate(event: Any, schema_path: str | Path, *, key: str | None = None) -> dict[str, Any]:
    """Check the shared envelope schema and that the Kafka key equals trace_id."""
    if not isinstance(event, dict):
        raise EnvelopeError("INVALID_EVENT_ENVELOPE", "record value is not a JSON object")
    errors = sorted(_validator(str(schema_path)).iter_errors(event), key=lambda e: list(e.absolute_path))
    if errors:
        e = errors[0]
        where = ".".join(str(p) for p in e.absolute_path) or "<root>"
        raise EnvelopeError("INVALID_EVENT_ENVELOPE", f"{where}: {e.message}")
    if key is not None and key != event["trace_id"]:
        raise EnvelopeError("EVENT_KEY_MISMATCH", f"kafka key {key!r} != trace_id {event['trace_id']!r}")
    return event


def decode(value: bytes | None, schema_path: str | Path, *, key: str | None = None) -> dict[str, Any]:
    if value is None:
        raise EnvelopeError("INVALID_JSON", "empty record value")
    try:
        event = json.loads(value)
    except ValueError as exc:
        raise EnvelopeError("INVALID_JSON", str(exc)) from None
    return validate(event, schema_path, key=key)
