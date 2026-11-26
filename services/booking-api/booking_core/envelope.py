"""Shared event envelope (shared/envelope.schema.json) + booking payload validation and DLQ records.

Every event is validated before it is produced and after it is consumed.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from . import ids
from .models import STATUSES
from .timeutil import iso, utcnow

SCHEMA_VERSION = "1.0"


class EnvelopeError(Exception):
    """A non-retryable event problem. `code` goes to the DLQ record's error_code."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@lru_cache(maxsize=4)
def _envelope_validator(schema_path: str) -> Draft202012Validator:
    schema = json.loads(Path(schema_path).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


_DATE = {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"}
_ID = {"type": "string", "minLength": 1, "maxLength": 40}
_STATUS = {"enum": list(STATUSES)}

PAYLOAD_SCHEMAS: dict[str, dict[str, Any]] = {
    "booking.requested": {
        "type": "object",
        "required": ["reservation_id", "property_id", "guest_id", "host_id", "check_in", "check_out", "guest_count"],
        "properties": {
            "reservation_id": _ID, "property_id": _ID, "guest_id": _ID, "host_id": _ID,
            "check_in": _DATE, "check_out": _DATE, "guest_count": {"type": "integer", "minimum": 1},
        },
    },
    "booking.updated": {
        "type": "object",
        "required": ["reservation_id", "property_id", "guest_id", "host_id", "from_status", "to_status", "reason",
                     "check_in", "check_out", "total_price", "version"],
        "properties": {
            "reservation_id": _ID, "property_id": _ID, "guest_id": _ID, "host_id": _ID,
            "from_status": {"anyOf": [_STATUS, {"type": "null"}]}, "to_status": _STATUS,
            "reason": {"type": ["string", "null"]}, "check_in": _DATE, "check_out": _DATE,
            "total_price": {"type": "number", "minimum": 0}, "version": {"type": "integer", "minimum": 1},
        },
    },
    "property.availability.updated": {
        "type": "object",
        "required": ["property_id", "change", "nights", "reservation_id"],
        "properties": {
            "property_id": _ID, "change": {"enum": ["held", "released"]},
            "nights": {"type": "array", "items": _DATE}, "reservation_id": _ID,
        },
    },
    "platform.dlq": {
        "type": "object",
        "required": ["original_topic", "original_partition", "original_offset", "original_key", "original_event",
                     "error_code", "error_message", "retry_count", "failed_at", "consumer_group"],
        "properties": {
            "original_topic": {"type": "string", "minLength": 1},
            "original_partition": {"type": ["integer", "null"]},
            "original_offset": {"type": ["integer", "null"]},
            "original_key": {"type": ["string", "null"]},
            "error_code": {"type": "string", "minLength": 1},
            "error_message": {"type": "string"},
            "retry_count": {"type": "integer", "minimum": 0},
            "failed_at": {"type": "string"},
            "consumer_group": {"type": "string", "minLength": 1},
        },
    },
}
_PAYLOAD_VALIDATORS = {k: Draft202012Validator(v) for k, v in PAYLOAD_SCHEMAS.items()}
# Kafka record key must equal this payload field (00_conventions.md topic registry).
KEY_FIELD = {"booking.requested": "property_id", "booking.updated": "property_id",
             "property.availability.updated": "property_id"}


def _first_error(validator: Draft202012Validator, instance: Any, prefix: str) -> str | None:
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path))
    if not errors:
        return None
    e = errors[0]
    path = ".".join(str(p) for p in e.absolute_path)
    if e.validator == "required":
        missing = e.message.split("'")[1] if "'" in e.message else e.message
        return f"MISSING_FIELD:{prefix}{(path + '.') if path else ''}{missing}"
    return f"{prefix}{path or '<root>'}: {e.message}"


def validate(envelope: Any, schema_path: str | Path, *, expected_type: str | None = None,
             key: str | None = None, check_key: bool = False) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise EnvelopeError("INVALID_EVENT_ENVELOPE", "record value is not a JSON object")
    problem = _first_error(_envelope_validator(str(schema_path)), envelope, "")
    if problem:
        raise EnvelopeError("INVALID_EVENT_ENVELOPE", problem)
    try:
        datetime.fromisoformat(envelope["timestamp"].replace("Z", "+00:00"))
    except ValueError:
        raise EnvelopeError("INVALID_EVENT_ENVELOPE", "timestamp: not an ISO 8601 date-time") from None
    etype = envelope["event_type"]
    if expected_type and etype != expected_type:
        raise EnvelopeError("UNKNOWN_EVENT_TYPE", f"expected {expected_type}, got {etype}")
    validator = _PAYLOAD_VALIDATORS.get(etype)
    if validator is None:
        raise EnvelopeError("UNKNOWN_EVENT_TYPE", f"no payload schema for {etype}")
    problem = _first_error(validator, envelope["payload"], "payload.")
    if problem:
        raise EnvelopeError("INVALID_PAYLOAD", problem)
    payload = envelope["payload"]
    for field in ("check_in", "check_out"):
        if field in payload:
            try:
                date.fromisoformat(payload[field])
            except ValueError:
                raise EnvelopeError("INVALID_PAYLOAD", f"payload.{field}: not a valid date") from None
    if "check_in" in payload and "check_out" in payload and payload["check_out"] <= payload["check_in"]:
        raise EnvelopeError("INVALID_PAYLOAD", "payload.check_out must be later than payload.check_in")
    if check_key:
        expected_key = payload[KEY_FIELD[etype]]
        if key != expected_key:
            raise EnvelopeError("EVENT_KEY_MISMATCH",
                                f"kafka key {key!r} != payload.{KEY_FIELD[etype]} {expected_key!r}")
    return envelope


def build(*, event_type: str, trace_id: str, actor_id: str, entity_type: str, entity_id: str,
          payload: dict[str, Any], idempotency_key: str, producer: str, timestamp: datetime | None = None,
          attempt: int = 1) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "trace_id": trace_id,
        "timestamp": iso(timestamp or utcnow()),
        "actor_id": actor_id,
        "entity": {"entity_type": entity_type, "entity_id": entity_id},
        "payload": payload,
        "idempotency_key": idempotency_key,
        "attempt": attempt,
        "event_id": ids.event_id(),
        "producer": producer,
        "schema_version": SCHEMA_VERSION,
    }


def dlq_record(*, original_topic: str, original_partition: int | None, original_offset: int | None,
               original_key: str | None, original_event: Any, error_code: str, error_message: str,
               retry_count: int, consumer_group: str, producer: str) -> dict[str, Any]:
    """platform.dlq record: a shared envelope whose payload carries the conventions DLQ fields."""
    orig = original_event if isinstance(original_event, dict) else {}
    trace = orig.get("trace_id") if isinstance(orig.get("trace_id"), str) and orig.get("trace_id") else ids.trace_id()
    orig_event_id = orig.get("event_id") if isinstance(orig.get("event_id"), str) and orig.get("event_id") else None
    now = utcnow()
    payload = {
        "original_topic": original_topic,
        "original_partition": original_partition,
        "original_offset": original_offset,
        "original_key": original_key,
        "original_event": original_event,
        "error_code": error_code,
        "error_message": error_message[:1000],
        "retry_count": retry_count,
        "failed_at": iso(now),
        "consumer_group": consumer_group,
        "original_event_id": orig_event_id,
        "trace_id": trace,
    }
    return build(event_type="platform.dlq", trace_id=trace, actor_id=producer, entity_type="event",
                 entity_id=orig_event_id or f"{original_topic}:{original_partition}:{original_offset}",
                 payload=payload,
                 idempotency_key=f"dlq:{consumer_group}:{original_topic}:{original_partition}:{original_offset}",
                 producer=producer, timestamp=now)
