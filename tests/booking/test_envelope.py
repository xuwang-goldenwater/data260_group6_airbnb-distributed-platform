"""Envelope validation before produce / after consume, and the DLQ record shape (00_conventions.md)."""
import copy
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from booking_core import envelope as env
from booking_core import events

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "shared/envelope.schema.json"
RES = {"reservation_id": "res-01JBOOK001", "property_id": "property-2001", "guest_id": "guest-1001",
       "host_id": "host-501", "check_in": date(2026, 12, 1), "check_out": date(2026, 12, 3), "guest_count": 2,
       "total_price": Decimal("400.00"), "version": 2, "trace_id": "tr-1",
       "created_datetime": datetime(2026, 11, 24, 10, 0, 0, 120000), "status": "pending_host"}
HIST = {"from_status": "requested", "to_status": "pending_host", "actor_id": "booking-worker", "reason": None,
        "trace_id": "tr-1", "changed_datetime": datetime(2026, 11, 24, 10, 0, 1)}


def requested():
    return events.booking_requested({**RES, "status": "requested", "version": 1})


def test_built_events_validate_against_shared_schema():
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()))
    evts = [requested()] + [e for _, _, e in events.events_for_transition(RES, HIST, producer="booking-worker")]
    assert [e["event_type"] for e in evts] == ["booking.requested", "booking.updated", "property.availability.updated"]
    for e in evts:
        assert list(validator.iter_errors(e)) == []
        env.validate(e, SCHEMA, key="property-2001", check_key=True)
    assert evts[1]["idempotency_key"] == "booking.updated:res-01JBOOK001:v2"
    assert evts[2]["payload"] == {"property_id": "property-2001", "change": "held",
                                  "nights": ["2026-12-01", "2026-12-02"], "reservation_id": "res-01JBOOK001"}
    assert evts[0]["timestamp"] == "2026-11-24T10:00:00.120Z"


def test_release_event_only_when_nights_were_held():
    cancel_requested = {**HIST, "from_status": "requested", "to_status": "cancelled"}
    cancel_confirmed = {**HIST, "from_status": "confirmed", "to_status": "cancelled"}
    assert len(events.events_for_transition(RES, cancel_requested, producer="x")) == 1
    evts = events.events_for_transition(RES, cancel_confirmed, producer="x")
    assert evts[1][2]["payload"]["change"] == "released"


def _mut(fn):
    e = requested()
    fn(e)
    return e


@pytest.mark.parametrize("mutate,code", [
    (lambda e: e.pop("trace_id"), "INVALID_EVENT_ENVELOPE"),
    (lambda e: e.update(schema_version="2.0"), "INVALID_EVENT_ENVELOPE"),
    (lambda e: e.update(attempt=0), "INVALID_EVENT_ENVELOPE"),
    (lambda e: e.update(extra_field=1), "INVALID_EVENT_ENVELOPE"),
    (lambda e: e.update(timestamp="yesterday"), "INVALID_EVENT_ENVELOPE"),
    (lambda e: e.update(event_type="booking.created"), "UNKNOWN_EVENT_TYPE"),
    (lambda e: e["payload"].pop("check_in"), "INVALID_PAYLOAD"),
    (lambda e: e["payload"].update(guest_count="two"), "INVALID_PAYLOAD"),
    (lambda e: e["payload"].update(check_out="2026-11-30"), "INVALID_PAYLOAD"),
])
def test_malformed_events_rejected(mutate, code):
    with pytest.raises(env.EnvelopeError) as e:
        env.validate(_mut(mutate), SCHEMA, expected_type="booking.requested", key="property-2001", check_key=True)
    assert e.value.code == code


def test_missing_field_message_names_the_field():
    with pytest.raises(env.EnvelopeError) as e:
        env.validate(_mut(lambda e: e["payload"].pop("check_in")), SCHEMA)
    assert e.value.message == "MISSING_FIELD:payload.check_in"


def test_key_mismatch_rejected():
    with pytest.raises(env.EnvelopeError) as e:
        env.validate(requested(), SCHEMA, key="property-9999", check_key=True)
    assert e.value.code == "EVENT_KEY_MISMATCH"


def test_dlq_record_has_conventions_fields_and_is_a_valid_envelope():
    original = requested()
    rec = env.dlq_record(original_topic="booking.requested", original_partition=1, original_offset=42,
                         original_key="property-2001", original_event=copy.deepcopy(original),
                         error_code="INVALID_PAYLOAD", error_message="MISSING_FIELD:payload.check_in",
                         retry_count=0, consumer_group="booking-worker", producer="booking-worker")
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()))
    assert list(validator.iter_errors(rec)) == []
    p = rec["payload"]
    for f in ("original_topic", "original_partition", "original_offset", "original_key", "original_event",
              "error_code", "error_message", "retry_count", "failed_at", "consumer_group"):
        assert f in p
    assert p["original_event"] == original
    assert rec["trace_id"] == original["trace_id"] and p["original_event_id"] == original["event_id"]
