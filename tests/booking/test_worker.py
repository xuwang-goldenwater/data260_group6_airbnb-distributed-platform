"""Integration: worker processing (steps 1-11 minus Kafka), dedup, DLQ routing and retries."""
from __future__ import annotations

import json

from booking_worker.consumer_utils import RecordMeta
from conftest import future, run_worker_on_requested
from test_api_flow import body, create

P = "property-2001"


def _count(db, sql, *args):
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchone().values())[0]


def _requested_event(client, auth, producer, **kw):
    r = create(client, auth, b=body(**kw))
    assert r.status_code == 202
    return r.json()["reservation_id"], producer.sent[-1][2]


def feed(worker, event, key=P, offset=0):
    raw = event if isinstance(event, (bytes, str)) else json.dumps(event)
    return worker.handle(RecordMeta("booking.requested", 0, offset, key), raw.encode() if isinstance(raw, str) else raw)


def test_redelivery_is_a_no_op(client, auth, worker, producer, clean_db):
    rid, ev = _requested_event(client, auth, producer)
    assert feed(worker, ev).kind == "held"
    n_events = len(producer.sent)
    assert feed(worker, ev).kind == "duplicate"
    assert feed(worker, ev).kind == "duplicate"
    assert len(producer.sent) == n_events                    # nothing re-published (publish_pending was cleared)
    assert _count(clean_db, "SELECT COUNT(*) FROM reservation_status_history WHERE reservation_id=%s", rid) == 2
    assert _count(clean_db, "SELECT COUNT(*) FROM availability_blocks WHERE reservation_id=%s", rid) == 2
    assert _count(clean_db, "SELECT COUNT(*) FROM processed_events WHERE idempotency_key=%s",
                  f"booking.requested:{rid}") == 1


def test_redelivery_after_crash_republishes_pending_events_once(client, auth, worker, producer, clean_db):
    rid, ev = _requested_event(client, auth, producer)
    producer.fail = True                                      # simulate: committed to MySQL, publish lost
    assert feed(worker, ev).kind == "held"
    producer.fail = False
    assert _count(clean_db, "SELECT publish_pending FROM reservations WHERE reservation_id=%s", rid) == 1
    assert feed(worker, ev).kind == "duplicate"
    assert [t for t, _, e in producer.sent if e["payload"].get("reservation_id") == rid][-2:] == [
        "booking.updated", "property.availability.updated"]
    assert _count(clean_db, "SELECT publish_pending FROM reservations WHERE reservation_id=%s", rid) == 0


def test_conflict_rejects_without_partial_hold(client, auth, worker, producer, clean_db, settings):
    import dataclasses
    client.app.state.booking["svc"].settings = dataclasses.replace(settings, precheck_enabled=False)
    a, ev_a = _requested_event(client, auth, producer, ci=10, co=12)
    b, ev_b = _requested_event(client, auth, producer, ci=11, co=14)      # overlaps night +11 only
    assert feed(worker, ev_a).kind == "held"
    assert feed(worker, ev_b).kind == "rejected"
    assert _count(clean_db, "SELECT COUNT(*) FROM availability_blocks WHERE reservation_id=%s", b) == 0
    assert _count(clean_db, "SELECT status_reason FROM reservations WHERE reservation_id=%s", b) == "DATES_UNAVAILABLE"
    upd = [e for t, _, e in producer.sent if t == "booking.updated" and e["payload"]["reservation_id"] == b]
    assert upd[0]["payload"]["to_status"] == "rejected" and upd[0]["payload"]["reason"] == "DATES_UNAVAILABLE"
    assert not [e for t, _, e in producer.sent if t == "property.availability.updated"
                and e["payload"]["reservation_id"] == b]


def _dlq(producer):
    return [e for t, _, e in producer.sent if t == "platform.dlq"]


def test_malformed_and_key_mismatch_go_to_dlq_without_retry(client, auth, worker, producer):
    rid, ev = _requested_event(client, auth, producer)
    bad = json.loads(json.dumps(ev))
    del bad["payload"]["check_in"]
    assert feed(worker, bad, offset=7).kind == "dlq"
    assert feed(worker, ev, key="property-9999", offset=8).kind == "dlq"
    assert feed(worker, b"{not json", offset=9).kind == "dlq"
    recs = _dlq(producer)
    assert [r["payload"]["error_code"] for r in recs] == ["INVALID_PAYLOAD", "EVENT_KEY_MISMATCH", "INVALID_JSON"]
    p = recs[0]["payload"]
    assert p["error_message"] == "MISSING_FIELD:payload.check_in" and p["retry_count"] == 0
    assert p["original_topic"] == "booking.requested" and p["original_offset"] == 7 and p["original_key"] == P
    assert p["consumer_group"] == "booking-worker" and p["original_event"]["event_id"] == ev["event_id"]
    assert recs[0]["trace_id"] == ev["trace_id"] and worker.sleeps == []
    assert feed(worker, ev).kind == "held"                    # the valid record is still processed afterwards


def test_unknown_reservation_retried_once_then_dlq(worker, producer):
    from booking_core import events
    from datetime import datetime
    ghost = {"reservation_id": "res-ghost", "property_id": P, "guest_id": "guest-1001", "host_id": "host-501",
             "check_in": future(5), "check_out": future(6), "guest_count": 1, "trace_id": "tr-ghost",
             "created_datetime": datetime(2026, 11, 24)}
    assert feed(worker, events.booking_requested(ghost)).kind == "dlq"
    assert worker.sleeps == [1.0]
    assert _dlq(producer)[0]["payload"]["error_code"] == "RESERVATION_NOT_FOUND"


def test_transient_errors_retried_with_backoff(client, auth, worker, producer):
    rid, ev = _requested_event(client, auth, producer)
    worker._faults_left = 3
    out = feed(worker, ev)
    assert out.kind == "held" and out.retries == 3 and worker.sleeps == [0.2, 1.0, 5.0]


def test_transient_errors_exhausted_go_to_dlq(client, auth, worker, producer, clean_db):
    rid, ev = _requested_event(client, auth, producer)
    worker._faults_left = 4
    assert feed(worker, ev).kind == "dlq"
    rec = _dlq(producer)[0]["payload"]
    assert rec["error_code"] == "TRANSIENT_RETRY_EXHAUSTED" and rec["retry_count"] == 3
    assert _count(clean_db, "SELECT status FROM reservations WHERE reservation_id=%s", rid) == "requested"


def test_worker_validates_published_events(client, auth, worker, producer):
    """Everything the worker produced passed the shared schema (InMemoryProducer validates on publish)."""
    _requested_event(client, auth, producer)
    run_worker_on_requested(worker, producer)
    assert {t for t, _, _ in producer.sent} == {"booking.requested", "booking.updated", "property.availability.updated"}
