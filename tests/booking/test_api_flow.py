"""Integration: Booking API against MySQL (airbnb_booking_test) + in-memory Kafka producer + real worker logic."""
from __future__ import annotations

import pytest

from conftest import future, run_worker_on_requested

P = "property-2001"          # seed: host-501, capacity 4, $180/night, $40 cleaning, active
HOST, GUEST, OTHER = "host-501", "guest-1001", "guest-1002"


def body(ci=10, co=12, **kw):
    return {"property_id": P, "check_in": future(ci).isoformat(), "check_out": future(co).isoformat(),
            "guest_count": 2, **kw}


def create(client, auth, sub=GUEST, b=None, key="auto"):
    return client.post("/bookings/create", json=b or body(), headers=auth(sub, key=key))


def held_booking(client, auth, worker, producer, ci=10, co=12, sub=GUEST):
    r = create(client, auth, sub, body(ci, co))
    assert r.status_code == 202, r.text
    run_worker_on_requested(worker, producer)
    return client.post("/bookings/get", json={"reservation_id": r.json()["reservation_id"]},
                       headers=auth(sub, key=None)).json()


# --------------------------------------------------------------------------------------- create + validation
def test_create_returns_202_and_publishes_booking_requested(client, auth, producer):
    r = create(client, auth)
    assert r.status_code == 202
    j = r.json()
    assert j["status"] == "requested" and j["nights"] == 2 and j["total_price"] == 400.0
    assert j["reservation_id"].startswith("res-") and r.headers["X-Trace-Id"] == j["trace_id"]
    assert producer.topics() == ["booking.requested"]
    topic, key, ev = producer.sent[0]
    assert key == P and ev["payload"]["host_id"] == HOST and ev["payload"]["guest_id"] == GUEST
    assert ev["idempotency_key"] == f"booking.requested:{j['reservation_id']}"


@pytest.mark.parametrize("b,status,code", [
    ({"property_id": P}, 400, "VALIDATION_ERROR"),
    (body(co=10), 400, "VALIDATION_ERROR"),
    (body(ci=-1, co=1), 400, "VALIDATION_ERROR"),
    (body(ci=1, co=32), 400, "VALIDATION_ERROR"),
    (body(guest_count=0), 400, "VALIDATION_ERROR"),
    ({**body(), "property_id": "property-9999"}, 404, "PROPERTY_NOT_FOUND"),
    ({**body(), "property_id": "property-2198"}, 404, "PROPERTY_NOT_FOUND"),   # draft property
    (body(guest_count=5), 422, "CAPACITY_EXCEEDED"),
])
def test_create_errors(client, auth, b, status, code):
    r = create(client, auth, b=b)
    assert r.status_code == status and r.json()["error"]["code"] == code
    assert set(r.json()["error"]) >= {"code", "message", "trace_id", "retryable"}


def test_host_cannot_book_own_property(client, auth):
    r = create(client, auth, sub=HOST)
    assert r.status_code == 403 and r.json()["error"]["code"] == "HOST_CANNOT_BOOK_OWN_PROPERTY"


def test_auth_and_idempotency_key_required(client, auth):
    assert client.post("/bookings/create", json=body()).status_code == 401
    bad = {"Authorization": "Bearer not-a-jwt", "Idempotency-Key": "k"}
    assert client.post("/bookings/create", json=body(), headers=bad).json()["error"]["code"] == "UNAUTHORIZED"
    r = client.post("/bookings/create", json=body(), headers=auth(GUEST, key=None))
    assert r.status_code == 400 and r.json()["error"]["details"]["header"] == "Idempotency-Key"
    assert client.post("/bookings/create", content=b"{not json", headers=auth(GUEST)).status_code == 400


def test_precheck_rejects_taken_nights_with_409(client, auth, worker, producer):
    held_booking(client, auth, worker, producer)
    r = create(client, auth, sub=OTHER)
    assert r.status_code == 409 and r.json()["error"]["code"] == "DATES_UNAVAILABLE"
    assert r.json()["error"]["details"]["conflicting_nights"] == [future(10).isoformat(), future(11).isoformat()]


# --------------------------------------------------------------------------------------- REST idempotency
def test_same_key_same_body_replays_without_second_row_or_event(client, auth, producer, clean_db):
    r1 = create(client, auth, key="key-replay-1")
    r2 = create(client, auth, key="key-replay-1")
    assert r1.status_code == r2.status_code == 202
    assert r1.json() == r2.json() and r2.headers.get("Idempotent-Replayed") == "true"
    assert producer.topics() == ["booking.requested"]
    with clean_db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM reservations")
        assert cur.fetchone()["n"] == 1


def test_same_key_different_body_is_422(client, auth):
    assert create(client, auth, key="key-reuse-1").status_code == 202
    r = create(client, auth, b=body(guest_count=3), key="key-reuse-1")
    assert r.status_code == 422 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_key_is_scoped_per_subject(client, auth):
    assert create(client, auth, sub=GUEST, key="shared-key").status_code == 202
    r = create(client, auth, sub=OTHER, b=body(20, 22), key="shared-key")
    assert r.status_code == 202


def test_new_key_double_click_creates_second_request_that_worker_rejects(client, auth, worker, producer, settings):
    import dataclasses
    # Pre-check off so both requests reach the worker (as in the double-booking demo).
    worker.s = dataclasses.replace(settings, precheck_enabled=False)
    client.app.state.booking["svc"].settings = dataclasses.replace(settings, precheck_enabled=False)
    a, b = create(client, auth), create(client, auth)
    assert a.status_code == b.status_code == 202
    outcomes = sorted(o.kind for o in run_worker_on_requested(worker, producer))
    assert outcomes == ["held", "rejected"]




def test_rate_limit_429(client, auth):
    from booking_api.ratelimit import RateLimiter
    client.app.state.booking["limiter"] = RateLimiter(1)
    assert create(client, auth).status_code == 202
    r = create(client, auth, b=body(20, 22))
    assert r.status_code == 429 and r.json()["error"]["code"] == "RATE_LIMITED" and "Retry-After" in r.headers
