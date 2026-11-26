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


# --------------------------------------------------------------------------------------- transitions
def _act(client, auth, action, sub, res, version=None, key="auto", **extra):
    return client.post(f"/bookings/{action}", headers=auth(sub, key=key),
                       json={"reservation_id": res["reservation_id"],
                             "expected_version": version if version is not None else res["version"], **extra})


def test_worker_holds_nights_then_host_approves(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    assert res["status"] == "pending_host" and res["version"] == 2
    assert [h["to_status"] for h in res["status_history"]] == ["requested", "pending_host"]
    producer.sent.clear()
    r = _act(client, auth, "approve", HOST, res)
    assert r.status_code == 200 and r.json()["status"] == "confirmed" and r.json()["version"] == 3
    assert producer.topics() == ["booking.updated"]
    ev = producer.sent[0][2]
    assert ev["payload"]["from_status"] == "pending_host" and ev["payload"]["to_status"] == "confirmed"
    assert ev["idempotency_key"] == f"booking.updated:{res['reservation_id']}:v3"


def test_approve_replay_with_same_key_returns_200_once(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    producer.sent.clear()
    r1 = _act(client, auth, "approve", HOST, res, key="approve-k")
    r2 = _act(client, auth, "approve", HOST, res, key="approve-k")
    assert r1.status_code == r2.status_code == 200 and r1.json() == r2.json()
    assert producer.topics() == ["booking.updated"]


def test_approve_rules(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    r = _act(client, auth, "approve", "host-502", res)
    assert r.status_code == 403 and r.json()["error"]["code"] == "NOT_PROPERTY_HOST"
    r = _act(client, auth, "approve", HOST, res, version=1)
    assert r.status_code == 409 and r.json()["error"]["code"] == "RESERVATION_VERSION_CONFLICT"
    assert _act(client, auth, "approve", HOST, res).status_code == 200
    r = _act(client, auth, "approve", HOST, {**res, "version": 3})
    assert r.status_code == 409 and r.json()["error"]["code"] == "INVALID_STATUS_TRANSITION"
    r = client.post("/bookings/approve", headers=auth(HOST), json={"reservation_id": "res-nope", "expected_version": 1})
    assert r.status_code == 404 and r.json()["error"]["code"] == "RESERVATION_NOT_FOUND"


def test_approve_requested_is_invalid_transition(client, auth):
    rid = create(client, auth).json()["reservation_id"]
    r = client.post("/bookings/approve", headers=auth(HOST), json={"reservation_id": rid, "expected_version": 1})
    assert r.status_code == 409 and r.json()["error"]["code"] == "INVALID_STATUS_TRANSITION"


def test_decline_releases_nights(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    producer.sent.clear()
    r = _act(client, auth, "decline", HOST, res, reason="maintenance")
    assert r.status_code == 200 and r.json()["status"] == "declined" and r.json()["status_reason"] == "HOST_DECLINED"
    assert producer.topics() == ["booking.updated", "property.availability.updated"]
    assert producer.sent[1][2]["payload"]["change"] == "released"
    cal = client.post("/bookings/calendar", json={"property_id": P, "from_date": future(0).isoformat(),
                                                  "to_date": future(60).isoformat()}).json()
    assert cal["unavailable_nights"] == []
    assert create(client, auth, sub=OTHER).status_code == 202   # nights are bookable again


def test_guest_cancel_confirmed_before_check_in(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    res = _act(client, auth, "approve", HOST, res).json()
    r = _act(client, auth, "cancel", GUEST, res, reason="plans changed")
    j = r.json()
    assert r.status_code == 200 and j["status"] == "cancelled" and j["cancelled_by"] == "guest"
    assert j["status_reason"] == "GUEST_CANCELLED"


def test_host_cancel_pending(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    j = _act(client, auth, "cancel", HOST, res).json()
    assert j["cancelled_by"] == "host" and j["status_reason"] == "HOST_CANCELLED"


def test_cancel_rules(client, auth, worker, producer, monkeypatch):
    res = held_booking(client, auth, worker, producer)
    r = _act(client, auth, "cancel", OTHER, res)
    assert r.status_code == 403 and r.json()["error"]["code"] == "NOT_RESERVATION_PARTY"
    res = _act(client, auth, "approve", HOST, res).json()
    monkeypatch.setenv("BOOKING_CLOCK_DATE", future(10).isoformat())        # check-in day
    r = _act(client, auth, "cancel", GUEST, res)
    assert r.status_code == 422 and r.json()["error"]["code"] == "CANCELLATION_WINDOW_CLOSED"
    monkeypatch.delenv("BOOKING_CLOCK_DATE")
    assert _act(client, auth, "cancel", GUEST, res).status_code == 200
    r = _act(client, auth, "cancel", GUEST, {**res, "version": res["version"] + 1})
    assert r.status_code == 409 and r.json()["error"]["code"] == "INVALID_STATUS_TRANSITION"


def test_cancel_requested_before_worker_then_worker_skips(client, auth, worker, producer):
    rid = create(client, auth).json()["reservation_id"]
    r = client.post("/bookings/cancel", headers=auth(GUEST), json={"reservation_id": rid, "expected_version": 1})
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert [o.kind for o in run_worker_on_requested(worker, producer)] == ["skipped"]
    hist = client.post("/bookings/get", json={"reservation_id": rid}, headers=auth(GUEST, key=None)).json()
    assert [h["to_status"] for h in hist["status_history"]] == ["requested", "cancelled"]


def test_completion_job(client, auth, worker, producer, monkeypatch):
    res = held_booking(client, auth, worker, producer, ci=1, co=3)
    _act(client, auth, "approve", HOST, res)
    svc = client.app.state.booking["svc"]
    assert svc.complete_stays()["completed"] == 0                          # check_out not passed yet
    monkeypatch.setenv("BOOKING_CLOCK_DATE", future(4).isoformat())
    out = svc.complete_stays()
    assert out["completed"] == 1 and out["reservation_ids"] == [res["reservation_id"]]
    monkeypatch.delenv("BOOKING_CLOCK_DATE")
    j = client.post("/bookings/get", json={"reservation_id": res["reservation_id"]}, headers=auth(GUEST, key=None)).json()
    assert j["status"] == "completed" and j["status_history"][-1]["actor_id"] == "completion-job"


# --------------------------------------------------------------------------------------- reads
def test_get_is_party_only(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    r = client.post("/bookings/get", json={"reservation_id": res["reservation_id"]}, headers=auth(OTHER, key=None))
    assert r.status_code == 403 and r.json()["error"]["code"] == "NOT_RESERVATION_PARTY"
    assert client.post("/bookings/get", json={"reservation_id": res["reservation_id"]},
                       headers=auth(HOST, key=None)).status_code == 200
    r = client.post("/bookings/get", json={"reservation_id": "res-x"}, headers=auth(GUEST, key=None))
    assert r.status_code == 404


def test_by_guest_and_by_host(client, auth, worker, producer):
    for i in range(3):
        create(client, auth, b=body(10 + 3 * i, 12 + 3 * i))
    run_worker_on_requested(worker, producer)
    g = client.post("/bookings/byGuest", json={"page": 1, "page_size": 2}, headers=auth(GUEST, key=None)).json()
    assert g["total"] == 3 and len(g["items"]) == 2 and g["page_size"] == 2
    g = client.post("/bookings/byGuest", json={"status": ["confirmed"]}, headers=auth(GUEST, key=None)).json()
    assert g["total"] == 0
    h = client.post("/bookings/byHost", json={"property_id": P}, headers=auth(HOST, key=None)).json()
    assert h["total"] == 3 and {"guest_id", "guest_count"} <= set(h["items"][0])
    r = client.post("/bookings/byHost", json={}, headers=auth(GUEST, key=None))
    assert r.status_code == 403 and r.json()["error"]["code"] == "HOST_PROFILE_REQUIRED"
    r = client.post("/bookings/byGuest", json={"page_size": 51}, headers=auth(GUEST, key=None))
    assert r.status_code == 400


def test_calendar_validation_and_public(client):
    r = client.post("/bookings/calendar", json={"property_id": P, "from_date": "2026-01-01", "to_date": "2027-06-01"})
    assert r.status_code == 400
    r = client.post("/bookings/calendar", json={"property_id": P, "from_date": "2026-01-01", "to_date": "2026-01-31"})
    assert r.status_code == 200 and r.json()["unavailable_nights"] == []


# --------------------------------------------------------------------------------------- internal
def test_internal_endpoints_need_service_auth(client, auth, worker, producer, settings):
    from conftest import make_token
    res = held_booking(client, auth, worker, producer)
    url = f"/internal/reservations/{res['reservation_id']}"
    assert client.get(url).status_code == 401
    assert client.get(url, headers=auth(GUEST, key=None)).status_code == 403
    svc_h = {"Authorization": f"Bearer {make_token(settings, 'review-service', role='service')}"}
    j = client.get(url, headers=svc_h).json()
    assert j == {"reservation_id": res["reservation_id"], "property_id": P, "guest_id": GUEST, "host_id": HOST,
                 "status": "pending_host", "check_in": res["check_in"], "check_out": res["check_out"]}
    assert client.get("/internal/reservations/res-none", headers={"X-Service-Token": "svc-test-token"}).status_code == 404
    chk = client.post("/internal/availability/check", headers=svc_h, json={
        "property_ids": [P, "property-2007"], "check_in": future(11).isoformat(), "check_out": future(13).isoformat()})
    assert chk.json()["results"] == [{"property_id": P, "available": False},
                                     {"property_id": "property-2007", "available": True}]
    too_many = {"property_ids": [f"property-{i}" for i in range(201)], "check_in": future(1).isoformat(),
                "check_out": future(2).isoformat()}
    assert client.post("/internal/availability/check", headers=svc_h, json=too_many).status_code == 400
    assert client.get(f"/internal/properties/{P}/active-reservations", headers=svc_h).json() == {
        "property_id": P, "has_active_reservations": True, "count": 1}
    assert client.get(f"/internal/guests/{OTHER}/active-reservations", headers=svc_h).json() == {
        "guest_id": OTHER, "has_active_reservations": False, "count": 0}




def test_rate_limit_429(client, auth):
    from booking_api.ratelimit import RateLimiter
    client.app.state.booking["limiter"] = RateLimiter(1)
    assert create(client, auth).status_code == 202
    r = create(client, auth, b=body(20, 22))
    assert r.status_code == 429 and r.json()["error"]["code"] == "RATE_LIMITED" and "Retry-After" in r.headers
