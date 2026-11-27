"""Review service tests. No MongoDB, Kafka or Booking needed.

Run (from services/review):  python3 -m pytest -q tests
Service-level tests need only the standard library; the HTTP tests are
skipped when FastAPI is not installed.
"""
import json
import sys
import time
from datetime import date
from pathlib import Path

import pytest

SERVICE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_DIR))
from review_core.service import ApiError, DependencyUnavailable, ReviewService  # noqa: E402
from review_core.store import MemoryReviewStore  # noqa: E402
from review_core.tagger import tag  # noqa: E402

SEED = SERVICE_DIR.parents[1] / "data" / "seed" / "generated" / "reviews.json"
GUEST = {"sub": "guest-1", "role": "guest"}
HOST = {"sub": "host-1", "role": "host"}
BODY = {"reservation_id": "res-1", "rating": 4, "review_text": "The kitchen was dirty but the host replied."}


class FakeBooking:
    def __init__(self):
        self.down = False
        self.reservations = {
            "res-1": dict(reservation_id="res-1", property_id="property-1", guest_id="guest-1",
                          host_id="host-1", status="completed", check_in="2026-11-01", check_out="2026-11-03"),
            "res-2": dict(reservation_id="res-2", property_id="property-1", guest_id="guest-1",
                          host_id="host-1", status="confirmed", check_in="2026-12-01", check_out="2026-12-03"),
        }

    def get_reservation(self, rid):
        if self.down:
            raise DependencyUnavailable("booking down")
        return self.reservations.get(rid)


class FakeKafka:
    def __init__(self):
        self.fail, self.sent = False, []

    def publish(self, topic, key, envelope):
        if self.fail:
            raise RuntimeError("kafka down")
        self.sent.append((topic, key, envelope))


@pytest.fixture
def env():
    store, booking, kafka = MemoryReviewStore(), FakeBooking(), FakeKafka()
    svc = ReviewService(store, booking, kafka, today=lambda: date(2026, 11, 26))
    return svc, store, booking, kafka


def code(fn, *args):
    with pytest.raises(ApiError) as e:
        fn(*args)
    return e.value.status, e.value.code


# ---------------------------------------------------------------- tagger
def test_tagger_reproduces_all_seed_tags():
    reviews = json.loads(SEED.read_text())
    assert all(tag(r["review_text"]) == sorted(r["tags"]) for r in reviews)


def test_tagger_multi_and_positive():
    assert tag("Wifi was not working and there was a hidden fee.") == ["missing_amenity", "price_mismatch"]
    assert tag("Great stay, very clean!") == ["positive"]


# ---------------------------------------------------------------- create
def test_create_publishes_review_created(env):
    svc, store, _, kafka = env
    status, review = svc.create(GUEST, BODY, "k1", "tr-1")
    assert status == 201 and review["tags"] == ["cleanliness"]
    assert review["property_id"] == "property-1" and review["host_id"] == "host-1"   # copied from Booking
    topic, key, ev = kafka.sent[0]
    assert (topic, key) == ("review.created", "property-1")
    assert ev["idempotency_key"] == f"review.created:{review['review_id']}" and ev["trace_id"] == "tr-1"
    assert "review_text" not in ev["payload"] and store.pending() == []


def test_idempotent_replay_returns_first_result(env):
    svc, store, _, kafka = env
    _, first = svc.create(GUEST, BODY, "k1", "tr")
    status, again = svc.create(GUEST, BODY, "k1", "tr")
    assert status == 200 and again["review_id"] == first["review_id"]
    assert len(store.docs) == 1 and len(kafka.sent) == 1


@pytest.mark.parametrize("change,expected", [
    ({"rating": 6}, (400, "VALIDATION_ERROR")),
    ({"rating": True}, (400, "VALIDATION_ERROR")),
    ({"review_text": "short"}, (400, "VALIDATION_ERROR")),
    ({"reservation_id": "res-x"}, (404, "RESERVATION_NOT_FOUND")),
    ({"reservation_id": "res-2"}, (422, "STAY_NOT_COMPLETED")),
])
def test_create_rejects(env, change, expected):
    svc = env[0]
    assert code(svc.create, GUEST, dict(BODY, **change), "k", "tr") == expected


def test_only_the_reservation_guest_can_review(env):
    assert code(env[0].create, {"sub": "guest-9", "role": "guest"}, BODY, "k", "tr") == (403, "NOT_RESERVATION_GUEST")


def test_one_review_per_reservation(env):
    svc = env[0]
    svc.create(GUEST, BODY, "k1", "tr")
    assert code(svc.create, GUEST, BODY, "k2", "tr") == (409, "REVIEW_ALREADY_EXISTS")


def test_booking_down_writes_nothing(env):
    svc, store, booking, _ = env
    booking.down = True
    assert code(svc.create, GUEST, BODY, "k", "tr") == (503, "DEPENDENCY_UNAVAILABLE")
    assert store.docs == {}


def test_idempotency_key_required(env):
    assert code(env[0].create, GUEST, BODY, "", "tr") == (400, "VALIDATION_ERROR")


def test_kafka_down_keeps_review_and_republishes_once(env):
    svc, store, _, kafka = env
    kafka.fail = True
    status, review = svc.create(GUEST, BODY, "k1", "tr")
    assert status == 201 and len(store.pending()) == 1 and kafka.sent == []
    kafka.fail = False
    assert svc.flush_pending() == 1 and svc.flush_pending() == 0
    assert kafka.sent[0][2]["idempotency_key"] == f"review.created:{review['review_id']}"


# ---------------------------------------------------------------- read
def test_by_property_summary_and_unknown_property(env):
    svc = env[0]
    svc.create(GUEST, BODY, "k1", "tr")
    out = svc.by_property({"property_id": "property-1"})
    assert out["summary"] == {"review_count": 1, "average_rating": 4.0, "tag_counts": {"cleanliness": 1}}
    assert svc.by_property({"property_id": "property-new"})["total"] == 0
    assert code(svc.by_property, {"property_id": "property-1", "page_size": 99}) == (400, "VALIDATION_ERROR")


def test_get_unknown_review(env):
    assert code(env[0].get, {"review_id": "rev-x"}) == (404, "REVIEW_NOT_FOUND")


# ---------------------------------------------------------------- respond
def test_host_responds_once(env):
    svc, _, _, kafka = env
    _, review = svc.create(GUEST, BODY, "k1", "tr")
    rid = review["review_id"]
    assert code(svc.respond, GUEST, {"review_id": rid, "response_text": "hi"}, "tr") == (403, "NOT_PROPERTY_HOST")
    assert svc.respond(HOST, {"review_id": rid, "response_text": "Sorry!"}, "tr")["response_text"] == "Sorry!"
    assert kafka.sent[-1][0] == "review.responded"
    assert code(svc.respond, HOST, {"review_id": rid, "response_text": "again"}, "tr") == (409, "RESPONSE_ALREADY_EXISTS")


# ---------------------------------------------------------------- HTTP
def test_http_api(env, monkeypatch):
    pytest.importorskip("fastapi")
    jwt = pytest.importorskip("jwt")
    from fastapi.testclient import TestClient
    from review_api.app import create_app
    secret = "test-secret-at-least-32-bytes-long!!"
    monkeypatch.setenv("JWT_SECRET", secret)
    client = TestClient(create_app(env[0], background_flush=False))

    def auth(sub, role):
        token = jwt.encode({"sub": sub, "role": role, "exp": int(time.time()) + 600}, secret, algorithm="HS256")
        return {"Authorization": f"Bearer {token}"}

    r = client.post("/reviews/create", json=BODY, headers={**auth("guest-1", "guest"), "Idempotency-Key": "k1",
                                                            "X-Trace-Id": "tr-demo"})
    assert r.status_code == 201 and r.headers["x-trace-id"] == "tr-demo"
    r2 = client.post("/reviews/create", json=BODY, headers={**auth("guest-1", "guest"), "Idempotency-Key": "k2"})
    assert r2.status_code == 409 and r2.json()["error"]["code"] == "REVIEW_ALREADY_EXISTS"
    assert client.post("/reviews/get", json={"review_id": "x"}).status_code == 401
    assert client.post("/reviews/get", json=[1], headers=auth("guest-1", "guest")).status_code == 400
    assert client.get("/health").json()["status"] == "ok"
