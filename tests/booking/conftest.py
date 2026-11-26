"""Role B test fixtures. Integration tests use the isolated database `airbnb_booking_test`
(created by `make booking-test-db`) and an in-memory producer; they are skipped if MySQL is unreachable."""
from __future__ import annotations

import dataclasses
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT / "services/booking-api", ROOT / "services/booking-worker", ROOT / "services/booking-api/scripts"):
    sys.path.insert(0, str(p))

import jwt  # noqa: E402

from booking_core.config import Settings  # noqa: E402
from booking_core.db import Database  # noqa: E402
from booking_core.kafka_producer import InMemoryProducer  # noqa: E402

TEST_DB = "airbnb_booking_test"
TEST_SECRET = "test-only-secret-not-used-anywhere-else-0123456789"
TABLES = ("availability_blocks", "reservation_status_history", "processed_events", "idempotency_keys", "reservations")


@pytest.fixture(scope="session")
def settings() -> Settings:
    base = Settings.from_env()
    return dataclasses.replace(base, mysql_database=TEST_DB, jwt_secret=TEST_SECRET, jwt_issuer="",
                               property_source="seed", user_service_url="", analytics_service_url="",
                               precheck_enabled=True, rate_limit_per_minute=0, ws_enabled=False,
                               service_token="svc-test-token", worker_fault="", worker_transient_faults=0,
                               test_spread_partitions=False, dev_token_endpoint=False)


@pytest.fixture(scope="session")
def db(settings):
    database = Database(settings, pool_size=8)
    try:
        database.ping()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"MySQL test database unavailable ({exc}); run `make booking-infra-up booking-test-db`")
    ddl = (ROOT / "db/mysql/02_bookings.sql").read_text()
    statements = [s.strip() for s in "\n".join(l for l in ddl.splitlines() if not l.strip().startswith("--")).split(";")]
    with database.transaction() as conn, conn.cursor() as cur:
        for stmt in statements:
            if stmt:
                cur.execute(stmt)
    return database


@pytest.fixture()
def clean_db(db):
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in TABLES:
            cur.execute(f"DELETE FROM {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    return db


@pytest.fixture()
def producer(settings):
    return InMemoryProducer(settings)


@pytest.fixture()
def client(settings, clean_db, producer):
    from fastapi.testclient import TestClient

    from booking_api.app import create_app
    app = create_app(settings, db=clean_db, producer=producer, start_background=False)
    with TestClient(app) as c:
        yield c


def make_token(settings, sub: str, role: str | None = None, ttl: int = 600) -> str:
    now = int(time.time())
    return jwt.encode({"sub": sub, "role": role or ("host" if sub.startswith("host-") else "guest"),
                       "iat": now, "exp": now + ttl}, settings.jwt_secret, algorithm="HS256")


@pytest.fixture()
def auth(settings):
    def _h(sub: str, *, key: str | None = "auto", role: str | None = None) -> dict[str, str]:
        h = {"Authorization": f"Bearer {make_token(settings, sub, role)}"}
        if key == "auto":
            h["Idempotency-Key"] = str(uuid.uuid4())
        elif key:
            h["Idempotency-Key"] = key
        return h
    return _h


def future(days: int) -> date:
    return date.today() + timedelta(days=days)


@pytest.fixture()
def worker(settings, clean_db, producer):
    from booking_worker.worker import BookingWorker
    sleeps: list[float] = []
    w = BookingWorker(settings, clean_db, producer, sleep=sleeps.append)
    w.sleeps = sleeps
    return w


def run_worker_on_requested(worker, producer):
    """Feed every booking.requested the API produced to the worker (like Kafka would)."""
    import json

    from booking_worker.consumer_utils import RecordMeta
    outcomes = []
    for i, (topic, key, event) in enumerate(list(producer.sent)):
        if topic == "booking.requested" and not event.get("_fed"):
            event["_fed"] = True
            clean = {k: v for k, v in event.items() if k != "_fed"}
            outcomes.append(worker.handle(RecordMeta(topic, 0, i, key), json.dumps(clean).encode()))
    return outcomes
