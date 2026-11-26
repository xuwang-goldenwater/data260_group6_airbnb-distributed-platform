"""Role B test fixtures. Integration tests use the isolated database `airbnb_booking_test`
(created by `make booking-test-db`); they are skipped if MySQL is unreachable."""
from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT / "services/booking-api", ROOT / "services/booking-worker", ROOT / "services/booking-api/scripts"):
    sys.path.insert(0, str(p))

from booking_core.config import Settings  # noqa: E402
from booking_core.db import Database  # noqa: E402

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
    # Rebuild the booking tables in the isolated test database so schema changes are always picked up.
    with database.transaction() as conn, conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
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
