from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
import json
import os
import sqlite3
import threading
import unittest
import uuid

from fastapi.testclient import TestClient
import httpx

from test_read_services import SQLFixtureRepository, Cursor, CFG, ROOT, SECRET, token
from listings_core.config import Settings
from listings_core.http import ApiError, Actor, unavailable
from listings_core.property_outbox import relay_one
from listings_core.property_writes import PropertyWrites
from listings_core.repository import MySQLRepository
from listings_core.write_clients import WriteDependencies
from listings_core.write_models import CreateProperty
from property_api.app import create_app


def create_body():
    return {"title": "Synthetic integration house", "description": "A synthetic listing for transaction and API tests only.",
            "property_type": "house", "location": {"address_line_1": "100 Example Lane", "city": "San Jose", "state": "CA",
            "postal_code": "95113", "country": "US", "latitude": 37.3382, "longitude": -121.8863},
            "capacity": 4, "bedrooms": 2, "bathrooms": 1.5, "amenities": ["kitchen", "parking", "wifi"],
            "nightly_price": 200.0, "cleaning_fee": 40.0}


class Dependencies:
    def __init__(self):
        self.active = False
        self.down = False
        self.host_calls = 0
        self.archive_calls = 0

    def require_host(self, host_id, trace):
        self.host_calls += 1
        if self.down:
            raise unavailable("User")

    def require_no_active_bookings(self, pid, trace):
        self.archive_calls += 1
        if self.down:
            raise unavailable("Booking")
        if self.active:
            raise ApiError(409, "ACTIVE_RESERVATION_EXISTS", "Active bookings")


class WriteCursor(Cursor):
    def execute(self, sql, args=()):
        self.fake = None
        if "GET_LOCK(" in sql:
            self.fake = {"acquired": 1}
            return
        if "RELEASE_LOCK(" in sql:
            self.fake = {"released": 1}
            return
        sql = sql.replace("INSERT IGNORE", "INSERT OR IGNORE").replace(" FOR UPDATE", "")
        sql = sql.replace("ON DUPLICATE KEY UPDATE request_hash=request_hash",
                          "ON CONFLICT(actor_id,endpoint,idempotency_key) DO NOTHING")
        args = [v.isoformat(sep=" ") if isinstance(v, datetime) else v for v in args]
        super().execute(sql, args)

    def fetchone(self):
        return self.fake if self.fake is not None else super().fetchone()

    @property
    def rowcount(self):
        return self.cursor.rowcount


class WriteConnection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return WriteCursor(self.db)

    def commit(self):
        self.db.commit()


class WriteSQLFixture(SQLFixtureRepository):
    def __init__(self):
        super().__init__()
        self.settings = CFG
        self.lock = threading.RLock()
        self.db.execute("CREATE UNIQUE INDEX amenities_unique ON amenities(amenity_name)")
        self.db.execute("CREATE UNIQUE INDEX property_id_unique ON properties(property_id)")
        self.db.execute("CREATE UNIQUE INDEX property_amenities_unique ON property_amenities(property_id,amenity_name)")
        self.db.execute("CREATE TABLE property_http_operations (actor_id TEXT, endpoint TEXT, idempotency_key TEXT, "
                        "request_hash TEXT, response_body TEXT, created_datetime TEXT, PRIMARY KEY(actor_id,endpoint,idempotency_key))")
        self.db.execute("CREATE TABLE property_event_outbox (outbox_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE, "
                        "topic TEXT, property_id TEXT, envelope TEXT, created_datetime TEXT, published_datetime TEXT, publish_attempts INTEGER DEFAULT 0)")
        self.db.commit()

    @contextmanager
    def connection(self, *, write=False):
        with self.lock:
            if write:
                self.db.execute("BEGIN")
            try:
                yield WriteConnection(self.db)
                if write:
                    self.db.commit()
            except BaseException:
                if write:
                    self.db.rollback()
                raise


class WriteAPITests(unittest.TestCase):
    def setUp(self):
        self.repo = WriteSQLFixture()
        self.deps = Dependencies()
        self.client = TestClient(create_app(CFG, self.repo, write_dependencies=self.deps))
        self.headers = token("host-501", "host")

    def tearDown(self):
        self.client.close()
        self.repo.db.close()

    def post(self, endpoint, body, key=None, headers=None):
        return self.client.post("/properties/" + endpoint, json=body,
                                headers={**(headers or self.headers), "Idempotency-Key": key or str(uuid.uuid4())})

    def create(self):
        r = self.post("create", create_body())
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def count(self, table):
        return self.repo.db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]

    def test_create_is_owned_draft_and_amenities_saved(self):
        p = self.create()
        self.assertEqual((p["host_id"], p["status"], p["version"], p["rating"]), ("host-501", "draft", 1, 0))
        self.assertEqual(p["amenities"], ["kitchen", "parking", "wifi"])
        self.assertEqual(self.count("property_event_outbox"), 1)
        self.assertEqual(self.deps.host_calls, 1)

    def test_idempotent_create_returns_original_and_no_extra_event(self):
        key = str(uuid.uuid4())
        first = self.post("create", create_body(), key)
        self.deps.down = True
        again = self.post("create", create_body(), key)
        self.assertEqual(again.status_code, 200)
        self.assertEqual(first.json(), again.json())
        self.assertEqual(self.count("property_event_outbox"), 1)
        changed = self.post("create", dict(create_body(), nightly_price=201), key)
        self.assertEqual(changed.status_code, 409)
        self.assertEqual(changed.json()["error"]["code"], "IDEMPOTENCY_KEY_REUSED")

    def test_lock_failures_retry_whole_transaction_after_rollback(self):
        original = self.repo.connection
        for code in (1213, 1205):
            attempts = []
            @contextmanager
            def flaky(*, write=False):
                with original(write=write) as conn:
                    yield conn
                    if write:
                        attempts.append(1)
                        if len(attempts) == 1:
                            error = unavailable("MySQL")
                            error.mysql_code = code
                            raise error
            self.repo.connection = flaky
            before = self.count("properties")
            events_before = self.count("property_event_outbox")
            writer = PropertyWrites(self.repo, self.deps, sleep=lambda _: None)
            result, replay = writer.execute("create", CreateProperty(**create_body()), Actor("host-501", "host"),
                                             str(uuid.uuid4()), "tr-retry")
            self.assertFalse(replay)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(self.count("properties"), before + 1)
            self.assertEqual(self.count("property_event_outbox"), events_before + 1)
            self.assertIsNotNone(self.repo.property(result["property_id"]))
            self.repo.connection = original

    def test_retry_is_bounded_and_does_not_retry_other_errors(self):
        original = self.repo.connection
        for code, expected in ((1213, 4), (1045, 1), (2013, 1)):
            calls = []
            @contextmanager
            def failure(*, write=False):
                calls.append(1)
                error = unavailable("MySQL")
                error.mysql_code = code
                raise error
                yield  # makes this a context manager without acquiring a connection
            self.repo.connection = failure
            writer = PropertyWrites(self.repo, self.deps, sleep=lambda _: None)
            with self.assertRaises(ApiError):
                writer.execute("create", CreateProperty(**create_body()), Actor("host-501", "host"),
                               str(uuid.uuid4()), "tr-retry-limit")
            self.assertEqual(len(calls), expected)
            self.repo.connection = original

    def test_update_replaces_amenities_and_checks_version(self):
        p = self.create()
        body = {"property_id": p["property_id"], "expected_version": 1,
                "changes": {"nightly_price": 210, "amenities": ["parking", "pet_friendly"], "status": "active"}}
        key = str(uuid.uuid4())
        r = self.post("update", body, key)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["version"], 2)
        self.assertEqual(r.json()["amenities"], ["parking", "pet_friendly"])
        self.assertEqual(self.post("update", body, key).json(), r.json())
        self.assertEqual(self.post("update", body).status_code, 409)
        self.assertEqual(self.count("property_event_outbox"), 2)

    def test_ownership_and_role_cannot_be_spoofed(self):
        p = self.create()
        self.assertEqual(self.post("create", create_body(), headers=token()).status_code, 403)
        body = {"property_id": p["property_id"], "expected_version": 1, "changes": {"nightly_price": 230}}
        self.assertEqual(self.post("update", body, headers=token("host-502", "host")).status_code, 403)
        self.assertEqual(self.post("create", dict(create_body(), host_id="host-502")).status_code, 400)

    def test_archive_is_soft_and_replayable(self):
        p = self.create()
        key = str(uuid.uuid4())
        body = {"property_id": p["property_id"], "expected_version": 1}
        r = self.post("delete", body, key)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["version"], 2)
        self.assertTrue(r.json()["deleted_datetime"].endswith("Z"))
        self.assertEqual(self.post("delete", body, key).json(), r.json())
        self.assertEqual(self.repo.property(p["property_id"])["status"], "archived")
        self.assertEqual(self.client.post("/properties/get", json={"property_id": p["property_id"]}).status_code, 404)

    def test_update_status_cannot_bypass_booking_guard(self):
        p = self.create()
        self.deps.active = True
        body = {"property_id": p["property_id"], "expected_version": 1}
        for endpoint, request in (("delete", body), ("update", dict(body, changes={"status": "archived"}))):
            r = self.post(endpoint, request)
            self.assertEqual(r.status_code, 409, r.text)
            self.assertEqual(r.json()["error"]["code"], "ACTIVE_RESERVATION_EXISTS")
        self.assertEqual(self.repo.property(p["property_id"])["version"], 1)
        self.assertEqual(self.count("property_event_outbox"), 1)

    def test_dependency_outage_rolls_back_claim_and_can_retry_same_key(self):
        key = str(uuid.uuid4())
        self.deps.down = True
        r = self.post("create", create_body(), key)
        self.assertEqual(r.status_code, 503)
        self.assertEqual(self.count("property_http_operations"), 0)
        self.assertEqual(self.count("property_event_outbox"), 0)
        self.deps.down = False
        self.assertEqual(self.post("create", create_body(), key).status_code, 201)

    def test_booking_outage_does_not_archive(self):
        p = self.create()
        self.deps.down = True
        r = self.post("delete", {"property_id": p["property_id"], "expected_version": 1})
        self.assertEqual(r.status_code, 503)
        self.assertEqual(self.repo.property(p["property_id"])["status"], "draft")

    def test_validation_and_required_idempotency_key(self):
        self.assertEqual(self.client.post("/properties/create", json=create_body(), headers=self.headers).status_code, 400)
        for change in ({"nightly_price": -1}, {"nightly_price": 1.001}, {"capacity": True}, {"rating": 5},
                       {"amenities": ["wifi", "wifi"]}, {"location": None}):
            p = self.create()
            r = self.post("update", {"property_id": p["property_id"], "expected_version": 1, "changes": change})
            self.assertEqual(r.status_code, 400, r.text)

    def test_outbox_rollback_when_event_insert_fails(self):
        self.repo.db.execute("CREATE TRIGGER reject_outbox BEFORE INSERT ON property_event_outbox BEGIN SELECT RAISE(ABORT, 'test fault'); END")
        before = self.count("properties")
        with self.assertLogs("listings", level="ERROR"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.create()
        self.assertEqual(self.count("properties"), before)
        self.assertEqual(self.count("property_http_operations"), 0)

    def test_event_envelope_key_and_publish_retry(self):
        p = self.create()
        class Publisher:
            def __init__(self):
                self.events = []
            def publish(self, topic, key, event):
                self.events.append((topic, key, event))
                if len(self.events) < 3:
                    raise RuntimeError("temporary outage")
        publisher = Publisher()
        result = relay_one(self.repo, publisher, sleep=lambda _: None)
        self.assertEqual(result["status"], "published")
        self.assertEqual(len(publisher.events), 3)
        self.assertEqual(len({e[2]["event_id"] for e in publisher.events}), 1)
        self.assertTrue(all(key == p["property_id"] for _, key, _ in publisher.events))
        self.assertEqual(relay_one(self.repo, publisher)["status"], "empty")

    def test_persistent_publish_failure_retains_pending_event(self):
        self.create()
        class Down:
            def publish(self, *args):
                raise RuntimeError("down")
        result = relay_one(self.repo, Down(), sleep=lambda _: None)
        self.assertEqual(result["status"], "pending_retry")
        row = self.repo.db.execute("SELECT published_datetime,publish_attempts FROM property_event_outbox").fetchone()
        self.assertIsNone(row[0])
        self.assertEqual(row[1], 4)


class DependencyHTTPTests(unittest.TestCase):
    def test_host_mismatch_and_booking_malformed_fail_closed(self):
        client = WriteDependencies(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"host_id": "other"})))
        with self.assertRaises(ApiError):
            client.require_host("host-501", "tr-test")
        for data in ({}, {"property_id": "p", "has_active_reservations": False, "count": 1},
                     {"property_id": "p", "has_active_reservations": False, "count": "0"}):
            client = WriteDependencies(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(200, json=data)))
            with self.assertRaises(ApiError):
                client.require_no_active_bookings("p", "tr-test")


@unittest.skipUnless(os.getenv("LISTINGS_WRITE_MYSQL_TEST") == "1", "Set LISTINGS_WRITE_MYSQL_TEST=1 for isolated MySQL write tests")
class LiveWriteTests(unittest.TestCase):
    def setUp(self):
        cfg = replace(Settings.from_env(), mysql_database="airbnb_property_test")
        self.repo = MySQLRepository(cfg)
        self.actor = Actor("host-test-" + uuid.uuid4().hex, "host")
        self.writes = PropertyWrites(self.repo, Dependencies())
        self.created = set()

    def tearDown(self):
        with self.repo.connection(write=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT property_id FROM properties WHERE host_id=%s", (self.actor.sub,))
            ids = [r["property_id"] for r in cur.fetchall()]
            for pid in ids:
                cur.execute("DELETE FROM property_event_outbox WHERE property_id=%s", (pid,))
                cur.execute("DELETE FROM property_amenities WHERE property_id=%s", (pid,))
                cur.execute("DELETE FROM properties WHERE property_id=%s", (pid,))
            cur.execute("DELETE FROM property_http_operations WHERE actor_id=%s", (self.actor.sub,))

    def test_concurrent_same_key_creates_exactly_one_listing_and_event(self):
        key = str(uuid.uuid4())
        body = CreateProperty(**create_body())
        def run(_):
            return self.writes.execute("create", body, self.actor, key, "tr-live-write")
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(run, range(8)))
        self.assertEqual(len({p["property_id"] for p, _ in results}), 1)
        self.assertEqual(sum(not replay for _, replay in results), 1)
        with self.repo.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM property_event_outbox WHERE property_id=%s", (results[0][0]["property_id"],))
            self.assertEqual(cur.fetchone()["n"], 1)

    def test_mysql_create_update_archive_and_outbox(self):
        from listings_core.write_models import UpdateProperty, ArchiveProperty
        p, _ = self.writes.execute("create", CreateProperty(**create_body()), self.actor, str(uuid.uuid4()), "tr-live-write")
        p, _ = self.writes.execute("update", UpdateProperty(property_id=p["property_id"], expected_version=1,
                                   changes={"status": "active", "amenities": ["wifi"]}), self.actor, str(uuid.uuid4()), "tr-live-write")
        self.assertEqual(p["version"], 2)
        self.assertEqual(p["amenities"], ["wifi"])
        result, _ = self.writes.execute("delete", ArchiveProperty(property_id=p["property_id"], expected_version=2),
                                        self.actor, str(uuid.uuid4()), "tr-live-write")
        self.assertEqual(result["status"], "archived")
        self.assertEqual(result["version"], 3)


if __name__ == "__main__":
    unittest.main()
