"""Guest detail-view outbox integration; replay suppresses events, not fresh reads."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sqlite3
import unittest
import uuid

from fastapi.testclient import TestClient
from test_read_services import CFG, token
from test_write_services import WriteSQLFixture
import test_profile_saved as profiles
from listings_core.http import Actor, unavailable
from listings_core.property_views import PropertyViews
from property_api.app import create_app
from shared.consumer import validate


class ViewTests(unittest.TestCase):
    def setUp(self):
        self.repo = WriteSQLFixture()
        self.client = TestClient(create_app(CFG, self.repo))

    def tearDown(self):
        self.client.close()
        self.repo.db.close()

    def get(self, pid="property-2001", headers=None, key=None):
        headers = dict(token() if headers is None else headers)
        headers["X-Trace-Id"] = "tr-view-test"
        if key is not None: headers["Idempotency-Key"] = key
        return self.client.post("/properties/get", json={"property_id": pid}, headers=headers)

    def events(self):
        return [json.loads(r[0]) for r in self.repo.db.execute("SELECT envelope FROM property_event_outbox")]

    def test_guest_view_uses_shared_envelope_and_jwt_identity(self):
        before = self.repo.property("property-2001")
        r = self.get()
        self.assertEqual(r.status_code, 200, r.text)
        e, = self.events()
        validate(e, key="property-2001", check_key=True)
        self.assertEqual(e["event_type"], "property.viewed")
        self.assertEqual(e["actor_id"], "guest-1001")
        self.assertEqual(e["trace_id"], "tr-view-test")
        self.assertEqual(e["payload"], {"property_id": "property-2001", "guest_id": "guest-1001", "viewed_datetime": e["timestamp"]})
        self.assertEqual(before, self.repo.property("property-2001"))
        self.assertEqual(r.headers["idempotent-replay"], "false")

    def test_anonymous_host_admin_service_and_agent_reads_emit_nothing(self):
        host = self.repo.property("property-2001")["host_id"]
        for headers in ({}, token(host, "host"), token("admin", "admin"), token("svc", "service"), token("agent", "agent")):
            self.assertEqual(self.get(headers=headers).status_code, 200)
        self.assertEqual(self.events(), [])

    def test_invalid_jwt_missing_property_and_private_property_emit_nothing(self):
        self.assertEqual(self.get(headers={"Authorization": "Bearer bad"}).status_code, 401)
        self.assertEqual(self.get(pid="property-missing").status_code, 404)
        self.assertEqual(self.get(pid="property-2200").status_code, 404)
        self.assertEqual(self.events(), [])

    def test_no_header_and_new_keys_are_separate_views(self):
        for key in (None, None, str(uuid.uuid4()), str(uuid.uuid4())):
            self.assertEqual(self.get(key=key).status_code, 200)
        events = self.events()
        self.assertEqual(len(events), 4)
        self.assertEqual(len({e["idempotency_key"] for e in events}), 4)

    def test_replay_suppresses_only_event_and_returns_current_listing(self):
        key = str(uuid.uuid4())
        self.assertEqual(self.get(key=key).status_code, 200)
        self.repo.db.execute("UPDATE properties SET title='Changed listing title' WHERE property_id='property-2001'")
        self.repo.db.commit()
        r = self.get(key=key)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["title"], "Changed listing title")
        self.assertEqual(r.headers["idempotent-replay"], "true")
        self.assertEqual(len(self.events()), 1)
        self.repo.db.execute("UPDATE properties SET status='archived' WHERE property_id='property-2001'")
        self.repo.db.commit()
        self.assertEqual(self.get(key=key).status_code, 404)

    def test_same_key_different_property_conflicts_but_other_guest_is_independent(self):
        key = str(uuid.uuid4())
        self.assertEqual(self.get(key=key).status_code, 200)
        self.assertEqual(self.get(pid="property-2007", key=key).status_code, 409)
        self.assertEqual(self.get(key=key, headers=token("guest-1002", "guest")).status_code, 200)
        self.assertEqual(len(self.events()), 2)

    def test_invalid_key_and_spoofed_guest_are_rejected(self):
        self.assertEqual(self.get(key="bad-key").status_code, 400)
        r = self.client.post("/properties/get", json={"property_id": "property-2001", "guest_id": "guest-1002"}, headers=token())
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.events(), [])

    def test_unknown_and_deleted_guest_do_not_record_new_views(self):
        self.assertEqual(self.get(headers=token("guest-missing", "guest")).status_code, 404)
        self.repo.db.execute("UPDATE guests SET deleted_datetime='2026-11-27 00:00:00' WHERE guest_id='guest-1001'")
        self.repo.db.commit()
        self.assertEqual(self.get().status_code, 404)
        self.assertEqual(self.events(), [])

    def test_outbox_insert_failure_rolls_back_operation_and_can_retry(self):
        key = str(uuid.uuid4())
        self.repo.db.execute("CREATE TRIGGER fail_view BEFORE INSERT ON property_event_outbox BEGIN SELECT RAISE(ABORT,'injected fault'); END")
        with self.assertLogs("listings", level="ERROR"):
            with self.assertRaises(sqlite3.IntegrityError): self.get(key=key)
        self.assertEqual(self.repo.db.execute("SELECT COUNT(*) FROM property_http_operations").fetchone()[0], 0)
        self.assertEqual(self.events(), [])
        self.repo.db.execute("DROP TRIGGER fail_view")
        self.assertEqual(self.get(key=key).status_code, 200)
        self.assertEqual(len(self.events()), 1)

    def test_my_sql_write_outage_returns_common_503_without_success(self):
        from contextlib import contextmanager
        original = self.repo.connection
        @contextmanager
        def failing(*, write=False):
            if write: raise unavailable("MySQL")
            with original() as conn: yield conn
        self.repo.connection = failing
        r = self.get()
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["error"]["code"], "DEPENDENCY_UNAVAILABLE")
        self.assertEqual(self.events(), [])

    def test_search_does_not_emit_views(self):
        r = self.client.post("/properties/search", json={"location": "San Jose"}, headers=token())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.events(), [])


@unittest.skipUnless(os.getenv("LISTINGS_VIEW_MYSQL_TEST") == "1", "Set LISTINGS_VIEW_MYSQL_TEST=1 for isolated live view test")
class LiveViewTests(unittest.TestCase):
    def setUp(self):
        profiles.LiveProfileSavedTests.setUp(self)

    def tearDown(self):
        profiles.LiveProfileSavedTests.tearDown(self)

    def test_concurrent_same_key_creates_one_durable_event(self):
        service, key = PropertyViews(self.repo), str(uuid.uuid4())
        actor = Actor(self.gid, "guest")
        def run(i): return service.record(self.pid, actor, "tr-concurrent-view", key)
        with ThreadPoolExecutor(max_workers=4) as pool: results = list(pool.map(run, range(8)))
        self.assertEqual(sum(not r[1] for r in results), 1)
        self.assertEqual(len({r[0]["event_id"] for r in results}), 1)
        with self.repo.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM property_event_outbox WHERE property_id=%s", (self.pid,))
            self.assertEqual(cursor.fetchone()["n"], 1)
