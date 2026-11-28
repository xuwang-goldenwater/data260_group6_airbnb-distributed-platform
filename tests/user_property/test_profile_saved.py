from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
import sqlite3
import unittest
import uuid

from fastapi.testclient import TestClient
import httpx

from test_read_services import CFG, ROOT, token, load
from test_write_services import WriteSQLFixture, Dependencies
from listings_core.config import Settings
from listings_core.http import Actor, ApiError, unavailable
from listings_core.profile_models import GuestUpdate, SaveProperty
from listings_core.profile_service import ProfileService
from listings_core.repository import MySQLRepository
from listings_core.saved_service import SavedService
from listings_core.write_clients import WriteDependencies
from property_api.app import create_app as property_app
from user_api.app import create_app as user_app


class ProfileDependencies(Dependencies):
    def require_guest(self, guest_id, trace_id):
        if self.down:
            raise unavailable("User")

    def require_no_guest_bookings(self, guest_id, trace_id):
        if self.down:
            raise unavailable("Booking")
        if self.active:
            raise ApiError(409, "ACTIVE_RESERVATION_EXISTS", "Active bookings")


class ProfileSQLFixture(WriteSQLFixture):
    def __init__(self):
        super().__init__()
        self.db.execute("CREATE TABLE user_http_operations (actor_id TEXT, endpoint TEXT, idempotency_key TEXT, request_hash TEXT, response_body TEXT, created_datetime TEXT, PRIMARY KEY(actor_id,endpoint,idempotency_key))")
        self.db.execute("CREATE TABLE saved_properties (guest_id TEXT, property_id TEXT, saved_datetime TEXT, PRIMARY KEY(guest_id,property_id))")
        self.db.executemany("INSERT INTO saved_properties VALUES (?,?,?)", [(s["guest_id"],s["property_id"],s["saved_datetime"]) for s in load("saved_properties")])
        self.db.execute("CREATE UNIQUE INDEX guest_email_unique ON guests(LOWER(email))")
        self.db.execute("CREATE UNIQUE INDEX host_email_unique ON hosts(LOWER(email))")
        self.db.create_function("CONCAT", -1, lambda *args: "".join(str(v) for v in args))
        self.db.commit()

    @contextmanager
    def connection(self, *, write=False):
        try:
            with super().connection(write=write) as conn:
                yield conn
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint failed" not in str(exc):
                raise
            error = unavailable("MySQL")
            error.mysql_code = 1062
            raise error from None


class ProfileAndSavedTests(unittest.TestCase):
    def setUp(self):
        self.repo = ProfileSQLFixture()
        self.deps = ProfileDependencies()
        self.user = TestClient(user_app(CFG, self.repo, self.deps))
        self.prop = TestClient(property_app(CFG, self.repo, write_dependencies=self.deps))
        existing = {r[0] for r in self.repo.db.execute("SELECT property_id FROM saved_properties WHERE guest_id='guest-1001'")}
        self.pid = next(p["property_id"] for p in load("properties") if p["status"] == "active" and p["property_id"] not in existing)

    def tearDown(self):
        self.user.close()
        self.prop.close()
        self.repo.db.close()

    def post(self, client, url, body, key=None, headers=None):
        return client.post(url, json=body, headers={**(headers or token()), "Idempotency-Key": key or str(uuid.uuid4())})

    def count(self, table):
        return self.repo.db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]

    def test_guest_update_merges_preferences_and_clears_optional_fields(self):
        body = {"guest_id": "guest-1001", "first_name": "Updated", "phone": "+14085550199", "preferences": {"max_nightly_price": 180}}
        key = str(uuid.uuid4())
        r = self.post(self.user, "/guests/update", body, key)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["first_name"], "Updated")
        self.assertEqual(r.json()["preferences"]["required_amenities"], ["parking", "kitchen"])
        self.assertEqual(r.json()["preferences"]["max_nightly_price"], 180)
        self.assertEqual(self.post(self.user, "/guests/update", body, key).json(), r.json())
        clear = self.post(self.user, "/guests/update", {"guest_id": "guest-1001", "phone": None})
        self.assertIsNone(clear.json()["phone"])

    def test_profile_updates_reject_spoofed_role_and_server_fields(self):
        for headers in (token("guest-1002", "guest"), token("host-501", "host"), token("service", "service")):
            self.assertEqual(self.post(self.user, "/guests/update", {"guest_id": "guest-1001", "first_name": "Name"}, headers=headers).status_code, 403)
        for field, value in (("booking_count", 99), ("saved_properties_count", 99), ("profile_created_datetime", "x")):
            self.assertEqual(self.post(self.user, "/guests/update", {"guest_id": "guest-1001", field: value}).status_code, 400)

    def test_invalid_profile_values_and_empty_edits(self):
        cases = [{}, {"email": "bad"}, {"email": None}, {"first_name": None}, {"phone": "4085550199"},
                 {"profile_photo_url": "http://example.com/a.jpg"}, {"profile_photo_url": "https://user:pass@example.com/a"},
                 {"profile_photo_url": "https://example.com:invalid/a"},
                 {"preferences": {"max_nightly_price": -1}}, {"preferences": {"property_types": ["boat"]}}]
        for changes in cases:
            with self.subTest(changes=changes):
                self.assertEqual(self.post(self.user, "/guests/update", {"guest_id": "guest-1001", **changes}).status_code, 400)

    def test_email_conflict_is_409_and_rolls_back_other_fields(self):
        before = self.repo.guest("guest-1001")["first_name"]
        r = self.post(self.user, "/guests/update", {"guest_id": "guest-1001", "email": "guest2@example.com", "first_name": "ShouldRollBack"})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(r.json()["error"]["code"], "EMAIL_ALREADY_EXISTS")
        self.assertEqual(self.repo.guest("guest-1001")["first_name"], before)
        self.assertEqual(self.count("user_http_operations"), 0)

    def test_host_update_cannot_elevate_access(self):
        headers = token("host-501", "host")
        r = self.post(self.user, "/hosts/update", {"host_id": "host-501", "name": "Updated Host"}, headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["name"], "Updated Host")
        self.assertEqual(self.post(self.user, "/hosts/update", {"host_id": "host-501", "access_level": "admin"}, headers=headers).status_code, 400)

    def test_delete_guard_dependency_failure_and_success(self):
        body = {"guest_id": "guest-1001"}
        self.deps.active = True
        self.assertEqual(self.post(self.user, "/guests/delete", body).status_code, 409)
        self.deps.active, self.deps.down = False, True
        self.assertEqual(self.post(self.user, "/guests/delete", body).status_code, 503)
        self.assertIsNotNone(self.repo.guest("guest-1001"))
        self.deps.down = False
        key = str(uuid.uuid4())
        r = self.post(self.user, "/guests/delete", body, key)
        self.assertEqual(r.status_code, 200)
        self.assertIs(r.json()["deleted"], True)
        self.assertIsNone(self.repo.guest("guest-1001"))
        self.assertEqual(self.post(self.user, "/guests/delete", body, key).json(), r.json())
        self.assertEqual(self.user.get("/internal/guests/guest-1001", headers=token("ai", "service")).status_code, 404)
        self.assertEqual(self.count("saved_properties"), 360)

    def test_guest_search_restricts_access_and_private_fields(self):
        self.assertEqual(self.user.post("/guests/search", json={}, headers=token()).status_code, 403)
        r = self.user.post("/guests/search", json={"name": "Demo1", "page_size": 2}, headers=token("admin", "admin"))
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(len(r.json()["items"]), 2)
        self.assertGreater(r.json()["total"], 2)
        self.assertNotIn("email", r.json()["items"][0])
        self.assertNotIn("phone", r.json()["items"][0])
        result = self.user.post("/guests/search", json={"name": "%"}, headers=token("svc", "service"))
        self.assertEqual(result.json()["total"], 0)

    def test_save_unsave_updates_row_count_and_events(self):
        body = {"property_id": self.pid, "saved": True}
        r = self.post(self.prop, "/properties/save", body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["guest_id"], "guest-1001")
        self.assertEqual(r.json()["saved_properties_count"], 4)
        self.assertEqual(self.repo.guest("guest-1001")["saved_properties_count"], 4)
        self.assertEqual(self.count("property_event_outbox"), 1)
        again = self.post(self.prop, "/properties/save", body)
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.json()["saved_properties_count"], 4)
        self.assertEqual(self.count("property_event_outbox"), 1)
        removed = self.post(self.prop, "/properties/save", {**body, "saved": False})
        self.assertEqual(removed.json()["saved_properties_count"], 3)
        self.assertEqual(self.repo.guest("guest-1001")["saved_properties_count"], 3)
        self.assertEqual(self.count("property_event_outbox"), 2)

    def test_save_replay_and_body_mismatch(self):
        body, key = {"property_id": self.pid, "saved": True}, str(uuid.uuid4())
        r = self.post(self.prop, "/properties/save", body, key)
        self.deps.down = True
        self.assertEqual(self.post(self.prop, "/properties/save", body, key).json(), r.json())
        self.assertEqual(self.post(self.prop, "/properties/save", {**body, "saved": False}, key).status_code, 409)
        self.assertEqual(self.count("property_event_outbox"), 1)

    def test_save_rejects_wrong_identity_missing_guest_and_nonactive_property(self):
        body = {"property_id": self.pid, "saved": True}
        self.assertEqual(self.post(self.prop, "/properties/save", body, headers=token("host-501", "host")).status_code, 403)
        self.assertEqual(self.post(self.prop, "/properties/save", dict(body, guest_id="guest-1002")).status_code, 400)
        self.assertEqual(self.post(self.prop, "/properties/save", body, headers=token("guest-missing", "guest")).status_code, 404)
        self.assertEqual(self.post(self.prop, "/properties/save", {"property_id": "property-2200", "saved": True}).status_code, 404)
        self.assertEqual(self.post(self.prop, "/properties/save", {"property_id": self.pid, "saved": "true"}).status_code, 400)

    def test_save_rolls_back_relationship_and_counter_on_outbox_failure(self):
        self.repo.db.execute("CREATE TRIGGER reject_saved_outbox BEFORE INSERT ON property_event_outbox BEGIN SELECT RAISE(ABORT, 'saved fault'); END")
        with self.assertLogs("listings", level="ERROR"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.post(self.prop, "/properties/save", {"property_id": self.pid, "saved": True})
        self.assertEqual(self.count("saved_properties"), 360)
        self.assertEqual(self.repo.guest("guest-1001")["saved_properties_count"], 3)
        self.assertEqual(self.count("property_http_operations"), 0)

    def test_saved_event_matches_contract(self):
        self.post(self.prop, "/properties/save", {"property_id": self.pid, "saved": True})
        event = json.loads(self.repo.db.execute("SELECT envelope FROM property_event_outbox").fetchone()[0])
        self.assertEqual(event["event_type"], "property.saved")
        self.assertEqual(event["payload"]["property_id"], self.pid)
        self.assertEqual(event["payload"]["guest_id"], "guest-1001")
        self.assertIs(event["payload"]["saved"], True)
        self.assertTrue(event["payload"]["saved_datetime"].endswith("Z"))


class GuestDependencyTests(unittest.TestCase):
    def test_guest_check_and_delete_guard_fail_closed(self):
        for payload in ({"guest_id": "guest-1001", "exists": True, "active": False}, {"exists": True}, []):
            dep = WriteDependencies(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(200, json=payload)))
            with self.assertRaises(ApiError):
                dep.require_guest("guest-1001", "tr-test")
        dep = WriteDependencies(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(200,
                                json={"guest_id": "guest-1001", "count": 1, "has_active_reservations": True})))
        with self.assertRaises(ApiError) as error:
            dep.require_no_guest_bookings("guest-1001", "tr-test")
        self.assertEqual(error.exception.code, "ACTIVE_RESERVATION_EXISTS")


@unittest.skipUnless(os.getenv("LISTINGS_PROFILE_MYSQL_TEST") == "1", "Set LISTINGS_PROFILE_MYSQL_TEST=1 for isolated profile/save MySQL tests")
class LiveProfileSavedTests(unittest.TestCase):
    def setUp(self):
        self.repo = MySQLRepository(replace(Settings.from_env(), mysql_database="airbnb_property_test"))
        self.gid = "guest-test-" + uuid.uuid4().hex
        self.pid = "property-test-" + uuid.uuid4().hex
        self.actor = Actor(self.gid, "guest")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.repo.connection(write=True) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO guests (guest_id,first_name,last_name,email,preferences,saved_properties_count,booking_count,profile_created_datetime,updated_datetime) VALUES (%s,%s,%s,%s,%s,0,0,%s,%s)",
                        (self.gid,"Test","Guest",self.gid+"@example.com",'{}',now,now))
            p = load("properties")[0]
            p.update(property_id=self.pid)
            p.update(p.pop("location"))
            p.pop("amenities")
            p["created_datetime"] = p["updated_datetime"] = now
            cur.execute("INSERT INTO properties (" + ",".join("`"+k+"`" for k in p) + ") VALUES (" + ",".join(["%s"]*len(p)) + ")", list(p.values()))

    def tearDown(self):
        with self.repo.connection(write=True) as conn, conn.cursor() as cur:
            for table in ("property_http_operations","user_http_operations"):
                cur.execute("DELETE FROM " + table + " WHERE actor_id=%s", (self.gid,))
            cur.execute("DELETE FROM property_event_outbox WHERE property_id=%s", (self.pid,))
            cur.execute("DELETE FROM saved_properties WHERE guest_id=%s", (self.gid,))
            cur.execute("DELETE FROM properties WHERE property_id=%s", (self.pid,))
            cur.execute("DELETE FROM guests WHERE guest_id=%s", (self.gid,))

    def test_concurrent_saved_state_has_one_relationship_counter_and_event(self):
        service = SavedService(self.repo, ProfileDependencies())
        body = SaveProperty(property_id=self.pid, saved=True)
        def run(i):
            return service.save(body, self.actor, str(uuid.uuid4()), "tr-concurrent-save")
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(run, range(8)))
        self.assertTrue(all(result[0]["saved_properties_count"] == 1 for result in results))
        self.assertEqual(self.repo.guest(self.gid)["saved_properties_count"], 1)
        with self.repo.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM property_event_outbox WHERE property_id=%s", (self.pid,))
            self.assertEqual(cur.fetchone()["n"], 1)

    def test_profile_update_and_save_removal_are_durable(self):
        profiles = ProfileService(self.repo, ProfileDependencies())
        result, _ = profiles.update("guest", GuestUpdate(guest_id=self.gid, first_name="Changed"), self.actor, str(uuid.uuid4()), "tr-profile")
        self.assertEqual(result["first_name"], "Changed")
        service = SavedService(self.repo, ProfileDependencies())
        service.save(SaveProperty(property_id=self.pid, saved=True), self.actor, str(uuid.uuid4()), "tr-save")
        result, _ = service.save(SaveProperty(property_id=self.pid, saved=False), self.actor, str(uuid.uuid4()), "tr-unsave")
        self.assertEqual(result["saved_properties_count"], 0)
        self.assertEqual(self.repo.guest(self.gid)["saved_properties_count"], 0)


if __name__ == "__main__":
    unittest.main()
