"""Role A read APIs. Default: SQL fixture tests. LISTINGS_MYSQL_TEST=1 adds real DB reads."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "services/user"), str(ROOT / "services/property")]
from fastapi.testclient import TestClient
import httpx
import jwt

from listings_core.booking import BookingClient
from listings_core.config import Settings
from listings_core.http import ApiError, unavailable
from listings_core.repository import MySQLRepository
from property_api.app import create_app as property_app
from user_api.app import create_app as user_app

SECRET = "test-only-0123456789abcdef0123456789abcdef"
CFG = Settings(jwt_secret=SECRET)
SEED = CFG.seed_dir


def token(subject="guest-1001", role="guest", **changes):
    claims = {"sub": subject, "role": role, "exp": int(time.time()) + 3600}
    claims.update(changes)
    return {"Authorization": "Bearer " + jwt.encode(claims, SECRET, algorithm="HS256")}


def load(name):
    return json.loads((SEED / (name + ".json")).read_text())


class Cursor:
    def __init__(self, connection):
        self.cursor = connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def execute(self, sql, args=()):
        self.cursor.execute(sql.replace("%s", "?"), args)

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in self.cursor.fetchall()]


class Connection:
    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return Cursor(self.connection)


class SQLFixtureRepository(MySQLRepository):
    """Executes the production SELECTs in SQLite; does NOT claim MySQL validation."""
    def __init__(self):
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        for table in ("guests", "hosts", "properties"):
            rows = load(table)
            flat = []
            for source in rows:
                row = dict(source)
                if table == "guests":
                    row["deleted_datetime"] = None
                    row["preferences"] = json.dumps(row["preferences"])
                if table == "properties":
                    row.update(row.pop("location"))
                    row.pop("amenities")
                flat.append(row)
            fields = list(flat[0])
            defs = []
            for key in fields:
                value = flat[0][key]
                kind = "INTEGER" if isinstance(value, int) else "REAL" if isinstance(value, float) else "TEXT"
                defs.append('"' + key + '" ' + kind + (" COLLATE NOCASE" if key == "city" else ""))
            self.db.execute("CREATE TABLE " + table + " (" + ",".join(defs) + ")")
            self.db.executemany("INSERT INTO " + table + " VALUES (" + ",".join(["?"] * len(fields)) + ")",
                                [list(row.values()) for row in flat])
        self.db.execute("CREATE TABLE amenities (amenity_name TEXT)")
        self.db.execute("CREATE TABLE property_amenities (property_id TEXT, amenity_name TEXT)")
        joins = [(p["property_id"], a) for p in load("properties") for a in p["amenities"]]
        self.db.executemany("INSERT INTO amenities VALUES (?)", [(a,) for a in sorted({a for _, a in joins})])
        self.db.executemany("INSERT INTO property_amenities VALUES (?, ?)", joins)
        self.db.commit()

    @contextmanager
    def connection(self):
        yield Connection(self.db)


class BookingFixture:
    def __init__(self):
        self.blocks = {}
        for b in load("availability_blocks"):
            self.blocks.setdefault(b["property_id"], set()).add(b["night_date"])
        self.calls = []

    def check(self, ids, ci, co, trace):
        self.calls.append((ids, ci, co, trace))
        nights = {(ci + timedelta(days=d)).isoformat() for d in range((co - ci).days)}
        return {pid: not bool(nights & self.blocks.get(pid, set())) for pid in ids}, "2026-11-27T12:00:00Z"


class ReadAPITests(unittest.TestCase):
    def setUp(self):
        self.repo = SQLFixtureRepository()
        self.booking = BookingFixture()
        self.user = TestClient(user_app(CFG, self.repo), raise_server_exceptions=False)
        self.prop = TestClient(property_app(CFG, self.repo, self.booking), raise_server_exceptions=False)

    def tearDown(self):
        self.user.close()
        self.prop.close()
        self.repo.db.close()

    def test_profile_identity_and_service_access(self):
        self.assertEqual(self.user.post("/guests/get", json={"guest_id": "guest-1001"}).status_code, 401)
        own = self.user.post("/guests/get", json={"guest_id": "guest-1001"}, headers=token())
        self.assertEqual(own.status_code, 200)
        self.assertEqual(own.json()["guest_id"], "guest-1001")
        self.assertIsInstance(own.json()["preferences"], dict)
        self.assertNotIn("deleted_datetime", own.json())
        self.assertEqual(self.user.post("/guests/get", json={"guest_id": "guest-1002"}, headers=token()).status_code, 403)
        self.assertEqual(self.user.post("/guests/get", json={"guest_id": "guest-1002"}, headers=token("booking-service", "service")).status_code, 200)

    def test_host_profile_privacy(self):
        body = {"host_id": "host-501"}
        self.assertEqual(self.user.post("/hosts/get", json=body, headers=token()).status_code, 403)
        self.assertEqual(self.user.post("/hosts/get", json=body, headers=token("host-501", "host")).status_code, 200)
        self.assertEqual(self.user.post("/hosts/get", json=body, headers=token("booking-service", "service")).status_code, 200)

    def test_internal_requires_service_and_excludes_deleted(self):
        self.assertEqual(self.user.get("/internal/guests/guest-1001", headers=token()).status_code, 403)
        headers = token("ai-service", "service")
        self.assertEqual(self.user.get("/internal/guests/guest-1001", headers=headers).json(),
                         {"guest_id": "guest-1001", "exists": True, "active": True})
        self.repo.db.execute("UPDATE guests SET deleted_datetime='2026-11-27' WHERE guest_id='guest-1001'")
        self.assertEqual(self.user.get("/internal/guests/guest-1001", headers=headers).status_code, 404)

    def test_invalid_expired_and_unsigned_tokens(self):
        for headers in ({"Authorization": "Bearer broken"}, token(exp=1), token(role="unknown"),
                        {"Authorization": "Bearer " + jwt.encode({"sub": "x", "exp": int(time.time()) + 10}, SECRET, algorithm="HS256")}):
            with self.subTest(headers=str(headers)[:35]):
                self.assertEqual(self.prop.post("/properties/get", json={"property_id": "property-2001"}, headers=headers).status_code, 401)

    def test_missing_auth_configuration_fails_closed(self):
        with TestClient(user_app(replace(CFG, jwt_secret=""), self.repo)) as client:
            self.assertEqual(client.post("/guests/get", json={"guest_id": "guest-1001"}, headers=token()).status_code, 503)

    def test_mysql_outage_returns_503_without_connection_details(self):
        import pymysql
        with patch("listings_core.repository.pymysql.connect", side_effect=pymysql.OperationalError(2003, "private connection details")):
            with TestClient(property_app(CFG)) as client:
                response = client.post("/properties/get", json={"property_id": "property-2001"})
        self.assertEqual(response.status_code, 503)
        self.assertTrue(response.json()["error"]["retryable"])
        self.assertNotIn("private connection details", response.text)

    def test_liveness_readiness_and_unknown_route(self):
        self.assertEqual(self.user.get("/health").status_code, 200)
        self.assertEqual(self.user.get("/ready").status_code, 200)
        self.assertEqual(self.prop.get("/not-a-route").status_code, 404)
        self.assertEqual(self.prop.get("/not-a-route").json()["error"]["code"], "HTTP_ERROR")

    def test_property_hydration_and_status_visibility(self):
        result = self.prop.post("/properties/get", json={"property_id": "property-2007"})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["rating"], 4.3)
        self.assertEqual(result.json()["location"]["city"], "San Jose")
        self.assertEqual(result.json()["amenities"], ["kitchen", "parking", "wifi"])
        pid = "property-2198"
        self.assertEqual(self.prop.post("/properties/get", json={"property_id": pid}).status_code, 404)
        host = self.repo.property(pid)["host_id"]
        self.assertEqual(self.prop.post("/properties/get", json={"property_id": pid}, headers=token(host, "host")).status_code, 200)
        self.assertEqual(self.prop.post("/properties/get", json={"property_id": pid}, headers=token("host-other", "host")).status_code, 404)

    def test_search_all_amenities_and_no_duplicate_rows(self):
        r = self.prop.post("/properties/search", json={"location": "San Jose", "guest_count": 2,
                           "maximum_nightly_price": 250, "required_amenities": ["parking", "kitchen"], "page_size": 50})
        self.assertEqual(r.status_code, 200, r.text)
        rows = r.json()["items"]
        self.assertEqual(len(rows), len({p["property_id"] for p in rows}))
        self.assertTrue(all({"parking", "kitchen"} <= set(p["amenities"]) for p in rows))
        self.assertTrue(all(p["available"] is None for p in rows))
        self.assertEqual(self.booking.calls, [])

    def test_sort_and_pagination(self):
        first = self.prop.post("/properties/search", json={"sort": "price_asc", "page_size": 5}).json()
        second = self.prop.post("/properties/search", json={"sort": "price_asc", "page_size": 5, "page": 2}).json()
        self.assertEqual(first["total"], 197)
        self.assertTrue(max(p["nightly_price"] for p in first["items"]) <= min(p["nightly_price"] for p in second["items"]))
        self.assertFalse({p["property_id"] for p in first["items"]} & {p["property_id"] for p in second["items"]})

    def test_availability_is_filtered_before_total_and_paging(self):
        body = {"location": "San Jose", "check_in": "2026-12-01", "check_out": "2026-12-03",
                "guest_count": 2, "maximum_nightly_price": 250, "required_amenities": ["parking", "kitchen"],
                "available_only": True, "page_size": 50}
        r = self.prop.post("/properties/search", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        ids = [p["property_id"] for p in r.json()["items"]]
        self.assertEqual(len(ids), r.json()["total"])
        self.assertNotIn("property-2002", ids)
        self.assertIn("property-2045", ids)
        small = self.prop.post("/properties/search", json=dict(body, page_size=1, page=2)).json()
        self.assertEqual(small["total"], len(ids))
        self.assertEqual(small["items"][0]["property_id"], ids[1])

    def test_explicit_public_rating_excludes_unknown(self):
        body = {"location": "San Jose", "minimum_rating": 3.8, "page_size": 50}
        ids = [p["property_id"] for p in self.prop.post("/properties/search", json=body).json()["items"]]
        self.assertNotIn("property-2045", ids)

    def test_validation_errors_have_common_envelope(self):
        for body in ({"guest_count": True}, {"guest_count": 0}, {"page_size": 51}, {"sort": "sql"},
                     {"check_in": "2026-12-01"}, {"available_only": True}, {"minimum_rating": 6},
                     {"check_in": "2026-12-03", "check_out": "2026-12-01"},
                     {"minimum_nightly_price": 200, "maximum_nightly_price": 100},
                     {"required_amenities": ["unknown_facility"]}, {"unexpected": 1}):
            with self.subTest(body=body):
                r = self.prop.post("/properties/search", json=body)
                self.assertEqual(r.status_code, 400, r.text)
                self.assertEqual(r.json()["error"]["code"], "VALIDATION_ERROR")
                self.assertTrue(r.json()["error"]["trace_id"])

    def test_malformed_json_is_400(self):
        r = self.prop.post("/properties/search", content="{bad", headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 400)

    def test_sql_values_are_bound(self):
        body = {"location": "San Jose' OR 1=1 --", "page_size": 50}
        r = self.prop.post("/properties/search", json=body)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["total"], 0)

    def test_dependency_failure_does_not_report_available(self):
        class Broken:
            def check(self, *args):
                raise unavailable("Booking availability")
        with TestClient(property_app(CFG, self.repo, Broken())) as client:
            r = client.post("/properties/availability", json={"property_id": "property-2001", "check_in": "2026-12-01", "check_out": "2026-12-03"}, headers={"X-Trace-Id": "tr-test"})
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["error"]["trace_id"], "tr-test")
        self.assertTrue(r.json()["error"]["retryable"])

    def test_capacity_and_held_dates(self):
        body = {"property_id": "property-2002", "check_in": "2026-12-01", "check_out": "2026-12-03"}
        self.assertIs(self.prop.post("/properties/availability", json=body).json()["available"], False)
        r = self.prop.post("/properties/availability", json=dict(body, guest_count=30))
        self.assertEqual(r.status_code, 422)

    def test_internal_candidates_match_all_50_benchmark_requests(self):
        benchmark = json.loads((ROOT / "benchmarks/requests.json").read_text())
        for req in benchmark["requests"]:
            q = req["expected_parse"]
            body = {"pool_id": benchmark["pool_id"], "dataset_version": benchmark["dataset_version"],
                    "selection_rule": "deterministic_filter_then_property_id",
                    "filters": {"location": q["city"], "guest_count": q["guests"], "check_in": q["check_in"],
                                "check_out": q["check_out"], "maximum_nightly_price": q["max_nightly_price"],
                                "required_amenities": q["required_amenities"], "minimum_rating": q["min_rating"]}}
            with self.subTest(request=req["request_id"]):
                result = self.prop.post("/internal/properties/candidates", json=body, headers=token("ai-service", "service"))
                self.assertEqual(result.status_code, 200, result.text)
                self.assertEqual(result.json()["candidate_property_ids"], req["candidate_property_ids"])

    def test_pool_rejects_modified_database_and_wrong_version(self):
        manifest = load("manifest")
        body = {"pool_id": manifest["pool_id"], "dataset_version": manifest["dataset_version"],
                "selection_rule": "deterministic_filter_then_property_id",
                "filters": {"check_in": "2026-12-01", "check_out": "2026-12-03"}}
        url = "/internal/properties/candidates"
        self.assertEqual(self.prop.post(url, json=body, headers=token()).status_code, 403)
        headers = token("ai-service", "service")
        self.assertEqual(self.prop.post(url, json=dict(body, dataset_version="wrong"), headers=headers).status_code, 409)
        self.repo.db.execute("UPDATE properties SET nightly_price=999 WHERE property_id='property-2001'")
        self.assertEqual(self.prop.post(url, json=body, headers=headers).status_code, 409)

    def test_host_listing_internal_route(self):
        r = self.prop.get("/internal/hosts/host-501/properties", headers=token("booking-service", "service"))
        self.assertEqual(r.status_code, 200)
        self.assertTrue(all(p["host_id"] == "host-501" for p in r.json()["items"]))
        self.assertEqual(self.prop.get("/internal/hosts/host-501/properties", headers=token()).status_code, 403)


class BookingHTTPTests(unittest.TestCase):
    def payload(self):
        return {"check_in": "2026-12-01", "check_out": "2026-12-03", "checked_datetime": "2026-11-27T12:00:00Z",
                "results": [{"property_id": "property-2001", "available": True}]}

    def test_http_request_uses_service_jwt_and_dates(self):
        def handler(request):
            self.assertEqual(request.url.path, "/internal/availability/check")
            self.assertEqual(json.loads(request.content)["property_ids"], ["property-2001"])
            claims = jwt.decode(request.headers["Authorization"][7:], SECRET, algorithms=["HS256"])
            self.assertEqual(claims["role"], "service")
            self.assertEqual(request.headers["X-Trace-Id"], "tr-test")
            return httpx.Response(200, json=self.payload())
        client = BookingClient(CFG, transport=httpx.MockTransport(handler))
        free, checked = client.check(["property-2001"], date(2026, 12, 1), date(2026, 12, 3), "tr-test")
        self.assertEqual(free, {"property-2001": True})

    def test_missing_duplicate_mismatched_and_nonboolean_results_rejected(self):
        good = self.payload()
        cases = [dict(good, results=[]), dict(good, results=good["results"] * 2),
                 dict(good, results=[{"property_id": "wrong", "available": True}]),
                 dict(good, results=[{"property_id": "property-2001", "available": "false"}]),
                 dict(good, check_in="2026-11-01"), dict(good, checked_datetime="invalid")]
        for body in cases:
            with self.subTest(body=body):
                client = BookingClient(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(200, json=body)))
                with self.assertRaises(ApiError) as error:
                    client.check(["property-2001"], date(2026, 12, 1), date(2026, 12, 3), "tr-test")
                self.assertEqual(error.exception.status, 503)

    def test_timeout_and_non_success_fail_closed(self):
        for status in (401, 403, 404, 500, 503):
            client = BookingClient(CFG, transport=httpx.MockTransport(lambda req: httpx.Response(status)))
            with self.assertRaises(ApiError):
                client.check(["property-2001"], date(2026, 12, 1), date(2026, 12, 3), "tr-test")
        def timeout(request):
            raise httpx.ReadTimeout("test timeout")
        client = BookingClient(CFG, transport=httpx.MockTransport(timeout))
        with self.assertRaises(ApiError):
            client.check(["property-2001"], date(2026, 12, 1), date(2026, 12, 3), "tr-test")

    def test_large_batches_are_split_without_losing_ids(self):
        ids = [f"property-{n}" for n in range(401)]
        sizes = []
        def handler(request):
            body = json.loads(request.content)
            sizes.append(len(body["property_ids"]))
            return httpx.Response(200, json={**self.payload(), "results": [
                {"property_id": pid, "available": True} for pid in body["property_ids"]]})
        client = BookingClient(CFG, transport=httpx.MockTransport(handler))
        free, _ = client.check(ids, date(2026, 12, 1), date(2026, 12, 3), "tr-test")
        self.assertEqual(sizes, [200, 200, 1])
        self.assertEqual(set(free), set(ids))


@unittest.skipUnless(os.getenv("LISTINGS_MYSQL_TEST") == "1", "Set LISTINGS_MYSQL_TEST=1 for real MySQL reads")
class LiveMySQLTests(unittest.TestCase):
    def test_seed_profiles_and_property_shape(self):
        cfg = Settings.from_env()
        repo = MySQLRepository(cfg)
        self.assertTrue(repo.ping())
        self.assertEqual(repo.guest("guest-1001")["guest_id"], "guest-1001")
        self.assertEqual(repo.host("host-501")["host_id"], "host-501")
        self.assertEqual(repo.property("property-2007")["rating"], 4.3)
        self.assertEqual(repo.property("property-2045")["review_count"], 0)

    def test_api_uses_real_mysql_and_matches_fixed_pool(self):
        # Booking is a fixture here. This checks real MySQL/API integration, not live Booking.
        cfg = replace(Settings.from_env(), jwt_secret=SECRET)
        with TestClient(property_app(cfg, booking=BookingFixture())) as client:
            benchmark = json.loads((ROOT / "benchmarks/requests.json").read_text())
            for req in benchmark["requests"]:
                q = req["expected_parse"]
                body = {"pool_id": benchmark["pool_id"], "dataset_version": benchmark["dataset_version"],
                        "selection_rule": "deterministic_filter_then_property_id",
                        "filters": {"location": q["city"], "guest_count": q["guests"], "check_in": q["check_in"],
                                    "check_out": q["check_out"], "maximum_nightly_price": q["max_nightly_price"],
                                    "required_amenities": q["required_amenities"], "minimum_rating": q["min_rating"]}}
                with self.subTest(request=req["request_id"]):
                    r = client.post("/internal/properties/candidates", json=body, headers=token("ai-service", "service"))
                    self.assertEqual(r.status_code, 200, r.text)
                    self.assertEqual(r.json()["candidate_property_ids"], req["candidate_property_ids"])


if __name__ == "__main__":
    unittest.main()
