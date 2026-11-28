"""Cache correctness with real API/SQL plus an opt-in Redis+MySQL check."""
from copy import deepcopy
from dataclasses import replace
from datetime import date
import json
import os
import time
import unittest
from unittest.mock import Mock, patch
import uuid

from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from test_read_services import CFG, BookingFixture, SQLFixtureRepository, token
from test_write_services import Dependencies
import test_profile_saved as profiles
from listings_core.cache_invalidation import CacheInvalidator, handle
from listings_core.config import Settings
from listings_core.http import Actor, unavailable
from listings_core.models import Search
from listings_core.property_writes import PropertyWrites
from listings_core.search_cache import SearchCache, make_client, normalized, prefix, revision
from listings_core.write_models import UpdateProperty
from property_api.app import create_app
from shared.consumer import build


class MemoryRedis:
    """Deterministic cache transport, not a claim to test actual Redis/Lua."""
    def __init__(self):
        self.data, self.expiry, self.clock = {}, {}, 0
        self.down = False

    def get(self, key):
        if self.down: raise RedisConnectionError("injected outage")
        if self.expiry.get(key, float('inf')) <= self.clock:
            self.data.pop(key, None); self.expiry.pop(key, None)
        return self.data.get(key)

    def set(self, key, value, *, nx=False, ex=None):
        if self.down: raise RedisConnectionError("injected outage")
        if nx and self.get(key) is not None: return None
        self.data[key] = value
        if ex is not None: self.expiry[key] = self.clock + ex
        return True

    def ping(self):
        if self.down: raise RedisConnectionError("injected outage")
        return True


def invalidation(pid="property-2001"):
    return build("property.updated", "host-501", "property", pid,
                 {"property_id": pid, "host_id": "host-501", "version": 2}, "property.updated:" + pid + ":2")


class Record:
    def __init__(self, e): self.event = e
    def value(self): return json.dumps(self.event).encode()
    def topic(self): return self.event["event_type"]
    def key(self): return self.event["payload"]["property_id"].encode()
    def partition(self): return 0
    def offset(self): return 1


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.repo, self.booking, self.redis = SQLFixtureRepository(), BookingFixture(), MemoryRedis()
        self.cfg = replace(CFG, cache_enabled=True)
        self.cache = SearchCache(self.cfg, client=self.redis)
        self.client = TestClient(create_app(self.cfg, self.repo, self.booking, search_cache=self.cache))
        self.original = self.repo.search
        self.repo.search = Mock(side_effect=self.original)

    def tearDown(self):
        self.client.close(); self.repo.db.close()

    def post(self, body=None):
        r = self.client.post("/properties/search", json=body or {})
        self.assertEqual(r.status_code, 200, r.text)
        return r

    def test_miss_hit_same_body_without_second_filtered_query(self):
        a, b = self.post(), self.post()
        self.assertEqual(a.headers["x-search-cache"], "MISS")
        self.assertEqual(b.headers["x-search-cache"], "HIT")
        self.assertEqual(a.json(), b.json())
        self.assertEqual(self.repo.search.call_count, 1)

    def test_cached_rows_are_not_mutated_by_availability_or_pagination(self):
        body = {"location": "San Jose", "check_in": "2026-12-01", "check_out": "2026-12-03", "available_only": True, "page_size": 1}
        a, b = self.post(body), self.post(body)
        self.assertEqual(a.json(), b.json())
        self.assertEqual(len(self.booking.calls), 2)
        key = next(k for k in self.redis.data if ":rows:" in k)
        self.assertTrue(all("available" not in p for p in json.loads(self.redis.data[key])["rows"]))
        self.assertGreater(len(json.loads(self.redis.data[key])["rows"]), 1)

    def test_booking_changes_override_warm_cache_before_total_and_paging(self):
        body = {"location": "San Jose", "check_in": "2026-12-01", "check_out": "2026-12-03", "available_only": True, "page_size": 50}
        a = self.post(body).json()
        self.assertIn("property-2001", [p["property_id"] for p in a["items"]])
        self.booking.blocks["property-2001"] = {"2026-12-01"}
        b = self.post(body)
        self.assertEqual(b.headers["x-search-cache"], "HIT")
        self.assertNotIn("property-2001", [p["property_id"] for p in b.json()["items"]])
        self.assertEqual(b.json()["total"], a["total"] - 1)

    def test_booking_outage_on_cache_hit_is_503(self):
        body = {"check_in": "2026-12-01", "check_out": "2026-12-03"}
        self.post(body)
        self.booking.check = Mock(side_effect=unavailable("Booking"))
        r = self.client.post("/properties/search", json=body)
        self.assertEqual(r.status_code, 503)

    def test_commit_revision_immediately_invalidates_price_status_and_rating(self):
        for column, value in (("nightly_price", 999), ("rating", 1), ("status", "archived")):
            self.post()
            self.repo.db.execute("UPDATE properties SET " + column + "=?,version=version+1 WHERE property_id='property-2001'", (value,))
            self.repo.db.commit()
            r = self.post()
            self.assertEqual(r.headers["x-search-cache"], "MISS")
            found = next((p for p in r.json()["items"] if p["property_id"] == "property-2001"), None)
            if column == "status": self.assertIsNone(found)
            else: self.assertEqual(found[column], value)

    def test_corrupt_cache_and_redis_outage_fall_back_to_sql(self):
        expected = self.post().json()
        key = next(k for k in self.redis.data if ":rows:" in k)
        for raw in ("not json", json.dumps({"key": key, "rows": [{"property_id": "bad"}]})):
            self.redis.data[key] = raw
            r = self.post()
            self.assertEqual(r.headers["x-search-cache"], "FALLBACK")
            self.assertEqual(r.json(), expected)
        self.redis.down = True
        r = self.post()
        self.assertEqual(r.headers["x-search-cache"], "FALLBACK")
        self.assertEqual(r.json(), expected)

    def test_generation_loss_does_not_reuse_existing_cache_entries(self):
        self.post(); old = self.redis.data.pop(self.cache.prefix + "generation")
        r = self.post()
        self.assertEqual(r.headers["x-search-cache"], "MISS")
        self.assertNotEqual(self.redis.data[self.cache.prefix + "generation"], old)

    def test_expired_cache_is_a_miss(self):
        self.post(); self.redis.clock += self.cfg.cache_ttl_s + 1
        self.assertEqual(self.post().headers["x-search-cache"], "MISS")

    def test_late_fill_cannot_resurrect_old_source_revision(self):
        def concurrent(body):
            rows = self.original(body)
            self.repo.db.execute("UPDATE properties SET nightly_price=999,version=version+1 WHERE property_id='property-2001'")
            self.repo.db.commit()
            self.repo.search.side_effect = self.original
            return rows
        self.repo.search.side_effect = concurrent
        r = self.post()
        self.assertEqual(r.headers["x-search-cache"], "RACE")
        p = next(p for p in r.json()["items"] if p["property_id"] == "property-2001")
        self.assertEqual(p["nightly_price"], 999)
        self.assertFalse(any(":rows:" in k for k in self.redis.data))

    def test_keys_normalize_sets_and_separate_dates_pages_sorts_and_dataset(self):
        a = Search(required_amenities=["kitchen", "parking"], property_types=["house", "room"])
        b = Search(required_amenities=["parking", "kitchen"], property_types=["room", "house"])
        self.assertEqual(normalized(a), normalized(b))
        key = self.cache.key(a, "200:200", "a" * 32)
        for change in ({"page": 2}, {"sort": "price_asc"}, {"check_in": "2026-12-01", "check_out": "2026-12-03"}, {"minimum_rating": 4}):
            body = Search.model_validate({**a.model_dump(mode="json"), **change})
            self.assertNotEqual(key, self.cache.key(body, "200:200", "a" * 32))
        raw = json.loads((CFG.seed_dir / "manifest.json").read_text())
        raw["dataset_version"] += "-new"
        with patch("pathlib.Path.read_text", return_value=json.dumps(raw)):
            self.assertNotEqual(key, self.cache.key(a, "200:200", "a" * 32))

    def test_stats_are_service_only_and_do_not_expose_config_or_requests(self):
        self.post(); self.post()
        self.assertEqual(self.client.get("/internal/cache/stats").status_code, 401)
        self.assertEqual(self.client.get("/internal/cache/stats", headers=token()).status_code, 403)
        r = self.client.get("/internal/cache/stats", headers=token("stats-service", "service"))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["outcomes"], {"miss": 1, "hit": 1})
        self.assertNotIn("redis_url", r.json())

    def test_disabled_cache_has_no_redis_dependency(self):
        cache = SearchCache(replace(CFG, cache_enabled=False), client=Mock())
        self.assertEqual(cache.load(self.repo, Search())[1], "BYPASS")
        self.assertIsNone(cache.redis)

    def test_cache_fill_failure_does_not_fail_search(self):
        original = self.redis.set
        def fail(key, value, **kwargs):
            if ":rows:" in key: raise RedisConnectionError("failed fill")
            return original(key, value, **kwargs)
        self.redis.set = fail
        self.assertEqual(self.post().headers["x-search-cache"], "FALLBACK")


class InvalidationTests(unittest.TestCase):
    def test_invalidation_precedes_offset_commit_and_uses_scoped_receipt(self):
        redis, order = Mock(), []
        redis.eval.side_effect = lambda *a: order.append("redis_ack") or 1
        invalidator = CacheInvalidator(CFG, client=redis)
        status = handle(Record(invalidation()), invalidator, Mock(), lambda: order.append("commit"), sleep=lambda _: None)
        self.assertEqual((status, order), ("processed", ["redis_ack", "commit"]))
        args = redis.eval.call_args.args
        self.assertEqual(args[1], 3)
        self.assertTrue(all(key.startswith(prefix(CFG)) for key in args[2:5]))

    def test_invalid_payload_goes_to_dlq_without_invalidation(self):
        redis, dead = Mock(), []
        e = invalidation(); e["payload"]["version"] = True
        self.assertEqual(handle(Record(e), CacheInvalidator(CFG, client=redis), lambda *a: dead.append(a), Mock(), sleep=lambda _: None), "dlq")
        redis.eval.assert_not_called()
        self.assertEqual(dead[0][2]["payload"]["original_event"], e)

    def test_redis_outage_retries_then_dlq_ack_before_offset(self):
        redis, order = Mock(), []
        redis.eval.side_effect = RedisConnectionError("injected outage")
        with self.assertLogs("platform.consumer", level="WARNING"):
            handle(Record(invalidation()), CacheInvalidator(CFG, client=redis), lambda *a: order.append("dlq_ack"), lambda: order.append("commit"), sleep=lambda _: None)
        self.assertEqual(redis.eval.call_count, 4)
        self.assertEqual(order, ["dlq_ack", "commit"])

    def test_failed_dlq_ack_does_not_commit(self):
        e, commit = invalidation(), Mock()
        e["payload"]["version"] = 0
        def fail(*a): raise RuntimeError("no broker acknowledgement")
        with self.assertRaises(RuntimeError): handle(Record(e), CacheInvalidator(CFG, client=Mock()), fail, commit)
        commit.assert_not_called()


@unittest.skipUnless(os.getenv("LISTINGS_CACHE_INTEGRATION_TEST") == "1", "Set LISTINGS_CACHE_INTEGRATION_TEST=1 for Redis+MySQL cache test")
class LiveCacheTests(unittest.TestCase):
    def setUp(self):
        profiles.LiveProfileSavedTests.setUp(self)
        # Use a dedicated namespace, never FLUSHDB and never touch another cache.
        self.cfg = replace(self.repo.settings, cache_enabled=True, mysql_host=self.repo.settings.mysql_host)
        self.redis = make_client(self.cfg)
        self.cache = SearchCache(self.cfg, client=self.redis)
        self.cache.prefix += "test-" + uuid.uuid4().hex + ":"
        self.invalidator = CacheInvalidator(self.cfg, client=self.redis)
        self.invalidator.prefix = self.cache.prefix
        self.city = "Cache Test " + uuid.uuid4().hex
        with self.repo.connection(write=True) as conn, conn.cursor() as cursor:
            cursor.execute("UPDATE properties SET city=%s,version=version+1 WHERE property_id=%s", (self.city, self.pid))

    def tearDown(self):
        for key in self.redis.scan_iter(match=self.cache.prefix + "*"):
            self.redis.delete(key)
        profiles.LiveProfileSavedTests.tearDown(self)

    def test_real_cache_commit_revision_and_atomic_event_dedup(self):
        body = Search(location=self.city)
        self.assertEqual(self.cache.load(self.repo, body)[1], "MISS")
        self.assertEqual(self.cache.load(self.repo, body)[1], "HIT")
        writes = PropertyWrites(self.repo, Dependencies())
        current = self.repo.property(self.pid)
        writes.execute("update", UpdateProperty(property_id=self.pid, expected_version=current["version"], changes={"nightly_price": 123.0}),
                       Actor(self.gid, "admin"), str(uuid.uuid4()), "tr-cache-test")
        rows, status = self.cache.load(self.repo, body)
        self.assertEqual((status, rows[0]["nightly_price"]), ("MISS", 123.0))
        e = invalidation(self.pid)
        self.assertEqual(self.invalidator.apply(e), 1)
        self.assertEqual(self.invalidator.apply(e), 0)
        self.assertEqual(self.cache.load(self.repo, body)[1], "MISS")
        self.assertEqual(self.redis.get(self.cache.prefix + "invalidations"), "1")
