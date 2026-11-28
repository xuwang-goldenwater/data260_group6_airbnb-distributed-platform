"""Rating transactions plus the actual shared retry/DLQ/offset boundary."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import sys
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts"), str(ROOT / "services/user")]
from test_read_services import load
from test_write_services import WriteCursor, WriteSQLFixture
from import_user_property_seed import load_snapshot
from listings_core.config import Settings
from listings_core.http import unavailable
from listings_core.repository import MySQLRepository
from listings_core.review_ratings import RatingProjection, average, initialize, require_initialized, transient
from listings_core.rating_worker import handle
from shared.consumer import PermanentError, build


def event(pid="property-2001", rating=4, rid=None, host="host-501"):
    rid = rid or "rev-test-" + uuid.uuid4().hex
    return build("review.created", "guest-1001", "review", rid,
                 {"review_id": rid, "reservation_id": "res-test", "property_id": pid,
                  "host_id": host, "rating": rating, "tags": ["positive"]}, "review.created:" + rid)


class RatingCursor(WriteCursor):
    def execute(self, sql, args=()):
        sql = sql.replace("ON DUPLICATE KEY UPDATE payload_hash=payload_hash", "ON CONFLICT DO NOTHING")
        args = [str(v) if isinstance(v, Decimal) else v for v in args]
        return super().execute(sql, args)


class RatingConnection:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return RatingCursor(self.db)


class RatingFixture(WriteSQLFixture):
    def __init__(self):
        super().__init__()
        self.db.executescript("""
        CREATE TABLE property_rating_state (property_id TEXT PRIMARY KEY, rating_sum INTEGER, review_count INTEGER);
        CREATE TABLE property_rating_receipts (review_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE,
          property_id TEXT, payload_hash TEXT, processed_datetime TEXT);
        CREATE TABLE property_rating_bootstrap (singleton INTEGER PRIMARY KEY, dataset_version TEXT,
          reviews_sha256 TEXT, initialized_datetime TEXT);
        """)

    @contextmanager
    def connection(self, *, write=False):
        with super().connection(write=write):
            yield RatingConnection(self.db)


class Record:
    def __init__(self, value, key="property-2001"):
        self.data = json.dumps(value).encode() if isinstance(value, dict) else value
        self.kafka_key = key.encode() if key is not None else None

    def topic(self): return "review.created"
    def partition(self): return 1
    def offset(self): return 42
    def key(self): return self.kafka_key
    def value(self): return self.data


class RatingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest, cls.data = load_snapshot(ROOT / "data/seed/generated")

    def setUp(self):
        self.repo = RatingFixture()
        self.projection = RatingProjection(self.repo)

    def tearDown(self):
        self.repo.db.close()

    def initialize(self):
        return initialize(self.repo, self.manifest, self.data)

    def count(self, table):
        return self.repo.db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]

    def matching_event(self, pid="property-2001", **kw):
        return event(pid, host=self.repo.property(pid)["host_id"], **kw)

    def deliver(self, e, *, key="property-2001", publish=None, commit=None, projection=None):
        return handle(Record(e, key), projection or self.projection, publish or (lambda *a: None),
                      commit or (lambda: None), sleep=lambda _: None)

    def test_bootstrap_preserves_all_listing_fields_and_records_existing_reviews(self):
        before = self.repo.properties()
        self.assertEqual(self.initialize(), "initialized")
        self.assertEqual(self.repo.properties(), before)
        self.assertEqual(self.count("property_rating_state"), 200)
        self.assertEqual(self.count("property_rating_receipts"), 1079)
        require_initialized(self.repo, self.manifest)

    def test_bootstrap_refuses_mismatch_and_rolls_back_every_receipt(self):
        self.repo.db.execute("UPDATE properties SET rating=1 WHERE property_id='property-2197'")
        self.repo.db.commit()
        with self.assertRaises(ValueError): self.initialize()
        self.assertEqual(self.count("property_rating_state"), 0)
        self.assertEqual(self.count("property_rating_receipts"), 0)
        self.assertEqual(self.count("property_rating_bootstrap"), 0)

    def test_baseline_required_and_different_snapshot_rejected(self):
        with self.assertRaises(ValueError): require_initialized(self.repo, self.manifest)
        self.initialize()
        bad = deepcopy(self.manifest)
        bad["sha256"]["reviews.json"] = "0" * 64
        with self.assertRaises(ValueError): require_initialized(self.repo, bad)
        with self.assertRaises(ValueError): initialize(self.repo, bad, self.data)

    def test_imported_review_replay_is_not_added_again(self):
        self.initialize()
        r = next(r for r in self.data["reviews"] if r["property_id"] == "property-2001")
        e = event(r["property_id"], r["rating"], r["review_id"], r["host_id"])
        e["payload"]["reservation_id"] = r["reservation_id"]
        before = self.repo.property("property-2001")
        self.assertEqual(self.projection.apply(e), "duplicate")
        self.assertEqual(self.repo.property("property-2001"), before)

    def test_new_review_and_replay_update_once_with_stable_version(self):
        self.initialize()
        before = self.repo.property("property-2001")
        e = self.matching_event(rating=2)
        self.assertEqual(self.projection.apply(e), "applied")
        row = self.repo.property("property-2001")
        self.assertEqual(row["review_count"], before["review_count"] + 1)
        self.assertEqual(row["rating"], float(average(42, 9)))
        self.assertEqual(row["version"], before["version"] + 1)
        e["event_id"] = "ev-republished"
        e["payload"]["tags"] = ["cleanliness"]
        self.assertEqual(self.projection.apply(e), "duplicate")
        self.assertEqual(self.repo.property("property-2001"), row)
        self.assertEqual(self.initialize(), "already_initialized")

    def test_same_review_changed_rating_is_conflict(self):
        self.initialize()
        e = self.matching_event()
        self.projection.apply(e)
        e["payload"]["rating"] = 1
        with self.assertRaises(PermanentError) as error: self.projection.apply(e)
        self.assertEqual(error.exception.code, "REVIEW_REPLAY_CONFLICT")

    def test_exact_integer_sum_avoids_incremental_rounding_drift(self):
        self.initialize()
        total, count = 40, 8
        for star in [1, 2, 5, 3, 1, 4, 2, 2, 3, 1, 5, 5, 4]:
            self.projection.apply(self.matching_event(rating=star))
            total, count = total + star, count + 1
            self.assertEqual(self.repo.property("property-2001")["rating"], float(average(total, count)))

    def test_first_review_zero_count_and_archived_property(self):
        # Closed listings can receive reviews of historical completed stays.
        self.repo.db.execute("UPDATE properties SET status='archived' WHERE property_id='property-2045'")
        self.repo.db.commit()
        self.projection.apply(self.matching_event("property-2045", rating=3))
        row = self.repo.property("property-2045")
        self.assertEqual((row["rating"], row["review_count"], row["status"]), (3, 1, "archived"))

    def test_missing_property_host_mismatch_roll_back_receipt(self):
        for e in (event("property-missing"), self.matching_event()):
            e["payload"]["host_id"] = "host-wrong"
            with self.assertRaises(PermanentError): self.projection.apply(e)
        self.assertEqual(self.count("property_rating_receipts"), 0)

    def test_nonzero_uninitialized_or_inconsistent_state_stops_without_receipt(self):
        with self.assertRaises(ValueError): self.projection.apply(self.matching_event())
        self.assertEqual(self.count("property_rating_receipts"), 0)
        self.initialize()
        self.repo.db.execute("UPDATE property_rating_state SET rating_sum=39 WHERE property_id='property-2001'")
        self.repo.db.commit()
        with self.assertRaises(ValueError): self.projection.apply(self.matching_event())
        self.assertEqual(self.count("property_rating_receipts"), 1079)

    def test_failed_property_update_rolls_back_sum_and_receipt(self):
        self.initialize()
        before = self.repo.property("property-2001")
        self.repo.db.execute("CREATE TRIGGER reject_rating BEFORE UPDATE ON properties BEGIN SELECT RAISE(ABORT,'injected fault'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.projection.apply(self.matching_event())
        self.assertEqual(self.repo.property("property-2001"), before)
        self.assertEqual(self.count("property_rating_receipts"), 1079)
        self.assertEqual(self.repo.db.execute("SELECT rating_sum FROM property_rating_state WHERE property_id='property-2001'").fetchone()[0], 40)

    def test_invalid_payload_values_go_to_dlq_without_receipt(self):
        for field, value in [("rating", True), ("rating", 2.5), ("rating", 0), ("rating", 6),
                             ("review_id", ""), ("property_id", "x"*65), ("host_id", None)]:
            e, dead = self.matching_event(), []
            e["payload"][field] = value
            self.assertEqual(self.deliver(e, publish=lambda *a: dead.append(a)), "dlq")
            self.assertEqual(len(dead), 1)
            self.assertEqual(dead[0][2]["payload"]["original_event"], e)
        self.assertEqual(self.count("property_rating_receipts"), 0)

    def test_bad_json_missing_envelope_key_and_entity_go_to_dlq(self):
        cases = [(b'bad json', None), (self.matching_event(), "wrong-property")]
        no_field = self.matching_event(); no_field.pop("trace_id")
        bad_entity = self.matching_event(); bad_entity["entity"]["entity_id"] = "rev-other"
        bad_key = self.matching_event(); bad_key["idempotency_key"] = "wrong"
        cases += [(no_field, "property-2001"), (bad_entity, "property-2001"), (bad_key, "property-2001")]
        for e, key in cases:
            order = []
            self.assertEqual(self.deliver(e, key=key, publish=lambda *a: order.append("dlq_ack"), commit=lambda: order.append("commit")), "dlq")
            self.assertEqual(order, ["dlq_ack", "commit"])

    def test_db_commit_precedes_offset_and_replay_after_offset_failure(self):
        self.initialize()
        e = self.matching_event()
        def fail_commit():
            self.assertEqual(self.repo.property("property-2001")["review_count"], 9)
            raise RuntimeError("injected offset failure")
        with self.assertRaises(RuntimeError): self.deliver(e, commit=fail_commit)
        self.assertEqual(self.deliver(e), "processed")
        self.assertEqual(self.repo.property("property-2001")["review_count"], 9)

    def test_transient_retries_three_times_then_dlq_ack_before_offset(self):
        calls, sleeps, order = [], [], []
        class Outage:
            def apply(self, e):
                calls.append(1)
                error = unavailable("MySQL"); error.mysql_code = 2003
                raise error
        dead = []
        def publish(*args): dead.append(args); order.append("dlq_ack")
        with self.assertLogs("platform.consumer", level="WARNING"):
            result = handle(Record(self.matching_event()), Outage(), publish, lambda: order.append("commit"), sleep=sleeps.append)
        self.assertEqual(result, "dlq")
        self.assertEqual((len(calls), sleeps), (4, [.2, 1, 5]))
        self.assertEqual(order, ["dlq_ack", "commit"])
        self.assertEqual(dead[0][2]["payload"]["retry_count"], 3)

    def test_failed_dlq_publish_never_commits_offset(self):
        committed = []
        def fail(*a): raise RuntimeError("no Kafka ack")
        with self.assertRaises(RuntimeError):
            self.deliver(b'bad json', publish=fail, commit=lambda: committed.append(1))
        self.assertEqual(committed, [])

    def test_transient_recovery_applies_once_before_offset_commit(self):
        self.initialize()
        e, calls, committed = self.matching_event(), [], []
        projection = self.projection
        class Recovering:
            def apply(self, value):
                calls.append(1)
                if len(calls) < 3:
                    error = unavailable("MySQL"); error.mysql_code = 1213
                    raise error
                return projection.apply(value)
        def commit():
            self.assertEqual(self.repo.property("property-2001")["review_count"], 9)
            committed.append(1)
        with self.assertLogs("platform.consumer", level="WARNING"):
            self.assertEqual(self.deliver(e, projection=Recovering(), commit=commit), "processed")
        self.assertEqual((len(calls), len(committed)), (3, 1))
        self.assertEqual(self.count("property_rating_receipts"), 1080)

    def test_auth_or_schema_mysql_error_is_not_retried_or_dlqd(self):
        for code in (1045, 1146, 1062):
            error = unavailable("MySQL"); error.mysql_code = code
            self.assertFalse(transient(error))
        calls = []
        class Broken:
            def apply(self, e): raise error
        with self.assertRaises(type(error)):
            self.deliver(self.matching_event(), projection=Broken(), commit=lambda: calls.append(1), publish=lambda *a: calls.append(2))
        self.assertEqual(calls, [])


@unittest.skipUnless(os.getenv("LISTINGS_RATING_MYSQL_TEST") == "1", "Set LISTINGS_RATING_MYSQL_TEST=1 for isolated rating MySQL tests")
class LiveRatingTests(unittest.TestCase):
    def setUp(self):
        self.repo = MySQLRepository(replace(Settings.from_env(), mysql_database="airbnb_property_test"))
        self.pid = "property-rating-" + uuid.uuid4().hex
        self.host = load("properties")[0]["host_id"]
        p = deepcopy(load("properties")[0]); p.update(property_id=self.pid, review_count=0, rating=0, version=1)
        p.update(p.pop("location")); p.pop("amenities")
        p["created_datetime"] = p["updated_datetime"] = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.repo.connection(write=True) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO properties (" + ",".join("`"+k+"`" for k in p) + ") VALUES (" + ",".join(["%s"]*len(p)) + ")", list(p.values()))
        self.projection = RatingProjection(self.repo)

    def tearDown(self):
        with self.repo.connection(write=True) as conn, conn.cursor() as cur:
            for table in ("property_rating_receipts", "property_rating_state", "properties"):
                cur.execute("DELETE FROM " + table + " WHERE property_id=%s", (self.pid,))

    def test_same_review_concurrency_counts_once(self):
        e = event(self.pid, host=self.host)
        def run(i):
            # Retry actual deadlocks with the same transaction boundary as worker.
            from shared.consumer import run_with_retry
            return run_with_retry(lambda: self.projection.apply(e), is_transient=transient)[0]
        with ThreadPoolExecutor(max_workers=4) as pool: results = list(pool.map(run, range(8)))
        self.assertEqual(results.count("applied"), 1)
        self.assertEqual(results.count("duplicate"), 7)
        row = self.repo.property(self.pid)
        self.assertEqual((row["review_count"], row["rating"], row["version"]), (1, 4, 2))

    def test_different_review_concurrency_preserves_sum(self):
        from shared.consumer import run_with_retry
        def run(i):
            e = event(self.pid, rating=i % 5 + 1, host=self.host)
            return run_with_retry(lambda: self.projection.apply(e), is_transient=transient)[0]
        with ThreadPoolExecutor(max_workers=4) as pool: results = list(pool.map(run, range(10)))
        self.assertEqual(results.count("applied"), 10)
        row = self.repo.property(self.pid)
        self.assertEqual((row["review_count"], row["rating"], row["version"]), (10, 3, 11))
