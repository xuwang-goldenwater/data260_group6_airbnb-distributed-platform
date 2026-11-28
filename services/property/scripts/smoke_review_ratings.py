"""Synthetic Kafka -> Property worker -> MySQL/DLQ check; does not call Review HTTP."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user"), str(ROOT / "scripts")]
from confluent_kafka import Consumer, TopicPartition
from import_user_property_seed import load_snapshot
from listings_core.config import Settings
from listings_core.rating_worker import AcknowledgedPublisher
from listings_core.repository import MySQLRepository
from listings_core.review_ratings import GROUP, require_initialized
from shared.consumer import build


def main():
    cfg = Settings.from_env()
    repo = MySQLRepository(cfg)
    manifest, data = load_snapshot(cfg.seed_dir)
    require_initialized(repo, manifest)
    observer = Consumer({"bootstrap.servers": cfg.kafka_bootstrap,
                         "group.id": "rating-smoke-" + uuid.uuid4().hex,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "allow.auto.create.topics": False})
    try:
        # Observe only new DLQ records, without changing the rating worker's group.
        metadata = observer.list_topics("platform.dlq", timeout=10).topics["platform.dlq"]
        if metadata.error:
            raise RuntimeError("Create platform.dlq before running the smoke check")
        starts = []
        for partition in metadata.partitions:
            tp = TopicPartition("platform.dlq", partition)
            _, high = observer.get_watermark_offsets(tp, timeout=10)
            starts.append(TopicPartition("platform.dlq", partition, high))
        observer.assign(starts)
        pid, trace = "property-rating-" + uuid.uuid4().hex, "tr-rating-smoke-" + uuid.uuid4().hex
        p = deepcopy(data["properties"][0])
        host = p["host_id"]
        p.update(property_id=pid, title="Synthetic rating integration fixture", status="draft", rating=0,
                 review_count=0, version=1)
        p.update(p.pop("location")); p.pop("amenities")
        p["created_datetime"] = p["updated_datetime"] = datetime.now(timezone.utc).replace(tzinfo=None)
        with repo.connection(write=True) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO properties (" + ",".join("`" + k + "`" for k in p) + ") VALUES ("
                           + ",".join(["%s"] * len(p)) + ")", list(p.values()))
        print(json.dumps({"created_fixture_property": pid, "trace_id": trace}), flush=True)
        def review(stars):
            rid = "rev-rating-" + uuid.uuid4().hex
            e = build("review.created", "guest-rating-smoke", "review", rid,
                      {"review_id": rid, "reservation_id": "res-rating-smoke-" + uuid.uuid4().hex,
                       "property_id": pid, "host_id": host, "rating": stars, "tags": ["positive"]},
                      "review.created:" + rid, trace)
            e["producer"] = "property-rating-synthetic-smoke"
            return e
        first, second, invalid = review(5), review(3), review(6)
        replay = deepcopy(first)
        replay["event_id"] = "ev-" + uuid.uuid4().hex
        publish = AcknowledgedPublisher(cfg.kafka_bootstrap)
        for e in (first, replay, second, invalid):
            publish("review.created", pid, e)
        # All four have the same Kafka key/partition. Receiving the last record's
        # DLQ proves the worker processed the earlier duplicate in sequence.
        deadline, dead = time.monotonic() + 45, None
        while time.monotonic() < deadline:
            record = observer.poll(1)
            if record is None:
                continue
            if record.error():
                raise RuntimeError(str(record.error()))
            env = json.loads(record.value())
            payload = env.get("payload", {})
            if payload.get("consumer_group") == GROUP and payload.get("original_event_id") == invalid["event_id"]:
                dead = env
                break
        if dead is None:
            raise RuntimeError("Timed out waiting for property-rating DLQ evidence; check the worker terminal; fixture retained")
        row = repo.property(pid)
        if (row["rating"], row["review_count"], row["version"]) != (4.0, 2, 3):
            raise ValueError("Expected two reviews, rating 4.0 and version 3; inspect retained fixture")
        with repo.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM property_rating_receipts WHERE property_id=%s", (pid,))
            if cursor.fetchone()["n"] != 2:
                raise ValueError("Expected exactly two committed review receipts")
        if (dead.get("trace_id") != trace or dead["payload"]["original_event"] != invalid
                or dead["payload"]["original_key"] != pid
                or dead["payload"]["error_code"] != "INVALID_REVIEW_RATING"):
            raise ValueError("DLQ did not preserve the expected event, key and trace")
        print(json.dumps({"status": "PASS", "property_id": pid, "rating": row["rating"],
                          "review_count": row["review_count"], "version": row["version"],
                          "checks": ["Kafka delivery to property-rating", "two review updates",
                                     "duplicate delivery counted once", "invalid rating sent to DLQ",
                                     "original event, key and trace preserved"],
                          "scope": "Synthetic event integration; Review HTTP/Mongo was not exercised",
                          "fixture": "New draft property retained; original seed listings unchanged"}, indent=2))
    finally:
        observer.close()


if __name__ == "__main__":
    main()
