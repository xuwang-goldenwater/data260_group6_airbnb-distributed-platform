"""Property-owned projection. Receipts, exact sum and listing change commit together."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json

from shared.consumer import PermanentError, validate
from .http import ApiError

GROUP = "property-rating"
TRANSIENT_MYSQL = {1040, 1042, 1047, 1158, 1159, 1160, 1161, 1205, 1213, 2002, 2003, 2006, 2013}


def transient(error):
    # Auth, missing schema, constraint errors and programming mistakes stop the
    # worker without advancing offsets. Only known connection/lock outages retry.
    return isinstance(error, ApiError) and getattr(error, "mysql_code", None) in TRANSIENT_MYSQL


def average(total, count):
    return (Decimal(total) / count).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP) if count else Decimal(0)


def review_payload(event):
    validate(event)
    if event["event_type"] != "review.created":
        raise PermanentError("UNEXPECTED_EVENT_TYPE", "Only review.created is accepted")
    p = event["payload"]
    for name in ("review_id", "reservation_id", "property_id", "host_id"):
        value = p.get(name)
        if not isinstance(value, str) or not value.strip() or len(value) > 64 or not value.isascii():
            raise PermanentError("INVALID_REVIEW_PAYLOAD", name + " must be a nonempty ASCII ID of at most 64 characters")
    if type(p.get("rating")) is not int or not 1 <= p["rating"] <= 5:
        raise PermanentError("INVALID_REVIEW_RATING", "rating must be an integer from 1 to 5")
    if event["entity"] != {"entity_type": "review", "entity_id": p["review_id"]}:
        raise PermanentError("REVIEW_ENTITY_MISMATCH", "entity must identify the review")
    if event["idempotency_key"] != "review.created:" + p["review_id"]:
        raise PermanentError("REVIEW_KEY_MISMATCH", "Expected review.created:<review_id>")
    return p


def fingerprint(payload):
    # Transport metadata and optional text/tag enrichment do not change a rating.
    core = {k: payload[k] for k in ("review_id", "reservation_id", "property_id", "host_id", "rating")}
    return hashlib.sha256(json.dumps(core, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def record_receipt(cursor, p, key, stamp):
    digest = fingerprint(p)
    cursor.execute("INSERT INTO property_rating_receipts "
                   "(review_id,idempotency_key,property_id,payload_hash,processed_datetime) VALUES (%s,%s,%s,%s,%s) "
                   "ON DUPLICATE KEY UPDATE payload_hash=payload_hash",
                   (p["review_id"], key, p["property_id"], digest, stamp))
    fresh = cursor.rowcount == 1
    cursor.execute("SELECT review_id,idempotency_key,payload_hash FROM property_rating_receipts "
                   "WHERE review_id=%s FOR UPDATE", (p["review_id"],))
    row = cursor.fetchone()
    if row is None or row["idempotency_key"] != key or row["payload_hash"] != digest:
        raise PermanentError("REVIEW_REPLAY_CONFLICT", "Review identity was replayed with different rating data")
    return fresh


def require_initialized(repository, manifest):
    with repository.connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT dataset_version,reviews_sha256 FROM property_rating_bootstrap WHERE singleton=1")
        row = cursor.fetchone()
    if row is None or row["dataset_version"] != manifest["dataset_version"] or row["reviews_sha256"] != manifest["sha256"]["reviews.json"]:
        raise ValueError("Initialize the rating baseline with the matching seed snapshot before starting the worker")


def initialize(repository, manifest, data):
    """Explicit one-time baseline; all imported reviews already contribute to rating.

    Refuse mismatched live aggregates instead of guessing or resetting listings.
    Entire bootstrap is one transaction; rerunning after live reviews is a no-op.
    """
    reviews = {p["property_id"]: [] for p in data["properties"]}
    for r in data["reviews"]:
        reviews[r["property_id"]].append(r)
    with repository.connection(write=True) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT dataset_version,reviews_sha256 FROM property_rating_bootstrap WHERE singleton=1 FOR UPDATE")
        prior = cursor.fetchone()
        if prior:
            if prior["dataset_version"] != manifest["dataset_version"] or prior["reviews_sha256"] != manifest["sha256"]["reviews.json"]:
                raise ValueError("Rating baseline belongs to a different dataset; no data changed")
            return "already_initialized"
        stamp = datetime.now(timezone.utc).replace(tzinfo=None)
        for pid, rows in sorted(reviews.items()):
            cursor.execute("SELECT host_id,rating,review_count FROM properties WHERE property_id=%s FOR UPDATE", (pid,))
            prop = cursor.fetchone()
            total, count = sum(r["rating"] for r in rows), len(rows)
            if prop is None or prop["review_count"] != count or Decimal(str(prop["rating"])) != average(total, count) or any(r["host_id"] != prop["host_id"] for r in rows):
                raise ValueError("Live rating differs from seed for " + pid + "; baseline refused without changes")
            cursor.execute("INSERT INTO property_rating_state (property_id,rating_sum,review_count) VALUES (%s,%s,%s)", (pid, total, count))
            for r in rows:
                record_receipt(cursor, r, "review.created:" + r["review_id"], stamp)
        cursor.execute("INSERT INTO property_rating_bootstrap (singleton,dataset_version,reviews_sha256,initialized_datetime) VALUES (1,%s,%s,%s)",
                       (manifest["dataset_version"], manifest["sha256"]["reviews.json"], stamp))
    return "initialized"


class RatingProjection:
    def __init__(self, repository):
        self.repo = repository

    def apply(self, event):
        p = review_payload(event)
        stamp = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.repo.connection(write=True) as conn, conn.cursor() as cursor:
            if not record_receipt(cursor, p, event["idempotency_key"], stamp):
                return "duplicate"
            cursor.execute("SELECT host_id,rating,review_count FROM properties WHERE property_id=%s FOR UPDATE", (p["property_id"],))
            prop = cursor.fetchone()
            if prop is None:
                raise PermanentError("PROPERTY_NOT_FOUND", "The reviewed property does not exist")
            if prop["host_id"] != p["host_id"]:
                raise PermanentError("REVIEW_HOST_MISMATCH", "Review host does not own this property")
            cursor.execute("SELECT rating_sum,review_count FROM property_rating_state WHERE property_id=%s FOR UPDATE", (p["property_id"],))
            state = cursor.fetchone()
            if state is None:
                if prop["review_count"] != 0 or Decimal(str(prop["rating"])) != 0:
                    raise ValueError("Nonempty property has no rating baseline; offset retained for operator repair")
                state = {"rating_sum": 0, "review_count": 0}
                cursor.execute("INSERT INTO property_rating_state (property_id,rating_sum,review_count) VALUES (%s,0,0)", (p["property_id"],))
            if state["review_count"] != prop["review_count"] or average(state["rating_sum"], state["review_count"]) != Decimal(str(prop["rating"])):
                raise ValueError("Rating projection differs from listing; offset retained for operator repair")
            total, count = state["rating_sum"] + p["rating"], state["review_count"] + 1
            cursor.execute("UPDATE property_rating_state SET rating_sum=%s,review_count=%s WHERE property_id=%s", (total, count, p["property_id"]))
            cursor.execute("UPDATE properties SET rating=%s,review_count=%s,version=version+1,updated_datetime=%s WHERE property_id=%s",
                           (average(total, count), count, stamp, p["property_id"]))
        return "applied"
