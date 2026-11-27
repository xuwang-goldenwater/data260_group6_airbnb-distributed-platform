"""Review service business logic (docs/api/review.md).

Dependencies are passed in, so tests run without MongoDB, Kafka or Booking:
  store      MongoReviewStore | MemoryReviewStore
  bookings   object with get_reservation(reservation_id) -> dict | None,
             raising DependencyUnavailable when Booking cannot be reached
  publisher  object with publish(topic, key, envelope), raising on failure
Errors use the shared error body (docs/api/00_conventions.md), same as Booking.

Write order: insert into MongoDB first (with publish_pending), then publish.
If publishing fails the review stays saved and marked; flush_pending()
republishes it later with the SAME idempotency_key, so consumers never count
a rating twice.
"""
import secrets
import time
from datetime import date, datetime, timezone

from .store import AlreadyResponded, DuplicateReview
from .tagger import TAGGER_VERSION, tag

PRODUCER = "review-service"
SCHEMA_VERSION = "1.0"


class ApiError(Exception):
    def __init__(self, status, code, message, retryable=False, details=None):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message
        self.retryable, self.details = retryable, details

    def body(self, trace_id):
        err = {"code": self.code, "message": self.message, "trace_id": trace_id, "retryable": self.retryable}
        if self.details:
            err["details"] = self.details
        return {"error": err}


class DependencyUnavailable(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id(prefix):
    return f"{prefix}-{int(time.time() * 1000):013d}{secrets.token_hex(4)}"


def envelope(event_type, entity_id, actor_id, payload, trace_id, idempotency_key, when):
    """Shared Kafka envelope (shared/envelope.schema.json)."""
    return {"event_type": event_type, "trace_id": trace_id, "timestamp": when, "actor_id": actor_id,
            "entity": {"entity_type": "review", "entity_id": entity_id}, "payload": payload,
            "idempotency_key": idempotency_key, "attempt": 1, "event_id": _new_id("evt"),
            "producer": PRODUCER, "schema_version": SCHEMA_VERSION}


class ReviewService:
    def __init__(self, store, bookings, publisher, clock=_now, today=date.today):
        self.store, self.bookings, self.publisher = store, bookings, publisher
        self.clock, self.today = clock, today

    # ---------------------------------------------------------------- create
    def create(self, actor, body, idempotency_key, trace_id):
        """actor: {"sub": ..., "role": ...} from the JWT. Returns (status_code, review)."""
        if not idempotency_key:
            raise ApiError(400, "VALIDATION_ERROR", "Idempotency-Key header is required.")
        previous = self.store.by_idempotency_key(idempotency_key)
        if previous:
            return 200, self._public(previous)                        # replay: same result, no new write

        # 1. fields
        rid, rating, text = body.get("reservation_id"), body.get("rating"), body.get("review_text")
        if not isinstance(rid, str) or not rid:
            raise ApiError(400, "VALIDATION_ERROR", "reservation_id is required.")
        if not isinstance(rating, int) or isinstance(rating, bool) or not 1 <= rating <= 5:
            raise ApiError(400, "VALIDATION_ERROR", "rating must be an integer from 1 to 5.")
        if not isinstance(text, str) or not 10 <= len(text.strip()) <= 2000:
            raise ApiError(400, "VALIDATION_ERROR", "review_text must be 10-2000 characters.")
        # 2. reservation exists (Booking service)
        try:
            res = self.bookings.get_reservation(rid)
        except DependencyUnavailable:
            raise ApiError(503, "DEPENDENCY_UNAVAILABLE", "Booking service is unavailable.", retryable=True,
                           details={"dependency": "booking"}) from None
        if res is None:
            raise ApiError(404, "RESERVATION_NOT_FOUND", "Reservation not found.", details={"reservation_id": rid})
        # 3. caller is the reservation's guest (identity from the JWT, never from the body)
        if actor["sub"] != res["guest_id"]:
            raise ApiError(403, "NOT_RESERVATION_GUEST", "Only the guest of this reservation can review it.")
        # 4. stay completed
        if res["status"] != "completed" or date.fromisoformat(res["check_out"]) > self.today():
            raise ApiError(422, "STAY_NOT_COMPLETED", "The stay must be completed before it can be reviewed.",
                           details={"status": res["status"], "check_out": res["check_out"]})
        # 5. one review per reservation (enforced again by the unique index on insert)
        when = self.clock()
        doc = {
            "review_id": _new_id("rev"), "reservation_id": rid,
            "property_id": res["property_id"], "guest_id": res["guest_id"], "host_id": res["host_id"],
            "rating": rating, "review_text": text.strip(), "tags": tag(text), "tagger_version": TAGGER_VERSION,
            "created_datetime": when, "response_text": None, "response_datetime": None,
            "trace_id": trace_id, "idempotency_key": idempotency_key,
            "publish_pending": ["review.created"],
        }
        try:
            self.store.insert(doc)
        except DuplicateReview:
            raise ApiError(409, "REVIEW_ALREADY_EXISTS", "This reservation has already been reviewed.",
                           details={"reservation_id": rid}) from None
        self._publish_created(doc)
        return 201, self._public(self.store.get(doc["review_id"]))

    # ---------------------------------------------------------------- read
    def get(self, body):
        review_id = body.get("review_id")
        doc = self.store.get(review_id) if isinstance(review_id, str) else None
        if doc is None:
            raise ApiError(404, "REVIEW_NOT_FOUND", "Review not found.", details={"review_id": review_id})
        return self._public(doc)

    def by_property(self, body):
        pid = body.get("property_id")
        if not isinstance(pid, str) or not pid:
            raise ApiError(400, "VALIDATION_ERROR", "property_id is required.")
        sort = body.get("sort") or "newest"
        if sort not in ("newest", "highest", "lowest"):
            raise ApiError(400, "VALIDATION_ERROR", "sort must be newest, highest or lowest.")
        page, size = body.get("page", 1), body.get("page_size", 20)
        if not isinstance(page, int) or page < 1 or not isinstance(size, int) or not 1 <= size <= 50:
            raise ApiError(400, "VALIDATION_ERROR", "page must be >= 1 and page_size 1-50.")
        min_rating = body.get("min_rating")
        if min_rating is not None and (not isinstance(min_rating, (int, float)) or not 1 <= min_rating <= 5):
            raise ApiError(400, "VALIDATION_ERROR", "min_rating must be between 1 and 5.")
        items, total = self.store.by_property(pid, sort, min_rating, body.get("tag"), page, size)
        # unknown or new property: 200 with an empty list, not an error
        return {"property_id": pid, "summary": self.store.summary(pid),
                "items": [self._public(d) for d in items], "page": page, "page_size": size, "total": total}

    # ---------------------------------------------------------------- respond
    def respond(self, actor, body, trace_id):
        review_id, text = body.get("review_id"), body.get("response_text")
        doc = self.store.get(review_id) if isinstance(review_id, str) else None
        if doc is None:
            raise ApiError(404, "REVIEW_NOT_FOUND", "Review not found.", details={"review_id": review_id})
        if actor["sub"] != doc["host_id"]:
            raise ApiError(403, "NOT_PROPERTY_HOST", "Only the host of this property can respond.")
        if not isinstance(text, str) or not 1 <= len(text.strip()) <= 1000:
            raise ApiError(400, "VALIDATION_ERROR", "response_text must be 1-1000 characters.")
        try:
            updated = self.store.set_response(review_id, text.strip(), self.clock())
        except AlreadyResponded:
            raise ApiError(409, "RESPONSE_ALREADY_EXISTS", "The host has already responded to this review.") from None
        self._publish(
            "review.responded", updated, actor["sub"], trace_id,
            {"review_id": review_id, "property_id": updated["property_id"], "host_id": updated["host_id"],
             "response_datetime": updated["response_datetime"]})
        return self._public(updated)

    # ---------------------------------------------------------------- events
    def _publish_created(self, doc):
        return self._publish("review.created", doc, doc["guest_id"], doc["trace_id"],
                      {"review_id": doc["review_id"], "reservation_id": doc["reservation_id"],
                       "property_id": doc["property_id"], "host_id": doc["host_id"],
                       "rating": doc["rating"], "tags": doc["tags"]})

    def _publish(self, event_type, doc, actor_id, trace_id, payload):
        env = envelope(event_type, doc["review_id"], actor_id, payload, trace_id,
                       f"{event_type}:{doc['review_id']}", self.clock())
        try:
            self.publisher.publish(event_type, doc["property_id"], env)     # key = property_id
        except Exception:
            return False                                                    # stays pending; flush_pending retries
        if event_type == "review.created":
            self.store.mark_published(doc["review_id"], event_type)
        return True

    def flush_pending(self):
        """Republish events that failed earlier. Returns how many were sent."""
        sent = 0
        for doc in self.store.pending():
            if "review.created" in doc.get("publish_pending", []):
                sent += self._publish_created(doc)
        return sent

    @staticmethod
    def _public(doc):
        return {k: v for k, v in doc.items() if k not in ("idempotency_key", "publish_pending")}
