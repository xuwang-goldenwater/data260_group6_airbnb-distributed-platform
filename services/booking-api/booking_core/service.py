"""Booking business logic. Route handlers call these methods; all SQL is in booking_core.dao."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable

from . import events, ids, outbox, validation
from .clients import AnalyticsClient, PropertyClient, UserClient
from .config import Settings
from .dao import availability as avail_dao
from .dao import history as history_dao
from .dao import idempotency as idem_dao
from .dao import reservations as res_dao
from .db import Database, is_duplicate_key, retry_transaction
from .errors import ApiError, DependencyUnavailable, dependency_unavailable, validation as validation_error
from .kafka_producer import EventProducer
from .models import reservation_json
from .auth import Actor
from .timeutil import iso, nights_between, today, utcnow

log = logging.getLogger("booking.service")
IDEMPOTENCY_TTL = timedelta(hours=24)
CENT = Decimal("0.01")


@dataclass
class Result:
    status: int
    body: dict[str, Any]
    replayed: bool = False


class BookingService:
    def __init__(self, settings: Settings, db: Database, producer: EventProducer, properties: PropertyClient,
                 users: UserClient, analytics: AnalyticsClient):
        self.settings, self.db, self.producer = settings, db, producer
        self.properties, self.users, self.analytics = properties, users, analytics

    # ---------------------------------------------------------------- idempotency (REST)
    @staticmethod
    def _require_key(key: str | None) -> str:
        if not key or len(key) > 64:
            raise validation_error("Header 'Idempotency-Key' is required (max 64 characters).",
                                   header="Idempotency-Key")
        return key

    def _idem_lookup(self, actor: Actor, endpoint: str, key: str, req_hash: str) -> Result | None:
        with self.db.transaction() as conn:
            row = idem_dao.get(conn, actor_id=actor.sub, endpoint=endpoint, key=key,
                               not_before=utcnow() - IDEMPOTENCY_TTL)
        if row is None:
            return None
        if row["request_hash"] != req_hash:
            raise ApiError(422, "IDEMPOTENCY_KEY_REUSED",
                           "This Idempotency-Key was already used with a different request body.",
                           details={"idempotency_key": key, "endpoint": endpoint})
        return Result(row["response_code"], row["response_body"], replayed=True)

    def _run_idempotent(self, actor: Actor, endpoint: str, key: str, req_hash: str,
                        work: Callable[[], Result]) -> Result:
        hit = self._idem_lookup(actor, endpoint, key, req_hash)
        if hit:
            return hit
        for attempt in (1, 2):
            try:
                return retry_transaction(work)          # deadlock / lock-wait → retried in place (3x)
            except Exception as exc:
                if not (is_duplicate_key(exc) and "idempotency_keys" in str(exc)):
                    raise
                hit = self._idem_lookup(actor, endpoint, key, req_hash)   # concurrent request with the same key
                if hit:
                    return hit
                if attempt == 2:
                    raise
                with self.db.transaction() as conn:                        # expired row not yet purged
                    idem_dao.delete_stale(conn, actor_id=actor.sub, endpoint=endpoint, key=key,
                                          older_than=utcnow() - IDEMPOTENCY_TTL)
        raise AssertionError("unreachable")

    # ---------------------------------------------------------------- create
    def create(self, actor: Actor, body: Any, idem_key: str | None, trace_id: str) -> Result:
        key = self._require_key(idem_key)
        endpoint = "/bookings/create"
        return self._run_idempotent(actor, endpoint, key, validation.request_hash(body),
                                    lambda: self._create(actor, body, key, endpoint, trace_id))

    def _create(self, actor: Actor, body: Any, key: str, endpoint: str, trace_id: str) -> Result:
        req = validation.create_request(body, today())                                    # step 1 → 400
        try:
            prop = self.properties.get(req.property_id)                                    # step 2
        except DependencyUnavailable:
            raise dependency_unavailable("property-service") from None
        if not prop or prop.get("status") != "active":
            raise ApiError(404, "PROPERTY_NOT_FOUND", "Property does not exist or is not bookable.",
                           details={"property_id": req.property_id})
        if prop["host_id"] == actor.sub:                                                   # step 3 → 403
            raise ApiError(403, "HOST_CANNOT_BOOK_OWN_PROPERTY", "Hosts cannot book their own property.",
                           details={"property_id": req.property_id})
        if req.guest_count > int(prop["capacity"]):                                        # step 4 → 422
            raise ApiError(422, "CAPACITY_EXCEEDED", "Guest count exceeds the property capacity.",
                           details={"property_id": req.property_id, "capacity": prop["capacity"],
                                    "guest_count": req.guest_count})
        nights = nights_between(req.check_in, req.check_out)
        if self.settings.precheck_enabled:                                                 # step 5 → 409
            with self.db.transaction() as conn:
                taken = avail_dao.taken_nights(conn, req.property_id, nights)
            if taken:
                raise ApiError(409, "DATES_UNAVAILABLE", "One or more requested nights are already booked.",
                               details={"property_id": req.property_id,
                                        "conflicting_nights": [d.isoformat() for d in taken]})
        nightly = Decimal(str(prop["nightly_price"])).quantize(CENT, ROUND_HALF_UP)
        cleaning = Decimal(str(prop.get("cleaning_fee") or 0)).quantize(CENT, ROUND_HALF_UP)
        now = utcnow()
        res = {
            "reservation_id": ids.reservation_id(), "property_id": req.property_id, "guest_id": actor.sub,
            "host_id": prop["host_id"], "check_in": req.check_in, "check_out": req.check_out,
            "nights": req.nights, "guest_count": req.guest_count, "nightly_price": nightly,
            "cleaning_fee": cleaning, "total_price": nightly * req.nights + cleaning, "status": "requested",
            "status_reason": None, "message_to_host": req.message_to_host, "cancelled_by": None,
            "trace_id": trace_id, "publish_pending": True, "version": 1,
            "created_datetime": now, "updated_datetime": now,
        }
        response = {
            "reservation_id": res["reservation_id"], "status": "requested", "property_id": req.property_id,
            "check_in": req.check_in.isoformat(), "check_out": req.check_out.isoformat(), "nights": req.nights,
            "guest_count": req.guest_count, "total_price": float(res["total_price"]), "trace_id": trace_id,
            "created_datetime": iso(now),
        }
        with self.db.transaction() as conn:
            res_dao.insert(conn, res)
            history_dao.insert(conn, reservation_id=res["reservation_id"], from_status=None, to_status="requested",
                               actor_id=actor.sub, reason=None, trace_id=trace_id, changed=now)
            idem_dao.insert(conn, actor_id=actor.sub, endpoint=endpoint, key=key,
                            request_hash=validation.request_hash(body), response_code=202,
                            response_body=response, now=now)
        outbox.publish_and_clear(self.db, self.producer, res,
                                 [(events.T_REQUESTED, res["property_id"], events.booking_requested(res))])
        log.info("booking requested reservation_id=%s property_id=%s guest_id=%s trace_id=%s",
                 res["reservation_id"], res["property_id"], actor.sub, trace_id)
        return Result(202, response)

    # ---------------------------------------------------------------- reads
    def get(self, actor: Actor, body: Any) -> dict[str, Any]:
        rid = validation.reservation_ref(body)
        with self.db.transaction() as conn:
            res = res_dao.get(conn, rid)
            if res is None:
                raise ApiError(404, "RESERVATION_NOT_FOUND", "Reservation not found.", details={"reservation_id": rid})
            if actor.sub not in (res["guest_id"], res["host_id"]):
                raise ApiError(403, "NOT_RESERVATION_PARTY", "Only the reservation's guest or host can read it.")
            hist = history_dao.list_for(conn, rid)
        return reservation_json(res, hist)
