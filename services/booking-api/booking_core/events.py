"""Builds the Booking events for a reservation's current state.

The same function is used for the first publish and for the outbox retry, so a retried event has the
same idempotency_key (booking.requested:<id>, booking.updated:<id>:v<version>, availability:<id>:<change>).
"""
from __future__ import annotations

from typing import Any

from . import envelope
from .models import _json_value
from .timeutil import nights_between

T_REQUESTED = "booking.requested"
T_UPDATED = "booking.updated"
T_AVAILABILITY = "property.availability.updated"
T_DLQ = "platform.dlq"

HELD_ON = {"pending_host"}
RELEASED_ON = {"declined", "cancelled"}
HOLDING_FROM = {"pending_host", "confirmed"}


def booking_requested(res: dict[str, Any], *, producer: str = "booking-api", attempt: int = 1) -> dict[str, Any]:
    return envelope.build(
        event_type=T_REQUESTED, trace_id=res["trace_id"], actor_id=res["guest_id"], entity_type="reservation",
        entity_id=res["reservation_id"], idempotency_key=f"{T_REQUESTED}:{res['reservation_id']}",
        producer=producer, timestamp=res["created_datetime"], attempt=attempt,
        payload={k: _json_value(res[k]) for k in ("reservation_id", "property_id", "guest_id", "host_id",
                                                   "check_in", "check_out", "guest_count")},
    )


def events_for_transition(res: dict[str, Any], hist: dict[str, Any], *, producer: str,
                          attempt: int = 1) -> list[tuple[str, str, dict[str, Any]]]:
    """(topic, key, envelope) list for the transition recorded in `hist` (res = row after the transition)."""
    rid, pid = res["reservation_id"], res["property_id"]
    out = [(T_UPDATED, pid, envelope.build(
        event_type=T_UPDATED, trace_id=hist["trace_id"], actor_id=hist["actor_id"], entity_type="reservation",
        entity_id=rid, idempotency_key=f"{T_UPDATED}:{rid}:v{res['version']}", producer=producer,
        timestamp=hist["changed_datetime"], attempt=attempt,
        payload={
            "reservation_id": rid, "property_id": pid, "guest_id": res["guest_id"], "host_id": res["host_id"],
            "from_status": hist["from_status"], "to_status": hist["to_status"], "reason": hist["reason"],
            "check_in": _json_value(res["check_in"]), "check_out": _json_value(res["check_out"]),
            "total_price": float(res["total_price"]), "version": res["version"],
        }))]
    change = None
    if hist["to_status"] in HELD_ON:
        change = "held"
    elif hist["to_status"] in RELEASED_ON and hist["from_status"] in HOLDING_FROM:
        change = "released"
    if change:
        nights = [n.isoformat() for n in nights_between(res["check_in"], res["check_out"])]
        out.append((T_AVAILABILITY, pid, envelope.build(
            event_type=T_AVAILABILITY, trace_id=hist["trace_id"], actor_id=hist["actor_id"], entity_type="property",
            entity_id=pid, idempotency_key=f"availability:{rid}:{change}", producer=producer,
            timestamp=hist["changed_datetime"], attempt=attempt,
            payload={"property_id": pid, "change": change, "nights": nights, "reservation_id": rid})))
    return out


def pending_events(res: dict[str, Any], last_hist: dict[str, Any] | None, *, producer: str,
                   attempt: int = 1) -> list[tuple[str, str, dict[str, Any]]]:
    """Events still owed for a row with publish_pending = TRUE."""
    if res["status"] == "requested" and res["version"] == 1:
        return [(T_REQUESTED, res["property_id"], booking_requested(res, producer=producer, attempt=attempt))]
    if last_hist is None:
        return []
    return events_for_transition(res, last_hist, producer=producer, attempt=attempt)
