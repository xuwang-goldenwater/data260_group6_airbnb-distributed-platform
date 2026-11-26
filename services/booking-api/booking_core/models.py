"""Reservation entity: statuses, allowed transitions and JSON serialization."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .errors import ApiError
from .timeutil import iso

STATUSES = ("requested", "pending_host", "confirmed", "rejected", "declined", "cancelled", "completed")
ACTIVE_STATUSES = ("requested", "pending_host", "confirmed")
HOLDING_STATUSES = ("pending_host", "confirmed", "completed")

# Exactly the arrows in docs/api/booking.md "Reservation Status Lifecycle".
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "requested": frozenset({"pending_host", "rejected", "cancelled"}),
    "pending_host": frozenset({"confirmed", "declined", "cancelled"}),
    "confirmed": frozenset({"completed", "cancelled"}),
    "rejected": frozenset(),
    "declined": frozenset(),
    "cancelled": frozenset(),
    "completed": frozenset(),
}


def is_allowed(from_status: str, to_status: str) -> bool:
    return to_status in ALLOWED_TRANSITIONS.get(from_status, frozenset())


def assert_transition(from_status: str, to_status: str, reservation_id: str) -> None:
    if not is_allowed(from_status, to_status):
        raise ApiError(409, "INVALID_STATUS_TRANSITION",
                       f"Cannot move a reservation from '{from_status}' to '{to_status}'.",
                       details={"reservation_id": reservation_id, "from_status": from_status,
                                "to_status": to_status})


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


RESERVATION_FIELDS = (
    "reservation_id", "property_id", "guest_id", "host_id", "check_in", "check_out", "nights",
    "guest_count", "nightly_price", "cleaning_fee", "total_price", "status", "status_reason",
    "message_to_host", "cancelled_by", "version", "trace_id", "created_datetime", "updated_datetime",
)


def reservation_json(row: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    out = {k: _json_value(row[k]) for k in RESERVATION_FIELDS}
    if history is not None:
        out["status_history"] = [history_json(h) for h in history]
    return out


def history_json(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _json_value(row[k]) for k in ("from_status", "to_status", "actor_id", "reason", "changed_datetime")}


def summary_json(row: dict[str, Any], *, host_view: bool = False) -> dict[str, Any]:
    keys = ["reservation_id", "property_id", "check_in", "check_out", "total_price", "status", "created_datetime"]
    if host_view:
        keys[2:2] = ["guest_id", "guest_count"]
    return {k: _json_value(row[k]) for k in keys}
