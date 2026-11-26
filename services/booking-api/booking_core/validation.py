"""Request-body validation (step 1 of /bookings/create and the other endpoints) → 400 VALIDATION_ERROR."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from .errors import validation
from .models import STATUSES

MAX_NIGHTS = 30
MAX_CALENDAR_DAYS = 366
MAX_AVAILABILITY_IDS = 200
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def request_hash(body: Any) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def require_object(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise validation("Request body must be a JSON object.")
    return body


def _id(body: dict, field: str, *, required: bool = True) -> str | None:
    v = body.get(field)
    if v is None and not required:
        return None
    if not isinstance(v, str) or not v.strip() or len(v) > 40:
        raise validation(f"'{field}' is required and must be a string of 1-40 characters.", field=field)
    return v


def _date(body: dict, field: str, *, required: bool = True) -> date | None:
    v = body.get(field)
    if v is None and not required:
        return None
    if not isinstance(v, str) or not _DATE_RE.match(v):
        raise validation(f"'{field}' must be a date in YYYY-MM-DD format.", field=field)
    try:
        return date.fromisoformat(v)
    except ValueError:
        raise validation(f"'{field}' is not a valid calendar date.", field=field) from None


def _int(body: dict, field: str, *, required: bool = True, minimum: int | None = None,
         maximum: int | None = None, default: int | None = None) -> int | None:
    v = body.get(field, default)
    if v is None and not required:
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        raise validation(f"'{field}' must be an integer.", field=field)
    if (minimum is not None and v < minimum) or (maximum is not None and v > maximum):
        raise validation(f"'{field}' must be between {minimum} and {maximum}.", field=field)
    return v


def _text(body: dict, field: str, max_len: int) -> str | None:
    v = body.get(field)
    if v is None:
        return None
    if not isinstance(v, str) or len(v) > max_len:
        raise validation(f"'{field}' must be a string of at most {max_len} characters.", field=field)
    return v.strip() or None


@dataclass(frozen=True)
class CreateRequest:
    property_id: str
    check_in: date
    check_out: date
    guest_count: int
    message_to_host: str | None

    @property
    def nights(self) -> int:
        return (self.check_out - self.check_in).days


def create_request(body: Any, today: date) -> CreateRequest:
    body = require_object(body)
    pid = _id(body, "property_id")
    ci, co = _date(body, "check_in"), _date(body, "check_out")
    gc = _int(body, "guest_count", minimum=1, maximum=1000)
    if co <= ci:
        raise validation("'check_out' must be later than 'check_in'.", field="check_out")
    if ci < today:
        raise validation("'check_in' cannot be in the past.", field="check_in", today=today.isoformat())
    if (co - ci).days > MAX_NIGHTS:
        raise validation(f"A stay can be at most {MAX_NIGHTS} nights.", field="check_out")
    return CreateRequest(pid, ci, co, gc, _text(body, "message_to_host", 1000))


@dataclass(frozen=True)
class TransitionRequest:
    reservation_id: str
    expected_version: int
    reason: str | None


def transition_request(body: Any) -> TransitionRequest:
    body = require_object(body)
    return TransitionRequest(_id(body, "reservation_id"), _int(body, "expected_version", minimum=1, maximum=10**9),
                             _text(body, "reason", 500))


def reservation_ref(body: Any) -> str:
    return _id(require_object(body), "reservation_id")


@dataclass(frozen=True)
class ListRequest:
    statuses: tuple[str, ...] | None
    from_date: date | None
    to_date: date | None
    page: int
    page_size: int
    property_id: str | None


def list_request(body: Any) -> ListRequest:
    body = require_object(body if body is not None else {})
    statuses = body.get("status")
    if statuses is not None:
        if isinstance(statuses, str):
            statuses = [statuses]
        if not isinstance(statuses, list) or not statuses or any(s not in STATUSES for s in statuses):
            raise validation(f"'status' must be a list of: {', '.join(STATUSES)}.", field="status")
        statuses = tuple(dict.fromkeys(statuses))
    fd, td = _date(body, "from_date", required=False), _date(body, "to_date", required=False)
    if fd and td and td < fd:
        raise validation("'to_date' must not be before 'from_date'.", field="to_date")
    return ListRequest(statuses, fd, td, _int(body, "page", minimum=1, maximum=10**6, default=1),
                       _int(body, "page_size", minimum=1, maximum=50, default=20),
                       _id(body, "property_id", required=False))


def calendar_request(body: Any) -> tuple[str, date, date]:
    body = require_object(body)
    pid, fd, td = _id(body, "property_id"), _date(body, "from_date"), _date(body, "to_date")
    if td < fd:
        raise validation("'to_date' must not be before 'from_date'.", field="to_date")
    if (td - fd).days + 1 > MAX_CALENDAR_DAYS:
        raise validation(f"Calendar range can be at most {MAX_CALENDAR_DAYS} days.", field="to_date")
    return pid, fd, td


def availability_check_request(body: Any) -> tuple[list[str], date, date]:
    body = require_object(body)
    ids = body.get("property_ids")
    if (not isinstance(ids, list) or not ids or len(ids) > MAX_AVAILABILITY_IDS
            or any(not isinstance(i, str) or not i or len(i) > 40 for i in ids)):
        raise validation(f"'property_ids' must be a list of 1-{MAX_AVAILABILITY_IDS} property IDs.",
                         field="property_ids")
    ci, co = _date(body, "check_in"), _date(body, "check_out")
    if co <= ci:
        raise validation("'check_out' must be later than 'check_in'.", field="check_out")
    if (co - ci).days > MAX_CALENDAR_DAYS:
        raise validation(f"Range can be at most {MAX_CALENDAR_DAYS} nights.", field="check_out")
    return list(dict.fromkeys(ids)), ci, co


def dashboard_days(body: Any) -> int:
    body = require_object(body if body is not None else {})
    return _int(body, "days", minimum=1, maximum=365, default=30)


def dashboard_property(body: Any) -> str | None:
    return _id(require_object(body if body is not None else {}), "property_id", required=False)
