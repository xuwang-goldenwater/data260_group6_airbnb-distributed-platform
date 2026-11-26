"""UTC helpers. MySQL stores naive UTC DATETIME(3); JSON uses ISO 8601 with milliseconds and Z."""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone


def utcnow() -> datetime:
    """Naive UTC datetime truncated to milliseconds (matches DATETIME(3))."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def today() -> date:
    """Business 'today' (UTC). BOOKING_CLOCK_DATE=YYYY-MM-DD overrides it for demos/tests."""
    override = os.environ.get("BOOKING_CLOCK_DATE", "").strip()
    if override:
        return date.fromisoformat(override)
    return datetime.now(timezone.utc).date()


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_utc(value: str) -> datetime:
    """Parse '2026-11-04T08:00:00Z' / '...+00:00' into naive UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def nights_between(check_in: date, check_out: date) -> list[date]:
    return [check_in + timedelta(days=i) for i in range((check_out - check_in).days)]
