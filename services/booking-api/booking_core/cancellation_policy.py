"""The single place for the cancellation rule (§13.12 "change the cancel rule" drill).

Current rule (docs/api/booking.md): requested / pending_host can be cancelled any time by guest or host;
a confirmed reservation can be cancelled only before check_in (i.e. today < check_in).
To change the rule (e.g. "at least 2 days before check-in"), edit CONFIRMED_MIN_DAYS_BEFORE_CHECK_IN or evaluate().
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

CANCELLABLE_STATUSES = frozenset({"requested", "pending_host", "confirmed"})
CONFIRMED_MIN_DAYS_BEFORE_CHECK_IN = 1   # 1 → must cancel on a day strictly before check_in


@dataclass(frozen=True)
class Decision:
    allowed: bool
    code: str | None = None
    message: str | None = None


def evaluate(reservation: dict[str, Any], cancelled_by: str, today: date) -> Decision:
    """Status eligibility is checked by the transition table; this decides the time window."""
    if reservation["status"] == "confirmed":
        days_before = (reservation["check_in"] - today).days
        if days_before < CONFIRMED_MIN_DAYS_BEFORE_CHECK_IN:
            return Decision(False, "CANCELLATION_WINDOW_CLOSED",
                            "A confirmed reservation can only be cancelled before check-in.")
    return Decision(True)


def status_reason(cancelled_by: str) -> str:
    return "GUEST_CANCELLED" if cancelled_by == "guest" else "HOST_CANCELLED"
