"""Guest and host dashboards (docs/api/booking.md "Dashboards"). Window = the last `days` days (UTC).

Host metrics are event-based: a request/confirmation/decline/cancellation counts when its status-history
transition happened inside the window. Occupancy uses nights in [today-days, today) of the host's active
properties held by confirmed/completed stays.
"""
from __future__ import annotations

import statistics
from datetime import timedelta
from typing import Any

from . import validation
from .auth import Actor
from .dao import dashboards as dash_dao
from .errors import ApiError
from .models import STATUSES
from .service import BookingService
from .timeutil import today, utcnow


def guest_dashboard(svc: BookingService, actor: Actor, body: Any) -> dict[str, Any]:
    days = validation.dashboard_days(body)
    since = utcnow() - timedelta(days=days)
    with svc.db.transaction() as conn:
        counts = dash_dao.guest_status_counts(conn, actor.sub, since)
        spent = dash_dao.guest_spent(conn, actor.sub, since)
        upcoming = dash_dao.guest_upcoming(conn, actor.sub, today())
    saved, saved_src = svc.users.saved_properties_count(actor.sub)
    return {
        "guest_id": actor.sub,
        "window_days": days,
        "bookings_in_window": sum(counts.values()),
        "status_summary": {s: counts.get(s, 0) for s in STATUSES},
        "upcoming": [{"reservation_id": u["reservation_id"], "property_id": u["property_id"],
                      "check_in": u["check_in"].isoformat(), "status": u["status"]} for u in upcoming],
        "saved_properties_count": saved,
        "total_spent": round(spent, 2),
        "sources": {"saved_properties_count": saved_src},
    }


def host_dashboard(svc: BookingService, actor: Actor, body: Any) -> dict[str, Any]:
    svc.require_host(actor)
    days = validation.dashboard_days(body)
    property_id = validation.dashboard_property(body)
    since = utcnow() - timedelta(days=days)
    props = svc.properties.list_by_host(actor.sub)
    if property_id:
        props = [p for p in props if p["property_id"] == property_id]
        if not props:
            raise ApiError(403, "NOT_PROPERTY_HOST", "That property does not belong to you.",
                           details={"property_id": property_id})
    active_ids = [p["property_id"] for p in props if p.get("status") == "active"]
    end = today()
    with svc.db.transaction() as conn:
        transitions = dash_dao.host_transition_counts(conn, actor.sub, property_id, since)
        revenue = dash_dao.host_revenue(conn, actor.sub, property_id, since)
        booked = dash_dao.booked_nights(conn, active_ids, end - timedelta(days=days), end)
        pairs = dash_dao.host_decision_pairs(conn, actor.sub, property_id, since)
    by_status: dict[str, int] = {}
    cancel = {"by_guest": 0, "by_host": 0}
    for row in transitions:
        by_status[row["to_status"]] = by_status.get(row["to_status"], 0) + row["n"]
        if row["to_status"] == "cancelled":
            cancel["by_host" if row["cancelled_by"] == "host" else "by_guest"] += row["n"]
    minutes = [(p["decided_at"] - p["pending_at"]).total_seconds() / 60 for p in pairs]
    rated = [p for p in props if p.get("review_count")]
    weight = sum(p["review_count"] for p in rated)
    avg_rating = (round(sum(float(p["rating"]) * p["review_count"] for p in rated) / weight, 2) if weight else None)
    funnel = svc.analytics.host_funnel(actor.sub, days, property_id)
    available_nights = len(active_ids) * days
    return {
        "host_id": actor.sub,
        "window_days": days,
        "funnel": {"views": funnel["views"] if funnel else None, "saves": funnel["saves"] if funnel else None,
                   "requests": by_status.get("requested", 0), "confirmed": by_status.get("confirmed", 0)},
        "occupancy_rate": round(booked / available_nights, 4) if available_nights else 0.0,
        "revenue": round(revenue, 2),
        "cancellations": cancel,
        "declined": by_status.get("declined", 0),
        "average_rating": avg_rating,
        "median_response_minutes": round(statistics.median(minutes)) if minutes else None,
        "sources": {"views_saves": "analytics-service" if funnel else "unavailable",
                    "average_rating": svc.properties.host_list_source,
                    "properties": svc.properties.host_list_source},
    }
