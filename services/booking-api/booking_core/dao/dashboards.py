"""Aggregate queries for /dashboards/guest and /dashboards/host."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Sequence


def guest_status_counts(conn, guest_id: str, since: datetime) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) AS n FROM reservations WHERE guest_id = %s AND created_datetime >= %s "
                    "GROUP BY status", (guest_id, since))
        return {r["status"]: r["n"] for r in cur.fetchall()}


def guest_spent(conn, guest_id: str, since: datetime) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(total_price), 0) AS s FROM reservations WHERE guest_id = %s "
                    "AND created_datetime >= %s AND status IN ('confirmed','completed')", (guest_id, since))
        return float(cur.fetchone()["s"])


def guest_upcoming(conn, guest_id: str, today: date, limit: int = 10) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT reservation_id, property_id, check_in, check_out, status FROM reservations "
                    "WHERE guest_id = %s AND check_in >= %s AND status IN ('requested','pending_host','confirmed') "
                    "ORDER BY check_in, reservation_id LIMIT %s", (guest_id, today, limit))
        return list(cur.fetchall())


def _host_filter(host_id: str, property_id: str | None) -> tuple[str, list[Any]]:
    clause, params = "r.host_id = %s", [host_id]
    if property_id:
        clause += " AND r.property_id = %s"
        params.append(property_id)
    return clause, params


def host_transition_counts(conn, host_id: str, property_id: str | None, since: datetime) -> list[dict[str, Any]]:
    """Transitions that happened in the window, grouped by target status and cancelled_by."""
    clause, params = _host_filter(host_id, property_id)
    with conn.cursor() as cur:
        cur.execute("SELECT h.to_status, r.cancelled_by, COUNT(*) AS n FROM reservation_status_history h "
                    "JOIN reservations r ON r.reservation_id = h.reservation_id "
                    f"WHERE {clause} AND h.changed_datetime >= %s GROUP BY h.to_status, r.cancelled_by",
                    params + [since])
        return list(cur.fetchall())


def host_revenue(conn, host_id: str, property_id: str | None, since: datetime) -> float:
    """Total price of reservations confirmed in the window that are still confirmed/completed."""
    clause, params = _host_filter(host_id, property_id)
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(r.total_price), 0) AS s FROM reservations r "
                    f"WHERE {clause} AND r.status IN ('confirmed','completed') AND EXISTS ("
                    "SELECT 1 FROM reservation_status_history h WHERE h.reservation_id = r.reservation_id "
                    "AND h.to_status = 'confirmed' AND h.changed_datetime >= %s)", params + [since])
        return float(cur.fetchone()["s"])


def booked_nights(conn, property_ids: Sequence[str], start: date, end_exclusive: date) -> int:
    if not property_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM availability_blocks b JOIN reservations r "
                    "ON r.reservation_id = b.reservation_id WHERE b.property_id IN ("
                    + ", ".join(["%s"] * len(property_ids)) + ") AND b.night_date >= %s AND b.night_date < %s "
                    "AND r.status IN ('confirmed','completed')", [*property_ids, start, end_exclusive])
        return cur.fetchone()["n"]


def host_decision_pairs(conn, host_id: str, property_id: str | None, since: datetime) -> list[dict[str, Any]]:
    """(entered pending_host, host decision time) for decisions made in the window."""
    clause, params = _host_filter(host_id, property_id)
    with conn.cursor() as cur:
        cur.execute("SELECT p.changed_datetime AS pending_at, d.changed_datetime AS decided_at "
                    "FROM reservation_status_history d "
                    "JOIN reservations r ON r.reservation_id = d.reservation_id "
                    "JOIN reservation_status_history p ON p.reservation_id = d.reservation_id "
                    "  AND p.to_status = 'pending_host' "
                    f"WHERE {clause} AND d.from_status = 'pending_host' AND d.to_status IN ('confirmed','declined') "
                    "AND d.changed_datetime >= %s", params + [since])
        return list(cur.fetchall())
