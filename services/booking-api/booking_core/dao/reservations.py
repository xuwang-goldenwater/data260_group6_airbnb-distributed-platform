"""Data access for `reservations`. All SQL for this table lives here."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Sequence

_COLS = ("reservation_id, property_id, guest_id, host_id, check_in, check_out, nights, guest_count, "
         "nightly_price, cleaning_fee, total_price, status, status_reason, message_to_host, cancelled_by, "
         "trace_id, publish_pending, version, created_datetime, updated_datetime")


def insert(conn, r: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO reservations ({_COLS}) VALUES "
            "(%(reservation_id)s, %(property_id)s, %(guest_id)s, %(host_id)s, %(check_in)s, %(check_out)s, "
            "%(nights)s, %(guest_count)s, %(nightly_price)s, %(cleaning_fee)s, %(total_price)s, %(status)s, "
            "%(status_reason)s, %(message_to_host)s, %(cancelled_by)s, %(trace_id)s, %(publish_pending)s, "
            "%(version)s, %(created_datetime)s, %(updated_datetime)s)",
            r,
        )


def get(conn, reservation_id: str, *, for_update: bool = False) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservations WHERE reservation_id = %s"
                    + (" FOR UPDATE" if for_update else ""), (reservation_id,))
        return cur.fetchone()


def update_status(conn, reservation_id: str, *, from_status: str, expected_version: int, to_status: str,
                  reason: str | None, now: datetime, cancelled_by: str | None = None) -> int:
    """Guarded single-statement transition. Returns rows affected (0 → someone else changed it)."""
    with conn.cursor() as cur:
        return cur.execute(
            "UPDATE reservations SET status = %s, status_reason = %s, "
            "cancelled_by = COALESCE(%s, cancelled_by), version = version + 1, publish_pending = TRUE, "
            "updated_datetime = %s "
            "WHERE reservation_id = %s AND status = %s AND version = %s",
            (to_status, reason, cancelled_by, now, reservation_id, from_status, expected_version),
        )


def clear_publish_pending(conn, reservation_id: str, version: int) -> int:
    with conn.cursor() as cur:
        return cur.execute("UPDATE reservations SET publish_pending = FALSE "
                           "WHERE reservation_id = %s AND version = %s AND publish_pending = TRUE",
                           (reservation_id, version))


def list_publish_pending(conn, older_than: datetime, limit: int = 100) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservations WHERE publish_pending = TRUE AND updated_datetime < %s "
                    "ORDER BY updated_datetime LIMIT %s", (older_than, limit))
        return list(cur.fetchall())


def _list(conn, where: list[str], params: list[Any], statuses: Sequence[str] | None, from_date: date | None,
          to_date: date | None, page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
    if statuses:
        where.append("status IN (" + ", ".join(["%s"] * len(statuses)) + ")")
        params.extend(statuses)
    if from_date:
        where.append("check_in >= %s")
        params.append(from_date)
    if to_date:
        where.append("check_in <= %s")
        params.append(to_date)
    clause = " AND ".join(where)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM reservations WHERE {clause}", params)
        total = cur.fetchone()["n"]
        cur.execute(f"SELECT {_COLS} FROM reservations WHERE {clause} "
                    "ORDER BY created_datetime DESC, reservation_id DESC LIMIT %s OFFSET %s",
                    params + [page_size, (page - 1) * page_size])
        return list(cur.fetchall()), total


def list_for_guest(conn, guest_id: str, **kw) -> tuple[list[dict[str, Any]], int]:
    return _list(conn, ["guest_id = %s"], [guest_id], **kw)


def list_for_host(conn, host_id: str, property_id: str | None = None, **kw) -> tuple[list[dict[str, Any]], int]:
    where, params = ["host_id = %s"], [host_id]
    if property_id:
        where.append("property_id = %s")
        params.append(property_id)
    return _list(conn, where, params, **kw)


def count_active(conn, *, column: str, value: str, today: date) -> int:
    assert column in ("property_id", "guest_id")
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM reservations WHERE {column} = %s "
                    "AND status IN ('requested','pending_host','confirmed') AND check_out >= %s",
                    (value, today))
        return cur.fetchone()["n"]


def list_completable(conn, today: date, limit: int = 500) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservations WHERE status = 'confirmed' AND check_out < %s "
                    "ORDER BY check_out LIMIT %s", (today, limit))
        return list(cur.fetchall())


def by_ids(conn, ids: Sequence[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservations WHERE reservation_id IN ("
                    + ", ".join(["%s"] * len(ids)) + ")", list(ids))
        return list(cur.fetchall())
