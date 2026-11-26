"""Data access for `availability_blocks` — the double-booking guard."""
from __future__ import annotations

from datetime import date, datetime
from typing import Sequence


def taken_nights(conn, property_id: str, nights: Sequence[date]) -> list[date]:
    if not nights:
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT night_date FROM availability_blocks WHERE property_id = %s AND night_date IN ("
                    + ", ".join(["%s"] * len(nights)) + ") ORDER BY night_date", [property_id, *nights])
        return [r["night_date"] for r in cur.fetchall()]


def taken_in_range(conn, property_id: str, from_date: date, to_date: date) -> list[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT night_date FROM availability_blocks WHERE property_id = %s "
                    "AND night_date BETWEEN %s AND %s ORDER BY night_date", (property_id, from_date, to_date))
        return [r["night_date"] for r in cur.fetchall()]


def insert_blocks(conn, property_id: str, nights: Sequence[date], reservation_id: str, now: datetime) -> int:
    """One multi-row INSERT. A duplicate (property_id, night_date) fails the whole statement (no partial hold)."""
    rows = [(property_id, n, reservation_id, now) for n in nights]
    with conn.cursor() as cur:
        return cur.execute("INSERT INTO availability_blocks (property_id, night_date, reservation_id, created_datetime) "
                           "VALUES " + ", ".join(["(%s, %s, %s, %s)"] * len(rows)),
                           [v for row in rows for v in row])


def release(conn, reservation_id: str) -> list[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT night_date FROM availability_blocks WHERE reservation_id = %s ORDER BY night_date "
                    "FOR UPDATE", (reservation_id,))
        nights = [r["night_date"] for r in cur.fetchall()]
        cur.execute("DELETE FROM availability_blocks WHERE reservation_id = %s", (reservation_id,))
    return nights


def nights_for(conn, reservation_id: str) -> list[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT night_date FROM availability_blocks WHERE reservation_id = %s ORDER BY night_date",
                    (reservation_id,))
        return [r["night_date"] for r in cur.fetchall()]


def unavailable_properties(conn, property_ids: Sequence[str], check_in: date, check_out: date) -> set[str]:
    if not property_ids:
        return set()
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT property_id FROM availability_blocks WHERE property_id IN ("
                    + ", ".join(["%s"] * len(property_ids)) + ") AND night_date >= %s AND night_date < %s",
                    [*property_ids, check_in, check_out])
        return {r["property_id"] for r in cur.fetchall()}
