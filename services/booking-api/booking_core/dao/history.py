"""Data access for `reservation_status_history`."""
from __future__ import annotations

from datetime import datetime
from typing import Any

_COLS = "id, reservation_id, from_status, to_status, actor_id, reason, trace_id, changed_datetime"


def insert(conn, *, reservation_id: str, from_status: str | None, to_status: str, actor_id: str,
           reason: str | None, trace_id: str, changed: datetime) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO reservation_status_history (reservation_id, from_status, to_status, actor_id, "
                    "reason, trace_id, changed_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (reservation_id, from_status, to_status, actor_id, reason, trace_id, changed))
        new_id = cur.lastrowid
    return {"id": new_id, "reservation_id": reservation_id, "from_status": from_status, "to_status": to_status,
            "actor_id": actor_id, "reason": reason, "trace_id": trace_id, "changed_datetime": changed}


def list_for(conn, reservation_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservation_status_history WHERE reservation_id = %s "
                    "ORDER BY changed_datetime, id", (reservation_id,))
        return list(cur.fetchall())


def last_for(conn, reservation_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM reservation_status_history WHERE reservation_id = %s "
                    "ORDER BY changed_datetime DESC, id DESC LIMIT 1", (reservation_id,))
        return cur.fetchone()
