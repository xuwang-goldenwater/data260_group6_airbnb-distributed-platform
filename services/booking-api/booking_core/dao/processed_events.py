"""Data access for Kafka consumer de-duplication (`processed_events`)."""
from __future__ import annotations

from datetime import datetime


def exists(conn, key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_events WHERE idempotency_key = %s", (key,))
        return cur.fetchone() is not None


def insert(conn, key: str, consumer: str, now: datetime, *, ignore_duplicate: bool = False) -> int:
    verb = "INSERT IGNORE" if ignore_duplicate else "INSERT"
    with conn.cursor() as cur:
        return cur.execute(f"{verb} INTO processed_events (idempotency_key, consumer, processed_datetime) "
                           "VALUES (%s, %s, %s)", (key, consumer, now))
