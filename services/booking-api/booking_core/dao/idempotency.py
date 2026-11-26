"""Data access for REST `idempotency_keys`."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def get(conn, *, actor_id: str, endpoint: str, key: str, not_before: datetime) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT request_hash, response_code, response_body FROM idempotency_keys "
                    "WHERE actor_id = %s AND endpoint = %s AND idempotency_key = %s AND created_datetime >= %s",
                    (actor_id, endpoint, key, not_before))
        row = cur.fetchone()
    if row and isinstance(row["response_body"], (str, bytes)):
        row["response_body"] = json.loads(row["response_body"])
    return row


def insert(conn, *, actor_id: str, endpoint: str, key: str, request_hash: str, response_code: int,
           response_body: dict[str, Any], now: datetime) -> None:
    """Plain INSERT (no range DELETE here: under REPEATABLE READ that takes gap locks and deadlocks concurrent
    creates). Raises IntegrityError(1062) if the key exists (concurrent same key, or a stale row not yet purged)."""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO idempotency_keys (idempotency_key, actor_id, endpoint, request_hash, response_code, "
                    "response_body, created_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (key, actor_id, endpoint, request_hash, response_code, json.dumps(response_body), now))


def delete_stale(conn, *, actor_id: str, endpoint: str, key: str, older_than: datetime) -> int:
    """Exact-primary-key delete of one expired key (record lock only)."""
    with conn.cursor() as cur:
        return cur.execute("DELETE FROM idempotency_keys WHERE actor_id = %s AND endpoint = %s AND idempotency_key = %s "
                           "AND created_datetime < %s", (actor_id, endpoint, key, older_than))


def purge(conn, older_than: datetime) -> int:
    with conn.cursor() as cur:
        return cur.execute("DELETE FROM idempotency_keys WHERE created_datetime < %s", (older_than,))

