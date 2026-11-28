"""Shared transaction boundary for Role A profile/save operations."""
from datetime import datetime, timezone
import hashlib
import json
import time

from .http import ApiError
from .property_writes import dumps, idempotency_key


def run_operation(repository, table, actor, endpoint, key, body, action, *, sleep=time.sleep):
    if table not in {"user_http_operations", "property_http_operations"}:
        raise ValueError("Unknown operation table")
    key = idempotency_key(key)
    digest = hashlib.sha256(dumps(body).encode()).hexdigest()
    for attempt in range(4):
        try:
            with repository.connection(write=True) as conn, conn.cursor() as cursor:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                cursor.execute("INSERT INTO " + table + " (actor_id,endpoint,idempotency_key,request_hash,created_datetime) "
                               "VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE request_hash=request_hash",
                               (actor.sub, endpoint, key, digest, now))
                created = cursor.rowcount == 1
                cursor.execute("SELECT request_hash,response_body FROM " + table
                               + " WHERE actor_id=%s AND endpoint=%s AND idempotency_key=%s FOR UPDATE",
                               (actor.sub, endpoint, key))
                saved = cursor.fetchone()
                if saved["request_hash"] != digest:
                    raise ApiError(409, "IDEMPOTENCY_KEY_REUSED", "This key was used with a different request.")
                if not created:
                    if saved["response_body"] is None:
                        raise ApiError(503, "DEPENDENCY_UNAVAILABLE", "Original operation has not completed.")
                    result = saved["response_body"]
                    return json.loads(result) if isinstance(result, str) else result, True
                result = action(cursor, now, key)
                cursor.execute("UPDATE " + table + " SET response_body=%s WHERE actor_id=%s AND endpoint=%s AND idempotency_key=%s",
                               (dumps(result), actor.sub, endpoint, key))
            return result, False
        except ApiError as error:
            if getattr(error, "mysql_code", None) not in {1205, 1213} or attempt == 3:
                raise
            sleep((0.05, 0.2, 0.5)[attempt])
