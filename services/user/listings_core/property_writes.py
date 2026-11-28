from datetime import datetime, timezone
import hashlib
import json
import time
import uuid

from .events import make_event
from .http import ApiError
from .repository import LOCATION, wire


def dumps(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def idempotency_key(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ApiError(400, "VALIDATION_ERROR", "Idempotency-Key must be a UUID.") from None


def read_property(cursor, property_id, *, lock=False):
    cursor.execute("SELECT * FROM properties WHERE property_id = %s" + (" FOR UPDATE" if lock else ""), (property_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    row["location"] = {field: row.pop(field) for field in LOCATION}
    cursor.execute("SELECT amenity_name FROM property_amenities WHERE property_id = %s ORDER BY amenity_name", (property_id,))
    row["amenities"] = [r["amenity_name"] for r in cursor.fetchall()]
    return wire(row)


class PropertyWrites:
    def __init__(self, repository, dependencies, *, sleep=time.sleep):
        self.repo, self.dependencies = repository, dependencies
        self.sleep = sleep

    def execute(self, operation, body, actor, key, trace_id):
        for attempt in range(4):
            try:
                return self._execute_once(operation, body, actor, key, trace_id)
            except ApiError as error:
                # connection() has rolled back and closed before we reach here.
                # Never retry auth errors, lost connections, or uncertain commits.
                if getattr(error, "mysql_code", None) not in {1205, 1213} or attempt == 3:
                    raise
                self.sleep((0.05, 0.2, 0.5)[attempt])

    def _execute_once(self, operation, body, actor, key, trace_id):
        if actor.role not in {"host", "admin"} or operation == "create" and actor.role != "host":
            raise ApiError(403, "HOST_PROFILE_REQUIRED", "An authenticated host is required.")
        key = idempotency_key(key)
        endpoint = "/properties/" + operation
        request = body.model_dump(mode="json", exclude_unset=operation == "update")
        digest = hashlib.sha256(dumps(request).encode()).hexdigest()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.repo.connection(write=True) as conn, conn.cursor() as cursor:
            # Duplicate-key UPDATE takes an exclusive record lock immediately.
            # INSERT IGNORE followed by SELECT FOR UPDATE can deadlock when
            # concurrent replays first acquire shared duplicate-key locks.
            cursor.execute("INSERT INTO property_http_operations "
                           "(actor_id, endpoint, idempotency_key, request_hash, created_datetime) VALUES (%s,%s,%s,%s,%s) "
                           "ON DUPLICATE KEY UPDATE request_hash=request_hash",
                           (actor.sub, endpoint, key, digest, now))
            created = cursor.rowcount == 1
            cursor.execute("SELECT request_hash, response_body FROM property_http_operations "
                           "WHERE actor_id=%s AND endpoint=%s AND idempotency_key=%s FOR UPDATE", (actor.sub, endpoint, key))
            saved = cursor.fetchone()
            if saved["request_hash"] != digest:
                raise ApiError(409, "IDEMPOTENCY_KEY_REUSED", "This key was used with a different request.")
            if not created:
                if saved["response_body"] is None:
                    raise ApiError(503, "DEPENDENCY_UNAVAILABLE", "The original operation has not completed.")
                result = saved["response_body"]
                return json.loads(result) if isinstance(result, str) else result, True
            if operation == "create":
                self.dependencies.require_host(actor.sub, trace_id)
                prop = body.model_dump(mode="json")
                prop.update(property_id="property-" + uuid.uuid4().hex, host_id=actor.sub,
                            status="draft", availability="available", rating=0.0, review_count=0,
                            version=1, created_datetime=now, updated_datetime=now)
                flat = dict(prop)
                flat.update(flat.pop("location"))
                amenities = flat.pop("amenities")
                columns = list(flat)
                cursor.execute("INSERT INTO properties (" + ",".join("`" + c + "`" for c in columns)
                               + ") VALUES (" + ",".join(["%s"] * len(columns)) + ")", [flat[c] for c in columns])
                self._amenities(cursor, prop["property_id"], amenities)
                topics, changed = ["property.created"], list(type(body).model_fields)
            else:
                prop = read_property(cursor, body.property_id, lock=True)
                if prop is None:
                    raise ApiError(404, "PROPERTY_NOT_FOUND", "The requested property does not exist.")
                if actor.role != "admin" and actor.sub != prop["host_id"]:
                    raise ApiError(403, "NOT_PROPERTY_HOST", "Only the owning host may modify this property.")
                if prop["version"] != body.expected_version:
                    raise ApiError(409, "PROPERTY_VERSION_CONFLICT", "Refresh the property before updating.")
                changes = {"status": "archived"} if operation == "delete" else body.changes.model_dump(mode="json", exclude_unset=True)
                if changes.get("status") == "archived":
                    # Both routes must obey this check; status updates cannot bypass it.
                    self.dependencies.require_no_active_bookings(body.property_id, trace_id)
                flat = dict(changes)
                amenities = flat.pop("amenities", None)
                if "location" in flat:
                    flat.update(flat.pop("location"))
                flat.update(version=body.expected_version + 1, updated_datetime=now)
                cursor.execute("UPDATE properties SET " + ",".join("`" + c + "`=%s" for c in flat)
                               + " WHERE property_id=%s AND version=%s",
                               [*flat.values(), body.property_id, body.expected_version])
                if cursor.rowcount != 1:
                    raise ApiError(409, "PROPERTY_VERSION_CONFLICT", "The property changed concurrently.")
                if amenities is not None:
                    cursor.execute("DELETE FROM property_amenities WHERE property_id=%s", (body.property_id,))
                    self._amenities(cursor, body.property_id, amenities)
                topics = ["property.archived"] if operation == "delete" else ["property.updated"]
                if operation == "update" and changes.get("status") == "archived":
                    topics.append("property.archived")
                changed = list(changes)
            prop = read_property(cursor, prop["property_id"])
            for topic in topics:
                event = make_event(topic, prop, actor, trace_id, now, changed)
                cursor.execute("INSERT INTO property_event_outbox "
                               "(event_id, topic, property_id, envelope, created_datetime) VALUES (%s,%s,%s,%s,%s)",
                               (event["event_id"], topic, prop["property_id"], dumps(event), now))
            result = prop if operation != "delete" else {"property_id": prop["property_id"], "status": "archived",
                     "version": prop["version"], "deleted_datetime": wire(now)}
            cursor.execute("UPDATE property_http_operations SET response_body=%s "
                           "WHERE actor_id=%s AND endpoint=%s AND idempotency_key=%s",
                           (dumps(result), actor.sub, endpoint, key))
        return result, False

    @staticmethod
    def _amenities(cursor, property_id, amenities):
        for amenity in sorted(amenities):
            cursor.execute("INSERT IGNORE INTO amenities (amenity_name) VALUES (%s)", (amenity,))
            cursor.execute("INSERT INTO property_amenities (property_id, amenity_name) VALUES (%s,%s)", (property_id, amenity))
