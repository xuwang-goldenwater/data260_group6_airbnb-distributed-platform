"""Record guest detail views durably without caching the property response."""
import uuid

from .events import view_event
from .http import ApiError
from .operations import run_operation
from .profile_store import lock_profile
from .property_writes import dumps


class PropertyViews:
    def __init__(self, repository):
        self.repo = repository

    def record(self, property_id, actor, trace_id, key=None):
        if actor.role != "guest":
            raise ApiError(403, "FORBIDDEN", "View activity requires a guest identity.")
        # Existing read clients need no new header. They create one view per
        # successful request; retry-aware clients may reuse an explicit UUID.
        operation_key = str(uuid.uuid4()) if key is None else key
        def action(cursor, now, normalized_key):
            lock_profile(cursor, "guest", actor.sub)
            cursor.execute("SELECT status FROM properties WHERE property_id=%s FOR UPDATE", (property_id,))
            row = cursor.fetchone()
            if row is None or row["status"] != "active":
                raise ApiError(404, "PROPERTY_NOT_FOUND", "The requested property does not exist.")
            event = view_event(property_id, actor.sub, trace_id, now, normalized_key)
            cursor.execute("INSERT INTO property_event_outbox (event_id,topic,property_id,envelope,created_datetime) VALUES (%s,%s,%s,%s,%s)",
                           (event["event_id"], "property.viewed", property_id, dumps(event), now))
            return {"event_id": event["event_id"]}
        return run_operation(self.repo, "property_http_operations", actor, "/properties/view", operation_key,
                             {"property_id": property_id}, action)
