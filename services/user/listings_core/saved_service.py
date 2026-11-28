from .events import activity_event
from .http import ApiError
from .operations import run_operation
from .profile_store import lock_profile, refresh_saved_count
from .property_writes import dumps
from .repository import wire


class SavedService:
    def __init__(self, repository, dependencies):
        self.repo, self.dependencies = repository, dependencies

    def save(self, body, actor, key, trace_id):
        if actor.role != "guest":
            raise ApiError(403, "FORBIDDEN", "Saving requires a guest identity.")
        def action(cursor, now, operation_key):
            self.dependencies.require_guest(actor.sub, trace_id)
            # Consistent order: guest -> property -> relationship. This also
            # serializes save-count changes for the same guest across properties.
            guest = lock_profile(cursor, "guest", actor.sub)
            cursor.execute("SELECT status FROM properties WHERE property_id=%s FOR UPDATE", (body.property_id,))
            prop = cursor.fetchone()
            if prop is None or prop["status"] != "active":
                raise ApiError(404, "PROPERTY_NOT_FOUND", "The property is unavailable for saving.")
            cursor.execute("SELECT saved_datetime FROM saved_properties WHERE guest_id=%s AND property_id=%s FOR UPDATE",
                           (actor.sub, body.property_id))
            previous = cursor.fetchone()
            changed = bool(previous) != body.saved
            saved_datetime = previous["saved_datetime"] if previous else None
            if changed and body.saved:
                saved_datetime = now
                cursor.execute("INSERT INTO saved_properties (guest_id,property_id,saved_datetime) VALUES (%s,%s,%s)",
                               (actor.sub, body.property_id, now))
            elif changed:
                cursor.execute("DELETE FROM saved_properties WHERE guest_id=%s AND property_id=%s", (actor.sub, body.property_id))
            count = refresh_saved_count(cursor, guest, now)
            if changed:
                event = activity_event("property.saved", body.property_id, actor.sub, trace_id, now, operation_key, saved=body.saved)
                cursor.execute("INSERT INTO property_event_outbox (event_id,topic,property_id,envelope,created_datetime) VALUES (%s,%s,%s,%s,%s)",
                               (event["event_id"], "property.saved", body.property_id, dumps(event), now))
            return {"guest_id": actor.sub, "property_id": body.property_id, "saved": body.saved,
                    "saved_datetime": wire(saved_datetime) if body.saved else None, "saved_properties_count": count}
        return run_operation(self.repo, "property_http_operations", actor, "/properties/save", key,
                             body.model_dump(mode="json"), action)
