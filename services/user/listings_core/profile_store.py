"""User-owned profile persistence, including the shared-DB saved-count projection."""
import json

from .http import ApiError
from .repository import wire


def lock_profile(cursor, kind, profile_id):
    if kind not in {"guest", "host"}:
        raise ValueError("Unknown profile kind")
    cursor.execute(f"SELECT * FROM {kind}s WHERE {kind}_id=%s FOR UPDATE", (profile_id,))
    row = cursor.fetchone()
    if row is None or kind == "guest" and row.get("deleted_datetime") is not None:
        raise ApiError(404, kind.upper() + "_NOT_FOUND", "The requested profile does not exist.")
    return row


def profile_response(row):
    row = dict(row)
    row.pop("deleted_datetime", None)
    if isinstance(row.get("preferences"), str):
        row["preferences"] = json.loads(row["preferences"])
    return wire(row)


def refresh_saved_count(cursor, guest, now):
    # Both Role A services deliberately share the existing MySQL database.
    # Caller holds this guest row lock before touching saved_properties.
    cursor.execute("SELECT COUNT(*) AS n FROM saved_properties WHERE guest_id=%s", (guest["guest_id"],))
    count = cursor.fetchone()["n"]
    if count != guest["saved_properties_count"]:
        cursor.execute("UPDATE guests SET saved_properties_count=%s,updated_datetime=%s WHERE guest_id=%s",
                       (count, now, guest["guest_id"]))
    return count
