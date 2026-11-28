import json

from .http import ApiError
from .operations import run_operation
from .profile_store import lock_profile, profile_response
from .property_writes import dumps
from .repository import wire


def write_owner(actor, profile_id, role):
    if actor.role != "admin" and (actor.role != role or actor.sub != profile_id):
        raise ApiError(403, "NOT_PROFILE_OWNER", "Only the profile owner or an administrator may modify it.")


class ProfileService:
    def __init__(self, repository, dependencies):
        self.repo, self.dependencies = repository, dependencies

    def update(self, kind, body, actor, key, trace_id):
        profile_id = getattr(body, kind + "_id")
        write_owner(actor, profile_id, kind)
        payload = body.model_dump(mode="json", exclude_unset=True)
        def action(cursor, now, operation_key):
            original = lock_profile(cursor, kind, profile_id)
            changes = {k: v for k, v in payload.items() if k != kind + "_id"}
            if isinstance(changes.get("preferences"), dict):
                existing = original.get("preferences") or {}
                if isinstance(existing, str):
                    existing = json.loads(existing)
                changes["preferences"] = dumps({**existing, **changes["preferences"]})
            changes["updated_datetime"] = now
            cursor.execute(f"UPDATE {kind}s SET " + ",".join("`" + k + "`=%s" for k in changes)
                           + f" WHERE {kind}_id=%s", [*changes.values(), profile_id])
            return profile_response(lock_profile(cursor, kind, profile_id))
        try:
            return run_operation(self.repo, "user_http_operations", actor, f"/{kind}s/update", key, payload, action)
        except ApiError as error:
            if getattr(error, "mysql_code", None) == 1062:
                raise ApiError(409, "EMAIL_ALREADY_EXISTS", "That email is already in use.") from None
            raise

    def delete_guest(self, body, actor, key, trace_id):
        write_owner(actor, body.guest_id, "guest")
        def action(cursor, now, operation_key):
            lock_profile(cursor, "guest", body.guest_id)
            self.dependencies.require_no_guest_bookings(body.guest_id, trace_id)
            cursor.execute("UPDATE guests SET deleted_datetime=%s,updated_datetime=%s WHERE guest_id=%s",
                           (now, now, body.guest_id))
            return {"guest_id": body.guest_id, "deleted": True, "deleted_datetime": wire(now)}
        return run_operation(self.repo, "user_http_operations", actor, "/guests/delete", key,
                             body.model_dump(mode="json"), action)

    def search(self, body, actor):
        if actor.role not in {"admin", "service"}:
            raise ApiError(403, "FORBIDDEN", "Guest search requires administrative or service access.")
        clauses, args = ["deleted_datetime IS NULL"], []
        def pattern(value):
            return "%" + value.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "%"
        if body.name:
            clauses.append("CONCAT(first_name,' ',last_name) LIKE %s ESCAPE '!'")
            args.append(pattern(body.name))
        if body.email:
            clauses.append("email=%s")
            args.append(str(body.email))
        if body.location:
            clauses.append("location LIKE %s ESCAPE '!'")
            args.append(pattern(body.location))
        where = " AND ".join(clauses)
        with self.repo.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM guests WHERE " + where, args)
            total = cursor.fetchone()["n"]
            cursor.execute("SELECT guest_id,first_name,last_name,location,profile_photo_url FROM guests WHERE "
                           + where + " ORDER BY guest_id LIMIT %s OFFSET %s",
                           [*args, body.page_size, (body.page - 1) * body.page_size])
            rows = wire(list(cursor.fetchall()))
        return {"items": rows, "page": body.page, "page_size": body.page_size, "total": total}
