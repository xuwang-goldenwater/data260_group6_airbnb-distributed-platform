from contextlib import contextmanager, suppress
from datetime import date, datetime, timezone
from decimal import Decimal
import json

import pymysql

from .http import unavailable

LOCATION = ("address_line_1", "city", "state", "postal_code", "country", "latitude", "longitude")


def wire(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [wire(v) for v in value]
    return value


def criteria(filters, *, include_unknown_rating=False):
    clauses = ["p.status = %s", "p.capacity >= %s", "p.bedrooms >= %s", "p.nightly_price >= %s"]
    args = ["active", filters.guest_count, filters.minimum_bedrooms, filters.minimum_nightly_price]
    if filters.location is not None:
        clauses.append("p.city = %s")
        args.append(filters.location)
    if filters.maximum_nightly_price is not None:
        clauses.append("p.nightly_price <= %s")
        args.append(filters.maximum_nightly_price)
    if filters.property_types:
        clauses.append("p.property_type IN (" + ",".join(["%s"] * len(filters.property_types)) + ")")
        args.extend(filters.property_types)
    if filters.minimum_rating is not None:
        clauses.append("(p.rating >= %s OR p.review_count = 0)" if include_unknown_rating else "p.rating >= %s")
        args.append(filters.minimum_rating)
    for amenity in filters.required_amenities:
        clauses.append("EXISTS (SELECT 1 FROM property_amenities pa WHERE pa.property_id = p.property_id AND pa.amenity_name = %s)")
        args.append(amenity)
    return " AND ".join(clauses), args


class MySQLRepository:
    def __init__(self, settings):
        self.settings = settings

    @contextmanager
    def connection(self, *, write=False):
        s = self.settings
        conn = None
        try:
            conn = pymysql.connect(host=s.mysql_host, port=s.mysql_port, user=s.mysql_user,
                                   password=s.mysql_password, database=s.mysql_database,
                                   charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
                                   autocommit=False, connect_timeout=3, read_timeout=5,
                                   write_timeout=5, init_command="SET time_zone = '+00:00'")
            conn.begin()
            yield conn
            if write:
                conn.commit()
        except pymysql.MySQLError as exc:
            error = unavailable("MySQL")
            # Retain only the numeric code internally so writes can distinguish
            # rolled-back lock failures from auth/network/ambiguous-commit errors.
            error.mysql_code = exc.args[0] if exc.args and isinstance(exc.args[0], int) else None
            if hasattr(error, "add_note"):
                error.add_note(f"MySQL error code: {error.mysql_code}")
            raise error from None
        finally:
            if conn is not None:
                with suppress(pymysql.MySQLError):
                    conn.rollback()  # also cleans up a failed write transaction
                with suppress(pymysql.MySQLError):
                    conn.close()

    def ping(self):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            return cursor.fetchone()["ok"] == 1

    def guest(self, guest_id):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT * FROM guests WHERE guest_id = %s AND deleted_datetime IS NULL", (guest_id,))
            row = cursor.fetchone()
        if row is None:
            return None
        row.pop("deleted_datetime", None)
        if isinstance(row.get("preferences"), str):
            row["preferences"] = json.loads(row["preferences"])
        return wire(row)

    def host(self, host_id):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT * FROM hosts WHERE host_id = %s", (host_id,))
            row = cursor.fetchone()
        return wire(row) if row else None

    def amenities(self):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT amenity_name FROM amenities")
            return {r["amenity_name"] for r in cursor.fetchall()}

    def properties(self, where="1 = 1", args=()):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT p.* FROM properties p WHERE " + where + " ORDER BY p.property_id", args)
            rows = list(cursor.fetchall())
            if not rows:
                return []
            amenities = {r["property_id"]: [] for r in rows}
            ids = list(amenities)
            for start in range(0, len(ids), 200):
                batch = ids[start:start + 200]
                cursor.execute("SELECT property_id, amenity_name FROM property_amenities WHERE property_id IN ("
                               + ",".join(["%s"] * len(batch)) + ") ORDER BY property_id, amenity_name", batch)
                for item in cursor.fetchall():
                    amenities[item["property_id"]].append(item["amenity_name"])
            for row in rows:
                row["location"] = {key: row.pop(key) for key in LOCATION}
                row["amenities"] = amenities[row["property_id"]]
        return wire(rows)

    def property(self, property_id):
        rows = self.properties("p.property_id = %s", (property_id,))
        return rows[0] if rows else None

    def search(self, filters):
        where, args = criteria(filters)
        return self.properties(where, args)

    def pool(self, ids):
        if not ids:
            return []
        return self.properties("p.property_id IN (" + ",".join(["%s"] * len(ids)) + ")", list(ids))
