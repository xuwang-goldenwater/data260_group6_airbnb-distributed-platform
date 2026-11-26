#!/usr/bin/env python3
"""Validate and import Role A's existing snapshot using Docker MySQL; stdlib only."""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import generate_seed

ROOT = Path(__file__).resolve().parents[1]
TABLE_COLUMNS = {
    "guests": ("guest_id", "first_name", "last_name", "email", "phone", "location",
               "profile_photo_url", "preferences", "saved_properties_count", "booking_count",
               "profile_created_datetime", "updated_datetime", "deleted_datetime"),
    "hosts": ("host_id", "name", "email", "phone", "profile_photo_url", "verification_status",
              "response_rate", "response_time", "host_since", "access_level", "updated_datetime"),
    "properties": ("property_id", "host_id", "title", "description", "property_type",
                   "address_line_1", "city", "state", "postal_code", "country", "latitude", "longitude",
                   "capacity", "bedrooms", "bathrooms", "nightly_price", "cleaning_fee",
                   "availability", "status", "rating", "review_count", "version",
                   "created_datetime", "updated_datetime"),
    "amenities": ("amenity_name",),
    "property_amenities": ("property_id", "amenity_name"),
    "saved_properties": ("guest_id", "property_id", "saved_datetime"),
}
DATETIMES = {"profile_created_datetime", "updated_datetime", "created_datetime",
             "saved_datetime", "deleted_datetime"}


def load_snapshot(directory):
    """Read and hash the same bytes, then reuse the full cross-file validator."""
    manifest = json.loads((directory / "manifest.json").read_bytes())
    names = set(manifest["counts"])
    required = {"guests", "hosts", "properties", "saved_properties", "reservations",
                "availability_blocks", "reservation_status_history", "reviews", "conversations", "messages"}
    if names != required or set(manifest["sha256"]) != {n + ".json" for n in names}:
        raise ValueError("Manifest must describe exactly the full known seed snapshot")
    data = {}
    for name in sorted(names):
        raw = (directory / (name + ".json")).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest["sha256"][name + ".json"]:
            raise ValueError("Checksum mismatch: " + name + ".json")
        data[name] = json.loads(raw)
    counts = generate_seed.validate(data, date.fromisoformat(manifest["anchor_date"]))
    if counts != manifest["counts"]:
        raise ValueError("Manifest counts differ")
    return manifest, data


def mysql_datetime(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Snapshot timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def prepare_tables(data):
    """Map API-shaped data into exactly the six tables in PR #13."""
    rows = {table: [] for table in TABLE_COLUMNS}
    for table in ("guests", "hosts", "properties", "saved_properties"):
        for source in data[table]:
            record = dict(source)
            if table == "guests":
                record["deleted_datetime"] = None
            if table == "properties":
                record.update(record.pop("location"))
                amenities = record.pop("amenities")
                if len(amenities) != len(set(amenities)):
                    raise ValueError("Duplicate property amenity")
                for amenity in amenities:
                    if not amenity or amenity != amenity.strip().lower() or not amenity.isascii():
                        raise ValueError("Amenity must be normalized nonempty ASCII")
                    rows["property_amenities"].append((record["property_id"], amenity))
            values = []
            for column in TABLE_COLUMNS[table]:
                value = record[column]
                if column in DATETIMES and value is not None:
                    value = mysql_datetime(value)
                if column == "preferences" and value is not None and not isinstance(value, dict):
                    raise ValueError("Guest preferences must be a JSON object")
                values.append(value)
            rows[table].append(tuple(values))
    rows["amenities"] = [(a,) for a in sorted({a for _, a in rows["property_amenities"]})]
    return rows


def literal(value):
    """UTF-8 hex literals avoid dependence on SQL quote/backslash escape modes."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError("Non-finite number")
        return format(number, "f")
    if isinstance(value, dict):
        value = json.dumps(value, ensure_ascii=False, allow_nan=False)
    if not isinstance(value, str):
        raise ValueError("Unsupported SQL value type: " + type(value).__name__)
    return "CONVERT(X'" + value.encode("utf-8").hex() + "' USING utf8mb4)"


def insert_sql(table, rows):
    columns = ", ".join("`" + c + "`" for c in TABLE_COLUMNS[table])
    for start in range(0, len(rows), 100):
        values = ["(" + ", ".join(literal(v) for v in row) + ")" for row in rows[start:start + 100]]
        yield "INSERT INTO `" + table + "` (" + columns + ") VALUES\n" + ",\n".join(values) + ";"


def import_sql(tables):
    lines = ["SET NAMES utf8mb4;", "SET time_zone = '+00:00';",
             "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';",
             "CREATE TEMPORARY TABLE _role_a_seed_guard (label VARCHAR(100) PRIMARY KEY, ok INT NOT NULL CHECK (ok = 1));",
             "START TRANSACTION;"]
    empty = " AND ".join("(SELECT COUNT(*) FROM `" + t + "`) = 0" for t in tables)
    lines.append("INSERT INTO _role_a_seed_guard VALUES ('target_tables_must_be_empty', " + empty + ");")
    for table, rows in tables.items():
        lines.extend(insert_sql(table, rows))
        lines.append("INSERT INTO _role_a_seed_guard VALUES (" + literal(table + "_count")
                     + ", (SELECT COUNT(*) FROM `" + table + "`) = " + str(len(rows)) + ");")
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def readback_sql():
    lines = ["SET time_zone = '+00:00';", "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
             "START TRANSACTION WITH CONSISTENT SNAPSHOT;"]
    for table, columns in TABLE_COLUMNS.items():
        expressions = ["DATE_FORMAT(`" + c + "`, '%Y-%m-%d %H:%i:%s.%f')" if c in DATETIMES
                       else "DATE_FORMAT(`host_since`, '%Y-%m-%d')" if c == "host_since"
                       else "`" + c + "`" for c in columns]
        lines.append("SELECT JSON_OBJECT('table', '" + table + "', 'row', JSON_ARRAY("
                     + ", ".join(expressions) + ")) FROM `" + table + "`;")
    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def mysql_run(sql, compose):
    # Credentials stay in the existing container environment; no new config or pip deps.
    shell = ('export MYSQL_PWD="$MYSQL_PASSWORD"; '
             'exec mysql --user="$MYSQL_USER" --database="$MYSQL_DATABASE" '
             '--default-character-set=utf8mb4 --batch --raw --skip-column-names --skip-reconnect')
    result = subprocess.run(["docker", "compose", "-f", str(compose), "exec", "-T", "mysql",
                             "sh", "-c", shell], input=sql, text=True, capture_output=True, cwd=ROOT)
    if result.returncode:
        raise RuntimeError("MySQL command failed:\n" + result.stderr.strip())
    return result.stdout


def normalized(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in value.items()}
    return value


def verify_readback(output, expected):
    """Compare every stored field, not just counts or a few scenario rows."""
    actual = {table: [] for table in TABLE_COLUMNS}
    for line in output.splitlines():
        record = json.loads(line, parse_float=Decimal)
        actual[record["table"]].append(record["row"])
    for table in expected:
        # First key columns uniquely identify rows; full list sorting also detects extras/duplicates.
        key = (lambda row: (row[0], row[1])) if table in {"property_amenities", "saved_properties"} else (lambda row: row[0])
        if sorted(normalized(actual[table]), key=key) != sorted(normalized(expected[table]), key=key):
            raise ValueError("Database differs from snapshot in " + table)
    return {table: len(rows) for table, rows in actual.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-dir", type=Path, default=ROOT / "data/seed/generated")
    parser.add_argument("--compose-file", type=Path, default=ROOT / "infra/docker-compose.yml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Import into EMPTY Role A tables, then verify every field")
    mode.add_argument("--verify-only", action="store_true", help="Read and compare existing DB contents; no inserts")
    args = parser.parse_args(argv)
    committed = False
    try:
        manifest, data = load_snapshot(args.seed_dir)
        tables = prepare_tables(data)
        if args.apply:
            mysql_run(import_sql(tables), args.compose_file)
            committed = True
        if args.apply or args.verify_only:
            counts = verify_readback(mysql_run(readback_sql(), args.compose_file), tables)
            status = "IMPORTED_AND_VERIFIED" if args.apply else "DATABASE_VERIFIED"
        else:
            # Build SQL too, so a dry run checks mapping/encoding before touching Docker.
            import_sql(tables)
            counts = {table: len(rows) for table, rows in tables.items()}
            status = "DRY_RUN_PASS"
        print(json.dumps({"status": status, "dataset_version": manifest["dataset_version"],
                          "counts": counts, "database_written": committed}, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
        print("ERROR: " + str(error), file=sys.stderr)
        if committed:
            print("Import committed, but readback verification failed. Do not reapply; investigate with --verify-only.", file=sys.stderr)
        elif args.apply:
            print("No replacement/reset is performed. A MySQL error before COMMIT rolls back this import. "
                  "If the connection failed during COMMIT, inspect the database with --verify-only before retrying.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
