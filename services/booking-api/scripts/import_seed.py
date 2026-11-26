"""Import the shared seed (data/seed/generated) into the Booking tables.

- Loads reservations.json, reservation_status_history.json, availability_blocks.json.
- Converts '...Z' timestamps to naive-UTC DATETIME(3).
- Idempotent: INSERT IGNORE on primary keys, so running it twice changes nothing.
- Verifies file checksums and that every seed row is present against manifest.json.

Usage: python services/booking-api/scripts/import_seed.py [--seed-dir DIR]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import _bootstrap  # noqa: F401
from booking_core.config import Settings
from booking_core.db import Database
from booking_core.timeutil import parse_utc

FILES = ("reservations", "reservation_status_history", "availability_blocks")
BATCH = 500


def _chunks(seq, n=BATCH):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _ts(value):
    return parse_utc(value) if value else None


def load(seed_dir: Path) -> tuple[dict, dict[str, list]]:
    manifest = json.loads((seed_dir / "manifest.json").read_text())
    data = {}
    for name in FILES:
        raw = (seed_dir / f"{name}.json").read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        expected = manifest["sha256"][f"{name}.json"]
        if digest != expected:
            sys.exit(f"checksum mismatch for {name}.json: {digest} != manifest {expected}")
        data[name] = json.loads(raw)
        if len(data[name]) != manifest["counts"][name]:
            sys.exit(f"{name}.json has {len(data[name])} records, manifest says {manifest['counts'][name]}")
    return manifest, data


def import_rows(db: Database, data: dict[str, list]) -> dict[str, int]:
    inserted = {}
    res_rows = [(
        r["reservation_id"], r["property_id"], r["guest_id"], r["host_id"], r["check_in"], r["check_out"],
        r["nights"], r["guest_count"], r["nightly_price"], r["cleaning_fee"], r["total_price"], r["status"],
        r.get("status_reason"), r.get("message_to_host"), r.get("cancelled_by"), r["trace_id"],
        bool(r.get("publish_pending", False)), r["version"], _ts(r["created_datetime"]), _ts(r["updated_datetime"]),
    ) for r in data["reservations"]]
    hist_rows = [(
        h["id"], h["reservation_id"], h.get("from_status"), h["to_status"], h["actor_id"], h.get("reason"),
        h["trace_id"], _ts(h["changed_datetime"]),
    ) for h in data["reservation_status_history"]]
    block_rows = [(b["property_id"], b["night_date"], b["reservation_id"], _ts(b["created_datetime"]))
                  for b in data["availability_blocks"]]
    with db.transaction() as conn, conn.cursor() as cur:
        n = 0
        for chunk in _chunks(res_rows):
            n += cur.executemany(
                "INSERT IGNORE INTO reservations (reservation_id, property_id, guest_id, host_id, check_in, check_out, "
                "nights, guest_count, nightly_price, cleaning_fee, total_price, status, status_reason, message_to_host, "
                "cancelled_by, trace_id, publish_pending, version, created_datetime, updated_datetime) VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", chunk)
        inserted["reservations"] = n
        n = 0
        for chunk in _chunks(hist_rows):
            n += cur.executemany(
                "INSERT IGNORE INTO reservation_status_history (id, reservation_id, from_status, to_status, actor_id, "
                "reason, trace_id, changed_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", chunk)
        inserted["reservation_status_history"] = n
        n = 0
        for chunk in _chunks(block_rows):
            n += cur.executemany(
                "INSERT IGNORE INTO availability_blocks (property_id, night_date, reservation_id, created_datetime) "
                "VALUES (%s, %s, %s, %s)", chunk)
        inserted["availability_blocks"] = n
    return inserted


def verify(db: Database, data: dict[str, list]) -> dict[str, int]:
    present = {name: 0 for name in FILES}
    with db.transaction() as conn, conn.cursor() as cur:
        for chunk in _chunks([r["reservation_id"] for r in data["reservations"]]):
            cur.execute("SELECT COUNT(*) AS n FROM reservations WHERE reservation_id IN ("
                        + ", ".join(["%s"] * len(chunk)) + ")", chunk)
            present["reservations"] += cur.fetchone()["n"]
        for chunk in _chunks([h["id"] for h in data["reservation_status_history"]]):
            cur.execute("SELECT COUNT(*) AS n FROM reservation_status_history WHERE id IN ("
                        + ", ".join(["%s"] * len(chunk)) + ")", chunk)
            present["reservation_status_history"] += cur.fetchone()["n"]
        for chunk in _chunks(data["availability_blocks"]):
            cur.execute("SELECT COUNT(*) AS n FROM availability_blocks WHERE (property_id, night_date, reservation_id) "
                        "IN (" + ", ".join(["(%s, %s, %s)"] * len(chunk)) + ")",
                        [v for b in chunk for v in (b["property_id"], b["night_date"], b["reservation_id"])])
            present["availability_blocks"] += cur.fetchone()["n"]
    return present


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seed-dir", type=Path, default=None)
    args = ap.parse_args()
    settings = Settings.from_env()
    seed_dir = args.seed_dir or settings.seed_dir
    manifest, data = load(seed_dir)
    db = Database(settings)
    inserted = import_rows(db, data)
    present = verify(db, data)
    print(f"dataset_version={manifest['dataset_version']} mysql={settings.mysql_host}:{settings.mysql_port}/"
          f"{settings.mysql_database}")
    print(f"{'table':32} {'manifest':>9} {'inserted_now':>13} {'present':>8}  result")
    ok = True
    for name in FILES:
        expected = manifest["counts"][name]
        good = present[name] == expected
        ok &= good
        print(f"{name:32} {expected:>9} {inserted[name]:>13} {present[name]:>8}  {'OK' if good else 'MISMATCH'}")
    if not ok:
        print("Seed rows are missing or were changed by a demo; run `make booking-reset` for a clean dataset.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
