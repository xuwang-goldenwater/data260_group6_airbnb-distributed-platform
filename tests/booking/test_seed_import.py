"""Schema and seed-import checks (task 2.2). Needs the isolated MySQL test database (`make booking-test-db`)."""
from __future__ import annotations

import json

import import_seed

from conftest import ROOT, TABLES


def test_schema_can_be_applied_twice(db):
    ddl = (ROOT / "db/mysql/02_bookings.sql").read_text()
    statements = [s.strip() for s in "\n".join(l for l in ddl.splitlines() if not l.strip().startswith("--")).split(";")]
    with db.transaction() as conn, conn.cursor() as cur:
        for stmt in statements:
            if stmt:
                cur.execute(stmt)
        cur.execute("SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = DATABASE() "
                    "AND table_name IN (" + ", ".join(["%s"] * len(TABLES)) + ")", TABLES)
        assert cur.fetchone()["n"] == len(TABLES)


def test_cross_service_ids_match_role_a_column_type(db):
    """property_id / guest_id / host_id (and actor_id) use the same VARCHAR(64) ascii_bin type as db/mysql/01_users_properties.sql."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name AS tbl, column_name AS col, character_maximum_length AS n, character_set_name AS cs, "
                    "collation_name AS coll FROM information_schema.columns WHERE table_schema = DATABASE() "
                    "AND column_name IN ('property_id', 'guest_id', 'host_id', 'actor_id')")
        rows = cur.fetchall()
    assert {(r["tbl"], r["col"]) for r in rows} >= {
        ("reservations", "property_id"), ("reservations", "guest_id"), ("reservations", "host_id"),
        ("availability_blocks", "property_id"), ("reservation_status_history", "actor_id"),
        ("idempotency_keys", "actor_id")}
    assert all((r["n"], r["cs"], r["coll"]) == (64, "ascii", "ascii_bin") for r in rows), rows


def test_seed_import_is_idempotent_and_matches_manifest(settings, clean_db):
    manifest, data = import_seed.load(settings.seed_dir)
    first = import_seed.import_rows(clean_db, data)
    present = import_seed.verify(clean_db, data)
    assert {k: present[k] for k in import_seed.FILES} == {k: manifest["counts"][k] for k in import_seed.FILES}
    assert first == {k: manifest["counts"][k] for k in import_seed.FILES}
    second = import_seed.import_rows(clean_db, data)          # INSERT IGNORE: nothing new the second time
    assert second == {k: 0 for k in import_seed.FILES}
    assert import_seed.verify(clean_db, data) == present


def test_manifest_checksums_are_verified(tmp_path, settings):
    seed = tmp_path / "seed"
    seed.mkdir()
    for name in ("manifest", *import_seed.FILES):
        (seed / f"{name}.json").write_bytes((settings.seed_dir / f"{name}.json").read_bytes())
    tampered = json.loads((seed / "reservations.json").read_text())
    tampered[0]["guest_count"] += 1
    (seed / "reservations.json").write_text(json.dumps(tampered))
    try:
        import_seed.load(seed)
    except SystemExit as exc:
        assert "checksum mismatch" in str(exc)
    else:
        raise AssertionError("tampered seed file was accepted")
