"""Stdlib tests; set AIRBNB_MYSQL_TEST=1 for local Docker MySQL checks after import."""
import copy
from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import generate_seed as seed
import import_user_property_seed as importer


class ImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = seed.build(260, date(2026, 11, 24))
        cls.tables = importer.prepare_tables(cls.data)

    def write_snapshot(self, directory):
        counts = seed.validate(self.data, date(2026, 11, 24))
        digests = {}
        for name, rows in self.data.items():
            raw = seed.encoded(rows)
            (directory / (name + ".json")).write_bytes(raw)
            digests[name + ".json"] = hashlib.sha256(raw).hexdigest()
        manifest = dict(anchor_date="2026-11-24", counts=counts, sha256=digests,
                        dataset_version="test-snapshot")
        (directory / "manifest.json").write_text(json.dumps(manifest))

    def readback(self):
        return "\n".join(json.dumps({"table": table, "row": row})
                         for table, rows in self.tables.items() for row in reversed(rows))

    def test_real_snapshot_and_nested_mapping(self):
        _, data = importer.load_snapshot(ROOT / "data/seed/generated")
        tables = importer.prepare_tables(data)
        self.assertEqual({t: len(tables[t]) for t in ("guests", "hosts", "properties", "saved_properties")},
                         {"guests": 120, "hosts": 60, "properties": 200, "saved_properties": 360})
        props = {row[0]: dict(zip(importer.TABLE_COLUMNS["properties"], row)) for row in tables["properties"]}
        self.assertEqual(props["property-2007"]["rating"], 4.3)
        self.assertEqual(props["property-2045"]["review_count"], 0)
        self.assertEqual(props["property-2001"]["city"], "San Jose")
        self.assertIn(("property-2001", "parking"), tables["property_amenities"])
        self.assertIsNone(tables["guests"][0][-1])

    def test_tampered_file_rejected_before_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            self.write_snapshot(folder)
            with (folder / "guests.json").open("ab") as file:
                file.write(b" ")
            with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
                importer.load_snapshot(folder)

    def test_wrong_manifest_count_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            self.write_snapshot(folder)
            path = folder / "manifest.json"
            manifest = json.loads(path.read_text())
            manifest["counts"]["guests"] += 1
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "Manifest counts"):
                importer.load_snapshot(folder)

    def test_incomplete_manifest_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            self.write_snapshot(folder)
            path = folder / "manifest.json"
            manifest = json.loads(path.read_text())
            del manifest["sha256"]["guests.json"]
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "full known seed"):
                importer.load_snapshot(folder)

    def test_unsafe_text_is_hex_encoded(self):
        text = "O'Reilly \\ 雪\n'); DROP TABLE guests; --"
        value = importer.literal(text)
        self.assertNotIn("DROP", value)
        self.assertEqual(bytes.fromhex(value.split("'")[1]).decode("utf-8"), text)
        with self.assertRaises(ValueError):
            importer.literal(float("nan"))
        self.assertEqual(importer.literal(Decimal("4.3000")), "4.3000")

    def test_timestamp_offset_is_converted_and_naive_rejected(self):
        self.assertEqual(importer.mysql_datetime("2026-11-24T04:00:00-08:00"),
                         "2026-11-24 12:00:00.000000")
        with self.assertRaises(ValueError):
            importer.mysql_datetime("2026-11-24T12:00:00")

    def test_readback_compares_values_and_ignores_row_order(self):
        counts = importer.verify_readback(self.readback(), self.tables)
        self.assertEqual(counts["properties"], 200)
        altered = self.readback().replace('"Demo1"', '"Changed"', 1)
        with self.assertRaisesRegex(ValueError, "guests"):
            importer.verify_readback(altered, self.tables)

    def test_readback_rejects_missing_or_extra_rows(self):
        lines = self.readback().splitlines()
        with self.assertRaises(ValueError):
            importer.verify_readback("\n".join(lines[1:]), self.tables)
        with self.assertRaises(ValueError):
            importer.verify_readback("\n".join(lines + [lines[0]]), self.tables)

    def test_duplicate_amenity_rejected(self):
        data = copy.deepcopy(self.data)
        data["properties"][0]["amenities"].append(data["properties"][0]["amenities"][0])
        with self.assertRaisesRegex(ValueError, "Duplicate property amenity"):
            importer.prepare_tables(data)

    def test_cli_dry_run_needs_no_docker(self):
        result = subprocess.run([sys.executable, str(ROOT / "scripts/import_user_property_seed.py")],
                                cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output["status"], "DRY_RUN_PASS")
        self.assertIs(output["database_written"], False)


@unittest.skipUnless(os.environ.get("AIRBNB_MYSQL_TEST") == "1", "Set AIRBNB_MYSQL_TEST=1 after local import")
class MySQLIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _, data = importer.load_snapshot(ROOT / "data/seed/generated")
        cls.tables = importer.prepare_tables(data)
        cls.compose = ROOT / "infra/docker-compose.yml"

    def assert_snapshot_unchanged(self):
        importer.verify_readback(importer.mysql_run(importer.readback_sql(), self.compose), self.tables)

    def test_full_database_matches_snapshot(self):
        self.assert_snapshot_unchanged()

    def test_nonempty_database_refused_without_changes(self):
        with self.assertRaisesRegex(RuntimeError, "MySQL command failed"):
            importer.mysql_run(importer.import_sql(self.tables), self.compose)
        self.assert_snapshot_unchanged()

    def test_invalid_price_and_capacity_roll_back(self):
        for assignment in ("nightly_price = 0", "capacity = 31"):
            with self.subTest(assignment=assignment):
                with self.assertRaisesRegex(RuntimeError, "Check constraint"):
                    importer.mysql_run("START TRANSACTION; UPDATE properties SET " + assignment
                                       + " WHERE property_id = 'property-2001'; COMMIT;", self.compose)
                self.assert_snapshot_unchanged()

    def test_duplicate_save_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Duplicate entry"):
            importer.mysql_run("START TRANSACTION; INSERT INTO saved_properties "
                               "SELECT * FROM saved_properties LIMIT 1; COMMIT;", self.compose)
        self.assert_snapshot_unchanged()

    def test_orphan_property_amenity_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "foreign key constraint fails"):
            importer.mysql_run("START TRANSACTION; INSERT INTO property_amenities "
                               "VALUES ('property-not-present', 'wifi'); COMMIT;", self.compose)
        self.assert_snapshot_unchanged()


if __name__ == "__main__":
    unittest.main()
