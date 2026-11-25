"""Run: python3 -m unittest discover -s tests -p 'test_seed_data.py' -v"""
import copy
from datetime import date, datetime
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("generate_seed", Path(__file__).resolve().parents[1]/"scripts/generate_seed.py")
seed = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(seed)


class SeedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.anchor = date(2026, 11, 24)
        cls.data = seed.build(260, cls.anchor)

    def test_complete_dataset(self):
        counts = seed.validate(self.data, self.anchor)
        self.assertEqual(counts["properties"], 200)

    def test_reproducible(self):
        self.assertEqual(seed.encoded(self.data), seed.encoded(seed.build(260, self.anchor)))

    def test_other_seed_and_anchor(self):
        anchor = date(2027, 1, 2)
        seed.validate(seed.build(999, anchor), anchor)

    def test_duplicate_night_rejected(self):
        data = copy.deepcopy(self.data)
        data["availability_blocks"].append(dict(data["availability_blocks"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate held"):
            seed.validate(data, self.anchor)

    def test_orphan_reservation_rejected(self):
        data = copy.deepcopy(self.data)
        data["reservations"][0]["guest_id"] = "missing"
        with self.assertRaisesRegex(ValueError, "Orphan reservation"):
            seed.validate(data, self.anchor)

    def test_wrong_price_rejected(self):
        data = copy.deepcopy(self.data)
        data["reservations"][0]["total_price"] += 1
        with self.assertRaisesRegex(ValueError, "Incorrect total"):
            seed.validate(data, self.anchor)

    def test_rating_mismatch_rejected(self):
        data = copy.deepcopy(self.data)
        data["properties"][0]["rating"] = 1.0
        with self.assertRaisesRegex(ValueError, "aggregate rating"):
            seed.validate(data, self.anchor)

    def test_review_must_follow_completed_stay(self):
        data = copy.deepcopy(self.data)
        future = next(r for r in data["reservations"] if r["status"] == "requested")
        data["reviews"][0]["reservation_id"] = future["reservation_id"]
        with self.assertRaisesRegex(ValueError, "non-completed"):
            seed.validate(data, self.anchor)

    def test_broken_history_rejected(self):
        data = copy.deepcopy(self.data)
        data["reservation_status_history"][0]["from_status"] = "confirmed"
        with self.assertRaisesRegex(ValueError, "history chain"):
            seed.validate(data, self.anchor)

    def test_message_participant_rejected(self):
        data = copy.deepcopy(self.data)
        data["messages"][0]["sender_id"] = "guest-unknown"
        with self.assertRaisesRegex(ValueError, "participants"):
            seed.validate(data, self.anchor)

    def test_named_candidates_are_available(self):
        start = self.anchor.isoformat()
        for pid in ["property-2001", "property-2007", "property-2044"]:
            self.assertFalse(any(b["property_id"] == pid and b["night_date"] >= start
                                 for b in self.data["availability_blocks"]))

    def test_both_cancellation_actors_have_matching_history(self):
        cancellations = [r for r in self.data["reservations"] if r["status"] == "cancelled"]
        self.assertEqual(sum(r["cancelled_by"] == "host" for r in cancellations), 16)
        self.assertEqual(sum(r["cancelled_by"] == "guest" for r in cancellations), 16)
        for r in cancellations:
            row = next(h for h in self.data["reservation_status_history"]
                       if h["reservation_id"] == r["reservation_id"] and h["to_status"] == "cancelled")
            self.assertEqual(row["reason"], r["cancelled_by"].upper()+"_CANCELLED")
            self.assertEqual(row["actor_id"], r[r["cancelled_by"]+"_id"])

    def test_decision_variation_and_profile_means(self):
        pending, by_host = {}, {}
        reservations = {r["reservation_id"]: r for r in self.data["reservations"]}
        for h in self.data["reservation_status_history"]:
            rid = h["reservation_id"]
            when = datetime.fromisoformat(h["changed_datetime"].replace("Z", "+00:00"))
            if h["to_status"] == "pending_host":
                pending[rid] = when
            elif h["from_status"] == "pending_host" and h["to_status"] in {"confirmed", "declined"}:
                minutes = (when-pending[rid]).total_seconds()/60
                by_host.setdefault(reservations[rid]["host_id"], []).append(minutes)
        durations = [x for values in by_host.values() for x in values]
        self.assertEqual(min(durations), 10)
        self.assertEqual(max(durations), 1440)
        self.assertGreater(len(set(durations)), 100)
        for host in self.data["hosts"]:
            values = by_host.get(host["host_id"], [])
            self.assertEqual(host["response_time"], round(sum(values)/len(values)) if values else None)

    def test_bad_cancellation_reason_rejected(self):
        data = copy.deepcopy(self.data)
        row = next(r for r in data["reservations"] if r["cancelled_by"] == "host")
        row["status_reason"] = "GUEST_CANCELLED"
        with self.assertRaisesRegex(ValueError, "Cancellation reason mismatch"):
            seed.validate(data, self.anchor)

    def test_bad_host_response_aggregate_rejected(self):
        data = copy.deepcopy(self.data)
        data["hosts"][0]["response_time"] += 1
        with self.assertRaisesRegex(ValueError, "Host response aggregate"):
            seed.validate(data, self.anchor)

    def test_cli_roundtrip_checksums_and_overwrite_guard(self):
        script = str(Path(seed.__file__))
        with tempfile.TemporaryDirectory() as folder:
            command = [sys.executable, script, "--output", folder]
            subprocess.run(command, check=True, capture_output=True)
            subprocess.run(command+["--validate-only"], check=True, capture_output=True)
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            subprocess.run(command+["--overwrite"], check=True, capture_output=True)
            path = Path(folder)/"guests.json"
            path.write_text(path.read_text()+" ")
            bad = subprocess.run(command+["--validate-only"], capture_output=True, text=True)
            self.assertNotEqual(bad.returncode, 0)
            self.assertIn("Checksum mismatch", bad.stderr)


if __name__ == "__main__":
    unittest.main()
