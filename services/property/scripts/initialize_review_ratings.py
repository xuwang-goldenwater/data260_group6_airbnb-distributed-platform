"""Validate the committed snapshot; --apply records its existing rating contributions."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user"), str(ROOT / "scripts")]
from import_user_property_seed import load_snapshot
from listings_core.config import Settings
from listings_core.repository import MySQLRepository
from listings_core.review_ratings import initialize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    cfg = Settings.from_env()
    manifest, data = load_snapshot(cfg.seed_dir)
    status = initialize(MySQLRepository(cfg), manifest, data) if args.apply else "DRY_RUN_PASS"
    print(json.dumps({"status": status, "dataset_version": manifest["dataset_version"],
                      "seed_properties": len(data["properties"]), "seed_review_receipts": len(data["reviews"]),
                      "database_written": args.apply and status == "initialized"}, indent=2))


if __name__ == "__main__":
    main()
