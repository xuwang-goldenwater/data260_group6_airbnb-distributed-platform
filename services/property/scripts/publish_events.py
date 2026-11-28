"""Run the property outbox relay separately from uvicorn. Ctrl+C stops it safely."""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "user"))
from listings_core.config import Settings
from listings_core.property_outbox import KafkaPublisher, relay_one
from listings_core.repository import MySQLRepository

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--once", action="store_true", help="Attempt one queued event and exit")
args = parser.parse_args()
cfg = Settings.from_env()
repo, publisher = MySQLRepository(cfg), KafkaPublisher(cfg)
try:
    while True:
        try:
            result = relay_one(repo, publisher)
        except Exception as exc:
            if args.once:
                raise SystemExit("Relay stopped: " + type(exc).__name__)
            print(json.dumps({"status": "retrying", "error_type": type(exc).__name__}), flush=True)
            time.sleep(5)
            continue
        if result["status"] not in {"empty", "busy"} or args.once:
            print(json.dumps(result), flush=True)
        if args.once:
            raise SystemExit(1 if result["status"] == "pending_retry" else 0)
        time.sleep(5 if result["status"] == "pending_retry" else 0.5)
except KeyboardInterrupt:
    print("Property outbox relay stopped.")
