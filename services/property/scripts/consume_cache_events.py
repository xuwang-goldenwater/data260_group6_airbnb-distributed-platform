"""Separate cache invalidator. Failures never advance a later source offset."""
import argparse
import logging
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user")]
from listings_core.config import Settings
from listings_core.search_cache import make_client, prefix
from listings_core.cache_invalidation import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invalidate-now", action="store_true", help="Rotate only this service's cache namespace and exit")
    args = parser.parse_args()
    cfg = Settings.from_env()
    if args.invalidate_now:
        make_client(cfg).set(prefix(cfg) + "generation", uuid.uuid4().hex)
        print("Property search namespace invalidated; no other Redis data touched.")
    else:
        logging.basicConfig(level=logging.INFO)
        try: run(cfg)
        except KeyboardInterrupt: print("Property cache invalidator stopped.")
