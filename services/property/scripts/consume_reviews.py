"""Run Property's review projection worker separately from the HTTP services."""
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user"), str(ROOT / "scripts")]
from import_user_property_seed import load_snapshot
from listings_core.config import Settings
from listings_core.repository import MySQLRepository
from listings_core.review_ratings import require_initialized
from listings_core.rating_worker import run


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = Settings.from_env()
    manifest, _ = load_snapshot(cfg.seed_dir)
    repo = MySQLRepository(cfg)
    require_initialized(repo, manifest)
    try:
        run(cfg, repo)
    except KeyboardInterrupt:
        print("Property rating worker stopped.")
