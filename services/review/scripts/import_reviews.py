"""Load the seed reviews (data/seed/generated/reviews.json) into MongoDB.

Re-tags every review with tagger-v1 and stops if a tag differs from the seed,
so the Risk agent, the Review service and the dashboards use the same tags.
Idempotent: upsert by review_id, so running it twice changes nothing.

Usage (from the repo root):
    python3 services/review/scripts/import_reviews.py --dry-run   # validate only, no database
    python3 services/review/scripts/import_reviews.py             # MONGO_URI / MONGO_DB from env
"""
import argparse
import json
import os
import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVICE_DIR))
from review_core.tagger import TAGGER_VERSION, tag  # noqa: E402

SEED = SERVICE_DIR.parents[1] / "data" / "seed" / "generated" / "reviews.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    reviews = json.loads(SEED.read_text())
    mismatch = [(r["review_id"], r["tags"], tag(r["review_text"]))
                for r in reviews if sorted(r["tags"]) != tag(r["review_text"])]
    if mismatch:
        sys.exit(f"{len(mismatch)} seed reviews disagree with {TAGGER_VERSION}, e.g. {mismatch[:3]}")
    print(f"{len(reviews)} seed reviews, tags match {TAGGER_VERSION}")
    if args.dry_run:
        return

    from pymongo import MongoClient, UpdateOne
    sys.path.insert(0, str(SERVICE_DIR))
    from review_core.store import MongoReviewStore

    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
                     serverSelectionTimeoutMS=3000)[os.environ.get("MONGO_DB", "airbnb")]
    MongoReviewStore(db).ensure_indexes()
    ops = [UpdateOne({"review_id": r["review_id"]},
                     {"$set": dict(r, tags=tag(r["review_text"]), tagger_version=TAGGER_VERSION)},
                     upsert=True) for r in reviews]
    res = db["reviews"].bulk_write(ops, ordered=False)
    print(f"upserted {res.upserted_count}, updated {res.modified_count}, "
          f"total in collection {db['reviews'].count_documents({})}")


if __name__ == "__main__":
    main()
