"""Build the blank labeling sheet for the 50-request benchmark.

One row per (request, candidate property). Candidates are shuffled with a fixed
seed so neither labeler sees them sorted by price or rating.

Usage (from repo root):
    python3 benchmarks/make_label_sheet.py
Output:
    benchmarks/labeling/label_sheet_blank.csv
Each labeler copies it to labels_<name>.csv and fills `rank` (1-10) for the
10 most suitable properties of every request. Leave other rows empty.
"""
import csv
import json
import random
from collections import Counter, defaultdict
from pathlib import Path

SEED_DIR = Path("data/seed/generated")
REQUESTS = Path("benchmarks/requests.json")
OUT = Path("benchmarks/labeling/label_sheet_blank.csv")
COMPLAINTS = ["cleanliness", "cancellation", "missing_amenity", "noise",
              "host_unresponsive", "price_mismatch"]

COLUMNS = ["request_id", "request_text", "property_id", "property_type", "nightly_price",
           "capacity", "bedrooms", "amenities", "rating", "review_count",
           "complaint_tags", "rank", "note"]


def load(path):
    return json.loads(Path(path).read_text())


def complaint_summary(reviews):
    """property_id -> 'cleanliness:2; cancellation:1' (empty if no complaints)."""
    counts = defaultdict(Counter)
    for r in reviews:
        for t in r.get("tags", []):
            if t in COMPLAINTS:
                counts[r["property_id"]][t] += 1
    return {pid: "; ".join(f"{t}:{n}" for t, n in sorted(c.items())) for pid, c in counts.items()}


def main():
    bench = load(REQUESTS)
    props = {p["property_id"]: p for p in load(SEED_DIR / "properties.json")}
    complaints = complaint_summary(load(SEED_DIR / "reviews.json"))
    rng = random.Random(bench["generator_seed"])

    rows = []
    for req in bench["requests"]:
        cands = list(req["candidate_property_ids"])
        rng.shuffle(cands)                      # random order, not sorted by any attribute
        for i, pid in enumerate(cands):
            p = props[pid]
            rows.append({
                "request_id": req["request_id"],
                "request_text": req["request_text"] if i == 0 else "",
                "property_id": pid,
                "property_type": p["property_type"],
                "nightly_price": p["nightly_price"],
                "capacity": p["capacity"],
                "bedrooms": p["bedrooms"],
                "amenities": ", ".join(p["amenities"]),
                "rating": p["rating"] if p["review_count"] else "new (no reviews)",
                "review_count": p["review_count"],
                "complaint_tags": complaints.get(pid, ""),
                "rank": "",
                "note": "",
            })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {OUT}: {len(rows)} rows for {len(bench['requests'])} requests")


if __name__ == "__main__":
    main()
