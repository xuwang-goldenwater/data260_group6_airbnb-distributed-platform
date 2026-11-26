"""Filter-count baseline for the 50-request benchmark (assignment 7.3.1 item 4).

Ranks every active property by how many request filters and required
amenities it satisfies (the assignment's definition). Ties are broken by
whether the property matches the preferred type, then by property_id.
No model is involved. Input is the expected (ground-truth) parse of each
request, so the baseline is not affected by Parser errors.

Usage (from repo root):
    python3 benchmarks/baseline.py
Output:
    benchmarks/results/baseline.csv   (one row per request, top-10 ranking)
"""
import csv
import json
import time
from datetime import date, timedelta
from pathlib import Path

SEED_DIR = Path("data/seed/generated")
REQUESTS = Path("benchmarks/requests.json")
OUT = Path("benchmarks/results/baseline.csv")
SCORING_VERSION = "filter-count-v1"
TOP_K = 10


def load(path):
    return json.loads(Path(path).read_text())


def stay_nights(check_in, check_out):
    start, end = date.fromisoformat(check_in), date.fromisoformat(check_out)
    return {(start + timedelta(d)).isoformat() for d in range((end - start).days)}


def filter_count(p, q, blocked):
    """Number of satisfied filters and required amenities: one point each."""
    score = 0
    score += p["location"]["city"] == q["city"]
    score += p["capacity"] >= q["guests"]
    score += p["nightly_price"] <= q["max_nightly_price"]
    score += not (blocked.get(p["property_id"], set()) & stay_nights(q["check_in"], q["check_out"]))
    score += sum(a in p["amenities"] for a in q["required_amenities"])   # one point per amenity
    if q["min_rating"]:
        # new listings (0 reviews) are unknown, not low: same rule as the candidate filter
        score += p["review_count"] == 0 or p["rating"] >= q["min_rating"]
    return score


def rank(q, props, blocked):
    scored = [(filter_count(p, q, blocked),
               p["property_type"] == q["preferred_property_type"],
               p["property_id"]) for p in props]
    # more filters satisfied first; ties: preferred type first, then property_id
    scored.sort(key=lambda x: (-x[0], not x[1], x[2]))
    return [(s, pid) for s, _, pid in scored[:TOP_K]]


def main():
    bench = load(REQUESTS)
    props = [p for p in load(SEED_DIR / "properties.json") if p["status"] == "active"]
    blocked = {}
    for b in load(SEED_DIR / "availability_blocks.json"):
        blocked.setdefault(b["property_id"], set()).add(b["night_date"])

    rows = []
    for req in bench["requests"]:
        t0 = time.perf_counter()
        top = rank(req["expected_parse"], props, blocked)
        rows.append({
            "request_id": req["request_id"],
            "config": "baseline",
            "trace_id": "",
            "scoring_version": SCORING_VERSION,
            "final_ranking": ";".join(pid for _, pid in top),
            "scores": ";".join(str(s) for s, _ in top),
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {OUT}: {len(rows)} requests, top-{TOP_K} each")


if __name__ == "__main__":
    main()
