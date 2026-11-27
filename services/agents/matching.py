"""Matching agent. Versions: matcher-v1 (default), matcher-v2 (rejected), matcher-v3.

Deterministic: the same parsed request and data always give the same ranking.
Hard filters pick the candidates; a weighted score ranks them.

Factors (each 0-1) and weights:
  availability 0.25  all requested nights free (also a hard gate)
  rating       0.25  (rating - 3) / 2, clipped; dropped for 0-review listings
  budget       0.20  (budget - price) / budget, cheaper is better
  amenities    0.15  half for parking, half for share of kitchen/pool/washer
  type         0.10  1 if the preferred property type matches
  location     0.05  1 if the city matches (all candidates share the city)
If a listing has no reviews its rating is unknown, so the rating factor is
removed and the other weights are scaled up to sum to 1.

matcher-v2 changes ONE thing, the type factor, based on recorded feedback
(benchmarks/approval/feedback.jsonl: all 7 edits replaced a shared private
room with an entire place at a similar price; adjudicated labels picked an
entire place in 6 of 11 private-room requests):
  guest prefers "room": entire place (apartment/house/condo/cabin) 1.0,
                        private room 0.8
  any other preference: unchanged (exact match 1.0, else 0.0)
  Result: worse than v1 on the fixed benchmark (mean top-pick rank 2.86 -> 3.34);
  NOT promoted. Labelers had picked a private room in 5 of those 11 requests,
  so penalizing rooms over-generalized one reviewer's 7 edits.

matcher-v3 (second, declared attempt) reads the same feedback as "an entire
place also satisfies a private-room preference", not "rooms are worse":
  guest prefers "room": entire place 1.0, private room 1.0
  any other preference: unchanged
  Result: also worse than v1 (mean top-pick rank 2.86 -> 3.28); NOT promoted.
  Both runs: benchmarks/results/eval_single_agent_matcher-v2.csv / -v3.csv.
matcher-v1 stays available for comparison and rollback.

Usage (from repo root), prints the top 10 for one benchmark request:
    python3 services/agents/matching.py R05 [matcher-v2]
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

SCORING_VERSION = "matcher-v1"           # default until v2 is approved
VERSIONS = ("matcher-v1", "matcher-v2", "matcher-v3")
ENTIRE_PLACE = ("apartment", "house", "condo", "cabin")
WEIGHTS = {"availability": 0.25, "rating": 0.25, "budget": 0.20,
           "amenities": 0.15, "type": 0.10, "location": 0.05}
OTHER_AMENITIES = ["kitchen", "pool", "washer"]
TOP_K = 10


def stay_nights(check_in, check_out):
    start, end = date.fromisoformat(check_in), date.fromisoformat(check_out)
    return {(start + timedelta(d)).isoformat() for d in range((end - start).days)}


def hard_filter(q, props, blocked):
    """Candidates: active, right city, enough capacity, within budget,
    all required amenities, meets min rating (unknown counts as meeting it),
    and free on every requested night."""
    nights = stay_nights(q["check_in"], q["check_out"])
    out = []
    for p in props:
        if p["status"] != "active" or p["location"]["city"] != q["city"]:
            continue
        if p["capacity"] < q["guests"] or p["nightly_price"] > q["max_nightly_price"]:
            continue
        if not set(q["required_amenities"]) <= set(p["amenities"]):
            continue
        if q.get("min_rating") and p["review_count"] > 0 and p["rating"] < q["min_rating"]:
            continue
        if blocked.get(p["property_id"], set()) & nights:
            continue
        out.append(p)
    return out


def type_factor(p, q, version):
    pref = q.get("preferred_property_type")
    if version == "matcher-v2" and pref == "room":
        return 1.0 if p["property_type"] in ENTIRE_PLACE else 0.8
    if version == "matcher-v3" and pref == "room":
        return 1.0                         # room or entire place both satisfy the preference
    return 1.0 if p["property_type"] == pref else 0.0


def factors(p, q, blocked, version=SCORING_VERSION):
    """Each factor in 0-1. 'rating' is None when the listing has no reviews."""
    free = not (blocked.get(p["property_id"], set()) & stay_nights(q["check_in"], q["check_out"]))
    budget = q["max_nightly_price"]
    has = set(p["amenities"])
    return {
        "availability": 1.0 if free else 0.0,
        "rating": None if p["review_count"] == 0 else min(1.0, max(0.0, (p["rating"] - 3) / 2)),
        "budget": max(0.0, (budget - p["nightly_price"]) / budget),
        "amenities": 0.5 * ("parking" in has) + 0.5 * sum(a in has for a in OTHER_AMENITIES) / len(OTHER_AMENITIES),
        "type": type_factor(p, q, version),
        "location": 1.0 if p["location"]["city"] == q["city"] else 0.0,
    }


def weighted_score(f):
    """Weighted sum; factors that are None are dropped and weights renormalized."""
    used = {k: w for k, w in WEIGHTS.items() if f[k] is not None}
    total = sum(used.values())
    return sum(f[k] * w / total for k, w in used.items())


def reasons(p, f, q=None):
    out = []
    if f["rating"] is None:
        out.append("new listing, no reviews yet (rating not used)")
    elif f["rating"] >= 0.5:
        out.append(f"rated {p['rating']}")
    if f["budget"] >= 0.3:
        out.append(f"${p['nightly_price']:.0f}/night, well under budget")
    if "parking" in p["amenities"]:
        out.append("has parking")
    if f["type"] == 1.0 and q and q.get("preferred_property_type") == "room" and p["property_type"] != "room":
        out.append(f"entire {p['property_type']}, not shared")
    elif f["type"]:
        out.append(f"preferred type ({p['property_type']})")
    return out[:3] or ["meets all requirements"]


def rank(q, props, blocked, version=SCORING_VERSION):
    """Return agent payload fields: candidates and the ranked top 10."""
    if version not in VERSIONS:
        raise ValueError(f"unknown matcher version {version}")
    cands = hard_filter(q, props, blocked)
    scored = []
    for p in cands:
        f = factors(p, q, blocked, version)
        scored.append((round(weighted_score(f), 4), p, f))
    scored.sort(key=lambda x: (-x[0], x[1]["property_id"]))   # ties by property_id
    ranked = [{
        "property_id": p["property_id"],
        "rank": i,
        "score": s,
        "score_breakdown": {k: (None if v is None else round(v, 4)) for k, v in f.items()},
        "reasons": reasons(p, f, q),
        "evidence_ids": [p["property_id"]],
    } for i, (s, p, f) in enumerate(scored[:TOP_K], start=1)]
    return {
        "agent_id": "matching",
        "scoring_version": version,
        "weights": WEIGHTS,
        "candidate_property_ids": sorted(p["property_id"] for p in cands),
        "ranked_properties": ranked,
    }


def load_seed(seed_dir="data/seed/generated"):
    d = Path(seed_dir)
    props = json.loads((d / "properties.json").read_text())
    blocked = {}
    for b in json.loads((d / "availability_blocks.json").read_text()):
        blocked.setdefault(b["property_id"], set()).add(b["night_date"])
    return props, blocked


if __name__ == "__main__":
    rid = sys.argv[1] if len(sys.argv) > 1 else "R01"
    version = sys.argv[2] if len(sys.argv) > 2 else SCORING_VERSION
    reqs = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    props, blocked = load_seed()
    out = rank(reqs[rid]["expected_parse"], props, blocked, version)
    print(reqs[rid]["request_text"])
    print(f"{len(out['candidate_property_ids'])} candidates, {version}")
    for r in out["ranked_properties"]:
        print(f"{r['rank']:>2}. {r['property_id']}  score {r['score']:.3f}  {'; '.join(r['reasons'])}")
