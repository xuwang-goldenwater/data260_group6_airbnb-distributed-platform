"""Generate the fixed 50-request benchmark from the seed property pool.

44 random requests (4 travel purposes) + 6 fixed scenario requests.
Deterministic: same seed + same seed data -> identical output.

Usage (from repo root):
    python3 benchmarks/make_requests.py
Output:
    benchmarks/requests.json
"""
import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED_DIR = Path("data/seed/generated")
OUT = Path("benchmarks/requests.json")

AMENITIES = ["kitchen", "parking", "pool", "washer"]   # wifi is in all 200, so never a filter
TYPES = ["apartment", "house", "condo", "cabin", "room"]
ENTIRE_PLACE = ["apartment", "house", "condo", "cabin"]   # "room" = private room in a shared home
TYPE_PHRASE = {"apartment": "an entire apartment", "house": "a whole house", "condo": "a condo",
               "cabin": "a cabin", "room": "a private room"}
CITIES = ["San Jose", "Santa Cruz", "Monterey", "San Francisco"]
FIRST_CHECKIN, LAST_CHECKOUT = date(2026, 11, 26), date(2026, 12, 3)  # future blocks end 12/02
MIN_CAND, MAX_CAND = 12, 40

# purpose -> how to draw the request parameters
PURPOSES = {
    "leisure":      {"guests": [2, 3, 4], "cities": CITIES, "nights": [2, 3],
                     "budget": (180, 300), "n_amen": [0, 1, 2], "must": []},
    "business":     {"guests": [1, 2], "cities": ["San Francisco", "San Jose"], "nights": [1, 2],
                     "budget": (180, 300), "n_amen": [0, 1], "must": [], "pool": ["parking"]},
    "group":        {"guests": [6, 7, 8], "cities": CITIES, "nights": [2],
                     "budget": (300, 400), "n_amen": [1], "must": ["kitchen"]},
    "family_visit": {"guests": [2, 3, 4, 5, 6], "cities": ["San Jose", "Santa Cruz"], "nights": [2, 3],
                     "budget": (180, 300), "n_amen": [0, 1], "must": ["kitchen"], "pool": ["washer"]},
}
COUNTS = {"leisure": 11, "business": 11, "group": 11, "family_visit": 11}  # 44 random

TEMPLATES = {
    "leisure": "We're {g} people planning a trip to {city} from {ci} to {co}. Budget is ${b} a night{am}{rt}. We'd love {pt}.",
    "business": "I'm traveling to {city} for work, {ci} to {co}, {gs}. Please keep it under ${b} per night{am}{rt}. {Pt} would be ideal.",
    "group": "Group of {g} friends visiting {city} from {ci} to {co}. We can spend up to ${b} per night{am}{rt}. Prefer {pt} so we can all stay together.",
    "family_visit": "Visiting family in {city} from {ci} to {co}, {g} of us. Max ${b} a night{am}{rt}. {Pt} would feel most like home.",
}


def load(name):
    return json.loads((SEED_DIR / name).read_text())


def blocked_nights():
    out = {}
    for b in load("availability_blocks.json"):
        out.setdefault(b["property_id"], set()).add(b["night_date"])
    return out


def candidates(req, props, blocks):
    """Hard filters only. Property type and rating order are NOT filters."""
    nights = [(date.fromisoformat(req["check_in"]) + timedelta(d)).isoformat()
              for d in range((date.fromisoformat(req["check_out"]) - date.fromisoformat(req["check_in"])).days)]
    out = []
    for p in props:
        if p["status"] != "active" or p["location"]["city"] != req["city"]:
            continue
        if p["capacity"] < req["guests"] or p["nightly_price"] > req["max_nightly_price"]:
            continue
        if not set(req["required_amenities"]) <= set(p["amenities"]):
            continue
        # min rating: new listings (0 reviews) are unknown, not low -> not excluded
        if req["min_rating"] and p["review_count"] > 0 and p["rating"] < req["min_rating"]:
            continue
        if blocks.get(p["property_id"], set()) & set(nights):
            continue
        out.append(p["property_id"])
    return sorted(out)


def text_for(purpose, r):
    am = ""
    if r["required_amenities"]:
        am = ", and we need " + " and ".join(r["required_amenities"])
    rt = f", rated at least {r['min_rating']}" if r["min_rating"] else ""
    return TEMPLATES[purpose].format(
        g=r["guests"], gs="just me" if r["guests"] == 1 else f"{r['guests']} of us", city=r["city"], ci=r["check_in"], co=r["check_out"],
        b=r["max_nightly_price"], am=am, rt=rt,
        pt=TYPE_PHRASE[r["preferred_property_type"]],
        Pt=TYPE_PHRASE[r["preferred_property_type"]].capitalize())


def draw(purpose, city, rng):
    spec = PURPOSES[purpose]
    nights = rng.choice(spec["nights"])
    guests = rng.choice(spec["guests"])
    latest = (LAST_CHECKOUT - FIRST_CHECKIN).days - nights
    ci = FIRST_CHECKIN + timedelta(rng.randint(0, latest))
    extra_pool = [a for a in spec.get("pool", AMENITIES) if a not in spec["must"]]
    extra = rng.sample(extra_pool, min(rng.choice(spec["n_amen"]), len(extra_pool)))
    return {
        "purpose": purpose,
        "city": city,
        "check_in": ci.isoformat(),
        "check_out": (ci + timedelta(nights)).isoformat(),
        "guests": guests,
        "max_nightly_price": rng.randrange(spec["budget"][0], spec["budget"][1] + 1, 10),
        "required_amenities": sorted(spec["must"] + extra),
        "min_rating": rng.choice([None, None, 3.8]),
        # a private room only makes sense for 1-2 guests
        "preferred_property_type": rng.choice(TYPES if guests <= 2 else ENTIRE_PLACE),
    }


def scenario_requests(manifest):
    """6 fixed requests that hit A's seeded Risk / Fairness / occupied-date scenarios."""
    sc = manifest["scenarios"]
    base = {"city": "San Jose", "check_in": sc["check_in"], "check_out": sc["check_out"],
            "guests": 4, "max_nightly_price": 200, "required_amenities": ["kitchen", "parking"],
            "min_rating": None}
    rows = [("leisure", "apartment"), ("family_visit", "house"), ("business", "condo"),
            ("leisure", "house"), ("family_visit", "apartment"), ("group", "house")]
    out = []
    for purpose, pt in rows:
        r = dict(base, purpose=purpose, preferred_property_type=pt, scenario=True)
        if purpose == "group":
            r.update(guests=4, max_nightly_price=220)  # keep the same candidates
        out.append(r)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=260)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    props = load("properties.json")
    manifest = load("manifest.json")
    blocks = blocked_nights()

    reqs = []
    for purpose, n in COUNTS.items():
        cities = PURPOSES[purpose]["cities"]
        for k in range(n):
            city = cities[k % len(cities)]   # spread requests evenly over the purpose's cities
            for _ in range(500):
                r = draw(purpose, city, rng)
                if MIN_CAND <= len(candidates(r, props, blocks)) <= MAX_CAND:
                    break
            else:
                raise SystemExit(f"no valid {purpose} request for {city}; widen the ranges")
            r["scenario"] = False
            reqs.append(r)
    reqs += scenario_requests(manifest)

    out = []
    for i, r in enumerate(reqs, 1):
        c = candidates(r, props, blocks)
        out.append({
            "request_id": f"R{i:02d}",
            "purpose": r["purpose"],
            "scenario": r["scenario"],
            "request_text": text_for(r["purpose"], r),
            "expected_parse": {k: r[k] for k in ["city", "check_in", "check_out", "guests",
                               "max_nightly_price", "required_amenities", "min_rating",
                               "preferred_property_type"]},
            "candidate_count": len(c),
            "candidate_property_ids": c,
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "pool_id": manifest["pool_id"],
        "dataset_version": manifest["dataset_version"],
        "generator_seed": args.seed,
        "requests": out,
    }, indent=2) + "\n")
    print(f"Wrote {OUT}: {len(out)} requests")


if __name__ == "__main__":
    main()
