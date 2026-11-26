"""Review & Risk agent, rules version risk-rules-v1.

Re-ranks the Matching agent's candidates using review complaints and host
cancellations. Most complaints are only flagged for the guest to ask the host
about; a missing amenity is the one that really lowers the score, more so if
it is an amenity the guest requires.

  score = matcher-v1 score x (1 - penalty),  penalty capped at 0.40

Penalties per occurrence:
  missing_amenity naming a required amenity  0.15  high
  missing_amenity (other)                    0.08  medium
  cleanliness / cancellation / noise /
  host_unresponsive / price_mismatch         0.02  low
  reservation cancelled by the host          0.02  low

Usage (from repo root), prints the top 10 and risk flags for one request:
    python3 services/agents/risk.py R45
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import matching  # noqa: E402

RULES_VERSION = "risk-rules-v1"
MAX_PENALTY = 0.40
MINOR_TAGS = ["cleanliness", "cancellation", "noise", "host_unresponsive", "price_mismatch"]
PENALTY = {"missing_required_amenity": 0.15, "missing_amenity": 0.08, "minor_complaint": 0.02,
           "host_cancellation": 0.02}
SEVERITY = {"missing_required_amenity": "high", "missing_amenity": "medium",
            "minor_complaint": "low", "host_cancellation": "low"}
ACTION = {"missing_required_amenity": "confirm the required amenity with the host before booking",
          "missing_amenity": "confirm the amenity list with the host",
          "minor_complaint": "mention to the guest; ask the host if it matters",
          "host_cancellation": "mention to the guest; ask the host to confirm the booking"}
TOP_K = 10


def load_evidence(seed_dir="data/seed/generated"):
    """property_id -> reviews, property_id -> host-cancelled reservations."""
    d = Path(seed_dir)
    reviews, cancels = defaultdict(list), defaultdict(list)
    for r in json.loads((d / "reviews.json").read_text()):
        reviews[r["property_id"]].append(r)
    for r in json.loads((d / "reservations.json").read_text()):
        if r["status"] == "cancelled" and r["cancelled_by"] == "host":
            cancels[r["property_id"]].append(r)
    return reviews, cancels


def flags_for(pid, q, reviews, cancels):
    """Risk flags for one property, each with its evidence id."""
    out = []
    required = set(q["required_amenities"])
    for rv in reviews.get(pid, []):
        tags = set(rv["tags"])
        if "missing_amenity" in tags:
            named = [a for a in required if a in rv["review_text"].lower()]
            kind = "missing_required_amenity" if named else "missing_amenity"
            detail = f"review says {named[0]} was missing" if named else "review reports a missing amenity"
            out.append((kind, rv["review_id"], detail))
        for t in MINOR_TAGS:
            if t in tags:
                out.append(("minor_complaint", rv["review_id"], f"{t.replace('_', ' ')} complaint"))
    for res in cancels.get(pid, []):
        out.append(("host_cancellation", res["reservation_id"], "host cancelled a reservation"))
    return [{"type": k, "severity": SEVERITY[k], "penalty": PENALTY[k],
             "evidence_ids": [eid], "detail": detail, "action": ACTION[k]} for k, eid, detail in out]


def rank(q, props, blocked, reviews, cancels):
    """Round 0 payload of the Risk agent."""
    cands = matching.hard_filter(q, props, blocked)
    scored = []
    for p in cands:
        base = matching.weighted_score(matching.factors(p, q, blocked))
        flags = flags_for(p["property_id"], q, reviews, cancels)
        penalty = min(MAX_PENALTY, sum(f["penalty"] for f in flags))
        scored.append((round(base * (1 - penalty), 4), p, base, penalty, flags))
    scored.sort(key=lambda x: (-x[0], x[1]["property_id"]))
    # flags for every candidate, so Risk can explain why it rejects another agent's pick
    risk_flags = [dict(f, property_id=p["property_id"]) for _, p, _, _, flags in scored for f in flags]
    ranked = []
    for i, (s, p, base, penalty, flags) in enumerate(scored[:TOP_K], start=1):
        pid = p["property_id"]
        worst = max((f["severity"] for f in flags), key=["low", "medium", "high"].index, default=None)
        ranked.append({
            "property_id": pid, "rank": i, "score": s,
            "score_breakdown": {"matcher_score": round(base, 4), "risk_penalty": round(penalty, 4)},
            "reasons": [f"{len(flags)} risk flag(s), worst {worst}"] if flags else ["no complaints in reviews"],
            "evidence_ids": [e for f in flags for e in f["evidence_ids"]] or [pid],
        })
    return {
        "agent_id": "risk",
        "scoring_version": RULES_VERSION,
        "candidate_property_ids": sorted(p["property_id"] for p in cands),
        "ranked_properties": ranked,
        "risk_flags": risk_flags,
    }


if __name__ == "__main__":
    rid = sys.argv[1] if len(sys.argv) > 1 else "R45"
    reqs = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    props, blocked = matching.load_seed()
    reviews, cancels = load_evidence()
    q = reqs[rid]["expected_parse"]
    out = rank(q, props, blocked, reviews, cancels)
    print(reqs[rid]["request_text"])
    for r in out["ranked_properties"]:
        b = r["score_breakdown"]
        print(f"{r['rank']:>2}. {r['property_id']}  score {r['score']:.3f}  "
              f"(matcher {b['matcher_score']:.3f}, penalty {b['risk_penalty']:.2f})")
    print("\nmedium/high flags (all candidates):")
    for f in [f for f in out["risk_flags"] if f["severity"] != "low"]:
        print(f"  {f['property_id']} [{f['severity']}] {f['detail']} -> {f['action']} ({f['evidence_ids'][0]})")
