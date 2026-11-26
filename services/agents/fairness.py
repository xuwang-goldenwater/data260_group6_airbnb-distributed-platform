"""Fairness Auditor agent, version fairness-v1.

Audited signal: review count. matcher-v1 trusts a listing's own rating no
matter how few reviews it has (2 reviews at 5.0 counts as much as 10 at 5.0),
and a listing with 0 reviews has no rating at all.

The auditor ranks with a Bayesian-average rating instead:
    fair_rating = (n * rating + M * pool_mean) / (n + M),   M = 5
so few reviews pull the rating toward the pool mean, and many reviews keep the
listing's own rating. Everything else is the matcher-v1 formula.

It raises an audit finding for every low-evidence listing (< 3 reviews) whose
rank differs by 2+ places between matcher-v1 and the fair ranking, or that is
in one top 10 but not the other.

Usage (from repo root):
    python3 services/agents/fairness.py R45
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import matching  # noqa: E402

VERSION = "fairness-v1"
PRIOR_WEIGHT = 5            # M: reviews' worth of weight given to the pool mean
LOW_EVIDENCE = 3            # listings with fewer reviews are audited
MIN_RANK_SHIFT = 2
TOP_K = 10


def pool_mean_rating(props):
    rated = [p["rating"] for p in props if p["review_count"] > 0]
    return sum(rated) / len(rated)


def fair_factors(p, q, blocked, mean):
    f = matching.factors(p, q, blocked)
    n = p["review_count"]
    fair = (n * p["rating"] + PRIOR_WEIGHT * mean) / (n + PRIOR_WEIGHT)
    f["rating"] = min(1.0, max(0.0, (fair - 3) / 2))     # same scale as matcher-v1
    return f, fair


def ordered(scored):
    scored.sort(key=lambda x: (-x[0], x[1]["property_id"]))
    return {p["property_id"]: i for i, (_, p, *_) in enumerate(scored, start=1)}


def rank(q, props, blocked):
    """Round 0 payload of the Fairness Auditor."""
    mean = pool_mean_rating(props)
    cands = matching.hard_filter(q, props, blocked)
    m_scored, f_scored = [], []
    for p in cands:
        m_scored.append((matching.weighted_score(matching.factors(p, q, blocked)), p))
        f, fair = fair_factors(p, q, blocked, mean)
        f_scored.append((round(matching.weighted_score(f), 4), p, f, fair))
    m_rank, f_rank = ordered(m_scored), ordered(f_scored)

    findings = []
    for _, p, _, fair in f_scored:
        pid, n = p["property_id"], p["review_count"]
        a, b = m_rank[pid], f_rank[pid]
        crosses = (a <= TOP_K) != (b <= TOP_K)
        if n < LOW_EVIDENCE and (abs(a - b) >= MIN_RANK_SHIFT or crosses):
            direction = "under-ranked" if b < a else "over-ranked"
            own = f"rating {p['rating']} from {n} review(s)" if n else "no reviews"
            findings.append({
                "signal": "review_count",
                "affected_property_ids": [pid],
                "objection": (f"{pid} is {direction} by matcher-v1 (#{a}): its {own} is weak evidence; "
                              f"with a review-count-adjusted rating ({fair:.2f}) it ranks #{b}"),
                "suggested_action": ("move_into_top_10" if b <= TOP_K < a else
                                     "move_out_of_top_10" if a <= TOP_K < b else
                                     "move_up" if b < a else "move_down"),
                "rank_with_signal": a,
                "rank_without_signal": b,
                "evidence_ids": [pid],
            })

    top = sorted(f_scored, key=lambda x: (-x[0], x[1]["property_id"]))[:TOP_K]
    ranked = [{
        "property_id": p["property_id"], "rank": i, "score": s,
        "score_breakdown": {k: (None if v is None else round(v, 4)) for k, v in f.items()},
        "reasons": [f"review-count-adjusted rating {fair:.2f} ({p['review_count']} reviews)"],
        "evidence_ids": [p["property_id"]],
    } for i, (s, p, f, fair) in enumerate(top, start=1)]
    return {
        "agent_id": "fairness",
        "scoring_version": VERSION,
        "candidate_property_ids": sorted(p["property_id"] for p in cands),
        "ranked_properties": ranked,
        "audit_findings": findings,
    }


if __name__ == "__main__":
    rid = sys.argv[1] if len(sys.argv) > 1 else "R45"
    reqs = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    props, blocked = matching.load_seed()
    out = rank(reqs[rid]["expected_parse"], props, blocked)
    print(reqs[rid]["request_text"])
    for r in out["ranked_properties"]:
        print(f"{r['rank']:>2}. {r['property_id']}  score {r['score']:.3f}  {r['reasons'][0]}")
    print(f"\n{len(out['audit_findings'])} audit finding(s):")
    for f in out["audit_findings"]:
        print(f"  [{f['suggested_action']}] {f['objection']}")
