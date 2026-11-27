"""Entire-place alternative for the Explanation step (explanation-v2).

Feedback: in all 7 edited reviews (benchmarks/approval/feedback.jsonl) the
reviewer replaced a recommended private room with an entire place at a
similar price. Changing the ranking for this (matcher-v2, matcher-v3) made the
benchmark worse, so explanation-v2 keeps the ranking and only SHOWS the guest
the best entire-place option next to a private-room recommendation.

Rule, fixed before evaluation:
  applies when the guest prefers "room" and the recommendation is a room
  candidates: entire places (apartment/house/condo/cabin) that pass the hard
  filters, cost at most MAX_EXTRA more per night, and are rated at least as
  high as the recommended room (listings with no reviews are not excluded)
  pick: the highest matcher-v1 score (which includes rating)

Result (benchmarks/results/explanation_v2_eval.json): an entire-place
alternative is shown in 7/7 edited reviews, and it is the reviewer's own pick
in 2/7 (R04, R17). The 23 approved explanations are unchanged and the ranking
does not change.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import matching  # noqa: E402

EXPLANATION_VERSION = "explanation-v2"
MAX_EXTRA = 20


def entire_place_alternative(q, recommended_id, props, blocked):
    by_id = {p["property_id"]: p for p in props}
    rec = by_id[recommended_id]
    if q.get("preferred_property_type") != "room" or rec["property_type"] != "room":
        return None
    best = None
    for p in matching.hard_filter(q, props, blocked):
        if p["property_type"] not in matching.ENTIRE_PLACE:
            continue
        if p["nightly_price"] > rec["nightly_price"] + MAX_EXTRA:
            continue
        if p["review_count"] and rec["review_count"] and p["rating"] < rec["rating"]:
            continue
        score = matching.weighted_score(matching.factors(p, q, blocked, "matcher-v1"))
        if best is None or (score, p["property_id"]) > (best[0], best[1]["property_id"]):
            best = (score, p)
    if best is None:
        return None
    p = best[1]
    return {"property_id": p["property_id"], "property_type": p["property_type"],
            "nightly_price": p["nightly_price"],
            "rating": p["rating"] if p["review_count"] else None, "review_count": p["review_count"]}
