"""Reflection step for the analysis agents (assignment 7.5.3).

Each agent checks its own Round 0 output before publishing it:
  constraint  every top-10 property passes the request's hard filters
  evidence    every ranked property cites evidence ids that exist in the data
  risk        no property in the top 3 has a HIGH-severity risk flag
  conflict    other agents' opinions are not visible in Round 0 -> "na"
  action      the output proposes no booking / message / availability change
All checks are code, not LLM. Limit: one initial check and at most one
revision (max 2 cycles).

Revision: a top-3 property with a high-risk flag is moved down to rank 4
(just outside the top 3), then the checks run again. A failed constraint or
evidence check cannot be repaired here, so the output is rejected
(status error, REFLECTION_REJECTED) instead of being published.

The payload gets a "reflection" object (docs/api/agent_payloads.md section 5).
"""
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import matching  # noqa: E402
import risk  # noqa: E402

MAX_CYCLES = 2
TOP_RISK = 3
PROHIBITED_KEYS = {"booking", "message", "availability_update", "action"}


def _known_ids(seed_dir="data/seed/generated"):
    d = Path(seed_dir)
    ids = {p["property_id"] for p in json.loads((d / "properties.json").read_text())}
    ids |= {r["review_id"] for r in json.loads((d / "reviews.json").read_text())}
    ids |= {r["reservation_id"] for r in json.loads((d / "reservations.json").read_text())}
    return ids


_KNOWN = None


def known_ids():
    global _KNOWN
    if _KNOWN is None:
        _KNOWN = _known_ids()
    return _KNOWN


def checks(payload, q, props, blocked, reviews, cancels):
    ranked = payload["ranked_properties"]
    valid = {p["property_id"] for p in matching.hard_filter(q, props, blocked)}
    bad_c = [r["property_id"] for r in ranked if r["property_id"] not in valid]
    bad_e = [r["property_id"] for r in ranked
             if not r.get("evidence_ids") or any(e not in known_ids() for e in r["evidence_ids"])]
    high = [r["property_id"] for r in ranked[:TOP_RISK]
            if any(f["severity"] == "high" for f in risk.flags_for(r["property_id"], q, reviews, cancels))]
    acts = sorted(PROHIBITED_KEYS & set(payload))
    return {
        "constraint": {"result": "fail" if bad_c else "pass",
                       "note": f"not matching hard filters: {bad_c}" if bad_c else "top 10 meet all hard filters"},
        "evidence": {"result": "fail" if bad_e else "pass",
                     "note": f"missing/unknown evidence: {bad_e}" if bad_e else "every ranked property cites stored evidence"},
        "risk": {"result": "warn" if high else "pass",
                 "note": f"high-risk flag in top {TOP_RISK}: {high}" if high else f"no high-risk flag in top {TOP_RISK}",
                 "properties": high},
        "conflict": {"result": "na", "note": "other agents are not visible in Round 0"},
        "action": {"result": "fail" if acts else "pass",
                   "note": f"prohibited fields: {acts}" if acts else "no external action proposed"},
    }


def _demote(ranked, pids):
    """Move each listed property from the top 3 down to rank 4, keep the rest in order."""
    keep = [r for r in ranked if r["property_id"] not in pids]
    moved = [r for r in ranked if r["property_id"] in pids]
    out = keep[:TOP_RISK] + moved + keep[TOP_RISK:]
    for i, r in enumerate(out, start=1):
        r["rank"] = i
    return out


def reflect(payload, q, props, blocked, reviews, cancels):
    """Return a new payload with a reflection object (and a revised ranking if needed)."""
    out = copy.deepcopy(payload)
    before = out["ranked_properties"][0]["property_id"] if out["ranked_properties"] else None
    history = []
    for cycle in range(1, MAX_CYCLES + 1):
        c = checks(out, q, props, blocked, reviews, cancels)
        history.append({"cycle": cycle, "checks": c})
        if any(c[k]["result"] == "fail" for k in ("constraint", "evidence", "action")):
            decision = "reject"
            break
        if c["risk"]["result"] == "warn" and cycle < MAX_CYCLES:
            out["ranked_properties"] = _demote(out["ranked_properties"], set(c["risk"]["properties"]))
            continue
        decision = "revise" if cycle > 1 else "keep"
        break
    after = out["ranked_properties"][0]["property_id"] if out["ranked_properties"] else None
    out["reflection"] = {
        "cycles": len(history),
        "checks": history[-1]["checks"],
        "history": history,
        "decision": decision,
        "before_top_1": before,
        "after_top_1": after,
        "changed_top_1": before != after,
    }
    if decision == "reject":
        out["status"] = "error"
        out["error"] = {"code": "REFLECTION_REJECTED", "detail": history[-1]["checks"]}
    return out


if __name__ == "__main__":
    import fairness  # noqa: E401
    rid = sys.argv[1] if len(sys.argv) > 1 else "R45"
    reqs = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    q = reqs[rid]["expected_parse"]
    props, blocked = matching.load_seed()
    reviews, cancels = risk.load_evidence()
    for name, payload in (("matching", matching.rank(q, props, blocked)),
                          ("risk", risk.rank(q, props, blocked, reviews, cancels)),
                          ("fairness", fairness.rank(q, props, blocked))):
        r = reflect(payload, q, props, blocked, reviews, cancels)["reflection"]
        print(f"{name:>9}: {r['decision']:<6} cycles {r['cycles']}  top-1 {r['before_top_1']} -> {r['after_top_1']}  "
              f"risk: {r['history'][0]['checks']['risk']['note']}")
