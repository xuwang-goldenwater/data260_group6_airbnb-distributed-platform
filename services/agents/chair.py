"""Chair decision logic: pure functions, no Kafka.

Implements the rules in docs/api/ai.md and docs/api/agent_payloads.md so the
offline benchmark and the Kafka Chair use exactly the same logic:
  1. check every agent ranked the same candidate set
  2. disagreement metrics from Round 0 (agent_payloads.md section 6)
  3. Round 1: original = Matching top-1; challenger = Risk top-1 if different,
     else Fairness top-1 if different; the Auditor always gets a question in
     full_debate
  4. Round 2 votes: each agent votes for whichever of the two it scored higher
     in its own Round 0 ranking (abstains if it has no preference)
  5. adjudication: safety_then_majority (safety gate, majority, tie-break by
     lower risk penalty, then higher Matching score, then original)
  6. final_ranking: Borda count over the agents' top-10s, winner first

Usage (from repo root), runs a full debate for one benchmark request:
    python3 services/agents/chair.py R45
"""
import json
import sys
from itertools import combinations
from pathlib import Path

DECISION_RULE = "safety_then_majority"
CHALLENGE_OVERLAP = 0.6
MAX_RISK_PENALTY = 0.40
TOP_K = 10


class CandidateMismatch(Exception):
    """Agents ranked different candidate sets: the round is invalid."""


def top_ids(payload, k=TOP_K):
    return [r["property_id"] for r in payload["ranked_properties"][:k]]


def scores(payload):
    return {r["property_id"]: r["score"] for r in payload["ranked_properties"]}


def check_candidates(round0):
    sets = {a: tuple(p["candidate_property_ids"]) for a, p in round0.items()}
    if len(set(sets.values())) > 1:
        raise CandidateMismatch(f"candidate sets differ across agents: {list(sets)}")


def disagreement(round0):
    agents = list(round0)
    tops = {a: top_ids(round0[a]) for a in agents}
    pairs = list(combinations(agents, 2))
    overlap = (sum(len(set(tops[a]) & set(tops[b])) / len(set(tops[a]) | set(tops[b])) for a, b in pairs)
               / len(pairs)) if pairs else 1.0
    gap = 0.0
    top3 = {pid for a in agents for pid in tops[a][:3]}
    for pid in top3:
        vals = [scores(round0[a])[pid] for a in agents if pid in scores(round0[a])]
        if len(vals) > 1:
            gap = max(gap, max(vals) - min(vals))
    top1_distinct = len({tops[a][0] for a in agents if tops[a]})
    return {"top1_distinct": top1_distinct, "top10_overlap": round(overlap, 4),
            "max_score_gap": round(gap, 4),
            "challenge_required": top1_distinct > 1 or overlap < CHALLENGE_OVERLAP}


def round1(round0, config):
    original = top_ids(round0["matching"], 1)[0]
    challenger, trigger = None, None
    for agent in ("risk", "fairness"):
        if agent in round0 and top_ids(round0[agent], 1)[0] != original:
            challenger, trigger = top_ids(round0[agent], 1)[0], f"{agent}_disagrees"
            break
    questions = []
    if challenger:
        questions.append({"question_id": "q-001", "target_agent_id": "matching", "property_id": original,
                          "question": f"Why rank {original} above {challenger}?"})
    if config == "full_debate":     # the Auditor is challenged in every full debate
        questions.append({"question_id": f"q-{len(questions) + 1:03d}", "target_agent_id": "fairness",
                          "property_id": original,
                          "question": f"Does the ranking of {original} depend on review count?"})
    return {"round": 1, "trigger": trigger or "audit_only", "original_property_id": original,
            "challenger_property_id": challenger, "questions": questions}


def vote(agent, payload, original, challenger):
    s = scores(payload)
    a, b = s.get(original), s.get(challenger)
    if a is None and b is None or a == b:
        return {"agent_id": agent, "choice": "abstain", "property_id": None,
                "reason": "no preference between the two in my Round 0 ranking", "evidence_ids": []}
    keep = b is None or (a is not None and a > b)
    pid = original if keep else challenger
    other = challenger if keep else original
    return {"agent_id": agent, "choice": "keep_original" if keep else "switch_to_challenger",
            "property_id": pid,
            "reason": f"I scored {pid} {s[pid]:.3f} vs {other} "
                      f"{'not in my top 10' if s.get(other) is None else f'{s[other]:.3f}'}",
            "evidence_ids": [f"round0:{agent}"]}


def risk_penalties(round0):
    pen = {}
    for f in round0.get("risk", {}).get("risk_flags", []):
        pen[f["property_id"]] = min(MAX_RISK_PENALTY, pen.get(f["property_id"], 0) + f["penalty"])
    return pen


def high_risk(round0):
    return {f["property_id"] for f in round0.get("risk", {}).get("risk_flags", []) if f["severity"] == "high"}


def adjudicate(round0, r1, votes):
    original, challenger = r1["original_property_id"], r1["challenger_property_id"]
    if challenger is None:
        return original, {"method": "consensus", "deadlock": False, "decision_rule": DECISION_RULE,
                          "tie_break_applied": None}
    options = [original, challenger]
    # 1. safety gate
    unsafe = high_risk(round0)
    safe = [p for p in options if p not in unsafe]
    if len(safe) == 1:
        return safe[0], {"method": "rule_forced", "deadlock": False, "decision_rule": DECISION_RULE,
                         "tie_break_applied": "safety_gate", "unsafe": sorted(set(options) - set(safe))}
    # 2. majority
    count = {p: sum(v["property_id"] == p for v in votes) for p in options}
    valid = sum(count.values())
    if valid and count[original] != count[challenger]:
        winner = max(options, key=count.get)
        method = "consensus" if count[winner] == valid else "majority"
        return winner, {"method": method, "deadlock": False, "decision_rule": DECISION_RULE,
                        "tie_break_applied": None}
    # 3. tie-break: lower risk penalty, then higher Matching score, then original
    pen, mscore = risk_penalties(round0), scores(round0["matching"])
    if valid == 0:
        winner, rule = original, "no_valid_votes"
    elif pen.get(original, 0) != pen.get(challenger, 0):
        winner, rule = min(options, key=lambda p: pen.get(p, 0)), "lower_risk_penalty"
    elif mscore.get(original, 0) != mscore.get(challenger, 0):
        winner, rule = max(options, key=lambda p: mscore.get(p, 0)), "higher_matching_score"
    else:
        winner, rule = original, "keep_original"
    return winner, {"method": "rule_forced", "deadlock": True, "decision_rule": DECISION_RULE,
                    "tie_break_applied": rule,
                    "risk_penalty": {p: round(pen.get(p, 0), 4) for p in options}}


def final_ranking(round0, winner):
    points = {}
    for payload in round0.values():
        for i, pid in enumerate(top_ids(payload)):
            points[pid] = points.get(pid, 0) + (TOP_K - i)
    order = sorted(points, key=lambda p: (-points[p], p))
    return [winner] + [p for p in order if p != winner][:TOP_K - 1]


def run(round0, config):
    """Round 0 payloads {agent_id: payload} -> ai.results-style decision."""
    check_candidates(round0)
    dis = disagreement(round0)
    r1 = round1(round0, config)
    votes = [vote(a, p, r1["original_property_id"], r1["challenger_property_id"])
             for a, p in round0.items()] if r1["challenger_property_id"] else []
    winner, adj = adjudicate(round0, r1, votes)
    dissent = [{"agent_id": v["agent_id"], "reason": v["reason"]}
               for v in votes if v["property_id"] not in (winner, None)]
    findings = round0.get("fairness", {}).get("audit_findings", [])
    return {
        "config": config,
        "recommendation": winner,
        "final_ranking": final_ranking(round0, winner),
        "disagreement": dis,
        "round1": r1,
        "votes": votes,
        "vote_summary": {c: sum(v["choice"] == c for v in votes)
                         for c in ("keep_original", "switch_to_challenger", "abstain")},
        "dissent": dissent,
        "adjudication": adj,
        "audit_findings": findings,
        "scoring_versions": {a: p["scoring_version"] for a, p in round0.items()},
    }


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    import fairness, matching, risk  # noqa: E401
    rid = sys.argv[1] if len(sys.argv) > 1 else "R45"
    reqs = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    q = reqs[rid]["expected_parse"]
    props, blocked = matching.load_seed()
    reviews, cancels = risk.load_evidence()
    round0 = {"matching": matching.rank(q, props, blocked),
              "risk": risk.rank(q, props, blocked, reviews, cancels),
              "fairness": fairness.rank(q, props, blocked)}
    out = run(round0, "full_debate")
    print(reqs[rid]["request_text"])
    for a, p in round0.items():
        print(f"  Round 0 {a:>9}: top-1 {top_ids(p, 1)[0]}")
    print("disagreement:", out["disagreement"])
    print("round 1:", out["round1"]["original_property_id"], "vs", out["round1"]["challenger_property_id"])
    for v in out["votes"]:
        print(f"  vote {v['agent_id']:>9}: {v['choice']:<21} {v['reason']}")
    print("decision:", out["recommendation"], out["adjudication"])
    print("final_ranking:", out["final_ranking"])
