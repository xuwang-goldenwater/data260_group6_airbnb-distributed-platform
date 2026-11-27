"""Run the two debate configurations on all 50 benchmark requests.

  debate_without_auditor : Matching + Risk
  full_debate            : Matching + Risk + Fairness Auditor

The structured request comes from the Parser (exact cache, same as
single_agent), so all agent configurations see the same parse. The Chair
logic is services/agents/chair.py.

Usage (from repo root):
    python3 benchmarks/run_debate.py
Output:
    benchmarks/results/debate_without_auditor.csv, full_debate.csv
        same columns as baseline.csv, plus the adjudication outcome
    benchmarks/results/traces_<config>.jsonl
        one full debate record per request (Round 0, Round 1, votes,
        adjudication) - the evidence for the report's case studies
"""
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path("services/agents")))
import chair  # noqa: E402
import fairness  # noqa: E402
import matching  # noqa: E402
import parser as parser_agent  # noqa: E402
import risk  # noqa: E402

REQUESTS = Path("benchmarks/requests.json")
RESULTS_DIR = Path("benchmarks/results")
CONFIGS = {"debate_without_auditor": ["matching", "risk"],
           "full_debate": ["matching", "risk", "fairness"]}


def main():
    bench = json.loads(REQUESTS.read_text())
    props, blocked = matching.load_seed()
    reviews, cancels = risk.load_evidence()
    cache = parser_agent.load_cache()

    rows = {c: [] for c in CONFIGS}
    traces = {c: [] for c in CONFIGS}
    for req in bench["requests"]:
        rid = req["request_id"]
        parsed = parser_agent.parse(req["request_text"], cache)
        q = parsed["request"]
        if q is None:
            print(f"{rid}: parser error, skipped ({parsed['error']})")
            continue
        t0 = time.perf_counter()
        round0_all = {"matching": matching.rank(q, props, blocked),
                      "risk": risk.rank(q, props, blocked, reviews, cancels),
                      "fairness": fairness.rank(q, props, blocked)}
        agents_ms = (time.perf_counter() - t0) * 1000
        matcher_top1 = round0_all["matching"]["ranked_properties"][0]["property_id"]

        decided = {}
        for config, agents in CONFIGS.items():
            t1 = time.perf_counter()
            out = chair.run({a: round0_all[a] for a in agents}, config)
            decided[config] = out["recommendation"]
            adj = out["adjudication"]
            rows[config].append({
                "request_id": rid,
                "config": config,
                "trace_id": f"tr-bench-{config}-{rid}",
                "scoring_version": "+".join(out["scoring_versions"].values()),
                "parser_cache_hit": parsed["llm_cache_hit"],
                "final_ranking": ";".join(out["final_ranking"]),
                "scores": "",
                "latency_ms": round(agents_ms + (time.perf_counter() - t1) * 1000, 2),
                "method": adj["method"],
                "tie_break": adj["tie_break_applied"] or "",
                "deadlock": adj["deadlock"],
                "challenge_required": out["disagreement"]["challenge_required"],
                "changed_matcher_top1": out["recommendation"] != matcher_top1,
                "auditor_changed_outcome": "",
            })
            traces[config].append(dict(out, request_id=rid, trace_id=f"tr-bench-{config}-{rid}",
                                       parse=q, round0={a: round0_all[a] for a in agents}))
        # did adding the Auditor change the final recommendation?
        rows["full_debate"][-1]["auditor_changed_outcome"] = (
            decided["full_debate"] != decided["debate_without_auditor"])

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for config in CONFIGS:
        path = RESULTS_DIR / f"{config}.csv"
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[config][0].keys()))
            w.writeheader()
            w.writerows(rows[config])
        tpath = RESULTS_DIR / f"traces_{config}.jsonl"
        tpath.write_text("".join(json.dumps(t, sort_keys=True) + "\n" for t in traces[config]))
        changed = sum(r["changed_matcher_top1"] for r in rows[config])
        print(f"Wrote {path} and {tpath}: {len(rows[config])} requests, "
              f"top-1 changed vs Matching in {changed}")
    print(f"Auditor changed the outcome in "
          f"{sum(r['auditor_changed_outcome'] is True for r in rows['full_debate'])} requests")


if __name__ == "__main__":
    main()
