"""Run the Matching agent on all 50 benchmark requests.

Writes the same CSV format as baseline.csv so eval.py can score it.
--parse expected  uses the ground-truth parse   -> config matcher_expected_parse
--parse parser    uses the Parser agent's output -> config single_agent
The difference between the two isolates Parser errors. A request the Parser
rejects gets an empty ranking (counted as a miss by eval.py).

Usage (from repo root):
    python3 benchmarks/run_matcher.py --parse expected
    python3 benchmarks/run_matcher.py --parse parser   # needs Ollama unless all cached
    python3 benchmarks/run_matcher.py --parse parser --matcher matcher-v2
                                                       # -> single_agent_matcher-v2
    python3 benchmarks/run_matcher.py --parse parser --reflect
                                                       # -> single_agent_reflect
Output:
    benchmarks/results/<config>.csv
"""
import argparse
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path("services/agents")))
import matching  # noqa: E402
import parser as parser_agent  # noqa: E402
import reflection  # noqa: E402
import risk  # noqa: E402

REQUESTS = Path("benchmarks/requests.json")
RESULTS_DIR = Path("benchmarks/results")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parse", choices=["expected", "parser"], default="expected",
                    help="where the structured request comes from")
    ap.add_argument("--matcher", choices=matching.VERSIONS, default=matching.SCORING_VERSION)
    ap.add_argument("--reflect", action="store_true", help="run the reflection step (7.5.3)")
    args = ap.parse_args()
    config = "single_agent" if args.parse == "parser" else "matcher_expected_parse"
    if args.matcher != matching.SCORING_VERSION:
        config += f"_{args.matcher}"
    if args.reflect:
        config += "_reflect"
        reviews, cancels = risk.load_evidence()
    cache = parser_agent.load_cache()

    bench = json.loads(REQUESTS.read_text())
    props, blocked = matching.load_seed()

    rows = []
    for req in bench["requests"]:
        t0 = time.perf_counter()
        if args.parse == "parser":
            parsed = parser_agent.parse(req["request_text"], cache)
            q, cache_hit = parsed["request"], parsed["llm_cache_hit"]
        else:
            q, cache_hit = req["expected_parse"], ""
        out = matching.rank(q, props, blocked, args.matcher) if q else {"ranked_properties": [],
              "scoring_version": matching.SCORING_VERSION}
        refl = {}
        if args.reflect and q:
            out = reflection.reflect(out, q, props, blocked, reviews, cancels)
            refl = out["reflection"]
        ranked = out["ranked_properties"]
        rows.append({
            "request_id": req["request_id"],
            "config": config,
            "trace_id": "",
            "scoring_version": out["scoring_version"],
            "parser_cache_hit": cache_hit,
            "final_ranking": ";".join(r["property_id"] for r in ranked),
            "scores": ";".join(f"{r['score']:.4f}" for r in ranked),
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
            **({"reflection_decision": refl.get("decision", ""),
                "reflection_cycles": refl.get("cycles", ""),
                "reflection_changed_top1": refl.get("changed_top_1", "")} if args.reflect else {}),
        })

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"{config}.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {path}: {len(rows)} requests")


if __name__ == "__main__":
    main()
