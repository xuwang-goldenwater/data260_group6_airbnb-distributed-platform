"""Run the Matching agent on all 50 benchmark requests.

Writes the same CSV format as baseline.csv so eval.py can score it.
For now the request is the expected (ground-truth) parse; once the Parser
exists, --parse parser will use its output instead (that is the real
single-agent configuration).

Usage (from repo root):
    python3 benchmarks/run_matcher.py
Output:
    benchmarks/results/matcher_expected_parse.csv
"""
import argparse
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path("services/agents")))
import matching  # noqa: E402

REQUESTS = Path("benchmarks/requests.json")
RESULTS_DIR = Path("benchmarks/results")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parse", choices=["expected"], default="expected",
                    help="where the structured request comes from")
    args = ap.parse_args()
    config = f"matcher_{args.parse}_parse"

    bench = json.loads(REQUESTS.read_text())
    props, blocked = matching.load_seed()

    rows = []
    for req in bench["requests"]:
        t0 = time.perf_counter()
        out = matching.rank(req["expected_parse"], props, blocked)
        ranked = out["ranked_properties"]
        rows.append({
            "request_id": req["request_id"],
            "config": config,
            "trace_id": "",
            "scoring_version": out["scoring_version"],
            "final_ranking": ";".join(r["property_id"] for r in ranked),
            "scores": ";".join(f"{r['score']:.4f}" for r in ranked),
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
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
