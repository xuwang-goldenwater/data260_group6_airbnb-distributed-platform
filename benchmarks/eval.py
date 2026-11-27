"""Score one configuration's rankings against the human labels (assignment 7.3.1).

Metrics per request, then averaged:
  precision@10 = hits / 10
  recall@10    = hits / |relevant|
  top_pick_rank = position of the human top choice in the system's top 10
                  (11 if it is not in the top 10)
where relevant = union of every labeler's top-10 for that request, and the
human top choice = rank 1 in the adjudicated file (or, before adjudication,
rank 1 of the first labeler; marked provisional).

precision_at_10_single = hits against ONE labeler's top 10 / 10, averaged over
labelers; this is the number comparable to the human agreement ceiling
(one labeler scored against the other, see adjudicate.py).

chance_precision_at_10 = expected precision of 10 properties picked at random
from the request's candidates (= |relevant| / |candidates|). Candidate sets are
small (11-29), so this is the floor to compare against, not zero.

Usage (from repo root):
    python3 benchmarks/eval.py benchmarks/results/baseline.csv \
        --labels benchmarks/labeling/labels_xu.csv benchmarks/labeling/labels_carol.csv \
        [--final benchmarks/labeling/labels_final.csv]
Output:
    benchmarks/results/eval_<config>.csv   per-request metrics
    benchmarks/results/summary.csv         one summary row appended per run
"""
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

RESULTS_DIR = Path("benchmarks/results")
REQUESTS = Path("benchmarks/requests.json")
K = 10
MISSING_RANK = K + 1


def read_labels(path):
    """request_id -> {rank: property_id} for rows that have a rank."""
    out = defaultdict(dict)
    with open(path, encoding="utf-8-sig", newline="") as f:   # tolerate Excel's BOM
        for r in csv.DictReader(f):
            if r["rank"].strip():
                rank = int(float(r["rank"]))                  # Excel may write "5.0"
                out[r["request_id"]].setdefault(rank, r["property_id"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", help="result CSV of one configuration")
    ap.add_argument("--labels", nargs="+", required=True, help="one CSV per labeler")
    ap.add_argument("--final", help="adjudicated labels (gives the human top choice)")
    args = ap.parse_args()

    labelers = [read_labels(p) for p in args.labels]
    top_source = read_labels(args.final) if args.final else labelers[0]
    provisional = args.final is None

    with open(args.results, newline="") as f:
        results = list(csv.DictReader(f))
    config = results[0]["config"]
    n_cand = {q["request_id"]: q["candidate_count"]
              for q in json.loads(REQUESTS.read_text())["requests"]}

    rows = []
    for r in results:
        rid = r["request_id"]
        system = r["final_ranking"].split(";")[:K]
        relevant = set()
        for lab in labelers:
            relevant |= set(lab.get(rid, {}).values())
        if not relevant:
            continue                                         # request not labeled yet
        hits = len(set(system) & relevant)
        top = top_source.get(rid, {}).get(1)
        top_rank = system.index(top) + 1 if top in system else MISSING_RANK
        rows.append({
            "request_id": rid,
            "config": config,
            "hits": hits,
            "relevant_size": len(relevant),
            "precision_at_10": round(hits / K, 4),
            "recall_at_10": round(hits / len(relevant), 4),
            "precision_at_10_single": round(sum(len(set(system) & set(lab.get(rid, {}).values()))
                                                for lab in labelers) / (K * len(labelers)), 4),
            "chance_precision_at_10": round(min(len(relevant), n_cand[rid]) / n_cand[rid], 4),
            "human_top_choice": top or "",
            "top_pick_rank": top_rank if top else "",
        })

    def mean(key):
        vals = [r[key] for r in rows if r[key] != ""]
        return round(sum(vals) / len(vals), 4) if vals else None

    summary = {
        "run_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "config": config,
        "scoring_version": results[0].get("scoring_version", ""),
        "requests": len(rows),
        "labelers": len(labelers),
        "precision_at_10": mean("precision_at_10"),
        "recall_at_10": mean("recall_at_10"),
        "precision_at_10_single": mean("precision_at_10_single"),
        "chance_precision_at_10": mean("chance_precision_at_10"),
        "mean_top_pick_rank": mean("top_pick_rank"),
        "top_pick_in_top10": sum(1 for r in rows if r["top_pick_rank"] not in ("", MISSING_RANK)),
        "top_choice_source": "provisional (first labeler)" if provisional else "adjudicated",
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    per_request = RESULTS_DIR / f"eval_{config}.csv"
    with per_request.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    summary_path = RESULTS_DIR / "summary.csv"
    new = not summary_path.exists()
    with summary_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary.keys()))
        if new:
            w.writeheader()
        w.writerow(summary)

    for k, v in summary.items():
        print(f"{k:>20}: {v}")
    print(f"Wrote {per_request} and appended to {summary_path}")


if __name__ == "__main__":
    main()
