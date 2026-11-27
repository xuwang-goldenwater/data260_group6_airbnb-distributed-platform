"""Summarize the human review of 30 recommendations (assignment 7.3.2) and
turn each decision into a feedback record for self-improvement (7.5.4).

Checks the filled sheet, then writes:
  benchmarks/approval/feedback.jsonl     one record per review, format of
                                         docs/api/agent_payloads.md section 8.2
  benchmarks/results/approval_summary.json
      approval rate, decisions, reason categories, and edit patterns
      (recommended type -> chosen type)

Usage (from repo root):
    python3 benchmarks/approval_report.py [--reviewer xu]
"""
import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SHEET = Path("benchmarks/approval/review_sheet.csv")
TRACES = Path("benchmarks/results/traces_full_debate.jsonl")
FEEDBACK = Path("benchmarks/approval/feedback.jsonl")
SUMMARY = Path("benchmarks/results/approval_summary.json")
SEED_DIR = Path("data/seed/generated")
DECISIONS = {"approved", "edited", "rejected"}
CATEGORIES = {"price", "location", "amenities", "risk", "fairness", "explanation", "other"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reviewer", default="xu")
    args = ap.parse_args()

    with SHEET.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    traces = {json.loads(line)["request_id"]: json.loads(line) for line in TRACES.read_text().splitlines()}
    props = {p["property_id"]: p for p in json.loads((SEED_DIR / "properties.json").read_text())}
    candidates = {r["request_id"]: set(r["candidate_property_ids"])
                  for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}

    # 1. check the sheet
    problems = []
    for r in rows:
        d, cat = r["decision"].strip().lower(), r["reason_category"].strip().lower()
        sel = r["selected_property_id"].strip()
        if d not in DECISIONS:
            problems.append(f"{r['review_id']}: decision must be one of {sorted(DECISIONS)}")
        if d in ("edited", "rejected"):
            if cat not in CATEGORIES:
                problems.append(f"{r['review_id']}: reason_category must be one of {sorted(CATEGORIES)}")
            if not r["reason"].strip():
                problems.append(f"{r['review_id']}: reason is required for {d}")
        if sel and sel not in candidates[r["request_id"]]:
            problems.append(f"{r['review_id']}: {sel} is not a valid candidate for {r['request_id']}")
    if problems:
        sys.exit("Fix the sheet first:\n  " + "\n  ".join(problems))

    # 2. feedback records (agent_payloads.md 8.2)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records = []
    for r in rows:
        t = traces[r["request_id"]]
        d = r["decision"].strip().lower()
        records.append({
            "review_id": r["review_id"],
            "request_id": r["request_id"],
            "trace_id": t["trace_id"],
            "recommended_property_id": t["recommendation"],
            "decision": d,
            "selected_property_id": r["selected_property_id"].strip() or None,
            "reason": r["reason"].strip() or None,
            "reason_category": r["reason_category"].strip().lower() or None,
            "scoring_version": t["scoring_versions"]["matching"],
            "config": t["config"],
            "reviewer": args.reviewer,
            "reviewer_role": "guest",
            "created_at": now,
        })
    FEEDBACK.write_text("".join(json.dumps(x) + "\n" for x in records))

    # 3. summary
    n = len(records)
    decisions = Counter(x["decision"] for x in records)
    edits = [x for x in records if x["decision"] == "edited" and x["selected_property_id"]]
    patterns = Counter(f"{props[x['recommended_property_id']]['property_type']} -> "
                       f"{props[x['selected_property_id']]['property_type']}" for x in edits)
    price_delta = [props[x["selected_property_id"]]["nightly_price"] - props[x["recommended_property_id"]]["nightly_price"]
                   for x in edits]
    summary = {
        "reviewed": n,
        "reviewer": args.reviewer,
        "approval_rate": round(decisions["approved"] / n, 4),
        "decisions": {k: decisions[k] for k in ("approved", "edited", "rejected")},
        "reason_categories": dict(Counter(x["reason_category"] for x in records if x["reason_category"])),
        "edit_patterns": dict(patterns),
        "edit_price_change_mean": round(sum(price_delta) / len(price_delta), 2) if price_delta else None,
        "reasons": dict(Counter(x["reason"] for x in records if x["reason"])),
        "scoring_version": records[0]["scoring_version"],
    }
    SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(json.dumps(summary, indent=2) + "\n")

    print(f"{n} reviewed by {args.reviewer}: approval rate {summary['approval_rate']:.0%}")
    for k, v in summary["decisions"].items():
        print(f"  {k:>9}: {v}")
    if patterns:
        print("edit patterns:", dict(patterns), f"(mean price change ${summary['edit_price_change_mean']})")
    print(f"Wrote {FEEDBACK} and {SUMMARY}")


if __name__ == "__main__":
    main()
