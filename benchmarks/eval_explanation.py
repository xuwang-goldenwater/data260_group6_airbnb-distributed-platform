"""Before/after check for explanation-v2 on the fixed human-review set.

For the 30 reviewed recommendations (benchmarks/approval/feedback.jsonl):
  - edited reviews: is the reviewer's chosen listing shown to the guest
    (as the recommendation or in the explanation)? v1 vs v2
  - approved reviews: how many explanations change (extra text the reviewer
    did not ask for)
Rankings are not touched, so the benchmark metrics of every configuration
stay exactly the same.

Usage (from repo root):
    python3 benchmarks/eval_explanation.py
Output:
    benchmarks/results/explanation_v2_eval.json
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path("services/agents")))
import alternatives  # noqa: E402
import matching  # noqa: E402

FEEDBACK = Path("benchmarks/approval/feedback.jsonl")
TRACES = Path("benchmarks/results/traces_full_debate.jsonl")
OUT = Path("benchmarks/results/explanation_v2_eval.json")


def main():
    fb = [json.loads(line) for line in FEEDBACK.read_text().splitlines()]
    traces = {json.loads(line)["request_id"]: json.loads(line) for line in TRACES.read_text().splitlines()}
    props, blocked = matching.load_seed()
    cases = []
    for f in fb:
        t = traces[f["request_id"]]
        alt = alternatives.entire_place_alternative(t["parse"], t["recommendation"], props, blocked)
        shown_v1 = {t["recommendation"]}
        shown_v2 = shown_v1 | ({alt["property_id"]} if alt else set())
        cases.append({"review_id": f["review_id"], "request_id": f["request_id"], "decision": f["decision"],
                      "reviewer_pick": f["selected_property_id"],
                      "alternative_v2": alt["property_id"] if alt else None,
                      "pick_shown_v1": f["selected_property_id"] in shown_v1,
                      "pick_shown_v2": f["selected_property_id"] in shown_v2})
    edited = [c for c in cases if c["decision"] == "edited"]
    approved = [c for c in cases if c["decision"] == "approved"]
    summary = {
        "explanation_version": alternatives.EXPLANATION_VERSION,
        "edited_reviews": len(edited),
        "reviewer_pick_shown_v1": sum(c["pick_shown_v1"] for c in edited),
        "reviewer_pick_shown_v2": sum(c["pick_shown_v2"] for c in edited),
        "edited_with_any_alternative_v2": sum(c["alternative_v2"] is not None for c in edited),
        "approved_reviews": len(approved),
        "approved_explanations_changed_v2": sum(c["alternative_v2"] is not None for c in approved),
        "ranking_changed": False,
        "cases": cases,
    }
    OUT.write_text(json.dumps(summary, indent=2) + "\n")
    for k, v in summary.items():
        if k != "cases":
            print(f"{k:>34}: {v}")
    for c in edited:
        print(f"  {c['review_id']} {c['request_id']}: your pick {c['reviewer_pick']}, v2 shows {c['alternative_v2']}")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
