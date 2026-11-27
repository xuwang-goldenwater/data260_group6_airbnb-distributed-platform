"""Build the human-approval sheet for 30 recommendations (assignment 7.3.2).

Samples 30 of the 50 full-debate results with a fixed seed. For each one the
reviewer sees the request, the recommended listing, the next two options, the
system's explanation (Role D's Explanation template, the text a guest would
see), risk notes and Fairness Auditor objections.

The reviewer fills:
  decision              approved | edited | rejected
  selected_property_id  the listing chosen instead (edited only)
  reason_category       price | location | amenities | risk | fairness | explanation | other
  reason               free text (required for edited / rejected)

Usage (from repo root):
    python3 benchmarks/make_review_sheet.py              # sheet + candidates
    python3 benchmarks/make_review_sheet.py --candidates-only
Output:
    benchmarks/approval/review_sheet.csv   (not overwritten if already filled)
    benchmarks/approval/candidates.csv     every valid candidate of the 30
        reviewed requests, cheapest first - where the reviewer finds
        selected_property_id
"""
import csv
import json
from collections import Counter
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path("benchmarks")))
sys.path.insert(0, str(Path("services/ai/ai_core")))
from make_label_sheet import complaint_summary  # noqa: E402
import explanation  # noqa: E402  (Role D's Explanation step)

TRACES = Path("benchmarks/results/traces_full_debate.jsonl")
SEED_DIR = Path("data/seed/generated")
OUT = Path("benchmarks/approval/review_sheet.csv")
CANDIDATES = Path("benchmarks/approval/candidates.csv")
N, SEED = 30, 260


def facts(p, complaints):
    rating = f"rating {p['rating']:.2f} ({p['review_count']} reviews)" if p["review_count"] else "new listing, no reviews"
    c = complaints.get(p["property_id"])
    return (f"{p['property_id']}: {p['property_type']}, ${p['nightly_price']:.0f}/night, sleeps {p['capacity']}, "
            f"{p['bedrooms']} bedroom(s), {rating}, amenities: {', '.join(p['amenities'])}"
            f"{'; complaints: ' + c if c else ''}")


def write_candidates(picked, requests_full, props, complaints, by_id):
    rows = []
    for i, rid in enumerate(picked, start=1):
        rec = by_id[rid]["recommendation"]
        for pid in sorted(requests_full[rid]["candidate_property_ids"],
                          key=lambda x: (props[x]["nightly_price"], x)):
            p = props[pid]
            rows.append({"review_id": f"AP{i:02d}", "request_id": rid, "property_id": pid,
                         "is_recommended": "YES" if pid == rec else "",
                         "property_type": p["property_type"],
                         "entire_place": "no (shared)" if p["property_type"] == "room" else "yes",
                         "nightly_price": int(p["nightly_price"]), "sleeps": p["capacity"],
                         "bedrooms": p["bedrooms"],
                         "rating": round(p["rating"], 2) if p["review_count"] else "new",
                         "review_count": p["review_count"], "amenities": ", ".join(p["amenities"]),
                         "complaints": complaints.get(pid, "")})
    with CANDIDATES.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {CANDIDATES}: {len(rows)} candidates")


def main():
    candidates_only = "--candidates-only" in sys.argv
    traces = [json.loads(line) for line in TRACES.read_text().splitlines()]
    props = {p["property_id"]: p for p in json.loads((SEED_DIR / "properties.json").read_text())}
    complaints = complaint_summary(json.loads((SEED_DIR / "reviews.json").read_text()))
    requests_full = {r["request_id"]: r for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    requests = {rid: r["request_text"] for rid, r in requests_full.items()}

    picked = sorted(random.Random(SEED).sample(sorted(t["request_id"] for t in traces), N))
    by_id = {t["request_id"]: t for t in traces}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(picked, requests_full, props, complaints, by_id)
    if candidates_only:
        return
    if OUT.exists() and any(r["decision"].strip() for r in csv.DictReader(OUT.open(encoding="utf-8-sig"))):
        sys.exit(f"{OUT} already has decisions; not overwriting it")
    rows = []
    for i, rid in enumerate(picked, start=1):
        t = by_id[rid]
        winner = t["recommendation"]
        ranking = t["final_ranking"]
        flags = [f for f in t["round0"]["risk"]["risk_flags"] if f["property_id"] == winner]
        entry = next((r for r in t["round0"]["matching"]["ranked_properties"] if r["property_id"] == winner),
                     {"property_id": winner, "reasons": []})
        runner_up = {"property_id": ranking[1]} if len(ranking) > 1 else None
        ctx = explanation.context(props[winner], entry, runner_up, flags, t["dissent"], [])
        objections = [f["objection"] for f in t.get("audit_findings", [])
                      if winner in f["affected_property_ids"]]
        rows.append({
            "review_id": f"AP{i:02d}",
            "request_id": rid,
            "trace_id": t["trace_id"],
            "request_text": requests[rid],
            "recommended": facts(props[winner], complaints),
            "option_2": facts(props[ranking[1]], complaints) if len(ranking) > 1 else "",
            "option_3": facts(props[ranking[2]], complaints) if len(ranking) > 2 else "",
            "explanation": explanation.template_text(ctx),
            "risk_notes": "; ".join(f"[{sev}] {detail}" + (f" x{n}" if n > 1 else "")
                                    for (sev, detail), n in Counter((f["severity"], f["detail"]) for f in flags).items()),
            "fairness_objection": " | ".join(objections),
            "decision": "",
            "selected_property_id": "",
            "reason_category": "",
            "reason": "",
        })

    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {OUT}: {len(rows)} recommendations to review")


if __name__ == "__main__":
    main()
