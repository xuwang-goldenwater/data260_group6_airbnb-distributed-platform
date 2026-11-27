"""Adjudicate the two independent benchmark labelings (assignment 7.3.1).

Step 1 (default): build the adjudication sheet.
  For every property, average its rank across labelers (not in someone's
  top 10 = rank 11). The suggested final top 10 is the 10 best averages.
  Only requests whose labelers picked a different #1 go into the sheet;
  the labelers discuss those and fill `final_top1`.
Step 2 (--finalize): read the filled sheet, write labels_final.csv
  (final #1 + next 9 by average rank) and the human agreement ceiling.

Usage (from repo root):
    python3 benchmarks/adjudicate.py
    # fill final_top1 in benchmarks/labeling/adjudication.csv together
    python3 benchmarks/adjudicate.py --finalize
Labelers default to every benchmarks/labeling/labels_*.csv except labels_final.csv.
"""
import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path("benchmarks")))
from make_label_sheet import complaint_summary  # noqa: E402

LABEL_DIR = Path("benchmarks/labeling")
SHEET = LABEL_DIR / "adjudication.csv"
FINAL = LABEL_DIR / "labels_final.csv"
AGREEMENT = Path("benchmarks/results/human_agreement.json")
SEED_DIR = Path("data/seed/generated")
K = 10
MISSING = K + 1


def read_labels(path):
    """request_id -> list of (rank, property_id); duplicates and gaps are kept."""
    out = defaultdict(list)
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r["rank"].strip():
                out[r["request_id"]].append((int(float(r["rank"])), r["property_id"]))
    return {rid: sorted(v) for rid, v in out.items()}


def top1(pairs):
    ones = [pid for rank, pid in pairs if rank == 1]
    return ones[0] if len(ones) == 1 else None          # missing or duplicated #1 -> no top pick


def average_order(labels, rid):
    """Properties in anyone's top 10, ordered by mean rank (absent = 11)."""
    ranks = [{pid: rank for rank, pid in lab.get(rid, [])} for lab in labels.values()]
    pool = set().union(*ranks)
    stats = {pid: (statistics.mean(r.get(pid, MISSING) for r in ranks),
                   min(r.get(pid, MISSING) for r in ranks)) for pid in pool}
    return sorted(pool, key=lambda p: (stats[p][0], stats[p][1], p)), stats


def build_sheet(labels, requests):
    props = {p["property_id"]: p for p in json.loads((SEED_DIR / "properties.json").read_text())}
    complaints = complaint_summary(json.loads((SEED_DIR / "reviews.json").read_text()))
    names = list(labels)
    rows, agreed = [], 0
    for rid in sorted(requests):
        tops = {n: top1(labels[n].get(rid, [])) for n in names}
        order, stats = average_order(labels, rid)
        if len(set(tops.values())) == 1 and None not in tops.values():
            agreed += 1
            continue
        options = list(dict.fromkeys([*[t for t in tops.values() if t], order[0]]))

        def describe(pid):
            p = props[pid]
            ranks = ", ".join(f"{n} #{dict((q, r) for r, q in labels[n].get(rid, [])).get(pid, '-')}" for n in names)
            rating = p["rating"] if p["review_count"] else "new"
            return (f"{pid} ({ranks}; {p['property_type']}, ${p['nightly_price']:.0f}, "
                    f"rating {rating}, {p['review_count']} reviews"
                    f"{', ' + complaints[pid] if complaints.get(pid) else ''})")

        row = {"request_id": rid, "request_text": requests[rid]}
        row.update({f"{n}_top1": tops[n] or "(none)" for n in names})
        row["suggested_top1"] = order[0]
        row["options"] = " | ".join(describe(p) for p in options)
        row["final_top1"] = ""
        row["note"] = ""
        rows.append(row)
    with SHEET.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"{agreed} requests: all labelers chose the same #1 (no discussion needed)")
    print(f"Wrote {SHEET}: {len(rows)} requests to discuss - fill final_top1, then run --finalize")


def finalize(labels, requests):
    names = list(labels)
    decided = {}
    with SHEET.open(encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            decided[r["request_id"]] = (r["final_top1"].strip(), r["note"].strip())
    missing = [rid for rid, (pid, _) in decided.items() if not pid]
    if missing:
        sys.exit(f"final_top1 is empty for: {', '.join(missing)}")

    final_rows, per = [], []
    for rid in sorted(requests):
        order, _ = average_order(labels, rid)
        first = decided[rid][0] if rid in decided else top1(next(iter(labels.values()))[rid])
        if first not in order:
            sys.exit(f"{rid}: final_top1 {first} is not in any labeler's top 10")
        final = [first] + [p for p in order if p != first][:K - 1]
        final_rows += [{"request_id": rid, "rank": i, "property_id": p} for i, p in enumerate(final, 1)]

        tops = {n: [pid for _, pid in labels[n].get(rid, [])] for n in names}
        union = set().union(*map(set, tops.values()))
        rec = {"request_id": rid}
        for n in names:
            others = set().union(*(set(tops[m]) for m in names if m != n))
            ranked = [pid for _, pid in labels[n].get(rid, [])]
            rec[n] = {
                "p10_vs_other_labeler": len(set(tops[n]) & others) / K,
                "r10_vs_union": len(set(tops[n]) & union) / len(union),
                "final_top1_rank": ranked.index(first) + 1 if first in ranked else MISSING,
                "same_top1_as_final": top1(labels[n].get(rid, [])) == first,
            }
        per.append(rec)

    with FINAL.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["request_id", "rank", "property_id"])
        w.writeheader()
        w.writerows(final_rows)

    def mean(key, n):
        return round(statistics.mean(float(r[n][key]) for r in per), 4)

    by_labeler = {n: {k: mean(k, n) for k in per[0][n]} for n in names}
    ceiling = {k: round(statistics.mean(by_labeler[n][k] for n in names), 4) for k in per[0][names[0]]}
    summary = {"labelers": names, "requests": len(per), "discussed": len(decided),
               "human_agreement_ceiling": ceiling, "by_labeler": by_labeler,
               "notes": {rid: note for rid, (_, note) in decided.items() if note}}
    AGREEMENT.parent.mkdir(parents=True, exist_ok=True)
    AGREEMENT.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote {FINAL} ({len(per)} requests x {K}) and {AGREEMENT}")
    print("human agreement ceiling (mean over labelers):")
    for k, v in ceiling.items():
        print(f"  {k:>22}: {v}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--finalize", action="store_true")
    args = ap.parse_args()
    files = sorted(p for p in LABEL_DIR.glob("labels_*.csv") if p.name != FINAL.name)
    labels = {p.stem.removeprefix("labels_"): read_labels(p) for p in files}
    requests = {r["request_id"]: r["request_text"]
                for r in json.loads(Path("benchmarks/requests.json").read_text())["requests"]}
    print(f"labelers: {', '.join(labels)}")
    (finalize if args.finalize else build_sheet)(labels, requests)


if __name__ == "__main__":
    main()
