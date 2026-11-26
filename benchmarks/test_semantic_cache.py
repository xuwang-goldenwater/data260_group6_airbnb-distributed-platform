"""Evaluate the semantic cache (no LLM needed, only embeddings).

1. Load the 50 benchmark requests into a fresh semantic cache with their
   expected parse.
2. Build three variant sets from each request's structured fields:
     paraphrase - same constraints, different sentence template -> SHOULD hit
     near_miss  - original template, ONE hard field changed (budget, dates,
                  amenity or property type)                     -> must NOT hit
     role_swap  - required amenities restated as optional, same words
                  (known limit: the hard-token gate cannot see this) -> must NOT hit
3. For several thresholds report hits and wrong reuses (a hit whose reused
   parse differs from the variant's true parse), with and without the
   hard-constraint gate. A near-miss can legitimately hit when the changed
   request equals another cached request (e.g. R48 with type apartment = R45).
4. Time an embedding lookup vs a real Parser (LLM) call on a small sample.

Usage (from repo root, Ollama running with nomic-embed-text and qwen2.5:3b):
    python3 benchmarks/test_semantic_cache.py [--llm-sample 5]
Output:
    benchmarks/results/semantic_cache_eval.json   summary per threshold
    benchmarks/results/semantic_cache_cases.csv   one row per variant
"""
import argparse
import csv
import json
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path("services/agents")))
sys.path.insert(0, str(Path("benchmarks")))
import make_requests  # noqa: E402  (reuse the original sentence templates)
import parser as parser_agent  # noqa: E402
import semantic_cache as sc  # noqa: E402

REQUESTS = Path("benchmarks/requests.json")
RESULTS_DIR = Path("benchmarks/results")
THRESHOLDS = [0.85, 0.90, 0.92, 0.95, 0.98]

PARAPHRASE = {
    "leisure": "Looking for a place in {city} for {g} of us, checking in {ci} and leaving {co}. We can pay up to ${b} per night{am}{rt}. Ideally {pt}.",
    "business": "Work trip: {city}, {ci} through {co}, {gs}. Nightly max ${b}{am}{rt}. Would prefer {pt}.",
    "group": "There are {g} of us heading to {city} ({ci} to {co}). Nightly cap ${b}{am}{rt}. Hoping for {pt}.",
    "family_visit": "Coming to {city} to see relatives, {ci} until {co}, party of {g}. No more than ${b} a night{am}{rt}. We'd like {pt}.",
}


def paraphrase(purpose, q):
    am = (", must have " + " and ".join(q["required_amenities"])) if q["required_amenities"] else ""
    rt = f", minimum rating {q['min_rating']}" if q["min_rating"] else ""
    return PARAPHRASE[purpose].format(
        city=q["city"], g=q["guests"], gs="just me" if q["guests"] == 1 else f"{q['guests']} of us",
        ci=q["check_in"], co=q["check_out"], b=q["max_nightly_price"], am=am, rt=rt,
        pt=make_requests.TYPE_PHRASE[q["preferred_property_type"]])


def near_miss(i, q):
    """Change exactly one hard field; which one rotates with the request index."""
    v = dict(q, required_amenities=list(q["required_amenities"]))
    kind = ["budget", "dates", "amenity", "type"][i % 4]
    if kind == "budget":
        v["max_nightly_price"] += 30
    elif kind == "dates":
        shift = lambda d: (date.fromisoformat(d) + timedelta(1)).isoformat()  # noqa: E731
        v["check_in"], v["check_out"] = shift(q["check_in"]), shift(q["check_out"])
    elif kind == "amenity":
        extra = [a for a in make_requests.AMENITIES if a not in v["required_amenities"]][0]
        v["required_amenities"] = sorted(v["required_amenities"] + [extra])
    else:
        others = [t for t in make_requests.TYPES if t != q["preferred_property_type"]
                  and (t != "room" or q["guests"] <= 2)]
        v["preferred_property_type"] = others[0]
    return kind, v


def role_swap(text, q):
    """Required amenities restated as optional: same words, different meaning."""
    need = ", and we need " + " and ".join(q["required_amenities"])
    nice = ", and " + " and ".join(q["required_amenities"]) + " would be nice but aren't required"
    return text.replace(need, nice)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--llm-sample", type=int, default=5, help="real Parser calls to time")
    args = ap.parse_args()
    reqs = json.loads(REQUESTS.read_text())["requests"]

    # 1. fresh cache with the 50 originals (never saved to disk)
    cache = sc.SemanticCache(path=RESULTS_DIR / "_unused.json")
    cache.entries = []
    t0 = time.perf_counter()
    for r in reqs:
        cache.add(r["request_text"], r["expected_parse"])
    embed_ms = (time.perf_counter() - t0) * 1000 / len(reqs)
    print(f"cached {len(reqs)} requests ({embed_ms:.0f} ms per embedding)")

    # 2. variants
    variants = []
    for i, r in enumerate(reqs):
        q = r["expected_parse"]
        variants.append(("paraphrase", "-", r, paraphrase(r["purpose"], q), q))
        kind, v = near_miss(i, q)
        variants.append(("near_miss", kind, r, make_requests.text_for(r["purpose"], v), v))
        if q["required_amenities"]:
            variants.append(("role_swap", "-", r, role_swap(r["request_text"], q),
                             dict(q, required_amenities=[])))

    rows, lookup_ms = [], []
    for group, kind, r, text, truth in variants:
        t1 = time.perf_counter()
        hit_q, d = cache.lookup(text)
        lookup_ms.append((time.perf_counter() - t1) * 1000)
        matched = next(e for e in cache.entries if e["text"] == d["matched_text"])
        rows.append({
            "group": group, "change": kind, "source_request": r["request_id"],
            "text": text, "similarity": d["similarity"],
            "matched_request": next(x["request_id"] for x in reqs if x["request_text"] == d["matched_text"]),
            "hard_gate": d["hard_gate"],
            "reused_parse_correct": matched["request"] == truth,
        })

    # 3. metrics per threshold
    summary = {"embedding_model": sc.EMBED_MODEL, "cached_requests": len(reqs),
               "variants": {g: sum(r["group"] == g for r in rows) for g in ("paraphrase", "near_miss", "role_swap")},
               "thresholds": {}}
    for th in THRESHOLDS:
        res = {}
        for g in ("paraphrase", "near_miss", "role_swap"):
            grp = [r for r in rows if r["group"] == g]
            hits = [r for r in grp if r["similarity"] >= th and r["hard_gate"]]
            sem_only = [r for r in grp if r["similarity"] >= th]
            res[g] = {"hits": len(hits), "hit_rate": round(len(hits) / len(grp), 3),
                      "wrong_parse_reused": sum(not r["reused_parse_correct"] for r in hits),
                      "hits_without_hard_gate": len(sem_only),
                      "wrong_without_hard_gate": sum(not r["reused_parse_correct"] for r in sem_only)}
        summary["thresholds"][str(th)] = res

    # 4. latency: lookup vs one real model call (no cache is read or written)
    llm_ms = []
    for r in reqs[:args.llm_sample]:
        messages = [{"role": "system", "content": parser_agent.SYSTEM_PROMPT},
                    {"role": "user", "content": f"Message: {paraphrase(r['purpose'], r['expected_parse'])}"}]
        t2 = time.perf_counter()
        parser_agent.call_ollama(messages)
        llm_ms.append((time.perf_counter() - t2) * 1000)
    summary["latency_ms"] = {"semantic_lookup_mean": round(statistics.mean(lookup_ms), 1),
                             "llm_parse_mean": round(statistics.mean(llm_ms), 1) if llm_ms else None,
                             "llm_sample": len(llm_ms)}

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "semantic_cache_eval.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (RESULTS_DIR / "semantic_cache_cases.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    nv = summary["variants"]
    print(f"\n{'threshold':>9} | {'paraphrase correct hits':>23} | {'near-miss wrong reuse':>21} "
          f"| {'role-swap wrong reuse':>21} | {'near-miss wrong w/o hard gate':>29}")
    for th, res in summary["thresholds"].items():
        p, n, s = res["paraphrase"], res["near_miss"], res["role_swap"]
        print(f"{th:>9} | {p['hits'] - p['wrong_parse_reused']:>17}/{nv['paraphrase']} "
              f"| {n['wrong_parse_reused']:>18}/{nv['near_miss']} "
              f"| {s['wrong_parse_reused']:>18}/{nv['role_swap']} "
              f"| {n['wrong_without_hard_gate']:>26}/{nv['near_miss']}")
    lat = summary["latency_ms"]
    print(f"\nlatency: semantic lookup {lat['semantic_lookup_mean']} ms vs LLM parse "
          f"{lat['llm_parse_mean']} ms (n={lat['llm_sample']})")
    print("Wrote benchmarks/results/semantic_cache_eval.json and semantic_cache_cases.csv")


if __name__ == "__main__":
    main()
