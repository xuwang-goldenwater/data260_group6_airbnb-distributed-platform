"""AI dashboard data (assignment section 8, Role E).

Two sources, kept apart on the page:
  live       the AI service's own MongoDB records (ai_results, ai_events,
             ai_approvals) - every debate the running system has done
  benchmark  the fixed 50-request evaluation in benchmarks/results/ -
             precision@10, recall@10, human approval study, offline debates
Measures (section 8 "AI dashboard"): agent publication timeline, disagreement
size, challenge count, votes, dissent rate, precision@10, recall@10, human
approval categories. A trace_id filter gives one recommendation's journey.
Pure functions over a Store (MemoryStore or MongoStore), no extra service.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from .config import REPO_ROOT

RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"


# ---------------------------------------------------------------- reading the store
def _all(store, kind: str) -> list[dict[str, Any]]:
    """Every ai_results / ai_approvals document, for either store implementation."""
    if hasattr(store, "_db"):
        return [{k: v for k, v in d.items() if k != "_id"} for d in store._db[f"ai_{kind}"].find()]
    return list(getattr(store, kind).values())


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ---------------------------------------------------------------- shared aggregation
def debate_stats(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Disagreement, challenges, votes and dissent over a list of results."""
    done = [r for r in results if r.get("status") != "failed" and r.get("disagreement")]
    if not done:
        return {"debates": 0}
    with_votes = [r for r in done if r.get("votes")]
    votes = Counter()
    for r in done:
        votes.update(r.get("vote_summary") or {})
    return {
        "debates": len(done),
        "top1_distinct": dict(sorted(Counter(r["disagreement"]["top1_distinct"] for r in done).items())),
        "mean_top10_overlap": round(mean(r["disagreement"]["top10_overlap"] for r in done), 3),
        "challenges": sum(1 for r in done if (r.get("round1") or {}).get("challenger_property_id")),
        "votes": {k: votes.get(k, 0) for k in ("keep_original", "switch_to_challenger", "abstain")},
        "dissent_rate": round(sum(1 for r in with_votes if r.get("dissent")) / len(with_votes), 3) if with_votes else 0.0,
        "methods": dict(Counter(r["adjudication"]["method"] for r in done)),
    }


# ---------------------------------------------------------------- live
def live(store) -> dict[str, Any]:
    results = _all(store, "results")
    approvals = _all(store, "approvals")
    totals = [r["timings_s"].get("done") for r in results if (r.get("timings_s") or {}).get("done") is not None]
    return {
        "results": len(results),
        "by_status": dict(Counter(r.get("status") for r in results)),
        "by_config": dict(Counter(r.get("config") for r in results)),
        "debate": debate_stats(results),
        "approvals": {k: sum(1 for a in approvals if a["decision"] == k) for k in ("approved", "edited", "rejected")},
        "mean_latency_s": round(mean(totals), 3) if totals else None,
        "recent": [{"trace_id": r["trace_id"], "config": r.get("config"), "status": r.get("status"),
                    "recommendation": (r.get("recommendation") or {}).get("property_id"),
                    "method": (r.get("adjudication") or {}).get("method")}
                   for r in sorted(results, key=lambda r: r["trace_id"])[-20:]],
    }


def timeline(store, trace_id: str) -> dict[str, Any] | None:
    """One recommendation's journey: every stored event with its offset, plus the plan."""
    events = store.events(trace_id)
    result = store.get_result(trace_id)
    if not events and result is None:
        return None
    t0 = min((_ts(e["timestamp"]) for e in events), default=None)
    rows = []
    for e in sorted(events, key=lambda e: e["timestamp"]):
        p = e.get("payload") or {}
        agent = p.get("agent_id") or e.get("actor_id")
        detail = ""
        if e["event_type"].startswith("debate.round.0"):
            top = (p.get("ranked_properties") or [{}])[0].get("property_id")
            detail = f"top-1 {top}" if top else p.get("status", "")
        elif e["event_type"].startswith("debate.round.2"):
            detail = (p.get("vote") or {}).get("choice", "")
        elif e["event_type"] == "debate.round.1":
            detail = f"{p.get('original_property_id')} vs {p.get('challenger_property_id')}"
        elif e["event_type"] == "ai.results":
            detail = (p.get("recommendation") or {}).get("property_id") or p.get("status", "")
        rows.append({"offset_ms": round((_ts(e["timestamp"]) - t0).total_seconds() * 1000, 1),
                     "event_type": e["event_type"], "agent": agent, "detail": detail})
    return {"trace_id": trace_id, "events": rows,
            "plan": (result or {}).get("plan", []), "timings_s": (result or {}).get("timings_s", {}),
            "recommendation": ((result or {}).get("recommendation") or {}).get("property_id"),
            "adjudication": (result or {}).get("adjudication"),
            "human_decision": (result or {}).get("human_decision")}


# ---------------------------------------------------------------- benchmark
def benchmark(results_dir: Path = RESULTS_DIR) -> dict[str, Any]:
    out: dict[str, Any] = {}
    summary = results_dir / "summary.csv"
    if summary.exists():
        with summary.open(newline="") as f:
            rows = {r["config"]: r for r in csv.DictReader(f)}          # last run per config wins
        out["quality"] = [{"config": c, "precision_at_10": float(r["precision_at_10"]),
                           "recall_at_10": float(r["recall_at_10"]),
                           "precision_at_10_single": float(r.get("precision_at_10_single") or "nan"),
                           "mean_top_pick_rank": float(r["mean_top_pick_rank"]),
                           "top_pick_in_top10": int(r["top_pick_in_top10"]), "requests": int(r["requests"])}
                          for c, r in rows.items()]
        out["chance_precision_at_10"] = float(next(iter(rows.values()))["chance_precision_at_10"])
    for name, key in (("human_agreement.json", "human_ceiling"), ("approval_summary.json", "approval_study")):
        path = results_dir / name
        if path.exists():
            data = json.loads(path.read_text())
            out[key] = data.get("human_agreement_ceiling", data)
    traces = results_dir / "traces_full_debate.jsonl"
    if traces.exists():
        out["full_debate"] = debate_stats([json.loads(line) for line in traces.read_text().splitlines()])
    return out


def dashboard(store, trace_id: str | None = None) -> dict[str, Any]:
    data = {"live": live(store), "benchmark": benchmark()}
    if trace_id:
        data["trace"] = timeline(store, trace_id)
    return data
