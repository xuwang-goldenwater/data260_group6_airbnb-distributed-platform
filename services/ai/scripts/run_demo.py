"""★ Demo scenarios for Role D, with evidence files for the report.

    python services/ai/scripts/run_demo.py three-debates  --launch      # task 4.8
    python services/ai/scripts/run_demo.py agent-timeout  --launch      # task 4.7
    python services/ai/scripts/run_demo.py invalid-output --launch      # task 5.4 (harness)
    python services/ai/scripts/run_demo.py latency        --launch      # task 5.4 (p50 / p95)

Needs Kafka and MongoDB (`docker compose -f infra/docker-compose.yml up -d kafka mongodb`).
--launch starts the API, the Chair and the four agent workers itself (and stops them afterwards),
with the slow or broken agent the scenario needs. Without --launch it talks to processes you already
started; then JWT_SECRET must match the API's.

Each run writes reports/perf/ai/<scenario>.json and exits 1 if an expectation failed.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

AI_DIR = Path(__file__).resolve().parents[1]
REPO = AI_DIR.parents[1]
sys.path.insert(0, str(AI_DIR))

from ai_core.agents_bridge import chair_rules, round0_payload  # noqa: E402
from ai_core.auth import make_token                            # noqa: E402
from ai_core.config import (T_APPROVALS, T_DLQ, T_PARSED, T_REQUESTS, T_RESULTS, T_ROUND0,  # noqa: E402
                            T_ROUND1, T_ROUND2, EXPECTED_AGENTS, Settings)

BENCH = {r["request_id"]: r for r in json.loads((REPO / "benchmarks/requests.json").read_text())["requests"]}
OUT_DIR = REPO / "reports" / "perf" / "ai"
ALL_TOPICS = [T_REQUESTS, T_PARSED, T_ROUND0, T_ROUND1, T_ROUND2, T_RESULTS, T_APPROVALS, T_DLQ]


# ------------------------------------------------------------------ processes
def launch(scenario: str, port: int, timeout_s: float) -> list[subprocess.Popen]:
    env = {**os.environ, "PYTHONPATH": str(AI_DIR), "AI_ROUND_TIMEOUT_S": str(timeout_s)}
    env.setdefault("JWT_SECRET", "dev-secret-change-me-0123456789abcdef")
    env.setdefault("AI_EXPLAIN_MODE", "llm")
    os.environ["JWT_SECRET"] = env["JWT_SECRET"]
    from ai_workers.kafka_io import ensure_topics
    from ai_workers.kafka_io import KafkaUnavailable
    print("waiting for Kafka ...", flush=True)
    try:
        created = ensure_topics(Settings(kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "localhost:9092")), ALL_TOPICS)
    except KafkaUnavailable as exc:
        sys.exit(f"{exc}. Is Docker Desktop running? Then: docker compose -f infra/docker-compose.yml up -d kafka mongodb")
    if created:
        print("created topics:", ", ".join(created))
    fairness = ["fairness"]
    if scenario == "agent-timeout":
        fairness += ["--delay-s", str(timeout_s + 5)]
    if scenario == "invalid-output":
        fairness += ["--corrupt"]
    cmds = [[sys.executable, "-m", "uvicorn", "ai_api.app:app", "--port", str(port), "--log-level", "warning"],
            [sys.executable, "-m", "ai_workers.chair_worker"]]
    cmds += [[sys.executable, "-m", "ai_workers.agent_worker", *a] for a in (["parser"], ["matching"], ["risk"], fairness)]
    log_dir = OUT_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / ".gitignore").write_text("*\n")          # process logs stay local
    procs = []
    for cmd in cmds:
        name = cmd[3] if cmd[2] == "ai_workers.agent_worker" else ("api" if "uvicorn" in cmd else "chair")
        log = open(log_dir / f"{scenario}-{name}.log", "w")
        procs.append(subprocess.Popen(cmd, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT))
    api = f"http://localhost:{port}"
    for _ in range(60):
        try:
            if httpx.get(f"{api}/ai/health", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    else:
        stop(procs)
        sys.exit(f"API did not come up; see {log_dir}")
    warm_up(api, scenario)
    return procs


def warm_up(api: str, scenario: str) -> None:
    """One request that is not measured: it returns only after every consumer group has joined
    and received partitions, so the measured runs time the debate, not the Kafka group rebalance."""
    token = make_token("warm-up", "guest", Settings(jwt_secret=os.environ["JWT_SECRET"]))
    print("warming up (waits for the consumer groups to get their partitions) ...", flush=True)
    t0 = time.time()
    # single_agent: the slow / corrupt fairness worker in this scenario does not take part
    r = run_one(api, token, "R45", "single_agent", wait_s=120)
    if r["status"] == "no result":
        sys.exit("warm-up request got no result in 120 s; see reports/perf/ai/logs/")
    print(f"ready after {time.time() - t0:.1f}s (not counted)", flush=True)


def stop(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()


# ------------------------------------------------------------------ requests
def run_one(api: str, token: str, request_id: str, config: str, wait_s: float) -> dict:
    h = {"Authorization": f"Bearer {token}"}
    t0 = time.time()
    r = httpx.post(f"{api}/ai/requests", headers=h,
                   json={"request_text": BENCH[request_id]["request_text"], "evaluation_config": config})
    r.raise_for_status()
    tid = r.json()["task_id"]
    while time.time() - t0 < wait_s:
        res = httpx.get(f"{api}/ai/results/{tid}", headers=h)
        if res.status_code == 200:
            body = res.json()
            return {"request_id": request_id, "task_id": tid, "end_to_end_s": round(time.time() - t0, 3),
                    "status": body["status"],
                    "recommendation": (body.get("recommendation") or {}).get("property_id"),
                    "final_ranking": body.get("final_ranking"),
                    "method": (body.get("adjudication") or {}).get("method"),
                    "limitations": body.get("limitations", []),
                    "explanation_source": (body.get("explanation") or {}).get("source"),
                    "chair_timings_s": body.get("timings_s", {})}
        time.sleep(0.25)
    task = httpx.get(f"{api}/ai/tasks/{tid}", headers=h).json()
    return {"request_id": request_id, "task_id": tid, "status": "no result", "task": task}


def offline(request_id: str, config: str) -> dict:
    q = BENCH[request_id]["expected_parse"]
    return chair_rules.run({a: round0_payload(a, q) for a in EXPECTED_AGENTS[config]}, config)


def pct(values, q):
    v = sorted(values)
    return round(v[min(len(v) - 1, max(0, round(q / 100 * len(v) + 0.5) - 1))], 3) if v else None


# ------------------------------------------------------------------ scenarios
def scenario_runs(args, token):
    ids = {"three-debates": ["R07", "R35", "R45"], "agent-timeout": ["R07"], "invalid-output": ["R13"],
           "latency": [f"R{n:02d}" for n in range(1, args.n + 1)]}[args.scenario]
    if args.scenario == "latency":         # one after another: measures latency, not queueing
        return [run_one(args.api, token, rid, args.config, args.wait_s) for rid in ids]
    with ThreadPoolExecutor(max_workers=len(ids)) as pool:      # all at once: interleaved on Kafka
        return list(pool.map(lambda rid: run_one(args.api, token, rid, args.config, args.wait_s), ids))


def check(scenario, runs, config, round_timeout_s):
    checks = []
    ok = lambda name, cond: checks.append({"check": name, "passed": bool(cond)})   # noqa: E731
    ok("every request produced a result", all(r["status"] != "no result" for r in runs))
    ok("one result per trace_id (no mixing)", len({r["task_id"] for r in runs}) == len(runs))
    if scenario == "three-debates":
        for r in runs:
            if r["status"] == "no result":
                continue
            exp = offline(r["request_id"], config)
            ok(f"{r['request_id']}: Kafka result == offline benchmark",
               r["recommendation"] == exp["recommendation"] and r["final_ranking"] == exp["final_ranking"])
    if scenario == "agent-timeout":
        r = runs[0]
        ok("partial result marked with the missing agent", "fairness timed out in round 0" in r.get("limitations", []))
        ok("published shortly after the deadline, not after the slow agent",
           r.get("end_to_end_s", 1e9) < round_timeout_s + 8)
    if scenario == "invalid-output":
        ok("invalid fairness output rejected by the harness",
           any(lim.startswith("fairness output rejected in round 0") for lim in runs[0].get("limitations", [])))
        ok("a recommendation was still made from the valid agents", runs[0].get("recommendation") is not None)
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", choices=["three-debates", "agent-timeout", "invalid-output", "latency"])
    ap.add_argument("--launch", action="store_true", help="start API + Chair + agents for this run")
    ap.add_argument("--port", type=int, default=8005)
    ap.add_argument("--api", default=None)
    ap.add_argument("--config", default="full_debate")
    ap.add_argument("--round-timeout-s", type=float, default=float(os.environ.get("AI_ROUND_TIMEOUT_S", "10")))
    ap.add_argument("--wait-s", type=float, default=60)
    ap.add_argument("-n", type=int, default=10, help="requests for the latency scenario")
    args = ap.parse_args()
    args.api = args.api or f"http://localhost:{args.port}"

    procs = launch(args.scenario, args.port, args.round_timeout_s) if args.launch else []
    try:
        secret = os.environ.get("JWT_SECRET")
        if not secret:
            sys.exit("set JWT_SECRET (same value as the API) or use --launch")
        token = make_token("guest-demo", "guest", Settings(jwt_secret=secret))
        t0 = time.time()
        runs = scenario_runs(args, token)
        wall = round(time.time() - t0, 3)
    finally:
        stop(procs)

    checks = check(args.scenario, runs, args.config, args.round_timeout_s)
    e2e = [r["end_to_end_s"] for r in runs if "end_to_end_s" in r]
    report = {
        "scenario": args.scenario, "config": args.config, "round_timeout_s": args.round_timeout_s,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "wall_clock_s": wall,
        "latency_s": {"n": len(e2e), "p50": pct(e2e, 50), "p95": pct(e2e, 95), "max": max(e2e) if e2e else None,
                      "mean": round(statistics.mean(e2e), 3) if e2e else None},
        "checks": checks, "runs": runs,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{args.scenario}.json"
    out.write_text(json.dumps(report, indent=2))

    for r in runs:
        print(f"{r['request_id']} {r['task_id']}: {r['status']} -> {r.get('recommendation')} "
              f"[{r.get('method')}] {r.get('end_to_end_s')}s  explanation={r.get('explanation_source')}")
        for lim in r.get("limitations", []):
            print(f"    limitation: {lim}")
    print(f"latency p50={report['latency_s']['p50']}s p95={report['latency_s']['p95']}s (n={len(e2e)})")
    for c in checks:
        print(("PASS " if c["passed"] else "FAIL ") + c["check"])
    print(f"evidence: {out.relative_to(REPO)}")
    sys.exit(0 if all(c["passed"] for c in checks) else 1)


if __name__ == "__main__":
    main()
