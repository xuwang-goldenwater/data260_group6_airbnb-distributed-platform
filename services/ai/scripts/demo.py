"""Send N recommendation requests at once and print how each debate ended.

    python services/ai/scripts/demo.py                 # one debate
    python services/ai/scripts/demo.py --count 3       # ★ three concurrent debates (task 4.8)
    # ★ agent timeout (task 4.7): start the fairness worker with --delay-s 15, then run with --count 1

Needs the API (port 8005), the Chair and the four agent workers running (see services/ai/README.md).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[3]
REQUESTS = [r for r in json.loads((REPO / "benchmarks/requests.json").read_text())["requests"]]
DEFAULT_IDS = ["R07", "R13", "R35", "R45", "R05"]


def run_one(api: str, request_id: str, config: str, wait_s: float) -> dict:
    text = next(r["request_text"] for r in REQUESTS if r["request_id"] == request_id)
    task = httpx.post(f"{api}/ai/requests", json={"request_text": text, "evaluation_config": config}).json()
    tid, t0 = task["task_id"], time.time()
    while time.time() - t0 < wait_s:
        r = httpx.get(f"{api}/ai/results/{tid}")
        if r.status_code == 200:
            return {"request": request_id, "task_id": tid, "seconds": round(time.time() - t0, 1), **r.json()}
        time.sleep(0.5)
    return {"request": request_id, "task_id": tid, "status": "no result yet",
            "task": httpx.get(f"{api}/ai/tasks/{tid}").json()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8005")
    ap.add_argument("--count", type=int, default=1)
    ap.add_argument("--config", default="full_debate")
    ap.add_argument("--wait-s", type=float, default=60)
    args = ap.parse_args()
    ids = DEFAULT_IDS[: args.count]
    with ThreadPoolExecutor(max_workers=len(ids)) as pool:
        results = list(pool.map(lambda rid: run_one(args.api, rid, args.config, args.wait_s), ids))
    for r in results:
        rec = (r.get("recommendation") or {}).get("property_id")
        adj = (r.get("adjudication") or {})
        print(f"{r['request']} {r['task_id']}: {r['status']} -> {rec} "
              f"[{adj.get('method')}{', ' + adj['tie_break_applied'] if adj.get('tie_break_applied') else ''}] "
              f"in {r.get('seconds')}s")
        for note in r.get("limitations", []):
            print(f"    limitation: {note}")
    traces = [r["task_id"] for r in results]
    sys.exit(0 if len(set(traces)) == len(traces) and all(r["status"] != "no result yet" for r in results) else 1)


if __name__ == "__main__":
    main()
