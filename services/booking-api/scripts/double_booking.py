"""★ Task 3.4 — double-booking test (docs/api/booking.md "Concurrency: preventing double booking").

Fires N concurrent POST /bookings/create for the same property and nights from N different guests, with the API
pre-check disabled (BOOKING_PRECHECK_ENABLED=false) so every request reaches the worker, and 2 worker processes
in the same consumer group `booking-worker`. Asserts exactly 1 pending_host, N-1 rejected/DATES_UNAVAILABLE, and
exactly `nights` availability_blocks rows for those nights.

Two routing modes are run (default: both):
  key  — production routing: Kafka key = property_id, so all N requests land on one partition / one worker.
  race — TEST ONLY: BOOKING_TEST_SPREAD_PARTITIONS=true spreads the N requests over all partitions, so both
         workers insert the same (property_id, night_date) rows concurrently; only the MySQL primary key decides.

Evidence → reports/perf/booking/double_booking/<mode>/ (requests.csv, sql_evidence.txt, worker logs, summary.json)
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import httpx

import harness as h

OUT = h.REPO_ROOT / "reports/perf/booking/double_booking"


def run_mode(mode: str, n: int, nights: int, property_id: str) -> bool:
    out = OUT / mode
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    h.log(f"=== mode={mode} N={n} nights={nights} property={property_id} ===")
    h.wait_no_active_members()
    api_env = {"BOOKING_PRECHECK_ENABLED": "false", "BOOKING_RATE_LIMIT_PER_MINUTE": "0",
               "BOOKING_TEST_SPREAD_PARTITIONS": "true" if mode == "race" else "false",
               "BOOKING_WS_ENABLED": "false", "BOOKING_COMPLETION_INTERVAL_S": "0"}
    procs = [h.start_api(out, env=api_env)]
    try:
        procs += [h.start_worker(out, "worker-1"), h.start_worker(out, "worker-2")]
        members = h.kafka_groups(members=True)
        h.write(out / "consumer_group_members.txt", members + "\n")
        check_in, check_out = h.free_window(property_id, nights)
        h.log(f"dates {check_in} -> {check_out} (free before the test)")
        guests = [f"guest-{1001 + i}" for i in range(n)]
        barrier = threading.Barrier(n)
        results: list[dict] = [None] * n  # type: ignore[list-item]

        def fire(i: int) -> None:
            with httpx.Client(timeout=30) as c:
                barrier.wait()
                t0 = time.perf_counter()
                r = h.create_booking(c, guests[i], property_id, check_in, check_out)
                ms = (time.perf_counter() - t0) * 1000
                j = r.json()
                results[i] = {"guest_id": guests[i], "http_status": r.status_code, "http_ms": round(ms, 1),
                              "reservation_id": j.get("reservation_id"), "trace_id": j.get("trace_id")
                              or j.get("error", {}).get("trace_id")}

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted = [r for r in results if r["http_status"] == 202]
        h.log(f"{len(accepted)}/{n} accepted with 202")
        rids = [r["reservation_id"] for r in accepted]
        final = h.wait_status(rids, timeout=60)
        for r in results:
            r["final_status"] = final.get(r["reservation_id"])

        ids_sql = ",".join(f"'{r}'" for r in rids)
        night_list = ",".join(f"'{check_in.fromordinal(check_in.toordinal() + i)}'" for i in range(nights))
        q_status = (f"SELECT status, status_reason, COUNT(*) AS reservations FROM reservations "
                    f"WHERE reservation_id IN ({ids_sql}) GROUP BY status, status_reason")
        q_blocks = (f"SELECT property_id, night_date, reservation_id FROM availability_blocks "
                    f"WHERE property_id = '{property_id}' AND night_date IN ({night_list}) ORDER BY night_date")
        q_hist = (f"SELECT to_status, actor_id, reason, COUNT(*) AS rows_ FROM reservation_status_history "
                  f"WHERE reservation_id IN ({ids_sql}) GROUP BY to_status, actor_id, reason")
        q_winner = (f"SELECT reservation_id, guest_id, status, version, trace_id FROM reservations "
                    f"WHERE reservation_id IN ({ids_sql}) AND status = 'pending_host'")
        sql_text = []
        for title, q in (("Reservation outcome", q_status), ("Held nights (availability_blocks)", q_blocks),
                         ("Winner", q_winner), ("Status history rows", q_hist)):
            sql_text.append(f"-- {title}\nmysql> {q};\n{h.mysql(q)}")
        h.write(out / "sql_evidence.txt", "\n".join(sql_text))
        block_rows = h.mysql_rows(q_blocks)
        statuses = Counter(final.values())
        winners = [r for r, s in final.items() if s == "pending_host"]
        reasons = Counter(row[1] for row in h.mysql_rows(
            f"SELECT reservation_id, IFNULL(status_reason,'') FROM reservations WHERE reservation_id IN ({ids_sql}) "
            f"AND status = 'rejected'"))
        # Which worker decided each reservation (from the worker logs)
        per_worker = {}
        for p in procs[1:]:
            txt = p.text()
            per_worker[p.name] = sum(1 for rid in rids if f"processed reservation_id={rid}" in txt)

        checks = {
            "all_accepted_202": len(accepted) == n,
            "exactly_one_pending_host": statuses.get("pending_host", 0) == 1,
            "n_minus_1_rejected": statuses.get("rejected", 0) == n - 1,
            "rejected_reason_dates_unavailable": reasons.get("DATES_UNAVAILABLE", 0) == n - 1,
            "blocks_equal_nights": len(block_rows) == nights,
            "blocks_owned_by_winner": len(winners) == 1 and all(row[2] == winners[0] for row in block_rows),
            "none_left_requested": statuses.get("requested", 0) == 0,
        }
        if mode == "race":
            checks["both_workers_processed_requests"] = all(v > 0 for v in per_worker.values())
        with open(out / "requests.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(results[0]))
            w.writeheader()
            w.writerows(results)
        summary = {"mode": mode, "n": n, "nights": nights, "property_id": property_id,
                   "check_in": check_in.isoformat(), "check_out": check_out.isoformat(), "api_precheck": False,
                   "workers": ["worker-1", "worker-2"], "consumer_group": h.WORKER_GROUP,
                   "statuses": dict(statuses), "winner": winners[0] if winners else None,
                   "processed_per_worker": per_worker, "availability_block_rows": len(block_rows),
                   "checks": checks, "passed": all(checks.values()), "git_rev": h.git_rev(),
                   "run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        h.write(out / "summary.json", summary)
        for name, ok in checks.items():
            h.log(f"  {'PASS' if ok else 'FAIL'}  {name}")
        h.log(f"statuses={dict(statuses)} per_worker={per_worker} blocks={len(block_rows)} winner={summary['winner']}")
        return summary["passed"]
    finally:
        for p in reversed(procs):
            p.stop()


def main() -> int:
    ap = argparse.ArgumentParser(description="★ double-booking test")
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--nights", type=int, default=2)
    ap.add_argument("--property", default="property-2001")
    ap.add_argument("--mode", choices=["key", "race", "both"], default="both")
    a = ap.parse_args()
    if a.n < 2 or a.n > 120:
        sys.exit("--n must be 2..120 (one seed guest per request)")
    modes = ["key", "race"] if a.mode == "both" else [a.mode]
    ok = all([run_mode(m, a.n, a.nights, a.property) for m in modes])
    h.log("RESULT: " + ("PASS — no double booking" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
