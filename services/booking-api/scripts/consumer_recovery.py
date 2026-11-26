"""★ Task 4.3 — consumer recovery (docs/api/booking.md "Consumer recovery (§13.2)").

  1. Worker stopped → a booking request lands in Kafka but is not processed.
  2. `kafka-consumer-groups --describe` shows committed offset < log-end offset (lag 1).
  3. Restart the worker: it resumes from the committed offset and processes the event once.
  4. Worker with WORKER_FAULT=crash_after_db_commit: MySQL commits, then the process dies before publishing
     and before committing the offset. Restart: Kafka redelivers; processed_events makes it a no-op, and the
     owed booking.updated is published once.
  5. Assert one status-history row per transition, one set of availability_blocks, one processed_events row,
     and exactly one booking.updated event per transition on the topic.

Evidence → reports/perf/booking/consumer_recovery/
"""
from __future__ import annotations

import json
import shutil
import time

import httpx

import harness as h

OUT = h.REPO_ROOT / "reports/perf/booking/consumer_recovery"
PROPERTY = "property-2044"


def describe(label: str, evidence: list[str]) -> dict:
    text = h.kafka_groups()
    evidence.append(f"### {label}\n$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe "
                    f"--group {h.WORKER_GROUP}\n{text}\n")
    return h.parse_lag(text)


def partition_of(rid: str) -> int:
    ev = h.topic_events("booking.requested", lambda e: e.get("payload", {}).get("reservation_id") == rid)
    return ev[0]["_meta"]["partition"]


def checks_for(rid: str, nights: int) -> dict:
    hist = h.mysql_rows(f"SELECT to_status, COUNT(*) FROM reservation_status_history WHERE reservation_id = '{rid}' "
                        f"GROUP BY to_status")
    blocks = int(h.mysql_rows(f"SELECT COUNT(*) FROM availability_blocks WHERE reservation_id = '{rid}'")[0][0])
    processed = int(h.mysql_rows(f"SELECT COUNT(*) FROM processed_events WHERE idempotency_key = "
                                 f"'booking.requested:{rid}'")[0][0])
    status, pending = h.mysql_rows(f"SELECT status, publish_pending FROM reservations WHERE reservation_id = '{rid}'")[0]
    updated = h.topic_events("booking.updated", lambda e: e.get("payload", {}).get("reservation_id") == rid)
    by_key: dict[str, int] = {}
    for e in updated:
        by_key[e["idempotency_key"]] = by_key.get(e["idempotency_key"], 0) + 1
    hist_counts = {k: int(v) for k, v in hist}
    return {
        "status": status, "publish_pending": int(pending), "history_rows": hist_counts, "availability_blocks": blocks,
        "processed_events_rows": processed, "booking_updated_events": by_key,
        "ok": (status == "pending_host" and hist_counts == {"requested": 1, "pending_host": 1} and blocks == nights
               and processed == 1 and int(pending) == 0 and by_key == {f"booking.updated:{rid}:v2": 1}),
    }


def main() -> int:
    if OUT.exists():
        for p in OUT.iterdir():
            if p.name != "run_output.txt":
                shutil.rmtree(p) if p.is_dir() else p.unlink()
    OUT.mkdir(parents=True, exist_ok=True)
    evidence: list[str] = []
    results: dict = {"property_id": PROPERTY, "consumer_group": h.WORKER_GROUP, "git_rev": h.git_rev(),
                     "run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    h.wait_no_active_members()
    api = h.start_api(OUT, env={"BOOKING_WS_ENABLED": "false", "BOOKING_COMPLETION_INTERVAL_S": "0",
                                "BOOKING_OUTBOX_MIN_AGE_S": "3600"})   # outbox off: the worker must do the republish
    procs = [api]
    nights = 2
    try:
        # Warm-up: make sure the group has committed offsets on every partition, then stop the worker.
        w = h.start_worker(OUT, "worker-1", log_name="worker_0_warmup")
        time.sleep(2)
        w.stop()
        h.log("warm-up worker stopped (group has committed offsets)")

        # ---------------------------------------------------------------- scenario A: stopped before processing
        h.log("A1: worker is stopped; sending a booking request")
        ci, co = h.free_window(PROPERTY, nights)
        with httpx.Client(timeout=30) as c:
            r = h.create_booking(c, "guest-1050", PROPERTY, ci, co)
        assert r.status_code == 202, r.text
        rid_a = r.json()["reservation_id"]
        part_a = partition_of(rid_a)
        time.sleep(1)
        status_a = h.mysql_rows(f"SELECT status FROM reservations WHERE reservation_id = '{rid_a}'")[0][0]
        lag_a = describe(f"A2 — worker stopped, {rid_a} in Kafka on partition {part_a}", evidence)
        h.log(f"A2: partition {part_a} lag={lag_a[part_a]['lag']} status={status_a}")

        h.log("A3: restarting worker-1")
        w = h.start_worker(OUT, "worker-1", log_name="worker_1_resume")
        w.wait_for_log(rf"committed offset partition={part_a}", timeout=30)
        time.sleep(1)
        lag_a2 = describe("A3 — after restart (resumed from committed offset)", evidence)
        processed_lines_a = w.text().count(f"processed reservation_id={rid_a}")
        w.stop()
        time.sleep(1)
        res_a = checks_for(rid_a, nights)
        results["scenario_A_stopped_before_processing"] = {
            "reservation_id": rid_a, "partition": part_a, "status_while_stopped": status_a,
            "lag_while_stopped": lag_a[part_a], "lag_after_restart": lag_a2[part_a],
            "worker_processed_log_lines": processed_lines_a, **res_a,
            "passed": res_a["ok"] and status_a == "requested" and lag_a[part_a]["lag"] == 1
            and lag_a2[part_a]["lag"] == 0 and processed_lines_a == 1}

        # ---------------------------------------------------------------- scenario B: crash after MySQL commit
        h.log("B1: starting worker-1 with WORKER_FAULT=crash_after_db_commit")
        w = h.start_worker(OUT, "worker-1", env={"WORKER_FAULT": "crash_after_db_commit"},
                           log_name="worker_2_crash")
        ci, co = h.free_window(PROPERTY, nights, co)
        with httpx.Client(timeout=30) as c:
            r = h.create_booking(c, "guest-1051", PROPERTY, ci, co)
        assert r.status_code == 202, r.text
        rid_b = r.json()["reservation_id"]
        w.popen.wait(30)
        exit_code = w.popen.returncode
        h.log(f"B1: worker exited with code {exit_code}")
        part_b = partition_of(rid_b)
        db_after_crash = h.mysql(f"SELECT reservation_id, status, version, publish_pending FROM reservations "
                                 f"WHERE reservation_id = '{rid_b}'; SELECT idempotency_key, consumer FROM "
                                 f"processed_events WHERE idempotency_key = 'booking.requested:{rid_b}'")
        evidence.append(f"### B1 — MySQL right after the crash (committed, publish_pending=1)\n{db_after_crash}")
        upd_after_crash = len(h.topic_events("booking.updated",
                                             lambda e: e.get("payload", {}).get("reservation_id") == rid_b))
        lag_b = describe(f"B2 — worker crashed after MySQL commit; offset for {rid_b} NOT committed "
                         f"(partition {part_b})", evidence)
        h.log(f"B2: partition {part_b} lag={lag_b[part_b]['lag']} booking.updated events so far={upd_after_crash}")

        h.log("B3: restarting worker-1 without fault injection")
        w = h.start_worker(OUT, "worker-1", log_name="worker_3_after_crash")
        w.wait_for_log(rf"committed offset partition={part_b}", timeout=30)
        time.sleep(1)
        lag_b2 = describe("B3 — after restart (redelivered, deduplicated)", evidence)
        dup_lines = w.text().count(f"processed reservation_id={rid_b} outcome=duplicate")
        w.stop()
        time.sleep(1)
        res_b = checks_for(rid_b, nights)
        results["scenario_B_crash_after_db_commit"] = {
            "reservation_id": rid_b, "partition": part_b, "crash_exit_code": exit_code,
            "booking_updated_before_restart": upd_after_crash, "lag_after_crash": lag_b[part_b],
            "lag_after_restart": lag_b2[part_b], "redelivery_logged_as_duplicate": dup_lines, **res_b,
            "passed": res_b["ok"] and exit_code == 137 and upd_after_crash == 0 and lag_b[part_b]["lag"] == 1
            and lag_b2[part_b]["lag"] == 0 and dup_lines == 1}

        final_sql = h.mysql(
            f"SELECT reservation_id, from_status, to_status, actor_id, changed_datetime FROM reservation_status_history "
            f"WHERE reservation_id IN ('{rid_a}','{rid_b}') ORDER BY reservation_id, id; "
            f"SELECT property_id, night_date, reservation_id FROM availability_blocks WHERE reservation_id IN "
            f"('{rid_a}','{rid_b}') ORDER BY night_date; "
            f"SELECT idempotency_key, consumer, processed_datetime FROM processed_events WHERE idempotency_key IN "
            f"('booking.requested:{rid_a}','booking.requested:{rid_b}')")
        evidence.append(f"### Step 5 — final MySQL state\n{final_sql}")
    finally:
        for p in reversed(procs):
            p.stop()
    h.write(OUT / "offsets_and_sql.txt", "\n".join(evidence))
    results["passed"] = all(v["passed"] for k, v in results.items() if k.startswith("scenario_"))
    h.write(OUT / "assertions.json", results)
    print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items()
                     if kk in ("reservation_id", "lag_after_crash", "lag_while_stopped", "lag_after_restart",
                               "history_rows", "availability_blocks", "processed_events_rows",
                               "booking_updated_events", "crash_exit_code", "passed")})
                      for k, v in results.items()}, indent=2))
    h.log("RESULT: " + ("PASS — no lost and no duplicated booking" if results["passed"] else "FAIL"))
    return 0 if results["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
