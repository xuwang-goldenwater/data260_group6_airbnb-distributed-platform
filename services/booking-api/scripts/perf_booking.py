"""Task 5.2 — Booking performance evidence. Every number in reports/perf/booking/ is produced by this script.

1. End-to-end latency: POST /bookings/create → booking.updated push on WebSocket /bookings/stream
   (pending_host or rejected), at concurrency 1, 10, 25, 50 (2 workers, normal key routing, pre-check on).
2. Retry timing under injected transient MySQL errors (WORKER_TRANSIENT_FAULTS=0..4, 1213 deadlock):
   0-3 faults recover in place (200 ms / 1 s / 5 s backoff); 4 faults → platform.dlq.
3. Duplicates after a worker restart: load with 2 workers, kill -9 one mid-run, restart it, count duplicate side
   effects in MySQL (expected 0).
4. DLQ negative tests: malformed records produced straight to booking.requested → count platform.dlq records.

Usage: make booking-perf   (or perf_booking.py --levels 1,10,25,50 --per-level 0 for the default request counts)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import platform
import statistics
import subprocess
import threading
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import httpx
import websockets
from confluent_kafka import Producer

import harness as h
from booking_core import events

OUT = h.REPO_ROOT / "reports/perf/booking"
SEED = json.loads((h.SETTINGS.seed_dir / "properties.json").read_text())
ACTIVE = [p["property_id"] for p in SEED if p["status"] == "active"]
GUESTS = [f"guest-{1001 + i}" for i in range(120)]

# Reference palette slots (dataviz skill, references/palette.md, light mode)
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e0"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(q / 100 * (len(s) - 1))))
    return s[k]


class Slots:
    """Unique (property, night) slots so load requests don't conflict with each other or earlier runs."""

    def __init__(self) -> None:
        last = h.mysql_rows("SELECT IFNULL(MAX(night_date), CURDATE()) FROM availability_blocks")[0][0]
        self.base = date.fromisoformat(last) + timedelta(days=2)
        self.k = 0
        self.lock = threading.Lock()

    def next(self) -> tuple[str, date, date]:
        with self.lock:
            k, self.k = self.k, self.k + 1
        ci = self.base + timedelta(days=2 * (k // len(ACTIVE)))
        return ACTIVE[k % len(ACTIVE)], ci, ci + timedelta(days=1)


# --------------------------------------------------------------------------------------- 1. e2e latency
async def vu(idx: int, guest: str, queue: asyncio.Queue, slots: Slots, http: httpx.AsyncClient, rows: list,
             level: int, timeout: float = 30.0) -> None:
    tok = h.token(guest)
    received: dict[str, tuple[float, str]] = {}
    waiters: dict[str, asyncio.Event] = {}
    async with websockets.connect(f"ws://127.0.0.1:{h.API_PORT}/bookings/stream?token={tok}") as ws:
        await ws.recv()

        async def listen():
            async for raw in ws:
                ev = json.loads(raw)
                if ev.get("event_type") == "booking.updated":
                    rid = ev["payload"]["reservation_id"]
                    received[rid] = (time.perf_counter(), ev["payload"]["to_status"])
                    waiters.setdefault(rid, asyncio.Event()).set()
        listener = asyncio.create_task(listen())
        await asyncio.sleep(0.05)
        try:
            while True:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                pid, ci, co = slots.next()
                t0 = time.perf_counter()
                r = await http.post(f"{h.api()}/bookings/create", headers={
                    "Authorization": f"Bearer {tok}", "Idempotency-Key": str(uuid.uuid4())},
                    json={"property_id": pid, "check_in": ci.isoformat(), "check_out": co.isoformat(), "guest_count": 1})
                t_http = time.perf_counter()
                j = r.json()
                row = {"concurrency": level, "vu": idx, "property_id": pid, "http_status": r.status_code,
                       "http_ms": round((t_http - t0) * 1000, 2), "reservation_id": j.get("reservation_id"),
                       "trace_id": j.get("trace_id") or j.get("error", {}).get("trace_id"),
                       "e2e_ms": None, "final_status": None}
                if r.status_code == 202:
                    rid = j["reservation_id"]
                    evt = waiters.setdefault(rid, asyncio.Event())
                    try:
                        await asyncio.wait_for(evt.wait(), timeout)
                        t1, st = received[rid]
                        row.update(e2e_ms=round((t1 - t0) * 1000, 2), final_status=st)
                    except asyncio.TimeoutError:
                        row["final_status"] = "timeout"
                rows.append(row)
        finally:
            listener.cancel()


async def run_level(level: int, n_requests: int, slots: Slots) -> tuple[list[dict], float]:
    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(n_requests):
        queue.put_nowait(1)
    rows: list[dict] = []
    limits = httpx.Limits(max_connections=level + 10, max_keepalive_connections=level + 10)
    async with httpx.AsyncClient(timeout=30, limits=limits) as http:
        t0 = time.perf_counter()
        await asyncio.gather(*(vu(i, GUESTS[i % len(GUESTS)], queue, slots, http, rows, level)
                               for i in range(level)))
        wall = time.perf_counter() - t0
    return rows, wall


def phase_latency(levels: list[int], per_level: int) -> list[dict]:
    h.log(f"phase 1: e2e latency levels={levels}")
    out = OUT / "logs/latency"
    procs = [h.start_api(out, env={"BOOKING_RATE_LIMIT_PER_MINUTE": "0", "BOOKING_COMPLETION_INTERVAL_S": "0"})]
    summary = []
    try:
        procs[0].wait_for_log(r"ws gateway assigned [1-9]", timeout=30)
        procs += [h.start_worker(out, "worker-1"), h.start_worker(out, "worker-2")]
        slots = Slots()
        asyncio.run(run_level(5, 20, slots))   # warm-up (not recorded)
        all_rows: list[dict] = []
        for level in levels:
            n = per_level or max(50, level * 6)
            rows, wall = asyncio.run(run_level(level, n, slots))
            all_rows += rows
            e2e = [r["e2e_ms"] for r in rows if r["e2e_ms"] is not None]
            http_ms = [r["http_ms"] for r in rows if r["http_status"] == 202]
            s = {"concurrency": level, "requests": len(rows), "accepted_202": len(http_ms),
                 "completed": len(e2e), "errors": len(rows) - len(e2e), "wall_s": round(wall, 2),
                 "throughput_rps": round(len(e2e) / wall, 1),
                 "e2e_p50_ms": round(pct(e2e, 50), 1), "e2e_p95_ms": round(pct(e2e, 95), 1),
                 "e2e_p99_ms": round(pct(e2e, 99), 1), "e2e_max_ms": round(max(e2e), 1) if e2e else None,
                 "http_p50_ms": round(pct(http_ms, 50), 1), "http_p95_ms": round(pct(http_ms, 95), 1),
                 "pending_host": sum(r["final_status"] == "pending_host" for r in rows),
                 "rejected": sum(r["final_status"] == "rejected" for r in rows)}
            summary.append(s)
            h.log(f"  c={level}: {s}")
        with open(OUT / "e2e_latency_raw.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(all_rows[0]))
            w.writeheader()
            w.writerows(all_rows)
        with open(OUT / "e2e_latency_summary.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary[0]))
            w.writeheader()
            w.writerows(summary)
        return summary
    finally:
        for p in reversed(procs):
            p.stop()


# --------------------------------------------------------------------------------------- 2. retry timing
async def one_booking_wait(guest: str, slots: Slots, timeout: float) -> tuple[dict, float | None, str | None]:
    tok = h.token(guest)
    async with websockets.connect(f"ws://127.0.0.1:{h.API_PORT}/bookings/stream?token={tok}") as ws:
        await ws.recv()
        await asyncio.sleep(0.05)
        pid, ci, co = slots.next()
        async with httpx.AsyncClient(timeout=30) as http:
            t0 = time.perf_counter()
            r = await http.post(f"{h.api()}/bookings/create", headers={"Authorization": f"Bearer {tok}",
                                                                       "Idempotency-Key": str(uuid.uuid4())},
                                json={"property_id": pid, "check_in": ci.isoformat(), "check_out": co.isoformat(),
                                      "guest_count": 1})
        j = r.json()
        try:
            while True:
                ev = json.loads(await asyncio.wait_for(ws.recv(), timeout))
                if ev.get("payload", {}).get("reservation_id") == j["reservation_id"]:
                    return j, (time.perf_counter() - t0) * 1000, ev["payload"]["to_status"]
        except asyncio.TimeoutError:
            return j, None, None


def phase_retry(trials: int) -> list[dict]:
    h.log("phase 2: retry timing with injected transient MySQL errors")
    out = OUT / "logs/retry"
    h.wait_no_active_members()
    api = h.start_api(out, env={"BOOKING_RATE_LIMIT_PER_MINUTE": "0", "BOOKING_COMPLETION_INTERVAL_S": "0"})
    api.wait_for_log(r"ws gateway assigned [1-9]", timeout=30)
    slots = Slots()
    rows = []
    try:
        for faults in range(5):
            for t in range(trials):
                w = h.start_worker(out, "worker-1", env={"WORKER_TRANSIENT_FAULTS": str(faults)},
                                   log_name=f"worker_faults{faults}_trial{t}")
                time.sleep(0.5)
                start = time.perf_counter()
                if faults <= 3:
                    j, ms, status = asyncio.run(one_booking_wait("guest-1100", slots, 30))
                    outcome = status or "timeout"
                else:
                    j, _, _ = asyncio.run(one_booking_wait("guest-1100", slots, 0.01))
                    m = w.wait_for_log(r"sent to platform\.dlq error_code=TRANSIENT_RETRY_EXHAUSTED", timeout=30)
                    ms = (time.perf_counter() - start) * 1000
                    outcome = "platform.dlq"
                    # leave no stuck request behind: the guest cancels the never-processed reservation
                    httpx.post(f"{h.api()}/bookings/cancel", headers={
                        "Authorization": f"Bearer {h.token('guest-1100')}", "Idempotency-Key": str(uuid.uuid4())},
                        json={"reservation_id": j["reservation_id"], "expected_version": 1}, timeout=10)
                retries = w.text().count("transient error")
                rows.append({"injected_faults": faults, "trial": t + 1, "retries_logged": retries,
                             "outcome": outcome, "elapsed_ms": round(ms, 1) if ms else None,
                             "reservation_id": j.get("reservation_id"), "trace_id": j.get("trace_id")})
                h.log(f"  faults={faults} trial={t + 1}: {rows[-1]}")
                w.stop()
        with open(OUT / "retry_timing.csv", "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=list(rows[0]))
            wr.writeheader()
            wr.writerows(rows)
        return rows
    finally:
        api.stop()


# --------------------------------------------------------------------------------------- 3. duplicates after restart
def phase_duplicates(n: int = 120, concurrency: int = 10) -> dict:
    h.log("phase 3: duplicates after a worker kill -9 + restart under load")
    out = OUT / "logs/restart"
    h.wait_no_active_members()
    api = h.start_api(out, env={"BOOKING_RATE_LIMIT_PER_MINUTE": "0", "BOOKING_COMPLETION_INTERVAL_S": "0"})
    procs = [api]
    try:
        api.wait_for_log(r"ws gateway assigned [1-9]", timeout=30)
        w1 = h.start_worker(out, "worker-1")
        w2 = h.start_worker(out, "worker-2", log_name="worker-2_before_kill")
        procs += [w1]
        slots = Slots()
        result: dict = {}

        def load():
            result["rows"], result["wall"] = asyncio.run(run_level(concurrency, n, slots))

        t = threading.Thread(target=load)
        t.start()
        deadline = time.time() + 30
        while w2.text().count("processed reservation_id=") < 15 and time.time() < deadline:
            time.sleep(0.02)
        w2.kill9()
        killed_after = w2.text().count("processed reservation_id=")
        h.log(f"  kill -9 worker-2 after it processed {killed_after} records")
        time.sleep(1.0)
        w2b = h.start_worker(out, "worker-2", log_name="worker-2_after_restart")
        procs.append(w2b)
        t.join(120)
        rows = result["rows"]
        rids = [r["reservation_id"] for r in rows if r["reservation_id"]]
        final = h.wait_status(rids, timeout=60)
        time.sleep(1)
        ids_sql = ",".join(f"'{r}'" for r in rids)
        dup_hist = h.mysql_rows(f"SELECT reservation_id, to_status, COUNT(*) c FROM reservation_status_history WHERE "
                                f"reservation_id IN ({ids_sql}) GROUP BY reservation_id, to_status HAVING c > 1")
        bad_blocks = h.mysql_rows(
            f"SELECT r.reservation_id, r.status, COUNT(b.night_date) n FROM reservations r LEFT JOIN availability_blocks b "
            f"ON b.reservation_id = r.reservation_id WHERE r.reservation_id IN ({ids_sql}) GROUP BY r.reservation_id, "
            f"r.status HAVING (r.status = 'pending_host' AND n <> 1) OR (r.status <> 'pending_host' AND n <> 0)")
        dup_processed = h.mysql_rows(f"SELECT COUNT(*) FROM processed_events WHERE idempotency_key IN ("
                                     + ",".join(f"'booking.requested:{r}'" for r in rids) + ")")[0][0]
        upd = h.topic_events("booking.updated", lambda e: e.get("payload", {}).get("reservation_id") in set(rids))
        keys: dict[str, int] = {}
        for e in upd:
            keys[e["idempotency_key"]] = keys.get(e["idempotency_key"], 0) + 1
        redelivered = w2b.text().count("outcome=duplicate") + w1.text().count("outcome=duplicate")
        res = {"requests": len(rows), "accepted": len(rids), "killed_worker": "worker-2",
               "worker2_processed_before_kill": killed_after,
               "worker2_processed_after_restart": w2b.text().count("processed reservation_id="),
               "redeliveries_detected_as_duplicate": redelivered,
               "final_status_counts": {s: list(final.values()).count(s) for s in set(final.values())},
               "duplicate_history_rows": len(dup_hist), "reservations_with_wrong_block_count": len(bad_blocks),
               "processed_events_rows": int(dup_processed),
               "booking_updated_events_on_topic": len(upd),
               "booking_updated_duplicate_keys_on_topic": sum(1 for v in keys.values() if v > 1),
               "duplicate_db_side_effects": len(dup_hist) + len(bad_blocks) + max(0, int(dup_processed) - len(rids))}
        h.write(OUT / "duplicates_after_restart.json", res)
        h.log(f"  {res}")
        return res
    finally:
        for p in reversed(procs):
            p.stop()


# --------------------------------------------------------------------------------------- 4. DLQ negative tests
def phase_dlq() -> list[dict]:
    h.log("phase 4: malformed events → platform.dlq")
    out = OUT / "logs/dlq"
    h.wait_no_active_members()
    procs = [h.start_worker(out, "worker-1"), h.start_worker(out, "worker-2")]
    marker = f"tr-neg-{int(time.time())}"
    from datetime import datetime
    ghost = {"reservation_id": f"res-neg-{uuid.uuid4().hex[:12]}", "property_id": "property-2001",
             "guest_id": "guest-1001", "host_id": "host-501", "check_in": date(2027, 1, 5),
             "check_out": date(2027, 1, 7), "guest_count": 2, "trace_id": marker,
             "created_datetime": datetime.utcnow()}

    def base():
        return json.loads(json.dumps(events.booking_requested(ghost)))

    def mut(fn):
        e = base()
        fn(e)
        return json.dumps(e).encode()

    cases = [
        ("not_json", "property-2001", b"this is not json " + marker.encode(), "INVALID_JSON"),
        ("empty_value", "property-2001", None, "INVALID_JSON"),
        ("missing_payload_check_in", "property-2001", mut(lambda e: e["payload"].pop("check_in")), "INVALID_PAYLOAD"),
        ("guest_count_wrong_type", "property-2001", mut(lambda e: e["payload"].update(guest_count="two")), "INVALID_PAYLOAD"),
        ("check_out_before_check_in", "property-2001", mut(lambda e: e["payload"].update(check_out="2027-01-01")), "INVALID_PAYLOAD"),
        ("missing_envelope_trace_id", "property-2001", mut(lambda e: e.pop("trace_id")), "INVALID_EVENT_ENVELOPE"),
        ("schema_version_2", "property-2001", mut(lambda e: e.update(schema_version="2.0")), "INVALID_EVENT_ENVELOPE"),
        ("unexpected_top_level_field", "property-2001", mut(lambda e: e.update(debug=True)), "INVALID_EVENT_ENVELOPE"),
        ("unknown_event_type", "property-2001", mut(lambda e: e.update(event_type="booking.created")), "UNKNOWN_EVENT_TYPE"),
        ("kafka_key_mismatch", "property-2999", json.dumps(base()).encode(), "EVENT_KEY_MISMATCH"),
        ("reservation_not_found", "property-2001", json.dumps(base()).encode(), "RESERVATION_NOT_FOUND"),
    ]
    before = h.mysql_rows("SELECT COUNT(*) FROM reservations")[0][0]
    p = Producer({"bootstrap.servers": h.SETTINGS.kafka_bootstrap})
    sent = {}

    def cb(name):
        def _cb(err, msg):
            sent[(msg.partition(), msg.offset())] = name
        return _cb
    for name, key, value, _ in cases:
        p.produce("booking.requested", key=key.encode(), value=value, on_delivery=cb(name))
    p.flush(10)
    rows = []
    try:
        deadline = time.time() + 40
        found: list[dict] = []
        while time.time() < deadline:
            found = h.topic_events("platform.dlq", lambda e: (e.get("payload", {}).get("original_topic") == "booking.requested"
                                   and (e["payload"].get("original_partition"), e["payload"].get("original_offset")) in sent))
            if len(found) >= len(cases):
                break
            time.sleep(1)
        by_pos = {(e["payload"]["original_partition"], e["payload"]["original_offset"]): e for e in found}
        for (part, off), name in sorted(sent.items(), key=lambda kv: [c[0] for c in cases].index(kv[1])):
            exp = next(c[3] for c in cases if c[0] == name)
            rec = by_pos.get((part, off))
            rows.append({"case": name, "partition": part, "offset": off, "expected_error_code": exp,
                         "dlq_error_code": rec["payload"]["error_code"] if rec else None,
                         "retry_count": rec["payload"]["retry_count"] if rec else None,
                         "dlq_records": sum(1 for e in found if (e["payload"]["original_partition"],
                                                                 e["payload"]["original_offset"]) == (part, off)),
                         "match": bool(rec) and rec["payload"]["error_code"] == exp})
        after = h.mysql_rows("SELECT COUNT(*) FROM reservations")[0][0]
        for r in rows:
            r["reservations_unchanged"] = before == after
            h.log(f"  {r}")
        with open(OUT / "dlq_negative_tests.csv", "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=list(rows[0]))
            wr.writeheader()
            wr.writerows(rows)
        if found:
            h.write(OUT / "dlq_sample_record.json", found[0])
        return rows
    finally:
        for pr in reversed(procs):
            pr.stop()


# --------------------------------------------------------------------------------------- graphs + README
def graphs(summary: list[dict], raw_csv: Path, retry: list[dict]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def style(ax, title, xlabel, ylabel):
        ax.set_facecolor(SURFACE)
        ax.set_title(title, loc="left", color=INK, fontsize=12, pad=12)
        ax.set_xlabel(xlabel, color=INK2)
        ax.set_ylabel(ylabel, color=INK2)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(GRID)
        ax.tick_params(colors=INK2)

    xs = [s["concurrency"] for s in summary]
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=SURFACE)
    for key, label, color in (("e2e_p50_ms", "p50", S1), ("e2e_p95_ms", "p95", S2)):
        ys = [s[key] for s in summary]
        ax.plot(xs, ys, color=color, linewidth=2, marker="o", markersize=8, markeredgecolor=SURFACE,
                markeredgewidth=2, label=label)
        ax.annotate(f"{label} {ys[-1]:.0f} ms", (xs[-1], ys[-1]), textcoords="offset points", xytext=(8, 0),
                    va="center", color=INK, fontsize=9)
    ax.set_xticks(xs)
    ax.set_xlim(0, max(xs) * 1.18)
    ax.set_ylim(0, max(s["e2e_p95_ms"] for s in summary) * 1.25)
    style(ax, "Booking end-to-end latency (create → booking.updated push)", "Concurrent guests", "Latency (ms)")
    ax.legend(frameon=False, labelcolor=INK2, loc="upper left")
    fig.tight_layout()
    fig.savefig(OUT / "e2e_latency_vs_concurrency.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=SURFACE)
    ys = [s["throughput_rps"] for s in summary]
    ax.bar([str(x) for x in xs], ys, color=S1, width=0.55)
    for i, y in enumerate(ys):
        ax.annotate(f"{y:.0f}", (i, y), textcoords="offset points", xytext=(0, 4), ha="center", color=INK, fontsize=9)
    style(ax, "Completed bookings per second (2 workers)", "Concurrent guests", "Bookings / s")
    fig.tight_layout()
    fig.savefig(OUT / "throughput_vs_concurrency.png", dpi=150)
    plt.close(fig)

    ok = [r for r in retry if r["elapsed_ms"] is not None]
    groups = sorted({r["injected_faults"] for r in ok})
    means = [statistics.mean(r["elapsed_ms"] for r in ok if r["injected_faults"] == g) / 1000 for g in groups]
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=SURFACE)
    labels = [f"{g} fault{'s' if g != 1 else ''}" + ("\n→ DLQ" if g == 4 else "") for g in groups]
    ax.bar(labels, means, color=[S2 if g == 4 else S1 for g in groups], width=0.55)
    for i, m in enumerate(means):
        ax.annotate(f"{m:.2f} s", (i, m), textcoords="offset points", xytext=(0, 4), ha="center", color=INK, fontsize=9)
    style(ax, "Time to outcome with injected transient MySQL errors (backoff 0.2 s / 1 s / 5 s)",
          "Injected deadlocks before success", "Seconds (mean of trials)")
    fig.tight_layout()
    fig.savefig(OUT / "retry_timing.png", dpi=150)
    plt.close(fig)


def summarize_logs() -> None:
    """Raw process logs stay local (reports/perf/booking/logs/ is git-ignored); commit a small summary instead."""
    keys = {"records processed": "processed reservation_id=", "transient retries": "transient error",
            "dlq writes": "sent to platform.dlq", "redeliveries (duplicate)": "outcome=duplicate",
            "offset commits": "committed offset", "errors": " ERROR "}
    lines = ["# Log summary (raw logs are kept locally in reports/perf/booking/logs/, not committed)", "",
             "| log file | lines | " + " | ".join(keys) + " |", "|---|---:|" + "---:|" * len(keys)]
    for path in sorted((OUT / "logs").rglob("*_log.txt")):
        text = path.read_text(errors="replace")
        counts = [str(text.count(v)) for v in keys.values()]
        lines.append(f"| {path.relative_to(OUT / 'logs')} | {text.count(chr(10))} | " + " | ".join(counts) + " |")
    (OUT / "logs_summary.md").write_text("\n".join(lines) + "\n")


def machine() -> dict:
    def sh(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:
            return ""
    info = {"os": platform.platform(), "python": platform.python_version(), "machine": platform.machine(),
            "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
            "logical_cpus": sh("sysctl", "-n", "hw.ncpu"),
            "memory_gb": round(int(sh("sysctl", "-n", "hw.memsize") or 0) / 2**30, 1),
            "docker": sh("docker", "version", "--format", "{{.Server.Version}}"),
            "docker_vm_cpus": sh("docker", "info", "--format", "{{.NCPU}}"),
            "docker_vm_memory_gb": round(int(sh("docker", "info", "--format", "{{.MemTotal}}") or 0) / 2**30, 1),
            "mysql_image": "mysql:8.4", "kafka_image": "apache/kafka:3.8.1",
            "topic_partitions_booking_requested": 3, "workers": 2, "git_rev": h.git_rev()}
    return info


def readme(summary, retry, dup, dlq, mach) -> str:
    lines = ["# Booking performance evidence (task 5.2, Role B)", "",
             "Every number below was produced by `make booking-perf` "
             "(`services/booking-api/scripts/perf_booking.py`) on the machine listed at the bottom. "
             "Raw data and console output are in this folder (raw process logs are summarized in "
             "`logs_summary.md`); nothing was edited by hand.", "",
             f"Run: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}, git `{mach['git_rev']}`.", "",
             "## 1. End-to-end booking latency", "",
             "Latency = `POST /bookings/create` sent → `booking.updated` (pending_host/rejected) received on the guest's "
             "`WebSocket /bookings/stream`. Each virtual guest has its own WebSocket; every request uses a distinct "
             "property/night so the outcome is `pending_host`. 1 API process, 2 worker processes, "
             "`booking.requested` with 3 partitions, MySQL/Kafka in Docker on the same laptop.", "",
             "| Concurrency | Requests | Completed | Errors | Throughput (bookings/s) | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | HTTP 202 p50 (ms) |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for s in summary:
        lines.append(f"| {s['concurrency']} | {s['requests']} | {s['completed']} | {s['errors']} | {s['throughput_rps']} | "
                     f"{s['e2e_p50_ms']} | {s['e2e_p95_ms']} | {s['e2e_p99_ms']} | {s['e2e_max_ms']} | {s['http_p50_ms']} |")
    lines += ["", "![latency](e2e_latency_vs_concurrency.png)", "", "![throughput](throughput_vs_concurrency.png)", "",
              "Files: `e2e_latency_summary.csv`, `e2e_latency_raw.csv` (one row per request with trace_id).", "",
              "## 2. Retry time under injected transient MySQL errors", "",
              "`WORKER_TRANSIENT_FAULTS=n` makes the worker raise MySQL error 1213 (deadlock) n times before the real "
              "transaction. Policy: 3 in-place retries with 200 ms / 1 s / 5 s backoff, then `platform.dlq`. "
              "Time = create sent → booking.updated push (0-3 faults) or → DLQ write logged (4 faults).", "",
              "| Injected faults | Trials | Outcome | Retries logged | Mean (s) | Min (s) | Max (s) |", "|---:|---:|---|---:|---:|---:|---:|"]
    for f in sorted({r["injected_faults"] for r in retry}):
        rs = [r for r in retry if r["injected_faults"] == f]
        el = [r["elapsed_ms"] / 1000 for r in rs if r["elapsed_ms"] is not None]
        outs = ", ".join(sorted({r["outcome"] for r in rs}))
        lines.append(f"| {f} | {len(rs)} | {outs} | {rs[0]['retries_logged']} | {statistics.mean(el):.2f} | "
                     f"{min(el):.2f} | {max(el):.2f} |")
    lines += ["", "![retry](retry_timing.png)", "", "File: `retry_timing.csv`.", "",
              "## 3. Duplicates after a worker restart", "",
              f"{dup['requests']} bookings at concurrency 10 with 2 workers; `worker-2` was killed with SIGKILL after "
              f"processing {dup['worker2_processed_before_kill']} records and restarted 1 s later.", "",
              "| Metric | Value |", "|---|---:|",
              f"| Accepted (202) | {dup['accepted']} |",
              f"| Final statuses | {', '.join(f'{k}: {v}' for k, v in sorted(dup['final_status_counts'].items()))} |",
              f"| Redeliveries detected by processed_events | {dup['redeliveries_detected_as_duplicate']} |",
              f"| Duplicate status-history rows | {dup['duplicate_history_rows']} |",
              f"| Reservations with wrong availability_blocks count | {dup['reservations_with_wrong_block_count']} |",
              f"| processed_events rows (expected = accepted) | {dup['processed_events_rows']} |",
              f"| booking.updated events on topic / duplicate idempotency keys | {dup['booking_updated_events_on_topic']} / {dup['booking_updated_duplicate_keys_on_topic']} |",
              f"| **Duplicate DB side effects** | **{dup['duplicate_db_side_effects']}** |", "",
              "File: `duplicates_after_restart.json`. The deterministic crash-between-commit-and-offset case is in "
              "`consumer_recovery/` (`make consumer-recovery`).", "",
              "## 4. DLQ negative tests", "",
              "Malformed records produced directly to `booking.requested` (bypassing the API) while 2 workers run.", "",
              "| Case | Expected error_code | DLQ error_code | retry_count | DLQ records |", "|---|---|---|---:|---:|"]
    for r in dlq:
        lines.append(f"| {r['case']} | {r['expected_error_code']} | {r['dlq_error_code']} | {r['retry_count']} | {r['dlq_records']} |")
    lines += ["", f"DLQ records: **{sum(r['dlq_records'] for r in dlq)} / {len(dlq)}** malformed inputs; "
              f"reservations table unchanged: {all(r['reservations_unchanged'] for r in dlq)}. "
              "Files: `dlq_negative_tests.csv`, `dlq_sample_record.json`.", "",
              "## How to reproduce", "", "```bash",
              "docker compose -f infra/docker-compose.yml up -d kafka mysql   # or: make booking-infra-up",
              "make booking-topics booking-db booking-seed",
              "make booking-perf          # ~3-4 min; stop any manually started booking workers first",
              "```", "", "## Machine", "", "| Item | Value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in mach.items()]
    lines += ["", "Note: everything (API, 2 workers, load generator, Docker MySQL + Kafka) ran on one laptop, so the "
              "numbers are a lower bound on what separate hosts would give and include load-generator overhead."]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--levels", default="1,10,25,50")
    ap.add_argument("--per-level", type=int, default=0)
    ap.add_argument("--trials", type=int, default=3)
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    h.wait_no_active_members()
    summary = phase_latency([int(x) for x in a.levels.split(",")], a.per_level)
    retry = phase_retry(a.trials)
    dup = phase_duplicates()
    dlq = phase_dlq()
    mach = machine()
    h.write(OUT / "machine.json", mach)
    graphs(summary, OUT / "e2e_latency_raw.csv", retry)
    summarize_logs()
    (OUT / "README.md").write_text(readme(summary, retry, dup, dlq, mach))
    ok = (all(s["errors"] == 0 for s in summary) and dup["duplicate_db_side_effects"] == 0
          and all(r["match"] and r["dlq_records"] == 1 for r in dlq)
          and all(r["outcome"] == ("platform.dlq" if r["injected_faults"] == 4 else "pending_host") for r in retry))
    h.log("RESULT: " + ("PASS" if ok else "CHECK FAILURES ABOVE"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
