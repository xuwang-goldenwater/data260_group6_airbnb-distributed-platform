"""Process + infrastructure helpers for the Role B demo / evidence scripts (double booking, recovery, perf).

Starts the real Booking API (uvicorn) and real worker processes as subprocesses, so every demo exercises the
same code paths as a manual run. SQL evidence is produced with the `mysql` client inside the compose container
and Kafka offsets with `kafka-consumer-groups.sh` inside the Kafka container.
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import _bootstrap  # noqa: F401
import httpx
from confluent_kafka import Consumer, KafkaError, TopicPartition

from booking_core.config import REPO_ROOT, Settings
from dev_token import mint

PY = sys.executable
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "infra/docker-compose.yml")]
API_PORT = int(os.environ.get("BOOKING_HARNESS_PORT", "8012"))
SETTINGS = Settings.from_env()
WORKER_GROUP = SETTINGS.worker_group


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Proc:
    def __init__(self, name: str, popen: subprocess.Popen, log_path: Path):
        self.name, self.popen, self.log_path = name, popen, log_path

    def alive(self) -> bool:
        return self.popen.poll() is None

    def text(self) -> str:
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""

    def wait_for_log(self, pattern: str, timeout: float = 30.0, start: int = 0) -> re.Match:
        rx = re.compile(pattern)
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = rx.search(self.text(), start)
            if m:
                return m
            if not self.alive():
                raise RuntimeError(f"{self.name} exited ({self.popen.returncode}) waiting for /{pattern}/:\n"
                                   + self.text()[-2000:])
            time.sleep(0.1)
        raise TimeoutError(f"{self.name}: no /{pattern}/ within {timeout}s\n" + self.text()[-2000:])

    def stop(self, timeout: float = 15.0) -> int | None:
        if self.alive():
            self.popen.send_signal(signal.SIGTERM)
            try:
                self.popen.wait(timeout)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait(5)
        return self.popen.returncode

    def kill9(self) -> None:
        if self.alive():
            self.popen.kill()
            self.popen.wait(5)


def _env(extra: dict[str, str] | None) -> dict[str, str]:
    e = dict(os.environ)
    e["PYTHONPATH"] = f"{REPO_ROOT}/services/booking-api:{REPO_ROOT}/services/booking-worker"
    e["PYTHONUNBUFFERED"] = "1"
    e.update(extra or {})
    return e


def start_api(log_dir: Path, *, env: dict[str, str] | None = None, port: int = API_PORT, name: str = "api") -> Proc:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{name}_log.txt"
    fh = open(path, "w")
    p = subprocess.Popen([PY, "-m", "uvicorn", "booking_api.app:app", "--port", str(port), "--log-level", "warning"],
                         cwd=REPO_ROOT / "services/booking-api", env=_env(env), stdout=fh, stderr=subprocess.STDOUT)
    proc = Proc(name, p, path)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1).status_code == 200:
                log(f"started {name} pid={p.pid} port={port}")
                return proc
        except httpx.HTTPError:
            pass
        if not proc.alive():
            raise RuntimeError(f"api exited:\n{proc.text()[-3000:]}")
        time.sleep(0.2)
    raise TimeoutError("api did not become healthy")


def start_worker(log_dir: Path, worker_id: str, *, env: dict[str, str] | None = None, log_name: str | None = None,
                 wait_assigned: bool = True, timeout: float = 40.0) -> Proc:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{log_name or worker_id}_log.txt"
    fh = open(path, "w")
    p = subprocess.Popen([PY, "-m", "booking_worker.worker"], cwd=REPO_ROOT, env=_env({"WORKER_ID": worker_id, **(env or {})}),
                         stdout=fh, stderr=subprocess.STDOUT)
    proc = Proc(worker_id, p, path)
    log(f"started {worker_id} pid={p.pid} env={env or {}}")
    if wait_assigned:
        proc.wait_for_log(r"assigned partitions \[\d", timeout=timeout)
    return proc


# ----------------------------------------------------------------------------------------- MySQL / Kafka CLI
def mysql(sql: str, *, table: bool = True) -> str:
    """Run SQL with the mysql client inside the compose container (genuine client output for evidence files)."""
    cmd = COMPOSE + ["exec", "-T", "-e", "MYSQL_PWD=airbnb", "mysql", "mysql", "-uairbnb", SETTINGS.mysql_database]
    cmd += ["-t"] if table else ["-N", "-B"]
    out = subprocess.run(cmd + ["-e", sql], capture_output=True, text=True, check=True)
    return out.stdout


def mysql_rows(sql: str) -> list[list[str]]:
    return [line.split("\t") for line in mysql(sql, table=False).splitlines() if line]


def kafka_groups(group: str = WORKER_GROUP, *, members: bool = False) -> str:
    cmd = COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
                     "localhost:9092", "--describe", "--group", group] + (["--members"] if members else [])
    r = subprocess.run(cmd, capture_output=True, text=True)
    return (r.stdout + r.stderr).strip()


def parse_lag(describe: str, topic: str = "booking.requested") -> dict[int, dict[str, Any]]:
    out = {}
    for line in describe.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[1] == topic and parts[2].isdigit():
            cur, end, lag = parts[3], parts[4], parts[5]
            out[int(parts[2])] = {"current": None if cur == "-" else int(cur), "end": int(end),
                                  "lag": None if lag == "-" else int(lag)}
    return out


def wait_no_active_members(group: str = WORKER_GROUP, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        text = kafka_groups(group, members=True)
        if "has no active members" in text or "does not exist" in text:
            return
        time.sleep(1)
    raise RuntimeError(f"consumer group {group} still has active members — stop any running booking workers "
                       f"(make booking-worker) before running this demo:\n{kafka_groups(group, members=True)}")


def topic_events(topic: str, predicate: Callable[[dict[str, Any]], bool], timeout: float = 20.0) -> list[dict]:
    """Read a whole topic (fresh group, no commits) and return the JSON values matching predicate."""
    c = Consumer({"bootstrap.servers": SETTINGS.kafka_bootstrap, "group.id": f"booking-evidence-{uuid.uuid4()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest", "enable.partition.eof": True})
    md = c.list_topics(topic, timeout=10)
    parts = [TopicPartition(topic, p, 0) for p in md.topics[topic].partitions]
    ends = {}
    for tp in parts:
        lo, hi = c.get_watermark_offsets(tp, timeout=10)
        ends[tp.partition] = hi
        tp.offset = lo
    c.assign(parts)
    pending = {p for p, hi in ends.items() if hi > 0 and _low(c, topic, p) < hi}
    found: list[dict] = []
    deadline = time.time() + timeout
    while pending and time.time() < deadline:
        msg = c.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pending.discard(msg.partition())
            continue
        raw = msg.value()
        try:
            value = json.loads(raw) if raw is not None else None
        except ValueError:
            value = None
        if not isinstance(value, dict):   # tombstones / malformed records from the DLQ negative tests
            value = {"_raw": raw.decode(errors="replace") if raw is not None else None}
        value["_meta"] = {"partition": msg.partition(), "offset": msg.offset(),
                          "key": msg.key().decode() if msg.key() else None}
        if predicate(value):
            found.append(value)
        if msg.offset() + 1 >= ends[msg.partition()]:
            pending.discard(msg.partition())
    c.close()
    return found


def _low(c: Consumer, topic: str, p: int) -> int:
    return c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[0]


# ----------------------------------------------------------------------------------------- API client helpers
def token(sub: str, role: str | None = None) -> str:
    return mint(SETTINGS, sub, role or ("host" if sub.startswith("host-") else "guest"))


def api(port: int = API_PORT) -> str:
    return f"http://127.0.0.1:{port}"


def create_booking(client: httpx.Client, guest: str, property_id: str, check_in: date, check_out: date,
                   guest_count: int = 2, port: int = API_PORT) -> httpx.Response:
    return client.post(f"{api(port)}/bookings/create", headers={"Authorization": f"Bearer {token(guest)}",
                                                                "Idempotency-Key": str(uuid.uuid4())},
                       json={"property_id": property_id, "check_in": check_in.isoformat(),
                             "check_out": check_out.isoformat(), "guest_count": guest_count})


def wait_status(rids: list[str], *, not_in: tuple[str, ...] = ("requested",), timeout: float = 60.0) -> dict[str, str]:
    deadline = time.time() + timeout
    ids_sql = ",".join(f"'{r}'" for r in rids)
    while True:
        rows = dict((r[0], r[1]) for r in mysql_rows(
            f"SELECT reservation_id, status FROM reservations WHERE reservation_id IN ({ids_sql})"))
        if all(rows.get(r) not in not_in for r in rids) or time.time() > deadline:
            return rows
        time.sleep(0.2)


def free_window(property_id: str, nights: int, start: date | None = None) -> tuple[date, date]:
    """First window of `nights` nights (from `start`, default 2027-06-01) with no blocks on the property."""
    d = start or date(2027, 6, 1)
    taken = {r[0] for r in mysql_rows(
        f"SELECT night_date FROM availability_blocks WHERE property_id = '{property_id}' AND night_date >= '{d}'")}
    while any((d + timedelta(days=i)).isoformat() in taken for i in range(nights)):
        d += timedelta(days=1)
    return d, d + timedelta(days=nights)


def git_rev() -> str:
    try:
        rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                             cwd=REPO_ROOT).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--", "services", "db"], capture_output=True,
                               text=True, cwd=REPO_ROOT).stdout.strip()
        return rev + ("+dirty" if dirty else "")
    except OSError:
        return "unknown"


def write(path: Path, content: str | dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if isinstance(content, str) else json.dumps(content, indent=2, default=str) + "\n")
