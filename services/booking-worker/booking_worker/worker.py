"""Booking worker: consumes booking.requested and holds nights in MySQL (docs/api/booking.md "Booking Worker").

Separate process from the API. Several instances share consumer group `booking-worker`.
Auto-commit is off; the offset is committed only after the MySQL transaction (or the DLQ write) succeeds.

Run: WORKER_ID=worker-1 python -m booking_worker.worker   (PYTHONPATH=services/booking-api:services/booking-worker)
Fault injection (demos): WORKER_FAULT=crash_after_db_commit, WORKER_TRANSIENT_FAULTS=<n>.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any

import pymysql
from confluent_kafka import Consumer, KafkaError, TopicPartition

from booking_core import envelope as env
from booking_core import events, outbox
from booking_core.config import Settings
from booking_core.dao import availability as avail_dao
from booking_core.dao import history as history_dao
from booking_core.dao import processed_events as processed_dao
from booking_core.dao import reservations as res_dao
from booking_core.db import Database, is_duplicate_key, is_transient
from booking_core.kafka_producer import EventProducer, KafkaEventProducer
from booking_core.timeutil import nights_between, utcnow

from .consumer_utils import (PermanentError, RecordMeta, RetryExhausted, decode, raw_event, run_with_retry,
                             send_to_dlq)

log = logging.getLogger("booking.worker")
ACTOR = "booking-worker"


class ReservationMissing(Exception):
    pass


@dataclass
class Outcome:
    kind: str                       # held | rejected | duplicate | skipped | dlq
    reservation: dict[str, Any] | None = None
    history: dict[str, Any] | None = None
    retries: int = 0


class BookingWorker:
    def __init__(self, settings: Settings, db: Database, producer: EventProducer, *, sleep=time.sleep):
        self.s, self.db, self.producer, self.sleep = settings, db, producer, sleep
        self.group = settings.worker_group
        self._faults_left = settings.worker_transient_faults

    # ------------------------------------------------------------------ one record (testable without Kafka)
    def handle(self, meta: RecordMeta, value: bytes | None) -> Outcome:
        try:                                                                   # step 1
            event = decode(value, meta, self.s.envelope_schema_path, expected_type=events.T_REQUESTED)
        except env.EnvelopeError as exc:
            send_to_dlq(self.producer, meta, raw_event(value), error_code=exc.code, error_message=exc.message,
                        retry_count=0, consumer_group=self.group, producer_name=ACTOR)
            return Outcome("dlq")
        rid = event["payload"]["reservation_id"]
        try:
            outcome, retries = self._with_not_found_retry(event)
        except RetryExhausted as exc:
            send_to_dlq(self.producer, meta, event, error_code="TRANSIENT_RETRY_EXHAUSTED",
                        error_message=repr(exc.last), retry_count=exc.retries, consumer_group=self.group,
                        producer_name=ACTOR)
            return Outcome("dlq", retries=exc.retries)
        except ReservationMissing:
            send_to_dlq(self.producer, meta, event, error_code="RESERVATION_NOT_FOUND",
                        error_message=f"reservation {rid} not found after 1 retry", retry_count=1,
                        consumer_group=self.group, producer_name=ACTOR)
            return Outcome("dlq", retries=1)
        except PermanentError as exc:
            send_to_dlq(self.producer, meta, event, error_code=exc.code, error_message=exc.message, retry_count=0,
                        consumer_group=self.group, producer_name=ACTOR)
            return Outcome("dlq")
        outcome.retries = retries
        log.info("processed reservation_id=%s outcome=%s retries=%d partition=%s offset=%s trace_id=%s",
                 rid, outcome.kind, retries, meta.partition, meta.offset, event["trace_id"])

        if outcome.kind in ("held", "rejected") and self.s.worker_fault == "crash_after_db_commit":
            log.critical("FAULT INJECTION crash_after_db_commit: MySQL committed reservation_id=%s; exiting before "
                         "publish and before the Kafka offset commit (partition=%s offset=%s)",
                         rid, meta.partition, meta.offset)
            logging.shutdown()
            os._exit(137)

        res = outcome.reservation                                              # step 10
        if res is not None and res.get("publish_pending"):
            if outcome.kind in ("held", "rejected"):
                evts = events.events_for_transition(res, outcome.history, producer=ACTOR)
            else:   # redelivery after a crash: the transition is committed but its events may not be published
                with self.db.transaction() as conn:
                    last = history_dao.last_for(conn, rid)
                evts = events.pending_events(res, last, producer=ACTOR, attempt=2)
            if evts:
                outbox.publish_and_clear(self.db, self.producer, res, evts)
        return outcome

    def _with_not_found_retry(self, event: dict[str, Any]) -> tuple[Outcome, int]:
        label = event["payload"]["reservation_id"]
        try:
            return run_with_retry(lambda: self._process(event), is_transient=is_transient, sleep=self.sleep,
                                  label=label)
        except ReservationMissing:
            self.sleep(1.0)   # covers a read-replica lag; then DLQ
            return run_with_retry(lambda: self._process(event), is_transient=is_transient, sleep=self.sleep,
                                  label=label)

    # ------------------------------------------------------------------ steps 2-9 in one MySQL transaction
    def _process(self, event: dict[str, Any]) -> Outcome:
        if self._faults_left > 0:
            self._faults_left -= 1
            raise pymysql.err.OperationalError(1213, "Deadlock found when trying to get lock (INJECTED)")
        p = event["payload"]
        rid, key = p["reservation_id"], event["idempotency_key"]
        with self.db.transaction() as conn:                                    # step 2 BEGIN
            if processed_dao.exists(conn, key):                                # step 3
                return Outcome("duplicate", res_dao.get(conn, rid))
            res = res_dao.get(conn, rid, for_update=True)                      # step 4
            if res is None:
                raise ReservationMissing(rid)
            if res["property_id"] != p["property_id"] or res["guest_id"] != p["guest_id"]:
                raise PermanentError("PAYLOAD_MISMATCH", "event payload does not match the stored reservation")
            if res["status"] != "requested":                                   # step 5
                processed_dao.insert(conn, key, ACTOR, utcnow(), ignore_duplicate=True)
                return Outcome("skipped", res)
            now = utcnow()
            nights = nights_between(res["check_in"], res["check_out"])
            try:                                                               # step 6
                avail_dao.insert_blocks(conn, res["property_id"], nights, rid, now)
                to_status, reason = "pending_host", None                       # 7a
            except pymysql.err.IntegrityError as exc:
                if not is_duplicate_key(exc):
                    raise
                to_status, reason = "rejected", "DATES_UNAVAILABLE"            # 7b (no nights stored)
            rows = res_dao.update_status(conn, rid, from_status="requested", expected_version=res["version"],
                                         to_status=to_status, reason=reason, now=now)
            if rows != 1:
                raise pymysql.err.OperationalError(1213, "reservation changed under FOR UPDATE")
            hist = history_dao.insert(conn, reservation_id=rid, from_status="requested", to_status=to_status,
                                      actor_id=ACTOR, reason=reason, trace_id=event["trace_id"], changed=now)
            processed_dao.insert(conn, key, ACTOR, now)                        # step 8
            after = res_dao.get(conn, rid)
        return Outcome("held" if to_status == "pending_host" else "rejected", after, hist)   # step 9 COMMIT done

    # ------------------------------------------------------------------ Kafka loop
    def run(self) -> None:
        consumer = Consumer({
            "bootstrap.servers": self.s.kafka_bootstrap,
            "group.id": self.group,
            "group.instance.id": self.s.worker_id,          # static membership: a restart rejoins without rebalance
            "client.id": self.s.worker_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 10000,
            "partition.assignment.strategy": "cooperative-sticky",
            "fetch.wait.max.ms": 50,
        })
        stopping = {"flag": False}

        def _stop(signum, _frame):
            log.info("signal %s: stopping after the current record", signum)
            stopping["flag"] = True

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        def on_assign(c, parts):
            log.info("assigned partitions %s", [p.partition for p in parts])

        def on_revoke(c, parts):
            log.info("revoked partitions %s", [p.partition for p in parts])

        consumer.subscribe([events.T_REQUESTED], on_assign=on_assign, on_revoke=on_revoke)
        log.info("worker started id=%s group=%s fault=%s transient_faults=%d", self.s.worker_id, self.group,
                 self.s.worker_fault or "-", self._faults_left)
        try:
            while not stopping["flag"]:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("kafka error: %s", msg.error())
                    continue
                meta = RecordMeta(msg.topic(), msg.partition(), msg.offset(),
                                  msg.key().decode() if msg.key() else None)
                try:
                    self.handle(meta, msg.value())
                except Exception:
                    # MySQL/Kafka down beyond retries and the DLQ write failed: do NOT commit; re-read the record.
                    log.exception("record not completed; seeking back partition=%s offset=%s", meta.partition,
                                  meta.offset)
                    time.sleep(1.0)
                    consumer.seek(TopicPartition(meta.topic, meta.partition, meta.offset))
                    continue
                consumer.commit(message=msg, asynchronous=False)               # step 11
                log.info("committed offset partition=%s next_offset=%s", meta.partition, meta.offset + 1)
        finally:
            consumer.close()
            log.info("worker stopped id=%s", self.s.worker_id)


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format=f"%(asctime)s %(levelname)s [{settings.worker_id}] %(name)s %(message)s")
    db = Database(settings, pool_size=4)
    producer = KafkaEventProducer(settings, client_id=f"booking-worker-{settings.worker_id}")
    try:
        BookingWorker(settings, db, producer).run()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
