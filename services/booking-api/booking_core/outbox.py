"""Publishes the events owed by a reservation and clears publish_pending (the 'outbox' flag).

Used inline right after a commit, and by the background publisher that retries rows still pending.
"""
from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import Any

from . import events
from .dao import history as history_dao
from .dao import idempotency as idem_dao
from .dao import reservations as res_dao
from .db import Database
from .kafka_producer import EventProducer, PublishError
from .timeutil import utcnow

log = logging.getLogger("booking.outbox")


def publish_and_clear(db: Database, producer: EventProducer, res: dict[str, Any],
                      evts: list[tuple[str, str, dict[str, Any]]]) -> bool:
    try:
        for topic, key, event in evts:
            producer.publish(topic, key, event)
    except PublishError as exc:
        log.warning("publish failed reservation_id=%s version=%s err=%s (outbox will retry)",
                    res["reservation_id"], res["version"], exc)
        return False
    try:   # best effort: the transaction is already committed; never let this make the caller retry the work
        with db.transaction() as conn:
            res_dao.clear_publish_pending(conn, res["reservation_id"], res["version"])
    except Exception as exc:
        log.warning("could not clear publish_pending reservation_id=%s (outbox will re-publish; consumers dedup): %s",
                    res["reservation_id"], exc)
    for topic, _, event in evts:
        log.info("published topic=%s idempotency_key=%s trace_id=%s", topic, event["idempotency_key"],
                 event["trace_id"])
    return True


def republish_pending(db: Database, producer: EventProducer, *, min_age_s: float, producer_name: str,
                      limit: int = 100) -> int:
    with db.transaction() as conn:
        rows = res_dao.list_publish_pending(conn, utcnow() - timedelta(seconds=min_age_s), limit)
        work = [(r, history_dao.last_for(conn, r["reservation_id"])) for r in rows]
    done = 0
    for res, last in work:
        evts = events.pending_events(res, last, producer=producer_name, attempt=2)
        if evts and publish_and_clear(db, producer, res, evts):
            done += 1
    return done


class OutboxPublisher(threading.Thread):
    def __init__(self, db: Database, producer: EventProducer, *, interval_s: float, min_age_s: float,
                 producer_name: str = "booking-api"):
        super().__init__(name="outbox-publisher", daemon=True)
        self._db, self._producer = db, producer
        self._interval, self._min_age, self._name = interval_s, min_age_s, producer_name
        self._stop_evt = threading.Event()

    def run(self) -> None:
        while not self._stop_evt.wait(self._interval):
            try:
                n = republish_pending(self._db, self._producer, min_age_s=self._min_age,
                                      producer_name=self._name)
                if n:
                    log.info("outbox republished %d reservation(s)", n)
                with self._db.transaction() as conn:
                    idem_dao.purge(conn, utcnow() - timedelta(hours=24))
            except Exception:  # keep the publisher alive across DB/Kafka outages
                log.exception("outbox cycle failed")

    def stop(self) -> None:
        self._stop_evt.set()
