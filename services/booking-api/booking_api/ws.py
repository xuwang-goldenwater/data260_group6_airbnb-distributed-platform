"""WebSocket /bookings/stream gateway.

A Kafka consumer thread (group BOOKING_WS_CONSUMER_GROUP, default `booking-status`) reads booking.updated and
property.availability.updated, validates each envelope, and pushes it to connected clients:
- booking.updated → the reservation's guest and host;
- property.availability.updated → clients that sent {"action": "subscribe_calendar", "property_id": ...}.
Clients recover missed pushes with /bookings/get or /bookings/calendar after reconnecting.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError
from fastapi import WebSocket

from booking_core import envelope as env
from booking_core.config import Settings
from booking_core.events import T_AVAILABILITY, T_DLQ, T_UPDATED
from booking_core.kafka_producer import EventProducer

log = logging.getLogger("booking.ws")


@dataclass(eq=False)
class Client:
    ws: WebSocket
    sub: str
    calendars: set[str] = field(default_factory=set)


class ConnectionManager:
    def __init__(self) -> None:
        self._clients: set[Client] = set()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.pushed = 0

    def add(self, c: Client) -> None:
        self._clients.add(c)

    def remove(self, c: Client) -> None:
        self._clients.discard(c)

    def targets(self, event: dict[str, Any]) -> list[Client]:
        p = event["payload"]
        if event["event_type"] == T_UPDATED:
            return [c for c in self._clients if c.sub in (p["guest_id"], p["host_id"])]
        if event["event_type"] == T_AVAILABILITY:
            return [c for c in self._clients if p["property_id"] in c.calendars]
        return []

    async def dispatch(self, event: dict[str, Any]) -> None:
        text = json.dumps(event)
        for c in self.targets(event):
            try:
                await c.ws.send_text(text)
                self.pushed += 1
            except Exception:
                self.remove(c)

    def dispatch_threadsafe(self, event: dict[str, Any]) -> None:
        if self.loop is not None and self._clients:
            asyncio.run_coroutine_threadsafe(self.dispatch(event), self.loop)

    @property
    def count(self) -> int:
        return len(self._clients)


class StatusConsumer(threading.Thread):
    def __init__(self, settings: Settings, manager: ConnectionManager, producer: EventProducer):
        super().__init__(name="ws-status-consumer", daemon=True)
        self._s, self._m, self._producer = settings, manager, producer
        self._stop_evt = threading.Event()

    def run(self) -> None:
        group = self._s.ws_consumer_group
        consumer = Consumer({"bootstrap.servers": self._s.kafka_bootstrap, "group.id": group,
                             "enable.auto.commit": False, "auto.offset.reset": "latest",
                             "fetch.wait.max.ms": 50, "session.timeout.ms": 10000})
        consumer.subscribe([T_UPDATED, T_AVAILABILITY],
                           on_assign=lambda c, parts: log.info("ws gateway assigned %d partitions", len(parts)))
        log.info("ws gateway consuming %s,%s as group=%s", T_UPDATED, T_AVAILABILITY, group)
        try:
            while not self._stop_evt.is_set():
                msg = consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("kafka error: %s", msg.error())
                    continue
                key = msg.key().decode() if msg.key() else None
                try:
                    event = json.loads(msg.value())
                    env.validate(event, self._s.envelope_schema_path, key=key, check_key=True)
                except (ValueError, env.EnvelopeError) as exc:
                    code = exc.code if isinstance(exc, env.EnvelopeError) else "INVALID_JSON"
                    raw: Any
                    try:
                        raw = json.loads(msg.value())
                    except ValueError:
                        raw = msg.value().decode(errors="replace")
                    try:
                        self._producer.publish(T_DLQ, key, env.dlq_record(
                            original_topic=msg.topic(), original_partition=msg.partition(),
                            original_offset=msg.offset(), original_key=key, original_event=raw, error_code=code,
                            error_message=str(exc), retry_count=0, consumer_group=group, producer="booking-api"),
                            validate=True)
                    except Exception:
                        log.exception("could not write DLQ record")
                    consumer.commit(message=msg, asynchronous=True)
                    continue
                self._m.dispatch_threadsafe(event)
                consumer.commit(message=msg, asynchronous=True)
        finally:
            consumer.close()

    def stop(self) -> None:
        self._stop_evt.set()
