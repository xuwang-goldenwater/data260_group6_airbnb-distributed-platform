"""Kafka producer wrapper: validates every envelope against the shared schema before producing and waits for
the broker acknowledgement (acks=all, idempotent producer)."""
from __future__ import annotations

import itertools
import json
import logging
import threading
from typing import Any, Protocol

from confluent_kafka import KafkaException, Producer

from . import envelope as env
from .config import Settings

log = logging.getLogger("booking.producer")


class PublishError(Exception):
    pass


class EventProducer(Protocol):
    def publish(self, topic: str, key: str | None, event: dict[str, Any], *, validate: bool = True) -> None: ...


class KafkaEventProducer:
    def __init__(self, settings: Settings, *, client_id: str, spread_partitions: bool = False,
                 timeout_s: float = 10.0):
        self._settings = settings
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 0,
            "message.timeout.ms": int(timeout_s * 1000),
        })
        self._timeout = timeout_s
        self._spread = spread_partitions
        self._rr = itertools.count()
        self._partitions: dict[str, int] = {}
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name=f"{client_id}-poll", daemon=True)
        self._poller.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.05)

    def _partition_count(self, topic: str) -> int:
        if topic not in self._partitions:
            md = self._producer.list_topics(topic, timeout=5)
            self._partitions[topic] = max(1, len(md.topics[topic].partitions))
        return self._partitions[topic]

    def publish(self, topic: str, key: str | None, event: dict[str, Any], *, validate: bool = True) -> None:
        if validate:
            env.validate(event, self._settings.envelope_schema_path, key=key,
                         check_key=event.get("event_type") in env.KEY_FIELD)
        done = threading.Event()
        result: dict[str, Any] = {}

        def on_delivery(err, msg):
            result["err"] = err
            result["partition"] = msg.partition() if msg is not None else None
            done.set()

        kwargs: dict[str, Any] = {}
        if self._spread and topic == "booking.requested":
            # TEST ONLY (double-booking race demo): bypass key-hash partitioning so several workers
            # process requests for the same property at the same time. The key itself is unchanged.
            kwargs["partition"] = next(self._rr) % self._partition_count(topic)
        try:
            self._producer.produce(topic, key=key.encode() if key is not None else None,
                                   value=json.dumps(event, separators=(",", ":")).encode(),
                                   on_delivery=on_delivery, **kwargs)
        except (KafkaException, BufferError) as exc:
            raise PublishError(str(exc)) from exc
        if not done.wait(self._timeout + 1):
            raise PublishError(f"no delivery report for {topic} within {self._timeout}s")
        if result.get("err") is not None:
            raise PublishError(str(result["err"]))

    def close(self) -> None:
        self._producer.flush(5)
        self._stop.set()


class InMemoryProducer:
    """Test double that records events (still validated against the shared schema)."""

    def __init__(self, settings: Settings | None = None, *, fail: bool = False):
        self.settings = settings
        self.sent: list[tuple[str, str | None, dict[str, Any]]] = []
        self.fail = fail
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str | None, event: dict[str, Any], *, validate: bool = True) -> None:
        if validate and self.settings is not None:
            env.validate(event, self.settings.envelope_schema_path, key=key,
                         check_key=event.get("event_type") in env.KEY_FIELD)
        if self.fail:
            raise PublishError("injected publish failure")
        with self._lock:
            self.sent.append((topic, key, event))

    def topics(self) -> list[str]:
        return [t for t, _, _ in self.sent]

    def close(self) -> None:
        pass
