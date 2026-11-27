"""Small Kafka helpers shared by the AI workers (confluent-kafka, same client as the Booking service)."""
from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from confluent_kafka import Consumer, Producer

from ai_core import envelope as env
from ai_core.config import T_DLQ, Settings

log = logging.getLogger("ai.kafka")


def make_producer(s: Settings, client_id: str) -> Producer:
    # acks=all + idempotence: a confirmed event is on every in-sync replica and never written twice
    return Producer({"bootstrap.servers": s.kafka_bootstrap, "client.id": client_id,
                     "acks": "all", "enable.idempotence": True})


def make_consumer(s: Settings, group_id: str, topics: list[str]) -> Consumer:
    c = Consumer({
        "bootstrap.servers": s.kafka_bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,        # we commit only after the work is saved and published
        "auto.offset.reset": "earliest",
        # a crashed member is noticed after 10 s instead of the 45 s default, so a restarted
        # worker gets its partitions back quickly
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })
    c.subscribe(topics)
    return c


def publish(producer: Producer, outgoing: Iterable[tuple[str, str, dict[str, Any]]]) -> list[str]:
    """Produce every event keyed by trace_id and wait until Kafka confirms. Returns the trace_ids sent."""
    sent = []
    for topic, key, event in outgoing:
        producer.produce(topic, key=key.encode(), value=json.dumps(event).encode())
        sent.append(key)
    if sent:
        remaining = producer.flush(10)
        if remaining:
            raise RuntimeError(f"{remaining} event(s) not confirmed by Kafka")
    return sent


def send_to_dlq(producer: Producer, msg, group_id: str, err: env.EnvelopeError) -> None:
    """Permanently invalid record -> platform.dlq with the original bytes and the reason."""
    raw = msg.value().decode(errors="replace") if msg.value() else None
    key = msg.key().decode(errors="replace") if msg.key() else "unknown"
    payload = {"original_topic": msg.topic(), "original_partition": msg.partition(),
               "original_offset": msg.offset(), "original_key": key, "original_event": raw,
               "error_code": err.code, "error_message": err.message, "retry_count": 0,
               "failed_at": env.utc_now_iso(), "consumer_group": group_id}
    event = env.build(event_type=T_DLQ, trace_id=key, actor_id=group_id, entity_type="record",
                      entity_id=f"{msg.topic()}:{msg.partition()}:{msg.offset()}", payload=payload,
                      idempotency_key=f"dlq:{msg.topic()}:{msg.partition()}:{msg.offset()}")
    publish(producer, [(T_DLQ, key, event)])
    log.warning("sent to DLQ: %s %s", err.code, err.message)


class KafkaUnavailable(RuntimeError):
    pass


def ensure_topics(s: Settings, topics: list[str], partitions: int = 3, replication: int = 1,
                  wait_s: float = 60) -> list[str]:
    """Wait for the broker (it needs ~20 s after `docker compose up`), then create missing topics
    (3 partitions so three debates can spread across them). Returns the ones created."""
    import time
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap, "log_level": 0})   # quiet while waiting
    deadline = time.time() + wait_s
    while True:
        try:
            existing = set(admin.list_topics(timeout=3).topics)
            break
        except KafkaException as exc:
            if time.time() > deadline:
                raise KafkaUnavailable(f"no Kafka broker at {s.kafka_bootstrap} after {wait_s:.0f}s") from exc
            time.sleep(2)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=replication) for t in topics if t not in existing]
    for topic, fut in (admin.create_topics(new).items() if new else []):
        try:
            fut.result(15)
        except Exception as exc:          # created by someone else in the meantime is fine
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return [t.topic for t in new]
