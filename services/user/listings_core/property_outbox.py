from contextlib import suppress
from datetime import datetime, timezone
import hashlib
import json
import time

import jsonschema

from .config import ROOT


class KafkaPublisher:
    def __init__(self, settings):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap,
                                  "client.id": "property-outbox", "acks": "all",
                                  "enable.idempotence": True, "message.timeout.ms": 5000})

    def publish(self, topic, key, event):
        errors = []
        def delivery(error, message):
            if error:
                errors.append(str(error))
        self.producer.produce(topic, key=key.encode(), value=json.dumps(event).encode(), on_delivery=delivery)
        if self.producer.flush(6) or errors:
            raise RuntimeError("Kafka did not acknowledge the property event")


def relay_one(repository, publisher, *, sleep=time.sleep):
    """Ordered, at-least-once relay. Every retry preserves the stored envelope."""
    schema = json.loads((ROOT / "shared/envelope.schema.json").read_text())
    lock = "role-a-property-outbox-" + hashlib.sha256(repository.settings.mysql_database.encode()).hexdigest()[:16]
    with repository.connection(write=True) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (lock,))
        if cursor.fetchone()["acquired"] != 1:
            return {"status": "busy"}
        try:
            cursor.execute("SELECT * FROM property_event_outbox WHERE published_datetime IS NULL ORDER BY outbox_id LIMIT 1 FOR UPDATE")
            row = cursor.fetchone()
            if row is None:
                return {"status": "empty"}
            event = json.loads(row["envelope"]) if isinstance(row["envelope"], str) else row["envelope"]
            jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(event)
            if event["event_id"] != row["event_id"] or event["event_type"] != row["topic"] or event["payload"]["property_id"] != row["property_id"]:
                raise ValueError("Outbox routing metadata differs from the envelope")
            for attempt in range(4):
                cursor.execute("UPDATE property_event_outbox SET publish_attempts=publish_attempts+1 WHERE outbox_id=%s", (row["outbox_id"],))
                try:
                    publisher.publish(row["topic"], row["property_id"], event)
                    break
                except Exception:
                    if attempt == 3:
                        conn.commit()
                        return {"status": "pending_retry", "event_id": row["event_id"]}
                    sleep((0.2, 1, 5)[attempt])
            cursor.execute("UPDATE property_event_outbox SET published_datetime=%s WHERE outbox_id=%s",
                           (datetime.now(timezone.utc).replace(tzinfo=None), row["outbox_id"]))
            # Commit the durable mark while still holding the connection-level relay lock.
            conn.commit()
            return {"status": "published", "event_id": row["event_id"], "topic": row["topic"]}
        finally:
            with suppress(Exception):
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock,))
