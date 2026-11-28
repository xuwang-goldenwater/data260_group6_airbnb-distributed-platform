"""Separate Kafka worker; reuses the platform validation/retry/DLQ boundary."""
import json
import logging

from shared.consumer import RecordMeta, process_record
from .review_ratings import GROUP, RatingProjection, transient


class AcknowledgedPublisher:
    def __init__(self, bootstrap):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "acks": "all", "message.timeout.ms": 5000})

    def __call__(self, topic, key, event):
        delivered = []
        self.producer.produce(topic, key=key, value=json.dumps(event).encode(),
                              on_delivery=lambda error, message: delivered.append(error))
        if self.producer.flush(6) or not delivered or delivered[0] is not None:
            raise RuntimeError("Kafka acknowledgement failed; source offset must not advance")


def handle(record, projection, publish, commit, *, sleep=None):
    meta = RecordMeta(record.topic(), record.partition(), record.offset(),
                      record.key().decode(errors="replace") if record.key() is not None else None)
    options = {"sleep": sleep} if sleep is not None else {}
    return process_record(record.value(), meta, projection.apply, publish, commit,
                          group=GROUP, is_transient=transient, **options)


def run(settings, repository):
    from confluent_kafka import Consumer
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": GROUP,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "auto.offset.reset": "earliest", "allow.auto.create.topics": False})
    publisher, projection = AcknowledgedPublisher(settings.kafka_bootstrap), RatingProjection(repository)
    consumer.subscribe(["review.created"])
    logging.getLogger(__name__).info("property-rating ready; subscribed to review.created")
    try:
        while True:
            record = consumer.poll(1)
            if record is None:
                continue
            if record.error():
                raise RuntimeError("Kafka polling failed: " + str(record.error()))
            # Never commit a later record over an unacknowledged DB/DLQ failure.
            result = handle(record, projection, publisher,
                            lambda: consumer.commit(message=record, asynchronous=False))
            logging.getLogger(__name__).info("%s partition=%d offset=%d", result, record.partition(), record.offset())
    finally:
        consumer.close()
