"""Run with python -m platform_service.worker. Safe replay, outbox and DLQ acknowledgement."""
import json
import logging
import os

from confluent_kafka import Consumer, Producer
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError

from shared.consumer import TOPICS, RecordMeta, process_record, validate
from .store import initialize, ingest

log = logging.getLogger(__name__)
TRANSIENT = (AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError)


class Publisher:
    def __init__(self, bootstrap):
        self.producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True})

    def __call__(self, topic, key, event):
        validate(event, key=key, check_key=True)
        delivered = []
        self.producer.produce(topic, key=key, value=json.dumps(event),
                              on_delivery=lambda error, message: delivered.append(error))
        remaining = self.producer.flush(10)
        if remaining or not delivered or delivered[0] is not None:
            raise RuntimeError(f'Kafka acknowledgement failed: {delivered}')


def flush_outbox(db, publish):
    for row in db.messages.find({'publish_pending': True}):
        publish('message.created', row['conversation_id'], row['event'])
        db.messages.update_one({'_id': row['_id']}, {'$set': {'publish_pending': False, 'delivery_status': 'sent'}})


def main():
    logging.basicConfig(level=logging.INFO)
    db = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017'), serverSelectionTimeoutMS=5000)[os.getenv('MONGO_DATABASE', 'airbnb')]
    initialize(db)
    group = os.getenv('PLATFORM_CONSUMER_GROUP', 'analytics')
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    publish = Publisher(bootstrap)
    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': group,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    consumer.subscribe(list(TOPICS) + ['platform.dlq'])
    try:
        while True:
            try:
                flush_outbox(db, publish)
            except (RuntimeError, *TRANSIENT):
                log.exception('Outbox retained for retry')
            record = consumer.poll(1)
            if record is None:
                continue
            if record.error():
                log.error('Kafka: %s', record.error())
                continue
            meta = RecordMeta(record.topic(), record.partition(), record.offset(),
                              record.key().decode(errors='replace') if record.key() else None)
            try:
                process_record(record.value(), meta, lambda event: ingest(db, event), publish,
                               lambda: consumer.commit(message=record, asynchronous=False), group=group,
                               is_transient=lambda exc: isinstance(exc, TRANSIENT))
            except Exception:
                # Never progress this partition after an unacknowledged DLQ/DB failure.
                # Restart from its committed offset instead of committing a later record.
                log.exception('Fatal processing failure; source offset remains uncommitted')
                raise
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
