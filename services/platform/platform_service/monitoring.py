"""Read actual Kafka committed offsets/high watermarks, without joining the consumer group."""
import os
from confluent_kafka import Consumer, TopicPartition
from shared.consumer import TOPICS


def consumer_lag():
    consumer = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                         'group.id': os.getenv('PLATFORM_CONSUMER_GROUP', 'analytics'), 'enable.auto.commit': False})
    try:
        metadata = consumer.list_topics(timeout=3)
        partitions = [TopicPartition(name, part) for name, topic in metadata.topics.items()
                      if name in TOPICS or name == 'platform.dlq' for part in topic.partitions]
        committed = consumer.committed(partitions, timeout=3)
        rows = []
        for part in committed:
            low, high = consumer.get_watermark_offsets(part, timeout=3)
            rows.append(dict(topic=part.topic, partition=part.partition, committed_offset=part.offset,
                             high_watermark=high, lag=max(0, high - (part.offset if part.offset >= 0 else low))))
        return {'status': 'ok', 'partitions': rows, 'total': sum(r['lag'] for r in rows)}
    except Exception as exc:
        return {'status': 'unavailable', 'error': type(exc).__name__, 'partitions': [], 'total': None}
    finally:
        consumer.close()
