import argparse
import os

from pymongo import MongoClient
from shared.consumer import TOPICS
from .store import initialize, seed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'seed', 'topics'])
    args = parser.parse_args()
    if args.action == 'topics':
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaException
        admin = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')})
        for topic, future in admin.create_topics([NewTopic(t, 3, 1) for t in (*TOPICS, 'platform.dlq')]).items():
            try:
                future.result()
            except KafkaException as exc:
                if exc.args[0].name() != 'TOPIC_ALREADY_EXISTS':
                    raise
            print(topic)
    else:
        db = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017'))[os.getenv('MONGO_DATABASE', 'airbnb')]
        initialize(db)
        if args.action == 'seed':
            seed(db)
        print('Mongo indexes/seed ready')


if __name__ == '__main__':
    main()
