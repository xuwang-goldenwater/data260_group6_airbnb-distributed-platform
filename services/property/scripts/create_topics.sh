#!/usr/bin/env bash
# Run from the repository root, after Kafka is healthy.
set -eu
for topic in property.created property.updated property.archived property.saved property.viewed; do
  docker compose -f infra/docker-compose.yml exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done
