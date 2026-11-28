#!/usr/bin/env bash
# Shared topic names/partition count; existing topics are retained unchanged.
set -eu
for topic in review.created platform.dlq; do
  docker compose -f infra/docker-compose.yml exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done
