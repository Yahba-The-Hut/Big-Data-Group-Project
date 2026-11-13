#!/usr/bin/env bash
# Run in Ubuntu (WSL2) with Docker Desktop WSL integration on.
# Starts Kafka (or restarts the existing container) and creates topic1/topic2.
set -euo pipefail
IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"   # change tag if the pull fails

if docker ps -a --format '{{.Names}}' | grep -qx kafka; then
  docker start kafka >/dev/null
  echo "Started existing 'kafka' container."
else
  docker pull "$IMAGE"
  docker run -d --name kafka -p 9092:9092 "$IMAGE" >/dev/null
  echo "Created 'kafka' container from $IMAGE."
fi

echo "Waiting for broker..."
for i in $(seq 1 30); do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done

for t in topic1 topic2; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$t" \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
