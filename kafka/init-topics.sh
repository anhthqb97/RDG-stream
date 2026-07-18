#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
KAFKA_HOME="/opt/kafka"

create_topic() {
  local topic="$1"
  local partitions="$2"
  "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
}

create_topic "metrics.raw" 3
create_topic "metrics.dlq" 1
create_topic "events.lifecycle" 1

echo "Kafka topics ready."
