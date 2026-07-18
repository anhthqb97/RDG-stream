#!/bin/bash
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"

echo "==> Creating Kafka topics..."
bash /kafka/init-topics.sh

echo "==> Applying Cassandra schema..."
cqlsh "${CASSANDRA_HOST}" -f /cassandra/schema.cql

echo "==> Init complete."