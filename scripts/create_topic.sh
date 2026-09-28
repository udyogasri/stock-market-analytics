#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="$PROJECT_ROOT/kafka"

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPIC_NAME="${2:-stock-trades}"
PARTITIONS="${3:-12}"
REPLICATION_FACTOR="${4:-1}"

echo "Creating topic '$TOPIC_NAME' with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR on $BOOTSTRAP_SERVER..."

"$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"

echo "Topic creation command completed."
