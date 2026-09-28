#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="$PROJECT_ROOT/kafka"

if [ ! -f "$KAFKA_DIR/config/kraft/server.properties" ]; then
    echo "ERROR: Kafka configuration not found. Please run ./scripts/setup_kafka.sh first."
    exit 1
fi

echo "============================================================"
echo " Starting Apache Kafka (KRaft mode) on localhost:9092"
echo " Keep this terminal open for the entire session."
echo " Press Ctrl+C to stop the broker."
echo "============================================================"

exec "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/kraft/server.properties"
