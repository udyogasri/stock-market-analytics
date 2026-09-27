#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="$PROJECT_ROOT/kafka"

echo "Stopping Kafka broker gracefully..."
"$KAFKA_DIR/bin/kafka-server-stop.sh"
echo "Kafka stop signal sent."
