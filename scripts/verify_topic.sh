#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="$PROJECT_ROOT/kafka"

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPIC_NAME="${2:-stock-trades}"

echo "Describing topic '$TOPIC_NAME' on $BOOTSTRAP_SERVER..."

"$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --topic "$TOPIC_NAME"
