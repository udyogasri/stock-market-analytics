#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="$PROJECT_ROOT/kafka"
KAFKA_VERSION="3.8.0"
SCALA_VERSION="2.13"
KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
FORMAT_MARKER="$KAFKA_DIR/.kraft_formatted"

echo "============================================================"
echo " Setting up Apache Kafka (KRaft mode) natively"
echo "============================================================"

# Ensure Java is available
if ! command -v java >/dev/null 2>&1; then
    echo "ERROR: Java is not installed or not in PATH. Please install Java 11 or 17."
    exit 1
fi

# Download Kafka if not present
if [ ! -d "$KAFKA_DIR/bin" ]; then
    echo "Kafka not found in $KAFKA_DIR. Downloading Kafka ${KAFKA_VERSION}..."
    mkdir -p "$PROJECT_ROOT/tmp_kafka"
    cd "$PROJECT_ROOT/tmp_kafka"

    if command -v curl >/dev/null 2>&1; then
        curl -fSL -o "$KAFKA_ARCHIVE" "$DOWNLOAD_URL"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$KAFKA_ARCHIVE" "$DOWNLOAD_URL"
    else
        echo "ERROR: Neither curl nor wget found. Please download $DOWNLOAD_URL manually."
        exit 1
    fi

    echo "Extracting $KAFKA_ARCHIVE..."
    tar -xzf "$KAFKA_ARCHIVE"
    EXTRACTED_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
    mkdir -p "$KAFKA_DIR"
    cp -r "$EXTRACTED_DIR"/* "$KAFKA_DIR/"
    cd "$PROJECT_ROOT"
    rm -rf "$PROJECT_ROOT/tmp_kafka"
    echo "Kafka installed to $KAFKA_DIR"
else
    echo "Kafka already present in $KAFKA_DIR."
fi

# Format storage for KRaft mode
if [ -f "$FORMAT_MARKER" ]; then
    echo "KRaft storage already formatted. Skipping format step."
else
    echo "Formatting KRaft storage directories..."
    CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
    echo "Generated KRaft Cluster ID: $CLUSTER_ID"
    "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_DIR/config/kraft/server.properties"
    touch "$FORMAT_MARKER"
    echo "KRaft storage formatted successfully."
fi

echo "============================================================"
echo " Setup complete!"
echo " To start the Kafka broker, run:"
echo "   ./scripts/start_kafka.sh"
echo "============================================================"
