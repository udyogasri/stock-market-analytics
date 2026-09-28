#!/usr/bin/env python3
"""
Simulated Stock Trades Kafka Producer
Generates realistic random-walk trade events for 20 NASDAQ tickers.
"""

import argparse
import csv
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("StockTradesProducer")

# Fallback tickers if metadata CSV is unavailable
DEFAULT_TICKERS = [
    "AAPL", "NVDA", "MSFT", "GOOG", "AMZN",
    "TSLA", "META", "AVGO", "COST", "NFLX",
    "AMD", "QCOM", "ADBE", "INTC", "CSCO",
    "TXN", "PYPL", "AMAT", "INTU", "MRVL"
]


def load_tickers_from_csv(csv_path: str) -> list[str]:
    """Load ticker symbols from company metadata CSV."""
    resolved_path = Path(csv_path)
    if not resolved_path.is_file():
        logger.warning("Metadata CSV not found at %s. Using default 20 tickers.", resolved_path)
        return DEFAULT_TICKERS

    tickers = []
    try:
        with open(resolved_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row.get("ticker", "").strip()
                if ticker:
                    tickers.append(ticker)
        logger.info("Loaded %d tickers from %s", len(tickers), resolved_path)
        return tickers if tickers else DEFAULT_TICKERS
    except Exception as exc:
        logger.warning("Failed reading CSV (%s). Using defaults.", exc)
        return DEFAULT_TICKERS


class StockPriceSimulator:
    """Maintains a realistic random-walk price for each stock ticker."""

    def __init__(self, tickers: list[str]):
        self.tickers = tickers
        # Initialize realistic initial base prices ($50 - $450)
        rng = random.Random(42)
        self.prices = {
            ticker: round(rng.uniform(50.0, 450.0), 2)
            for ticker in tickers
        }

    def next_trade(self) -> tuple[str, dict]:
        """
        Generate the next simulated trade event with random-walk price.
        Returns:
            (ticker, trade_event_dict)
        """
        ticker = random.choice(self.tickers)
        current_price = self.prices[ticker]

        # Random walk: small percentage drift using normal distribution (std ~0.15%)
        pct_change = random.gauss(0.0, 0.0015)
        new_price = max(1.0, current_price * (1.0 + pct_change))
        new_price = round(new_price, 2)
        self.prices[ticker] = new_price

        event = {
            "event_id": str(uuid.uuid4()),
            "ticker": ticker,
            "price": new_price,
            "volume": random.randint(1, 5000),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return ticker, event


def create_kafka_producer(bootstrap_servers: str):
    """
    Instantiate a high-throughput Kafka producer with batching and lz4 compression.
    Supports confluent_kafka (primary) with fallback to kafka-python if needed.
    """
    try:
        from confluent_kafka import Producer

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "lz4",
            "linger.ms": 20,                # 20ms linger for micro-batching
            "batch.size": 65536,             # 64KB batch size
            "acks": 1,                       # Fast ack for streaming local dev
            "queue.buffering.max.messages": 200000,
            "queue.buffering.max.kbytes": 1048576,
        }

        producer = Producer(conf)

        def delivery_callback(err, msg):
            if err:
                logger.error("Message delivery failed: %s", err)

        class ConfluentKafkaWrapper:
            def __init__(self, p):
                self.p = p

            def send(self, topic, key, value):
                self.p.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(value).encode("utf-8"),
                    on_delivery=delivery_callback
                )

            def poll(self):
                self.p.poll(0)

            def flush(self):
                self.p.flush()

        logger.info("Using confluent-kafka with LZ4 compression & batching.")
        return ConfluentKafkaWrapper(producer)

    except ImportError:
        logger.info("confluent-kafka not found. Falling back to kafka-python-ng...")
        try:
            from kafka import KafkaProducer
            from kafka.codec import has_lz4

            compression = "lz4" if has_lz4() else "gzip"
            logger.info("Using kafka-python-ng with %s compression & batching.", compression.upper())

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                compression_type=compression,
                linger_ms=20,
                batch_size=65536,
                acks=1,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            class KafkaPythonWrapper:
                def __init__(self, p):
                    self.p = p

                def send(self, topic, key, value):
                    self.p.send(topic, key=key, value=value)

                def poll(self):
                    pass

                def flush(self):
                    self.p.flush()

            return KafkaPythonWrapper(producer)

        except ImportError as exc:
            logger.error("Neither confluent-kafka nor kafka-python is installed. Run: pip install -r requirements.txt")
            raise exc


def run_producer(bootstrap_servers: str, topic: str, rate: int, metadata_path: str):
    tickers = load_tickers_from_csv(metadata_path)
    simulator = StockPriceSimulator(tickers)
    producer = create_kafka_producer(bootstrap_servers)

    running = True

    def signal_handler(signum, frame):
        nonlocal running
        logger.info("Shutdown signal received. Stopping producer...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting trade producer for topic '%s' at target rate %d events/sec", topic, rate)
    logger.info("Simulating %d tickers with random-walk pricing.", len(tickers))

    # Rate limiting & pacing setup
    # Slice size allows sub-millisecond precision on systems with coarse sleep timers
    slice_size = max(1, min(100, rate // 20))
    slice_interval = slice_size / float(rate)

    total_produced = 0
    window_produced = 0
    window_start_time = time.perf_counter()
    next_slice_time = time.perf_counter()

    try:
        while running:
            # Produce a slice of events
            for _ in range(slice_size):
                ticker, event = simulator.next_trade()
                producer.send(topic, key=ticker, value=event)
                total_produced += 1
                window_produced += 1

            producer.poll()

            # Log throughput every 5 seconds
            current_time = time.perf_counter()
            elapsed_window = current_time - window_start_time
            if elapsed_window >= 5.0:
                actual_throughput = window_produced / elapsed_window
                logger.info(
                    "Throughput: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                    actual_throughput, total_produced, rate
                )
                window_produced = 0
                window_start_time = current_time

            # Accurate pacing
            next_slice_time += slice_interval
            sleep_duration = next_slice_time - time.perf_counter()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            elif sleep_duration < -0.5:
                # If falling severely behind schedule, reset timer to prevent burst storms
                next_slice_time = time.perf_counter()

    finally:
        logger.info("Flushing producer buffers before exit...")
        producer.flush()
        logger.info("Producer exited cleanly. Total messages sent: %d", total_produced)


def main():
    parser = argparse.ArgumentParser(description="Real-time simulated stock trade event Kafka producer.")
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Target trade events generated per second (default: 100, tested up to 10,000)"
    )
    parser.add_argument(
        "--bootstrap-server",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap server (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="stock-trades",
        help="Kafka topic name (default: stock-trades)"
    )
    parser.add_argument(
        "--metadata-csv",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "data", "company_metadata.csv"),
        help="Path to company_metadata.csv"
    )

    args = parser.parse_args()

    if args.rate <= 0:
        logger.error("--rate must be a positive integer.")
        sys.exit(1)

    run_producer(
        bootstrap_servers=args.bootstrap_server,
        topic=args.topic,
        rate=args.rate,
        metadata_path=args.metadata_csv
    )


if __name__ == "__main__":
    main()
