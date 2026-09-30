#!/usr/bin/env python3
"""
Simulated Stock Trades Kafka Producer
Generates realistic random-walk trade events for 20 NASDAQ tickers.
Supports injecting out-of-order/late events via --late-event-minutes.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure repo root is in sys.path to import config
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC

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

    def next_trade(self, current_time: Optional[datetime] = None) -> tuple[str, dict]:
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

        event_ts = current_time.isoformat() if current_time is not None else datetime.now(timezone.utc).isoformat()
        event = {
            "event_id": str(uuid.uuid4()),
            "ticker": ticker,
            "price": new_price,
            "volume": random.randint(1, 5000),
            "timestamp": event_ts
        }
        return ticker, event

    def generate_late_trade(self, late_minutes: float) -> tuple[str, dict]:
        """
        Generate a single simulated trade event with a timestamp in the past: (now - late_minutes).
        Returns:
            (ticker, trade_event_dict)
        """
        ticker = random.choice(self.tickers)
        current_price = self.prices[ticker]

        pct_change = random.gauss(0.0, 0.0015)
        new_price = max(1.0, current_price * (1.0 + pct_change))
        new_price = round(new_price, 2)
        self.prices[ticker] = new_price

        past_timestamp = (datetime.now(timezone.utc) - timedelta(minutes=late_minutes)).isoformat()
        event = {
            "event_id": str(uuid.uuid4()),
            "ticker": ticker,
            "price": new_price,
            "volume": random.randint(1, 5000),
            "timestamp": past_timestamp
        }
        return ticker, event


def create_kafka_producer(bootstrap_servers: str):
    """
    Instantiate a high-throughput Kafka producer with batching and lz4 compression.
    Supports confluent_kafka (primary) with fallback to kafka-python if needed.
    """
    try:
        from confluent_kafka import Producer  # type: ignore

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "lz4",
            "linger.ms": 20,                # 20ms linger for micro-batching
            "batch.size": 65536,            # 64KB batch size
            "acks": "all",                  # Idempotent producer requirement
            "enable.idempotence": True,     # Kafka idempotent producer
            "max.in.flight.requests.per.connection": 5,
            "retries": 1000000,
            "queue.buffering.max.messages": 500000,
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

        logger.info("Using confluent-kafka with LZ4 compression, idempotence=true, acks=all & batching.")
        return ConfluentKafkaWrapper(producer)

    except ImportError:
        logger.info("confluent-kafka not found. Falling back to kafka-python-ng...")
        try:
            from kafka import KafkaProducer
            from kafka.codec import has_lz4

            compression = "lz4" if has_lz4() else None
            comp_name = compression.upper() if compression else "NONE"
            logger.info("Using kafka-python-ng with %s compression & batching.", comp_name)

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                compression_type=compression,
                linger_ms=20,
                batch_size=65536,
                acks="all",
                api_version=(2, 6, 0),
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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


def _worker_process_loop(
    worker_id: int,
    tickers: list[str],
    rate: int,
    bootstrap_servers: str,
    topic: str,
    stop_event: Any,
    shared_counts: Any,
    late_event_minutes: float | None = None,
    max_events: int | None = None,
    start_timestamp: str | None = None,
    step_seconds: float | None = None,
) -> None:
    """Worker process loop generating trade events for its assigned tickers."""
    # Set up signal handler inside worker to ignore SIGINT so parent controls shutdown
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    simulator = StockPriceSimulator(tickers)
    producer = create_kafka_producer(bootstrap_servers)

    sim_time = datetime.fromisoformat(start_timestamp) if start_timestamp else None
    if sim_time and sim_time.tzinfo is None:
        sim_time = sim_time.replace(tzinfo=timezone.utc)

    total_produced = 0

    # Inject late event if assigned to this worker
    if late_event_minutes is not None and late_event_minutes > 0:
        if max_events is None or total_produced < max_events:
            late_ticker, late_event = simulator.generate_late_trade(late_event_minutes)
            producer.send(topic, key=late_ticker, value=late_event)
            producer.poll()
            total_produced += 1
            if shared_counts is not None:
                shared_counts[worker_id] = total_produced
            logger.info(
                "*** LATE EVENT SENT [Worker %d] *** | event_id=%s | ticker=%s | timestamp=%s | late_by_minutes=%.1f",
                worker_id,
                late_event["event_id"],
                late_ticker,
                late_event["timestamp"],
                late_event_minutes,
            )

    slice_size = max(1, min(100, rate // 20))
    slice_interval = slice_size / float(rate)
    next_slice_time = time.perf_counter()

    try:
        while not stop_event.is_set():
            if max_events is not None and total_produced >= max_events:
                break

            current_slice = slice_size
            if max_events is not None:
                current_slice = min(slice_size, max_events - total_produced)

            for _ in range(current_slice):
                ticker, event = simulator.next_trade(current_time=sim_time)
                if sim_time is not None and step_seconds:
                    sim_time += timedelta(seconds=step_seconds)
                producer.send(topic, key=ticker, value=event)
                total_produced += 1

            if shared_counts is not None:
                shared_counts[worker_id] = total_produced
            producer.poll()

            if max_events is not None and total_produced >= max_events:
                break

            next_slice_time += slice_interval
            sleep_duration = next_slice_time - time.perf_counter()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            elif sleep_duration < -0.5:
                next_slice_time = time.perf_counter()

    finally:
        producer.flush()
        if shared_counts is not None:
            shared_counts[worker_id] = total_produced


def run_producer(
    bootstrap_servers: str,
    topic: str,
    rate: int,
    metadata_path: str,
    late_event_minutes: float | None = None,
    max_events: int | None = None,
    start_timestamp: str | None = None,
    step_seconds: float | None = None,
    workers: int = 1,
) -> None:
    """Run simulated stock trades generation loop with optional multi-worker multiprocessing."""
    tickers = load_tickers_from_csv(metadata_path)

    if workers <= 1:
        # Single-worker mode: run in current process
        simulator = StockPriceSimulator(tickers)
        producer = create_kafka_producer(bootstrap_servers)

        sim_time = datetime.fromisoformat(start_timestamp) if start_timestamp else None
        if sim_time and sim_time.tzinfo is None:
            sim_time = sim_time.replace(tzinfo=timezone.utc)

        running = True

        def signal_handler(signum, frame):
            nonlocal running
            logger.info("Shutdown signal received. Stopping producer...")
            running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Starting trade producer for topic '%s' at target rate %d events/sec (1 worker)", topic, rate)
        if max_events is not None:
            logger.info("Configured to stop after producing exactly %d events.", max_events)
        logger.info("Simulating %d tickers with random-walk pricing.", len(tickers))

        total_produced = 0
        window_produced = 0

        if late_event_minutes is not None and late_event_minutes > 0:
            if max_events is None or total_produced < max_events:
                late_ticker, late_event = simulator.generate_late_trade(late_event_minutes)
                producer.send(topic, key=late_ticker, value=late_event)
                producer.poll()
                total_produced += 1
                logger.info(
                    "*** LATE EVENT SENT *** | event_id=%s | ticker=%s | timestamp=%s | late_by_minutes=%.1f",
                    late_event["event_id"],
                    late_ticker,
                    late_event["timestamp"],
                    late_event_minutes,
                )

        slice_size = max(1, min(100, rate // 20))
        slice_interval = slice_size / float(rate)
        window_start_time = time.perf_counter()
        next_slice_time = time.perf_counter()

        try:
            while running:
                if max_events is not None and total_produced >= max_events:
                    logger.info("Reached maximum target events (%d). Finishing...", max_events)
                    break

                current_slice = slice_size
                if max_events is not None:
                    current_slice = min(slice_size, max_events - total_produced)

                for _ in range(current_slice):
                    ticker, event = simulator.next_trade(current_time=sim_time)
                    if sim_time is not None and step_seconds:
                        sim_time += timedelta(seconds=step_seconds)
                    producer.send(topic, key=ticker, value=event)
                    total_produced += 1
                    window_produced += 1

                producer.poll()

                if max_events is not None and total_produced >= max_events:
                    logger.info("Reached maximum target events (%d). Finishing...", max_events)
                    break

                current_time = time.perf_counter()
                elapsed_window = current_time - window_start_time
                if elapsed_window >= 5.0:
                    actual_throughput = window_produced / elapsed_window
                    logger.info(
                        "Throughput [Worker 0]: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                        actual_throughput, total_produced, rate
                    )
                    logger.info(
                        "Throughput [COMBINED]: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                        actual_throughput, total_produced, rate
                    )
                    logger.info(
                        "Throughput: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                        actual_throughput, total_produced, rate
                    )
                    window_produced = 0
                    window_start_time = current_time

                next_slice_time += slice_interval
                sleep_duration = next_slice_time - time.perf_counter()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
                elif sleep_duration < -0.5:
                    next_slice_time = time.perf_counter()

        finally:
            logger.info("Flushing producer buffers before exit...")
            producer.flush()
            logger.info("Producer exited cleanly. Total messages sent: %d", total_produced)

    else:
        # Multi-worker mode: fan out across N processes partitioned by tickers
        import multiprocessing as mp

        n_workers = min(workers, len(tickers))
        worker_tickers_list = [
            [t for idx, t in enumerate(tickers) if idx % n_workers == i]
            for i in range(n_workers)
        ]
        base_rate = rate // n_workers
        rate_rem = rate % n_workers
        worker_rates = [base_rate + (1 if i < rate_rem else 0) for i in range(n_workers)]

        if max_events is not None:
            base_ev = max_events // n_workers
            rem_ev = max_events % n_workers
            worker_max_events = [base_ev + (1 if i < rem_ev else 0) for i in range(n_workers)]
        else:
            worker_max_events = [None] * n_workers

        logger.info(
            "Starting multi-worker producer with %d workers for target rate %d events/sec",
            n_workers, rate
        )
        for i in range(n_workers):
            logger.info(
                "  Worker %d: rate=%d msg/sec, %d tickers (%s)",
                i, worker_rates[i], len(worker_tickers_list[i]), ", ".join(worker_tickers_list[i])
            )

        stop_event = mp.Event()
        shared_counts = mp.Array("q", [0] * n_workers)

        processes = []
        for i in range(n_workers):
            p = mp.Process(
                target=_worker_process_loop,
                kwargs={
                    "worker_id": i,
                    "tickers": worker_tickers_list[i],
                    "rate": worker_rates[i],
                    "bootstrap_servers": bootstrap_servers,
                    "topic": topic,
                    "stop_event": stop_event,
                    "shared_counts": shared_counts,
                    "late_event_minutes": late_event_minutes if i == 0 else None,
                    "max_events": worker_max_events[i],
                    "start_timestamp": start_timestamp,
                    "step_seconds": step_seconds,
                },
            )
            p.daemon = True
            p.start()
            processes.append(p)

        def signal_handler(signum, frame):
            logger.info("Shutdown signal received in parent. Stopping all workers...")
            stop_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        last_counts = [0] * n_workers
        last_time = time.perf_counter()

        try:
            while not stop_event.is_set():
                time.sleep(0.2)
                now = time.perf_counter()
                elapsed = now - last_time
                if elapsed >= 5.0:
                    current_counts = [shared_counts[i] for i in range(n_workers)]
                    for i in range(n_workers):
                        w_delta = current_counts[i] - last_counts[i]
                        w_rate = w_delta / elapsed
                        logger.info(
                            "Throughput [Worker %d]: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                            i, w_rate, current_counts[i], worker_rates[i]
                        )
                    combined_delta = sum(current_counts) - sum(last_counts)
                    combined_rate = combined_delta / elapsed
                    total_produced = sum(current_counts)
                    logger.info(
                        "Throughput [COMBINED]: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                        combined_rate, total_produced, rate
                    )
                    logger.info(
                        "Throughput: %.2f msg/sec | Total produced: %d | Target rate: %d msg/sec",
                        combined_rate, total_produced, rate
                    )
                    last_counts = current_counts
                    last_time = now

                # Check if all workers have finished (e.g. max_events reached)
                if not any(p.is_alive() for p in processes):
                    break

        finally:
            stop_event.set()
            for p in processes:
                p.join(timeout=5.0)

            total_produced = sum(shared_counts[i] for i in range(n_workers))
            logger.info("All workers exited cleanly. Total messages sent: %d", total_produced)


def main():
    parser = argparse.ArgumentParser(description="Real-time simulated stock trade event Kafka producer.")
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Target trade events generated per second (default: 100, tested up to 10,000)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of producer processes to fan out tickers across (default: 1)"
    )
    parser.add_argument(
        "--bootstrap-server",
        type=str,
        default=KAFKA_BOOTSTRAP,
        help=f"Kafka bootstrap server (default: {KAFKA_BOOTSTRAP})"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help=f"Kafka topic name (default: {KAFKA_TOPIC})"
    )
    parser.add_argument(
        "--metadata-csv",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "data", "company_metadata.csv"),
        help="Path to company_metadata.csv"
    )
    parser.add_argument(
        "--late-event-minutes",
        type=float,
        default=None,
        help="Send ONE extra trade event with timestamp (now - N minutes) before normal streaming.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum number of events to produce before stopping (default: None, run indefinitely)",
    )
    parser.add_argument(
        "--start-timestamp",
        type=str,
        default=None,
        help="Optional ISO-8601 start timestamp for event generation",
    )
    parser.add_argument(
        "--step-seconds",
        type=float,
        default=None,
        help="Optional step in seconds to increment event timestamp with each generated event",
    )

    args = parser.parse_args()

    if args.rate <= 0:
        logger.error("--rate must be a positive integer.")
        sys.exit(1)

    if args.workers <= 0:
        logger.error("--workers must be a positive integer.")
        sys.exit(1)

    if args.max_events is not None and args.max_events <= 0:
        logger.error("--max-events must be a positive integer.")
        sys.exit(1)

    run_producer(
        bootstrap_servers=args.bootstrap_server,
        topic=args.topic,
        rate=args.rate,
        metadata_path=args.metadata_csv,
        late_event_minutes=args.late_event_minutes,
        max_events=args.max_events,
        start_timestamp=args.start_timestamp,
        step_seconds=args.step_seconds,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()

