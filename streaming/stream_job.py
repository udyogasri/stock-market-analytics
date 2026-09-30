#!/usr/bin/env python3
"""
PySpark Structured Streaming Job: Phase 3
Ingests real-time stock trades from Kafka, applies watermarking and windowed
OHLCV aggregations (5-minute and 1-hour), enriches with company metadata,
and writes with exactly-once idempotence to Delta Lake tables (or console via --console).
"""

import argparse
import os
import sys
from pathlib import Path

# Path bootstrap: ensure repo root is always in sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import (
    CHECKPOINT_BASE,
    DELTA_BASE_PATH,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    SPARK_MASTER,
    SPARK_TRIGGER_INTERVAL,
)
from pyspark.sql import SparkSession

from monitoring.listener import PipelineMetricsListener
from streaming.aggregations import build_ohlcv
from streaming.enrichment import enrich, load_metadata
from streaming.sinks import init_delta_tables, write_agg, write_raw
from streaming.source import read_trades


def create_spark_session(
    app_name: str = "StockTradesStreamingJobPhase3",
    master: str = SPARK_MASTER,
) -> SparkSession:
    """
    Initialize SparkSession configured for Delta Lake and Kafka streaming:
    - DeltaSparkSessionExtension & DeltaCatalog
    - Compatible package pairings (delta-spark 3.2.0 + spark-sql-kafka 3.5.0)
    - UTC timezone, conservative driver memory, and 12 shuffle partitions for local dev.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.legacy.allowAmbiguousPathsInCreateTable", "true")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def main() -> None:
    """Main entrypoint for Phase 3 streaming pipeline."""
    default_checkpoint_phase3 = str((Path(CHECKPOINT_BASE) / "phase3").resolve())

    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Job - Phase 3 (Delta Lake)")
    parser.add_argument(
        "--bootstrap-server",
        type=str,
        default=KAFKA_BOOTSTRAP,
        help=f"Kafka bootstrap server (default: {KAFKA_BOOTSTRAP})",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help=f"Kafka topic to consume (default: {KAFKA_TOPIC})",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default=SPARK_TRIGGER_INTERVAL,
        help=f"Processing trigger interval (default: '{SPARK_TRIGGER_INTERVAL}')",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="earliest",
        help="Starting offsets: 'latest' or 'earliest' (default: earliest)",
    )
    parser.add_argument(
        "--metadata-path",
        type=str,
        default=str((repo_root / "data" / "company_metadata.csv").resolve()),
        help="Path to company_metadata.csv",
    )
    parser.add_argument(
        "--delta-base-path",
        type=str,
        default=DELTA_BASE_PATH,
        help=f"Base path for Delta Lake tables (default: {DELTA_BASE_PATH})",
    )
    parser.add_argument(
        "--checkpoint-base",
        type=str,
        default=default_checkpoint_phase3,
        help=f"Base checkpoint directory (default: {default_checkpoint_phase3})",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=SPARK_MASTER,
        help=f"Spark master (default: '{SPARK_MASTER}')",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        default=False,
        help="Fall back to console output sink instead of Delta Lake tables",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Starting PySpark Structured Streaming Job - Phase 3")
    print(f"Master                 : {args.master}")
    print(f"Kafka bootstrap server : {args.bootstrap_server}")
    print(f"Subscribed topic       : {args.topic}")
    print(f"Trigger interval       : {args.trigger_interval}")
    print(f"Sink mode              : {'CONSOLE' if args.console else 'DELTA LAKE'}")
    if not args.console:
        print(f"Delta base path        : {args.delta_base_path}")
    print(f"Metadata path          : {args.metadata_path}")
    print(f"Base checkpoint dir    : {args.checkpoint_base}")
    print("=" * 80)

    spark = create_spark_session(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

    # Register PipelineMetricsListener for real-time pipeline observability
    metrics_listener = PipelineMetricsListener(trigger_interval_ms=5000.0)
    spark.streams.addListener(metrics_listener)
    print("Registered PipelineMetricsListener for pipeline observability.")

    # 1. Read raw parsed trades from Kafka source
    trades_df = read_trades(
        spark=spark,
        bootstrap_servers=args.bootstrap_server,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )

    # 2. Build 5-minute and 1-hour windowed OHLCV aggregations with 10-minute watermark
    agg_5min_df = build_ohlcv(trades_df=trades_df, window_duration="5 minutes")
    agg_1hour_df = build_ohlcv(trades_df=trades_df, window_duration="1 hour")

    # 3. Load static company metadata and enrich post-aggregation via broadcast join
    metadata_df = load_metadata(spark=spark, metadata_path=args.metadata_path)
    enriched_5min_df = enrich(agg_5min_df, metadata_df)
    enriched_1hour_df = enrich(agg_1hour_df, metadata_df)

    checkpoint_raw = str((Path(args.checkpoint_base) / "raw_trades").resolve())
    checkpoint_5min = str((Path(args.checkpoint_base) / "agg_5min").resolve())
    checkpoint_1hour = str((Path(args.checkpoint_base) / "agg_1hour").resolve())

    if args.console:
        # Fallback console sink mode
        raw_query = (
            trades_df.writeStream.queryName("raw_trades")
            .format("console")
            .outputMode("append")
            .trigger(processingTime=args.trigger_interval)
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", checkpoint_raw)
            .start()
        )

        agg_5min_query = (
            enriched_5min_df.writeStream.queryName("agg_5min")
            .format("console")
            .outputMode("update")
            .trigger(processingTime=args.trigger_interval)
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", checkpoint_5min)
            .start()
        )

        agg_1hour_query = (
            enriched_1hour_df.writeStream.queryName("agg_1hour")
            .format("console")
            .outputMode("update")
            .trigger(processingTime=args.trigger_interval)
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", checkpoint_1hour)
            .start()
        )
    else:
        # Phase 3 Delta Lake sink mode: pre-create tables with explicit schemas
        print("Initializing Delta Lake tables if not present...")
        init_delta_tables(spark=spark, delta_base_path=args.delta_base_path)
        print("Delta Lake tables initialized successfully.")

        # Query 1: raw_trades (append mode, idempotent writes with txnAppId and txnVersion)
        raw_query = (
            trades_df.writeStream.queryName("raw_trades")
            .foreachBatch(lambda bdf, bid: write_raw(bdf, bid, args.delta_base_path))
            .outputMode("append")
            .trigger(processingTime=args.trigger_interval)
            .option("checkpointLocation", checkpoint_raw)
            .start()
        )

        # Query 2: agg_5min (update mode, idempotent MERGE INTO ON ticker + window_start)
        agg_5min_query = (
            enriched_5min_df.writeStream.queryName("agg_5min")
            .foreachBatch(write_agg("agg_5min", args.delta_base_path))
            .outputMode("update")
            .trigger(processingTime=args.trigger_interval)
            .option("checkpointLocation", checkpoint_5min)
            .start()
        )

        # Query 3: agg_1hour (update mode, idempotent MERGE INTO ON ticker + window_start)
        agg_1hour_query = (
            enriched_1hour_df.writeStream.queryName("agg_1hour")
            .foreachBatch(write_agg("agg_1hour", args.delta_base_path))
            .outputMode("update")
            .trigger(processingTime=args.trigger_interval)
            .option("checkpointLocation", checkpoint_1hour)
            .start()
        )

    print("Active streaming queries started:")
    print("  - raw_trades (append mode -> Delta raw_trades)")
    print("  - agg_5min   (update mode -> Delta agg_5min MERGE)")
    print("  - agg_1hour  (update mode -> Delta agg_1hour MERGE)")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nTermination signal received. Stopping streaming queries...")
        for query in spark.streams.active:
            query.stop()
        metrics_listener.sink.close()
        spark.stop()
        print("Spark streaming job stopped cleanly.")


if __name__ == "__main__":
    main()
