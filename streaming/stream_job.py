#!/usr/bin/env python3
"""
PySpark Structured Streaming Job: Phase 2
Ingests real-time stock trades from Kafka, applies watermarking and windowed
OHLCV aggregations (5-minute and 1-hour), enriches with company metadata,
and displays results on the console for verification.
"""

import argparse
import os
import sys

# Tune JVM and native memory for Windows local development (prevent malloc/heap exhaustion)
os.environ.setdefault("_JAVA_OPTIONS", "-Xmx768m -Xms256m -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Xss256k -XX:CICompilerCount=2")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

# Configure JAVA_HOME on Windows if not set
if "JAVA_HOME" not in os.environ:
    import shutil
    java_exe = shutil.which("java")
    if java_exe:
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(java_exe)))
        if os.path.isdir(os.path.join(parent_dir, "bin")):
            os.environ["JAVA_HOME"] = parent_dir
    else:
        fallback_jdk = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.12.7-hotspot"
        if os.path.isdir(fallback_jdk):
            os.environ["JAVA_HOME"] = fallback_jdk

# Configure HADOOP_HOME on Windows if not set
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    local_hadoop = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "hadoop"))
    if os.path.isdir(local_hadoop):
        os.environ["HADOOP_HOME"] = local_hadoop
        hadoop_bin = os.path.join(local_hadoop, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# Ensure repo root is on sys.path for direct script execution
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from pyspark.sql import SparkSession

from streaming.aggregations import build_ohlcv
from streaming.enrichment import enrich, load_metadata
from streaming.source import read_trades


def create_spark_session(
    app_name: str = "StockTradesStreamingJobPhase2",
    master: str = "local[2]",
) -> SparkSession:
    """
    Initialize SparkSession configured for Kafka streaming, UTC timezone,
    conservative driver memory, and 12 shuffle partitions for local development.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.driver.memory", "768m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def main() -> None:
    """Main entrypoint for Phase 2 streaming pipeline."""
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Job - Phase 2")
    parser.add_argument(
        "--bootstrap-server",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap server (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="stock-trades",
        help="Kafka topic to consume (default: stock-trades)",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default="5 seconds",
        help="Processing trigger interval (default: '5 seconds')",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="latest",
        help="Starting offsets: 'latest' or 'earliest' (default: latest)",
    )
    parser.add_argument(
        "--metadata-path",
        type=str,
        default=os.path.join(repo_root, "data", "company_metadata.csv"),
        help="Path to company_metadata.csv",
    )
    parser.add_argument(
        "--checkpoint-base",
        type=str,
        default=os.path.join(repo_root, "checkpoints", "phase2"),
        help="Base checkpoint directory (default: checkpoints/phase2)",
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[2]",
        help="Spark master (default: 'local[2]')",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Starting PySpark Structured Streaming Job - Phase 2")
    print(f"Master                 : {args.master}")
    print(f"Kafka bootstrap server : {args.bootstrap_server}")
    print(f"Subscribed topic       : {args.topic}")
    print(f"Trigger interval       : {args.trigger_interval}")
    print(f"Metadata path          : {args.metadata_path}")
    print(f"Base checkpoint dir    : {args.checkpoint_base}")
    print("=" * 80)

    spark = create_spark_session(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

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

    checkpoint_raw = os.path.join(args.checkpoint_base, "raw")
    checkpoint_5min = os.path.join(args.checkpoint_base, "agg_5min")
    checkpoint_1hour = os.path.join(args.checkpoint_base, "agg_1hour")

    # Query 1: Raw parsed trades stream (append mode, 5-second trigger)
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

    # Queries 2 & 3: Windowed OHLCV aggregations
    # NOTE ON OUTPUT MODE:
    # In append mode, a windowed aggregation query only emits after the window end
    # plus the 10-minute watermark has passed (i.e. window_end + 10m).
    # Here we use outputMode("update") with the console sink so partial window results
    # are visible while the window is still open.
    # Phase 3 will handle finalized writes to durable storage (e.g. Delta Lake) via foreachBatch.

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

    print("Active streaming queries started:")
    print("  - raw_trades (append mode)")
    print("  - agg_5min   (update mode, enriched)")
    print("  - agg_1hour  (update mode, enriched)")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nTermination signal received. Stopping streaming queries...")
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        print("Spark streaming job stopped cleanly.")


if __name__ == "__main__":
    main()
