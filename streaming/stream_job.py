#!/usr/bin/env python3
"""
PySpark Structured Streaming Job: Phase 1
Ingests real-time stock trade events from Kafka topic 'stock-trades',
parses JSON payloads against an explicit schema, and prints parsed rows to console in append mode.
"""

import argparse
import os
import sys

# Configure HADOOP_HOME on Windows if not set
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    local_hadoop = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "hadoop"))
    if os.path.isdir(local_hadoop):
        os.environ["HADOOP_HOME"] = local_hadoop
        hadoop_bin = os.path.join(local_hadoop, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_spark_session(app_name: str = "StockTradesStreamingJob") -> SparkSession:
    """Initialize SparkSession with Kafka SQL connector package configured."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def define_trade_schema() -> StructType:
    """
    Explicit schema for the JSON trade payload:
    - ticker: string
    - price: double
    - volume: int
    - timestamp: timestamp
    - event_id: string
    """
    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("ticker", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("volume", IntegerType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
        ]
    )


def main():
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming for Stock Trades")
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

    args = parser.parse_args()

    print("=" * 80)
    print(f"Starting Spark Streaming Job connecting to Kafka at {args.bootstrap_server}")
    print(f"Subscribing to topic: {args.topic}")
    print(f"Trigger processing time: {args.trigger_interval}")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define explicit payload schema
    trade_schema = define_trade_schema()

    # 2. Read streaming DataFrame from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 3. Parse JSON message value using schema
    parsed_df = (
        kafka_df.selectExpr("CAST(key AS STRING) as message_key", "CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), trade_schema).alias("trade"))
        .select(
            col("trade.event_id"),
            col("trade.ticker"),
            col("trade.price"),
            col("trade.volume"),
            col("trade.timestamp"),
        )
    )

    # 4. Write stream to console in append mode with 5 seconds trigger
    query = (
        parsed_df.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime=args.trigger_interval)
        .option("truncate", "false")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nTermination signal received. Stopping streaming query...")
        query.stop()
        spark.stop()
        print("Spark streaming job stopped cleanly.")


if __name__ == "__main__":
    main()
