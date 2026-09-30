"""
Delta Lake sink module for Phase 3.
Implements idempotent batch writers for raw trades and windowed OHLCV aggregates.
Provides upfront table initialization using DeltaTable.createIfNotExists.
"""

import os
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from delta.tables import DeltaTable

from config import DELTA_BASE_PATH


def resolve_delta_path(base_path: str, table_name: str) -> str:
    """
    Resolve and normalize table path for local disk or cloud object storage.
    On Windows local disk, converts to posix format to ensure Delta catalog compatibility.
    """
    if base_path.startswith("s3://") or base_path.startswith("s3a://"):
        return f"{base_path.rstrip('/')}/{table_name}"
    resolved = (Path(base_path) / table_name).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved.as_posix()


def get_raw_trades_schema() -> StructType:
    """Explicit schema for raw_trades Delta table."""
    return StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("ticker", StringType(), nullable=True),
            StructField("price", DoubleType(), nullable=True),
            StructField("volume", IntegerType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("trade_date", DateType(), nullable=True),
            StructField("kafka_partition", IntegerType(), nullable=True),
            StructField("kafka_offset", LongType(), nullable=True),
            StructField("ingested_at", TimestampType(), nullable=True),
        ]
    )


def get_agg_ohlcv_schema() -> StructType:
    """Explicit schema for agg_5min and agg_1hour Delta tables."""
    return StructType(
        [
            StructField("ticker", StringType(), nullable=True),
            StructField("company_name", StringType(), nullable=True),
            StructField("sector", StringType(), nullable=True),
            StructField("exchange", StringType(), nullable=True),
            StructField("window_start", TimestampType(), nullable=True),
            StructField("window_end", TimestampType(), nullable=True),
            StructField("open", DoubleType(), nullable=True),
            StructField("high", DoubleType(), nullable=True),
            StructField("low", DoubleType(), nullable=True),
            StructField("close", DoubleType(), nullable=True),
            StructField("volume", LongType(), nullable=True),
            StructField("trade_count", LongType(), nullable=True),
        ]
    )


def init_delta_tables(spark: SparkSession, delta_base_path: str = DELTA_BASE_PATH) -> None:
    """
    Pre-create Delta Lake tables with explicit schemas if they do not already exist.
    Ensures data types and table partitions remain stable across runs.
    """
    # 1. raw_trades table, partitioned by trade_date
    raw_path = resolve_delta_path(delta_base_path, "raw_trades")
    DeltaTable.createIfNotExists(spark) \
        .location(raw_path) \
        .addColumns(get_raw_trades_schema()) \
        .partitionedBy("trade_date") \
        .execute()

    # 2. agg_5min table
    agg_5min_path = resolve_delta_path(delta_base_path, "agg_5min")
    DeltaTable.createIfNotExists(spark) \
        .location(agg_5min_path) \
        .addColumns(get_agg_ohlcv_schema()) \
        .execute()

    # 3. agg_1hour table
    agg_1hour_path = resolve_delta_path(delta_base_path, "agg_1hour")
    DeltaTable.createIfNotExists(spark) \
        .location(agg_1hour_path) \
        .addColumns(get_agg_ohlcv_schema()) \
        .execute()


def write_raw(batch_df: DataFrame, batch_id: int, delta_base_path: str = DELTA_BASE_PATH) -> None:
    """
    Append raw trades micro-batch to {DELTA_BASE_PATH}/raw_trades partitioned by trade_date.
    Uses Delta idempotent write transaction options (txnAppId, txnVersion) to guarantee
    that replayed micro-batches after a crash neither duplicate nor lose rows.
    """
    if batch_df.isEmpty():
        return

    raw_path = resolve_delta_path(delta_base_path, "raw_trades")
    target_columns = [
        "event_id",
        "ticker",
        "price",
        "volume",
        "timestamp",
        "trade_date",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    ]

    (
        batch_df.select(target_columns)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", "raw_trades_writer")
        .option("txnVersion", batch_id)
        .save(raw_path)
    )


def write_agg(
    table_name: str,
    delta_base_path: str = DELTA_BASE_PATH,
) -> Callable[[DataFrame, int], None]:
    """
    Return a foreachBatch writer for aggregated windowed metrics tables (agg_5min, agg_1hour).
    Flattens window struct into window_start / window_end, then performs an idempotent MERGE INTO
    the target Delta table on (ticker, window_start).

    Since aggregation queries run in update output mode, each micro-batch carries the full current
    value of every window touched during that micro-batch.
    """
    target_path = resolve_delta_path(delta_base_path, table_name)

    def _batch_writer(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        df = batch_df

        # Flatten window struct if not already flattened
        if "window" in df.columns:
            df = df.withColumn("window_start", col("window.start")) \
                   .withColumn("window_end", col("window.end")) \
                   .drop("window")

        ordered_cols = [
            "ticker",
            "company_name",
            "sector",
            "exchange",
            "window_start",
            "window_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        ]
        df = df.select([c for c in ordered_cols if c in df.columns])

        # The aggregation queries stay in update output mode, so each batch carries the full
        # current value of every window it touched; assert at most one row per (ticker, window_start)
        # per batch to guarantee unambiguous MERGE semantics.
        key_count = df.select("ticker", "window_start").distinct().count()
        row_count = df.count()
        assert key_count == row_count, (
            f"Expected at most one row per (ticker, window_start) in batch {batch_id} for table '{table_name}', "
            f"found {row_count} total rows and {key_count} distinct keys"
        )

        spark_session = df.sparkSession
        target_delta = DeltaTable.forPath(spark_session, target_path)

        (
            target_delta.alias("target")
            .merge(
                source=df.alias("source"),
                condition="target.ticker = source.ticker AND target.window_start = source.window_start",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return _batch_writer
