#!/usr/bin/env python3
"""
Maintenance script for Delta Lake tables.
Performs manual OPTIMIZE (compaction of small files created by micro-batch streaming)
and VACUUM (with default 7-day retention) across raw_trades, agg_5min, and agg_1hour tables.
"""

import argparse
import sys
from pathlib import Path

# Path bootstrap: ensure repo root is in sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import DELTA_BASE_PATH, SPARK_MASTER
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from streaming.sinks import resolve_delta_path


def create_spark_session(master: str = SPARK_MASTER) -> SparkSession:
    """Initialize SparkSession configured for Delta Lake maintenance."""
    return (
        SparkSession.builder.appName("DeltaLakeMaintenance")
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
        .getOrCreate()
    )


def maintain_table(spark: SparkSession, table_name: str, delta_base_path: str) -> None:
    """Run OPTIMIZE and VACUUM (7-day default retention) on a specific Delta table."""
    table_path = resolve_delta_path(delta_base_path, table_name)
    print(f"\n--- Checking table '{table_name}' at: {table_path} ---")

    if not DeltaTable.isDeltaTable(spark, table_path):
        print(f"Table '{table_name}' at {table_path} is not an existing Delta table. Skipping.")
        return

    delta_table = DeltaTable.forPath(spark, table_path)

    # 1. OPTIMIZE (executeCompaction)
    print(f"Running OPTIMIZE on '{table_name}'...")
    optimize_result = delta_table.optimize().executeCompaction()
    print(f"OPTIMIZE completed for '{table_name}'.")
    optimize_result.show(truncate=False)

    # 2. VACUUM with default 7-day (168 hours) retention
    print(f"Running VACUUM on '{table_name}' with default 7-day retention...")
    vacuum_result = delta_table.vacuum()
    print(f"VACUUM completed for '{table_name}'.")
    vacuum_result.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Lake Table Maintenance (OPTIMIZE & VACUUM)")
    parser.add_argument(
        "--delta-base-path",
        type=str,
        default=DELTA_BASE_PATH,
        help=f"Base path for Delta Lake tables (default: {DELTA_BASE_PATH})",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=["raw_trades", "agg_5min", "agg_1hour"],
        help="List of Delta tables to maintain (default: raw_trades agg_5min agg_1hour)",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=SPARK_MASTER,
        help=f"Spark master (default: '{SPARK_MASTER}')",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Starting Delta Lake Maintenance Job")
    print(f"Delta base path : {args.delta_base_path}")
    print(f"Target tables   : {args.tables}")
    print(f"Master          : {args.master}")
    print("=" * 80)

    spark = create_spark_session(master=args.master)
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table_name in args.tables:
            maintain_table(spark=spark, table_name=table_name, delta_base_path=args.delta_base_path)
    finally:
        spark.stop()
        print("\nMaintenance job finished cleanly.")


if __name__ == "__main__":
    main()
