"""
Spark batch job to export finalized aggregation windows from Delta Lake to Parquet.
Exports only finalized windows older than the streaming watermark (max_timestamp - 10 min),
matching Redshift DDL column ordering and types, and writes manifest.json LAST to trigger
downstream warehouse ingestion via AWS Lambda or local runner.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional
import uuid

# Ensure repo root is in sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import (
    DELTA_BASE_PATH,
    EXPORT_BASE,
    SPARK_MASTER,
    is_aws_delta,
    is_aws_export,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ExportFinalized")

REDSHIFT_COLUMNS = [
    ("ticker", "string"),
    ("company_name", "string"),
    ("sector", "string"),
    ("exchange", "string"),
    ("window_start", "timestamp"),
    ("window_end", "timestamp"),
    ("open", "double"),
    ("high", "double"),
    ("low", "double"),
    ("close", "double"),
    ("volume", "long"),
    ("trade_count", "long"),
]


def create_batch_spark_session(app_name: str = "ExportFinalizedWindows") -> SparkSession:
    """Create SparkSession with Delta Lake and optional S3A packages."""
    packages = ["io.delta:delta-spark_2.12:3.2.0"]
    if is_aws_delta() or is_aws_export():
        # Matching Hadoop 3.3.4 bundled with PySpark 3.5.0
        packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        packages.append("com.amazonaws:aws-java-sdk-bundle:1.12.262")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.legacy.allowAmbiguousPathsInCreateTable", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "512m")
    )

    if is_aws_delta() or is_aws_export():
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    return builder.getOrCreate()


def _parse_s3_uri(uri: str):
    """Return (bucket, key_prefix) from s3:// or s3a:// URI."""
    clean = uri.replace("s3a://", "").replace("s3://", "")
    parts = clean.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix.rstrip("/")


def load_export_state(export_base: str) -> Dict[str, Any]:
    """Load export high-water mark state from state file."""
    if export_base.startswith("s3a://") or export_base.startswith("s3://"):
        try:
            import boto3
            bucket, prefix = _parse_s3_uri(export_base)
            s3 = boto3.client("s3")
            key = f"{prefix}/.export_state.json" if prefix else ".export_state.json"
            resp = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(resp["Body"].read().decode("utf-8"))
        except Exception:
            return {}
    else:
        state_file = Path(export_base) / ".export_state.json"
        if state_file.is_file():
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}


def save_export_state(export_base: str, state: Dict[str, Any]) -> None:
    """Save export high-water mark state to state file."""
    content = json.dumps(state, indent=2, default=str)
    if export_base.startswith("s3a://") or export_base.startswith("s3://"):
        try:
            import boto3
            bucket, prefix = _parse_s3_uri(export_base)
            s3 = boto3.client("s3")
            key = f"{prefix}/.export_state.json" if prefix else ".export_state.json"
            s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        except Exception as e:
            logger.warning(f"Failed to save S3 export state: {e}")
    else:
        state_file = Path(export_base) / ".export_state.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as f:
            f.write(content)


def write_manifest(export_dir: str, manifest: Dict[str, Any]) -> None:
    """Write manifest.json LAST into export directory."""
    content = json.dumps(manifest, indent=2, default=str)
    if export_dir.startswith("s3a://") or export_dir.startswith("s3://"):
        import boto3
        bucket, prefix = _parse_s3_uri(export_dir)
        s3 = boto3.client("s3")
        key = f"{prefix}/manifest.json" if prefix else "manifest.json"
        s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    else:
        p = Path(export_dir) / "manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)


def list_parquet_files(export_dir: str) -> List[str]:
    """Return list of parquet files in the export directory."""
    if export_dir.startswith("s3a://") or export_dir.startswith("s3://"):
        import boto3
        bucket, prefix = _parse_s3_uri(export_dir)
        s3 = boto3.client("s3")
        files = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    files.append(f"s3://{bucket}/{key}")
        return files
    else:
        p = Path(export_dir)
        return [str(f.resolve()) for f in p.glob("*.parquet")]


def export_table(
    spark: SparkSession,
    table_name: str,
    delta_base: str,
    export_base: str,
    cutoff_ts: datetime,
    state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Export finalized unexported windows for a single table.
    Writes Parquet to {EXPORT_BASE}/{table}/run_id=<uuid>/ and manifest.json LAST.
    """
    table_path = f"{delta_base.rstrip('/')}/{table_name}"
    try:
        agg_df = spark.read.format("delta").load(table_path)
    except Exception as e:
        logger.warning(f"Could not load Delta table {table_path}: {e}")
        return None

    # Filter finalized windows (window_end <= cutoff_ts)
    filter_cond = col("window_end") <= lit(cutoff_ts)

    # Filter out already exported windows based on high-water mark state
    table_state = state.get(table_name, {})
    last_window_end_str = table_state.get("last_exported_window_end")
    if last_window_end_str:
        filter_cond = filter_cond & (col("window_end") > lit(last_window_end_str))

    final_df = agg_df.filter(filter_cond)
    row_count = final_df.count()

    if row_count == 0:
        logger.info(f"[{table_name}] No new finalized windows to export (cutoff={cutoff_ts.isoformat()}).")
        return None

    run_id = str(uuid.uuid4())
    export_dir = f"{export_base.rstrip('/')}/{table_name}/run_id={run_id}"
    logger.info(f"[{table_name}] Exporting {row_count} rows for run_id={run_id} to {export_dir}...")

    # Order and cast columns strictly to match Redshift DDL
    select_exprs = [col(name).cast(dtype) for name, dtype in REDSHIFT_COLUMNS]
    ordered_df = final_df.select(select_exprs)

    # Write Parquet with TIMESTAMP_MICROS
    ordered_df.write.mode("overwrite").parquet(export_dir)

    # Compute window range stats
    stats = final_df.selectExpr(
        "min(window_start) as min_ws",
        "max(window_start) as max_ws",
        "max(window_end) as max_we",
    ).collect()[0]

    min_ws = stats["min_ws"].isoformat() if stats["min_ws"] else None
    max_ws = stats["max_ws"].isoformat() if stats["max_ws"] else None
    new_max_we = stats["max_we"].isoformat() if stats["max_we"] else None

    # List written parquet files
    files = list_parquet_files(export_dir)

    manifest = {
        "table": table_name,
        "run_id": run_id,
        "export_prefix": export_dir,
        "files": files,
        "row_count": row_count,
        "min_window_start": min_ws,
        "max_window_start": max_ws,
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }

    # Write manifest.json LAST (its arrival triggers downstream Lambda)
    write_manifest(export_dir, manifest)
    logger.info(f"[{table_name}] Successfully wrote manifest.json for run_id={run_id}.")

    # Update state high-water mark ONLY after manifest is written
    state[table_name] = {
        "last_exported_window_end": new_max_we,
        "last_run_id": run_id,
        "last_export_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    save_export_state(export_base, state)

    return manifest


def run_export_cycle(
    spark: SparkSession,
    delta_base: str = DELTA_BASE_PATH,
    export_base: str = EXPORT_BASE,
) -> List[Dict[str, Any]]:
    """
    Execute one export cycle for agg_5min and agg_1hour.
    Returns list of generated manifests.
    """
    raw_trades_path = f"{delta_base.rstrip('/')}/raw_trades"
    try:
        raw_df = spark.read.format("delta").load(raw_trades_path)
        max_ts_row = raw_df.select(spark_max("timestamp")).collect()[0]
        max_ts = max_ts_row[0]
    except Exception as e:
        logger.warning(f"Could not read raw_trades to calculate watermark: {e}")
        return []

    if max_ts is None:
        logger.info("raw_trades has no records. Skipping export.")
        return []

    # Watermark threshold: max(timestamp) - 10 minutes (matching streaming watermark)
    cutoff_ts = max_ts - timedelta(minutes=10)
    logger.info(f"Watermark cutoff timestamp: {cutoff_ts.isoformat()} (max raw timestamp: {max_ts.isoformat()})")

    state = load_export_state(export_base)
    manifests = []

    for table in ["agg_5min", "agg_1hour"]:
        manifest = export_table(
            spark=spark,
            table_name=table,
            delta_base=delta_base,
            export_base=export_base,
            cutoff_ts=cutoff_ts,
            state=state,
        )
        if manifest:
            manifests.append(manifest)

    return manifests


def main():
    parser = argparse.ArgumentParser(description="Export finalized Delta Lake aggregations to Parquet for warehouse loading.")
    parser.add_argument("--once", action="store_true", default=False, help="Run a single export cycle and exit")
    parser.add_argument("--loop", action="store_true", default=False, help="Run continuously in a loop")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Loop interval in seconds (default: 300)")
    parser.add_argument("--delta-base-path", type=str, default=DELTA_BASE_PATH, help=f"Delta base path (default: {DELTA_BASE_PATH})")
    parser.add_argument("--export-base", type=str, default=EXPORT_BASE, help=f"Export base path (default: {EXPORT_BASE})")
    args = parser.parse_args()

    spark = create_batch_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    run_once = args.once or (not args.loop)

    try:
        if run_once:
            logger.info("Executing single export cycle (--once)...")
            manifests = run_export_cycle(spark, args.delta_base_path, args.export_base)
            logger.info(f"Export cycle completed. Manifests generated: {len(manifests)}")
        else:
            logger.info(f"Starting continuous export loop (interval: {args.interval_seconds}s)...")
            while True:
                run_export_cycle(spark, args.delta_base_path, args.export_base)
                time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        logger.info("Export loop interrupted by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
