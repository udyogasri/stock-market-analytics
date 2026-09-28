"""
Central configuration module for the stock market streaming pipeline.
Reads settings from environment variables with local-friendly defaults.
Supports seamless switching between LOCAL and AWS execution modes.
"""

import os
from pathlib import Path


def _load_dotenv(dotenv_path: str = ".env") -> None:
    """Lightweight .env file parser to avoid mandatory external dependencies."""
    env_file = Path(dotenv_path)
    if not env_file.is_file():
        return
    try:
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


# Automatically load .env if present in root
_load_dotenv()

# Root directory reference
REPO_ROOT = Path(__file__).resolve().parent

# --- Kafka Settings ---
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "stock-trades")

# --- Spark Streaming Settings ---
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[2]")
SPARK_TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
SPARK_CHECKPOINT_BASE: str = os.getenv(
    "SPARK_CHECKPOINT_BASE",
    str(REPO_ROOT / "checkpoints"),
)

# --- Storage / Delta Settings ---
# LOCAL: local folder (e.g. ./data/delta)
# AWS: S3 bucket URI (e.g. s3a://my-bucket/delta)
DELTA_BASE_PATH: str = os.getenv(
    "DELTA_BASE_PATH",
    str(REPO_ROOT / "data" / "delta"),
)

# --- Metrics Sink Settings (file | cloudwatch | both) ---
METRICS_SINK: str = os.getenv("METRICS_SINK", "file").lower()
METRICS_LOG_PATH: str = os.getenv(
    "METRICS_LOG_PATH",
    str(REPO_ROOT / "logs" / "metrics.log"),
)
CLOUDWATCH_NAMESPACE: str = os.getenv("CLOUDWATCH_NAMESPACE", "StockMarketStreaming")

# --- Warehouse Target (duckdb | redshift) ---
WAREHOUSE_TARGET: str = os.getenv("WAREHOUSE_TARGET", "duckdb").lower()
DUCKDB_PATH: str = os.getenv(
    "DUCKDB_PATH",
    str(REPO_ROOT / "data" / "warehouse.duckdb"),
)

# --- AWS Settings (Uses default boto3 credential chain) ---
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
REDSHIFT_DATABASE: str = os.getenv("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT: int = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_USER: str = os.getenv("REDSHIFT_USER", "awsuser")
REDSHIFT_CLUSTER_IDENTIFIER: str = os.getenv("REDSHIFT_CLUSTER_IDENTIFIER", "stock-cluster")


def is_aws_delta() -> bool:
    """Return True if Delta Lake is configured to write to S3."""
    return DELTA_BASE_PATH.startswith("s3://") or DELTA_BASE_PATH.startswith("s3a://")


def is_cloudwatch_metrics() -> bool:
    """Return True if metrics should be published to AWS CloudWatch."""
    return METRICS_SINK in ("cloudwatch", "both")


def is_redshift_warehouse() -> bool:
    """Return True if the target data warehouse is Amazon Redshift."""
    return WAREHOUSE_TARGET == "redshift"
