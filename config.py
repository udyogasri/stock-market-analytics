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

# Ensure REPO_ROOT is on sys.path
import sys
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Tune JVM and native memory for Windows local development (prevent malloc/heap exhaustion)
os.environ.setdefault(
    "_JAVA_OPTIONS",
    "-Xmx512m -Xms64m -XX:G1HeapRegionSize=1m -XX:ParallelGCThreads=2 -XX:ConcGCThreads=1 -XX:CompressedClassSpaceSize=64m -XX:MaxMetaspaceSize=256m -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Xss256k -XX:CICompilerCount=2",
)
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
    local_hadoop = (REPO_ROOT / "hadoop").resolve()
    if local_hadoop.is_dir():
        os.environ["HADOOP_HOME"] = str(local_hadoop)
        hadoop_bin = str(local_hadoop / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# --- Kafka Settings ---
# Defaults per Phase 3 requirements: KAFKA_BOOTSTRAP=localhost:9092, KAFKA_TOPIC=stock-trades
KAFKA_BOOTSTRAP: str = os.getenv(
    "KAFKA_BOOTSTRAP",
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)
KAFKA_BOOTSTRAP_SERVERS: str = KAFKA_BOOTSTRAP
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "stock-trades")

# --- Storage / Delta Settings ---
# Default per Phase 3 requirements: DELTA_BASE_PATH=./delta
# Can be local path (./delta) or S3 path (s3a://...)
DELTA_BASE_PATH: str = os.getenv("DELTA_BASE_PATH", "./delta")

# --- Checkpoints Settings ---
# Default per Phase 3 requirements: CHECKPOINT_BASE=./checkpoints
CHECKPOINT_BASE: str = os.getenv(
    "CHECKPOINT_BASE",
    os.getenv("SPARK_CHECKPOINT_BASE", "./checkpoints"),
)
SPARK_CHECKPOINT_BASE: str = CHECKPOINT_BASE

# --- Spark Streaming Settings ---
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[2]")
SPARK_TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")

# MAX_OFFSETS_PER_TRIGGER (unset = unlimited, or integer limit to throttle ingestion for lag testing)
_max_offsets_raw = os.getenv("MAX_OFFSETS_PER_TRIGGER", "").strip()
MAX_OFFSETS_PER_TRIGGER: int | None = int(_max_offsets_raw) if _max_offsets_raw.isdigit() else None

# --- Metrics Sink Settings (file | cloudwatch | both) ---
METRICS_SINK: str = os.getenv("METRICS_SINK", "file").lower()
METRICS_LOG_PATH: str = os.getenv(
    "METRICS_LOG_PATH",
    str(REPO_ROOT / "monitoring" / "metrics.log"),
)
CLOUDWATCH_NAMESPACE: str = os.getenv("CLOUDWATCH_NAMESPACE", "StockPipeline")

# --- Warehouse & Export Settings ---
EXPORT_BASE: str = os.getenv("EXPORT_BASE", "./export")
WAREHOUSE_TARGET: str = os.getenv("WAREHOUSE_TARGET", "duckdb").lower()
DUCKDB_PATH: str = os.getenv(
    "DUCKDB_PATH",
    str(REPO_ROOT / "warehouse" / "local.duckdb"),
)

# --- AWS Settings (Uses default boto3 credential chain) ---
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
REDSHIFT_DATABASE: str = os.getenv("REDSHIFT_DATABASE", "dev")
REDSHIFT_PORT: int = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_USER: str = os.getenv("REDSHIFT_USER", "awsuser")
REDSHIFT_CLUSTER_IDENTIFIER: str = os.getenv("REDSHIFT_CLUSTER_IDENTIFIER", "stock-cluster")
REDSHIFT_WORKGROUP: str = os.getenv("REDSHIFT_WORKGROUP", "stock-workgroup")
REDSHIFT_COPY_ROLE_ARN: str = os.getenv("REDSHIFT_COPY_ROLE_ARN", "")


def is_aws_delta() -> bool:
    """Return True if Delta Lake is configured to write to S3."""
    return DELTA_BASE_PATH.startswith("s3://") or DELTA_BASE_PATH.startswith("s3a://")


def is_aws_export() -> bool:
    """Return True if exported Parquet files should be written to S3."""
    return EXPORT_BASE.startswith("s3://") or EXPORT_BASE.startswith("s3a://")


def is_cloudwatch_metrics() -> bool:
    """Return True if metrics should be published to AWS CloudWatch."""
    return METRICS_SINK in ("cloudwatch", "both")


def is_redshift_warehouse() -> bool:
    """Return True if the target data warehouse is Amazon Redshift."""
    return WAREHOUSE_TARGET == "redshift"

