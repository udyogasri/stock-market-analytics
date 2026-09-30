"""
AWS Lambda handler for orchestrating Amazon Redshift (or local DuckDB) warehouse loading.
Triggered by S3 ObjectCreated on export/**/manifest.json (or manual invocation).
Orchestrates idempotent staged COPY, delete-insert key replacement, and row count verification.
"""

import abc
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional

# Structured JSON logging
logger = logging.getLogger("WarehouseLoadHandler")
logger.setLevel(logging.INFO)


def _log_json(level: str, message: str, **kwargs) -> None:
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "message": message,
        **kwargs,
    }
    print(json.dumps(payload, default=str))


class BaseWarehouse(abc.ABC):
    """Abstract interface for warehouse ingestion targets."""

    @abc.abstractmethod
    def load_manifest(self, manifest: Dict[str, Any], s3_bucket: Optional[str] = None) -> Dict[str, Any]:
        """Load data described by manifest into warehouse and return load summary."""
        pass


class DuckDBWarehouse(BaseWarehouse):
    """
    Local DuckDB warehouse implementation for offline development and testing.
    Executes identical staged COPY and delete-insert idempotence logic.
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        try:
            import duckdb
            self._duckdb = duckdb
        except ImportError:
            raise RuntimeError("duckdb is required for local warehouse testing. Run: pip install duckdb")

        # Resolve path
        if db_path is None:
            db_path = os.getenv("DUCKDB_PATH", "./warehouse/local.duckdb")
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def _get_connection(self):
        return self._duckdb.connect(str(self.db_path))

    def _init_tables(self) -> None:
        con = self._get_connection()
        try:
            con.execute("""
            CREATE TABLE IF NOT EXISTS agg_5min (
                ticker VARCHAR NOT NULL,
                company_name VARCHAR,
                sector VARCHAR,
                exchange VARCHAR,
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                trade_count BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS agg_1hour (
                ticker VARCHAR NOT NULL,
                company_name VARCHAR,
                sector VARCHAR,
                exchange VARCHAR,
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                trade_count BIGINT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS load_audit (
                run_id VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                row_count BIGINT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR NOT NULL
            );
            """)
        finally:
            con.close()

    def load_manifest(self, manifest: Dict[str, Any], s3_bucket: Optional[str] = None) -> Dict[str, Any]:
        table = manifest["table"]
        run_id = manifest["run_id"]
        expected_rows = int(manifest["row_count"])
        export_prefix = manifest.get("export_prefix", "")
        files = manifest.get("files", [])

        if table not in ("agg_5min", "agg_1hour"):
            raise ValueError(f"Unsupported table: {table}")

        # Locate parquet files
        parquet_targets = []
        if files:
            parquet_targets = [f.replace("s3://", "s3a://") for f in files if f.endswith(".parquet")]
        if not parquet_targets and export_prefix:
            p = Path(export_prefix)
            if p.is_dir():
                parquet_targets = [str(f.resolve().as_posix()) for f in p.glob("*.parquet")]

        if not parquet_targets:
            raise FileNotFoundError(f"No Parquet files found for manifest run_id={run_id}")

        con = self._get_connection()
        try:
            con.begin()

            # 1. Create temporary staging table
            con.execute("CREATE OR REPLACE TEMP TABLE stg_load AS SELECT * FROM read_parquet(?);", [parquet_targets])

            # 2. Assert staged row count == manifest row_count
            staged_count = con.execute("SELECT COUNT(*) FROM stg_load;").fetchone()[0]
            _log_json("INFO", "Staged rows count verification", staged=staged_count, expected=expected_rows, run_id=run_id)

            if staged_count != expected_rows:
                raise ValueError(
                    f"Row count mismatch for {table} run_id={run_id}: staged={staged_count}, manifest={expected_rows}"
                )

            # 3. Idempotent delete-insert on (ticker, window_start)
            cols = [
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
            cols_str = ", ".join(cols)

            con.execute(f"""
            DELETE FROM {table}
            WHERE (ticker, window_start) IN (
                SELECT ticker, window_start FROM stg_load
            );
            """)

            con.execute(f"INSERT INTO {table} ({cols_str}) SELECT {cols_str} FROM stg_load;")


            # 4. Insert into load_audit
            con.execute(
                "INSERT INTO load_audit (run_id, table_name, row_count, loaded_at, status) VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'SUCCESS');",
                [run_id, table, staged_count],
            )

            con.commit()
            _log_json("INFO", "DuckDB load transaction committed successfully", table=table, run_id=run_id, rows_loaded=staged_count)

            return {
                "status": "SUCCESS",
                "warehouse": "duckdb",
                "table": table,
                "run_id": run_id,
                "rows_loaded": staged_count,
            }
        except Exception as e:
            con.rollback()
            _log_json("ERROR", f"DuckDB warehouse load failed: {e}", table=table, run_id=run_id)
            raise
        finally:
            con.close()


class RedshiftWarehouse(BaseWarehouse):
    """
    Amazon Redshift warehouse implementation using the Redshift Data API.
    Executes atomic BatchExecuteStatement containing:
    1. CREATE TEMP TABLE stg (LIKE target);
    2. COPY stg FROM s3_prefix IAM_ROLE role_arn FORMAT AS PARQUET;
    3. DELETE FROM target USING stg WHERE ticker=ticker AND window_start=window_start;
    4. INSERT INTO target SELECT * FROM stg;
    5. INSERT INTO load_audit ...
    6. SELECT COUNT(*) FROM stg;
    """

    def __init__(
        self,
        workgroup_name: Optional[str] = None,
        cluster_identifier: Optional[str] = None,
        database: Optional[str] = None,
        copy_role_arn: Optional[str] = None,
        secret_arn: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> None:
        import boto3
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.workgroup_name = workgroup_name or os.getenv("REDSHIFT_WORKGROUP")
        self.cluster_identifier = cluster_identifier or os.getenv("REDSHIFT_CLUSTER_IDENTIFIER")
        self.database = database or os.getenv("REDSHIFT_DATABASE", "dev")
        self.copy_role_arn = copy_role_arn or os.getenv("REDSHIFT_COPY_ROLE_ARN")
        self.secret_arn = secret_arn or os.getenv("REDSHIFT_SECRET_ARN")

        self.client = boto3.client("redshift-data", region_name=self.region_name)

    def _execute_sql_batch(self, sql_statements: List[str]) -> str:
        """Call Redshift Data API BatchExecuteStatement and return statement execution ID."""
        params: Dict[str, Any] = {
            "Database": self.database,
            "Sqls": sql_statements,
        }
        if self.workgroup_name:
            params["WorkgroupName"] = self.workgroup_name
        elif self.cluster_identifier:
            params["ClusterIdentifier"] = self.cluster_identifier

        if self.secret_arn:
            params["SecretArn"] = self.secret_arn

        resp = self.client.batch_execute_statement(**params)
        return resp["Id"]

    def _wait_for_statement(self, statement_id: str, timeout_seconds: int = 180) -> Dict[str, Any]:
        """Poll DescribeStatement until FINISHED, FAILED, or ABORTED."""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            desc = self.client.describe_statement(Id=statement_id)
            status = desc.get("Status")

            if status == "FINISHED":
                return desc
            elif status in ("FAILED", "ABORTED"):
                error_msg = desc.get("Error", "Unknown Redshift error")
                raise RuntimeError(f"Redshift statement {statement_id} failed with status '{status}': {error_msg}")

            time.sleep(2.0)

        raise TimeoutError(f"Redshift statement {statement_id} timed out after {timeout_seconds}s")

    def load_manifest(self, manifest: Dict[str, Any], s3_bucket: Optional[str] = None) -> Dict[str, Any]:
        table = manifest["table"]
        run_id = manifest["run_id"]
        expected_rows = int(manifest["row_count"])
        export_prefix = manifest.get("export_prefix", "")

        if not self.copy_role_arn:
            raise ValueError("REDSHIFT_COPY_ROLE_ARN environment variable is required for Redshift COPY.")

        # Resolve S3 prefix URI for Redshift COPY
        if not export_prefix.startswith("s3://"):
            if s3_bucket:
                # Build s3:// URI from bucket and relative prefix
                clean_prefix = export_prefix.lstrip("/")
                s3_copy_path = f"s3://{s3_bucket}/{clean_prefix}"
            else:
                s3_copy_path = export_prefix.replace("s3a://", "s3://")
        else:
            s3_copy_path = export_prefix

        # Ensure trailing slash for directory copy
        if not s3_copy_path.endswith("/"):
            s3_copy_path += "/"

        # Redshift transactional batch statements
        sql_batch = [
            f"CREATE TEMP TABLE stg_load (LIKE {table});",
            f"COPY stg_load FROM '{s3_copy_path}' IAM_ROLE '{self.copy_role_arn}' FORMAT AS PARQUET;",
            f"DELETE FROM {table} USING stg_load WHERE {table}.ticker = stg_load.ticker AND {table}.window_start = stg_load.window_start;",
            f"INSERT INTO {table} SELECT * FROM stg_load;",
            f"INSERT INTO load_audit (run_id, table_name, row_count, loaded_at, status) VALUES ('{run_id}', '{table}', {expected_rows}, SYSDATE, 'SUCCESS');",
            "SELECT COUNT(*) FROM stg_load;",
        ]

        _log_json("INFO", "Submitting Redshift Data API batch statements", table=table, run_id=run_id, path=s3_copy_path)
        statement_id = self._execute_sql_batch(sql_batch)

        # Poll for completion
        desc = self._wait_for_statement(statement_id)
        sub_statements = desc.get("SubStatements", [])

        # Retrieve result from the last statement: SELECT COUNT(*) FROM stg_load
        staged_count = expected_rows
        if sub_statements:
            last_sub_id = sub_statements[-1]["Id"]
            res = self.client.get_statement_result(Id=last_sub_id)
            records = res.get("Records", [])
            if records and records[0]:
                staged_count = int(records[0][0].get("longValue", records[0][0].get("stringValue", expected_rows)))

        _log_json("INFO", "Redshift batch load succeeded", table=table, run_id=run_id, staged_rows=staged_count)

        if staged_count != expected_rows:
            raise ValueError(f"Staged count {staged_count} != manifest count {expected_rows} for run_id={run_id}")

        return {
            "status": "SUCCESS",
            "warehouse": "redshift",
            "table": table,
            "run_id": run_id,
            "rows_loaded": staged_count,
            "statement_id": statement_id,
        }


def get_warehouse_target() -> BaseWarehouse:
    """Factory selecting warehouse implementation based on WAREHOUSE_TARGET env var."""
    target = os.getenv("WAREHOUSE_TARGET", "duckdb").lower().strip()
    if target == "redshift":
        return RedshiftWarehouse()
    return DuckDBWarehouse()


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Main Lambda entry point.
    Handles both S3 ObjectCreated events and direct invocation payloads.
    """
    _log_json("INFO", "Received invocation event", event=event)

    manifest_data: Optional[Dict[str, Any]] = None
    s3_bucket: Optional[str] = None

    # 1. Parse direct manual event (manifest_path)
    if "manifest_path" in event:
        path = Path(event["manifest_path"])
        if not path.is_file():
            raise FileNotFoundError(f"Manifest file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)

    # 2. Parse direct embedded manifest
    elif "manifest" in event and isinstance(event["manifest"], dict):
        manifest_data = event["manifest"]

    # 3. Parse standard S3 event
    elif "Records" in event and len(event["Records"]) > 0:
        first_record = event["Records"][0]
        s3_info = first_record.get("s3", {})
        s3_bucket = s3_info.get("bucket", {}).get("name")
        s3_key = s3_info.get("object", {}).get("key")

        if not s3_key or not s3_key.endswith("manifest.json"):
            _log_json("INFO", f"Skipping non-manifest S3 object key: {s3_key}")
            return {"statusCode": 200, "body": json.dumps({"status": "SKIPPED", "key": s3_key})}

        if Path(s3_key).is_file():
            with open(s3_key, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        else:
            import boto3
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            manifest_data = json.loads(resp["Body"].read().decode("utf-8"))

    # 4. Parse direct manual event (manifest_key + optional bucket)
    elif "manifest_key" in event:
        s3_key = event["manifest_key"]
        s3_bucket = event.get("bucket") or os.getenv("EXPORT_BUCKET")
        if Path(s3_key).is_file():
            with open(s3_key, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        else:
            import boto3
            s3 = boto3.client("s3")
            resp = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            manifest_data = json.loads(resp["Body"].read().decode("utf-8"))

    else:
        raise ValueError(f"Unrecognized event payload format: {event}")

    if not manifest_data:
        raise ValueError("Failed to resolve manifest data from event.")

    # Execute warehouse load
    warehouse = get_warehouse_target()
    result = warehouse.load_manifest(manifest_data, s3_bucket=s3_bucket)

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
