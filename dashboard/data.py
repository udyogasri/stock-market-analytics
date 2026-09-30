"""
Data access layer for the Phase 6 Streamlit Analytics Dashboard.

Provides read-only access to:
1. Delta Lake tables (via delta-rs / deltalake package, no Spark session).
   - Live view reads Delta tables directly.
   - raw_trades queries strictly prune by trade_date partition.
2. Warehouse tables (Amazon Redshift or local DuckDB).
   - Parameterized queries only (no string concatenation for user parameters).
   - Historical view reads warehouse.
3. Pipeline metrics (monitoring/metrics.log or AWS CloudWatch get_metric_data).
   - Latency, throughput, consumer lag, and data freshness calculations.
"""

from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from config import (
    DELTA_BASE_PATH,
    DUCKDB_PATH,
    WAREHOUSE_TARGET,
    METRICS_LOG_PATH,
    METRICS_SINK,
    CLOUDWATCH_NAMESPACE,
    AWS_REGION,
    REDSHIFT_DATABASE,
    REDSHIFT_WORKGROUP,
    REDSHIFT_CLUSTER_IDENTIFIER,
)

# ---------------------------------------------------------------------------
# 1. Delta Lake Access (Live View - Read Only, No Spark)
# ---------------------------------------------------------------------------

def _get_delta_storage_options() -> Dict[str, str]:
    """Configure storage options for S3 access when DELTA_BASE_PATH is on AWS."""
    options = {}
    if DELTA_BASE_PATH.startswith("s3://") or DELTA_BASE_PATH.startswith("s3a://"):
        options["AWS_REGION"] = AWS_REGION
        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            options["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "")
            options["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        if os.getenv("AWS_SESSION_TOKEN"):
            options["AWS_SESSION_TOKEN"] = os.getenv("AWS_SESSION_TOKEN", "")
    return options


def get_delta_table(
    table_name: str,
    trade_date: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Read a Delta table using the `deltalake` package (delta-rs) without Spark.
    
    If table_name == 'raw_trades', requires trade_date partition filtering to
    prevent full-table scans.
    """
    import deltalake

    # Normalize URI for delta-rs (convert s3a:// to s3://)
    base_uri = DELTA_BASE_PATH.replace("s3a://", "s3://").rstrip("/\\")
    table_uri = f"{base_uri}/{table_name}"
    
    # Check existence for local paths
    if not table_uri.startswith("s3://"):
        local_path = Path(table_uri).resolve()
        if not (local_path / "_delta_log").is_dir():
            raise FileNotFoundError(f"Delta table '{table_name}' not found at {local_path}")
        table_uri_str = str(local_path)
    else:
        table_uri_str = table_uri

    storage_options = _get_delta_storage_options()
    dt = deltalake.DeltaTable(table_uri_str, storage_options=storage_options)

    partition_filters = None
    if table_name == "raw_trades":
        if not trade_date:
            trade_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        partition_filters = [("trade_date", "=", trade_date)]

    # Read data: Try direct deltalake -> pandas; fallback to reading active Parquet URIs via DuckDB
    # if pyarrow native DLL is restricted by system security policies
    try:
        return dt.to_pandas(partitions=partition_filters, columns=columns)
    except Exception:
        # Fallback to reading delta-rs resolved active files directly via DuckDB's native Parquet reader
        uris = dt.file_uris(partition_filters=partition_filters) if partition_filters else dt.file_uris()
        files = [u.replace("%20", " ") for u in uris]
        if not files:
            return pd.DataFrame()

        conn = duckdb.connect()
        try:
            col_sql = ", ".join(f'"{c}"' for c in columns) if columns else "*"
            df = conn.execute(f"SELECT {col_sql} FROM read_parquet(?)", [files]).df()
            return df
        finally:
            conn.close()


def get_live_candles(table_name: str = "agg_5min", ticker: Optional[str] = None) -> pd.DataFrame:
    """Fetch windowed candles from the live Delta table, sorted by window_start."""
    df = get_delta_table(table_name)
    if df.empty:
        return df
    
    if ticker:
        df = df[df["ticker"] == ticker]
    
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])
    df = df.sort_values(by="window_start").reset_index(drop=True)
    return df


def get_live_top_movers(table_name: str = "agg_5min") -> pd.DataFrame:
    """
    Compute top-movers table from live Delta table:
    ticker, company_name, sector, last price, % change over the last ~60 minutes
    (latest close vs close of the window ending ~60 minutes earlier; if history is shorter,
    use the earliest window and label the column 'change since first window'), volume.
    """
    df = get_delta_table(table_name)
    if df.empty:
        return pd.DataFrame()

    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])

    movers = []
    # Identify overall time span to determine column label
    all_tickers = df["ticker"].unique()

    for ticker in all_tickers:
        tdf = df[df["ticker"] == ticker].sort_values(by="window_end")
        if tdf.empty:
            continue
        
        latest_row = tdf.iloc[-1]
        latest_time = latest_row["window_end"]
        latest_close = float(latest_row["close"])
        target_60m = latest_time - timedelta(minutes=60)

        # Look for window ending closest to 60m ago
        past_candidates = tdf[tdf["window_end"] <= target_60m]
        is_shorter = False
        if not past_candidates.empty:
            ref_row = past_candidates.iloc[-1]
        else:
            ref_row = tdf.iloc[0]
            if len(tdf) > 1 and (latest_time - tdf.iloc[0]["window_end"]).total_seconds() < 3000:
                is_shorter = True

        ref_close = float(ref_row["close"])
        pct_change = ((latest_close - ref_close) / ref_close * 100.0) if ref_close > 0 else 0.0

        # Sum volume over recent window or ticker total
        total_vol = int(tdf["volume"].sum())

        movers.append({
            "ticker": ticker,
            "company_name": latest_row.get("company_name", ticker),
            "sector": latest_row.get("sector", "N/A"),
            "last_price": latest_close,
            "pct_change": round(pct_change, 2),
            "change_type": "change since first window" if is_shorter else "% change (60m)",
            "volume": total_vol,
        })

    movers_df = pd.DataFrame(movers)
    if not movers_df.empty:
        movers_df = movers_df.sort_values(by="pct_change", ascending=False).reset_index(drop=True)
    return movers_df


# ---------------------------------------------------------------------------
# 2. Warehouse Access (Historical View - Read Only, Parameterized Queries)
# ---------------------------------------------------------------------------

def test_warehouse_connection() -> Tuple[bool, str]:
    """Test warehouse reachability and return (is_healthy, status_message)."""
    target = WAREHOUSE_TARGET.lower()
    if target == "duckdb":
        db_path = Path(DUCKDB_PATH).resolve()
        if not db_path.is_file():
            return False, f"DuckDB warehouse file not found at {db_path}"
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            tables = conn.execute("SHOW TABLES").fetchall()
            conn.close()
            return True, f"DuckDB connected ({len(tables)} tables available)"
        except Exception as e:
            return False, f"DuckDB connection error: {str(e)}"
    elif target == "redshift":
        try:
            import boto3
            client = boto3.client("redshift-data", region_name=AWS_REGION)
            kwargs = {"Database": REDSHIFT_DATABASE, "Sql": "SELECT 1;"}
            if REDSHIFT_WORKGROUP:
                kwargs["WorkgroupName"] = REDSHIFT_WORKGROUP
            elif REDSHIFT_CLUSTER_IDENTIFIER:
                kwargs["ClusterIdentifier"] = REDSHIFT_CLUSTER_IDENTIFIER
            resp = client.execute_statement(**kwargs)
            return True, f"Redshift Data API reachable (StatementId: {resp.get('Id')})"
        except Exception as e:
            return False, f"Redshift unreachable: {str(e)}"
    else:
        return False, f"Unsupported WAREHOUSE_TARGET: {target}"


def query_warehouse_candles(
    table_name: str = "agg_5min",
    tickers: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sectors: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Execute parameterized read-only query against warehouse (DuckDB or Redshift).
    Never uses string concatenation for user parameters.
    """
    if table_name not in ("agg_5min", "agg_1hour"):
        raise ValueError(f"Invalid warehouse table: {table_name}")

    target = WAREHOUSE_TARGET.lower()

    if target == "duckdb":
        db_path = Path(DUCKDB_PATH).resolve()
        if not db_path.is_file():
            return pd.DataFrame()

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            sql = f"SELECT * FROM {table_name} WHERE 1=1"
            params: List[Any] = []

            if start_date:
                sql += " AND window_start >= ?"
                params.append(start_date)

            if end_date:
                sql += " AND window_end <= ?"
                params.append(end_date)

            if tickers:
                placeholders = ", ".join(["?"] * len(tickers))
                sql += f" AND ticker IN ({placeholders})"
                params.extend(tickers)

            if sectors:
                placeholders = ", ".join(["?"] * len(sectors))
                sql += f" AND sector IN ({placeholders})"
                params.extend(sectors)

            sql += " ORDER BY window_start ASC, ticker ASC"

            df = conn.execute(sql, params).df()
            if not df.empty:
                df["window_start"] = pd.to_datetime(df["window_start"])
                df["window_end"] = pd.to_datetime(df["window_end"])
            return df
        finally:
            conn.close()

    elif target == "redshift":
        import boto3
        client = boto3.client("redshift-data", region_name=AWS_REGION)
        
        # Build parameterized Redshift Data API statement
        sql = f"SELECT * FROM {table_name} WHERE 1=1"
        parameters = []

        if start_date:
            sql += " AND window_start >= :start_date"
            parameters.append({"name": "start_date", "value": str(start_date)})

        if end_date:
            sql += " AND window_end <= :end_date"
            parameters.append({"name": "end_date", "value": str(end_date)})

        if tickers:
            t_clauses = []
            for i, t in enumerate(tickers):
                param_name = f"t_{i}"
                t_clauses.append(f":{param_name}")
                parameters.append({"name": param_name, "value": t})
            sql += f" AND ticker IN ({', '.join(t_clauses)})"

        if sectors:
            s_clauses = []
            for i, s in enumerate(sectors):
                param_name = f"s_{i}"
                s_clauses.append(f":{param_name}")
                parameters.append({"name": param_name, "value": s})
            sql += f" AND sector IN ({', '.join(s_clauses)})"

        sql += " ORDER BY window_start ASC, ticker ASC"

        kwargs: Dict[str, Any] = {
            "Database": REDSHIFT_DATABASE,
            "Sql": sql,
            "Parameters": parameters,
        }
        if REDSHIFT_WORKGROUP:
            kwargs["WorkgroupName"] = REDSHIFT_WORKGROUP
        elif REDSHIFT_CLUSTER_IDENTIFIER:
            kwargs["ClusterIdentifier"] = REDSHIFT_CLUSTER_IDENTIFIER

        exec_resp = client.execute_statement(**kwargs)
        statement_id = exec_resp["Id"]

        # Poll for completion (read-only query)
        for _ in range(60):
            desc = client.describe_statement(Id=statement_id)
            status = desc.get("Status")
            if status == "FINISHED":
                break
            elif status in ("FAILED", "ABORTED"):
                raise RuntimeError(f"Redshift query {statement_id} failed: {desc.get('Error')}")
            time.sleep(0.5)

        # Retrieve rows
        result = client.get_statement_result(Id=statement_id)
        col_names = [c["name"] for c in result.get("ColumnMetadata", [])]
        rows = []
        for r in result.get("Records", []):
            row_vals = [list(val.values())[0] if val else None for val in r]
            rows.append(row_vals)

        df = pd.DataFrame(rows, columns=col_names)
        if not df.empty and "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])
            df["window_end"] = pd.to_datetime(df["window_end"])
        return df

    return pd.DataFrame()


def get_warehouse_metadata() -> Dict[str, Any]:
    """Retrieve distinct tickers, sectors, and date boundaries from warehouse."""
    target = WAREHOUSE_TARGET.lower()
    if target == "duckdb":
        db_path = Path(DUCKDB_PATH).resolve()
        if not db_path.is_file():
            return {"tickers": [], "sectors": [], "min_date": None, "max_date": None, "total_rows": 0}

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            # Query agg_5min metadata
            res = conn.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(window_start) as min_date,
                    MAX(window_end) as max_date
                FROM agg_5min;
            """).fetchone()

            tickers = [r[0] for r in conn.execute("SELECT DISTINCT ticker FROM agg_5min ORDER BY ticker").fetchall()]
            sectors = [r[0] for r in conn.execute("SELECT DISTINCT sector FROM agg_5min WHERE sector IS NOT NULL ORDER BY sector").fetchall()]

            return {
                "tickers": tickers,
                "sectors": sectors,
                "min_date": res[1] if res else None,
                "max_date": res[2] if res else None,
                "total_rows": res[0] if res else 0,
            }
        finally:
            conn.close()

    # Fallback default
    return {"tickers": [], "sectors": [], "min_date": None, "max_date": None, "total_rows": 0}


# ---------------------------------------------------------------------------
# 3. Pipeline Health & Metrics Access
# ---------------------------------------------------------------------------

def get_pipeline_metrics_history(limit: int = 500) -> pd.DataFrame:
    """
    Parse monitoring/metrics.log into a structured DataFrame.
    If METRICS_SINK is cloudwatch, queries CloudWatch get_metric_data when available.
    """
    # 1. CloudWatch option if configured
    if "cloudwatch" in METRICS_SINK:
        try:
            import boto3
            cw = boto3.client("cloudwatch", region_name=AWS_REGION)
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(hours=1)
            queries = [
                {
                    "Id": "lag",
                    "MetricStat": {
                        "Metric": {"Namespace": CLOUDWATCH_NAMESPACE, "MetricName": "KafkaConsumerLag"},
                        "Period": 60,
                        "Stat": "Maximum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "throughput",
                    "MetricStat": {
                        "Metric": {"Namespace": CLOUDWATCH_NAMESPACE, "MetricName": "ProcessedRowsPerSecond"},
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "duration",
                    "MetricStat": {
                        "Metric": {"Namespace": CLOUDWATCH_NAMESPACE, "MetricName": "BatchDurationMs"},
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                },
            ]
            resp = cw.get_metric_data(MetricDataQueries=queries, StartTime=start_time, EndTime=now)
            # If metrics returned, convert to dataframe
            cw_records = []
            results = {r["Id"]: r for r in resp.get("MetricDataResults", [])}
            if "throughput" in results and results["throughput"]["Timestamps"]:
                for i, ts in enumerate(results["throughput"]["Timestamps"]):
                    cw_records.append({
                        "timestamp": ts,
                        "query_name": "cloudwatch_aggregate",
                        "processed_rows_per_second": results["throughput"]["Values"][i],
                        "kafka_consumer_lag": results.get("lag", {}).get("Values", [0])[min(i, len(results.get("lag", {}).get("Values", [0])) - 1)] if results.get("lag") else 0,
                        "batch_duration_ms": results.get("duration", {}).get("Values", [0])[min(i, len(results.get("duration", {}).get("Values", [0])) - 1)] if results.get("duration") else 0,
                        "num_input_rows": 0,
                    })
                return pd.DataFrame(cw_records)
        except Exception:
            # Fall back to file if CloudWatch fails or credentials missing
            pass

    # 2. File-based metrics.log parser
    log_path = Path(METRICS_LOG_PATH).resolve()
    if not log_path.is_file():
        return pd.DataFrame()

    records = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # Read the most recent lines up to limit
        for line in lines[-limit:]:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                records.append(data)
            except json.JSONDecodeError:
                continue
    except Exception:
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(by="timestamp").reset_index(drop=True)
    return df


def get_live_summary_metrics() -> Dict[str, Any]:
    """
    Compute live metric card numbers:
    - events/sec (processed_rows_per_second)
    - consumer lag
    - active tickers
    - last batch duration (ms)
    - data freshness (seconds since max window_end)
    - is_stalled flag (true if freshness > 120s)
    """
    metrics_df = get_pipeline_metrics_history(limit=50)

    events_per_sec = 0.0
    consumer_lag = 0
    last_batch_duration = 0.0

    if not metrics_df.empty:
        # Sum of latest processed_rows_per_second across active queries
        latest_queries = metrics_df.groupby("query_name").last()
        events_per_sec = float(latest_queries["processed_rows_per_second"].sum()) if "processed_rows_per_second" in latest_queries else 0.0
        consumer_lag = int(latest_queries["kafka_consumer_lag"].max()) if "kafka_consumer_lag" in latest_queries else 0
        last_batch_duration = float(metrics_df.iloc[-1].get("batch_duration_ms", 0.0))

    # Active tickers & freshness from Delta table
    active_tickers = 0
    freshness_seconds = 99999.0
    max_window_end_str = "None"
    
    try:
        df_5m = get_delta_table("agg_5min")
        if not df_5m.empty:
            active_tickers = int(df_5m["ticker"].nunique())
            max_end = pd.to_datetime(df_5m["window_end"]).max()
            if max_end.tzinfo is None:
                max_end = max_end.replace(tzinfo=timezone.utc)
            max_window_end_str = max_end.strftime("%Y-%m-%d %H:%M:%S UTC")
            now_utc = datetime.now(timezone.utc)
            freshness_seconds = max(0.0, (now_utc - max_end).total_seconds())
    except Exception:
        pass

    # Pipeline stalled threshold: 120 seconds (2 minutes)
    is_stalled = (freshness_seconds > 120.0)

    return {
        "events_per_sec": round(events_per_sec, 1),
        "consumer_lag": consumer_lag,
        "active_tickers": active_tickers,
        "last_batch_duration_ms": round(last_batch_duration, 1),
        "data_freshness_seconds": round(freshness_seconds, 1),
        "max_window_end": max_window_end_str,
        "is_stalled": is_stalled,
    }
