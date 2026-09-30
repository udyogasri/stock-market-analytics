# Pipeline Architecture & Data Contracts

> **Phase 7 Documentation**: Complete structural documentation of the real-time stock market streaming analytics platform, covering end-to-end data pipelines, table schemas, monitoring systems, and failure modes.

---

## 1. System Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        P["Trade Producer (producer.py)<br/>• 20 NASDAQ Tickers<br/>• Idempotence: acks=all<br/>• Multiprocessing Workers"]
        K["Apache Kafka (KRaft mode)<br/>• Topic: stock-trades<br/>• Partitioned by Ticker"]
        P -->|JSON Trade Events| K
    end

    subgraph Processing["2. Stream Processing Layer (PySpark)"]
        SS["Spark Structured Streaming (stream_job.py)<br/>• Trigger: 5s Micro-batches<br/>• 10-Min Event-Time Watermark<br/>• Static Metadata Broadcast Join"]
        K -->|Consumer Stream| SS

        ML["PipelineMetricsListener<br/>(listener.py)<br/>• Throughput & Lag<br/>• Batch Durations<br/>• State Store Metrics"]
        SS -.->|onQueryProgress| ML
    end

    subgraph Observability["3. Observability & Monitoring"]
        FS["FileMetricsSink<br/>(monitoring/metrics.log)<br/>• Rotating JSON lines"]
        CW["CloudWatchMetricsSink<br/>(Async background queue)<br/>*(Untested against real AWS)*"]
        WP["Live Terminal Dashboard<br/>(monitoring/watch.py)"]
        ML --> FS
        ML -.-> CW
        FS --> WP
    end

    subgraph Storage["4. Delta Lake Storage Layer (ACID)"]
        RAW["delta/raw_trades<br/>• Append-only<br/>• Idempotent (txnAppId + txnVersion)<br/>• Partitioned by trade_date"]
        A5["delta/agg_5min<br/>• 5-Min OHLCV + Metadata<br/>• MERGE INTO (ticker + window_start)"]
        A1["delta/agg_1hour<br/>• 1-Hour OHLCV + Metadata<br/>• MERGE INTO (ticker + window_start)"]
        
        SS -->|write_raw| RAW
        SS -->|write_agg| A5
        SS -->|write_agg| A1
    end

    subgraph WarehouseLoad["5. Batch Export & Warehouse Ingestion"]
        EXP["Export Finalized Windows<br/>(export/export_finalized.py)<br/>• High-water mark state tracking"]
        A5 --> EXP
        A1 --> EXP

        LAMBDA["Lambda Ingestion Handler<br/>(lambda/load_to_redshift/handler.py)"]
        EXP -->|Manifest & Parquet| LAMBDA

        DUCK["Local DuckDB Warehouse<br/>(warehouse/local.duckdb)<br/>• Offline Local Dev & Test"]
        RED["Amazon Redshift<br/>• Cloud Data Warehouse<br/>*(Implementation complete, deployment pending)*"]
        
        LAMBDA -->|Local Runner / Verification| DUCK
        LAMBDA -.->|S3 Copy / Data API (Pending)| RED
    end

    subgraph Presentation["6. Presentation & Serving Layer"]
        ST["Streamlit Analytics Dashboard<br/>(dashboard/app.py)<br/>• Tab 1: Live Streaming (Delta Lake)<br/>• Tab 2: Historical Analytics (Warehouse)"]
        RAW --> ST
        A5 --> ST
        DUCK --> ST
    end
```

---

## 2. Data Contracts & Table Schemas

All schemas below are pulled directly from [`streaming/sinks.py`](file:///d:/KMEdTech/Realtime%20stock%20market%20streaming%20platform/streaming/sinks.py) and [`warehouse/schema.sql`](file:///d:/KMEdTech/Realtime%20stock%20market%20streaming%20platform/warehouse/schema.sql).

### Table 1: `raw_trades` (Delta Lake)
- **Path**: `delta/raw_trades`
- **Write Mode**: Append-only micro-batch with Delta transactional idempotence (`txnAppId`, `txnVersion`).
- **Partitioning**: Partitioned by `trade_date` (DateType).

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `event_id` | `StringType` | True | Unique UUID v4 identifying the trade event |
| `ticker` | `StringType` | True | Stock ticker symbol (e.g., AAPL, NVDA, TSLA) |
| `price` | `DoubleType` | True | Executed trade price in USD |
| `volume` | `IntegerType` | True | Quantity of shares traded |
| `timestamp` | `TimestampType`| True | UTC timestamp when the trade occurred |
| `trade_date` | `DateType` | True | Partition column extracted from `timestamp` (UTC date) |
| `kafka_partition`| `IntegerType`| True | Kafka topic partition from which event was consumed |
| `kafka_offset` | `LongType` | True | Kafka offset within the partition |
| `ingested_at` | `TimestampType`| True | UTC timestamp when Spark ingested and committed row |

---

### Table 2: `agg_5min` and `agg_1hour` (Delta Lake & Warehouse)
- **Delta Path**: `delta/agg_5min` and `delta/agg_1hour`
- **Write Mode**: Idempotent `MERGE INTO` keyed on `(ticker, window_start)`.
- **Enrichment**: Broadcast-joined with `data/company_metadata.csv` on `ticker`.

| Column Name | Spark Delta Type | Warehouse SQL Type | Description |
| :--- | :--- | :--- | :--- |
| `ticker` | `StringType` | `VARCHAR(16) NOT NULL` | Stock ticker symbol |
| `company_name` | `StringType` | `VARCHAR(128)` | Full corporate name from company metadata |
| `sector` | `StringType` | `VARCHAR(64)` | Industry sector (e.g., Technology, Consumer Discretionary) |
| `exchange` | `StringType` | `VARCHAR(16)` | Primary stock exchange (e.g., NASDAQ) |
| `window_start` | `TimestampType` | `TIMESTAMP NOT NULL` | Inclusive start of window boundary (UTC) |
| `window_end` | `TimestampType` | `TIMESTAMP NOT NULL` | Exclusive end of window boundary (UTC) |
| `open` | `DoubleType` | `DOUBLE PRECISION NOT NULL`| Price of trade with earliest `(timestamp, kafka_offset)` |
| `high` | `DoubleType` | `DOUBLE PRECISION NOT NULL`| Maximum trade price during window |
| `low` | `DoubleType` | `DOUBLE PRECISION NOT NULL`| Minimum trade price during window |
| `close` | `DoubleType` | `DOUBLE PRECISION NOT NULL`| Price of trade with latest `(timestamp, kafka_offset)` |
| `volume` | `LongType` | `BIGINT NOT NULL` | Cumulative shares traded across all trades in window |
| `trade_count` | `LongType` | `BIGINT NOT NULL` | Total count of distinct trade events in window |

---

### Table 3: `load_audit` (Warehouse Audit Trail)
- **Database**: DuckDB (`warehouse/local.duckdb`) and Amazon Redshift (schema.sql).
- **Purpose**: Tracks batch load manifest executions, idempotency, and loaded row counts.

| Column Name | SQL Type | Description |
| :--- | :--- | :--- |
| `run_id` | `VARCHAR(64) NOT NULL` | Unique batch execution or manifest run UUID |
| `table_name` | `VARCHAR(64) NOT NULL` | Target destination table (`agg_5min` or `agg_1hour`) |
| `row_count` | `BIGINT NOT NULL` | Number of rows loaded or updated in this run |
| `loaded_at` | `TIMESTAMP DEFAULT SYSDATE`| Ingestion completion timestamp |
| `status` | `VARCHAR(32) NOT NULL` | Execution outcome (`SUCCESS` or `FAILED`) |

---

## 3. Component Failure Matrix ("What Happens If X Crashes")

| Pipeline Component | Failure Scenario | Detection Mechanism | Recovery & Resilience Behavior |
| :--- | :--- | :--- | :--- |
| **Trade Producer** (`producer.py`) | Process crash, SIGKILL, or unhandled exception. | Exit code, process supervisor. | • With `enable.idempotence=true` and `acks=all`, any message ACKed before crash is committed to Kafka with assigned sequence numbers.<br/>• Un-ACKed in-flight messages are safely discarded by producer client buffers.<br/>• On restart, ticker price random walks resume without corrupting topic partitions. |
| **Kafka Broker** (`localhost:9092`) | Broker process terminated or temporary network partition. | TCP connection refusal, producer buffer alerts, Spark source warnings. | • Producer buffers in memory (up to 500k messages / 1GB) and retries with backoff.<br/>• PySpark Structured Streaming Kafka consumer detects connection drop, logs warnings, and retries automatically.<br/>• When Kafka restarts, KRaft logs and partition states are preserved; Spark reconnects and drains the backlog with zero data loss and no manual restart required. |
| **Spark Streaming Job** (`stream_job.py`) | Mid-batch hard kill (`kill -9`), OOM, or JVM termination. | Process exit, metrics logging pauses. | • Delta Lake write operations are atomic. Uncommitted micro-batches leave no visible commits in Delta transaction log.<br/>• Checkpoints in `checkpoints/phase3/` preserve Kafka offset ranges and WAL state.<br/>• Upon restart, Spark reads checkpoint WAL, re-fetches exact uncommitted micro-batch offsets from Kafka, and re-executes idempotent write (`txnAppId`/`txnVersion` for `raw_trades`, MERGE INTO for aggregates). Zero duplicate records, zero gaps. |
| **Delta Lake Storage Writer** (`sinks.py`) | Crash during parquet file write or metadata log commit. | PySpark exception, failed micro-batch trigger. | • ACID transactions guarantee atomicity: partial files are ignored until atomic commit in `_delta_log/*.json`.<br/>• Concurrent readers (Streamlit dashboard, export scripts) only observe committed snapshots.<br/>• Orphaned parquet files from uncommitted writes are safely purged via `VACUUM` maintenance. |
| **Batch Export Job** (`export_finalized.py`) | Crash mid-export or before state update. | Process termination, incomplete manifest file. | • Inspects `export_state.json` high-water mark.<br/>• Queries only finalized windows (`window_end <= current_watermark`).<br/>• If terminated mid-run, re-running detects existing export files or exports missing windows idempotently. |
| **Lambda Ingestion Handler** (`local_lambda_runner.py`) | Worker timeout, database lock, or crash during load. | AWS Lambda CloudWatch error / local runner exception. | • Target warehouse queries execute within atomic staging table transactions (`BEGIN ... DELETE FROM ... INSERT INTO ... COMMIT`).<br/>• Keyed on `(ticker, window_start)` to guarantee that re-running the same manifest produces strictly identical results.<br/>• Writes status to `load_audit` upon completion. |
| **Amazon Redshift** | **Status: Not Yet Deployed** (Verified locally against DuckDB). | N/A (Deployment pending). | *Redshift cluster crash recovery will be handled by AWS Redshift managed cluster failover and transactional COPY/DELETE-INSERT staging tables upon cloud deployment.* |
