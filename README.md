# Real-Time Stock Market Streaming Platform (Phase 3)

This repository implements an end-to-end real-time stock market streaming pipeline:
High-throughput simulated trade events flow from an Apache Kafka broker (running natively in KRaft mode) into a PySpark Structured Streaming engine with watermarking, windowed OHLCV aggregations, metadata enrichment, and **Delta Lake sinks featuring exactly-once idempotent writes**.

---

## Architecture Overview

```
+------------------------------------+
|  Fake NASDAQ Metadata              |
|  (data/company_metadata.csv)       |
+-----------------+------------------+
                  | (static broadcast)
                  v
+-----------------+------------------+
|  Kafka Producer (producer.py)      |
|  - 20 tickers with random walk     |
|  - Rate control (--rate 100..10k)  |
|  - Exact event count (--max-events)|
|  - Late event injection flag       |
|    (--late-event-minutes N)        |
|  - LZ4 compression & batching      |
+-----------------+------------------+
                  | (stock-trades topic, 12 partitions)
                  v
+-----------------+------------------+
|  Apache Kafka 3.8 (Native KRaft)   |
|  Local broker on localhost:9092    |
+-----------------+------------------+
                  |
                  v
+-----------------+----------------------------------------------------+
|  PySpark Structured Streaming (streaming/stream_job.py)             |
|                                                                      |
|  1. streaming/source.py                                              |
|     - Reads stock-trades from Kafka                                  |
|     - Parses JSON payload with explicit schema                       |
|     - Lineage tracking: kafka_partition, kafka_offset, timestamp     |
|     - Ingest metadata: ingested_at = current_timestamp(), trade_date |
|                                                                      |
|  2. streaming/aggregations.py                                        |
|     - withWatermark("timestamp", "10 minutes")                       |
|     - Windowed OHLCV: 5 minutes and 1 hour                           |
|     - Deterministic open = min_by(price, struct(timestamp, offset))  |
|     - Deterministic close = max_by(price, struct(timestamp, offset)) |
|     - high = max(price), low = min(price)                            |
|     - volume = sum(volume), trade_count = count(*)                   |
|     - Window flattened to window_start, window_end                   |
|                                                                      |
|  3. streaming/enrichment.py                                          |
|     - Loads data/company_metadata.csv                                |
|     - Broadcast join on ticker applied post-aggregation              |
|     - Adds company_name, sector, exchange                           |
|                                                                      |
|  4. streaming/sinks.py (Delta Lake Sinks / ForeachBatch)             |
|     - Table 1: raw_trades (append mode, partitioned by trade_date,   |
|                 idempotent write with txnAppId and txnVersion)       |
|     - Table 2: agg_5min (update mode, MERGE INTO ON                  |
|                 target.ticker = source.ticker AND                    |
|                 target.window_start = source.window_start)           |
|     - Table 3: agg_1hour (update mode, MERGE INTO ON                 |
|                 target.ticker = source.ticker AND                    |
|                 target.window_start = source.window_start)           |
|     - Fallback: --console flag falls back to console sink            |
+-----------------+----------------------------------------------------+
                  |
                  v
+-----------------+----------------------------------------------------+
|  Delta Lake Storage (./delta/)                                       |
|  - delta/raw_trades/ (partitioned by trade_date)                     |
|  - delta/agg_5min/                                                   |
|  - delta/agg_1hour/                                                  |
+----------------------------------------------------------------------+
```

---

## Critical Checkpoint Warning

> [!WARNING]
> **NEVER DELETE A CHECKPOINT DIRECTORY WITHOUT DELETING THE MATCHING DELTA TABLE!**
> Delta Lake's idempotent write mechanism uses `txnVersion` tied directly to the streaming micro-batch ID (`batch_id`). If you delete a checkpoint directory while leaving the Delta table intact, Spark resets its micro-batch ID back to `0`. Delta Lake will then see `batch_id <= last_recorded_version` and **silently skip writing new batches**, resulting in silent data loss.
>
> If you need to reset a stream:
> 1. Stop the streaming job.
> 2. Delete the checkpoint directory under `checkpoints/phase3/<query_name>`.
> 3. Delete the corresponding Delta table directory under `delta/<table_name>`.

---

## Directory Structure

```text
├── config.py                         # Central configuration module reading env vars with defaults
├── data/
│   └── company_metadata.csv          # 20 NASDAQ tickers metadata (ticker, company_name, sector, exchange)
├── delta/                            # Delta Lake tables storage directory
│   ├── raw_trades/                   # Partitioned raw trades Delta table
│   ├── agg_5min/                     # 5-min OHLCV Delta table
│   └── agg_1hour/                    # 1-hour OHLCV Delta table
├── checkpoints/phase3/               # Query checkpoint directories (raw_trades, agg_5min, agg_1hour)
├── hadoop/bin/                       # Windows native Hadoop binaries (winutils.exe, hadoop.dll)
├── producer/
│   └── producer.py                   # Trade simulator (--rate, --max-events, --late-event-minutes)
├── scripts/
│   ├── setup_kafka.bat / .sh         # Download & format Kafka in KRaft mode
│   ├── start_kafka.bat / .sh         # Start Kafka broker
│   ├── create_topic.bat / .sh        # Create stock-trades topic (12 partitions)
│   ├── verify_topic.bat / .sh        # Describe topic
│   ├── stop_kafka.bat / .sh          # Stop Kafka broker
│   └── maintenance.py                # Delta Lake OPTIMIZE & VACUUM maintenance script
├── streaming/
│   ├── __init__.py                   # Package marker
│   ├── source.py                     # Kafka consumer, JSON parser & lineage tracker
│   ├── aggregations.py               # 10-minute watermark & OHLCV with deterministic tie-breaks
│   ├── enrichment.py                 # Company metadata loader & broadcast join
│   ├── sinks.py                      # Delta Lake idempotent foreachBatch writers & schema initializer
│   └── stream_job.py                 # Main streaming orchestrator (Delta Lake or --console)
├── tests/
│   ├── test_phase2.py                # Phase 2 unit & integration tests
│   └── verify_delta.py               # Phase 3 exactly-once validation & batch recompute comparison
└── requirements.txt                  # Python dependencies (pyspark==3.5.0, delta-spark==3.2.0, etc.)
```

---

## Configuration

All modules import settings from `config.py` at the repo root. Configuration is read from environment variables (or `.env` file) with local-friendly defaults:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker bootstrap address |
| `KAFKA_TOPIC` | `stock-trades` | Kafka topic for trade events |
| `DELTA_BASE_PATH` | `./delta` | Path for Delta Lake tables (local or `s3a://...`) |
| `CHECKPOINT_BASE` | `./checkpoints` | Base directory for query checkpoints |
| `SPARK_MASTER` | `local[2]` | Spark master deployment mode |
| `SPARK_TRIGGER_INTERVAL` | `5 seconds` | Micro-batch processing interval |

---

## Setup & Run Instructions

### 0. Install Python Dependencies

```bash
pip install -r requirements.txt
```

*(Note: `delta-spark==3.2.0` is strictly paired with `pyspark==3.5.0` for full compatibility).*

---

### Step 1: Start Kafka Broker (Terminal 1)

If Kafka is not already running, start it locally in KRaft mode:

- **Windows**:
  ```cmd
  scripts\start_kafka.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/start_kafka.sh
  ```

---

### Step 2: Run the PySpark Structured Streaming Job (Terminal 2)

Scripts run from the repository root:

```bash
python -m streaming.stream_job
```

*(Direct execution via `python streaming/stream_job.py` is also supported via path bootstrap).*

Optional flags:
- `--console`: Fall back to console sink output instead of Delta Lake.
- `--trigger-interval "5 seconds"`: Customize micro-batch frequency.
- `--starting-offsets earliest|latest`: Choose starting offset position.

The streaming job:
1. Automatically initializes Delta Lake tables with explicit schemas if they do not exist.
2. Writes `raw_trades` using Delta idempotent writes (`txnAppId="raw_trades_writer"`, `txnVersion=batch_id`).
3. Merges `agg_5min` and `agg_1hour` into Delta using `MERGE INTO ... WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *`.

---

### Step 3: Run the Producer (Terminal 3)

Generate real-time trades:

```bash
# Continuous streaming at 100 events/sec
python producer/producer.py --rate 100

# Or produce an exact count of events (e.g. for testing)
python producer/producer.py --rate 500 --max-events 200000
```

---

## Maintenance: OPTIMIZE and VACUUM

Because the 5-second streaming trigger creates many small files over time, run the maintenance script periodically:

```bash
python scripts/maintenance.py
```

This runs:
- `OPTIMIZE`: Compaction of small Parquet files into optimal larger files.
- `VACUUM`: Cleans up unreferenced historical files using the default 7-day (168 hours) retention window.

---

## Phase 3 Verification

Verification is automated by `tests/verify_delta.py`.

### A) 200,000 Events Exactly-Once Count & Lineage Verification

1. Start the Spark streaming job:
   ```bash
   python -m streaming.stream_job
   ```
2. In another terminal, produce exactly 200,000 events:
   ```bash
   python producer/producer.py --rate 1000 --max-events 200000
   ```
3. Wait for the producer to finish and allow the Spark job to drain all remaining micro-batches.
4. Run the automated verification script:
   ```bash
   python tests/verify_delta.py --expected-events 200000
   ```

**Checks Performed:**
- `raw_trades count == 200,000`
- `count == count(distinct event_id)` (zero duplicate event IDs)
- Zero duplicate `(kafka_partition, kafka_offset)` pairs
- All tickers strictly belong to the expected 20-ticker set

---

### B) OHLCV Batch Recomputation Equivalence Check

Included automatically in `tests/verify_delta.py`:
- Recomputes 5-minute and 1-hour OHLCV metrics directly from `raw_trades` using a plain batch Spark job.
- Breaks open/close ties deterministically using `min_by`/`max_by` on `struct(timestamp, kafka_offset)`.
- Validates that `agg_5min` and `agg_1hour` have exactly one row per `(ticker, window_start)`.
- Compares recomputed values against the Delta tables: `open`, `high`, `low`, `close`, `volume`, and `trade_count` must match exactly.

---

### C) Failure & Mid-Run Crash Recovery Test

To test fault tolerance and exactly-once idempotence across hard crashes:

1. Start the Spark streaming job:
   ```bash
   python -m streaming.stream_job
   ```
2. Start the producer producing 200,000 events:
   ```bash
   python producer/producer.py --rate 1000 --max-events 200000
   ```
3. After ~3 micro-batches have been processed, simulate an abrupt crash by terminating the Spark job process:
   - On Windows PowerShell:
     ```powershell
     Stop-Process -Name "java" -Force
     ```
   - On Linux / macOS:
     ```bash
     kill -9 $(pgrep -f "stream_job")
     ```
4. Restart the Spark streaming job:
   ```bash
   python -m streaming.stream_job
   ```
5. Allow the producer to finish and the Spark job to drain completely.
6. Re-run verification:
   ```bash
   python tests/verify_delta.py --expected-events 200000
   ```

**Expected Result:**
All checks in (A) and (B) pass with zero duplicate rows and zero duplicate offsets, verifying that replayed micro-batches neither duplicated nor lost data.

---

## Phase 4: Pipeline Monitoring (Metrics + CloudWatch)

Phase 4 adds complete observability across all active streaming queries (`raw_trades`, `agg_5min`, `agg_1hour`) using a custom `PipelineMetricsListener` registered directly on the PySpark streaming engine.

### Architecture

```
Spark Streaming Queries
  ├── raw_trades
  ├── agg_5min
  └── agg_1hour
        │
   (onQueryProgress)
        ▼
PipelineMetricsListener
  │  - Throughput (inputRowsPerSec, processedRowsPerSec)
  │  - Micro-batch duration (durationMs.triggerExecution)
  │  - Kafka Consumer Lag (sources[0].metrics["maxOffsetsBehindLatest"])
  │  - Watermark & Stateful operator active rows/memory
  │  - falling_behind flag (duration > trigger interval)
        │
   METRICS_SINK
        ├── file ───────► monitoring/metrics.log (size-based rotation)
        │                         │
        │                         ▼
        │                  monitoring/watch.py (Live Rich Terminal Dashboard)
        │
        └── cloudwatch ──► AWS CloudWatch ("StockPipeline" namespace)
                                  ├── Metrics (Input/Processed Rates, BatchDuration, Lag, StateRows)
                                  ├── Alarm (Kafka consumer lag > threshold for 3 mins)
                                  └── Dashboard (Throughput, Lag, Duration, State widgets)
```

---

### Real-Time Terminal Dashboard (`watch.py`)

To inspect real-time throughput, latency, Kafka lag, and falling-behind alerts in the terminal:

```bash
python monitoring/watch.py
# or
python -m monitoring.watch
```

The live Rich table updates every 2 seconds and displays:
- **Query Name & Batch ID**
- **Input Rate & Processed Rate** (rows/sec)
- **Batch Execution Duration** (ms)
- **Kafka Consumer Lag** (offsets behind latest)
- **Watermark & State Store Rows**
- **Health Status**: `HEALTHY` (green) or `FALLING BEHIND` (red) when batch duration exceeds the 5-second trigger interval.

---

### CloudWatch Resources & AWS Cost Transparency

When running with `METRICS_SINK=cloudwatch` or `METRICS_SINK=both`:
- Metrics are published asynchronously via an in-memory queue to prevent AWS network delays from blocking Spark execution.
- Metrics are published under the `StockPipeline` namespace with the `QueryName` dimension.

#### AWS Costs Incurred:
- **Custom Metrics**: Charged per metric per month (each query publishes 5 custom metrics: `InputRowsPerSecond`, `ProcessedRowsPerSecond`, `BatchDurationMs`, `KafkaOffsetsBehindLatest`, `StateRows`).
- **Alarms**: Charged per metric alarm per month (Standard 1-minute resolution alarm).
- **Dashboards**: Charged per dashboard per month.
- *Always check current [AWS CloudWatch Pricing](https://aws.amazon.com/cloudwatch/pricing/) for up-to-date regional rates.*

#### Provisioning CloudWatch Alarm & Dashboard:
```bash
python scripts/create_cloudwatch_resources.py --threshold 50000
```
This idempotently creates:
1. **Alarm**: `StockPipeline-KafkaConsumerLag-High` — triggers when `KafkaOffsetsBehindLatest` > 50,000 for 3 consecutive 1-minute periods.
2. **Dashboard**: `StockPipeline-Dashboard` — displays throughput, consumer lag, micro-batch duration, and state rows widgets.

#### Tearing Down CloudWatch Resources:
To avoid ongoing charges when testing completes:
```bash
python scripts/delete_cloudwatch_resources.py
```

---

### Phase 4 Verification Scenarios

#### A) Baseline & High Throughput (`METRICS_SINK=file`)
1. Start Spark streaming job:
   ```bash
   python -m streaming.stream_job
   ```
2. Start the live metrics dashboard:
   ```bash
   python monitoring/watch.py
   ```
3. Run producer at baseline rate:
   ```bash
   python producer/producer.py --rate 100
   ```
   **Observation**: Consumer lag is `~0`, processed rate matches input rate (`~100/s`), status is `HEALTHY`.
4. Run producer at high throughput (5,000 events/sec):
   ```bash
   python producer/producer.py --rate 5000
   ```
   **Observation**: Dashboard shows sustained processing rates without pipeline failure.

---

#### B) Artificial Throttling & Lag Recovery (`MAX_OFFSETS_PER_TRIGGER=200`)
1. Set throttle environment variable:
   ```powershell
   # PowerShell:
   $env:MAX_OFFSETS_PER_TRIGGER = "200"
   ```
2. Start the streaming job:
   ```powershell
   python -m streaming.stream_job
   ```
3. Open `python monitoring/watch.py` in another terminal.
4. Run the producer at 2,000 events/sec:
   ```powershell
   python producer/producer.py --rate 2000
   ```
   **Observation**:
   - Because Spark is throttled to 200 offsets per micro-batch, consumer lag steadily climbs.
   - Status switches to `FALLING BEHIND` in red on `watch.py`.
5. Stop the stream, remove the throttle, and restart:
   ```powershell
   Remove-Item Env:MAX_OFFSETS_PER_TRIGGER
   python -m streaming.stream_job
   ```
   **Observation**: Spark processes full batches at maximum throughput; Kafka consumer lag rapidly drains back down to `~0`, and status returns to `HEALTHY`.

---

#### C) CloudWatch Sink & Alarm Triggering (`METRICS_SINK=cloudwatch`)
*(Requires configured AWS credentials)*
1. Set metrics sink:
   ```powershell
   $env:METRICS_SINK = "both"  # publishes to local file and CloudWatch
   ```
2. Provision CloudWatch resources:
   ```powershell
   python scripts/create_cloudwatch_resources.py --threshold 5000
   ```
3. Run the throttling scenario from **(B)**:
   - High production rate with throttled Spark ingestion causes Kafka lag to climb above 5,000.
4. In the AWS CloudWatch Console:
   - Verify metrics under namespace `StockPipeline`.
   - Open dashboard `StockPipeline-Dashboard` to view real-time graphs.
   - Verify the alarm `StockPipeline-KafkaConsumerLag-High` transitions to `ALARM` state after 3 consecutive 1-minute periods above threshold.
5. Clean up when finished:
   ```powershell
   python scripts/delete_cloudwatch_resources.py
   ```

---

## Phase 5: Warehouse Load Path (Delta Lake -> AWS Lambda -> Amazon Redshift)

Phase 5 establishes the automated warehouse batch loading path from Delta Lake into **Amazon Redshift** (or offline local **DuckDB**) orchestrated by an event-driven **AWS Lambda** function.

### Architectural Constraints & Design Rules

1. **Lambda Orchestrates, Spark Processes**:
   AWS Lambda is a lightweight serverless coordinator. It cannot run Apache Spark and never reads Delta transaction logs directly. Instead, a Spark batch job pre-extracts and formats finalized Parquet files, and Lambda coordinates Redshift's native high-performance `COPY` command.
2. **Never COPY Straight from a Delta Folder**:
   Delta Lake streaming tables utilize ACID `MERGE INTO` operations. Even after data compaction, older Parquet files remain in the folder marked as tombstoned in the `_delta_log/`. A direct Redshift `COPY` from a Delta directory would indiscriminately ingest superseded Parquet files, creating stale duplicate records. The warehouse is loaded strictly from clean exports of finalized windows.
3. **Event-Driven Manifest Trigger**:
   The Spark export job writes Parquet data files first and writes `manifest.json` **last**. The arrival of `manifest.json` is the sole atomic S3 trigger for downstream Lambda execution.
4. **Idempotent Delete-Insert Key Replacement**:
   Lambda instructs the warehouse to load data into a temporary staging table, verifies row counts against the manifest, and executes an atomic transaction that deletes existing `(ticker, window_start)` records before inserting the new rows. Re-invocations or duplicate S3 events are 100% idempotent.

```
Delta Lake (agg_5min, agg_1hour)
             │
             ▼
   batch/export_finalized.py (Spark Batch Job)
     - Selects finalized windows: window_end <= (max(timestamp) - 10 min)
     - Formats columns strictly to match Redshift DDL
     - Output timestamp: TIMESTAMP_MICROS (avoids INT96 compatibility bugs)
     - Writes Parquet to {EXPORT_BASE}/{table}/run_id=<uuid>/
     - Writes manifest.json LAST
             │
             ▼
   S3 / Local Storage ({EXPORT_BASE}/.../manifest.json)
             │
      (S3 ObjectCreated or local_lambda_runner.py)
             ▼
   AWS Lambda (lambda/load_to_redshift/handler.py)
     - Parses manifest (row_count, files, table)
     - Executes transactional Redshift Data API / DuckDB statements:
         1. CREATE TEMP TABLE stg (LIKE target);
         2. COPY stg FROM run_id_prefix;
         3. SELECT COUNT(*) FROM stg (asserts staged == manifest count);
         4. DELETE FROM target USING stg WHERE ticker=ticker AND window_start=window_start;
         5. INSERT INTO target SELECT * FROM stg;
         6. INSERT INTO load_audit ...
             │
             ▼
   Warehouse Target (Amazon Redshift Serverless / Local DuckDB)
```

---

### S3 Storage & Single Writer Assumption

- To write Delta tables or exported Parquet to AWS S3, set:
  ```powershell
  $env:DELTA_BASE_PATH = "s3a://your-bucket-name/delta"
  $env:EXPORT_BASE = "s3a://your-bucket-name/export"
  ```
- PySpark automatically bundles `org.apache.hadoop:hadoop-aws:3.3.4` and `com.amazonaws:aws-java-sdk-bundle:1.12.262` matching the PySpark 3.5.0 Hadoop build.
- AWS authentication uses the default `DefaultAWSCredentialsProviderChain`.
- **Note**: Delta Lake on AWS S3 without an external multi-cluster DynamoDB logstore assumes a **single writer** streaming job. Checkpoint locations remain on local filesystem or mounted EFS storage.

---

### Running the Export Job (`export_finalized.py`)

Run a single export cycle:
```bash
python -m batch.export_finalized --once
```

Run as a continuous background daemon (polling every 5 minutes):
```bash
python -m batch.export_finalized --loop --interval-seconds 300
```

The job tracks the exported high-water mark per table in `{EXPORT_BASE}/.export_state.json`. Re-running will never skip or repeat already exported window ranges.

---

### Local Warehouse Testing (No AWS Account Required)

Phase 5 includes a complete, offline local warehouse path powered by **DuckDB**:

```bash
# 1. Export finalized windows to ./export/
python -m batch.export_finalized --once

# 2. Simulate S3 Lambda trigger against local DuckDB (warehouse/local.duckdb)
python scripts/local_lambda_runner.py

# 3. Run automated verification suite
python scripts/verify_warehouse.py --target duckdb
```

---

### Automated Verification Suite (`verify_warehouse.py`)

`scripts/verify_warehouse.py` validates the entire warehouse load path:
1. **Check A: Metric Equivalence**: Compares row count, `SUM(volume)`, and `SUM(trade_count)` between finalized Delta windows and warehouse tables.
2. **Check B: Idempotency**: Re-invokes Lambda with the same manifest and asserts row counts and metric sums remain strictly unchanged.
3. **Check C: Incremental State**: Validates that `.export_state.json` tracks `last_exported_window_end` so subsequent runs export only newer windows.
4. **Check D: Audit Trail**: Verifies `load_audit` entries record `run_id`, `table_name`, `row_count`, and `status`.

---

### AWS Deployment & Teardown

#### Deploy CloudFormation Stack
```bash
# On Linux/macOS or Git Bash:
chmod +x scripts/deploy_aws.sh scripts/teardown_aws.sh
./scripts/deploy_aws.sh
```
Provisions:
- Encrypted S3 bucket with S3 `ObjectCreated` event notifications for `export/**/manifest.json`.
- Amazon Redshift Serverless namespace and workgroup with base capacity (8 RPUs).
- Least-privilege IAM roles for Redshift `COPY` and Lambda execution.
- SQS Dead-Letter Queue (DLQ) for failed Lambda retries.
- Runs `warehouse/schema.sql` to initialize tables.

#### Teardown Billable Resources
```bash
./scripts/teardown_aws.sh
```

---

### Cost and Teardown Transparency

When deploying Phase 5 infrastructure to AWS, the following resources incur costs:
- **Amazon Redshift Serverless**: Charged per RPU-hour consumed during active query and `COPY` execution (minimum 8 RPUs base capacity). Idle namespaces with no running queries do not incur compute costs.
- **AWS Lambda**: Billed per invocation and compute duration (256 MB memory allocation).
- **Amazon S3**: Billed per GB storage per month and PUT/GET request API calls.
- **Amazon SQS**: Billed per million messages (negligible for DLQ usage).

> **Cost Safety Recommendation**:
> Set up an **AWS Budget Alert** in the AWS Billing Console (e.g. $5.00 limit) to receive immediate email notifications if test workloads exceed your threshold. Always execute `./scripts/teardown_aws.sh` when testing is complete.

---

## Phase 6 — Analytics Dashboard (Streamlit)

The Phase 6 Analytics Dashboard provides an interactive web interface offering unified real-time and historical market insights, powered by an Apple-inspired dark-mode user experience.

### Architecture

```text
+-----------------------------------------------------------------------------------+
|                           STREAMLIT ANALYTICS DASHBOARD                           |
|                                (dashboard/app.py)                                 |
+--------------------------+------------------------------+-------------------------+
|        LIVE VIEW         |       HISTORICAL VIEW        |     PIPELINE HEALTH     |
| (5s Auto-Refresh / Delta)| (Batch Staged / Warehouse)   |  (Observability & Lag)  |
+--------------------------+------------------------------+-------------------------+
             |                            |                            |
   (delta-rs, no Spark)           (Parameterized SQL)           (metrics.log / CW)
             v                            v                            v
   +-------------------+        +--------------------+       +-------------------+
   | Delta Lake Tables |        |  Data Warehouse    |       | Pipeline Metrics  |
   | - agg_5min        |        |  - DuckDB (Local)  |       | - Events/sec      |
   | - agg_1hour       |        |  - Amazon Redshift |       | - Consumer Lag    |
   | - raw_trades      |        |    (Serverless)    |       | - Batch Duration  |
   +-------------------+        +--------------------+       +-------------------+
```

- **Live View**: Reads Delta tables directly using `deltalake` (delta-rs in native Rust; no Spark session required in the dashboard). Auto-refreshes seamlessly every 5 seconds using `@st.fragment(run_every=5)` and `@st.cache_data(ttl=5)`. Queries against `raw_trades` strictly prune by `trade_date` partition to prevent full-table scans.
- **Historical View**: Queries the data warehouse (`warehouse/local.duckdb` or Amazon Redshift) using strictly parameterized queries (preventing SQL injection) with date-range filters, sector filters, multi-ticker selection, and CSV export.
- **Pipeline Health**: Continuously checks stream latency, consumer lag, and watermark freshness (`now - max(window_end)` in `agg_5min`). Displays a prominent red **"PIPELINE STALLED"** alert banner if no new finalized windows arrive for > 2 minutes (120s).

### Screenshot

![Phase 6 Real-Time & Historical Stock Market Analytics Dashboard](docs/images/dashboard_screenshot.png)
*(Run `streamlit run dashboard/app.py` to view live in browser)*

### How to Run

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Launch the dashboard**:
   ```bash
   streamlit run dashboard/app.py
   ```
   Open your browser at `http://localhost:8501`.

### Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DELTA_BASE_PATH` | `./delta` | Path or S3 URI (`s3://...` / `s3a://...`) to Delta Lake tables. |
| `WAREHOUSE_TARGET` | `duckdb` | Warehouse target: `duckdb` (offline local) or `redshift` (AWS). |
| `DUCKDB_PATH` | `./warehouse/local.duckdb` | Path to local DuckDB database file for offline testing. |
| `METRICS_LOG_PATH` | `monitoring/metrics.log` | Path to streaming metrics log file. |
| `METRICS_SINK` | `file` | Metrics sink: `file`, `cloudwatch`, or `both`. |
| `AWS_REGION` | `us-east-1` | AWS region for Redshift Data API and CloudWatch. |
| `REDSHIFT_DATABASE` | `dev` | Amazon Redshift database name. |
| `REDSHIFT_WORKGROUP` | `stock-workgroup` | Redshift Serverless workgroup name. |


