# Real-Time Stock Market Streaming Platform (Phase 2)

This repository implements a real-time stock market streaming pipeline:
End-to-end event flow from an Apache Kafka producer running natively in KRaft mode to a PySpark Structured Streaming job with watermarking, windowed OHLCV aggregations, and company metadata enrichment.

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
|     - Explicit JSON schema parsing                                   |
|     - Event time timestamp cast (UTC)                                |
|                                                                      |
|  2. streaming/aggregations.py                                        |
|     - withWatermark("timestamp", "10 minutes")                       |
|     - Windowed OHLCV: 5 minutes and 1 hour                           |
|     - Deterministic open = min_by(price, timestamp)                  |
|     - Deterministic close = max_by(price, timestamp)                 |
|     - high = max(price), low = min(price)                            |
|     - volume = sum(volume), trade_count = count(*)                   |
|     - Window flattened to window_start, window_end                   |
|                                                                      |
|  3. streaming/enrichment.py                                          |
|     - Loads data/company_metadata.csv                                |
|     - Broadcast join on ticker applied post-aggregation              |
|     - Adds company_name, sector, exchange                           |
|                                                                      |
|  4. Output Sinks (Console)                                           |
|     - Query 1: raw_trades (append mode, 5s trigger)                  |
|     - Query 2: agg_5min   (update mode, 5s trigger)                  |
|     - Query 3: agg_1hour  (update mode, 5s trigger)                  |
+----------------------------------------------------------------------+
```

---

## Directory Structure

```text
├── data/
│   └── company_metadata.csv          # 20 NASDAQ tickers metadata (ticker, company_name, sector, exchange)
├── hadoop/bin/                       # Windows native Hadoop binaries (winutils.exe, hadoop.dll)
├── producer/
│   └── producer.py                   # Trade event simulator with --rate and --late-event-minutes
├── scripts/                          # Kafka automation scripts (setup, start, stop, topic creation)
├── streaming/
│   ├── __init__.py                   # Package marker
│   ├── source.py                     # Kafka consumer & JSON parser (stateless)
│   ├── aggregations.py               # 10-minute watermark & OHLCV window builder
│   ├── enrichment.py                 # Static metadata loader & broadcast join
│   └── stream_job.py                 # Main streaming orchestrator (3 active queries)
├── tests/
│   └── test_phase2.py                # Unit & integration tests for Phase 2 components
└── requirements.txt                  # Python dependencies (pyspark, kafka-python-ng)
```

---

## Prerequisites

1. **Java 11 or 17**: Required by both Apache Kafka and PySpark (`java -version` should work).
2. **Python 3.10+**: With pip.

---

## Setup & Run Instructions

### 0. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

### Step 1: Run `setup_kafka` (Once)

Downloads Apache Kafka into `kafka/` and formats storage for KRaft mode:

- **Windows (CMD / PowerShell)**:
  ```cmd
  scripts\setup_kafka.bat
  ```
- **Linux / macOS**:
  ```bash
  chmod +x scripts/*.sh
  ./scripts/setup_kafka.sh
  ```

---

### Step 2: Start Kafka (Leave Running in Terminal 1)

- **Windows (CMD / PowerShell)**:
  ```cmd
  scripts\start_kafka.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/start_kafka.sh
  ```

Kafka will run natively in KRaft mode on `localhost:9092`.

---

### Step 3: Create & Verify Topic (Terminal 2 - Once)

Create the `stock-trades` topic with 12 partitions:

- **Windows**:
  ```cmd
  scripts\create_topic.bat
  scripts\verify_topic.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/create_topic.sh
  ./scripts/verify_topic.sh
  ```

---

### Step 4: Run the PySpark Structured Streaming Job (Terminal 2)

Start the streaming job:

```bash
python streaming/stream_job.py
```

The streaming job starts three concurrent queries:
1. `raw_trades`: Appends parsed trade rows as they arrive from Kafka.
2. `agg_5min`: 5-minute OHLCV window aggregations, updated in place with metadata enrichment.
3. `agg_1hour`: 1-hour OHLCV window aggregations, updated in place with metadata enrichment.

> **Note on Output Modes**:
> - The raw query uses `outputMode("append")`.
> - The aggregation queries use `outputMode("update")` with the console sink so partial window results are immediately visible while the window is still open.
> - In append mode, a windowed aggregation query only emits after the window end plus the 10-minute watermark has elapsed (`window_end + 10m`). Phase 3 will handle finalized writes via `foreachBatch`.

---

### Step 5: Run the Producer (Terminal 3)

Generate simulated real-time trades at 100 events/second:

```bash
python producer/producer.py --rate 100
```

---

## Phase 2 Verification

Follow these steps to verify watermarking, OHLCV calculations, enrichment, and late-event handling:

### Step-by-Step Verification Procedure

1. **Start Kafka and the topic**:
   Ensure Kafka is running on `localhost:9092` and the `stock-trades` topic is created.

2. **Launch the Spark streaming job**:
   ```bash
   python streaming/stream_job.py
   ```

3. **Advance the watermark**:
   Start the trade producer:
   ```bash
   python producer/producer.py --rate 100
   ```
   Let the producer run for **at least 2 minutes**. This advances the event-time watermark in Spark past earlier windows (`watermark = max(event_time) - 10 minutes`).

4. **Verify Windowed Aggregations & Metadata Enrichment**:
   Examine the console tables for `agg_5min` and `agg_1hour`:
   - Output columns are ordered sensibly:
     `ticker`, `company_name`, `sector`, `exchange`, `window_start`, `window_end`, `open`, `high`, `low`, `close`, `volume`, `trade_count`.
   - Company metadata (`company_name`, `sector`, `exchange`) is populated correctly via broadcast join.
   - For every window row, OHLCV mathematical consistency holds:
     - `low <= open <= high`
     - `low <= close <= high`
     - `volume` equals the sum of raw trade volumes for that ticker in that window.
     - `open` and `close` are deterministic (calculated using `min_by` and `max_by` on event timestamps).

5. **Send a 30-Minutes Late Event (Older than Watermark)**:
   In another terminal, send a trade that is 30 minutes late:
   ```bash
   python producer/producer.py --late-event-minutes 30
   ```
   The producer logs:
   ```text
   *** LATE EVENT SENT *** | event_id=<uuid> | ticker=<TICKER> | timestamp=<T-30m> | late_by_minutes=30.0
   ```

   **Expected Result**:
   - The late event **appears** in the `raw_trades` query output (watermark does not affect stateless queries).
   - The late event **does NOT appear** in, nor change any existing row of, the `agg_5min` or `agg_1hour` queries (its event timestamp is older than the 10-minute watermark threshold, so Spark Structured Streaming automatically drops it from state).

6. **Send a 3-Minutes Late Event (Inside Watermark)**:
   Send a trade that is mildly late (3 minutes in the past):
   ```bash
   python producer/producer.py --late-event-minutes 3
   ```

   **Expected Result**:
   - The mildly late event **appears** in the `raw_trades` query output.
   - The mildly late event **SHOULD still update** the corresponding current or recent 5-minute (and 1-hour) window aggregate row, because its timestamp falls within the 10-minute watermark window (`now - 3m > now - 10m`).

---

## Running Automated Tests

Run the test suite to verify schema, windowing, OHLCV calculations, broadcast joins, and late event generation:

```bash
python tests/test_phase2.py
```

---

## Stopping the Pipeline Gracefully

1. In the producer terminal: press `Ctrl+C`.
2. In the streaming terminal: press `Ctrl+C`.
3. To stop Kafka:
   - Either press `Ctrl+C` in Terminal 1,
   - Or run:
     - Windows: `scripts\stop_kafka.bat`
     - Linux / macOS: `./scripts/stop_kafka.sh`
