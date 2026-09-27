# Real-Time Stock Market Streaming Platform (Phase 1)

This repository implements **Phase 1** of a real-time stock streaming pipeline:
End-to-end event flow from an Apache Kafka producer running natively in KRaft mode to a PySpark Structured Streaming job.

---

## Architecture Overview

```
+------------------------------------+
|  Fake NASDAQ Metadata              |
|  (data/company_metadata.csv)       |
+-----------------+------------------+
                  |
                  v
+-----------------+------------------+
|  Kafka Producer (producer.py)      |
|  - 20 tickers with random walk     |
|  - Rate control (--rate 100..10k)  |
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
+-----------------+------------------+
|  PySpark Structured Streaming      |
|  (streaming/stream_job.py)         |
|  - Explicit JSON schema parsing    |
|  - 5-second processing trigger     |
|  - Console sink in append mode     |
+------------------------------------+
```

---

## Prerequisites

1. **Java 11 or 17**: Required by both Apache Kafka and PySpark (`java -version` should work).
2. **Python 3.10+**: With pip.

---

## Setup & Run Instructions

### 0. Install Python Dependencies

In your terminal or virtual environment:

```bash
pip install -r requirements.txt
```

---

### Step 1: Run `setup_kafka` (Once)

Downloads Apache Kafka (3.8.0, Scala 2.13 build) into the local `kafka/` directory and formats storage for KRaft mode using a generated cluster ID (skips if already formatted).

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

> **Important**: Kafka must stay running in its own terminal window for the entire duration of your session.

- **Windows (CMD / PowerShell)**:
  ```cmd
  scripts\start_kafka.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/start_kafka.sh
  ```

Kafka will start in KRaft mode in the foreground listening on `localhost:9092`.

---

### Step 3: Create the `stock-trades` Topic (Terminal 2 - Once)

Open a **new terminal** window and create the topic with 12 partitions and replication factor 1:

- **Windows (CMD / PowerShell)**:
  ```cmd
  scripts\create_topic.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/create_topic.sh
  ```

---

### Step 4: Verify the Topic Exists

Verify that the topic has been initialized with all 12 partitions:

- **Windows (CMD / PowerShell)**:
  ```cmd
  scripts\verify_topic.bat
  ```
- **Linux / macOS**:
  ```bash
  ./scripts/verify_topic.sh
  ```

You will see output confirming 12 partitions (`Partition: 0` through `Partition: 11`):
```text
Topic: stock-trades	TopicId: ...	PartitionCount: 12	ReplicationFactor: 1	Configs: ...
```

---

### Step 5: Run the Producer (Terminal 2)

In the same terminal (or another new terminal), start the simulated trade producer:

```bash
python producer/producer.py --rate 100
```

- Target events per second can be configured using `--rate` (default: `100`, tested up to `10000`).
- Every 5 seconds, the producer logs the actual throughput:
  ```text
  2026-09-27 10:30:05 [INFO] Throughput: 100.12 msg/sec | Total produced: 501 | Target rate: 100 msg/sec
  ```

---

### Step 6: Run the PySpark Structured Streaming Job (Terminal 3)

Open another terminal and start the streaming job:

```bash
python streaming/stream_job.py
```

*(Alternatively, if using an existing Spark distribution)*:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming/stream_job.py
```

---

## Success Criteria Verification

Within a few seconds of launching the streaming job while the producer is running, parsed trade rows will appear in the console output in tabular batches:

```text
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------+------+------+------+-----------------------+
|event_id                            |ticker|price |volume|timestamp              |
+------------------------------------+------+------+------+-----------------------+
|e5e954c2-9e28-4447-b2e1-456673f4e246|AAPL  |182.45|1420  |2026-09-27 05:00:01.123|
|74a62174-a0dc-4cfb-81d2-0852899dc631|NVDA  |455.10|3890  |2026-09-27 05:00:01.128|
|c7112028-2fbf-4043-85f2-901b0f512705|MSFT  |330.12|850   |2026-09-27 05:00:01.135|
+------------------------------------+------+------+------+-----------------------+
```

- **No schema errors**: All fields (`event_id`, `ticker`, `price`, `volume`, `timestamp`) are correctly populated with non-null values.
- **Micro-batches**: Rows refresh every 5 seconds per the `trigger(processingTime="5 seconds")` configuration.

---

## Stopping the Pipeline Gracefully

1. In the producer terminal: press `Ctrl+C`.
2. In the streaming terminal: press `Ctrl+C`.
3. To stop Kafka:
   - Either press `Ctrl+C` in Terminal 1 (where `start_kafka` is running),
   - Or run the graceful stop script from another terminal:
     - Windows: `scripts\stop_kafka.bat`
     - Linux / macOS: `./scripts/stop_kafka.sh`
