# Project Rules (All Phases)

## Architecture & Data Flow
- **End-to-end Pipeline Flow**: `producer -> Kafka -> Spark Structured Streaming -> Delta Lake -> export -> AWS Lambda -> Amazon Redshift -> Streamlit dashboard`.
- **Metrics**: Spark pipeline metrics go to CloudWatch (or local file sink).
- **Kafka Environment**: Kafka runs natively on `localhost:9092` (no Docker). **Do NOT reintroduce Docker**.

## Tech Stack & Standards
- Python 3.10+, PySpark 3.5.x.
- Type hints and short docstrings on every function.
- Small, single-purpose, modular files.
- Pin every new dependency added to `requirements.txt`.

## Configuration & Environment
- **Centralized Configuration**: All settings come from environment variables via a shared `config.py` module with local-friendly defaults, plus `.env.example`.
- **Zero Hardcoded Secrets**: Never hardcode credentials, AWS keys, or account IDs. AWS access must use the default `boto3` credential chain.
- **Dual Execution (LOCAL vs AWS)**: Every phase must be switchable between `LOCAL` and `AWS` through configuration:
  - `DELTA_BASE_PATH`: Local directory path (e.g. `./data/delta`) or S3 path (`s3a://...`).
  - `METRICS_SINK`: `file` | `cloudwatch` | `both`.
  - `WAREHOUSE_TARGET`: `duckdb` | `redshift`.

## Phase Progression & Verification
- Do not break earlier phases. Do not refactor files a phase does not touch.
- Every phase must conclude with a dedicated `"Phase N verification"` section in `README.md` containing exact commands, step-by-step procedures, and expected results.
