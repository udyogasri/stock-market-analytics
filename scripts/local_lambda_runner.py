#!/usr/bin/env python3
"""
Local runner simulating S3 ObjectCreated events triggering the load_to_redshift Lambda handler.
Operates with WAREHOUSE_TARGET=duckdb and EXPORT_BASE=./export so the entire warehouse load path
can be verified locally without requiring an active AWS account.
"""

import argparse
import json
import os
from pathlib import Path
import sys
from typing import List

# Ensure repo root is in sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import DUCKDB_PATH, EXPORT_BASE

import importlib
load_to_redshift = importlib.import_module("lambda.load_to_redshift.handler")
lambda_handler = load_to_redshift.lambda_handler




def run_local_lambda(
    manifest_path: str = None,
    export_base: str = EXPORT_BASE,
    duckdb_path: str = DUCKDB_PATH,
) -> List[dict]:
    """Find local manifest(s) and simulate Lambda invocation."""
    os.environ["WAREHOUSE_TARGET"] = "duckdb"
    os.environ["DUCKDB_PATH"] = str(Path(duckdb_path).resolve())
    os.environ["EXPORT_BASE"] = export_base

    manifest_files: List[Path] = []

    if manifest_path:
        mp = Path(manifest_path).resolve()
        if not mp.is_file():
            print(f"[ERROR] Specified manifest file not found: {mp}")
            sys.exit(1)
        manifest_files.append(mp)
    else:
        exp_dir = Path(export_base).resolve()
        if not exp_dir.is_dir():
            print(f"[WARNING] Export base directory does not exist: {exp_dir}")
            return []
        manifest_files = sorted(exp_dir.glob("**/manifest.json"))

    if not manifest_files:
        print(f"[INFO] No manifest.json files found in {export_base}. Run export_finalized.py first.")
        return []

    print("=" * 80)
    print("Local Lambda Execution Runner (WAREHOUSE_TARGET=duckdb)")
    print(f"Export Base Directory : {export_base}")
    print(f"Target DuckDB Path    : {duckdb_path}")
    print(f"Manifests to process  : {len(manifest_files)}")
    print("=" * 80)

    results = []

    for idx, mf in enumerate(manifest_files, 1):
        print(f"\n[{idx}/{len(manifest_files)}] Simulating S3 ObjectCreated event for: {mf}")

        # Build simulated S3 event payload with manifest_path fallback
        event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "bucket": {"name": "local-stock-pipeline-bucket"},
                        "object": {"key": mf.as_posix()},
                    },
                }
            ],
            "manifest_path": str(mf.resolve()),
        }

        try:
            resp = lambda_handler(event, context=None)
            status_code = resp.get("statusCode", 500)
            body = json.loads(resp.get("body", "{}"))

            if status_code == 200 and body.get("status") == "SUCCESS":
                print(f"   [SUCCESS] Loaded into {body.get('table')}: {body.get('rows_loaded')} rows (run_id={body.get('run_id')})")
                results.append(body)
            else:
                print(f"   [FAILURE] Lambda returned: {resp}")
        except Exception as e:
            print(f"   [ERROR] Lambda execution raised exception: {e}")
            raise

    print("\n" + "=" * 80)
    print(f"Local Lambda Runner Completed: {len(results)}/{len(manifest_files)} manifests successfully ingested.")
    print("=" * 80)
    return results


def main():
    parser = argparse.ArgumentParser(description="Simulate S3 ObjectCreated trigger on Lambda with DuckDB target.")
    parser.add_argument("--manifest-path", type=str, default=None, help="Path to specific manifest.json")
    parser.add_argument("--export-base", type=str, default=EXPORT_BASE, help=f"Export base directory (default: {EXPORT_BASE})")
    parser.add_argument("--duckdb-path", type=str, default=DUCKDB_PATH, help=f"DuckDB path (default: {DUCKDB_PATH})")
    args = parser.parse_args()

    run_local_lambda(
        manifest_path=args.manifest_path,
        export_base=args.export_base,
        duckdb_path=args.duckdb_path,
    )


if __name__ == "__main__":
    main()
