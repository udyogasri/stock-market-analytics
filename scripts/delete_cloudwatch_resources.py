#!/usr/bin/env python3
"""
Tear down CloudWatch alarm and dashboard created for the streaming pipeline.
Idempotent: safe to run even if resources do not exist.
"""

import argparse
from pathlib import Path
import sys

# Path bootstrap to allow running from repo root or scripts dir
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import AWS_REGION


def delete_resources(
    region: str = AWS_REGION,
    alarm_name: str = "StockPipeline-KafkaConsumerLag-High",
    dashboard_name: str = "StockPipeline-Dashboard",
) -> None:
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, PartialCredentialsError
    except ImportError:
        print("ERROR: boto3 is required to manage AWS resources. Run: pip install boto3")
        sys.exit(1)

    print("=" * 80)
    print("Deleting Amazon CloudWatch Observability Resources")
    print(f"AWS Region      : {region}")
    print(f"Lag Alarm Name  : {alarm_name}")
    print(f"Dashboard Name  : {dashboard_name}")
    print("=" * 80)

    try:
        cw = boto3.client("cloudwatch", region_name=region)

        # 1. Delete Alarm
        print(f"\n1. Deleting CloudWatch Alarm: '{alarm_name}'...")
        try:
            cw.delete_alarms(AlarmNames=[alarm_name])
            print(f"   [SUCCESS] Deleted alarm '{alarm_name}'.")
        except Exception as e:
            print(f"   [WARNING] Could not delete alarm '{alarm_name}': {e}")

        # 2. Delete Dashboard
        print(f"\n2. Deleting CloudWatch Dashboard: '{dashboard_name}'...")
        try:
            cw.delete_dashboards(DashboardNames=[dashboard_name])
            print(f"   [SUCCESS] Deleted dashboard '{dashboard_name}'.")
        except Exception as e:
            print(f"   [WARNING] Could not delete dashboard '{dashboard_name}': {e}")

        print("\nCleanup completed.")

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"\n[ERROR] AWS credentials not found or incomplete: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Failed to delete CloudWatch resources: {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete CloudWatch alarm and dashboard.")
    parser.add_argument("--region", type=str, default=AWS_REGION, help=f"AWS Region (default: {AWS_REGION})")
    parser.add_argument("--alarm-name", type=str, default="StockPipeline-KafkaConsumerLag-High", help="Alarm name")
    parser.add_argument("--dashboard-name", type=str, default="StockPipeline-Dashboard", help="Dashboard name")
    args = parser.parse_args()

    delete_resources(
        region=args.region,
        alarm_name=args.alarm_name,
        dashboard_name=args.dashboard_name,
    )


if __name__ == "__main__":
    main()
