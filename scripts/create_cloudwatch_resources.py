#!/usr/bin/env python3
"""
Create CloudWatch alarm and dashboard for PySpark streaming pipeline monitoring.
- Alarm: Fires when KafkaOffsetsBehindLatest > threshold (default 50,000) for 3 consecutive 1-minute periods.
- Dashboard: Displays Throughput, Kafka Consumer Lag, Batch Duration, and Stateful Rows.
Idempotent: safe to run multiple times to update configurations.
"""

import argparse
import json
import os
from pathlib import Path
import sys

# Path bootstrap to allow running from repo root or scripts dir
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from config import AWS_REGION, CLOUDWATCH_NAMESPACE


def create_resources(
    threshold: int = 50000,
    namespace: str = CLOUDWATCH_NAMESPACE,
    region: str = AWS_REGION,
    alarm_name: str = "StockPipeline-KafkaConsumerLag-High",
    dashboard_name: str = "StockPipeline-Dashboard",
    query_name: str = "raw_trades",
) -> None:
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, PartialCredentialsError
    except ImportError:
        print("ERROR: boto3 is required to manage AWS resources. Run: pip install boto3")
        sys.exit(1)

    print("=" * 80)
    print("Provisioning Amazon CloudWatch Observability Resources")
    print(f"AWS Region      : {region}")
    print(f"Namespace       : {namespace}")
    print(f"Lag Alarm Name  : {alarm_name} (Threshold: > {threshold:,} offsets)")
    print(f"Dashboard Name  : {dashboard_name}")
    print("=" * 80)

    try:
        cw = boto3.client("cloudwatch", region_name=region)

        # 1. Create or update CloudWatch Metric Alarm
        print(f"\n1. Creating/updating CloudWatch Alarm: '{alarm_name}'...")
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Alarm when {query_name} Kafka consumer lag exceeds {threshold:,} offsets for 3 consecutive minutes.",
            ActionsEnabled=False,
            MetricName="KafkaOffsetsBehindLatest",
            Namespace=namespace,
            Statistic="Maximum",
            Dimensions=[{"Name": "QueryName", "Value": query_name}],
            Period=60,  # 1-minute period
            EvaluationPeriods=3,  # 3 consecutive periods
            DatapointsToAlarm=3,
            Threshold=float(threshold),
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
            Unit="Count",
        )
        print(f"   [SUCCESS] Alarm '{alarm_name}' configured successfully.")

        # 2. Build CloudWatch Dashboard Body JSON
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [namespace, "InputRowsPerSecond", "QueryName", "raw_trades", {"label": "raw_trades Input Rate"}],
                            [namespace, "ProcessedRowsPerSecond", "QueryName", "raw_trades", {"label": "raw_trades Processed Rate"}],
                            [namespace, "InputRowsPerSecond", "QueryName", "agg_5min", {"label": "agg_5min Input Rate"}],
                            [namespace, "ProcessedRowsPerSecond", "QueryName", "agg_5min", {"label": "agg_5min Processed Rate"}],
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": region,
                        "title": "Query Throughput (rows/sec)",
                        "period": 60,
                        "stat": "Average",
                    },
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [namespace, "KafkaOffsetsBehindLatest", "QueryName", "raw_trades", {"label": "raw_trades Consumer Lag", "color": "#ff7f0e"}],
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": region,
                        "title": "Kafka Consumer Lag (offsets behind latest)",
                        "period": 60,
                        "stat": "Maximum",
                        "annotations": {
                            "horizontal": [
                                {
                                    "value": float(threshold),
                                    "label": f"Lag Alarm Threshold ({threshold:,})",
                                    "color": "#d62728",
                                    "fill": "above",
                                }
                            ]
                        },
                    },
                },
                {
                    "type": "metric",
                    "x": 0,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [namespace, "BatchDurationMs", "QueryName", "raw_trades", {"label": "raw_trades Batch Duration (ms)"}],
                            [namespace, "BatchDurationMs", "QueryName", "agg_5min", {"label": "agg_5min Batch Duration (ms)"}],
                            [namespace, "BatchDurationMs", "QueryName", "agg_1hour", {"label": "agg_1hour Batch Duration (ms)"}],
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": region,
                        "title": "Micro-Batch Execution Duration (ms)",
                        "period": 60,
                        "stat": "Average",
                        "annotations": {
                            "horizontal": [
                                {
                                    "value": 5000.0,
                                    "label": "Trigger Interval Target (5,000 ms)",
                                    "color": "#e377c2",
                                }
                            ]
                        },
                    },
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            [namespace, "StateRows", "QueryName", "agg_5min", {"label": "agg_5min Active State Rows"}],
                            [namespace, "StateRows", "QueryName", "agg_1hour", {"label": "agg_1hour Active State Rows"}],
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": region,
                        "title": "Watermarked Stateful Aggregation State Rows",
                        "period": 60,
                        "stat": "Maximum",
                    },
                },
            ]
        }

        # 3. Create or update CloudWatch Dashboard
        print(f"\n2. Creating/updating CloudWatch Dashboard: '{dashboard_name}'...")
        cw.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body),
        )
        print(f"   [SUCCESS] Dashboard '{dashboard_name}' configured successfully.")

        dashboard_url = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#dashboards:name={dashboard_name}"
        print("\nAll CloudWatch resources are provisioned.")
        print(f"Dashboard URL: {dashboard_url}")

    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"\n[ERROR] AWS credentials not found or incomplete: {e}")
        print("Please configure AWS CLI credentials via 'aws configure' or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Failed to provision CloudWatch resources: {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create CloudWatch alarm and dashboard for Spark pipeline monitoring.")
    parser.add_argument("--threshold", type=int, default=50000, help="Consumer lag alarm threshold in offsets (default: 50000)")
    parser.add_argument("--region", type=str, default=AWS_REGION, help=f"AWS Region (default: {AWS_REGION})")
    parser.add_argument("--namespace", type=str, default=CLOUDWATCH_NAMESPACE, help=f"CloudWatch metric namespace (default: {CLOUDWATCH_NAMESPACE})")
    parser.add_argument("--alarm-name", type=str, default="StockPipeline-KafkaConsumerLag-High", help="Alarm name")
    parser.add_argument("--dashboard-name", type=str, default="StockPipeline-Dashboard", help="Dashboard name")
    args = parser.parse_args()

    create_resources(
        threshold=args.threshold,
        namespace=args.namespace,
        region=args.region,
        alarm_name=args.alarm_name,
        dashboard_name=args.dashboard_name,
    )


if __name__ == "__main__":
    main()
