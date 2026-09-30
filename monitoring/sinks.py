"""
Metrics sinks for PySpark Structured Streaming pipeline observability.
Provides FileMetricsSink (rotating JSON lines) and CloudWatchMetricsSink
(asynchronous, non-blocking background queue with retry/drop safety).
"""

import abc
from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import queue
import threading
from typing import Any, Dict, List, Optional

from config import AWS_REGION, CLOUDWATCH_NAMESPACE, METRICS_LOG_PATH, METRICS_SINK

logger = logging.getLogger("PipelineMetrics")


class BaseMetricsSink(abc.ABC):
    """Abstract base class for streaming pipeline metrics sinks."""

    @abc.abstractmethod
    def record_progress(self, metrics: Dict[str, Any]) -> None:
        """Record a single streaming query progress snapshot."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Flush and release resources."""
        pass


class FileMetricsSink(BaseMetricsSink):
    """
    Writes metrics as newline-delimited JSON objects with size-based log rotation.
    Thread-safe and local-filesystem friendly.
    """

    def __init__(
        self,
        log_path: str = METRICS_LOG_PATH,
        max_bytes: int = 10 * 1024 * 1024,  # 10 MB per log file
        backup_count: int = 5,
    ) -> None:
        self.log_path = Path(log_path).resolve()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger(f"FileMetricsSink_{id(self)}")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        # Configure size-based rotating file handler
        self._handler = RotatingFileHandler(
            filename=str(self.log_path),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._handler.setFormatter(logging.Formatter("%(message)s"))
        self._logger.addHandler(self._handler)

    def record_progress(self, metrics: Dict[str, Any]) -> None:
        """Write single JSON line to rotating log."""
        try:
            line = json.dumps(metrics, default=str)
            self._logger.info(line)
            self._handler.flush()
        except Exception as e:
            logger.warning(f"FileMetricsSink error writing metrics: {e}")

    def close(self) -> None:
        """Close log handler."""
        try:
            self._handler.close()
            self._logger.removeHandler(self._handler)
        except Exception:
            pass


class CloudWatchMetricsSink(BaseMetricsSink):
    """
    Asynchronously publishes streaming query metrics to AWS CloudWatch.
    Uses an in-memory bounded queue and daemon worker thread so AWS calls
    never block or crash the Spark streaming execution.
    """

    def __init__(
        self,
        namespace: str = CLOUDWATCH_NAMESPACE,
        region_name: str = AWS_REGION,
        queue_size: int = 2000,
        max_batch_size: int = 20,
    ) -> None:
        self.namespace = namespace
        self.region_name = region_name
        self.max_batch_size = min(max_batch_size, 20)  # CloudWatch conservative chunking
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()

        self._cw_client = None
        self._init_client_attempted = False

        # Start background worker thread
        self._worker = threading.Thread(
            target=self._process_queue,
            name="CloudWatchMetricsWorker",
            daemon=True,
        )
        self._worker.start()

    def _get_client(self):
        """Lazily initialize boto3 CloudWatch client using default credential chain."""
        if self._cw_client is None and not self._init_client_attempted:
            self._init_client_attempted = True
            try:
                import boto3
                self._cw_client = boto3.client("cloudwatch", region_name=self.region_name)
            except Exception as e:
                logger.warning(f"Could not initialize CloudWatch client: {e}. Metrics will be dropped.")
        return self._cw_client

    def record_progress(self, metrics: Dict[str, Any]) -> None:
        """Enqueue metric data for asynchronous publication."""
        query_name = metrics.get("query_name", "unknown")
        raw_ts = metrics.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                # Parse ISO timestamp or fallback to current UTC time
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            except Exception:
                dt = datetime.now(timezone.utc)
        elif isinstance(raw_ts, datetime):
            dt = raw_ts
        else:
            dt = datetime.now(timezone.utc)

        # Build CloudWatch metric datums with strict standard units
        data: List[Dict[str, Any]] = [
            {
                "MetricName": "InputRowsPerSecond",
                "Dimensions": [{"Name": "QueryName", "Value": str(query_name)}],
                "Timestamp": dt,
                "Value": float(metrics.get("input_rows_per_second", 0.0)),
                "Unit": "Count/Second",
            },
            {
                "MetricName": "ProcessedRowsPerSecond",
                "Dimensions": [{"Name": "QueryName", "Value": str(query_name)}],
                "Timestamp": dt,
                "Value": float(metrics.get("processed_rows_per_second", 0.0)),
                "Unit": "Count/Second",
            },
            {
                "MetricName": "BatchDurationMs",
                "Dimensions": [{"Name": "QueryName", "Value": str(query_name)}],
                "Timestamp": dt,
                "Value": float(metrics.get("batch_duration_ms", 0.0)),
                "Unit": "Milliseconds",
            },
            {
                "MetricName": "KafkaOffsetsBehindLatest",
                "Dimensions": [{"Name": "QueryName", "Value": str(query_name)}],
                "Timestamp": dt,
                "Value": float(metrics.get("kafka_consumer_lag", 0)),
                "Unit": "Count",
            },
            {
                "MetricName": "StateRows",
                "Dimensions": [{"Name": "QueryName", "Value": str(query_name)}],
                "Timestamp": dt,
                "Value": float(metrics.get("state_rows", 0)),
                "Unit": "Count",
            },
        ]

        try:
            self._queue.put_nowait(data)
        except queue.Full:
            logger.warning("CloudWatch metrics queue is full; dropping metrics snapshot.")

    def _process_queue(self) -> None:
        """Worker loop draining the queue and issuing batched put_metric_data calls."""
        batch: List[Dict[str, Any]] = []

        while not self._stop_event.is_set():
            try:
                # Wait for next item with short timeout
                try:
                    items = self._queue.get(timeout=1.0)
                    batch.extend(items)
                    self._queue.task_done()
                except queue.Empty:
                    pass

                # Drain additional available items up to chunk limit
                while not self._queue.empty() and len(batch) < self.max_batch_size:
                    try:
                        more_items = self._queue.get_nowait()
                        batch.extend(more_items)
                        self._queue.task_done()
                    except queue.Empty:
                        break

                if batch:
                    # Conservative chunking (<= 20 items per put_metric_data call)
                    while batch:
                        chunk = batch[: self.max_batch_size]
                        batch = batch[self.max_batch_size :]
                        self._send_chunk(chunk)

            except Exception as e:
                logger.warning(f"Unexpected error in CloudWatch metrics worker: {e}")

    def _send_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        """Send a single chunk of metric datums. Logs warning on failure and never raises."""
        client = self._get_client()
        if client is None:
            return

        try:
            client.put_metric_data(
                Namespace=self.namespace,
                MetricData=chunk,
            )
        except Exception as e:
            logger.warning(f"Failed to publish metrics to CloudWatch: {e}. Dropping {len(chunk)} metrics.")

    def close(self) -> None:
        """Signal worker thread to finish remaining items and terminate."""
        self._stop_event.set()
        if self._worker.is_alive():
            self._worker.join(timeout=2.0)


class CompositeMetricsSink(BaseMetricsSink):
    """Dispatches metrics to multiple underlying sinks simultaneously."""

    def __init__(self, sinks: List[BaseMetricsSink]) -> None:
        self.sinks = sinks

    def record_progress(self, metrics: Dict[str, Any]) -> None:
        for sink in self.sinks:
            try:
                sink.record_progress(metrics)
            except Exception as e:
                logger.warning(f"Error in sink {type(sink).__name__}: {e}")

    def close(self) -> None:
        for sink in self.sinks:
            try:
                sink.close()
            except Exception:
                pass


def get_metrics_sink(sink_type: Optional[str] = None) -> BaseMetricsSink:
    """
    Factory creating configured metrics sink based on METRICS_SINK environment variable:
    - 'file': FileMetricsSink writing to monitoring/metrics.log
    - 'cloudwatch': CloudWatchMetricsSink publishing to Amazon CloudWatch
    - 'both': CompositeMetricsSink sending to both file and CloudWatch
    """
    choice = (sink_type or METRICS_SINK).lower().strip()

    if choice == "cloudwatch":
        return CloudWatchMetricsSink()
    elif choice == "both":
        return CompositeMetricsSink([FileMetricsSink(), CloudWatchMetricsSink()])
    else:
        # Default is file
        return FileMetricsSink()
