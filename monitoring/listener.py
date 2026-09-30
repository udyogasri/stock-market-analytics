"""
StreamingQueryListener implementation for PySpark Structured Streaming.
Extracts per-query throughput, processing duration, consumer lag, watermark,
and state store metrics, forwarding snapshots to configured metrics sinks.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional

from pyspark.sql.streaming import StreamingQueryListener

from monitoring.sinks import BaseMetricsSink, get_metrics_sink

logger = logging.getLogger("PipelineMetricsListener")


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Helper to safely extract field whether obj is an object or dictionary."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        val = obj.get(key)
        return val if val is not None else default
    if hasattr(obj, key):
        val = getattr(obj, key)
        return val if val is not None else default
    return default


class PipelineMetricsListener(StreamingQueryListener):
    """
    Observability listener registered on SparkSession.streams.
    Captures onQueryProgress events, extracts health and performance metrics,
    and forwards them to the configured BaseMetricsSink.
    """

    def __init__(
        self,
        sink: Optional[BaseMetricsSink] = None,
        trigger_interval_ms: float = 5000.0,
    ) -> None:
        super().__init__()
        self.sink: BaseMetricsSink = sink or get_metrics_sink()
        self.trigger_interval_ms: float = trigger_interval_ms

    def onQueryStarted(self, event: Any) -> None:
        """Called when a streaming query is started."""
        query_name = _safe_get(event, "name", "unknown")
        query_id = _safe_get(event, "id", "")
        logger.info(f"Streaming query started: name='{query_name}', id='{query_id}'")

    def onQueryProgress(self, event: Any) -> None:
        """
        Called on every micro-batch completion.
        Extracts throughput, duration, Kafka lag, and state store statistics.
        """
        progress = _safe_get(event, "progress", event)
        if progress is None:
            return

        query_name = _safe_get(progress, "name") or str(_safe_get(progress, "id", "unknown"))
        query_id = str(_safe_get(progress, "id", ""))
        run_id = str(_safe_get(progress, "runId", ""))
        batch_id = int(_safe_get(progress, "batchId", 0))

        num_input_rows = int(_safe_get(progress, "numInputRows", 0))
        input_rows_per_second = float(_safe_get(progress, "inputRowsPerSecond", 0.0))
        processed_rows_per_second = float(_safe_get(progress, "processedRowsPerSecond", 0.0))

        # Batch duration ms (durationMs.triggerExecution with fallback to sum of durations)
        duration_ms = _safe_get(progress, "durationMs", {}) or {}
        batch_duration_ms = float(_safe_get(duration_ms, "triggerExecution", 0.0) or 0.0)
        if batch_duration_ms == 0.0 and duration_ms:
            batch_duration_ms = float(sum(v for v in duration_ms.values() if isinstance(v, (int, float))))

        # Event-time watermark
        event_time = _safe_get(progress, "eventTime", {}) or {}
        watermark = _safe_get(event_time, "watermark", None)

        # State store operator metrics (for stateful queries like window aggregations)
        state_operators = _safe_get(progress, "stateOperators", []) or []
        state_rows = 0
        state_memory_bytes = 0
        for op in state_operators:
            state_rows += int(_safe_get(op, "numRowsTotal", 0) or 0)
            state_memory_bytes += int(_safe_get(op, "memoryUsedBytes", 0) or 0)

        # Kafka consumer lag: sources[0].metrics["maxOffsetsBehindLatest"]
        sources = _safe_get(progress, "sources", []) or []
        kafka_consumer_lag = 0
        if sources:
            src_metrics = _safe_get(sources[0], "metrics", {}) or {}
            raw_lag = _safe_get(src_metrics, "maxOffsetsBehindLatest", 0)
            try:
                kafka_consumer_lag = int(raw_lag)
            except (ValueError, TypeError):
                kafka_consumer_lag = 0

        # Derive falling_behind flag: batch execution exceeded target trigger interval
        falling_behind = batch_duration_ms > self.trigger_interval_ms

        # Timestamp format
        raw_ts = _safe_get(progress, "timestamp")
        if raw_ts:
            ts_str = str(raw_ts)
        else:
            ts_str = datetime.now(timezone.utc).isoformat()

        metrics_record: Dict[str, Any] = {
            "timestamp": ts_str,
            "query_name": query_name,
            "query_id": query_id,
            "run_id": run_id,
            "batch_id": batch_id,
            "num_input_rows": num_input_rows,
            "input_rows_per_second": input_rows_per_second,
            "processed_rows_per_second": processed_rows_per_second,
            "batch_duration_ms": batch_duration_ms,
            "watermark": watermark,
            "state_rows": state_rows,
            "state_memory_bytes": state_memory_bytes,
            "kafka_consumer_lag": kafka_consumer_lag,
            "falling_behind": falling_behind,
        }

        # Forward record to sink
        try:
            self.sink.record_progress(metrics_record)
        except Exception as e:
            logger.warning(f"Error forwarding progress metrics to sink: {e}")

    def onQueryTerminated(self, event: Any) -> None:
        """Called when a streaming query is stopped or terminated."""
        query_id = _safe_get(event, "id", "")
        exception = _safe_get(event, "exception", None)
        if exception:
            logger.warning(f"Streaming query terminated with error: id='{query_id}', error='{exception}'")
        else:
            logger.info(f"Streaming query terminated gracefully: id='{query_id}'")

    def onQueryIdle(self, event: Any) -> None:
        """Called when a streaming query is idle waiting for new data."""
        pass
