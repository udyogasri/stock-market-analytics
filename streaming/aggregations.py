"""
Windowed OHLCV aggregation module for streaming trade events.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    lit,
    max as spark_max,
    max_by,
    min as spark_min,
    min_by,
    struct,
    sum as spark_sum,
    window,
)


def build_ohlcv(trades_df: DataFrame, window_duration: str) -> DataFrame:
    """
    Build OHLCV aggregations over the specified window duration with a 10-minute watermark.

    Computes deterministic open/close using min_by/max_by over struct(timestamp, kafka_offset):
      - open: price at earliest (timestamp, kafka_offset) in window
      - high: maximum price in window
      - low: minimum price in window
      - close: price at latest (timestamp, kafka_offset) in window
      - volume: sum of volumes across all trades in window
      - trade_count: total count of trades in window (count(*))

    Flattens the nested window struct into window_start and window_end columns.
    """
    offset_col = col("kafka_offset") if "kafka_offset" in trades_df.columns else lit(0).alias("kafka_offset")
    order_col = struct(col("timestamp"), offset_col)

    return (
        trades_df.withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), window_duration).alias("window"),
            col("ticker"),
        )
        .agg(
            min_by(col("price"), order_col).alias("open"),
            spark_max(col("price")).alias("high"),
            spark_min(col("price")).alias("low"),
            max_by(col("price"), order_col).alias("close"),
            spark_sum(col("volume")).alias("volume"),
            count("*").alias("trade_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("ticker"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trade_count"),
        )
    )
