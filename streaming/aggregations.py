"""
Windowed OHLCV aggregation module for streaming trade events.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    max as spark_max,
    max_by,
    min as spark_min,
    min_by,
    sum as spark_sum,
    window,
)


def build_ohlcv(trades_df: DataFrame, window_duration: str) -> DataFrame:
    """
    Build OHLCV aggregations over the specified window duration with a 10-minute watermark.

    Computes deterministic open/close using min_by/max_by on the event timestamp:
      - open: price at earliest event timestamp in window
      - high: maximum price in window
      - low: minimum price in window
      - close: price at latest event timestamp in window
      - volume: sum of volumes across all trades in window
      - trade_count: total count of trades in window (count(*))

    Flattens the nested window struct into window_start and window_end columns.
    """
    return (
        trades_df.withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), window_duration).alias("window"),
            col("ticker"),
        )
        .agg(
            min_by(col("price"), col("timestamp")).alias("open"),
            spark_max(col("price")).alias("high"),
            spark_min(col("price")).alias("low"),
            max_by(col("price"), col("timestamp")).alias("close"),
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
