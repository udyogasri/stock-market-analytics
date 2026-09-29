"""
Metadata enrichment module for static company metadata broadcast joins.
"""

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast


def load_metadata(
    spark: SparkSession,
    metadata_path: str = "data/company_metadata.csv",
) -> DataFrame:
    """
    Read company metadata CSV as a static DataFrame.
    Resolves relative path from working directory or project root.
    """
    if not os.path.isabs(metadata_path) and not os.path.exists(metadata_path):
        candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", metadata_path))
        if os.path.exists(candidate):
            metadata_path = candidate

    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(metadata_path)
    )


def enrich(df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    """
    Broadcast join aggregated trade metrics with company metadata on ticker.
    Adds company_name, sector, and exchange, ordered sensibly for console output:
    ticker, company_name, sector, exchange, window_start, window_end,
    open, high, low, close, volume, trade_count.
    """
    enriched_df = df.join(broadcast(metadata_df), on="ticker", how="left")

    ordered_columns = [
        "ticker",
        "company_name",
        "sector",
        "exchange",
        "window_start",
        "window_end",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
    ]

    # Select standard column order if all aggregate and metadata columns exist
    if all(col_name in enriched_df.columns for col_name in ordered_columns):
        return enriched_df.select(ordered_columns)

    return enriched_df
