"""
Unit and integration tests for Phase 2 streaming modules and producer.
"""

import os
import sys
import unittest
from datetime import datetime, timezone, timedelta

# Ensure repo root is on sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Configure HADOOP_HOME on Windows if not set
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    local_hadoop = os.path.abspath(os.path.join(repo_root, "hadoop"))
    if os.path.isdir(local_hadoop):
        os.environ["HADOOP_HOME"] = local_hadoop
        hadoop_bin = os.path.join(local_hadoop, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from producer.producer import StockPriceSimulator, load_tickers_from_csv
from streaming.aggregations import build_ohlcv
from streaming.enrichment import enrich, load_metadata
from streaming.source import define_trade_schema


class TestPhase2Streaming(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("TestPhase2Streaming")
            .master("local[2]")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_define_trade_schema(self):
        schema = define_trade_schema()
        field_names = [f.name for f in schema.fields]
        self.assertEqual(field_names, ["event_id", "ticker", "price", "volume", "timestamp"])
        self.assertIsInstance(schema["event_id"].dataType, StringType)
        self.assertIsInstance(schema["ticker"].dataType, StringType)
        self.assertIsInstance(schema["price"].dataType, DoubleType)
        self.assertIsInstance(schema["volume"].dataType, IntegerType)
        self.assertIsInstance(schema["timestamp"].dataType, TimestampType)

    def test_load_metadata_and_enrichment(self):
        metadata_df = load_metadata(self.spark, "data/company_metadata.csv")
        self.assertIn("ticker", metadata_df.columns)
        self.assertIn("company_name", metadata_df.columns)
        self.assertIn("sector", metadata_df.columns)
        self.assertIn("exchange", metadata_df.columns)
        self.assertGreaterEqual(metadata_df.count(), 20)

    def test_build_ohlcv_and_enrichment(self):
        # Create a synthetic DataFrame using spark.range
        # id=0: AAPL @ 10:00:01, price 100.0, volume 10
        # id=1: AAPL @ 10:00:30, price 105.0, volume 20
        # id=2: AAPL @ 10:01:00, price 95.0,  volume 30
        # id=3: AAPL @ 10:01:30, price 102.0, volume 40
        data = [
            ("AAPL", 100.0, 10, "2026-09-28 10:00:01"),
            ("AAPL", 105.0, 20, "2026-09-28 10:00:30"),
            ("AAPL", 95.0, 30, "2026-09-28 10:01:00"),
            ("AAPL", 102.0, 40, "2026-09-28 10:01:30"),
        ]

        test_df = self.spark.range(4).select(
            lit("AAPL").alias("ticker"),
            col("id").alias("id_val"),
            (col("id") * 10 + 10).cast("int").alias("volume"),
            (to_timestamp(lit("2026-09-28 10:00:00")).cast("long") + col("id") * 60).cast("timestamp").alias("timestamp"),
        ).withColumn(
            "price",
            (col("id_val") == 0).cast("double") * 100.0
            + (col("id_val") == 1).cast("double") * 105.0
            + (col("id_val") == 2).cast("double") * 95.0
            + (col("id_val") == 3).cast("double") * 102.0
        ).select("ticker", "price", "volume", "timestamp")

        agg_df = build_ohlcv(test_df, "5 minutes")
        metadata_df = load_metadata(self.spark, "data/company_metadata.csv")
        enriched_df = enrich(agg_df, metadata_df)

        expected_columns = [
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
        self.assertEqual(enriched_df.columns, expected_columns)

        row = enriched_df.collect()[0]
        self.assertEqual(row["ticker"], "AAPL")
        self.assertEqual(row["company_name"], "Apex Analytics Platform")
        self.assertEqual(row["sector"], "Technology")
        self.assertEqual(row["exchange"], "NASDAQ")

        # Success criteria verification:
        # low <= open, close <= high
        self.assertLessEqual(row["low"], row["open"])
        self.assertLessEqual(row["close"], row["high"])
        self.assertLessEqual(row["low"], row["close"])
        self.assertLessEqual(row["open"], row["high"])

        # OHLC values:
        # open should be 100.0 (at 10:00:00)
        # high should be 105.0 (at 10:01:00)
        # low should be 95.0 (at 10:02:00)
        # close should be 102.0 (at 10:03:00)
        # volume should be 10 + 20 + 30 + 40 = 100
        # trade_count should be 4
        self.assertEqual(row["open"], 100.0)
        self.assertEqual(row["high"], 105.0)
        self.assertEqual(row["low"], 95.0)
        self.assertEqual(row["close"], 102.0)
        self.assertEqual(row["volume"], 100)
        self.assertEqual(row["trade_count"], 4)

    def test_producer_late_trade_generation(self):
        tickers = ["AAPL", "MSFT"]
        simulator = StockPriceSimulator(tickers)
        ticker, event = simulator.generate_late_trade(30.0)

        self.assertIn(ticker, tickers)
        self.assertEqual(event["ticker"], ticker)
        self.assertIn("event_id", event)
        self.assertIn("price", event)
        self.assertIn("volume", event)
        self.assertIn("timestamp", event)

        # Parse event timestamp and check it is ~30 minutes in the past
        event_time = datetime.fromisoformat(event["timestamp"])
        now = datetime.now(timezone.utc)
        diff = now - event_time
        # diff should be around 30 minutes (between 29 and 31 minutes)
        self.assertTrue(timedelta(minutes=29) <= diff <= timedelta(minutes=31))


if __name__ == "__main__":
    unittest.main()
