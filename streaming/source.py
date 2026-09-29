"""
Kafka source module for reading and parsing real-time stock trade events.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def define_trade_schema() -> StructType:
    """
    Return the explicit schema for the JSON trade payload:
    - event_id: string
    - ticker: string
    - price: double
    - volume: int
    - timestamp: timestamp
    """
    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("ticker", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("volume", IntegerType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
        ]
    )


def read_trades(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "stock-trades",
    starting_offsets: str = "latest",
) -> DataFrame:
    """
    Read stock-trades from Kafka, parse JSON payloads with the explicit schema,
    cast timestamp to a proper TimestampType (event time, UTC), and return the parsed DataFrame.
    """
    trade_schema = define_trade_schema()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), trade_schema).alias("trade"))
        .select(
            col("trade.event_id").cast(StringType()).alias("event_id"),
            col("trade.ticker").cast(StringType()).alias("ticker"),
            col("trade.price").cast(DoubleType()).alias("price"),
            col("trade.volume").cast(IntegerType()).alias("volume"),
            col("trade.timestamp").cast(TimestampType()).alias("timestamp"),
        )
    )

    return parsed_df
