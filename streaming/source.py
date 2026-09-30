"""
Kafka source module for reading and parsing real-time stock trade events.
Preserves Kafka partition, offset, and timestamp lineage for exactly-once tracking.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, MAX_OFFSETS_PER_TRIGGER


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
    bootstrap_servers: str = KAFKA_BOOTSTRAP,
    topic: str = KAFKA_TOPIC,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int | None = None,
) -> DataFrame:
    """
    Read stock-trades from Kafka, parse JSON payload with explicit schema,
    cast event timestamp to UTC TimestampType, preserve Kafka partition/offset/timestamp lineage,
    and add trade_date and ingested_at metadata.
    """
    trade_schema = define_trade_schema()

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )

    limit = max_offsets_per_trigger if max_offsets_per_trigger is not None else MAX_OFFSETS_PER_TRIGGER
    if limit is not None and limit > 0:
        reader = reader.option("maxOffsetsPerTrigger", str(limit))

    kafka_df = reader.load()

    parsed_df = (
        kafka_df.select(
            col("partition").cast(IntegerType()).alias("kafka_partition"),
            col("offset").cast(LongType()).alias("kafka_offset"),
            col("timestamp").cast(TimestampType()).alias("kafka_timestamp"),
            from_json(col("value").cast(StringType()), trade_schema).alias("trade"),
            current_timestamp().alias("ingested_at"),
        )
        .select(
            col("trade.event_id").cast(StringType()).alias("event_id"),
            col("trade.ticker").cast(StringType()).alias("ticker"),
            col("trade.price").cast(DoubleType()).alias("price"),
            col("trade.volume").cast(IntegerType()).alias("volume"),
            col("trade.timestamp").cast(TimestampType()).alias("timestamp"),
            to_date(col("trade.timestamp")).alias("trade_date"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
        )
    )

    return parsed_df
