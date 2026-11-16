"""
Subscribes one Kafka topic as a streaming DataFrame.
Keeps key/value/topic raw (value stays Avro bytes for transformation.py)
and renames Kafka's record ts to kafka_ingest_timestamp to dodge payload clashes.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from .logger import get_logger
from .config import BronzeConfig

logger = get_logger(__name__)


class KafkaReader:
    def __init__(self, spark: SparkSession, config: BronzeConfig):

        self.spark = spark
        self.bootstrap_servers = config.kafka_bootstrap_servers
        self.starting_offsets = config.kafka_starting_offsets

    def read_stream(self, topic: str) -> DataFrame:

        logger.info(f"Initializing Kafka stream reader for topic: '{topic}' at {self.bootstrap_servers}")
        try:
            raw_stream_df = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", self.starting_offsets)
                # 120s - remote broker over public internet - give slow polls room
                .option("kafkaConsumer.pollTimeoutMs", "120000")
                .option("maxOffsetsPerTrigger", "50000")
                .load()
            )

            # value stays raw Avro bytes (not string, would corrupt) - rename kafka ts to avoid payload clash
            clean_stream_df = raw_stream_df.select(
                "key",
                "value",
                "topic",
                F.col("timestamp").alias("kafka_ingest_timestamp"),
            )

            logger.info(f"Successfully connected and subscribed to Kafka topic: '{topic}'")
            return clean_stream_df

        except Exception as e:
            logger.error(f"Failed to create stream reader for topic '{topic}': {str(e)}")
            raise e