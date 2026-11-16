"""
The single-topic Structured Streaming job (one Spark app per topic): read
Kafka -> Avro decode -> split good/DLQ -> Parquet to bronze, then block on
query.awaitTermination(). Only ever run under spark-submit (see launcher.py,
which spark-submits this once per topic via run_bronze_ingestion.py --topic).
"""

from __future__ import annotations

from .config import BronzeConfig
from .kafka_reader import KafkaReader
from .logger import get_logger
from .minio_writer import MinIOWriter
from .spark_session import build_spark_session
from .transformation import SchemaTransformer

logger = get_logger(__name__)


def run(topic: str) -> None:
    config = BronzeConfig.from_env()
    spark = build_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    reader = KafkaReader(spark, config)
    transformer = SchemaTransformer(spark, config.schema_registry_url)
    writer = MinIOWriter(config)

    raw = reader.read_stream(topic)
    decoded = transformer.process(raw, topic)
    # one query, two sinks: good -> bronze, decode failures -> DLQ
    query = writer.write_stream(
        decoded,
        topic,
        good_transform=lambda b: transformer.good_rows(b, topic),
        dlq_transform=lambda b: transformer.dead_letters(b, topic),
    )
    logger.info(f"started stream for topic={topic}")
    query.awaitTermination()
