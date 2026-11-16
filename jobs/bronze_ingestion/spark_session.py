"""
Builds the job's SparkSession (UTC session tz, FAIR scheduler).
S3A/Kafka/Avro come from jars baked into the image (spark/Dockerfile),
not spark.jars.packages - those resolve before driver Python runs.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import BronzeConfig


def build_spark_session(config: BronzeConfig) -> SparkSession:
    return (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # FAIR not FIFO - round-robin main.py's per-topic pools - else topics starve
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )
