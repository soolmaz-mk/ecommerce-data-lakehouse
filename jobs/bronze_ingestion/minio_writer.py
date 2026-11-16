"""
MinIOWriter: one streaming query per topic that foreachBatch-writes a decoded
stream to two Parquet sinks - good rows to s3a://<bucket>/<bronze>/<topic>
(partitioned by event_date), decode failures to s3a://<bucket>/<dlq>/<topic>.
One Kafka read feeds both (foreachBatch, not two queries) - so file writes are
at-least-once, not exactly-once; dedupe downstream in silver.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from .config import BronzeConfig
from .logger import get_logger

Transform = Callable[[DataFrame], DataFrame]


class MinIOWriter:

    def __init__(self, config: BronzeConfig) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    def write_stream(
        self,
        df: DataFrame,
        topic: str,
        good_transform: Transform,
        dlq_transform: Transform,
    ) -> StreamingQuery:
        """Start the topic's dual-sink query (good -> bronze, failures -> DLQ)."""

        if df is None:
            raise ValueError("Input DataFrame cannot be None")
        if not df.isStreaming:
            raise ValueError("MinIOWriter.write_stream expects a streaming DataFrame")

        clean_topic = self._clean_topic(topic)
        good_path = self._build_path(self.config.bronze_path, clean_topic)
        dlq_path = self._build_path(self.config.dlq_path, clean_topic)
        checkpoint_path = self._build_path(self.config.checkpoint_path, clean_topic)

        def write_batch(batch_df: DataFrame, batch_id: int) -> None:
            # persist - good and dlq both scan it, so decode runs once per batch
            batch_df.persist()
            try:
                good_df = good_transform(batch_df)
                if not good_df.isEmpty():
                    row_count = good_df.count()
                    (
                        good_df.coalesce(1).write.mode("append")
                        .option("compression", "snappy")
                        .partitionBy("event_date")
                        .parquet(good_path)
                    )
                    self.logger.info(
                        "topic=%s batch=%s: ingested %d row(s) -> %s",
                        clean_topic, batch_id, row_count, good_path,
                    )

                dlq_df = dlq_transform(batch_df)
                if not dlq_df.isEmpty():
                    
                    dlq_count = dlq_df.count()
                    (
                        dlq_df.coalesce(1).write.mode("append")
                        .option("compression", "snappy")
                        .parquet(dlq_path)
                    )
                    self.logger.warning(
                        "topic=%s batch=%s: %d dead letter(s) -> %s",
                        clean_topic, batch_id, dlq_count, dlq_path,
                    )
            finally:
                batch_df.unpersist()

        self.logger.info(
            "Starting MinIO writer for topic=%s good=%s dlq=%s checkpoint=%s",
            clean_topic,
            good_path,
            dlq_path,
            checkpoint_path,
        )

        return (
            df.writeStream
            .foreachBatch(write_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=self.config.trigger_interval)
            .queryName(f"bronze_minio_writer_{clean_topic}")
            .start()
        )

    def _build_path(self, base_path: str, topic: str) -> str:
        # s3a://<bucket>/<base>/<topic>, or s3a://<bucket>/<topic> when base is empty
        bucket = self.config.minio_bucket.strip().strip("/")
        base = base_path.strip().strip("/")
        if not bucket:
            raise ValueError("MinIO bucket cannot be empty")
        prefix = f"{base}/" if base else ""
        return f"s3a://{bucket}/{prefix}{topic}"

    @staticmethod
    def _clean_topic(topic: str) -> str:
        if topic is None:
            raise ValueError("Topic cannot be None")
        clean_topic = topic.strip().strip("/")
        if not clean_topic:
            raise ValueError("Topic cannot be empty")
        return clean_topic
