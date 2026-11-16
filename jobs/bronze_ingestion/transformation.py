"""
Decodes each topic's Confluent-Avro Kafka value and splits good vs DLQ.
Decode per record by embedded schema id (from_avro CASE-WHEN over all
registered versions, PERMISSIVE so a bad record -> null not a batch crash),
unified via to_json/from_json onto the latest schema. process() keeps the
raw value + data struct so the writer can route decode failures to a DLQ.
Stays on the JVM (no UDF); logical types survive, only string ts get a cast.
"""

from __future__ import annotations

import json
import time

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType

from .config import EVENT_DATE_SANITY_FLOOR, TOPIC_EVENT_DATE_COLUMN, TOPIC_STRING_TIMESTAMP_COLUMNS
from .logger import get_logger
from .schema_registry import fetch_all_schemas

logger = get_logger(__name__)

# magic(1) + schema id(4), ahead of the Avro payload
_CONFLUENT_PREFIX_BYTES = 5

# micros + UTC - both sides of to_json/from_json - default truncates to millis
_TIMESTAMP_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"

_KAFKA_INGEST_TS_COLUMN = "kafka_ingest_timestamp"

# refetch a topic's schema list at most this often - twice a day,
_SCHEMA_CACHE_TTL_SECONDS = 12 * 60 * 60


class SchemaTransformer:
    """Decodes Avro Kafka payloads per topic using the Schema Registry."""

    def __init__(self, spark: SparkSession, schema_registry_url: str):
        self.spark = spark
        self.schema_registry_url = schema_registry_url
        # (fetched_at, schemas) per topic - re-fetch once _SCHEMA_CACHE_TTL_SECONDS
        self._schemas_cache: dict[str, tuple[float, list[tuple[int, str]]]] = {}

    def _schemas_for(self, topic: str) -> list[tuple[int, str]]:
        cached = self._schemas_cache.get(topic)
        if cached is not None and time.monotonic() - cached[0] < _SCHEMA_CACHE_TTL_SECONDS:
            return cached[1]
        schemas = fetch_all_schemas(self.schema_registry_url, f"{topic}-value")
        self._schemas_cache[topic] = (time.monotonic(), schemas)
        return schemas

    def _avro_json_to_struct(self, avro_json: str) -> StructType:
        """Avro schema JSON -> Spark StructType, via JVM SchemaConverters."""
        # reuse from_avro's own type mapping - exact decimal/date/timestamp types
        jvm = self.spark._jvm
        avro_schema = jvm.org.apache.avro.Schema.Parser().parse(avro_json)
        sql_type = jvm.org.apache.spark.sql.avro.SchemaConverters.toSqlType(
            avro_schema
        ).dataType()
        return StructType.fromJson(json.loads(sql_type.json()))

    @staticmethod
    def _schema_id_col() -> Column:
        # schema id = big-endian bytes 2..5 (byte 1 = magic) -> int
        return F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast("long")

    def _decode_by_schema_id(self, schemas: list[tuple[int, str]]) -> Column:
        """JSON-string col: each row decoded by its embedded schema id.

        One from_avro branch per version (can't vary schema per row); PERMISSIVE
        so a corrupt payload yields null (-> DLQ) instead of failing the batch;
        differently-typed structs unified as JSON.
        """
        schema_id = self._schema_id_col()
        payload = F.expr(f"substring(value, {_CONFLUENT_PREFIX_BYTES + 1})")

        decoded_json: Column = F.lit(None).cast("string")
        for sid, schema_json in schemas:
            # correct writer schema per branch - to_json unifies the types
            branch = F.to_json(
                from_avro(payload, schema_json, {"mode": "PERMISSIVE"}),
                {"timestampFormat": _TIMESTAMP_FMT},
            )
            decoded_json = F.when(schema_id == F.lit(sid), branch).otherwise(decoded_json)
        return decoded_json

    @staticmethod
    def _value_is_valid_col(topic: str, reader_struct: StructType) -> Column:
        """True unless the topic's event-date field decoded to an implausibly
        old value (config.EVENT_DATE_SANITY_FLOOR) - Avro-valid but
        semantically garbage, e.g. an unset upstream field defaulting to the
        Unix epoch. True (nothing to flag) when the topic has no configured
        event-date column, the field isn't in this schema version, or the
        field is simply absent on this row - only a *present, implausible*
        value counts as invalid.
        """
        source_column = TOPIC_EVENT_DATE_COLUMN.get(topic)
        if source_column is None or source_column not in reader_struct.fieldNames():
            return F.lit(True)
        value = F.col("data")[source_column].cast("date")
        return value.isNull() | (value >= F.lit(EVENT_DATE_SANITY_FLOOR).cast("date"))

    def process(self, df: DataFrame, topic: str) -> DataFrame:
        """Decode `topic`'s value; keep raw fields + `data` struct for splitting.

        `data` is null when decoding failed (unknown schema id or corrupt
        bytes); `value_is_valid` is false when decoding succeeded but the
        event-date field is implausible (see _value_is_valid_col). Both
        splits happen downstream in good_rows()/dead_letters().
        """
        schemas = self._schemas_for(topic)
        if not schemas:
            raise ValueError(f"No Avro schema registered for topic '{topic}'")

        # Reader schema = the topic's latest registered version (last entry).
        reader_struct = self._avro_json_to_struct(schemas[-1][1])

        data = F.from_json(
            self._decode_by_schema_id(schemas),
            reader_struct,
            {"mode": "PERMISSIVE", "timestampFormat": _TIMESTAMP_FMT},
        )
        return df.select(
            "key",
            "value",
            "topic",
            _KAFKA_INGEST_TS_COLUMN,
            self._schema_id_col().alias("schema_id"),
            data.alias("data"),
        ).withColumn("value_is_valid", self._value_is_valid_col(topic, reader_struct))

    def good_rows(self, df: DataFrame, topic: str) -> DataFrame:
        """Decoded AND value-valid rows: explode data.*, cast string ts, add event_date."""
        decoded = df.where(
            F.col("data").isNotNull() & F.col("value_is_valid")
        ).select("key", _KAFKA_INGEST_TS_COLUMN, "data.*")
        for column in TOPIC_STRING_TIMESTAMP_COLUMNS.get(topic, ()):
            if column in decoded.columns:
                decoded = decoded.withColumn(column, F.to_timestamp(F.col(column)))
        return self.add_partition_column(decoded, topic)

    def dead_letters(self, df: DataFrame, topic: str) -> DataFrame:
        """Undecodable or value-invalid rows, with raw bytes preserved for the DLQ."""
        known_ids = [sid for sid, _ in self._schemas_for(topic)]
        reason = (
            F.when(F.col("data").isNull() & ~F.col("schema_id").isin(known_ids), F.lit("unknown_schema_id"))
            .when(F.col("data").isNull(), F.lit("avro_decode_failed"))
            .otherwise(F.lit("invalid_value"))
        )
        return (
            df.where(
                F.col("value").isNotNull()
                & (F.col("data").isNull() | ~F.col("value_is_valid"))
            )
            # dlq_reason references data/schema_id - compute it before the
            # select below drops them, not after (AnalysisException: a
            # withColumn referencing an already-projected-away column fails
            # at plan-analysis time, on every batch, for every topic)
            .withColumn("dlq_reason", reason)
            .select("key", "value", "topic", _KAFKA_INGEST_TS_COLUMN, "schema_id", "dlq_reason")
            .withColumn("dlq_ingested_at", F.current_timestamp())
        )

    @staticmethod
    def add_partition_column(df: DataFrame, topic: str) -> DataFrame:
        """Add the `event_date` column bronze Parquet output is partitioned by."""
        source_column = TOPIC_EVENT_DATE_COLUMN.get(topic, _KAFKA_INGEST_TS_COLUMN)
        if source_column not in df.columns:
            source_column = _KAFKA_INGEST_TS_COLUMN
        return df.withColumn("event_date", F.to_date(F.col(source_column)))
