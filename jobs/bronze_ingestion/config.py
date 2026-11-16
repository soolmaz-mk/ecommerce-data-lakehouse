"""
Loads bronze job settings from env / .env into a frozen BronzeConfig.
Fails fast on a missing required var; splits KAFKA_TOPICS on commas.
"""

from __future__ import annotations

from typing import Tuple

try:
    from typing import Annotated  # Python >= 3.9
except ImportError:
    from typing_extensions import Annotated  # Python 3.8 (the Spark image's version)

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class BronzeConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", frozen=True, extra="ignore")

    kafka_bootstrap_servers: str
    kafka_topics: Annotated[Tuple[str, ...], NoDecode]
    # earliest not latest - capture full backlog + new - first run only, then checkpoint
    kafka_starting_offsets: str = "earliest"
    schema_registry_url: str
    # dedicated bronze bucket - data at root, dlq + checkpoints as _-prefixed siblings
    minio_bucket: str = "bronze"
    bronze_path: str = ""            # s3a://bronze/<topic>
    dlq_path: str = "_dlq"           # s3a://bronze/_dlq/<topic>
    checkpoint_path: str = "_checkpoints"  # s3a://bronze/_checkpoints/<topic>
    trigger_interval: str = "15 seconds"
    app_name: str = Field(default="bronze-ingestion", validation_alias="SPARK_APP_NAME")

    @field_validator("kafka_topics", mode="before")
    @classmethod
    def _split_topics(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(topic.strip() for topic in value.split(",") if topic.strip())
        return value

    @classmethod
    def from_env(cls) -> "BronzeConfig":
        return cls()


# --- per-topic decode config (structural, tied to the Avro schemas) ---

# plain-string ts fields needing an explicit cast - others resolve via Avro logical types
TOPIC_STRING_TIMESTAMP_COLUMNS: dict[str, tuple[str, ...]] = {
    "behavioral.events": ("timestamp",),
}

# event_date source column per topic - falls back to kafka_ingest_timestamp
TOPIC_EVENT_DATE_COLUMN: dict[str, str] = {
    "transactional.orders": "timestamp",
    "transactional.users": "signup_date",
    "transactional.product_price_history": "valid_from",
    "behavioral.events": "timestamp",
    # returns_refunds not live yet - confirm field name once it produces
    "transactional.returns_refunds": "return_timestamp",
}

EVENT_DATE_SANITY_FLOOR = "2000-01-01"
