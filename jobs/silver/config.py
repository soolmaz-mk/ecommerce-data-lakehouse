# All settings for the silver layer, read from environment variables.
# Pydantic checks them when the job starts, so a bad value fails early.
from __future__ import annotations

import datetime as dt
from functools import lru_cache

from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SilverConfig(BaseSettings):

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        frozen=True,
        populate_by_name=True,
    )

    minio_endpoint: str = Field(default="http://minio:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(alias="MINIO_ROOT_USER", min_length=1)
    minio_secret_key: SecretStr = Field(alias="MINIO_ROOT_PASSWORD")
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")

    bronze_bucket: str = Field(default="bronze", alias="MINIO_BUCKET", min_length=1)
    warehouse_bucket: str = Field(default="warehouse", alias="WAREHOUSE_BUCKET", min_length=1)

    catalog_name: str = Field(default="silver", alias="ICEBERG_CATALOG_NAME", min_length=1)
    catalog_uri: str = Field(default="http://iceberg-rest:8181", alias="ICEBERG_REST_URI")
    namespace: str = Field(default="silver", alias="SILVER_NAMESPACE", min_length=1)

    lookback_days: int = Field(default=3, alias="SILVER_LOOKBACK_DAYS", ge=0, le=365)

    bronze_hive_partitioning: bool = Field(default=True, alias="SILVER_BRONZE_HIVE")
    bronze_extra_columns: str = Field(default="ignore", alias="SILVER_BRONZE_EXTRA_COLUMNS")
    bronze_missing_columns: str = Field(default="insert", alias="SILVER_BRONZE_MISSING_COLUMNS")
    staging_filename: str = Field(default="data.parquet", alias="SILVER_STAGING_FILENAME")
    staging_compression: str = Field(default="snappy", alias="SILVER_STAGING_COMPRESSION")

    dim_date_start: dt.date = Field(default=dt.date(2020, 1, 1), alias="SILVER_DIM_DATE_START")
    dim_date_end: dt.date = Field(default=dt.date(2030, 12, 31), alias="SILVER_DIM_DATE_END")

    snapshot_retention_days: int = Field(default=7, alias="SILVER_SNAPSHOT_RETENTION_DAYS", ge=1)
    stale_branch_age_hours: int = Field(default=24, alias="SILVER_STALE_BRANCH_AGE_HOURS", ge=1)

    dq_reconciliation_tolerance: float = Field(default=0.01, alias="SILVER_DQ_TOLERANCE", ge=0)
    dq_max_mismatch_frac: float = Field(default=0.02, alias="SILVER_DQ_MAX_MISMATCH", ge=0, le=1)
    dq_max_missing_lines_frac: float = Field(default=0.60, alias="SILVER_DQ_MAX_MISSING_LINES", ge=0, le=1)
    dq_max_returns_unresolved_order_frac: float = Field(
        default=0.25, alias="SILVER_DQ_MAX_RETURNS_NO_ORDER", ge=0, le=1
    )
    dq_max_returns_unresolved_product_frac: float = Field(
        default=0.70, alias="SILVER_DQ_MAX_RETURNS_NO_PRODUCT", ge=0, le=1
    )

    @field_validator("minio_endpoint", "catalog_uri")
    @classmethod
    def _must_be_http_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"must start with http:// or https://, got {v!r}")
        return v.rstrip("/")

    @field_validator("bronze_extra_columns")
    @classmethod
    def _valid_extra(cls, v: str) -> str:
        if v not in ("ignore", "raise"):
            raise ValueError(f"must be 'ignore' or 'raise', got {v!r}")
        return v

    @field_validator("bronze_missing_columns")
    @classmethod
    def _valid_missing(cls, v: str) -> str:
        if v not in ("insert", "raise"):
            raise ValueError(f"must be 'insert' or 'raise', got {v!r}")
        return v

    @field_validator("dim_date_end")
    @classmethod
    def _end_after_start(cls, v: dt.date, info) -> dt.date:
        start = info.data.get("dim_date_start")
        if start and v <= start:
            raise ValueError(f"dim_date_end {v} must be after dim_date_start {start}")
        return v

    @computed_field
    @property
    def bronze_uri(self) -> str:
        return f"s3://{self.bronze_bucket}"

    @computed_field
    @property
    def warehouse_uri(self) -> str:
        return f"s3://{self.warehouse_bucket}"

    @property
    def snapshot_retention(self) -> dt.timedelta:
        return dt.timedelta(days=self.snapshot_retention_days)

    @property
    def stale_branch_age(self) -> dt.timedelta:
        return dt.timedelta(hours=self.stale_branch_age_hours)

    def bronze_topic_uri(self, topic: str) -> str:
        return f"{self.bronze_uri}/{topic}"

    def staging_uri(self, interval_nodash: str, source: str) -> str:
        return f"{self.warehouse_uri}/_staging/{interval_nodash}/{source}"

    def quarantine_uri(self, interval_nodash: str, source: str) -> str:
        return f"{self.warehouse_uri}/_quarantine/{interval_nodash}/{source}"

    def table_identifier(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    @property
    def polars_storage_options(self) -> dict[str, str]:
        return {
            "aws_access_key_id": self.minio_access_key,
            "aws_secret_access_key": self.minio_secret_key.get_secret_value(),
            "aws_endpoint_url": self.minio_endpoint,
            "aws_region": self.aws_region,
            "aws_allow_http": "true",
        }

    @property
    def pyiceberg_catalog_properties(self) -> dict[str, str]:
        return {
            "type": "rest",
            "uri": self.catalog_uri,
            "warehouse": f"{self.warehouse_uri}/",
            "s3.endpoint": self.minio_endpoint,
            "s3.access-key-id": self.minio_access_key,
            "s3.secret-access-key": self.minio_secret_key.get_secret_value(),
            "s3.path-style-access": "true",
            "s3.region": self.aws_region,
        }


@lru_cache(maxsize=1)
def load_config() -> SilverConfig:
    return SilverConfig()
