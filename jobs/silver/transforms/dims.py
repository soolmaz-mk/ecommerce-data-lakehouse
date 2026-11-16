# Builds the 9 dimension tables from the staged data.
# Every dimension gets an Unknown (-1) and a Late arriving (-2) row.
from __future__ import annotations

import datetime as dt

import polars as pl

from jobs.silver.config import SilverConfig, load_config
from jobs.silver.transforms.common import DOMAINS, KEYS, Hashing


class DimensionBuilder:

    USER_TRACKED = ["loyalty_tier", "city", "province"]
    PRODUCT_TRACKED = ["product_name", "list_price", "category_top", "category_sub", "category_leaf"]

    def __init__(self, cfg: SilverConfig | None = None):
        self.cfg = cfg or load_config()

    def date(self, start: dt.date | None = None, end: dt.date | None = None) -> pl.DataFrame:
        start = start or self.cfg.dim_date_start
        end = end or self.cfg.dim_date_end
        days = pl.date_range(start, end, interval="1d", eager=True).alias("full_date")
        return pl.DataFrame(days).with_columns(
            date_key=pl.col("full_date").dt.strftime("%Y%m%d").cast(pl.Int32),
            year=pl.col("full_date").dt.year().cast(pl.Int32),
            quarter=pl.col("full_date").dt.quarter().cast(pl.Int32),
            month=pl.col("full_date").dt.month().cast(pl.Int32),
            month_name=pl.col("full_date").dt.strftime("%B"),
            iso_week=pl.col("full_date").dt.week().cast(pl.Int32),
            day_of_week=pl.col("full_date").dt.weekday().cast(pl.Int32),
            day_name=pl.col("full_date").dt.strftime("%A"),
            is_weekend=pl.col("full_date").dt.weekday().is_in([6, 7]),
        )

    def user(self, staged_users: pl.DataFrame) -> pl.DataFrame:
        body = staged_users.with_columns(
            valid_from=pl.lit(KEYS.EPOCH, dtype=pl.Datetime("us")),
            valid_to=pl.lit(None, dtype=pl.Datetime("us")),
            is_current=pl.lit(True),
            row_hash=Hashing.content(self.USER_TRACKED),
        ).with_columns(
            user_key=Hashing.surrogate("user_id", "valid_from"),
        ).select(
            "user_key", "user_id", "username", "email", "signup_date",
            "city", "province", "loyalty_tier", "valid_from", "valid_to",
            "is_current", "row_hash",
        )
        return pl.concat([self._user_sentinels(), body], how="vertical")

    @staticmethod
    def _user_sentinels() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "user_key": [KEYS.UNKNOWN, KEYS.LATE],
                "user_id": [KEYS.UNKNOWN_LABEL, KEYS.LATE_LABEL],
                "username": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "email": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "signup_date": [None, None],
                "city": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "province": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "loyalty_tier": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "valid_from": [KEYS.EPOCH, KEYS.EPOCH],
                "valid_to": [None, None],
                "is_current": [True, True],
                "row_hash": [KEYS.SENTINEL_HASH, KEYS.SENTINEL_HASH],
            },
            schema_overrides={
                "user_key": pl.Int64, "signup_date": pl.Date,
                "valid_from": pl.Datetime("us"), "valid_to": pl.Datetime("us"),
            },
        )

    def product(self, staged_price_history: pl.DataFrame, staged_products: pl.DataFrame) -> pl.DataFrame:
        names = staged_products.select("product_id", "product_name")
        body = (
            staged_price_history.join(names, on="product_id", how="left")
            .with_columns(
                product_name=pl.col("product_name").fill_null(KEYS.UNKNOWN_LABEL),
                category_top=pl.lit(KEYS.UNKNOWN_LABEL),
                category_sub=pl.lit(KEYS.UNKNOWN_LABEL),
                category_leaf=pl.lit(KEYS.UNKNOWN_LABEL),
            )
            .with_columns(row_hash=Hashing.content(self.PRODUCT_TRACKED))
            .with_columns(product_key=Hashing.surrogate("product_id", "valid_from"))
            .select(
                "product_key", "product_id", "product_name", "list_price",
                "category_top", "category_sub", "category_leaf",
                "valid_from", "valid_to", "is_current", "row_hash",
            )
        )
        return pl.concat([self._product_sentinels(), body], how="vertical")

    @staticmethod
    def _product_sentinels() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "product_key": [KEYS.UNKNOWN, KEYS.LATE],
                "product_id": [KEYS.UNKNOWN_LABEL, KEYS.LATE_LABEL],
                "product_name": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "list_price": [None, None],
                "category_top": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "category_sub": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "category_leaf": [KEYS.UNKNOWN_LABEL, KEYS.UNKNOWN_LABEL],
                "valid_from": [KEYS.EPOCH, KEYS.EPOCH],
                "valid_to": [None, None],
                "is_current": [True, True],
                "row_hash": [KEYS.SENTINEL_HASH, KEYS.SENTINEL_HASH],
            },
            schema_overrides={
                "product_key": pl.Int64, "list_price": pl.Decimal(10, 2),
                "valid_from": pl.Datetime("us"), "valid_to": pl.Datetime("us"),
            },
        )

    def category(self, staged_categories: pl.DataFrame) -> pl.DataFrame:
        body = staged_categories.with_columns(
            category_key=Hashing.surrogate("category_id")
        ).select(
            "category_key", "category_id", "category_name", "parent_category_id",
            "category_top", "category_sub", "category_leaf",
        )
        unknown = pl.DataFrame(
            {
                "category_key": [KEYS.UNKNOWN],
                "category_id": [KEYS.UNKNOWN_LABEL],
                "category_name": [KEYS.UNKNOWN_LABEL],
                "parent_category_id": [None],
                "category_top": [KEYS.UNKNOWN_LABEL],
                "category_sub": [KEYS.UNKNOWN_LABEL],
                "category_leaf": [KEYS.UNKNOWN_LABEL],
            },
            schema_overrides={
                "category_key": pl.Int64, "category_id": pl.Utf8, "parent_category_id": pl.Utf8,
            },
        )
        return pl.concat([unknown, body], how="vertical")

    @staticmethod
    def _decode(values, key_col: str, val_col: str) -> pl.DataFrame:
        values = list(values)
        return pl.DataFrame(
            {
                key_col: [KEYS.UNKNOWN] + list(range(1, len(values) + 1)),
                val_col: [KEYS.UNKNOWN_LABEL] + values,
            },
            schema_overrides={key_col: pl.Int32},
        )

    def payment_method(self) -> pl.DataFrame:
        return self._decode(sorted(DOMAINS.payment_methods), "payment_method_key", "payment_method")

    def order_status(self) -> pl.DataFrame:
        return self._decode(list(DOMAINS.order_statuses), "order_status_key", "order_status")

    def event_type(self) -> pl.DataFrame:
        return self._decode(sorted(DOMAINS.event_types), "event_type_key", "event_type")

    def device(self) -> pl.DataFrame:
        return self._decode(sorted(DOMAINS.devices), "device_key", "device")

    def return_reason(self) -> pl.DataFrame:
        return self._decode(sorted(DOMAINS.return_reasons), "return_reason_key", "return_reason")
