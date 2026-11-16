# Builds the 4 fact tables and attaches the dimension keys.
# It uses a point-in-time join, so old rows keep the old dimension version.
from __future__ import annotations

import polars as pl

from jobs.silver.transforms.common import KEYS, Expressions


class FactBuilder:

    RECONCILIATION_TOLERANCE = 0.01

    @staticmethod
    def _asof_scd2(
        facts: pl.DataFrame,
        dim: pl.DataFrame,
        ts_col: str,
        natural_key: str,
        key_col: str,
        fill: int = KEYS.LATE,
    ) -> pl.DataFrame:
        d = dim.select([natural_key, "valid_from", key_col]).sort([natural_key, "valid_from"])
        f = facts.sort([natural_key, ts_col])
        out = f.join_asof(
            d, left_on=ts_col, right_on="valid_from", by=natural_key, strategy="backward"
        )
        return out.with_columns(pl.col(key_col).fill_null(fill))

    @staticmethod
    def _decode_key(facts: pl.DataFrame, dim: pl.DataFrame, val_col: str, key_col: str) -> pl.DataFrame:
        out = facts.join(dim.select([val_col, key_col]), on=val_col, how="left")
        return out.with_columns(pl.col(key_col).fill_null(KEYS.UNKNOWN))

    def orders(self, orders: pl.DataFrame, items: pl.DataFrame,
               dim_user: pl.DataFrame, dim_payment: pl.DataFrame,
               dim_status: pl.DataFrame) -> pl.DataFrame:
        rollup = items.group_by("order_id").agg(
            order_net_amount=pl.col("net_sales_amount").sum(),
            _lines_gross=pl.col("gross_sales_amount").sum(),
            order_line_count=pl.len().cast(pl.Int32),
            order_total_quantity=pl.col("quantity").sum().cast(pl.Int32),
        )
        f = orders.join(rollup, on="order_id", how="left").with_columns(
            order_date_key=Expressions.date_key(pl.col("order_timestamp")),
            order_net_amount=pl.col("order_net_amount").fill_null(0).cast(pl.Decimal(18, 2)),
            order_discount_amount=(
                pl.col("order_gross_amount") - pl.col("order_net_amount").fill_null(0)
            ).cast(pl.Decimal(18, 2)),
            order_line_count=pl.col("order_line_count").fill_null(0),
            order_total_quantity=pl.col("order_total_quantity").fill_null(0),
            source_total_matches_lines=(
                (
                    pl.col("order_gross_amount").cast(pl.Float64)
                    - pl.col("_lines_gross").cast(pl.Float64)
                ).abs() < self.RECONCILIATION_TOLERANCE
            ).fill_null(False),
        )
        f = self._asof_scd2(f, dim_user, "order_timestamp", "user_id", "user_key")
        f = self._decode_key(f, dim_payment, "payment_method", "payment_method_key")
        f = self._decode_key(f, dim_status, "order_status", "order_status_key")
        return f.select(
            "order_date_key", "user_key", "payment_method_key", "order_status_key",
            "order_id", "order_timestamp", "order_gross_amount", "order_net_amount",
            "order_discount_amount", "order_line_count", "order_total_quantity",
            "source_total_matches_lines",
        )

    def sales(self, items: pl.DataFrame, orders: pl.DataFrame,
              dim_user: pl.DataFrame, dim_product: pl.DataFrame,
              dim_payment: pl.DataFrame) -> pl.DataFrame:
        order_ctx = orders.select(
            "order_id", "user_id", "order_timestamp", "order_status", "payment_method"
        )
        f = items.join(order_ctx, on="order_id", how="inner").with_columns(
            order_date_key=Expressions.date_key(pl.col("order_timestamp")),
        )
        f = self._asof_scd2(f, dim_user, "order_timestamp", "user_id", "user_key")
        f = self._asof_scd2(
            f, dim_product, "order_timestamp", "product_id", "product_key", fill=KEYS.UNKNOWN
        )
        f = self._decode_key(f, dim_payment, "payment_method", "payment_method_key")
        return f.select(
            "order_date_key", "user_key", "product_key", "payment_method_key",
            "order_status", "order_id", "order_item_id", "order_timestamp",
            "quantity", "unit_price", "gross_sales_amount", "discount_amount",
            "net_sales_amount",
        )

    def returns(self, returns: pl.DataFrame, dim_user: pl.DataFrame,
                dim_product: pl.DataFrame, dim_reason: pl.DataFrame) -> pl.DataFrame:
        f = returns.with_columns(
            return_date_key=Expressions.date_key(pl.col("return_timestamp")),
            order_date_key=pl.when(pl.col("order_timestamp").is_not_null())
            .then(Expressions.date_key(pl.col("order_timestamp")))
            .otherwise(pl.lit(KEYS.UNKNOWN, dtype=pl.Int32)),
            return_count=pl.lit(1, dtype=pl.Int32),
            _product_asof=pl.coalesce(["order_timestamp", "return_timestamp"]),
        ).with_columns(
            return_lag_days=(
                pl.col("return_timestamp") - pl.col("order_timestamp")
            ).dt.total_days().cast(pl.Int32),
        )
        f = self._asof_scd2(f, dim_user, "return_timestamp", "user_id", "user_key")
        f = self._asof_scd2(
            f, dim_product, "_product_asof", "product_id", "product_key", fill=KEYS.UNKNOWN
        )
        f = self._decode_key(f, dim_reason, "return_reason", "return_reason_key")
        return f.select(
            "return_date_key", "order_date_key", "user_key", "product_key",
            "return_reason_key", "return_refund_id", "order_id", "order_item_id",
            "return_timestamp", "order_timestamp", "refund_amount",
            "return_lag_days", "return_count",
        )

    def clickstream(self, events: pl.DataFrame, dim_user: pl.DataFrame,
                    dim_product: pl.DataFrame, dim_event_type: pl.DataFrame,
                    dim_device: pl.DataFrame) -> pl.DataFrame:
        f = events.with_columns(
            event_date_key=Expressions.date_key(pl.col("event_timestamp")),
            event_hour=pl.col("event_timestamp").dt.hour().cast(pl.Int32),
            event_count=pl.lit(1, dtype=pl.Int32),
        )
        f = self._asof_scd2(f, dim_user, "event_timestamp", "user_id", "user_key")
        f = self._asof_scd2(
            f, dim_product, "event_timestamp", "product_id", "product_key", fill=KEYS.UNKNOWN
        )
        f = self._decode_key(f, dim_event_type, "event_type", "event_type_key")
        f = self._decode_key(f, dim_device, "device", "device_key")
        return f.select(
            "event_date_key", "event_hour", "user_key", "product_key",
            "event_type_key", "device_key", "session_id", "order_id", "event_key",
            "event_timestamp", "event_count", "quantity", "cart_value", "duration_sec",
        )
