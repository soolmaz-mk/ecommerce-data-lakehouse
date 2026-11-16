# Cleans each bronze source: removes duplicates and separates bad rows.
# Bad rows go to quarantine with a reason, they are never deleted.
from __future__ import annotations

import datetime as dt

import polars as pl

from jobs.silver.config import SilverConfig
from jobs.silver.io import BronzeReader
from jobs.silver.transforms.common import DOMAINS, KEYS, Expressions, Hashing

StagedPair = tuple[pl.DataFrame, pl.DataFrame]


class StagingTransforms:

    EMPTY_QUARANTINE = pl.DataFrame(schema={"reject_reason": pl.Utf8})

    TOPIC_USERS = "transactional.users"
    TOPIC_PRODUCTS = "transactional.products"
    TOPIC_CATEGORIES = "transactional.categories"
    TOPIC_PRICE_HISTORY = "transactional.product_price_history"
    TOPIC_ORDERS = "transactional.orders"
    TOPIC_ORDER_ITEMS = "transactional.order_items"
    TOPIC_RETURNS = "transactional.returns_refunds"
    TOPIC_EVENTS = "behavioral.events"

    DEDUPE_TIMESTAMP = "kafka_ingest_timestamp"

    def __init__(self, cfg: SilverConfig, reader: BronzeReader | None = None):
        self.cfg = cfg
        self.reader = reader or BronzeReader(cfg)

    @classmethod
    def _dedupe_latest(cls, lf: pl.LazyFrame, key: list[str]) -> pl.LazyFrame:
        return lf.sort(cls.DEDUPE_TIMESTAMP).unique(
            subset=key, keep="last", maintain_order=True
        )

    def users(self) -> StagedPair:
        lf = self._dedupe_latest(self.reader.read(self.TOPIC_USERS), ["user_id"])
        good = (
            lf.filter(pl.col("user_id").is_not_null())
            .with_columns(
                city=pl.col("location").str.split(", ").list.get(0, null_on_oob=True).str.strip_chars(),
                province=pl.col("location").str.split(", ").list.get(1, null_on_oob=True).str.strip_chars(),
                loyalty_tier=Expressions.map_to_domain("loyalty_tier", DOMAINS.loyalty_tiers),
            )
            .select(
                "user_id",
                pl.col("username").str.strip_chars(),
                pl.col("email").str.strip_chars(),
                "signup_date",
                "city",
                "province",
                "loyalty_tier",
            )
            .collect()
        )
        return good, self.EMPTY_QUARANTINE

    def products(self) -> StagedPair:
        lf = self._dedupe_latest(self.reader.read(self.TOPIC_PRODUCTS), ["product_id"])
        good = (
            lf.filter(pl.col("product_id").is_not_null())
            .select(
                "product_id",
                pl.col("name").str.strip_chars().alias("product_name"),
                pl.col("price").cast(pl.Decimal(10, 2)).alias("list_price"),
            )
            .collect()
        )
        return good, self.EMPTY_QUARANTINE

    def categories(self) -> StagedPair:
        cats = (
            self.reader.read(self.TOPIC_CATEGORIES)
            .unique(subset=["category_id"], keep="last")
            .select(
                pl.col("category_id").cast(pl.Utf8).str.strip_chars(),
                pl.col("name").str.strip_chars().alias("category_name"),
                pl.col("parent_category_id").cast(pl.Utf8).str.strip_chars(),
            )
            .collect()
        )
        lut = cats.select(["category_id", "category_name", "parent_category_id"])
        p1 = lut.rename({
            "category_id": "p1_id", "category_name": "p1_name", "parent_category_id": "p1_parent",
        })
        p2 = lut.rename({
            "category_id": "p2_id", "category_name": "p2_name", "parent_category_id": "p2_parent",
        })

        joined = (
            cats.join(p1, left_on="parent_category_id", right_on="p1_id", how="left")
            .join(p2, left_on="p1_parent", right_on="p2_id", how="left")
        )
        good = joined.select(
            "category_id",
            "category_name",
            "parent_category_id",
            category_top=pl.coalesce(["p2_name", "p1_name", "category_name"]),
            category_sub=pl.when(pl.col("p2_name").is_not_null())
            .then(pl.col("p1_name"))
            .when(pl.col("p1_name").is_not_null())
            .then(pl.col("category_name"))
            .otherwise(pl.lit(None)),
            category_leaf=pl.when(pl.col("p2_name").is_not_null())
            .then(pl.col("category_name"))
            .when(pl.col("p1_name").is_not_null())
            .then(pl.col("category_name"))
            .otherwise(pl.lit(None)),
        )
        return good, self.EMPTY_QUARANTINE

    def price_history(self) -> StagedPair:
        ph = (
            self.reader.read(self.TOPIC_PRICE_HISTORY)
            .unique(subset=["price_history_id"], keep="last")
            .select(
                pl.col("product_id"),
                pl.col("price").cast(pl.Decimal(10, 2)).alias("list_price"),
                pl.col("valid_from").cast(pl.Datetime("us")),
                pl.col("price_history_id"),
            )
            .collect()
        )
        ph = ph.sort(["product_id", "valid_from", "price_history_id"])
        ph = ph.with_columns(
            _prev=pl.col("list_price").shift(1).over("product_id")
        ).filter((pl.col("_prev").is_null()) | (pl.col("_prev") != pl.col("list_price")))
        ph = (
            ph.with_columns(valid_to=pl.col("valid_from").shift(-1).over("product_id"))
            .with_columns(is_current=pl.col("valid_to").is_null())
            .drop(["_prev", "price_history_id"])
        )

        prods = (
            self.reader.read(self.TOPIC_PRODUCTS)
            .unique(subset=["product_id"], keep="last")
            .select("product_id", pl.col("price").cast(pl.Decimal(10, 2)).alias("list_price"))
            .collect()
        )
        missing = prods.join(ph.select("product_id").unique(), on="product_id", how="anti")
        seed = missing.with_columns(
            valid_from=pl.lit(KEYS.EPOCH, dtype=pl.Datetime("us")),
            valid_to=pl.lit(None, dtype=pl.Datetime("us")),
            is_current=pl.lit(True),
        )
        good = pl.concat([ph, seed.select(ph.columns)], how="vertical")
        return good, self.EMPTY_QUARANTINE

    def orders(self, lo: dt.date, hi: dt.date) -> StagedPair:
        lf = self._dedupe_latest(
            self.reader.read_window(self.TOPIC_ORDERS, lo, hi), ["order_id"]
        )
        frame = lf.with_columns(
            order_timestamp=pl.col("timestamp").cast(pl.Datetime("us")),
            order_status=Expressions.map_to_domain("status", DOMAINS.order_statuses),
            payment_method=Expressions.map_to_domain("payment_method", DOMAINS.payment_methods),
        ).collect()

        bad = frame.filter(
            pl.col("order_id").is_null()
            | pl.col("user_id").is_null()
            | pl.col("order_timestamp").is_null()
            | (pl.col("total").cast(pl.Float64) < 0)
        ).with_columns(reject_reason=pl.lit("null_key_or_negative_total"))
        good = frame.join(bad.select("order_id"), on="order_id", how="anti").select(
            "order_id",
            "user_id",
            "order_timestamp",
            pl.col("total").cast(pl.Decimal(18, 2)).alias("order_gross_amount"),
            "order_status",
            "payment_method",
        )
        return good, bad

    def order_items(self) -> StagedPair:
        lf = self._dedupe_latest(
            self.reader.read(self.TOPIC_ORDER_ITEMS), ["order_item_id"]
        )
        frame = lf.select(
            "order_item_id",
            "order_id",
            "product_id",
            pl.col("quantity").cast(pl.Int32),
            pl.col("unit_price").cast(pl.Decimal(10, 2)),
            pl.col("item_total_amount").cast(pl.Decimal(18, 2)),
        ).collect()

        bad = frame.filter(
            pl.col("order_item_id").is_null()
            | pl.col("order_id").is_null()
            | (pl.col("quantity") <= 0)
            | (pl.col("unit_price").cast(pl.Float64) < 0)
        ).with_columns(reject_reason=pl.lit("null_key_or_bad_measure"))
        good = (
            frame.join(bad.select("order_item_id"), on="order_item_id", how="anti")
            .with_columns(
                gross_sales_amount=(pl.col("quantity") * pl.col("unit_price")).cast(pl.Decimal(18, 2)),
            )
            .with_columns(
                net_sales_amount=pl.col("item_total_amount"),
                discount_amount=(
                    pl.col("gross_sales_amount") - pl.col("item_total_amount")
                ).cast(pl.Decimal(18, 2)),
            )
            .select(
                "order_item_id", "order_id", "product_id", "quantity", "unit_price",
                "gross_sales_amount", "discount_amount", "net_sales_amount",
            )
        )
        return good, bad

    def returns(self) -> StagedPair:
        rets = self._dedupe_latest(
            self.reader.read(self.TOPIC_RETURNS), ["return_refund_id"]
        ).with_columns(
            return_timestamp=pl.col("return_timestamp").cast(pl.Datetime("us")),
            return_reason=Expressions.map_to_domain("return_reason", DOMAINS.return_reasons),
            refund_amount=pl.col("refund_amount").cast(pl.Decimal(18, 2)),
        ).collect()

        bad = rets.filter(
            pl.col("return_refund_id").is_null()
            | pl.col("order_id").is_null()
            | pl.col("return_timestamp").is_null()
            | (pl.col("refund_amount").cast(pl.Float64) < 0)
        ).with_columns(reject_reason=pl.lit("null_key_or_negative_refund"))

        good = rets.join(bad.select("return_refund_id"), on="return_refund_id", how="anti")

        order_ctx = (
            self._dedupe_latest(self.reader.read(self.TOPIC_ORDERS), ["order_id"])
            .select(
                "order_id",
                "user_id",
                pl.col("timestamp").cast(pl.Datetime("us")).alias("order_timestamp"),
            )
            .collect()
        )
        item_ctx = (
            self._dedupe_latest(self.reader.read(self.TOPIC_ORDER_ITEMS), ["order_item_id"])
            .select("order_item_id", "product_id")
            .collect()
        )

        good = (
            good.join(order_ctx, on="order_id", how="left")
            .join(item_ctx, on="order_item_id", how="left")
            .select(
                "return_refund_id", "order_id", "order_item_id", "user_id", "product_id",
                "return_timestamp", "order_timestamp", "refund_amount", "return_reason",
            )
        )
        return good, bad.select(["return_refund_id", "order_id", "reject_reason"])

    def events(self, lo: dt.date, hi: dt.date) -> StagedPair:
        lf = self.reader.read_window(self.TOPIC_EVENTS, lo, hi)
        frame = lf.with_columns(
            event_timestamp=pl.col("timestamp").cast(pl.Datetime("us")),
            device=Expressions.map_to_domain("device", DOMAINS.devices),
            event_type=Expressions.map_to_domain("event_type", DOMAINS.event_types),
        ).with_columns(
            event_key=Hashing.content(
                ["user_id", "session_id", "event_timestamp", "event_type", "product_id"]
            ),
        ).collect()

        bad = frame.filter(
            pl.col("user_id").is_null()
            | pl.col("session_id").is_null()
            | pl.col("event_timestamp").is_null()
        ).with_columns(reject_reason=pl.lit("null_mandatory_field"))
        good = (
            frame.join(bad.select("event_key"), on="event_key", how="anti")
            .unique(subset=["event_key"], keep="first")
            .select(
                "event_key", "user_id", "session_id", "event_timestamp", "event_type",
                "device", "product_id", "order_id",
                pl.col("quantity").cast(pl.Int32),
                pl.col("cart_value").cast(pl.Decimal(18, 2)),
                pl.col("duration_sec").cast(pl.Int32),
            )
        )
        return good, bad
