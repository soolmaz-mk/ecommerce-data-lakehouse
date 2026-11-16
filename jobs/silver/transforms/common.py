# Shared pieces for all transforms: sentinel keys, allowed values, hashing.
# Hashing makes surrogate keys that stay the same between runs.
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class Keys:
    UNKNOWN: int = -1
    LATE: int = -2
    UNKNOWN_LABEL: str = "Unknown"
    LATE_LABEL: str = "Late Arriving"
    SENTINEL_HASH: str = "sentinel"
    EPOCH: dt.datetime = dt.datetime(1970, 1, 1)
    FAR_FUTURE: dt.datetime = dt.datetime(9999, 12, 31)


@dataclass(frozen=True)
class Domains:
    loyalty_tiers: tuple = ("Bronze", "Silver", "Gold", "Platinum")
    payment_methods: tuple = (
        "bank_transfer", "shaparak", "debit_card", "credit_card",
        "google_pay", "apple_pay", "paypal", "cash_on_delivery",
    )
    order_statuses: tuple = ("created", "shipped", "delivered", "cancelled", "refunded")
    event_types: tuple = (
        "page_view", "product_search", "cart_view", "add_to_cart", "wishlist_add",
        "remove_from_cart", "checkout_start", "payment_attempt", "order_complete",
        "review_submit",
    )
    devices: tuple = ("mobile", "desktop", "tablet")
    return_reasons: tuple = (
        "defective", "damaged", "wrong_item", "not_as_described",
        "size_issue", "late_delivery", "customer_changed_mind",
    )


KEYS = Keys()
DOMAINS = Domains()


class Hashing:

    SEPARATOR = "|"
    NULL_TOKEN = "<null>"
    SURROGATE_SEED = 0
    CONTENT_SEED = 1
    POSITIVE_MASK = 0x7FFFFFFFFFFFFFFF

    @classmethod
    def _concat(cls, cols: list[str | pl.Expr]) -> pl.Expr:
        exprs = [pl.col(c) if isinstance(c, str) else c for c in cols]
        return pl.concat_str(
            [e.cast(pl.Utf8).fill_null(cls.NULL_TOKEN) for e in exprs],
            separator=cls.SEPARATOR,
        )

    @classmethod
    def surrogate(cls, *cols: str | pl.Expr) -> pl.Expr:
        return (
            cls._concat(list(cols)).hash(seed=cls.SURROGATE_SEED) & cls.POSITIVE_MASK
        ).cast(pl.Int64)

    @classmethod
    def content(cls, cols: list[str]) -> pl.Expr:
        return cls._concat(list(cols)).hash(seed=cls.CONTENT_SEED).cast(pl.Utf8)


class Expressions:

    @staticmethod
    def map_to_domain(col: str, allowed, default: str = KEYS.UNKNOWN_LABEL) -> pl.Expr:
        return (
            pl.when(pl.col(col).is_in(list(allowed)))
            .then(pl.col(col))
            .otherwise(pl.lit(default))
        )

    @staticmethod
    def date_key(ts: pl.Expr) -> pl.Expr:
        return ts.dt.strftime("%Y%m%d").cast(pl.Int32)
