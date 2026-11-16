# The 8 data quality checks that run before publishing.
# If one fails, the run stops and main is never updated.
from __future__ import annotations

import polars as pl

from jobs.silver.catalog import IcebergCatalog
from jobs.silver.config import SilverConfig, load_config
from jobs.silver.io import IcebergReader
from jobs.silver.transforms.common import KEYS


class DQError(AssertionError):
    pass


class DataQualityGate:

    GRAIN_KEYS = {
        "fact_sales": ["order_id", "order_item_id"],
        "fact_orders": ["order_id"],
        "fact_clickstream": ["event_key"],
        "fact_returns": ["return_refund_id"],
    }

    FOREIGN_KEYS = {
        "fact_sales": ["order_date_key", "user_key", "product_key", "payment_method_key"],
        "fact_orders": ["order_date_key", "user_key", "payment_method_key", "order_status_key"],
        "fact_clickstream": [
            "event_date_key", "user_key", "product_key", "event_type_key", "device_key",
        ],
        "fact_returns": [
            "return_date_key", "order_date_key", "user_key", "product_key", "return_reason_key",
        ],
    }

    SCD2_DIMS = [("dim_user", "user_id"), ("dim_product", "product_id")]

    def __init__(self, cfg: SilverConfig | None = None, branch: str | None = None,
                 catalog: IcebergCatalog | None = None):
        self.cfg = cfg or load_config()
        self.branch = branch
        self.catalog = catalog or IcebergCatalog(self.cfg)
        self.reader = IcebergReader(self.cfg)

    def read(self, name: str) -> pl.DataFrame:
        table = self.catalog.load(name)
        snapshot = None
        if self.branch and self.branch in table.metadata.refs:
            snapshot = table.metadata.refs[self.branch].snapshot_id
        return self.reader.read(table, snapshot)

    @staticmethod
    def _fail(msg: str) -> None:
        raise DQError(msg)

    def unique_grain(self) -> dict:
        report = {}
        for name, keys in self.GRAIN_KEYS.items():
            df = self.read(name)
            dups = df.height - df.select(keys).unique().height
            report[name] = dups
            if dups > 0:
                self._fail(f"{name}: {dups} duplicate rows on grain {keys}")
        return report

    def no_null_fks(self) -> dict:
        report = {}
        for name, cols in self.FOREIGN_KEYS.items():
            df = self.read(name)
            for c in cols:
                nulls = df.select(pl.col(c).is_null().sum()).item()
                if nulls > 0:
                    self._fail(f"{name}.{c}: {nulls} NULL FKs")
            late = int(
                df.select(
                    pl.sum_horizontal([pl.col(c) == KEYS.LATE for c in cols]).sum()
                ).item() or 0
            )
            report[name] = {"late_-2": late}
        return report

    def measure_sanity(self) -> dict:
        df = self.read("fact_sales")
        tol = self.cfg.dq_reconciliation_tolerance
        bad = df.filter(
            (pl.col("net_sales_amount").cast(pl.Float64)
             > pl.col("gross_sales_amount").cast(pl.Float64) + tol)
            | (pl.col("discount_amount").cast(pl.Float64) < -tol)
            | (pl.col("quantity") <= 0)
            | (pl.col("net_sales_amount").cast(pl.Float64) < -tol)
        )
        if bad.height:
            self._fail(
                f"fact_sales: {bad.height} rows violate net<=gross / discount>=0 / qty>0 / net>=0"
            )
        rets = self.read("fact_returns")
        bad_rets = rets.filter(pl.col("refund_amount").cast(pl.Float64) < -tol)
        if bad_rets.height:
            self._fail(f"fact_returns: {bad_rets.height} rows with a negative refund_amount")
        return {"fact_sales_rows": df.height, "fact_returns_rows": rets.height}

    def reconciliation(self) -> dict:
        df = self.read("fact_orders")
        scoped = df.filter(pl.col("order_line_count") > 0)
        if scoped.height == 0:
            return {"orders_with_lines": 0, "mismatch_frac": 0.0}
        mismatches = scoped.filter(~pl.col("source_total_matches_lines")).height
        frac = mismatches / scoped.height
        limit = self.cfg.dq_max_mismatch_frac
        if frac > limit:
            self._fail(
                f"reconciliation: {mismatches}/{scoped.height} ({frac:.1%}) orders WITH LINES "
                f"where total != sum(line gross) > {limit:.0%} allowed"
            )
        return {"orders_with_lines": scoped.height, "mismatches": mismatches,
                "mismatch_frac": round(frac, 4)}

    def order_completeness(self) -> dict:
        df = self.read("fact_orders")
        if df.height == 0:
            return {"orders": 0, "missing_lines_frac": 0.0}
        missing = df.filter(pl.col("order_line_count") == 0).height
        frac = missing / df.height
        limit = self.cfg.dq_max_missing_lines_frac
        if frac > limit:
            self._fail(
                f"completeness: {missing}/{df.height} ({frac:.1%}) orders have NO line "
                f"items > {limit:.0%} allowed -- the order_items feed may be broken"
            )
        return {"orders": df.height, "missing_lines": missing,
                "missing_lines_frac": round(frac, 4)}

    def scd2_invariants(self) -> dict:
        report = {}
        for name, nk in self.SCD2_DIMS:
            df = (
                self.read(name)
                .filter(pl.col(nk) != KEYS.UNKNOWN_LABEL)
                .filter(pl.col(nk) != KEYS.LATE_LABEL)
            )
            multi = (
                df.filter(pl.col("is_current")).group_by(nk).len()
                .filter(pl.col("len") > 1).height
            )
            if multi:
                self._fail(f"{name}: {multi} natural keys with >1 is_current row")
            overlaps = (
                df.sort([nk, "valid_from"])
                .with_columns(_next_from=pl.col("valid_from").shift(-1).over(nk))
                .filter(pl.col("valid_to").is_not_null() & pl.col("_next_from").is_not_null())
                .filter(pl.col("_next_from") < pl.col("valid_to"))
                .height
            )
            if overlaps:
                self._fail(f"{name}: {overlaps} overlapping SCD2 windows")
            report[name] = {"rows": df.height}
        return report

    def returns_reference_sales(self) -> dict:
        df = self.read("fact_returns")
        if df.height == 0:
            return {"returns": 0, "unresolved_frac": 0.0}
        unresolved = df.filter(pl.col("order_date_key") == KEYS.UNKNOWN).height
        frac = unresolved / df.height
        limit = self.cfg.dq_max_returns_unresolved_order_frac
        if frac > limit:
            self._fail(
                f"fact_returns: {unresolved}/{df.height} ({frac:.1%}) returns could not "
                f"be matched to an order > {limit:.0%} allowed"
            )
        return {"returns": df.height, "unresolved": unresolved,
                "unresolved_frac": round(frac, 4)}

    def returns_resolve_products(self) -> dict:
        df = self.read("fact_returns")
        if df.height == 0:
            return {"returns": 0, "unresolved_frac": 0.0}
        unresolved = df.filter(pl.col("product_key") == KEYS.UNKNOWN).height
        frac = unresolved / df.height
        limit = self.cfg.dq_max_returns_unresolved_product_frac
        if frac > limit:
            self._fail(
                f"fact_returns: {unresolved}/{df.height} ({frac:.1%}) returns could not be "
                f"traced to a product > {limit:.0%} allowed -- the order_items feed has "
                f"likely broken, not just lagged"
            )
        return {"returns": df.height, "unresolved_products": unresolved,
                "unresolved_frac": round(frac, 4),
                "product_rate_trustworthy_over": round(1 - frac, 4)}

    CHECKS = (
        "unique_grain",
        "no_null_fks",
        "measure_sanity",
        "reconciliation",
        "order_completeness",
        "scd2_invariants",
        "returns_reference_sales",
        "returns_resolve_products",
    )

    def run_all(self) -> dict:
        return {name: getattr(self, name)() for name in self.CHECKS}
