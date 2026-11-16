# Reads bronze Parquet files and reads/writes the temporary staging files.
# It also reads an Iceberg table into Polars.
from __future__ import annotations

import datetime as dt

import polars as pl

from jobs.silver.config import SilverConfig


class BronzeReader:

    def __init__(self, cfg: SilverConfig):
        self.cfg = cfg

    def _scan(self, glob: str) -> pl.LazyFrame:
        return pl.scan_parquet(
            glob,
            storage_options=self.cfg.polars_storage_options,
            hive_partitioning=self.cfg.bronze_hive_partitioning,
            extra_columns=self.cfg.bronze_extra_columns,
            missing_columns=self.cfg.bronze_missing_columns,
        )

    def read(self, topic: str) -> pl.LazyFrame:
        return self._scan(f"{self.cfg.bronze_topic_uri(topic)}/**/*.parquet")

    def read_window(self, topic: str, lo: dt.date, hi: dt.date) -> pl.LazyFrame:
        event_date = pl.col("event_date").cast(pl.Utf8).str.to_date("%Y-%m-%d")
        return self.read(topic).filter(
            (event_date >= pl.lit(lo)) & (event_date <= pl.lit(hi))
        )


class StagingStore:

    def __init__(self, cfg: SilverConfig):
        self.cfg = cfg

    def _path(self, uri: str) -> str:
        return f"{uri}/{self.cfg.staging_filename}"

    def write(self, df: pl.DataFrame, uri: str) -> str:
        target = self._path(uri)
        df.write_parquet(
            target,
            storage_options=self.cfg.polars_storage_options,
            compression=self.cfg.staging_compression,
        )
        return target

    def read(self, uri: str) -> pl.LazyFrame:
        return pl.scan_parquet(
            self._path(uri), storage_options=self.cfg.polars_storage_options
        )

    def write_staged(self, source: str, interval_nodash: str, df: pl.DataFrame) -> str:
        return self.write(df, self.cfg.staging_uri(interval_nodash, source))

    def write_quarantine(self, source: str, interval_nodash: str, df: pl.DataFrame) -> str:
        return self.write(df, self.cfg.quarantine_uri(interval_nodash, source))

    def read_staged(self, source: str, interval_nodash: str) -> pl.DataFrame:
        return self.read(self.cfg.staging_uri(interval_nodash, source)).collect()


class IcebergReader:

    def __init__(self, cfg: SilverConfig):
        self.cfg = cfg

    def scan(self, table, snapshot_id: int | None = None) -> pl.LazyFrame:
        return pl.from_arrow(table.scan(snapshot_id=snapshot_id).to_arrow()).lazy()

    def read(self, table, snapshot_id: int | None = None) -> pl.DataFrame:
        return self.scan(table, snapshot_id).collect()
