# Talks to the Iceberg catalog: creates tables and writes data to them.
# It also casts a Polars frame to the exact table schema before writing.
from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.table import Table

from jobs.silver.config import SilverConfig, load_config
from jobs.silver.ddl import REGISTRY

MAIN_BRANCH = "main"


class IcebergCatalog:

    def __init__(self, cfg: SilverConfig | None = None, registry=None):
        self.cfg = cfg or load_config()
        self.registry = registry or REGISTRY
        self._catalog: Catalog | None = None

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog = load_catalog(
                self.cfg.catalog_name, **self.cfg.pyiceberg_catalog_properties
            )
        return self._catalog

    def identifier(self, name: str) -> str:
        return self.cfg.table_identifier(name)

    def ensure_namespace(self) -> None:
        try:
            self.catalog.create_namespace(self.cfg.namespace)
        except NamespaceAlreadyExistsError:
            pass

    def ensure_tables(self) -> list[str]:
        self.ensure_namespace()
        existing = {".".join(t) for t in self.catalog.list_tables(self.cfg.namespace)}
        created: list[str] = []
        for name, (schema, spec) in self.registry.tables.items():
            ident = self.identifier(name)
            if ident in existing:
                continue
            self.catalog.create_table(identifier=ident, schema=schema, partition_spec=spec)
            created.append(name)
        return created

    def load(self, name: str) -> Table:
        return self.catalog.load_table(self.identifier(name))

    def exists(self, name: str) -> bool:
        return self.catalog.table_exists(self.identifier(name))

    @staticmethod
    def is_empty(table: Table) -> bool:
        return table.metadata.current_snapshot_id is None

    @staticmethod
    def to_arrow(table: Table, df) -> pa.Table:
        target = table.schema().as_arrow()
        return df.select([f.name for f in target]).to_arrow().cast(target)

    def append(self, table: Table, df, branch: str = MAIN_BRANCH) -> None:
        table.append(self.to_arrow(table, df), branch=branch)

    def overwrite(self, table: Table, df, branch: str = MAIN_BRANCH) -> None:
        table.overwrite(self.to_arrow(table, df), branch=branch)

    def dynamic_overwrite(self, table: Table, df, branch: str = MAIN_BRANCH) -> None:
        table.dynamic_partition_overwrite(self.to_arrow(table, df), branch=branch)
