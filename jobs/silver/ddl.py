# Reads the CREATE TABLE files in model/ and turns them into Iceberg tables.
# The .sql files are the only place a column is defined.
from __future__ import annotations

import re
from pathlib import Path

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


class DDLError(ValueError):
    pass


class DDLParser:

    MODEL_DIR = Path(__file__).parent / "model"
    PARTITION_FIELD_ID_START = 1000

    SCALAR_TYPES: dict[str, IcebergType] = {
        "INT": IntegerType(),
        "INTEGER": IntegerType(),
        "BIGINT": LongType(),
        "LONG": LongType(),
        "STRING": StringType(),
        "BOOLEAN": BooleanType(),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }

    _DECIMAL = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
    _CREATE = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?P<ns>[\w]+)\.)?(?P<table>[\w]+)\s*\(",
        re.IGNORECASE,
    )
    _PARTITION = re.compile(r"PARTITIONED\s+BY\s*\(([^)]*)\)", re.IGNORECASE)
    _COMMENT_CLAUSE = re.compile(r"\s+COMMENT\s+'(?:[^']|'')*'", re.IGNORECASE)
    _NOT_NULL = re.compile(r"\s+NOT\s+NULL\b", re.IGNORECASE)

    @classmethod
    def _strip_line_comments(cls, sql: str) -> str:
        out = []
        for line in sql.splitlines():
            in_str = False
            cut = len(line)
            i = 0
            while i < len(line):
                ch = line[i]
                if ch == "'":
                    in_str = not in_str
                elif not in_str and ch == "-" and line[i:i + 2] == "--":
                    cut = i
                    break
                i += 1
            out.append(line[:cut])
        return "\n".join(out)

    @classmethod
    def _split_top_level(cls, body: str) -> list[str]:
        parts, depth, in_str, cur = [], 0, False, []
        for ch in body:
            if ch == "'":
                in_str = not in_str
            elif not in_str:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                elif ch == "," and depth == 0:
                    parts.append("".join(cur))
                    cur = []
                    continue
            cur.append(ch)
        if "".join(cur).strip():
            parts.append("".join(cur))
        return [p.strip() for p in parts if p.strip()]

    @classmethod
    def _match_column_block(cls, sql: str, open_paren: int) -> tuple[str, int]:
        depth, in_str = 0, False
        for i in range(open_paren, len(sql)):
            ch = sql[i]
            if ch == "'":
                in_str = not in_str
            elif not in_str:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        return sql[open_paren + 1:i], i + 1
        raise DDLError("unbalanced parentheses in the column list")

    @classmethod
    def _parse_type(cls, token: str) -> IcebergType:
        t = token.strip()
        dec = cls._DECIMAL.match(t)
        if dec:
            return DecimalType(int(dec.group(1)), int(dec.group(2)))
        key = t.upper()
        if key in cls.SCALAR_TYPES:
            return cls.SCALAR_TYPES[key]
        raise DDLError(
            f"unsupported column type {t!r}; supported: "
            f"{', '.join(sorted(cls.SCALAR_TYPES))}, DECIMAL(p,s)"
        )

    @classmethod
    def _parse_column(cls, clause: str, field_id: int) -> NestedField:
        text = cls._COMMENT_CLAUSE.sub("", clause).strip()
        required = bool(cls._NOT_NULL.search(text))
        text = cls._NOT_NULL.sub("", text).strip()
        bits = text.split(None, 1)
        if len(bits) != 2:
            raise DDLError(f"cannot parse column definition {clause.strip()!r}")
        return NestedField(
            field_id=field_id,
            name=bits[0],
            field_type=cls._parse_type(bits[1].strip()),
            required=required,
        )

    @classmethod
    def parse(cls, sql: str) -> tuple[str, Schema, PartitionSpec]:
        cleaned = cls._strip_line_comments(sql)
        m = cls._CREATE.search(cleaned)
        if not m:
            raise DDLError("no CREATE TABLE statement found")

        body, after = cls._match_column_block(cleaned, m.end() - 1)
        columns = cls._split_top_level(body)
        if not columns:
            raise DDLError(f"table {m.group('table')!r} declares no columns")

        fields = [cls._parse_column(c, i) for i, c in enumerate(columns, start=1)]
        names = [f.name for f in fields]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise DDLError(f"table {m.group('table')!r} has duplicate columns: {sorted(dupes)}")

        schema = Schema(*fields)
        spec = PartitionSpec()
        pm = cls._PARTITION.search(cleaned[after:])
        if pm:
            part_cols = [c.strip() for c in pm.group(1).split(",") if c.strip()]
            part_fields = []
            for offset, col in enumerate(part_cols):
                if col not in names:
                    raise DDLError(
                        f"table {m.group('table')!r} is PARTITIONED BY {col!r}, "
                        f"which is not one of its columns"
                    )
                part_fields.append(
                    PartitionField(
                        source_id=schema.find_field(col).field_id,
                        field_id=cls.PARTITION_FIELD_ID_START + offset,
                        transform=IdentityTransform(),
                        name=col,
                    )
                )
            spec = PartitionSpec(*part_fields)

        return m.group("table"), schema, spec

    @classmethod
    def parse_file(cls, path: Path) -> tuple[str, Schema, PartitionSpec]:
        try:
            name, schema, spec = cls.parse(path.read_text())
        except DDLError as exc:
            raise DDLError(f"{path.name}: {exc}") from exc
        if name != path.stem:
            raise DDLError(
                f"{path.name}: declares table {name!r}; the file name must match "
                f"the table name so the two cannot drift"
            )
        return name, schema, spec


class ModelRegistry:

    LOAD_ORDER = (
        "dim_date",
        "dim_user",
        "dim_product",
        "dim_category",
        "dim_payment_method",
        "dim_order_status",
        "dim_event_type",
        "dim_device",
        "dim_return_reason",
        "fact_orders",
        "fact_sales",
        "fact_clickstream",
        "fact_returns",
    )

    def __init__(self, model_dir: Path | None = None):
        self.model_dir = model_dir or DDLParser.MODEL_DIR
        self._tables = self._ordered(self._load())

    def _load(self) -> dict[str, tuple[Schema, PartitionSpec]]:
        files = sorted(self.model_dir.glob("*.sql"))
        if not files:
            raise DDLError(f"no .sql files found in {self.model_dir}")
        tables: dict[str, tuple[Schema, PartitionSpec]] = {}
        for path in files:
            name, schema, spec = DDLParser.parse_file(path)
            tables[name] = (schema, spec)
        return tables

    @classmethod
    def _ordered(cls, tables: dict) -> dict:
        known = [n for n in cls.LOAD_ORDER if n in tables]
        extra = sorted(n for n in tables if n not in cls.LOAD_ORDER)
        return {n: tables[n] for n in [*known, *extra]}

    @property
    def tables(self) -> dict[str, tuple[Schema, PartitionSpec]]:
        return self._tables

    def schema(self, name: str) -> Schema:
        return self._tables[name][0]

    def partition_spec(self, name: str) -> PartitionSpec:
        return self._tables[name][1]

    def names(self) -> list[str]:
        return list(self._tables)

    def __contains__(self, name: str) -> bool:
        return name in self._tables

    def __iter__(self):
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)


REGISTRY = ModelRegistry()
TABLES = REGISTRY.tables
