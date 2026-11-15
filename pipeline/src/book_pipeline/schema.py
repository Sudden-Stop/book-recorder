"""Table schemas from `schema/*.yaml` (the single source for code, drift checks and the dataset card)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pyarrow as pa
import yaml

TABLES = ("book_deltas", "book_snapshots", "trades", "markets", "coverage", "pairs")

PRICE = pa.decimal128(10, 6)
SIZE = pa.decimal128(24, 6)
TIMESTAMP = pa.timestamp("ms", tz="UTC")
LEVEL = pa.struct([pa.field("price", PRICE, nullable=False), pa.field("size", SIZE, nullable=False)])
TOKEN = pa.struct([pa.field("token_id", pa.string()), pa.field("outcome", pa.string())])

TYPES: dict[str, pa.DataType] = {
    "string": pa.string(),
    "bool": pa.bool_(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "price": PRICE,
    "size": SIZE,
    "timestamp": TIMESTAMP,
    "list<string>": pa.list_(pa.string()),
    "levels": pa.list_(LEVEL),
    "tokens": pa.list_(TOKEN),
}


class SchemaDrift(Exception):
    """A curated file, or a schema YAML, does not match what the pipeline expects."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    meaning: str
    sensitive: str | None = None


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    columns: tuple[Column, ...]
    order: tuple[str, ...]

    @property
    def arrow(self) -> pa.Schema:
        return pa.schema([pa.field(c.name, TYPES[c.type], nullable=c.nullable) for c in self.columns])

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


def schema_dir() -> Path:
    env = os.environ.get("BOOK_SCHEMA_DIR")
    if env:
        return Path(env)
    # pipeline/src/book_pipeline/schema.py -> repo root / schema
    return Path(__file__).resolve().parents[3] / "schema"


@lru_cache(maxsize=None)
def load(name: str, directory: str | None = None) -> Table:
    path = Path(directory) if directory else schema_dir()
    doc = yaml.safe_load((path / f"{name}.yaml").read_text(encoding="utf-8"))
    if doc.get("table") != name:
        raise SchemaDrift(f"{name}.yaml declares table {doc.get('table')!r}")
    cols = []
    seen = set()
    for c in doc["columns"]:
        if c["type"] not in TYPES:
            raise SchemaDrift(f"{name}.{c['name']}: unknown type {c['type']!r}")
        if c["name"] in seen:
            raise SchemaDrift(f"{name}.{c['name']}: duplicate column")
        if not str(c.get("meaning") or "").strip():
            raise SchemaDrift(f"{name}.{c['name']}: missing meaning")
        seen.add(c["name"])
        cols.append(Column(c["name"], c["type"], bool(c.get("nullable", True)), str(c["meaning"]).strip(),
                           c.get("sensitive")))
    return Table(name, " ".join(str(doc.get("description", "")).split()), tuple(cols), tuple(doc.get("order") or ()))


def load_all() -> dict[str, Table]:
    return {t: load(t) for t in TABLES}


def diff(table: Table, actual: pa.Schema, allow_missing: set[str] = frozenset()) -> list[str]:
    """Differences between a file's schema and the declared one (empty list = no drift)."""
    problems = []
    expected = table.arrow
    exp_names = expected.names
    act_names = actual.names
    for name in exp_names:
        if name not in act_names and name not in allow_missing:
            problems.append(f"{table.name}: missing column {name}")
    for name in act_names:
        if name not in exp_names:
            problems.append(f"{table.name}: unexpected column {name}")
    for name in exp_names:
        if name not in act_names:
            continue
        e, a = expected.field(name), actual.field(name)
        if not _same_type(e.type, a.type):
            problems.append(f"{table.name}.{name}: type {a.type} != declared {e.type}")
        if e.nullable != a.nullable:
            problems.append(f"{table.name}.{name}: nullable={a.nullable} != declared {e.nullable}")
    present = [n for n in act_names if n in exp_names]
    if present != [n for n in exp_names if n in act_names]:
        problems.append(f"{table.name}: column order {present} differs from declared order")
    return problems


def _same_type(e: pa.DataType, a: pa.DataType) -> bool:
    # Parquet round trips rename list items ("item" vs "element") and drop struct-field nullability flags
    # on read in some versions; compare structure, not field names of list items.
    if pa.types.is_list(e) and pa.types.is_list(a):
        return _same_type(e.value_type, a.value_type)
    if pa.types.is_struct(e) and pa.types.is_struct(a):
        return [f.name for f in e] == [f.name for f in a] and all(
            _same_type(fe.type, fa.type) for fe, fa in zip(e, a))
    return e.equals(a)
