"""Shared state and value helpers for venue parsers."""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import orjson

from ..coverage import CoverageTracker
from ..writer import TableWriter


@dataclass
class Out:
    """Where a venue parser puts its rows, plus what it learns along the way."""
    venue: str
    writers: dict[str, TableWriter]
    coverage: CoverageTracker
    token_market: dict[str, str] = field(default_factory=dict)
    stats: Counter = field(default_factory=Counter)
    unknown_event_types: Counter = field(default_factory=Counter)
    new_keys: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))

    def row(self, table: str, row: dict) -> None:
        row.setdefault("venue", self.venue)
        self.writers[table].append(row)
        self.stats[f"rows.{table}"] += 1

    def keys(self, kind: str, rec: dict, known: frozenset[str]) -> None:
        extra = rec.keys() - known
        if extra:
            self.new_keys[kind].update(extra)


def to_ms(value, seconds_if_small: bool = False) -> int | None:
    """Venue timestamp (int/float/str, ms or s) to int ms. Seconds are detected below 1e11 when allowed."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return iso_ms(value)
    if v != v:
        return None
    if seconds_if_small and abs(v) < 1e11:
        v *= 1000.0
    return int(v)


def iso_ms(value) -> int | None:
    if not value or not isinstance(value, str):
        return None
    s = value.strip().replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if re.search(r"[+-]\d\d$", s):
        s = s + ":00"  # "+00" -> "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def first(rec: dict, *keys):
    for k in keys:
        v = rec.get(k)
        if v is not None and v != "":
            return v
    return None


def as_list(value):
    """A list field that the venue may send as a JSON string (Gamma `clobTokenIds`, `outcomes`)."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = orjson.loads(value)
        except orjson.JSONDecodeError:
            return None
    return value if isinstance(value, list) else None


def as_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    return None


def as_int(value) -> int | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def as_float(value) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def dumps(value) -> str:
    return orjson.dumps(value).decode()


def text(value) -> str | None:
    if value is None:
        return None
    s = str(value)
    return s if s else None
