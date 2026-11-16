"""Buffered Parquet writer that converts plain Python rows into the declared Arrow schema."""
from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import LEVEL, PRICE, SIZE, Table
from .storage import Root

_Q = Decimal("0.000001")
ROUNDED = {"count": 0}


def dec(x) -> str | None:
    """Normalise a venue number to a decimal string with at most 6 fractional digits (None stays None).

    Strings are kept digit-for-digit when they fit; binary floats (activity feed) are rounded to 6 decimals.
    """
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        if x != x or x in (float("inf"), float("-inf")):
            return None
        return format(x, ".6f")
    s = str(x).strip()
    dot = s.find(".")
    if ("e" in s or "E" in s) or (dot >= 0 and len(s) - dot - 1 > 6):
        try:
            q = Decimal(s).quantize(_Q, rounding=ROUND_HALF_EVEN)
        except InvalidOperation:
            return None
        if "e" not in s and "E" not in s:
            ROUNDED["count"] += 1
        return format(q, "f")
    return s


def _levels(levels: list[tuple[str, str]] | None):
    if levels is None:
        return None
    return [{"price": p, "size": s} for p, s in levels]


class TableWriter:
    """Accumulates rows column-wise and writes zstd Parquet row groups of `batch_rows` rows.

    Decimal columns take decimal strings (see `dec`), timestamp columns take int milliseconds, `levels`
    columns take lists of (price, size) string pairs.
    """

    def __init__(self, table: Table, root: Root, path: str, batch_rows: int = 200_000):
        self.table = table
        self.schema = table.arrow
        self.root = root
        self.path = path
        self.batch_rows = batch_rows
        self.cols: dict[str, list] = {n: [] for n in self.schema.names}
        self.rows = 0
        self.total = 0
        self._writer: pq.ParquetWriter | None = None
        self._sink = None

    def append(self, row: dict) -> None:
        for name, buf in self.cols.items():
            buf.append(row.get(name))
        self.rows += 1
        if self.rows >= self.batch_rows:
            self.flush()

    def _array(self, field: pa.Field, values: list) -> pa.Array:
        t = field.type
        if t == PRICE or t == SIZE:
            return pa.array(values, pa.string()).cast(t)
        if pa.types.is_timestamp(t):
            return pa.array(values, pa.int64()).cast(t)
        if pa.types.is_list(t) and t.value_type == LEVEL:
            strs = [_levels(v) for v in values]
            flat_p, flat_s, offsets, valid = [], [], [0], []
            for v in strs:
                valid.append(v is not None)
                for lv in v or ():
                    flat_p.append(lv["price"])
                    flat_s.append(lv["size"])
                offsets.append(len(flat_p))
            struct = pa.StructArray.from_arrays(
                [pa.array(flat_p, pa.string()).cast(PRICE), pa.array(flat_s, pa.string()).cast(SIZE)],
                fields=list(LEVEL))
            mask = pa.array([not v for v in valid])
            return pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), struct, mask=mask, type=t)
        return pa.array(values, t)

    def flush(self) -> None:
        if self.rows == 0:
            return
        arrays = [self._array(f, self.cols[f.name]) for f in self.schema]
        batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
        if self._writer is None:
            self._open()
        self._writer.write_batch(batch)
        self.total += self.rows
        self.cols = {n: [] for n in self.schema.names}
        self.rows = 0

    def close(self) -> int:
        """Write remaining rows and move the file into place. An empty table still gets a file (0 rows)."""
        self.flush()
        if self._writer is None:
            self._open()
        self._writer.close()
        self._sink.close()
        if self._staged:
            if self.root.exists(self.path):
                self.root.fs.delete_file(self.path)
            self.root.fs.move(self.path + ".tmp", self.path)
        return self.total

    @property
    def _staged(self) -> bool:
        """Local files are written to `.tmp` and renamed, so a crash never leaves a half file in place.

        Object stores (S3) write straight to the final key: a multipart upload only becomes visible when it
        completes, and a "move" there is a server-side copy plus a delete, which times out on large parts
        and needs s3:DeleteObject (the instance role deliberately has none).
        """
        return self.root.fs.type_name == "local"

    def _open(self) -> None:
        self.root.makedirs(self.path)
        target = self.path + ".tmp" if self._staged else self.path
        self._sink = self.root.fs.open_output_stream(target)
        self._writer = pq.ParquetWriter(self._sink, self.schema, compression="zstd", compression_level=6)
