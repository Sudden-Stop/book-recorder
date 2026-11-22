"""curate: one venue-hour of raw `ndjson.zst` -> curated Parquet tables.

Venues and their tables: `polymarket` and `kalshi` -> `book_deltas`, `book_snapshots`, `trades`, `markets`,
`coverage`; `cross` -> `pairs`. Streams are read one line at a time and rows are written in row groups, so
memory stays bounded by the batch size, not the hour's size. Metadata streams go first (they map tokens to
markets); `coverage` is written last, since it needs everything else.

Raw input: the S3 layout `raw/venue=<v>/stream=<s>/dt=…/hour=HH/<host>-pN.ndjson.zst`, or the recorder's local
data directory `{venue}/{stream}/dt=…/HH.pN.ndjson.zst` (attributed to host `local_host`). Every part of the
hour is read; a part cut short by a crash yields its complete lines.

A recorder restart that left no `disconnect` line gets one inferred (`restarts`), which needs the first lines of
the host's next recorded hour when the restart crossed the hour's end.

Venues with `WARM` handlers first read streams of the previous hour without writing rows: Kalshi replays
`book_events` (its deltas are relative) to know each book at the start of the hour, Polymarket reads the tail of
`activity` so fills whose copies straddle the hour boundary are kept once. A parser's `finish` runs after the
last stream.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Iterator

from . import restarts, schema
from .coverage import CoverageTracker
from .storage import RawLine, ReadStats, Root, iter_raw, raw_files
from .venues import PARSERS
from .venues.common import Out
from .writer import ROUNDED, TableWriter


def curated_path(root: Root, table: str, venue: str, dt: str, hour: str) -> str:
    return root.join("curated", table, f"venue={venue}", f"dt={dt}", f"part-{hour}.parquet")


def report_path(root: Root, kind: str, venue: str, dt: str, hour: str) -> str:
    return root.join("reports", kind, f"venue={venue}", f"dt={dt}", f"hour-{hour}.json")


def hour_start_ms(dt: str, hour: str) -> int:
    d = datetime.strptime(f"{dt} {int(hour):02d}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)


def previous_hour(dt: str, hour: str) -> tuple[str, str]:
    d = datetime.strptime(f"{dt} {int(hour):02d}", "%Y-%m-%d %H") - timedelta(hours=1)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


def following_raw(src: Root, venue: str, stream: str, dt: str, hour: str, host: str,
                  local_host: str = "local", hours: int = 24) -> Iterator[RawLine]:
    """The host's lines in the first later hour, up to `hours` ahead, in which it recorded the stream."""
    start = datetime.strptime(f"{dt} {int(hour):02d}", "%Y-%m-%d %H")
    for ahead in range(1, hours + 1):
        t = start + timedelta(hours=ahead)
        d, h = t.strftime("%Y-%m-%d"), t.strftime("%H")
        if any(f[0] == host for f in raw_files(src, venue, stream, d, h, local_host)[1]):
            yield from iter_raw(src, venue, stream, d, h, local_host=local_host, only_host=host)
            return


def run(raw: str, out_root: str, venue: str, dt: str, hour: str, batch_rows: int = 200_000,
        local_host: str = "local", warm: bool = True) -> dict:
    """Curate one venue-hour. Returns (and writes under `reports/curate/`) a stats report."""
    if venue not in PARSERS:
        raise ValueError(f"unknown venue {venue!r}; expected one of {sorted(PARSERS)}")
    hour = f"{int(hour):02d}"
    src, dst = Root.parse(raw), Root.parse(out_root)
    parser = PARSERS[venue]
    tables = schema.load_all()
    # rows wait as Python objects until their row group is written: smaller groups where a row is big (a snapshot
    # holds a whole book, a trade a dozen strings)
    shrink = {"book_snapshots": 40, "trades": 8}
    writers = {t: TableWriter(tables[t], dst, curated_path(dst, t, venue, dt, hour),
                              batch_rows=max(1, batch_rows // shrink.get(t, 1)))
               for t in parser.TABLES if t != "coverage"}
    h0 = hour_start_ms(dt, hour)
    out = Out(venue=venue, writers=writers, coverage=CoverageTracker(venue, h0))
    ROUNDED["count"] = 0
    read: dict[str, dict] = {}
    if warm:
        pdt, ph = previous_hour(dt, hour)
        for stream, fn in getattr(parser, "WARM", ()):
            st = ReadStats()
            handler = getattr(parser, fn)
            for ln in iter_raw(src, venue, stream, pdt, ph, st, local_host):
                handler(ln, out)
            read[f"warm:{stream}"] = asdict(st)
    for stream, fn in parser.STREAMS:
        st = ReadStats()
        handler = getattr(parser, fn)
        lines = iter_raw(src, venue, stream, dt, hour, st, local_host)
        if stream == restarts.STREAM:
            following = partial(following_raw, src, venue, stream, dt, hour, local_host=local_host)
            lines = restarts.with_disconnects(lines, out, following)
        for ln in lines:
            handler(ln, out)
        read[stream] = asdict(st)
    if hasattr(parser, "finish"):
        parser.finish(out)
    counts = {t: w.close() for t, w in writers.items()}
    if "coverage" in parser.TABLES:
        cov = TableWriter(tables["coverage"], dst, curated_path(dst, "coverage", venue, dt, hour))
        for row in out.coverage.rows(out.token_market):
            cov.append(row)
        counts["coverage"] = cov.close()
    report = {
        "venue": venue, "dt": dt, "hour": hour, "raw": raw, "out": out_root,
        "unverified_parser": bool(getattr(parser, "UNVERIFIED", False)),
        "read": read, "rows": counts,
        "truncated_files": [p for r in read.values() for p in r["truncated_files"]],
        "stats": dict(sorted(out.stats.items())),
        "unknown_event_types": dict(out.unknown_event_types),
        "new_keys": {k: sorted(v) for k, v in out.new_keys.items()},
        "decimals_rounded": ROUNDED["count"],
        "curated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    path = report_path(dst, "curate", venue, dt, hour)
    dst.makedirs(path)
    with dst.fs.open_output_stream(path) as f:
        f.write(json.dumps(report, indent=2).encode())
    return report
