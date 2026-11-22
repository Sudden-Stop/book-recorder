"""validate: schema drift, replay match and coverage for curated venue-hours.

Fails (raises `ValidationFailed`, CLI exits 1) when
- a curated file's schema differs from `schema/<table>.yaml` (drift), or a table file is missing;
- `strict` and the curate report lists unknown event types or new frame keys (raw-side drift);
- the replay match rate is below `min_match_rate` (when given and something was evaluated).

Warns, without failing, when the replay match rate is below `WARN_MATCH_RATE` (`warnings`).
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow.parquet as pq

from . import replay, schema
from .curate import curated_path, report_path
from .storage import Root
from .venues import tables_of


# Of the 300 venue-hours of 2026-11-15..21, two were below this: the launch hour (98.4 %) and a recorder
# restart that coverage had missed (86.1 %). The lowest ordinary hour was 98.95 %.
WARN_MATCH_RATE = 0.985


class ValidationFailed(Exception):
    def __init__(self, problems: list[str], report: dict):
        super().__init__("; ".join(problems))
        self.problems = problems
        self.report = report


@dataclass
class Hour:
    venue: str
    dt: str
    hour: str


def drift(root: Root, hours: list[Hour]) -> list[str]:
    problems = []
    for h in hours:
        for name in tables_of(h.venue):
            table = schema.load(name)
            path = curated_path(root, name, h.venue, h.dt, h.hour)
            if not root.exists(path):
                problems.append(f"{name}: missing {path}")
                continue
            with root.fs.open_input_file(path) as f:
                actual = pq.read_schema(f)
            problems += [f"{h.dt}/{h.hour}: {p}" for p in schema.diff(table, actual)]
    return problems


def raw_drift(root: Root, hours: list[Hour]) -> list[str]:
    problems = []
    for h in hours:
        path = report_path(root, "curate", h.venue, h.dt, h.hour)
        if not root.exists(path):
            problems.append(f"{h.dt}/{h.hour}: no curate report at {path}")
            continue
        with root.fs.open_input_stream(path, compression=None) as f:
            rep = json.loads(f.read())
        if rep.get("unknown_event_types"):
            problems.append(f"{h.dt}/{h.hour}: unknown event types {rep['unknown_event_types']}")
        if rep.get("new_keys"):
            problems.append(f"{h.dt}/{h.hour}: new frame keys {rep['new_keys']}")
        if rep.get("read", {}).get("book_events", {}).get("bad_json"):
            problems.append(f"{h.dt}/{h.hour}: unparseable lines in book_events")
    return problems


def warnings(report: dict) -> list[str]:
    """What a validate report (new or stored) says that deserves a look but does not fail the hour."""
    replayed = report.get("replay") or {}
    rate = replayed.get("match_rate")
    if rate is None or rate >= WARN_MATCH_RATE:
        return []
    return [f"replay match rate {rate:.2%} < {WARN_MATCH_RATE:.1%} "
            f"({(replayed.get('outcomes') or {}).get('mismatch', 0)} books differ)"]


def coverage_summary(con_paths: list[str]) -> dict:
    con = replay.connect(con_paths)
    row = con.execute(
        "SELECT count(*), sum(subscribed_s), sum(connected_s), sum(gap_s), sum(gap_count), "
        "count(*) FILTER (WHERE gap_count > 0), count(*) FILTER (WHERE start_assumed), "
        "sum(n_book), sum(n_level_changes), sum(n_trades_ws), sum(n_snapshots), sum(n_trades_activity), "
        "sum(seq_gaps) "
        "FROM read_parquet(?)", [con_paths]).fetchone()
    keys = ("market_hours", "subscribed_s", "connected_s", "gap_s", "gaps", "market_hours_with_gaps",
            "market_hours_start_assumed", "n_book", "n_level_changes", "n_trades_ws", "n_snapshots",
            "n_trades_activity", "seq_gaps")
    out = dict(zip(keys, row))
    out["connected_share"] = (out["connected_s"] / out["subscribed_s"]) if out["subscribed_s"] else None
    return out


def run(curated_root: str, hours: list[Hour], window_ms: int = 2000, min_match_rate: float | None = None,
        strict: bool = False, memory_limit: str = "512MB", write_report: bool = True) -> dict:
    root = Root.parse(curated_root)
    problems = drift(root, hours)
    raw_problems = raw_drift(root, hours)
    report: dict = {"hours": [h.__dict__ for h in hours], "schema_drift": problems, "raw_drift": raw_problems}
    if strict:
        problems = problems + raw_problems
    blocking = [p for p in report["schema_drift"] if any(t in p for t in ("book_deltas", "book_snapshots", "coverage"))]
    has_books = all("book_deltas" in tables_of(h.venue) for h in hours)
    if has_books and not blocking:
        uri = (lambda p: p) if "://" not in curated_root else (lambda p: _s3_uri(curated_root, root, p))
        deltas = [uri(curated_path(root, "book_deltas", h.venue, h.dt, h.hour)) for h in hours]
        snaps = [uri(curated_path(root, "book_snapshots", h.venue, h.dt, h.hour)) for h in hours]
        cov = [uri(curated_path(root, "coverage", h.venue, h.dt, h.hour)) for h in hours]
        report["replay"] = replay.run(deltas, snaps, window_ms=window_ms, memory_limit=memory_limit)
        report["coverage"] = coverage_summary(cov)
        rate = report["replay"]["match_rate"]
        if min_match_rate is not None and rate is not None and rate < min_match_rate:
            problems.append(f"replay match rate {rate:.4f} < {min_match_rate}")
    report["ok"] = not problems
    if write_report and len({(h.venue, h.dt, h.hour) for h in hours}) == 1:
        h = hours[0]
        path = report_path(root, "validate", h.venue, h.dt, h.hour)
        root.makedirs(path)
        with root.fs.open_output_stream(path) as f:
            f.write(json.dumps(report, indent=2, default=str).encode())
    if problems:
        raise ValidationFailed(problems, report)
    return report


def _s3_uri(curated_root: str, root: Root, path: str) -> str:
    # DuckDB reads s3:// URIs itself (httpfs + credentials from the environment)
    scheme = curated_root.split("://", 1)[0]
    return f"{scheme}://{path}"
