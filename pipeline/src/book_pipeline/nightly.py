"""nightly: curate and validate every recorded hour of one UTC day, venue by venue.

Run by the host's `book-recorder-pipeline.timer` as `book-pipeline nightly` (deploy/docker-compose.yml). One
storage root holds `raw/`, `curated/` and `reports/` (the S3 bucket in production). For each venue, the hours
that have any raw file in any of its streams are curated one at a time (memory stays bounded by the batch
size) and then validated. An hour that already has a curate report is skipped unless `force`, so a rerun after a
crash resumes where it stopped. A failing hour is recorded and the run moves on; the summary lists it and the
CLI exits 1.

Polymarket hours are then checked against the chain (`chain_check`: every on-chain fill should be in the
`activity` feed) when `POLYGON_RPC_URL` is set, writing `reports/chain/`. A failed check counts as a failed hour:
activity rows or prices that disagree with the chain, an unknown exchange, more than 0.05 % of fills lost while
every socket was down, or the feed below 99 % outside outages. The feed's usual loss (~0.1-0.3 %: dropped orders
and skipped blocks) is a warning only (`chain_check.problems` / `warnings`).

Publishing is not part of the nightly run: it needs the wallet and licence decisions (`book-pipeline publish`).
"""
from __future__ import annotations

import gc
import json
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import Callable

import pyarrow as pa

from . import chain_check, curate, validate
from .storage import Root, raw_files
from .venues import PARSERS

VENUES = ("polymarket", "kalshi", "cross")
HOURS = tuple(f"{h:02d}" for h in range(24))


def yesterday_utc(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(days=1)).strftime("%Y-%m-%d")


def default_root() -> str | None:
    """`BOOK_ROOT`, else `s3://$BR_S3_BUCKET` (the recorder's bucket, set in the host's .env)."""
    if os.environ.get("BOOK_ROOT"):
        return os.environ["BOOK_ROOT"]
    if os.environ.get("BR_S3_BUCKET"):
        return f"s3://{os.environ['BR_S3_BUCKET']}"
    return None


def recorded_hours(root: Root, venue: str, dt: str, local_host: str = "local") -> list[str]:
    """Hours of `dt` with at least one raw file in any of the venue's streams."""
    streams = [s for s, _ in PARSERS[venue].STREAMS]
    return [h for h in HOURS if any(raw_files(root, venue, s, dt, h, local_host)[1] for s in streams)]


def run(root_uri: str, dt: str, venues: tuple[str, ...] = VENUES, hours: list[str] | None = None,
        force: bool = False, batch_rows: int = 200_000, window_ms: int = 2000,
        min_match_rate: float | None = None, memory_limit: str = "256MB", local_host: str = "local",
        chain: bool | None = None, log: Callable[[dict], None] = lambda e: print(json.dumps(e, default=str), flush=True)) -> dict:
    root = Root.parse(root_uri)
    summary: dict = {"dt": dt, "root": root_uri, "venues": {}, "failed": [],
                     "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for venue in venues:
        if venue not in PARSERS:
            raise ValueError(f"unknown venue {venue!r}; expected one of {sorted(PARSERS)}")
        todo = [f"{int(h):02d}" for h in hours] if hours else recorded_hours(root, venue, dt, local_host)
        vs = {"hours": todo, "curated": [], "skipped": [], "validated": [], "failed": []}
        summary["venues"][venue] = vs
        log({"venue": venue, "dt": dt, "hours": todo})
        for h in todo:
            done = root.exists(curate.report_path(root, "curate", venue, dt, h))
            if done and not force:
                vs["skipped"].append(h)
            else:
                try:
                    rep = curate.run(root_uri, root_uri, venue, dt, h, batch_rows=batch_rows,
                                     local_host=local_host)
                    vs["curated"].append(h)
                    log({"step": "curate", "venue": venue, "hour": h, "rows": rep["rows"],
                         "truncated_files": rep["truncated_files"]})
                except Exception as e:  # one bad hour must not stop the night
                    _fail(summary, vs, venue, h, "curate", f"{type(e).__name__}: {e}", log)
                    log({"step": "curate", "venue": venue, "hour": h, "traceback": traceback.format_exc()})
                    continue
            validated = root.exists(curate.report_path(root, "validate", venue, dt, h))
            if validated and done and not force:
                continue
            try:
                _release()  # curate's freed Arrow buffers back to the OS before DuckDB starts
                rep = validate.run(root_uri, [validate.Hour(venue, dt, h)], window_ms=window_ms,
                                   min_match_rate=min_match_rate, memory_limit=memory_limit)
                vs["validated"].append(h)
                log({"step": "validate", "venue": venue, "hour": h, "ok": True,
                     "match_rate": rep.get("replay", {}).get("match_rate"),
                     "connected_share": rep.get("coverage", {}).get("connected_share")})
            except validate.ValidationFailed as e:
                _fail(summary, vs, venue, h, "validate", "; ".join(e.problems), log)
            except Exception as e:
                _fail(summary, vs, venue, h, "validate", f"{type(e).__name__}: {e}", log)
        if venue == "polymarket" and _chain_enabled(chain):
            _chain(root, root_uri, dt, todo, vs, summary, force, log)
    summary["ok"] = not summary["failed"]
    summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = root.join("reports", "nightly", f"dt={dt}.json")
    root.makedirs(path)
    with root.fs.open_output_stream(path) as f:
        f.write(json.dumps(summary, indent=2, default=str).encode())
    return summary


def _chain_enabled(chain: bool | None) -> bool:
    """`None` (the default): on when a Polygon RPC is configured. The public RPCs are too flaky for 24 hours."""
    return bool(os.environ.get(chain_check.RPC_ENV)) if chain is None else chain


def _chain(root: Root, root_uri: str, dt: str, hours: list[str], vs: dict, summary: dict, force: bool,
           log) -> None:
    """Completeness of the activity feed against on-chain fills, for every curated hour."""
    vs["chain_checked"] = []
    for h in hours:
        if not root.exists(curate.report_path(root, "curate", "polymarket", dt, h)):
            continue
        if root.exists(chain_check.report_path(root, dt, h)) and not force:
            continue
        try:
            _release()
            rep = chain_check.run(root_uri, dt, [h], write=True)
        except Exception as e:  # an RPC outage must not hide the other hours
            _fail(summary, vs, "polymarket", h, "chain", f"{type(e).__name__}: {e}", log)
            continue
        vs["chain_checked"].append(h)
        log({"step": "chain", "venue": "polymarket", "hour": h, "ok": rep["ok"], "warnings": rep.get("warnings"),
             "in_scope": rep["in_scope"], "missing_by_cause": rep["missing"]["by_cause"]})
        if not rep["ok"]:
            _fail(summary, vs, "polymarket", h, "chain", "; ".join(rep["problems"]), log)


def _release() -> None:
    """Return freed memory to the OS. Arrow's allocator keeps it otherwise, and curate then validate in one
    process under a 1 GB container cap was enough to get the job OOM-killed."""
    gc.collect()
    pa.default_memory_pool().release_unused()


def _fail(summary: dict, vs: dict, venue: str, hour: str, step: str, error: str, log) -> None:
    vs["failed"].append(hour)
    summary["failed"].append({"venue": venue, "hour": hour, "step": step, "error": error})
    log({"step": step, "venue": venue, "hour": hour, "ok": False, "error": error})
