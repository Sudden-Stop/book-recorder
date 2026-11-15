"""nightly: curate and validate every recorded hour of one UTC day, venue by venue.

Run by the host's `book-recorder-pipeline.timer` as `book-pipeline nightly` (deploy/docker-compose.yml). One
storage root holds `raw/`, `curated/` and `reports/` (the S3 bucket in production). For each venue, the hours
that have any raw file in any of its streams are curated one at a time (memory stays bounded by the batch
size) and then validated. An hour that already has a curate report is skipped unless `force`, so a rerun after a
crash resumes where it stopped. A failing hour is recorded and the run moves on; the summary lists it and the
CLI exits 1.

Publishing is not part of the nightly run: it needs the wallet and licence decisions (`book-pipeline publish`).
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timedelta, timezone
from typing import Callable

from . import curate, validate
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
        min_match_rate: float | None = None, memory_limit: str = "512MB", local_host: str = "local",
        log: Callable[[dict], None] = lambda e: print(json.dumps(e, default=str), flush=True)) -> dict:
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
    summary["ok"] = not summary["failed"]
    summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = root.join("reports", "nightly", f"dt={dt}.json")
    root.makedirs(path)
    with root.fs.open_output_stream(path) as f:
        f.write(json.dumps(summary, indent=2, default=str).encode())
    return summary


def _fail(summary: dict, vs: dict, venue: str, hour: str, step: str, error: str, log) -> None:
    vs["failed"].append(hour)
    summary["failed"].append({"venue": venue, "hour": hour, "step": step, "error": error})
    log({"step": step, "venue": venue, "hour": hour, "ok": False, "error": error})
