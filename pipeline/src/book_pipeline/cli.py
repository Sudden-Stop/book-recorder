"""book-pipeline curate | validate | nightly | publish

  book-pipeline curate   --raw s3://bucket --out s3://bucket --venue polymarket --dt 2026-11-14 --hour 07
  book-pipeline validate --curated s3://bucket --venue polymarket --dt 2026-11-14 --hour 07 [--strict]
  book-pipeline nightly  [--root s3://bucket] [--dt 2026-11-14] [--venue polymarket] [--force]
  book-pipeline publish  --curated s3://bucket --stage ./stage --wallets hashed [--repo-id org/name] [--public]

`--raw` / `--out` / `--curated` are storage roots holding `raw/…`, `curated/…` and `reports/…`; a local
directory works the same as an S3 prefix. `publish` without `--repo-id` only stages (dry run).
"""
from __future__ import annotations

import argparse
import json
import sys

from . import curate, nightly, publish, schema, validate
from .storage import Root


def _hours(args) -> list[validate.Hour]:
    hours = args.hour
    if not hours:  # every hour curated for that venue and date
        root = Root.parse(args.curated)
        table = "pairs" if args.venue == "cross" else "book_deltas"
        files = root.list(f"curated/{table}/venue={args.venue}/dt={args.dt}")
        hours = [f.rsplit("part-", 1)[-1][:2] for f in files if f.endswith(".parquet")]
    return [validate.Hour(args.venue, args.dt, f"{int(h):02d}") for h in hours]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="book-pipeline", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("curate", help="raw venue-hour -> curated Parquet tables")
    c.add_argument("--raw", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--venue", required=True, choices=["polymarket", "kalshi", "cross"])
    c.add_argument("--dt", required=True)
    c.add_argument("--hour", required=True, action="append", help="UTC hour; repeat for several")
    c.add_argument("--batch-rows", type=int, default=200_000)
    c.add_argument("--local-host", default="local",
                   help="host name for files in the recorder's local data-dir layout (no host in the name)")
    c.add_argument("--no-warm", action="store_true", help="do not read the previous hour (Kalshi books, Polymarket activity dedup)")

    v = sub.add_parser("validate", help="schema drift, replay match and coverage")
    v.add_argument("--curated", required=True)
    v.add_argument("--venue", required=True, choices=["polymarket", "kalshi", "cross"])
    v.add_argument("--dt", required=True)
    v.add_argument("--hour", action="append", help="UTC hour; repeat for several (default: one run per hour)")
    v.add_argument("--window-ms", type=int, default=2000)
    v.add_argument("--min-match-rate", type=float)
    v.add_argument("--strict", action="store_true", help="also fail on unknown event types / new frame keys")
    v.add_argument("--memory-limit", default="512MB")

    n = sub.add_parser("nightly", help="curate + validate every recorded hour of one day (default: yesterday UTC)")
    n.add_argument("--root", default=nightly.default_root(),
                   help="storage root with raw/, curated/, reports/ (default: $BOOK_ROOT, else s3://$BR_S3_BUCKET)")
    n.add_argument("--dt", default=None, help="UTC date YYYY-MM-DD (default: yesterday)")
    n.add_argument("--venue", action="append", choices=list(nightly.VENUES), help="repeat; default: all")
    n.add_argument("--hour", action="append", help="UTC hour; repeat (default: every hour with raw data)")
    n.add_argument("--force", action="store_true", help="redo hours that already have a curate report")
    n.add_argument("--batch-rows", type=int, default=200_000)
    n.add_argument("--window-ms", type=int, default=2000)
    n.add_argument("--min-match-rate", type=float)
    n.add_argument("--memory-limit", default="256MB",
                   help="DuckDB cap for validate; it uses ~150 MB on top, and the container cap is 1 GB")
    n.add_argument("--local-host", default="local")
    n.add_argument("--chain", action=argparse.BooleanOptionalAction, default=None,
                   help="check Polymarket activity against on-chain fills (default: on when POLYGON_RPC_URL is set)")

    b = sub.add_parser("publish", help="stage curated tables for Hugging Face and optionally push")
    b.add_argument("--curated", required=True)
    b.add_argument("--stage", required=True)
    b.add_argument("--wallets", required=True, choices=publish.WALLET_MODES)
    b.add_argument("--tables", nargs="*", default=list(schema.TABLES), choices=schema.TABLES)
    b.add_argument("--venue", action="append")
    b.add_argument("--dt-from")
    b.add_argument("--dt-to")
    b.add_argument("--pretty-name", default="Prediction market order books")
    b.add_argument("--repo-id", help="push to this dataset repo; omitted = stage only")
    b.add_argument("--public", action="store_true", help="create the repo public (default private)")

    args = p.parse_args(argv)
    if args.cmd == "curate":
        for h in args.hour:
            rep = curate.run(args.raw, args.out, args.venue, args.dt, h, batch_rows=args.batch_rows,
                             local_host=args.local_host, warm=not args.no_warm)
            print(json.dumps({"hour": rep["hour"], "rows": rep["rows"],
                              "unknown_event_types": rep["unknown_event_types"],
                              "truncated_files": rep["truncated_files"]}))
        return 0
    if args.cmd == "validate":
        status = 0
        for h in _hours(args):
            try:
                rep = validate.run(args.curated, [h], window_ms=args.window_ms, min_match_rate=args.min_match_rate,
                                   strict=args.strict, memory_limit=args.memory_limit)
                print(json.dumps({"hour": h.hour, "ok": True, "match_rate": rep.get("replay", {}).get("match_rate"),
                                  "connected_share": rep.get("coverage", {}).get("connected_share")}))
            except validate.ValidationFailed as e:
                print(json.dumps({"hour": h.hour, "ok": False, "problems": e.problems}))
                status = 1
        return status
    if args.cmd == "nightly":
        if not args.root:
            p.error("nightly needs --root (or BOOK_ROOT / BR_S3_BUCKET in the environment)")
        summary = nightly.run(args.root, args.dt or nightly.yesterday_utc(),
                              venues=tuple(args.venue) if args.venue else nightly.VENUES, hours=args.hour,
                              force=args.force, batch_rows=args.batch_rows, window_ms=args.window_ms,
                              min_match_rate=args.min_match_rate, memory_limit=args.memory_limit,
                              local_host=args.local_host, chain=args.chain)
        print(json.dumps({"dt": summary["dt"], "ok": summary["ok"], "failed": summary["failed"],
                          "warnings": summary["warnings"]}))
        return 0 if summary["ok"] else 1
    if args.cmd == "publish":
        manifest = publish.stage(args.curated, args.stage, wallets=args.wallets, tables=tuple(args.tables),
                                 venues=tuple(args.venue) if args.venue else None, dt_from=args.dt_from,
                                 dt_to=args.dt_to, pretty_name=args.pretty_name)
        print(json.dumps(manifest, indent=2))
        if args.repo_id:
            print(publish.push(args.stage, args.repo_id, public=args.public))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
