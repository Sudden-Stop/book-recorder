"""prepare: derived tables that make recorded days ready for `docs/assumption_tests.md`. Curated data is only read.

    python -m book_pipeline.prepare --root <root> --from 2026-11-15 --to 2026-11-21 --out <local dir>
        [--rpc URL] [--no-chain] [--exclusions docs/data_exclusions.csv]

`<root>` holds `curated/` (markets, coverage, trades) and `reports/chain/`. Written to `--out`:

- `markets.parquet`: one row per tracked market with category, tick, fees and outcome, each from the venue's own
  record. A Polymarket market without a recorded outcome gets it from the conditional-tokens contract's
  `ConditionResolution` event on Polygon, after that source has reproduced the outcomes the venue did report.
  `resolution_ts` is when the market settled, which can be hours after the event was decided: measure horizons from
  `event_start_ts` (Polymarket games) or the listed `end_ts`.
- `trade_reach.parquet`: one row per Polymarket `market_ws` trade with the worst price it reached. The print itself
  is often the average of a sweep, so the depth comes from the maker orders of the same transaction in `activity`.
  `reach_price` is set only when those makers add up to the whole trade.
- `activity_surplus.parquet`: `activity` rows with no fill on chain, in the hours whose chain report counted some.
  Of the copies of one fill, the one timed furthest from its block is the surplus.
- `exclusions.parquet`: windows not to use, from `docs/data_exclusions.csv`.
- `manifest.json`: counts, and what each source could not fill.

Trade rows are identified by `(dt, hour, source, host, line, item)`: `line` restarts every hour and in each stream.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import chain_check as cc
from . import replay
from .schema import TIMESTAMP
from .storage import Root
from .venues import kalshi
from .venues.common import iso_ms

CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"  # Gnosis conditional tokens, as used by Polymarket
RESOLUTION = "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894"  # ConditionResolution
RESOLUTION_RPCS = ("https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org")  # widest ranges first
RESOLUTION_RANGE = 10_000
MIN_AGREEMENT = 0.995  # chain against venue, on the markets where both have an outcome
SIZE_EPS = 0.011  # shares: what the makers may differ from the taker by through rounding
PRICE_EPS = 0.00006

MARKETS = pa.schema([
    ("venue", pa.string()), ("market_id", pa.string()), ("event_id", pa.string()), ("series", pa.string()),
    ("category", pa.string()), ("category_source", pa.string()), ("is_sports", pa.bool_()),
    ("tick_size", pa.float64()), ("price_structure", pa.string()),
    ("fees_enabled", pa.bool_()), ("fee_type", pa.string()), ("fee_multiplier", pa.float64()),
    ("fee_rate", pa.float64()),
    ("outcome", pa.string()), ("outcome_source", pa.string()), ("payouts", pa.list_(pa.float64())),
    ("resolution_ts", TIMESTAMP), ("resolution_source", pa.string()), ("chain_resolved_ts", TIMESTAMP),
    ("event_start_ts", TIMESTAMP), ("end_ts", TIMESTAMP), ("first_recv_ts", TIMESTAMP), ("last_recv_ts", TIMESTAMP),
])
SURPLUS = pa.schema([
    ("dt", pa.string()), ("hour", pa.string()), ("source", pa.string()), ("host", pa.string()),
    ("line", pa.int64()), ("item", pa.int32()), ("tx_hash", pa.string()),
])
REACH = pa.schema([
    ("dt", pa.string()), ("hour", pa.string()), ("source", pa.string()), ("host", pa.string()),
    ("line", pa.int64()), ("item", pa.int32()), ("recv_ts", TIMESTAMP), ("market_id", pa.string()),
    ("token_id", pa.string()), ("side", pa.string()), ("tx_hash", pa.string()),
    ("print_price", pa.float64()), ("size", pa.float64()), ("reach_price", pa.float64()),
    ("first_price", pa.float64()), ("vwap", pa.float64()), ("n_levels", pa.int32()),
    ("n_makers_seen", pa.int32()), ("status", pa.string()),
])
EXCLUSIONS = pa.schema([
    ("venue", pa.string()), ("from_ts", TIMESTAMP), ("to_ts", TIMESTAMP), ("reason", pa.string()),
])


# --- metadata ------------------------------------------------------------------------------------------------

def polymarket_category(raw: dict) -> tuple[str | None, str | None]:
    """(category, where it comes from). The record has no tags, but its fee schedule is named after the category."""
    if raw.get("sportsMarketType"):
        return "sports", "sportsMarketType"
    fee = raw.get("feeType")
    if fee:
        head = fee.split("_fees")[0].split("_")
        return ("elections" if "elections" in head else head[0]), "feeType"
    return None, None  # fee-free markets name no category


def polymarket_labels(raw: dict) -> list[str]:
    labels = raw.get("outcomes")
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except ValueError:
            return []
    return [str(x) for x in labels] if isinstance(labels, list) else []


# --- resolutions on chain ------------------------------------------------------------------------------------

def decode_resolution(log: dict) -> dict:
    d = log["data"][2:]
    w = [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]
    at = w[1] // 32  # offset of the payout array
    nums = w[at + 1:at + 1 + w[at]]
    total = sum(nums)
    return {"market_id": log["topics"][1].lower(), "block": int(log["blockNumber"], 16),
            "ts": int(log["blockTimestamp"], 16) if log.get("blockTimestamp") else None,
            "payouts": [n / total for n in nums] if total else []}


def fetch_resolutions(rpc: cc.Rpc, b0: int, b1: int, wanted: set[str]) -> dict[str, dict]:
    """Every resolution of a `wanted` market in blocks [b0, b1]. Raises rather than return part of the range."""
    span = dict.fromkeys(rpc.urls, RESOLUTION_RANGE)
    out: dict[str, dict] = {}
    b = b0
    while b <= b1:
        for u in rpc.urls:
            logs = None
            while logs is None:
                e = min(b1, b + span[u] - 1)
                try:
                    logs = rpc.call_one(u, "eth_getLogs", [{"fromBlock": hex(b), "toBlock": hex(e), "address": CTF,
                                                            "topics": [RESOLUTION]}])
                except cc.RpcError:
                    if span[u] == 1:
                        break
                    span[u] = max(1, span[u] // 2)
                except Exception:  # noqa: BLE001 - not answering: next RPC
                    break
            if logs is not None:
                for lg in logs:
                    if not lg.get("removed") and lg["topics"][1].lower() in wanted:
                        r = decode_resolution(lg)
                        out[r["market_id"]] = r
                b = e + 1
                break
        else:
            raise RuntimeError(f"no RPC returned resolutions for blocks {b}..")
    missing_ts = sorted({r["block"] for r in out.values() if r["ts"] is None})
    if missing_ts:
        times = cc.block_times(rpc, missing_ts)
        for r in out.values():
            if r["ts"] is None:
                r["ts"] = times[r["block"]]
    return out


def winner(payouts: list[float], labels: list[str]) -> str | None:
    """The label paid in full; None on a split payout."""
    if len(payouts) != len(labels):
        return None
    won = [label for p, label in zip(payouts, labels) if p == 1]
    return won[0] if len(won) == 1 else None


# --- markets -------------------------------------------------------------------------------------------------

def _days(dt_from: str, dt_to: str) -> list[str]:
    a, b = (datetime.strptime(d, "%Y-%m-%d") for d in (dt_from, dt_to))
    return [(a + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((b - a).days + 1)]


def _files(root: Root, root_uri: str, table: str, venue: str, days: list[str] | None = None) -> list[str]:
    """The table's parts as paths DuckDB can read."""
    scheme = root_uri.split("://", 1)[0] + "://" if "://" in root_uri else ""
    found = [p for p in root.glob_dirs(f"curated/{table}/venue={venue}") if p.endswith(".parquet")]
    return [scheme + p for p in found if days is None or any(f"/dt={d}/" in p for d in days)]


def markets(con, root: Root, root_uri: str, days: list[str], rpc: cc.Rpc | None) -> tuple[list[dict], dict]:
    rows: list[dict] = []
    info: dict = {}
    for venue in ("polymarket", "kalshi"):
        cov, meta = _files(root, root_uri, "coverage", venue, days), _files(root, root_uri, "markets", venue)
        if not cov or not meta:
            continue
        tracked = {m: (a, b) for m, a, b in con.execute(
            "SELECT market_id, epoch_ms(min(first_recv_ts)), epoch_ms(max(last_recv_ts)) FROM read_parquet(?, "
            "hive_partitioning=false) WHERE market_id IS NOT NULL GROUP BY 1", [cov]).fetchall()}
        latest = {m: (json.loads(raw), ev, tick, fee, end) for m, raw, ev, tick, fee, end in con.execute(
            "SELECT market_id, arg_max(raw, snapshot_ts), arg_max(event_id, snapshot_ts), "
            "CAST(arg_max(tick_size, snapshot_ts) AS DOUBLE), arg_max(fees_enabled, snapshot_ts), "
            "epoch_ms(arg_max(end_ts, snapshot_ts)) FROM read_parquet(?, hive_partitioning=false) "
            "WHERE source = 'metadata' GROUP BY 1", [meta]).fetchall()}
        # any day counts for the outcome: a market tracked in the range may have resolved after it
        resolved = {m: (o, t) for m, o, t in con.execute(
            "SELECT market_id, arg_max(resolved_outcome, snapshot_ts), epoch_ms(min(resolution_ts)) "
            "FROM read_parquet(?, hive_partitioning=false) WHERE resolved_outcome IS NOT NULL GROUP BY 1",
            [meta]).fetchall()}
        series = {m: (cat, ft, fm) for m, cat, ft, fm in con.execute(
            "SELECT market_id, arg_max(tags[1], snapshot_ts), arg_max(fee_type, snapshot_ts), "
            "arg_max(fee_multiplier, snapshot_ts) FROM read_parquet(?, hive_partitioning=false) "
            "WHERE source = 'series' GROUP BY 1", [meta]).fetchall()}
        chain: dict[str, dict] = {}
        if venue == "polymarket" and rpc is not None:
            t0 = int(datetime.strptime(days[0], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            chain = fetch_resolutions(rpc, cc.block_at(rpc, t0), rpc.head(), {m.lower() for m in tracked})
        agree = compared = 0
        mine: list[dict] = []
        for m, (seen_from, seen_to) in sorted(tracked.items()):
            raw, event, tick, fees, end = latest.get(m, ({}, None, None, None, None))
            row = dict.fromkeys(MARKETS.names)
            row.update(venue=venue, market_id=m, event_id=event, first_recv_ts=seen_from, last_recv_ts=seen_to,
                       tick_size=tick, fees_enabled=fees, end_ts=end)
            if m in resolved:
                row["outcome"], row["resolution_ts"] = resolved[m]
                row["outcome_source"] = "venue"
                if row["resolution_ts"] is not None:
                    row["resolution_source"] = "venue"
            if venue == "kalshi":
                name = (event or m).split("-")[0]
                cat, fee_type, mult = series.get(name, (None, None, None))
                row.update(series=name, category=cat.lower() if cat else None,
                           category_source="series" if cat else None, fee_type=fee_type, fee_multiplier=mult,
                           price_structure=raw.get("price_level_structure"))
                if tick is None:
                    step = kalshi.min_step(raw)
                    row["tick_size"] = float(step) if step is not None else None
            else:
                ev = raw.get("events")
                ev = ev[0] if isinstance(ev, list) and ev and isinstance(ev[0], dict) else {}
                cat, src = polymarket_category(raw)
                row.update(series=ev.get("seriesSlug"), category=cat, category_source=src,
                           fee_type=raw.get("feeType"), fee_rate=(raw.get("feeSchedule") or {}).get("rate"),
                           # Kalshi has no counterpart: its `occurrence_datetime` is the expected end
                           event_start_ts=iso_ms(raw.get("gameStartTime")))
                on_chain = chain.get(m.lower())
                if on_chain:
                    won = winner(on_chain["payouts"], polymarket_labels(raw))
                    row.update(payouts=on_chain["payouts"], chain_resolved_ts=on_chain["ts"] * 1000)
                    if m in resolved:
                        compared += 1
                        agree += won == row["outcome"]
                    else:
                        row.update(outcome=won, outcome_source="chain" if won else "chain_split")
                    if row["resolution_ts"] is None:
                        row.update(resolution_ts=on_chain["ts"] * 1000, resolution_source="chain")
            if row["category"]:
                row["is_sports"] = row["category"] == "sports"
            mine.append(row)
        if compared and agree / compared < MIN_AGREEMENT:
            raise RuntimeError(f"chain and venue agree on only {agree} of {compared} known outcomes; not using the chain")
        rows += mine
        info[venue] = {
            "markets": len(mine),
            "category": sum(r["category"] is not None for r in mine),
            "tick_size": sum(r["tick_size"] is not None for r in mine),
            "fee_type": sum(r["fee_type"] is not None for r in mine),
            "outcome": dict(Counter(r["outcome_source"] or "none" for r in mine)),
        }
        if venue == "polymarket" and rpc is not None:
            info[venue].update(chain_compared=compared, chain_agree=agree)
    return rows, info


# --- depth a trade reached -----------------------------------------------------------------------------------

KEY_OF_FILE = ("regexp_extract(filename, 'dt=([0-9-]{10})', 1) AS dt, "
               "regexp_extract(filename, 'part-([0-9][0-9])[.]parquet', 1) AS hour")

# in steps, each a table keyed by a small integer: over a week one query of strings takes tens of minutes
PRINTS_SQL = f"""
CREATE OR REPLACE TEMP TABLE prints AS
SELECT row_number() OVER () AS id, * FROM (
  SELECT {KEY_OF_FILE}, source, host, line, item, recv_ts, market_id, token_id, side, lower(tx_hash) AS tx,
         CAST(price AS DOUBLE) AS price, CAST(size AS DOUBLE) AS size
  FROM read_parquet(?, filename=true, hive_partitioning=false)
  WHERE source = 'market_ws'
) WHERE list_contains(?, dt)
"""

ORDERS_SQL = f"""
CREATE OR REPLACE TEMP TABLE orders AS
WITH wanted AS (SELECT DISTINCT tx FROM prints WHERE tx IS NOT NULL),
near AS (
  SELECT filename, source, host, line, item, lower(tx_hash) AS tx, market_id, token_id, side,
         CAST(price AS DOUBLE) AS price, CAST(size AS DOUBLE) AS size
  FROM read_parquet(?, filename=true, hive_partitioning=false)
  WHERE source = 'activity' AND lower(tx_hash) IN (SELECT tx FROM wanted)
)
SELECT tx, market_id, token_id, side, price, size
FROM (SELECT *, {KEY_OF_FILE} FROM near) ANTI JOIN surplus USING (dt, hour, source, host, line, item)
"""

# the taker's own order is the row with its token and side; a maker is on the other side of the same token, or on
# the same side of the other token (1 - price in the taker's terms)
MAKERS_SQL = """
CREATE OR REPLACE TEMP TABLE makers AS
SELECT id, count(*) AS n, count(DISTINCT round(p, 4)) AS levels, min(p) AS lo, max(p) AS hi, sum(s) AS s,
       sum(p * s) / nullif(sum(s), 0) AS vwap, bool_and(valid) AS valid
FROM (
  SELECT w.id, a.size AS s,
         CASE WHEN a.token_id = w.token_id THEN a.price ELSE 1 - a.price END AS p,
         a.market_id = w.market_id AND (a.token_id = w.token_id) = (a.side != w.side) AS valid
  FROM prints w JOIN orders a USING (tx)
  WHERE NOT (a.token_id = w.token_id AND a.side = w.side)
) GROUP BY id
"""

REACH_SQL = f"""
COPY (
  WITH j AS (
    SELECT w.*, m.n, m.levels, m.lo, m.hi, m.vwap,
           CASE WHEN m.n IS NULL THEN 'no_makers'
                WHEN NOT coalesce(m.valid, false) THEN 'inconsistent'
                WHEN abs(m.s - w.size) > {SIZE_EPS} THEN 'partial'
                WHEN w.side IS NULL THEN 'inconsistent'
                ELSE 'exact' END AS status
    FROM prints w LEFT JOIN makers m USING (id)
  )
  SELECT dt, hour, source, host, line, item, recv_ts, market_id, token_id, side, tx AS tx_hash,
         price AS print_price, size,
         CASE WHEN status = 'exact' THEN round(CASE WHEN side = 'buy' THEN hi ELSE lo END, 4) END AS reach_price,
         CASE WHEN status = 'exact' THEN round(CASE WHEN side = 'buy' THEN lo ELSE hi END, 4) END AS first_price,
         CASE WHEN status = 'exact' THEN vwap END AS vwap,
         CASE WHEN status = 'exact' THEN CAST(levels AS INTEGER) END AS n_levels,
         CAST(coalesce(n, 0) AS INTEGER) AS n_makers_seen,
         status
  FROM j ORDER BY dt, hour, host, line, item
) TO '{{out}}' (FORMAT parquet, COMPRESSION zstd)
"""


def trade_reach(con, root: Root, root_uri: str, days: list[str], surplus: list[dict], out: Path) -> dict:
    # a day's margin on each side: the makers of a trade at an edge can arrive across it
    a, b = (datetime.strptime(d, "%Y-%m-%d") for d in (days[0], days[-1]))
    around = [(a - timedelta(days=1)).strftime("%Y-%m-%d"), *days, (b + timedelta(days=1)).strftime("%Y-%m-%d")]
    files = _files(root, root_uri, "trades", "polymarket", around)
    if not files:
        pq.write_table(REACH.empty_table(), out)
        return {}
    con.register("surplus", pa.Table.from_pylist(surplus, schema=SURPLUS))
    target = out.as_posix().replace("'", "''")
    con.execute(PRINTS_SQL, [files, days])
    con.execute(ORDERS_SQL, [files])
    con.execute(MAKERS_SQL)
    con.execute(REACH_SQL.replace("{out}", target))
    con.execute("DROP TABLE orders; DROP TABLE makers; DROP TABLE prints")
    by_status = dict(con.execute("SELECT status, count(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                                 [str(out)]).fetchall())
    sweeps, off = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE abs(print_price - reach_price) > {PRICE_EPS}) "
        "FROM read_parquet(?) WHERE n_levels > 1", [str(out)]).fetchone()
    return {"status": by_status, "sweeps": sweeps, "sweeps_print_is_not_reach": off}


# --- activity rows without a fill ----------------------------------------------------------------------------

def surplus_rows(fills: list[dict], rows: list[dict], t0: int, t1: int) -> list[dict]:
    """The rows in [t0, t1) beyond the fills of their key, keeping the rows timed nearest to their block."""
    by_key: dict[tuple, list[dict]] = defaultdict(list)
    for f in fills:
        by_key[cc.fill_key(f)].append(f)
    copies: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        copies[cc.row_key(r)].append(r)
    extra = []
    for key, rs in copies.items():
        left = list(by_key.get(key, ()))
        if len(rs) <= len(left):
            continue
        times = [f["block_ts"] * 1000 for f in left]
        ranked = sorted(rs, key=lambda r: (min((abs(r["ts_ms"] - t) for t in times), default=0), r["recv_ms"],
                                           r["line"]))
        extra += [r for r in ranked[len(left):] if t0 * 1000 <= r["ts_ms"] < t1 * 1000]
    return extra


def activity_surplus(con, root: Root, root_uri: str, days: list[str], rpc: cc.Rpc) -> tuple[list[dict], dict]:
    out: list[dict] = []
    info = {"hours_checked_on_chain": 0, "days_without_chain_check": [], "surplus_by_hour": {}}
    trades = _files(root, root_uri, "trades", "polymarket")
    for dt in days:
        checked = 0
        for path in sorted(root.glob_dirs(f"reports/chain/venue=polymarket/dt={dt}")):
            if not path.endswith(".json"):
                continue
            with root.fs.open_input_stream(path, compression=None) as f:
                rep = json.loads(f.read())
            checked += 1
            expected = rep["extra_activity_rows"]["n"]
            if not expected:
                continue
            hour = path.rsplit("hour-", 1)[1][:2]
            at = datetime.strptime(f"{dt} {hour}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
            t0, t1 = int(at.timestamp()), int(at.timestamp()) + 3600
            near = [at + timedelta(hours=h) for h in (-1, 0, 1)]
            files = [p for p in trades for h in near if p.endswith(f"/dt={h:%Y-%m-%d}/part-{h:%H}.parquet")]
            cur = con.execute(
                "SELECT regexp_extract(filename, 'dt=([0-9-]{10})', 1) AS dt, "
                "regexp_extract(filename, 'part-([0-9][0-9])[.]parquet', 1) AS hour, source, host, line, item, "
                "tx_hash, wallet, token_id, side, size, epoch_ms(exchange_ts) AS ts_ms, epoch_ms(recv_ts) AS recv_ms "
                "FROM read_parquet(?, filename=true, hive_partitioning=false) "
                "WHERE source = 'activity' AND epoch_ms(exchange_ts) BETWEEN ? AND ?",
                [files, (t0 - cc.MARGIN_S) * 1000, (t1 + cc.MARGIN_S) * 1000 - 1])
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
            fills, _ = cc.fetch_fills(rpc, t0 - cc.MARGIN_S, t1 + cc.MARGIN_S)
            extra = surplus_rows(fills, rows, t0, t1)
            if len(extra) != expected:
                raise RuntimeError(f"{dt}T{hour}: {len(extra)} surplus rows, the chain report says {expected}")
            out += [{k: r[k] for k in SURPLUS.names} for r in extra]
            info["surplus_by_hour"][f"{dt}T{hour}"] = expected
        info["hours_checked_on_chain"] += checked
        if not checked:
            info["days_without_chain_check"].append(dt)
    return out, info


# --- exclusions ----------------------------------------------------------------------------------------------

def read_exclusions(path: Path, days: list[str]) -> list[dict]:
    def ms(s: str) -> int:
        return int(datetime.strptime(s, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc).timestamp() * 1000)

    lo = ms(days[0] + "T00:00Z")
    hi = ms(days[-1] + "T00:00Z") + 86_400_000
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            a, b = ms(r["from_utc"]), ms(r["to_utc"])
            if b <= a:
                raise ValueError(f"empty exclusion: {r}")
            if a < hi and b > lo:
                out.append({"venue": r["venue"], "from_ts": a, "to_ts": b, "reason": r["reason"]})
    return out


def run(root_uri: str, dt_from: str, dt_to: str, out_dir: str | Path, exclusions: str | Path | None = None,
        rpc: cc.Rpc | None = None) -> dict:
    """`rpc=None` skips what needs the chain, and the manifest says so."""
    root, out = Root.parse(root_uri), Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    days = _days(dt_from, dt_to)
    con = replay.connect([root_uri.rstrip("/") + "/"], memory_limit="4GB")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET threads = {min(8, os.cpu_count() or 2)}")  # run by hand, not inside the nightly's cap
    manifest: dict = {"from": dt_from, "to": dt_to, "chain": rpc is not None,
                      "made_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    rows, manifest["markets"] = markets(con, root, root_uri, days, rpc)
    pq.write_table(pa.Table.from_pylist(rows, schema=MARKETS), out / "markets.parquet", compression="zstd")
    surplus: list[dict] = []
    if rpc is not None:
        surplus, manifest["activity"] = activity_surplus(con, root, root_uri, days, rpc)
    pq.write_table(pa.Table.from_pylist(surplus, schema=SURPLUS), out / "activity_surplus.parquet")
    manifest["trade_reach"] = trade_reach(con, root, root_uri, days, surplus, out / "trade_reach.parquet")
    excluded = read_exclusions(Path(exclusions), days) if exclusions else []
    pq.write_table(pa.Table.from_pylist(excluded, schema=EXCLUSIONS), out / "exclusions.parquet")
    manifest["exclusions"] = len(excluded)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="book_pipeline.prepare", description=__doc__.splitlines()[0])
    p.add_argument("--root", required=True)
    p.add_argument("--from", dest="dt_from", required=True, help="first UTC day, YYYY-MM-DD")
    p.add_argument("--to", dest="dt_to", required=True, help="last UTC day, YYYY-MM-DD")
    p.add_argument("--out", required=True, help="local directory")
    p.add_argument("--exclusions", default=str(Path(__file__).resolve().parents[3] / "docs" / "data_exclusions.csv"))
    p.add_argument("--rpc", action="append", help="Polygon RPC; repeat")
    p.add_argument("--no-chain", action="store_true", help="no outcomes from chain and no surplus activity rows")
    a = p.parse_args(argv)
    rpc = None if a.no_chain else cc.Rpc(a.rpc or RESOLUTION_RPCS)
    print(json.dumps(run(a.root, a.dt_from, a.dt_to, a.out, a.exclusions, rpc), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
