"""Replay check: rebuild each token's book from `book_deltas` and compare it with the REST `book_snapshots`.

For every pair of consecutive snapshots S[k-1], S[k] of one (host, token), the book starts as S[k-1] and the
delta rows that arrived after S[k-1]'s time and up to S[k]'s time are applied in arrival order (host, line,
item). Times are venue `exchange_ts` when present, else `recv_ts`. Because arrival order and venue timestamps
do not agree exactly, the boundary is "the first delta whose running-max timestamp passes the snapshot time".

Outcomes per interval:
- `exact`: the replayed book equals S[k] level for level at the boundary;
- `window`: not exact, but some replayed state within +/- `window_ms` of S[k]'s time equals it (clock skew,
  REST snapshot taken mid-burst);
- `top`: neither, but best bid/ask prices and sizes agree at the boundary;
- `mismatch`: none of the above;
- `skipped`: a `gap` row with no later `reset` before the boundary (book unknown), or no delta stream for the
  token on that host.

A `reset` (WebSocket full book) inside an interval replaces the book, as the live feed would.
Match rate = (exact + window) / evaluated, evaluated = all intervals minus skipped.
"""
from __future__ import annotations

import os
import tempfile
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

SCALE = 1_000_000


@dataclass
class TokenResult:
    host: str
    token_id: str
    market_id: str | None
    outcomes: Counter = field(default_factory=Counter)
    examples: list[dict] = field(default_factory=list)


def _key(book):
    bids, asks = book
    return (frozenset(bids.items()), frozenset(asks.items()))


def _top(book):
    bids, asks = book
    bb = max(bids) if bids else None
    ba = min(asks) if asks else None
    return (bb, bids.get(bb), ba, asks.get(ba))


def _apply(book, ev) -> None:
    action, side, price, size = ev[1], ev[2], ev[3], ev[4]
    if action != "set":
        return
    lv = book[0] if side == "bid" else book[1]
    if size <= 0:
        lv.pop(price, None)
    else:
        lv[price] = size


def replay_token(snaps: list[tuple], events: list[tuple], window_ms: int, res: TokenResult) -> None:
    """snaps: [(t, bids{p:s}, asks{p:s})] sorted by t; events: [(t, action, side, price, size, reset_levels)]
    in arrival order, reset_levels a pre-built book for `reset` rows."""
    if not events:
        res.outcomes["skipped"] += max(0, len(snaps) - 1)
        return
    runmax = []
    m = None
    for ev in events:
        m = ev[0] if m is None else max(m, ev[0])
        runmax.append(m)

    def boundary(t):  # number of events whose running-max time <= t
        return bisect_right(runmax, t)

    lines = [ev[6] if len(ev) > 6 else -1 for ev in events]

    for k in range(1, len(snaps)):
        t_prev, t_k = snaps[k - 1][0], snaps[k][0]
        target = (snaps[k][1], snaps[k][2])
        target_key = _key(target)
        if len(snaps[k]) > 4 and snaps[k][3] == "ws" and snaps[k - 1][3] == "ws":
            # socket snapshots are `reset` rows in the same stream: bound by stream position, not time,
            # and leave both snapshots' own resets out (else the replay would compare a book with itself)
            a, b = bisect_right(lines, snaps[k - 1][4]), bisect_left(lines, snaps[k][4])
            w_lo = w_hi = b
        else:
            a, b = boundary(t_prev), boundary(t_k)
            w_lo, w_hi = boundary(t_k - window_ms), boundary(t_k + window_ms)
        book = (dict(snaps[k - 1][1]), dict(snaps[k - 1][2]))
        valid, boundary_valid = True, True
        exact = top_ok = in_window = False
        stop = max(b, w_hi)
        i = a  # loop invariant: `book` is the state after events[a:i]
        while True:
            if i == b:
                boundary_valid = valid
                if valid:
                    exact = _key(book) == target_key
                    top_ok = _top(book) == _top(target)
            if valid and not in_window and w_lo <= i <= w_hi and (i == b and exact or _key(book) == target_key):
                in_window = True
            if i >= stop:
                break
            ev = events[i]
            if ev[1] == "reset":
                book = (dict(ev[5][0]), dict(ev[5][1]))
                valid = True
            elif ev[1] == "gap":
                valid = False
            elif valid:
                _apply(book, ev)
            i += 1
        if exact:
            res.outcomes["exact"] += 1
        elif not boundary_valid:
            res.outcomes["skipped"] += 1
        elif in_window:
            res.outcomes["window"] += 1
        elif top_ok:
            res.outcomes["top"] += 1
        else:
            res.outcomes["mismatch"] += 1
            if len(res.examples) < 3:
                res.examples.append(_explain(book, target, t_k))


def _explain(book, target, t) -> dict:
    diff = 0
    for side in (0, 1):
        got, want = book[side], target[side]
        diff += sum(1 for p in set(got) | set(want) if got.get(p) != want.get(p))
    return {"t": t, "levels_differing": diff, "replayed_top": [None if x is None else x / SCALE for x in _top(book)],
            "snapshot_top": [None if x is None else x / SCALE for x in _top(target)]}


def connect(paths, memory_limit: str = "512MB") -> duckdb.DuckDBPyConnection:
    """DuckDB connection with a memory cap; loads httpfs and the AWS credential chain for s3:// paths
    (that path is untested here: this machine has no S3 access)."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    # an in-memory database only spills large sorts to disk when it has a temp directory
    spill = os.environ.get("BOOK_DUCKDB_TEMP") or os.path.join(tempfile.gettempdir(), "book_pipeline_duckdb")
    os.makedirs(spill, exist_ok=True)
    con.execute(f"SET temp_directory='{Path(spill).as_posix()}'")
    flat = [paths] if isinstance(paths, str) else list(paths)
    if any(str(p).startswith("s3://") for p in flat):
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws")
        con.execute("CREATE OR REPLACE SECRET s3 (TYPE s3, PROVIDER credential_chain)")
    return con


def run(deltas_glob: str | list[str], snapshots_glob: str | list[str], window_ms: int = 2000,
        memory_limit: str = "512MB") -> dict:
    con = connect([*([deltas_glob] if isinstance(deltas_glob, str) else deltas_glob)], memory_limit)
    con.execute("CREATE TEMP TABLE s AS SELECT host, token_id, any_value(market_id) OVER (PARTITION BY token_id) "
                "AS market_id, epoch_ms(coalesce(exchange_ts, recv_ts)) AS t, source, line, "
                "list_transform(bids, lambda x: [CAST(x.price * 1000000 AS BIGINT), CAST(x.size * 1000000 AS BIGINT)]) AS b, "
                "list_transform(asks, lambda x: [CAST(x.price * 1000000 AS BIGINT), CAST(x.size * 1000000 AS BIGINT)]) AS a "
                "FROM read_parquet(?)", [snapshots_glob])
    snaps: dict[tuple[str, str], list] = defaultdict(list)
    markets: dict[str, str | None] = {}
    duplicates: Counter = Counter()
    # con.sql, not con.execute: with preserve_insertion_order=false, DuckDB 2.0's execute() result readers do not
    # keep ORDER BY across batches (checked on the fixtures).
    for host, tok, market, t, source, line, b, a in con.sql(
            "SELECT host, token_id, market_id, t, source, line, b, a FROM s ORDER BY host, token_id, t, line").fetchall():
        snap = (t, {p: s for p, s in b}, {p: s for p, s in a}, source, line)
        prev = snaps[(host, tok)][-1] if snaps[(host, tok)] else None
        if prev is not None and prev[:3] == snap[:3]:
            duplicates[(host, tok)] += 1  # same book at the same venue time: nothing to replay
            continue
        snaps[(host, tok)].append(snap)
        markets[tok] = market
    results: dict[tuple[str, str], TokenResult] = {
        k: TokenResult(k[0], k[1], markets.get(k[1])) for k in snaps}
    for k, n in duplicates.items():
        results[k].outcomes["duplicate_snapshots"] += n

    rel = con.sql(
        "SELECT host, token_id, epoch_ms(coalesce(exchange_ts, recv_ts)) AS t, action, side, "
        "CAST(price * 1000000 AS BIGINT) AS p, CAST(size * 1000000 AS BIGINT) AS z, line "
        "FROM read_parquet(?) WHERE token_id IN (SELECT DISTINCT token_id FROM s) "
        "ORDER BY host, token_id, line, item", params=[deltas_glob])
    # DuckDB 2.x: to_arrow_reader; 1.x: fetch_record_batch
    reader = (rel.to_arrow_reader(100_000) if hasattr(rel, "to_arrow_reader") else rel.fetch_record_batch(100_000))
    cur_key, cur_events = None, []
    reset_line, reset_book = None, None

    def finish(key, events):
        if key in snaps:
            replay_token(snaps[key], events, window_ms, results[key])

    seen = set()
    for batch in reader:
        cols = batch.to_pydict()
        for host, tok, t, action, side, p, z, line in zip(cols["host"], cols["token_id"], cols["t"], cols["action"],
                                                          cols["side"], cols["p"], cols["z"], cols["line"]):
            key = (host, tok)
            if key != cur_key:
                if key in seen:
                    raise RuntimeError(f"deltas for {key} are not contiguous; the ORDER BY was not honoured")
                if cur_key is not None:
                    finish(cur_key, cur_events)
                    seen.add(cur_key)
                cur_key, cur_events, reset_line, reset_book = key, [], None, None
            if action == "reset":
                reset_book = ({}, {})
                reset_line = line
                cur_events.append((t, "reset", None, None, None, reset_book, line))
            elif action == "set" and line == reset_line:
                # levels of the full book that just reset: fill the reset's book instead of separate events
                if z > 0:
                    (reset_book[0] if side == "bid" else reset_book[1])[p] = z
            else:
                reset_line = None
                cur_events.append((t, action, side, p, z, None, line))
    if cur_key is not None:
        finish(cur_key, cur_events)
        seen.add(cur_key)
    for key in snaps:
        if key not in seen:
            replay_token(snaps[key], [], window_ms, results[key])

    total = Counter()
    per_token = []
    for r in results.values():
        total.update(r.outcomes)
        ev = sum(r.outcomes[k] for k in ("exact", "window", "top", "mismatch"))
        per_token.append({"host": r.host, "token_id": r.token_id, "market_id": r.market_id, **dict(r.outcomes),
                          "evaluated": ev,
                          "match_rate": (r.outcomes["exact"] + r.outcomes["window"]) / ev if ev else None,
                          "examples": r.examples})
    evaluated = sum(total[k] for k in ("exact", "window", "top", "mismatch"))
    return {
        "window_ms": window_ms,
        "tokens": len(results),
        "intervals": evaluated + total["skipped"],
        "evaluated": evaluated,
        "duplicate_snapshots": total["duplicate_snapshots"],
        "outcomes": {k: total[k] for k in ("exact", "window", "top", "mismatch", "skipped")},
        "exact_rate": total["exact"] / evaluated if evaluated else None,
        "match_rate": (total["exact"] + total["window"]) / evaluated if evaluated else None,
        "per_token": sorted(per_token, key=lambda r: (r["market_id"] or "", r["token_id"])),
    }
