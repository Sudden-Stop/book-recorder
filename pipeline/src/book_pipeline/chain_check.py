"""chain_check: completeness of the Polymarket `activity` feed against on-chain fills (Polygon).

    python -m book_pipeline.chain_check --root s3://bucket --dt 2026-11-18 --hour 12 [--hour 13 …]
        [--rpc URL] [--cache ./chain-cache] [--out report.json | --write-report]

The nightly run calls it for every curated Polymarket hour when `POLYGON_RPC_URL` is set (`nightly.py`).

Source of truth: every `OrderFilled` log of Polymarket's exchanges in the blocks of the window. Each order that
takes part in a match emits one `OrderFilled` (the resting maker orders and the taker order, whose `taker` is the
exchange itself), and the activity feed publishes one row per order filled: the row's `proxyWallet` is the log's
`maker` (the order's funder), `asset` its token, `side` its side and `size` its shares. So the reconciliation is a
multiset join on `(tx_hash, wallet, token, side, shares in 1e-6 units)`; within a key, rows are paired with the
fill closest in price, and price is then checked on every pair.

`OrderFilled` as emitted by the exchanges in use since late 2026 (the earlier CTF Exchange `0x4bFb…` and NegRisk
CTF Exchange `0xC5d5…` emit no fills any more; their only logs are cancellations):

    topics: [sig, orderHash, maker, taker]
    data:   [side (0 BUY, 1 SELL), tokenId, makerAmountFilled, takerAmountFilled, fee, ?, ?]   (uint256 words)

A BUY order gives collateral and receives shares (shares = takerAmountFilled), a SELL order the reverse. Amounts
have 6 decimals. Logs are queried by event signature only, so a new exchange contract shows up by itself (in
`unknown_exchanges`, which fails the check) instead of being missed.

Activity rows come from the curated `trades` table (`source == 'activity'`, already reduced to one copy across the
recorder's sockets), for the window's hours plus one hour on each side, so a fill whose row was received across an
hour boundary is not counted as missing. When a neighbouring hour is not curated (hour 23 during the nightly
run), the edge of the window next to it is left out of the count (`EDGE_S`). Recorder outages come from the raw
`activity` stream: silences in the union of the sockets' frames, with the `ctl: disconnect` reasons inside them.

RPC: `--rpc` (repeatable), else `$POLYGON_RPC_URL` followed by public RPCs. Each URL keeps its own `eth_getLogs`
block range, halved whenever that RPC refuses a range (Alchemy's free plan allows 10 blocks). Block times are taken
from the logs' `blockTimestamp` when the RPC returns it, otherwise fetched per block.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .storage import ReadStats, Root, iter_raw

ORDER_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
EXCHANGES = {  # address -> label
    "0xe111180000d2663c0091e4f400237545b87b996b": "ctf_exchange",
    "0xe2222d279d744050d28e00520010520000310f59": "neg_risk_exchange",
    "0xe3333700ca9d93003f00f0f71f8515005f6c00aa": "exchange_e333",
}
# Exchanges the activity feed is expected to cover. `exchange_e333` (its own conditional-token contract) never
# appears in the feed; it is reported but not part of the pass/fail figure.
IN_SCOPE = ("ctf_exchange", "neg_risk_exchange")
RPC_ENV = "POLYGON_RPC_URL"
PUBLIC_RPCS = ("https://polygon.drpc.org", "https://polygon-bor-rpc.publicnode.com")
ADDRESS_REQUIRED = {"https://polygon-bor-rpc.publicnode.com"}  # refuses eth_getLogs without an address filter
LOGS_RANGE = 40  # initial eth_getLogs block range per RPC
MARGIN_S = 120  # on-chain fills fetched this far around the window, so activity rows near its edges match
EDGE_S = 30  # next to a neighbouring hour that is not curated, this much of the window is not counted
SILENCE_MS = 3000  # a gap this long in the union of the sockets' frames counts as an outage
MIN_COMPLETENESS_PCT = 99.8  # in-scope fills present, outages aside; below it the check fails


def default_rpcs() -> tuple[str, ...]:
    own = os.environ.get(RPC_ENV, "").strip()
    return ((own,) if own else ()) + PUBLIC_RPCS


# --- RPC -----------------------------------------------------------------------------------------------------

class RpcError(RuntimeError):
    """The RPC answered and refused the request (JSON-RPC error or HTTP 4xx other than 429)."""


class Rpc:
    def __init__(self, urls=None):
        self.urls = list(urls or default_rpcs())
        self.logs_range = {u: LOGS_RANGE for u in self.urls}
        self.calls = 0

    def _post(self, url: str, body):
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json", "User-Agent": "rpc-client"})
        self.calls += 1
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                raise
            body = e.read()[:300].decode(errors="replace")
            raise RpcError(f"HTTP {e.code}: {body}") from None

    def call_one(self, url: str, method: str, params: list):
        """One URL: transport failures and rate limits are retried; a refusal raises RpcError at once."""
        last: Exception | None = None
        for attempt in range(4):
            try:
                j = self._post(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            except RpcError:
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = e
                time.sleep(1 + attempt * 2)
                continue
            err = j.get("error")
            if err:
                if err.get("code") in (429, -32005, -32090):  # rate limited
                    last = RpcError(str(err))
                    time.sleep(2 + attempt * 3)
                    continue
                raise RpcError(str(err)[:300])
            return j["result"]
        raise last or RuntimeError("no answer")

    def call(self, method: str, params: list):
        """First URL that answers."""
        last: Exception | None = None
        for u in self.urls:
            try:
                return self.call_one(u, method, params)
            except Exception as e:  # noqa: BLE001 - try the next RPC
                last = e
        raise last or RuntimeError("no RPC")

    def block_ts(self, n: int) -> int:
        return int(self.call("eth_getBlockByNumber", [hex(n), False])["timestamp"], 16)

    def head(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)


def block_at(rpc: Rpc, ts: int) -> int:
    """First block with timestamp >= ts."""
    hi = rpc.head()
    head_ts = rpc.block_ts(hi)
    guess = max(0, hi - (head_ts - ts) // 2)  # Polygon makes a block about every 2 s
    lo, hi = max(0, guess - 20_000), min(hi, guess + 20_000)
    while lo > 0 and rpc.block_ts(lo) >= ts:
        lo = max(0, lo - 50_000)
    while lo < hi:
        mid = (lo + hi) // 2
        if rpc.block_ts(mid) < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def get_logs(rpc: Rpc, b0: int, b1: int) -> list[dict]:
    """OrderFilled logs in blocks [b0, b1]. Each RPC's range is halved when it refuses one; an RPC that fails at
    a range of one block, or does not answer, hands over to the next."""
    out, b = [], b0
    while b <= b1:
        for u in rpc.urls:
            got = None
            while got is None:
                e = min(b1, b + rpc.logs_range[u] - 1)
                flt = {"fromBlock": hex(b), "toBlock": hex(e), "topics": [ORDER_FILLED]}
                if u in ADDRESS_REQUIRED:
                    flt["address"] = list(EXCHANGES)
                try:
                    got = rpc.call_one(u, "eth_getLogs", [flt])
                except RpcError:
                    if rpc.logs_range[u] == 1:
                        break
                    rpc.logs_range[u] = max(1, rpc.logs_range[u] // 2)
                except Exception:  # noqa: BLE001 - not answering: next RPC
                    break
            if got is not None:
                out += got
                b = e + 1
                break
        else:
            raise RuntimeError(f"no RPC returned logs for blocks {b}..")
    return out


def block_times(rpc: Rpc, blocks: list[int], batch: int = 50) -> dict[int, int]:
    out: dict[int, int] = {}
    for i in range(0, len(blocks), batch):
        chunk = blocks[i:i + batch]
        body = [{"jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), False]}
                for n in chunk]
        for u in rpc.urls:
            try:
                res = rpc._post(u, body)
                if isinstance(res, list) and all(r.get("result") for r in res):
                    out.update({r["id"]: int(r["result"]["timestamp"], 16) for r in res})
                    break
            except Exception:  # noqa: BLE001
                continue
        else:
            for n in chunk:
                out[n] = rpc.block_ts(n)
    return out


# --- on-chain fills ------------------------------------------------------------------------------------------

def decode(log: dict) -> dict:
    t, d = log["topics"], log["data"][2:]
    w = [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]
    side = "buy" if w[0] == 0 else "sell"
    maker_amt, taker_amt = w[2], w[3]
    shares, usdc = (taker_amt, maker_amt) if side == "buy" else (maker_amt, taker_amt)
    exchange = log["address"].lower()
    f = {
        "exchange": exchange, "tx_hash": log["transactionHash"].lower(), "block": int(log["blockNumber"], 16),
        "maker": "0x" + t[2][-40:], "side": side, "token_id": str(w[1]), "shares": shares, "usdc": usdc,
        "is_taker_order": ("0x" + t[3][-40:]) == exchange,
    }
    if log.get("blockTimestamp"):
        f["block_ts"] = int(log["blockTimestamp"], 16)
    return f


def fetch_fills(rpc: Rpc, t0: int, t1: int, cache: Path | None = None) -> tuple[list[dict], dict]:
    """Decoded OrderFilled logs with block time in [t0, t1) (seconds), plus fetch info."""
    key = cache / f"orderfilled-{t0}-{t1}.json" if cache else None
    if key and key.exists():
        blob = json.loads(key.read_text())
        return blob["fills"], blob["info"]
    started = time.time()
    b0, b1 = block_at(rpc, t0), block_at(rpc, t1) - 1
    fills = [decode(lg) for lg in get_logs(rpc, b0, b1) if not lg.get("removed")]
    need = sorted({f["block"] for f in fills if "block_ts" not in f})
    if need:
        times = block_times(rpc, need)
        for f in fills:
            f.setdefault("block_ts", times.get(f["block"]))
    info = {"blocks": [b0, b1], "logs": len(fills), "rpc_calls": rpc.calls, "fetch_s": round(time.time() - started),
            "logs_range": {_host(u): r for u, r in rpc.logs_range.items()}}
    if key:
        key.parent.mkdir(parents=True, exist_ok=True)
        key.write_text(json.dumps({"fills": fills, "info": info}))
    return fills, info


def _host(url: str) -> str:
    """URL without path (an API key may sit in the path)."""
    return url.split("/")[2] if "://" in url else url


# --- activity rows and outages -------------------------------------------------------------------------------

def hour_list(dt: str, hours: list[str], pad: int) -> list[tuple[str, str]]:
    starts = sorted(datetime.strptime(f"{dt} {int(h):02d}", "%Y-%m-%d %H") for h in hours)
    out = []
    d, end = starts[0] - timedelta(hours=pad), starts[-1] + timedelta(hours=pad)
    while d <= end:
        out.append((d.strftime("%Y-%m-%d"), d.strftime("%H")))
        d += timedelta(hours=1)
    return out


def trades_path(root: Root, dt: str, h: str) -> str:
    return root.join("curated", "trades", "venue=polymarket", f"dt={dt}", f"part-{h}.parquet")


def micro(x) -> int:
    return int((Decimal(str(x)) * 1_000_000).to_integral_value())


def load_activity(root: Root, hours: list[tuple[str, str]], lo_s: int, hi_s: int) -> tuple[list[dict], list[str]]:
    """Activity rows of the curated hours with exchange time in [lo_s, hi_s)."""
    rows, missing = [], []
    cols = ["tx_hash", "wallet", "token_id", "side", "size", "price", "exchange_ts", "source"]
    for dt, h in hours:
        path = trades_path(root, dt, h)
        if not root.exists(path):
            missing.append(f"{dt}T{h}")
            continue
        t = pq.read_table(path, filesystem=root.fs, columns=cols)
        ts = t["exchange_ts"]
        per_s = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[ts.type.unit]
        sec = pc.divide(pc.cast(ts, pa.int64()), per_s)
        keep = pc.and_(pc.equal(t["source"], "activity"),
                       pc.and_(pc.greater_equal(sec, lo_s), pc.less(sec, hi_s)))
        rows += t.filter(pc.fill_null(keep, False)).drop_columns(["source"]).to_pylist()
    return rows, missing


def outages(root: Root, hours: list[tuple[str, str]]) -> list[dict]:
    """Windows with no activity frame on any socket: from the last frame before to the first frame after.

    Each carries the `ctl: disconnect` reasons seen inside it (empty for a silence the recorder did not notice).
    """
    frames: list[int] = []
    ctl: list[tuple[int, str]] = []
    for dt, h in hours:
        for ln in iter_raw(root, "polymarket", "activity", dt, h, ReadStats(), "local"):
            rec = ln.rec
            ts = rec.get("recv_ms")
            if not isinstance(ts, int):
                continue
            if rec.get("ctl"):
                if rec["ctl"] == "disconnect":
                    ctl.append((ts, str(rec.get("reason"))))
            else:
                frames.append(ts)
    frames.sort()
    out = []
    for a, b in zip(frames, frames[1:]):
        if b - a >= SILENCE_MS:
            out.append({"from_ms": a, "to_ms": b, "s": round((b - a) / 1000, 1),
                        "disconnects": [r for t, r in ctl if a <= t <= b]})
    return out


# --- reconciliation ------------------------------------------------------------------------------------------

def fill_key(f: dict) -> tuple:
    return (f["tx_hash"], f["maker"], f["token_id"], f["side"], f["shares"])


def row_key(r: dict) -> tuple:
    return ((r["tx_hash"] or "").lower(), (r["wallet"] or "").lower(), r["token_id"], r["side"], micro(r["size"]))


def _price(f: dict) -> float:
    return f["usdc"] / f["shares"] if f["shares"] else 0.0


def _ms(v) -> int:
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    return int(v)


def reconcile(fills: list[dict], rows: list[dict], t0: int, t1: int, gaps: list[dict]) -> dict:
    """Multiset join. Counts on-chain fills with block time in [t0, t1) and activity rows with exchange_ts there."""
    by_key_rows: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by_key_rows[row_key(r)].append(r)
    by_key_fills: dict[tuple, list[dict]] = defaultdict(list)
    for f in fills:
        by_key_fills[fill_key(f)].append(f)

    matched, unmatched_f, unmatched_r = [], [], []
    for k, fs in by_key_fills.items():
        # one wallet can fill several equal-size orders in a transaction at different prices: give each row the
        # remaining fill closest in price, so the fills left over are the ones the feed did not send
        left = list(fs)
        for r in by_key_rows.get(k, [])[:len(fs)]:
            p = float(r["price"])
            f = min(left, key=lambda f: abs(_price(f) - p))
            left.remove(f)
            matched.append((f, r))
        unmatched_f += left
    for k, rs in by_key_rows.items():
        unmatched_r += rs[len(by_key_fills.get(k, ())):]

    def in_win(f):
        return t0 <= f["block_ts"] < t1

    def row_in_win(r):
        return r["exchange_ts"] is not None and t0 * 1000 <= _ms(r["exchange_ts"]) < t1 * 1000

    expected = [f for f in fills if in_win(f)]
    missing = [f for f in unmatched_f if in_win(f)]
    extra = [r for r in unmatched_r if row_in_win(r)]
    bad_price = sum(1 for f, r in matched if abs(float(r["price"]) - _price(f)) > 1e-4)

    fill_txs = {f["tx_hash"] for f in fills}
    rows_by_tx: Counter = Counter((r["tx_hash"] or "").lower() for r in rows)
    spans = [(g["from_ms"] / 1000 - 3, g["to_ms"] / 1000 + 1, g) for g in gaps]

    def cause(f: dict) -> str:
        for a, b, g in spans:
            if a <= f["block_ts"] <= b:
                return f"outage:{g['disconnects'][0]}" if g["disconnects"] else "outage:silence"
        return "tx_partially_in_feed" if rows_by_tx.get(f["tx_hash"]) else "tx_absent_from_feed"

    label = lambda f: EXCHANGES.get(f["exchange"], f["exchange"])  # noqa: E731
    causes = {id(f): cause(f) for f in missing}

    def summarise(fs: list[dict]) -> dict:
        return {
            "n": len(fs),
            "by_cause": dict(Counter(causes[id(f)] for f in fs).most_common()),
            "by_exchange": dict(Counter(label(f) for f in fs).most_common()),
            "by_order_role": dict(Counter("taker" if f["is_taker_order"] else "maker" for f in fs).most_common()),
            "shares": round(sum(f["shares"] for f in fs) / 1e6, 2),
            "usdc": round(sum(f["usdc"] for f in fs) / 1e6, 2),
        }

    def pct(a, b):
        return round(100 * a / b, 4) if b else None

    exp_by_ex, miss_by_ex = Counter(map(label, expected)), Counter(map(label, missing))
    scope_n = sum(exp_by_ex[x] for x in IN_SCOPE)
    scope_missing = [f for f in missing if label(f) in IN_SCOPE]
    scope_outage = sum(1 for f in scope_missing if causes[id(f)].startswith("outage:"))
    usdc_exp = sum(f["usdc"] for f in expected)
    return {
        "onchain_fills": len(expected),
        "onchain_txs": len({f["tx_hash"] for f in expected}),
        "activity_rows_in_window": sum(1 for r in rows if row_in_win(r)),
        "matched": len(expected) - len(missing),
        "completeness_pct": pct(len(expected) - len(missing), len(expected)),
        "completeness_pct_usdc": pct(usdc_exp - sum(f["usdc"] for f in missing), usdc_exp),
        "in_scope": {
            "exchanges": list(IN_SCOPE), "fills": scope_n, "missing": len(scope_missing),
            "missing_in_outages": scope_outage,
            "completeness_pct": pct(scope_n - len(scope_missing), scope_n),
            # lost while the recorder was not listening (all sockets down) is not held against the feed
            "completeness_excl_outages_pct": pct(scope_n - len(scope_missing) + scope_outage, scope_n),
        },
        "completeness_by_exchange": {ex: {"fills": n, "missing": miss_by_ex.get(ex, 0),
                                          "pct": pct(n - miss_by_ex.get(ex, 0), n)}
                                     for ex, n in exp_by_ex.most_common()},
        "price_mismatches": bad_price,
        "missing": summarise(missing),
        "missing_examples": [{k: f[k] for k in ("tx_hash", "block_ts", "exchange", "maker", "side", "token_id",
                                                "shares", "usdc", "is_taker_order")} | {"cause": causes[id(f)]}
                             for f in missing[:20]],
        "extra_activity_rows": {
            "n": len(extra),
            "tx_on_chain": sum(1 for r in extra if (r["tx_hash"] or "").lower() in fill_txs),
            "examples": [{k: (str(v) if k in ("size", "price", "exchange_ts") else v) for k, v in r.items()}
                         for r in extra[:20]],
        },
    }


def problems(rep: dict, min_pct: float = MIN_COMPLETENESS_PCT) -> list[str]:
    """What makes the check fail; empty when it passes."""
    out = []
    if not rep["onchain_fills"]:
        out.append("no on-chain fills in the window")
    if rep["unknown_exchanges"]:
        out.append(f"OrderFilled from unknown exchanges: {rep['unknown_exchanges']}")
    if rep["extra_activity_rows"]["n"]:
        out.append(f"{rep['extra_activity_rows']['n']} activity rows without an on-chain fill")
    if rep["price_mismatches"]:
        out.append(f"{rep['price_mismatches']} matched fills with a different price")
    c = rep["in_scope"]["completeness_excl_outages_pct"]
    if c is not None and c < min_pct:
        out.append(f"in-scope completeness outside outages {c}% < {min_pct}%")
    return out


def report_path(root: Root, dt: str, hour: str) -> str:
    return root.join("reports", "chain", "venue=polymarket", f"dt={dt}", f"hour-{hour}.json")


def run(root_uri: str, dt: str, hours: list[str], cache: str | None = None, rpcs=None,
        min_pct: float = MIN_COMPLETENESS_PCT, write: bool = False) -> dict:
    hours = sorted(f"{int(h):02d}" for h in hours)
    root = Root.parse(root_uri)
    t0 = int(datetime.strptime(f"{dt} {hours[0]}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc).timestamp())
    t1 = int(datetime.strptime(f"{dt} {hours[-1]}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc).timestamp()) + 3600
    rpc = Rpc(rpcs)
    fills, info = fetch_fills(rpc, t0 - MARGIN_S, t1 + MARGIN_S, Path(cache) if cache else None)
    around = hour_list(dt, hours, pad=1)
    rows, missing_parts = load_activity(root, around, t0 - MARGIN_S, t1 + MARGIN_S)
    if f"{around[0][0]}T{around[0][1]}" in missing_parts:
        t0 += EDGE_S
    if f"{around[-1][0]}T{around[-1][1]}" in missing_parts:
        t1 -= EDGE_S
    gaps = [g for g in outages(root, hour_list(dt, hours, pad=0))
            if g["to_ms"] >= t0 * 1000 and g["from_ms"] < t1 * 1000]
    rep = {"venue": "polymarket", "dt": dt, "hours": hours, "window_s": [t0, t1], "fetch": info,
           "curated_parts_missing": missing_parts,
           "unknown_exchanges": sorted({f["exchange"] for f in fills} - set(EXCHANGES)),
           **reconcile(fills, rows, t0, t1, gaps),
           "outages": gaps,
           "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    rep["problems"] = problems(rep, min_pct)
    rep["ok"] = not rep["problems"]
    if write:
        path = report_path(root, dt, hours[0] if len(hours) == 1 else f"{hours[0]}-{hours[-1]}")
        root.makedirs(path)
        with root.fs.open_output_stream(path) as f:
            f.write(json.dumps(rep, indent=2, default=str).encode())
    return rep


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m book_pipeline.chain_check", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", required=True, help="storage root with raw/ and curated/ (s3://bucket or a dir)")
    p.add_argument("--dt", required=True)
    p.add_argument("--hour", required=True, action="append", help="UTC hour; repeat for a contiguous window")
    p.add_argument("--cache", help="directory caching on-chain logs between runs")
    p.add_argument("--rpc", action="append", help=f"Polygon RPC URL (default: ${RPC_ENV}, then public RPCs)")
    p.add_argument("--min-completeness", type=float, default=MIN_COMPLETENESS_PCT)
    p.add_argument("--out", help="write the JSON report to this file")
    p.add_argument("--write-report", action="store_true", help="write it under <root>/reports/chain/")
    a = p.parse_args(argv)
    rep = run(a.root, a.dt, a.hour, a.cache, tuple(a.rpc) if a.rpc else None, a.min_completeness,
              write=a.write_report)
    if a.out:
        Path(a.out).write_text(json.dumps(rep, indent=2, default=str))
    print(json.dumps({k: rep[k] for k in ("ok", "problems", "onchain_fills", "completeness_pct", "in_scope")},
                     default=str))
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
