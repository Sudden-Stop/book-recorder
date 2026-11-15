"""Polymarket raw frames -> curated rows.

Frame semantics follow `data-ingestion/apps/book-compact/src/flatten-events.ts` and
`data-ingestion/notebooks/mm_data.py::load_tape`:

- market channel (`book_events`): a frame is one event or a list of events; `event_type` is `book` (full book
  of one asset, also sent after every trade and on (re)subscribe), `price_change` (`price_changes[]`, each with
  its own `asset_id`, `side` BUY = bid / SELL = ask, `size` = new absolute size, 0 removes the level, plus the
  venue's `best_bid`/`best_ask` after the change), `last_trade_price` (taker side) and `tick_size_change`. An
  event without `event_type` but with `bids` is a `book`. A `payload` wrapper is unwrapped.
- REST book (`book_snapshots`): `GET /book` (one object) or `POST /books` (a list) as received.
- activity feed (`activity`): `{"type":"trades","topic":"activity","payload":{…}}`; `payload.timestamp` is in
  seconds, prices and sizes are binary floats.
- `markets`: Gamma market records (a list, or `{"markets": [...]}` / `{"data": [...]}`, or one record).
- `universe`: the recorder's selection records (`top` or `markets` list of `{rank, conditionId, slug, tokens…}`).
"""
from __future__ import annotations

from ..storage import RawLine, frame_of
from ..writer import dec
from .common import (Out, as_bool, as_float, as_int, as_list, dumps, first, iso_ms, text, to_ms)

VENUE = "polymarket"
# stream -> handler, in processing order (metadata first: it maps tokens to markets)
STREAMS = (("markets", "markets"), ("universe", "universe"), ("book_events", "book_event"),
           ("book_snapshots", "snapshot"), ("activity", "activity"))
TABLES = ("book_deltas", "book_snapshots", "trades", "markets", "coverage")

K_BOOK = frozenset({"market", "asset_id", "timestamp", "hash", "bids", "asks", "event_type", "tick_size",
                    "last_trade_price", "min_order_size", "neg_risk"})
K_PC = frozenset({"market", "price_changes", "timestamp", "event_type"})
K_PC_ITEM = frozenset({"asset_id", "price", "size", "side", "hash", "best_bid", "best_ask"})
K_LTP = frozenset({"market", "asset_id", "price", "size", "fee_rate_bps", "side", "timestamp", "event_type",
                   "transaction_hash"})
K_TICK = frozenset({"market", "asset_id", "old_tick_size", "new_tick_size", "timestamp", "event_type", "side"})
SIDE = {"BUY": "bid", "SELL": "ask", "buy": "bid", "sell": "ask"}


def _levels(raw) -> list[tuple[str, str]]:
    out = []
    for lv in raw or ():
        if isinstance(lv, dict):
            p, s = dec(lv.get("price")), dec(lv.get("size"))
        elif isinstance(lv, (list, tuple)) and len(lv) >= 2:
            p, s = dec(lv[0]), dec(lv[1])
        else:
            continue
        if p is not None and s is not None:
            out.append((p, s))
    return out


def _events(frame):
    if isinstance(frame, list):
        for f in frame:
            yield from _events(f)
    elif isinstance(frame, dict):
        body = frame.get("payload") if isinstance(frame.get("payload"), dict) else frame
        yield body


# --- book_events ---------------------------------------------------------------------------------------------

def book_event(ln: RawLine, out: Out) -> None:
    rec = ln.rec
    recv = to_ms(rec.get("recv_ms"))
    conn = as_int(rec.get("conn"))
    base = {"host": ln.host, "conn": conn, "line": ln.line, "recv_ts": recv}
    ctl = rec.get("ctl")
    if ctl:
        before = out.coverage.ctl(ln.host, conn, recv, str(ctl), rec.get("assets"))
        out.stats[f"ctl.{ctl}"] += 1
        if ctl in ("disconnect", "seq_gap"):
            for i, tok in enumerate(sorted(before)):
                out.row("book_deltas", {**base, "item": i, "token_id": tok, "market_id": out.token_market.get(tok),
                                        "action": "gap"})
        return
    frame = frame_of(rec)
    if frame is None:
        out.stats["frames.unparsed"] += 1
        return
    item = 0
    for ev in _events(frame):
        et = ev.get("event_type") or ev.get("type") or ("book" if "bids" in ev else None)
        if et == "book":
            item = _book(ev, base, item, out)
        elif et == "price_change":
            item = _price_change(ev, base, item, out)
        elif et == "last_trade_price":
            out.keys("last_trade_price", ev, K_LTP)
            tok = text(first(ev, "asset_id", "assetId", "asset"))
            price, size = dec(ev.get("price")), dec(ev.get("size"))
            if not tok or price is None or size is None:
                out.stats["skipped.last_trade_price"] += 1
                continue
            market = _market(ev, tok, out)
            out.row("trades", {**base, "item": item, "token_id": tok, "market_id": market,
                               "exchange_ts": to_ms(ev.get("timestamp")), "source": "market_ws",
                               "price": price, "size": size, "side": _side(ev.get("side")),
                               "fee_rate_bps": as_int(ev.get("fee_rate_bps")),
                               "tx_hash": text(first(ev, "transaction_hash", "transactionHash"))})
            out.coverage.frame(ln.host, conn, recv, tok, "n_trades_ws")
            item += 1
        elif et == "tick_size_change":
            out.keys("tick_size_change", ev, K_TICK)
            tok = text(first(ev, "asset_id", "assetId", "asset"))
            if not tok:
                continue
            out.row("book_deltas", {**base, "item": item, "token_id": tok, "market_id": _market(ev, tok, out),
                                    "exchange_ts": to_ms(ev.get("timestamp")), "action": "tick_size",
                                    "tick_size": dec(first(ev, "new_tick_size", "newTickSize"))})
            out.coverage.frame(ln.host, conn, recv, tok, "n_tick_size_changes")
            item += 1
        else:
            out.unknown_event_types[str(et)] += 1


def _market(ev: dict, tok: str, out: Out) -> str | None:
    m = text(first(ev, "market", "condition_id", "conditionId"))
    if m:
        out.token_market.setdefault(tok, m)
        return m
    return out.token_market.get(tok)


def _side(s) -> str | None:
    return s.lower() if isinstance(s, str) and s.lower() in ("buy", "sell") else None


def _book(ev: dict, base: dict, item: int, out: Out) -> int:
    out.keys("book", ev, K_BOOK)
    tok = text(first(ev, "asset_id", "assetId", "asset"))
    if not tok:
        out.stats["skipped.book"] += 1
        return item
    market = _market(ev, tok, out)
    ts = to_ms(ev.get("timestamp"))
    common = {**base, "token_id": tok, "market_id": market, "exchange_ts": ts}
    out.row("book_deltas", {**common, "item": item, "action": "reset", "book_hash": text(ev.get("hash")),
                            "tick_size": dec(first(ev, "tick_size", "tickSize"))})
    item += 1
    for side, key in (("bid", "bids"), ("ask", "asks")):
        for p, s in _levels(ev.get(key)):
            out.row("book_deltas", {**common, "item": item, "action": "set", "side": side, "price": p, "size": s})
            item += 1
    out.coverage.frame(base["host"], base["conn"], base["recv_ts"], tok, "n_book")
    return item


def _price_change(ev: dict, base: dict, item: int, out: Out) -> int:
    out.keys("price_change", ev, K_PC)
    fallback = text(first(ev, "asset_id", "assetId"))
    ts = to_ms(ev.get("timestamp"))
    changes = first(ev, "price_changes", "priceChanges", "changes") or []
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        out.keys("price_change.item", ch, K_PC_ITEM)
        tok = text(first(ch, "asset_id", "assetId", "asset")) or fallback
        side = SIDE.get(str(ch.get("side")))
        p, s = dec(ch.get("price")), dec(ch.get("size"))
        if not tok or side is None or p is None or s is None:
            out.stats["skipped.price_change"] += 1
            continue
        out.row("book_deltas", {**base, "item": item, "token_id": tok, "market_id": _market(ev, tok, out),
                                "exchange_ts": ts, "action": "set", "side": side, "price": p, "size": s,
                                "best_bid": dec(first(ch, "best_bid", "bestBid")),
                                "best_ask": dec(first(ch, "best_ask", "bestAsk")),
                                "book_hash": text(ch.get("hash"))})
        out.coverage.frame(base["host"], base["conn"], base["recv_ts"], tok, "n_level_changes")
        item += 1
    return item


# --- book_snapshots ------------------------------------------------------------------------------------------

def snapshot(ln: RawLine, out: Out) -> None:
    """Recorder line: `{"asset","recv_ms","req_ms","status","msg":<REST /book body>}` (legacy dumps also read)."""
    rec = ln.rec
    if rec.get("ctl"):
        return
    status = as_int(rec.get("status"))
    if status is not None and status != 200:
        out.stats[f"snapshot.http_{status}"] += 1
        return
    frame = frame_of(rec)
    recv = to_ms(first(rec, "recv_ms", "ts_ms"))
    req = to_ms(rec.get("req_ms"))
    line_asset = text(rec.get("asset")) if "msg" in rec else None
    for ev in _events(frame):
        tok = text(first(ev, "asset_id", "assetId", "asset")) or line_asset
        if not tok or ("bids" not in ev and "asks" not in ev):
            out.stats["skipped.snapshot"] += 1
            continue
        bids = sorted(_levels(ev.get("bids")), key=lambda x: -float(x[0]))
        asks = sorted(_levels(ev.get("asks")), key=lambda x: float(x[0]))
        out.row("book_snapshots", {
            "host": ln.host, "line": ln.line, "recv_ts": recv, "req_ts": req, "source": "rest", "token_id": tok,
            "market_id": _market(ev, tok, out),
            "exchange_ts": to_ms(first(ev, "timestamp", "exchange_ms")),
            "book_hash": text(ev.get("hash")),
            "tick_size": dec(first(ev, "tick_size", "tickSize")),
            "min_order_size": dec(first(ev, "min_order_size", "minOrderSize")),
            "neg_risk": as_bool(ev.get("neg_risk")),
            "bids": bids, "asks": asks,
        })
        out.coverage.count(ln.host, tok, "n_snapshots")


# --- activity ------------------------------------------------------------------------------------------------

def activity(ln: RawLine, out: Out) -> None:
    rec = ln.rec
    if rec.get("ctl"):
        out.stats[f"activity.ctl.{rec['ctl']}"] += 1
        return
    frame = frame_of(rec)
    if not isinstance(frame, dict):
        out.stats["activity.unparsed"] += 1
        return
    kind = frame.get("type")
    p = frame.get("payload")
    if kind != "trades" or not isinstance(p, dict):
        out.unknown_event_types[f"activity:{kind}"] += 1
        return
    tok = text(p.get("asset"))
    price, size = dec(as_float(p.get("price"))), dec(as_float(p.get("size")))
    if not tok or price is None or size is None:
        out.stats["skipped.activity"] += 1
        return
    market = text(p.get("conditionId"))
    if market:
        out.token_market.setdefault(tok, market)
    wallet = text(p.get("proxyWallet"))
    out.row("trades", {
        "host": ln.host, "conn": as_int(rec.get("conn")), "line": ln.line, "item": 0,
        "recv_ts": to_ms(rec.get("recv_ms")), "token_id": tok, "market_id": market or out.token_market.get(tok),
        "exchange_ts": to_ms(p.get("timestamp"), seconds_if_small=True), "source": "activity",
        "price": price, "size": size, "side": _side(p.get("side")),
        "tx_hash": text(p.get("transactionHash")), "wallet": wallet.lower() if wallet else None,
        "outcome": text(p.get("outcome")), "outcome_index": as_int(p.get("outcomeIndex")),
        "event_slug": text(first(p, "eventSlug", "slug")),
    })
    out.coverage.count(ln.host, tok, "n_trades_activity")


# --- markets / universe --------------------------------------------------------------------------------------

def _tokens(m: dict) -> list[dict] | None:
    toks = m.get("tokens")
    if isinstance(toks, list) and toks:
        out = []
        for t in toks:
            if isinstance(t, dict):
                tid = text(first(t, "tokenId", "token_id", "asset_id"))
                if tid:
                    out.append({"token_id": tid, "outcome": text(t.get("outcome"))})
        return out or None
    ids, outcomes = as_list(m.get("clobTokenIds")), as_list(m.get("outcomes")) or []
    if not ids:
        return None
    return [{"token_id": str(t), "outcome": text(outcomes[i]) if i < len(outcomes) else None}
            for i, t in enumerate(ids)]


def _resolved(m: dict, tokens) -> str | None:
    if not as_bool(m.get("closed")):
        return None
    prices = as_list(m.get("outcomePrices")) or []
    outcomes = as_list(m.get("outcomes")) or [t.get("outcome") for t in tokens or ()]
    for i, p in enumerate(prices):
        if as_float(p) == 1.0 and i < len(outcomes):
            return text(outcomes[i])
    return None


def _tags(m: dict) -> list[str] | None:
    tags = m.get("tags")
    if not isinstance(tags, list):
        cat = text(m.get("category"))
        return [cat] if cat else None
    out = [text(t.get("label") or t.get("slug")) if isinstance(t, dict) else text(t) for t in tags]
    return [t for t in out if t] or None


def _market_row(m: dict, snap_ts: int, source: str, host: str, out: Out, extra: dict | None = None) -> None:
    mid = text(first(m, "conditionId", "condition_id", "market"))
    if not mid:
        out.stats[f"skipped.{source}"] += 1
        return
    toks = _tokens(m)
    for t in toks or ():
        out.token_market.setdefault(t["token_id"], mid)
    events = m.get("events")
    event_id = text(m.get("eventSlug")) or (text(events[0].get("slug")) if isinstance(events, list) and events
                                             and isinstance(events[0], dict) else None)
    out.row("markets", {
        "snapshot_ts": snap_ts, "source": source, "market_id": mid, "event_id": event_id,
        "slug": text(m.get("slug")), "question": text(first(m, "question", "title")), "tokens": toks,
        "tick_size": dec(as_float(first(m, "orderPriceMinTickSize", "tick_size", "minimum_tick_size"))),
        "min_order_size": dec(as_float(first(m, "orderMinSize", "min_order_size", "minimum_order_size"))),
        "neg_risk": as_bool(first(m, "negRisk", "neg_risk")),
        "end_ts": iso_ms(first(m, "endDate", "endDateIso", "end_date_iso")),
        "closed": as_bool(m.get("closed")),
        "resolved_outcome": _resolved(m, toks),
        "resolution_ts": iso_ms(first(m, "closedTime", "umaEndDate")) if as_bool(m.get("closed")) else None,
        "volume_24h": as_float(first(m, "volume24hr", "volume_24hr", "volume24h")),
        "fees_enabled": as_bool(first(m, "feesEnabled", "fees_enabled")),
        "tags": _tags(m), "raw": dumps(m), "host": host, **(extra or {}),
    })


def _records(frame) -> list[dict]:
    if isinstance(frame, list):
        return [m for m in frame if isinstance(m, dict)]
    if isinstance(frame, dict):
        for k in ("markets", "data"):
            if isinstance(frame.get(k), list):
                return [m for m in frame[k] if isinstance(m, dict)]
        return [frame]
    return []


def markets(ln: RawLine, out: Out) -> None:
    """Recorder line: `{"ts_ms","reason":"added|daily|removed|final","market":<full Gamma record>}`."""
    rec = ln.rec
    if rec.get("ctl"):
        return
    ts = to_ms(first(rec, "ts_ms", "recv_ms"))
    if isinstance(rec.get("market"), dict):
        _market_row(rec["market"], ts, "metadata", ln.host, out, {"reason": text(rec.get("reason"))})
        return
    for m in _records(frame_of(rec)):  # older shapes: a list of Gamma records, possibly under "markets"/"data"
        _market_row(m, ts, "metadata", ln.host, out)


def universe(ln: RawLine, out: Out) -> None:
    """Recorder line: `{"ts_ms","pool","selected":[{condition_id,slug,reason,rank,volume24hr,tokens}],
    "in_grace":[id],"added":[id],"removed":[id]}` (the legacy `top` list is also read)."""
    if ln.rec.get("ctl"):
        return
    frame = frame_of(ln.rec)
    if not isinstance(frame, dict):
        return
    ts = to_ms(first(frame, "ts_ms", "recv_ms") or ln.rec.get("recv_ms"))
    grace = frame.get("in_grace") if isinstance(frame.get("in_grace"), list) else []
    grace_ids = {text(g.get("condition_id") or g.get("conditionId")) if isinstance(g, dict) else text(g) for g in grace}
    items = [m for m in (first(frame, "selected", "top", "markets") or []) if isinstance(m, dict)]
    seen = set()
    for m in items:
        rec = m.get("gamma") if isinstance(m.get("gamma"), dict) else m
        mid = text(first(rec, "condition_id", "conditionId"))
        seen.add(mid)
        _market_row({**rec, **({"tokens": m["tokens"]} if "tokens" in m else {})}, ts, "universe", ln.host, out,
                    {"universe_rank": as_int(m.get("rank")), "reason": text(m.get("reason")),
                     "in_grace": bool(as_bool(m.get("in_grace")) or (mid in grace_ids))})
    for g in grace:
        gid = text(g.get("condition_id") or g.get("conditionId")) if isinstance(g, dict) else text(g)
        if gid and gid not in seen:
            _market_row(g if isinstance(g, dict) else {"condition_id": gid}, ts, "universe", ln.host, out,
                        {"in_grace": True, "reason": "grace"})