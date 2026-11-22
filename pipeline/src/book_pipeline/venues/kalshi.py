"""Kalshi raw frames -> curated rows.

Frame shapes follow the recorder (`recorder/src/kalshi.rs`, checked by that session against docs.kalshi.com on
2026-11-15). They are NOT yet checked against a recorded Kalshi hour: no Kalshi data exists yet, and the tests
use synthetic frames in these shapes. Confirm with the first real hour (add it as a fixture).

Kalshi has one market-data socket stream, `book_events`; every message is an envelope
`{"type", "sid", "seq", "msg": {…}}` and is split by `type`:

| type | table | mapping |
|---|---|---|
| `orderbook_snapshot` `{market_ticker, yes_dollars_fp: [[price, size]], no_dollars_fp: [[price, size]]}` | `book_deltas` + `book_snapshots` | `reset`, then YES bids as `bid` levels at p and NO bids as `ask` levels at 1 - p; the same book is a `book_snapshots` row with `source = ws` and the same `line` |
| `orderbook_delta` `{market_ticker, price_dollars, delta_fp, side: yes/no, ts_ms}` | `book_deltas` | `set` with the ABSOLUTE size after applying `delta_fp` to the book kept since the last snapshot (carried over from the previous hour when that hour is available; otherwise dropped until the first snapshot and counted in `stats`) |
| `trade` `{trade_id, market_ticker, yes_price_dollars, count_fp, taker_side, ts}` (every public trade on the exchange) | `trades` | `source = market_ws`, price in YES terms, `side` = `buy` if the taker bought YES, size = count |
| `market_lifecycle_v2` `{market_ticker, event_type, close_ts, result, determination_ts, settled_ts, …}` (every market) | `markets` | `source = lifecycle`, `reason` = event type |
| `subscribed`, `ok`, `error` | none | channel of each `sid` learned from `subscribed`; `error` counted |
| control `seq_gap` `{sid, expected, got}` | `book_deltas` | `gap` rows for the tickers on that order book subscription; their books are dropped until the next snapshot |

Other streams: `universe` `{ts_ms, pool, selected:[{ticker, event_ticker, reason, rank, volume_24h}], in_grace,
added, removed}`, `markets` `{ts_ms, reason, market:<Kalshi market record>}`, `series` `{ts_ms, series:<GET
/series response>}` (fee_type, fee_multiplier).

`token_id` = `market_id` = market ticker: the book is expressed in YES terms, one token per market.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from ..storage import RawLine, frame_of
from ..writer import dec
from .common import Out, as_float, as_int, dumps, first, iso_ms, text, to_ms

VENUE = "kalshi"
UNVERIFIED = True  # shapes from the recorder's code, not yet from recorded data
# previous-hour stream -> handler, read first to seed state (see curate)
WARM = (("book_events", "warm"),)
STREAMS = (("series", "series"), ("markets", "markets"), ("universe", "universe"), ("book_events", "book_event"))
TABLES = ("book_deltas", "book_snapshots", "trades", "markets", "coverage")
ONE = Decimal(1)
BOOK_TYPES = ("orderbook_snapshot", "orderbook_delta")


def _d(value) -> Decimal | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _price(dollars=None, cents=None) -> Decimal | None:
    p = _d(dollars)
    if p is not None:
        return p
    c = _d(cents)
    return c / 100 if c is not None else None


def _side_levels(raw, cents: bool) -> dict[Decimal, Decimal]:
    out = {}
    for lv in raw or ():
        if isinstance(lv, (list, tuple)) and len(lv) >= 2:
            p = _price(cents=lv[0]) if cents else _d(lv[0])
            q = _d(lv[1])
            if p is not None and q is not None and q > 0:
                out[p] = q
    return out


def _book_from(msg: dict) -> tuple[dict, dict]:
    for yes_k, no_k, cents in (("yes_dollars_fp", "no_dollars_fp", False), ("yes_dollars", "no_dollars", False),
                               ("yes", "no", True)):
        if yes_k in msg or no_k in msg:
            return _side_levels(msg.get(yes_k), cents), _side_levels(msg.get(no_k), cents)
    return {}, {}


def _as_bid_ask(yes: dict, no: dict) -> tuple[list, list]:
    bids = sorted(((dec(str(p)), dec(str(q))) for p, q in yes.items()), key=lambda x: -Decimal(x[0]))
    asks = sorted(((dec(str(ONE - p)), dec(str(q))) for p, q in no.items()), key=lambda x: Decimal(x[0]))
    return bids, asks


class _State:
    def __init__(self):
        self.books: dict[tuple[str, str], tuple[dict, dict]] = {}
        self.sid_channel: dict[tuple[str, int, int], str] = {}
        self.sid_tickers: dict[tuple[str, int, int], set[str]] = {}


def _state(out: Out) -> _State:
    st = getattr(out, "_kalshi_state", None)
    if st is None:
        st = _State()
        out._kalshi_state = st
    return st


def _envelope(rec: dict):
    env = frame_of(rec)
    if not isinstance(env, dict):
        return None, None, {}
    msg = env.get("msg") if isinstance(env.get("msg"), dict) else {}
    return env, env.get("type"), msg


def _track_book(ln: RawLine, out: Out, st: _State, env: dict, kind, msg: dict) -> tuple[str | None, int | None]:
    """Book state + sid bookkeeping shared by `book_event` and `warm`. Returns (ticker, new absolute size)."""
    conn = as_int(ln.rec.get("conn")) or 0
    tok = text(first(msg, "market_ticker", "ticker"))
    sid = as_int(env.get("sid"))
    if kind == "subscribed":
        ch, s = text(msg.get("channel")), as_int(msg.get("sid"))
        if ch and s is not None:
            st.sid_channel[(ln.host, conn, s)] = ch
        return None, None
    if sid is not None and tok and kind in BOOK_TYPES:
        st.sid_channel.setdefault((ln.host, conn, sid), "orderbook_delta")
        st.sid_tickers.setdefault((ln.host, conn, sid), set()).add(tok)
    elif sid is not None and kind in ("trade", "market_lifecycle_v2"):
        st.sid_channel.setdefault((ln.host, conn, sid), kind)
    if kind == "orderbook_snapshot" and tok:
        st.books[(ln.host, tok)] = _book_from(msg)
    elif kind == "orderbook_delta" and tok:
        book = st.books.get((ln.host, tok))
        if book is None:
            return tok, None
        p = _price(msg.get("price_dollars"), msg.get("price"))
        d = _d(first(msg, "delta_fp", "delta")) or Decimal(0)
        side = book[0] if msg.get("side") == "yes" else book[1]
        new = side.get(p, Decimal(0)) + d
        if new <= 0:
            side.pop(p, None)
            new = Decimal(0)
        else:
            side[p] = new
        return tok, new
    return tok, None


def _seq_gap_tickers(ln: RawLine, st: _State) -> set[str] | None:
    """Tickers whose books a `seq_gap` invalidates; None when the gap is on a trade/lifecycle subscription."""
    conn = as_int(ln.rec.get("conn")) or 0
    sid = as_int(ln.rec.get("sid"))
    key = (ln.host, conn, sid)
    channel = st.sid_channel.get(key)
    if channel in ("trade", "market_lifecycle_v2"):
        return None
    if sid is not None and key in st.sid_tickers:
        return set(st.sid_tickers[key])
    # unknown subscription: be conservative, every ticker with a book on this connection
    return {t for (h, c, _s), ts in st.sid_tickers.items() if h == ln.host and c == conn for t in ts} or \
        {t for (h, t) in st.books if h == ln.host}


def warm(ln: RawLine, out: Out) -> None:
    """Replay the previous hour's book frames to know each book at the start of this hour (no rows written)."""
    st = _state(out)
    if ln.rec.get("ctl"):
        if ln.rec["ctl"] == "seq_gap":
            for t in _seq_gap_tickers(ln, st) or ():
                st.books.pop((ln.host, t), None)
        elif ln.rec["ctl"] == "disconnect":
            for key in [k for k in st.books if k[0] == ln.host]:
                st.books.pop(key, None)
        return
    env, kind, msg = _envelope(ln.rec)
    if env is not None:
        _track_book(ln, out, st, env, kind, msg)


def book_event(ln: RawLine, out: Out) -> None:
    rec = ln.rec
    recv = to_ms(rec.get("recv_ms"))
    conn = as_int(rec.get("conn"))
    base = {"host": ln.host, "conn": conn, "line": ln.line, "recv_ts": recv}
    st = _state(out)
    ctl = rec.get("ctl")
    if ctl:
        out.stats[f"ctl.{ctl}"] += 1
        for a in rec.get("assets") or ():
            out.token_market.setdefault(str(a), str(a))  # Kalshi: token id = market ticker
        if ctl == "seq_gap":
            tickers = _seq_gap_tickers(ln, st)
            if tickers is None:
                out.stats["kalshi.seq_gap_other_channel"] += 1
                return
            out.coverage.ctl(ln.host, conn, recv, "seq_gap", None)
            for i, tok in enumerate(sorted(tickers)):
                out.row("book_deltas", {**base, "item": i, "token_id": tok, "market_id": tok, "action": "gap"})
                st.books.pop((ln.host, tok), None)
            return
        before = out.coverage.ctl(ln.host, conn, recv, str(ctl), rec.get("assets"))
        if ctl == "disconnect":
            for i, tok in enumerate(sorted(before), start=rec.get("item0", 0)):
                out.row("book_deltas", {**base, "item": i, "token_id": tok, "market_id": tok, "action": "gap"})
                st.books.pop((ln.host, tok), None)
        return
    env, kind, msg = _envelope(rec)
    if env is None:
        out.stats["frames.unparsed"] += 1
        return
    tok, new = _track_book(ln, out, st, env, kind, msg)
    if tok and kind in BOOK_TYPES:
        out.token_market.setdefault(tok, tok)
    if kind == "orderbook_snapshot" and tok:
        yes, no = st.books[(ln.host, tok)]
        bids, asks = _as_bid_ask(yes, no)
        ts = to_ms(first(msg, "ts_ms", "ts"), seconds_if_small=True)
        common = {**base, "token_id": tok, "market_id": tok, "exchange_ts": ts}
        out.row("book_deltas", {**common, "item": 0, "action": "reset"})
        item = 1
        for side, levels in (("bid", bids), ("ask", asks)):
            for p, s in levels:
                out.row("book_deltas", {**common, "item": item, "action": "set", "side": side, "price": p, "size": s})
                item += 1
        out.row("book_snapshots", {"host": ln.host, "line": ln.line, "recv_ts": recv, "source": "ws",
                                   "token_id": tok, "market_id": tok, "exchange_ts": ts, "bids": bids, "asks": asks})
        out.coverage.frame(ln.host, conn, recv, tok, "n_book")
        out.coverage.count(ln.host, tok, "n_snapshots")
    elif kind == "orderbook_delta" and tok:
        if new is None:
            out.stats["kalshi.delta_without_book"] += 1
            out.coverage.frame(ln.host, conn, recv, tok, "n_level_changes")
            return
        p = _price(msg.get("price_dollars"), msg.get("price"))
        yes = msg.get("side") == "yes"
        out.row("book_deltas", {**base, "item": 0, "token_id": tok, "market_id": tok,
                                "exchange_ts": to_ms(first(msg, "ts_ms", "ts"), seconds_if_small=True),
                                "action": "set", "side": "bid" if yes else "ask",
                                "price": dec(str(p if yes else ONE - p)), "size": dec(str(new))})
        out.coverage.frame(ln.host, conn, recv, tok, "n_level_changes")
    elif kind == "trade" and tok:
        price = _price(msg.get("yes_price_dollars"), msg.get("yes_price"))
        size = _d(first(msg, "count_fp", "count"))
        if price is None or size is None:
            out.stats["skipped.trade"] += 1
            return
        out.row("trades", {**base, "item": 0, "token_id": tok, "market_id": tok,
                           "exchange_ts": to_ms(first(msg, "ts_ms", "ts"), seconds_if_small=True),
                           "source": "market_ws", "price": dec(str(price)), "size": dec(str(size)),
                           "side": {"yes": "buy", "no": "sell"}.get(str(msg.get("taker_side"))),
                           "trade_id": text(msg.get("trade_id"))})
        # every public trade arrives, tracked market or not: count it without marking the market as recorded
        out.coverage.count(ln.host, tok, "n_trades_ws")
    elif kind == "market_lifecycle_v2" and tok:
        _lifecycle_row(msg, recv, ln.host, out)
    elif kind in ("subscribed", "ok", "unsubscribed"):
        out.stats[f"kalshi.reply.{kind}"] += 1
    elif kind == "error":
        out.stats["kalshi.reply.error"] += 1
    else:
        out.unknown_event_types[str(kind)] += 1


def _secs(value) -> int | None:
    return to_ms(value, seconds_if_small=True) if not isinstance(value, str) or value.isdigit() else iso_ms(value)


def _lifecycle_row(msg: dict, ts: int, host: str, out: Out) -> None:
    ev = text(msg.get("event_type"))
    result = text(msg.get("result"))
    out.row("markets", {
        "snapshot_ts": ts, "source": "lifecycle", "reason": ev, "market_id": text(msg.get("market_ticker")),
        "event_id": text(msg.get("event_ticker")), "tokens": None,
        "end_ts": _secs(msg.get("close_ts")),
        "closed": True if ev in ("deactivated", "determined", "settled") else (False if ev == "activated" else None),
        "resolved_outcome": result if result else None,
        "resolution_ts": _secs(first(msg, "determination_ts", "settled_ts")),
        "raw": dumps(msg), "host": host,
    })


def _market_row(m: dict, ts: int, source: str, host: str, out: Out, extra: dict | None = None) -> None:
    tid = text(first(m, "ticker", "market_ticker"))
    if not tid:
        out.stats[f"skipped.{source}"] += 1
        return
    status = text(m.get("status"))
    result = text(m.get("result"))
    tick = _price(first(m, "tick_size_dollars", "price_increment_dollars"), m.get("tick_size"))
    out.row("markets", {
        "snapshot_ts": ts, "source": source, "market_id": tid, "event_id": text(m.get("event_ticker")),
        "slug": None, "question": text(first(m, "title", "yes_sub_title", "subtitle")),
        "tokens": [{"token_id": tid, "outcome": "yes"}],
        "tick_size": dec(str(tick)) if tick is not None else None,
        "end_ts": iso_ms(first(m, "expected_expiration_time", "close_time", "expiration_time")),
        "closed": status in ("closed", "settled", "finalized", "determined") if status else None,
        "resolved_outcome": result if result else None,
        "resolution_ts": iso_ms(first(m, "settlement_ts", "settlement_time")),
        "volume_24h": as_float(first(m, "volume_24h_fp", "volume_24h")),
        "tags": [text(m["category"])] if m.get("category") else None,
        "raw": dumps(m), "host": host, **(extra or {}),
    })


def markets(ln: RawLine, out: Out) -> None:
    """Recorder line: `{"ts_ms","reason":"added|daily|removed|final","market":<Kalshi market record>}`."""
    rec = ln.rec
    if rec.get("ctl") or not isinstance(rec.get("market"), dict):
        out.stats["skipped.markets"] += 0 if rec.get("ctl") else 1
        return
    _market_row(rec["market"], to_ms(first(rec, "ts_ms", "recv_ms")), "metadata", ln.host, out,
                {"reason": text(rec.get("reason"))})


def universe(ln: RawLine, out: Out) -> None:
    frame = frame_of(ln.rec)
    if ln.rec.get("ctl") or not isinstance(frame, dict):
        return
    ts = to_ms(first(frame, "ts_ms", "recv_ms"))
    grace = [text(g) for g in frame.get("in_grace") or [] if text(g)]
    seen = set()
    for m in frame.get("selected") or []:
        if isinstance(m, dict):
            seen.add(text(m.get("ticker")))
            _market_row(m, ts, "universe", ln.host, out,
                        {"universe_rank": as_int(m.get("rank")), "reason": text(m.get("reason")),
                         "in_grace": text(m.get("ticker")) in grace})
    for g in grace:
        if g not in seen:
            _market_row({"ticker": g}, ts, "universe", ln.host, out, {"in_grace": True, "reason": "grace"})


def series(ln: RawLine, out: Out) -> None:
    """Recorder line: `{"ts_ms","series":<GET /series/{ticker} response>}` (the response nests `series`)."""
    rec = ln.rec
    s = rec.get("series")
    if isinstance(s, dict) and isinstance(s.get("series"), dict):
        s = s["series"]
    if rec.get("ctl") or not isinstance(s, dict) or not text(s.get("ticker")):
        out.stats["skipped.series"] += 0 if rec.get("ctl") else 1
        return
    out.row("markets", {
        "snapshot_ts": to_ms(first(rec, "ts_ms", "recv_ms")), "source": "series", "market_id": text(s["ticker"]),
        "question": text(s.get("title")), "fee_type": text(s.get("fee_type")),
        "fee_multiplier": as_float(s.get("fee_multiplier")),
        "tags": [text(s["category"])] if s.get("category") else None, "raw": dumps(s), "host": ln.host,
    })
