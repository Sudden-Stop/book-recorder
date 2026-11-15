"""Venue `cross`: the recorder's cross-venue `pairs` stream.

Recorder line (rewritten in full at every universe refresh):
`{"ts_ms","rule","confidence","pairs":[{"league","date","teams":[a,b],
  "polymarket":{"condition_id","slug","tokens":[{token_id,outcome}]},
  "kalshi":{"event_ticker","market_tickers":[…]}}]}`
One curated row per Kalshi market ticker of each pair.
"""
from __future__ import annotations

from ..storage import RawLine, frame_of
from .common import Out, dumps, first, text, to_ms

VENUE = "cross"
STREAMS = (("pairs", "pairs"),)
TABLES = ("pairs",)


def pairs(ln: RawLine, out: Out) -> None:
    if ln.rec.get("ctl"):
        return
    frame = frame_of(ln.rec)
    if not isinstance(frame, dict):
        out.stats["skipped.pairs"] += 1
        return
    ts = to_ms(first(frame, "ts_ms", "recv_ms") or ln.rec.get("recv_ms"))
    rule = text(frame.get("rule")) or "unknown"
    confidence = text(frame.get("confidence")) or "unknown"
    for p in frame.get("pairs") or []:
        pm = p.get("polymarket") if isinstance(p.get("polymarket"), dict) else {}
        ks = p.get("kalshi") if isinstance(p.get("kalshi"), dict) else {}
        pid, event = text(pm.get("condition_id")), text(ks.get("event_ticker"))
        if not pid or not event:
            out.stats["skipped.pairs"] += 1
            continue
        tokens = [{"token_id": text(t.get("token_id")), "outcome": text(t.get("outcome"))}
                  for t in pm.get("tokens") or [] if isinstance(t, dict)] or None
        teams = [text(t) for t in p.get("teams") or []] or None
        base = {"recv_ts": ts, "league": text(p.get("league")), "game_date": text(p.get("date")), "teams": teams,
                "polymarket_market_id": pid, "polymarket_slug": text(pm.get("slug")), "polymarket_tokens": tokens,
                "kalshi_event_ticker": event, "rule": text(p.get("rule")) or rule,
                "confidence": text(p.get("confidence")) or confidence, "raw": dumps(p), "host": ln.host}
        tickers = [text(t) for t in ks.get("market_tickers") or [] if text(t)]
        for t in tickers or [None]:
            out.row("pairs", {**base, "kalshi_market_id": t})
