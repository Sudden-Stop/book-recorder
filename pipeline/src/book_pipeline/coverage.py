"""Per market per hour coverage, built from the control lines of a market-data stream.

Connection model (one recorder WebSocket = one `(host, conn)`):

- `connect` (with `assets`) opens the connection and sets its subscriptions; `subscribe` / `unsubscribe`
  (with `assets`) change them; `disconnect` closes it; `connect_error` is a failed attempt (stays down);
  `seq_gap` (Kalshi) leaves the connection up but marks every subscribed market's book unknown until its next
  snapshot (counted in `seq_gaps`, not in the connected time).
- The state at the start of the hour is not in the file when the connection opened in an earlier hour. It is
  inferred: a connection whose first control line is not `connect`, or that sends frames before it, was up
  from the start of the hour, subscribed to the tokens it sent frames for before its first `connect` (plus any
  it later unsubscribes). Rows built on that inference have `start_assumed = true`.
- A market is subscribed while any of its tokens is subscribed on any connection of the host, and connected
  while such a connection is also up. Overlapping connections are merged, not added.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

HOUR_MS = 3_600_000


@dataclass
class _Conn:
    ctl: list[tuple[int, str, list[str] | None]] = field(default_factory=list)
    first_frame_ms: int | None = None
    seen: set[str] = field(default_factory=set)
    seen_before_connect: set[str] = field(default_factory=set)
    live: set[str] = field(default_factory=set)
    had_connect: bool = False


@dataclass
class _Counts:
    n_book: int = 0
    n_level_changes: int = 0
    n_trades_ws: int = 0
    n_tick_size_changes: int = 0
    n_snapshots: int = 0
    n_trades_activity: int = 0
    first_recv_ts: int | None = None
    last_recv_ts: int | None = None


COUNTERS = ("n_book", "n_level_changes", "n_trades_ws", "n_tick_size_changes", "n_snapshots", "n_trades_activity")


class CoverageTracker:
    def __init__(self, venue: str, hour_start_ms: int):
        self.venue = venue
        self.h0 = hour_start_ms
        self.h1 = hour_start_ms + HOUR_MS
        self.conns: dict[tuple[str, int], _Conn] = defaultdict(_Conn)
        self.counts: dict[tuple[str, str], _Counts] = defaultdict(_Counts)

    # -- feeding ---------------------------------------------------------------------------------------------
    def ctl(self, host: str, conn: int, recv_ms: int, kind: str, assets: list[str] | None) -> set[str]:
        """Record a control line; returns the tokens that were live on the connection before it."""
        c = self.conns[(host, conn)]
        before = set(c.live)
        assets = [str(a) for a in assets] if isinstance(assets, list) else None
        c.ctl.append((recv_ms, kind, assets))
        if kind == "connect":
            c.had_connect = True
            if assets is not None:
                c.live = set(assets)
        elif kind == "subscribe" and assets:
            c.live |= set(assets)
        elif kind == "unsubscribe" and assets:
            c.live -= set(assets)
        return before

    def frame(self, host: str, conn: int | None, recv_ms: int, token: str, counter: str, n: int = 1) -> None:
        if conn is not None:
            c = self.conns[(host, conn)]
            if c.first_frame_ms is None:
                c.first_frame_ms = recv_ms
            if token not in c.seen:
                c.seen.add(token)
                if not c.had_connect:
                    c.seen_before_connect.add(token)
            c.live.add(token)
        k = self.counts[(host, token)]
        setattr(k, counter, getattr(k, counter) + n)
        if counter not in ("n_snapshots", "n_trades_activity"):
            k.first_recv_ts = recv_ms if k.first_recv_ts is None else min(k.first_recv_ts, recv_ms)
            k.last_recv_ts = recv_ms if k.last_recv_ts is None else max(k.last_recv_ts, recv_ms)

    def count(self, host: str, token: str, counter: str, n: int = 1) -> None:
        k = self.counts[(host, token)]
        setattr(k, counter, getattr(k, counter) + n)

    def live(self, host: str, conn: int) -> set[str]:
        return set(self.conns[(host, conn)].live)

    # -- result ----------------------------------------------------------------------------------------------
    def _conn_segments(self, c: _Conn):
        """Per token: list of (start, end, connected) segments; plus disconnect and connect events."""
        clamp = lambda t: min(max(t, self.h0), self.h1)  # noqa: E731
        ctl = c.ctl
        first_ctl = ctl[0] if ctl else None
        frames_first = c.first_frame_ms is not None and (first_ctl is None or c.first_frame_ms < first_ctl[0])
        if first_ctl is None:
            connected, assumed = c.first_frame_ms is not None, True
        elif first_ctl[1] in ("connect", "connect_error") and not frames_first:
            connected, assumed = False, False
        else:
            connected, assumed = True, True
        later_subscribed = {tok for _, k, al in ctl if k == "subscribe" and al for tok in al}
        unsub_first = set()
        for _, k, a in ctl:
            if k == "connect":
                break
            if k == "unsubscribe" and a:
                unsub_first |= set(a)
        if c.had_connect:
            subs = set(c.seen_before_connect) | unsub_first
        else:
            subs = (set(c.seen) - later_subscribed) | unsub_first
        segs: dict[str, list[tuple[int, int, bool]]] = defaultdict(list)
        events: dict[str, list[tuple[str, int]]] = defaultdict(list)
        t_prev = self.h0
        for t, kind, assets in ctl:
            t = clamp(t)
            if t > t_prev:
                for tok in subs:
                    segs[tok].append((t_prev, t, connected))
            t_prev = max(t_prev, t)
            if kind == "connect":
                connected = True
                if assets is not None:
                    subs = set(assets)
                for tok in subs:
                    events[tok].append(("connect", t))
            elif kind == "disconnect":
                if connected:
                    for tok in subs:
                        events[tok].append(("disconnect", t))
                connected = False
            elif kind == "seq_gap":
                for tok in subs:
                    events[tok].append(("seq_gap", t))
            elif kind == "subscribe" and assets:
                subs |= set(assets)
            elif kind == "unsubscribe" and assets:
                subs -= set(assets)
        if self.h1 > t_prev:
            for tok in subs:
                segs[tok].append((t_prev, self.h1, connected))
        return segs, events, assumed

    def rows(self, token_market: dict[str, str]) -> list[dict]:
        hosts = {h for h, _ in self.conns} | {h for h, _ in self.counts}
        out = []
        for host in sorted(hosts):
            tok_segs: dict[str, list[tuple[int, int, bool]]] = defaultdict(list)
            tok_events: dict[str, set[tuple[int, str, int]]] = defaultdict(set)
            tok_assumed: dict[str, bool] = defaultdict(bool)
            tokens: set[str] = {t for h, t in self.counts if h == host}
            for (h, conn), c in self.conns.items():
                if h != host:
                    continue
                segs, events, assumed = self._conn_segments(c)
                for tok, s in segs.items():
                    tok_segs[tok].extend(s)
                    tok_assumed[tok] |= assumed
                for tok, evs in events.items():
                    for kind, t in evs:
                        tok_events[tok].add((conn, kind, t))
                tokens |= set(segs) | c.seen | {a for _, _, al in c.ctl if al for a in al}
            groups: dict[str | None, set[str]] = defaultdict(set)
            for tok in tokens:
                m = token_market.get(tok)
                groups[m if m else f"\0{tok}"].add(tok)
            for key, toks in groups.items():
                market = None if key is None or key.startswith("\0") else key
                segs = [s for t in toks for s in tok_segs.get(t, ())]
                subscribed = _union([(a, b) for a, b, _ in segs])
                connected = _union([(a, b) for a, b, up in segs if up])
                evs = set().union(*[tok_events.get(t, set()) for t in toks]) if toks else set()
                row = {
                    "venue": self.venue,
                    "hour_start": self.h0,
                    "host": host,
                    "market_id": market,
                    "token_ids": sorted(toks),
                    "subscribed_s": subscribed / 1000.0,
                    "connected_s": connected / 1000.0,
                    "gap_s": (subscribed - connected) / 1000.0,
                    "gap_count": len({(c, t) for c, k, t in evs if k == "disconnect"}),
                    "seq_gaps": len({(c, t) for c, k, t in evs if k == "seq_gap"}),
                    "connects": len({(c, t) for c, k, t in evs if k == "connect"}),
                    "start_assumed": any(tok_assumed.get(t, False) for t in toks),
                }
                firsts, lasts = [], []
                for name in COUNTERS:
                    row[name] = 0
                for t in toks:
                    k = self.counts.get((host, t))
                    if k is None:
                        continue
                    for name in COUNTERS:
                        row[name] += getattr(k, name)
                    if k.first_recv_ts is not None:
                        firsts.append(k.first_recv_ts)
                        lasts.append(k.last_recv_ts)
                row["first_recv_ts"] = min(firsts) if firsts else None
                row["last_recv_ts"] = max(lasts) if lasts else None
                recorded = subscribed > 0 or row["first_recv_ts"] is not None or row["n_snapshots"] > 0
                if recorded:  # markets seen only on the global activity feed were never tracked
                    out.append(row)
        out.sort(key=lambda r: (r["host"], r["market_id"] is None, r["market_id"] or "", r["token_ids"]))
        return out


def _union(intervals: list[tuple[int, int]]) -> int:
    total, cur_a, cur_b = 0, None, None
    for a, b in sorted(intervals):
        if b <= a:
            continue
        if cur_b is None or a > cur_b:
            if cur_b is not None:
                total += cur_b - cur_a
            cur_a, cur_b = a, b
        else:
            cur_b = max(cur_b, b)
    if cur_b is not None:
        total += cur_b - cur_a
    return total
