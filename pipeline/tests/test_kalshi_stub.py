"""Kalshi parsing on synthetic frames in the recorder's shapes (recorder/src/kalshi.rs). Not yet checked against
a recorded Kalshi hour: none exists."""
from decimal import Decimal

import pyarrow.parquet as pq

from book_pipeline import curate, replay
from book_pipeline.curate import curated_path, previous_hour
from book_pipeline.storage import Root

from conftest import DT, H0, HOUR, write_raw

T = "KXNBAGAME-26NOV14AAABBB-AAA"
U = "KXNBAGAME-26NOV14AAABBB-BBB"


def env(t, kind, sid, seq, msg, conn=1):
    return {"recv_ms": H0 + t, "conn": conn, "msg": {"type": kind, "sid": sid, "seq": seq, "msg": msg}}


def snap(t, seq, ticker, yes, no):
    return env(t, "orderbook_snapshot", 4, seq, {"market_ticker": ticker, "yes_dollars_fp": yes, "no_dollars_fp": no})


def delta(t, seq, ticker, price, d, side):
    return env(t, "orderbook_delta", 4, seq, {"market_ticker": ticker, "price_dollars": price, "delta_fp": d,
                                              "side": side, "ts_ms": H0 + t})


def _tables(root):
    r = Root.parse(root)
    return {t: pq.read_table(curated_path(r, t, "kalshi", DT, HOUR)).to_pylist()
            for t in ("book_deltas", "book_snapshots", "trades", "markets", "coverage")}


def test_kalshi_book_trades_lifecycle_series(tmp_path):
    write_raw(tmp_path, "kalshi", "book_events", [
        {"recv_ms": H0 + 1, "conn": 1, "ctl": "connect", "assets": [T, U]},
        {"recv_ms": H0 + 2, "conn": 1, "msg": {"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 4}}},
        {"recv_ms": H0 + 3, "conn": 1, "msg": {"id": 2, "type": "subscribed", "msg": {"channel": "trade", "sid": 5}}},
        snap(10, 1, T, [["0.4000", "100.00"], ["0.3800", "50.00"]], [["0.5500", "70.00"]]),
        snap(11, 2, U, [["0.5500", "10.00"]], []),
        delta(20, 3, T, "0.4000", "-30.00", "yes"),
        delta(21, 4, T, "0.5500", "5.00", "no"),
        env(30, "trade", 5, 1, {"trade_id": "t1", "market_ticker": T, "yes_price_dollars": "0.4100",
                                "count_fp": "7.00", "taker_side": "no", "ts": (H0 + 30_000) // 1000}),
        env(31, "trade", 5, 2, {"trade_id": "t2", "market_ticker": "KXOTHER-1", "yes_price_dollars": "0.2000",
                                "count_fp": "1.00", "taker_side": "yes", "ts": (H0 + 31_000) // 1000}),
        env(32, "market_lifecycle_v2", 6, 1, {"market_ticker": "KXOTHER-1", "event_type": "determined",
                                              "result": "yes", "determination_ts": (H0 + 32_000) // 1000}),
        # a trade-channel gap must not invalidate any book
        {"recv_ms": H0 + 33, "conn": 1, "ctl": "seq_gap", "sid": 5, "expected": 3, "got": 4},
        # an order-book gap invalidates both books until the next snapshot
        {"recv_ms": H0 + 40, "conn": 1, "ctl": "seq_gap", "sid": 4, "expected": 5, "got": 7},
        delta(41, 7, T, "0.3800", "1.00", "yes"),  # no book: dropped
        snap(50, 8, T, [["0.4000", "70.00"]], [["0.5500", "75.00"]]),
        {"recv_ms": H0 + 60, "conn": 1, "msg": {"id": 9, "type": "error", "msg": {"code": 8}}},
    ])
    write_raw(tmp_path, "kalshi", "series", [{"ts_ms": H0 + 5, "series": {"series": {
        "ticker": "KXNBAGAME", "title": "NBA game", "fee_type": "quadratic", "fee_multiplier": 1, "category": "Sports"}}}])
    write_raw(tmp_path, "kalshi", "markets", [{"ts_ms": H0 + 6, "reason": "added", "market": {
        "ticker": T, "event_ticker": "KXNBAGAME-26NOV14AAABBB", "title": "AAA vs BBB", "status": "active",
        "close_time": "2026-11-15T03:00:00Z", "volume_24h_fp": "123.00"}}])
    write_raw(tmp_path, "kalshi", "universe", [{"ts_ms": H0 + 7, "pool": 900, "selected": [
        {"ticker": T, "event_ticker": "KXNBAGAME-26NOV14AAABBB", "reason": "paired", "rank": 1, "volume_24h": "123.00"}],
        "in_grace": [U], "added": [T], "removed": []}])
    rep = curate.run(str(tmp_path), str(tmp_path), "kalshi", DT, HOUR)
    assert rep["unverified_parser"] is True and rep["unknown_event_types"] == {}
    assert rep["stats"]["kalshi.reply.error"] == 1 and rep["stats"]["kalshi.seq_gap_other_channel"] == 1
    assert rep["stats"]["kalshi.delta_without_book"] == 1
    t = _tables(tmp_path)

    dt_ = [(r["token_id"][-3:], r["action"], r["side"], r["price"], r["size"]) for r in t["book_deltas"]]
    D = Decimal
    assert dt_[:4] == [("AAA", "reset", None, None, None), ("AAA", "set", "bid", D("0.4"), D("100")),
                       ("AAA", "set", "bid", D("0.38"), D("50")), ("AAA", "set", "ask", D("0.45"), D("70"))]
    assert ("AAA", "set", "bid", D("0.4"), D("70")) in dt_  # absolute size after the delta
    assert ("AAA", "set", "ask", D("0.45"), D("75")) in dt_  # NO bid at 0.55 = YES ask at 0.45
    assert sorted(x[0] for x in dt_ if x[1] == "gap") == ["AAA", "BBB"]

    s = t["book_snapshots"]
    assert [(r["token_id"][-3:], r["source"]) for r in s] == [("AAA", "ws"), ("BBB", "ws"), ("AAA", "ws")]

    tr = {r["trade_id"]: r for r in t["trades"]}
    assert tr["t1"]["price"] == D("0.41") and tr["t1"]["side"] == "sell" and tr["t1"]["size"] == D("7")
    assert tr["t2"]["market_id"] == "KXOTHER-1"

    m = {(r["source"], r["market_id"]): r for r in t["markets"]}
    assert m[("series", "KXNBAGAME")]["fee_type"] == "quadratic" and m[("series", "KXNBAGAME")]["fee_multiplier"] == 1
    assert m[("metadata", T)]["reason"] == "added" and m[("metadata", T)]["volume_24h"] == 123.0
    assert m[("universe", T)]["reason"] == "paired" and m[("universe", U)]["in_grace"] is True
    lc = m[("lifecycle", "KXOTHER-1")]
    assert lc["reason"] == "determined" and lc["resolved_outcome"] == "yes" and lc["closed"] is True

    cov = {r["market_id"]: r for r in t["coverage"]}
    assert set(cov) == {T, U}  # an untracked market seen only in the trade feed gets no coverage row
    assert cov[T]["seq_gaps"] == 1 and cov[T]["gap_count"] == 0 and cov[T]["n_snapshots"] == 2

    # replay between the two socket snapshots of T: the order-book gap makes the interval unknown
    rp = replay.run([curated_path(Root.parse(tmp_path), "book_deltas", "kalshi", DT, HOUR)],
                    [curated_path(Root.parse(tmp_path), "book_snapshots", "kalshi", DT, HOUR)])
    assert rp["outcomes"]["skipped"] == 1 and rp["evaluated"] == 0


def test_ws_snapshot_replay_uses_stream_position(tmp_path):
    lines = [
        {"recv_ms": H0, "conn": 1, "ctl": "connect", "assets": [T]},
        snap(10, 1, T, [["0.4000", "100.00"]], [["0.5500", "70.00"]]),
        delta(20, 2, T, "0.4000", "-30.00", "yes"),
        delta(21, 3, T, "0.3900", "5.00", "yes"),
        snap(30, 4, T, [["0.4000", "70.00"], ["0.3900", "5.00"]], [["0.5500", "70.00"]]),
        delta(40, 5, T, "0.3900", "-5.00", "yes"),
        snap(50, 6, T, [["0.4000", "70.00"], ["0.3900", "1.00"]], [["0.5500", "70.00"]]),  # disagrees on purpose
    ]
    write_raw(tmp_path, "kalshi", "book_events", lines)
    curate.run(str(tmp_path), str(tmp_path), "kalshi", DT, HOUR)
    r = Root.parse(tmp_path)
    rp = replay.run([curated_path(r, "book_deltas", "kalshi", DT, HOUR)],
                    [curated_path(r, "book_snapshots", "kalshi", DT, HOUR)])
    assert rp["outcomes"]["exact"] == 1 and rp["outcomes"]["mismatch"] + rp["outcomes"]["top"] == 1


def test_previous_hour_warms_the_books(tmp_path):
    pdt, ph = previous_hour(DT, HOUR)
    write_raw(tmp_path, "kalshi", "book_events", [snap(-60_000, 1, T, [["0.4000", "100.00"]], [])], dt=pdt, hour=ph)
    write_raw(tmp_path, "kalshi", "book_events", [delta(5, 2, T, "0.4000", "-40.00", "yes")])
    rep = curate.run(str(tmp_path), str(tmp_path), "kalshi", DT, HOUR)
    assert "kalshi.delta_without_book" not in rep["stats"]
    d = _tables(tmp_path)["book_deltas"]
    assert [(r["action"], r["size"]) for r in d] == [("set", Decimal("60"))]
    rep = curate.run(str(tmp_path), str(tmp_path / "cold"), "kalshi", DT, HOUR, warm=False)
    assert rep["stats"]["kalshi.delta_without_book"] == 1
