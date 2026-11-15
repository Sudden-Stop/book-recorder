import json
from decimal import Decimal

import pyarrow.compute as pc
import pyarrow.parquet as pq

from book_pipeline import curate
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root

from conftest import DT, H0, HOUR, write_raw


def _table(curated, name, venue="polymarket"):
    return pq.read_table(curated_path(Root.parse(curated), name, venue, DT, HOUR))


def _report(curated):
    return json.loads((curated / "_report.json").read_text())


def test_all_tables_written_with_rows(curated):
    rows = _report(curated)["rows"]
    for name in ("book_deltas", "book_snapshots", "trades", "markets", "coverage"):
        assert rows[name] > 0, name
    assert "pairs" not in rows
    assert json.loads((curated / "_report_cross.json").read_text())["rows"] == {"pairs": 2}
    assert rows["coverage"] == 4  # four markets in the fixture
    assert _report(curated)["unknown_event_types"] == {}


def test_rows_stay_in_bounded_row_groups(curated):
    f = pq.ParquetFile(curated_path(Root.parse(curated), "book_deltas", "polymarket", DT, HOUR))
    assert f.metadata.num_row_groups > 1
    assert max(f.metadata.row_group(i).num_rows for i in range(f.metadata.num_row_groups)) <= 1000


def test_book_frames_become_reset_then_levels(curated):
    t = _table(curated, "book_deltas").to_pylist()
    first_reset = next(i for i, r in enumerate(t) if r["action"] == "reset")
    r0 = t[first_reset]
    levels = [r for r in t if r["line"] == r0["line"] and r["token_id"] == r0["token_id"] and r["action"] == "set"]
    assert levels and all(r["side"] in ("bid", "ask") for r in levels)
    assert {r["item"] for r in levels} == set(range(r0["item"] + 1, r0["item"] + 1 + len(levels)))
    assert r0["exchange_ts"] is not None


def test_price_change_sides_and_best_prices(curated):
    t = _table(curated, "book_deltas")
    pcs = t.filter(pc.and_(pc.equal(t["action"], "set"), pc.is_valid(t["best_bid"]))).to_pylist()
    assert pcs
    for r in pcs[:200]:
        assert r["best_bid"] < r["best_ask"]
        # a bid level never sits above the best bid it reports, nor an ask below the best ask (when non-empty)
        if r["size"] > 0 and r["side"] == "bid":
            assert r["price"] <= r["best_bid"]
        if r["size"] > 0 and r["side"] == "ask":
            assert r["price"] >= r["best_ask"]


def test_trades_from_both_sources(curated):
    t = _table(curated, "trades").to_pylist()
    ws = [r for r in t if r["source"] == "market_ws"]
    act = [r for r in t if r["source"] == "activity"]
    assert ws and act
    assert all(r["wallet"] is None and r["side"] in ("buy", "sell") for r in ws)
    assert all(r["wallet"] and r["wallet"] == r["wallet"].lower() and r["wallet"].startswith("0x") for r in act)
    # activity timestamps are whole seconds
    assert all(int(r["exchange_ts"].timestamp() * 1000) % 1000 == 0 for r in act)
    assert all(Decimal(0) < r["price"] < Decimal(1) for r in t)
    assert all(H0 <= int(r["recv_ts"].timestamp() * 1000) < H0 + 3_600_000 for r in t)


def test_markets_and_pairs(curated):
    m = _table(curated, "markets").to_pylist()
    meta = [r for r in m if r["source"] == "metadata"]
    uni = [r for r in m if r["source"] == "universe" and r["universe_rank"] is not None]
    assert len(meta) == 4 and len(uni) == 4
    assert all(len(r["tokens"]) == 2 and r["tick_size"] == Decimal("0.01") for r in meta)
    assert all(json.loads(r["raw"])["conditionId"] == r["market_id"] and r["reason"] == "added" for r in meta)
    assert all(r["reason"] == "top" and r["in_grace"] is False for r in uni)
    p = _table(curated, "pairs", "cross").to_pylist()
    assert [r["kalshi_market_id"] for r in p] == ["SYNTH-26NOV14-AAA", "SYNTH-26NOV14-BBB"]
    assert all(r["venue"] == "cross" and r["confidence"] == "exact" and r["teams"] == ["aaa", "bbb"]
               and len(r["polymarket_tokens"]) == 2 and r["kalshi_event_ticker"] == "SYNTH-26NOV14" for r in p)


def test_snapshot_lines_carry_request_time_and_skip_errors(curated, tmp_path):
    r = _table(curated, "book_snapshots").to_pylist()[0]
    assert r["source"] == "rest" and r["req_ts"] < r["recv_ts"]
    write_raw(tmp_path, "polymarket", "book_snapshots", [
        {"asset": "1", "recv_ms": H0 + 5, "req_ms": H0, "status": 404, "msg": {"error": "No orderbook"}},
        {"asset": "2", "recv_ms": H0 + 9, "req_ms": H0 + 1, "status": 200,
         "msg": {"market": "0xm", "asset_id": "2", "timestamp": str(H0), "bids": [{"price": "0.4", "size": "3"}],
                 "asks": []}},
    ])
    rep = curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    assert rep["rows"]["book_snapshots"] == 1 and rep["stats"]["snapshot.http_404"] == 1


def _pc(t, size="1"):
    return {"recv_ms": H0 + t, "conn": 1, "msg": {
        "event_type": "price_change", "market": "0xm", "timestamp": str(H0 + t),
        "price_changes": [{"asset_id": "1", "price": "0.5", "size": size, "side": "BUY"}]}}


def test_truncated_part_yields_complete_lines(tmp_path):
    p = write_raw(tmp_path, "polymarket", "book_events", [_pc(i, str(i + 1)) for i in range(2000)])
    data = p.read_bytes()
    p.write_bytes(data[: len(data) * 2 // 3])  # crash: no zstd end marker, last line half written
    write_raw(tmp_path, "polymarket", "book_events", [_pc(5000, "77")], part=1)  # the restart's part
    rep = curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    assert rep["truncated_files"] and rep["truncated_files"][0].endswith("rec-a-p0.ndjson.zst")
    rows = pq.read_table(curated_path(Root.parse(tmp_path), "book_deltas", "polymarket", DT, HOUR)).to_pylist()
    sizes = [int(r["size"]) for r in rows]
    assert 0 < len(sizes) < 2001 and sizes[:-1] == list(range(1, len(sizes))) and sizes[-1] == 77
    assert [r["line"] for r in rows] == list(range(len(rows)))  # numbering continues across parts


def test_local_data_dir_layout(tmp_path):
    import zstandard
    d = tmp_path / "polymarket" / "book_events" / f"dt={DT}"
    d.mkdir(parents=True)
    for part, size in ((0, "1"), (1, "2")):
        (d / f"{HOUR}.p{part}.ndjson.zst").write_bytes(
            zstandard.ZstdCompressor().compress((json.dumps(_pc(part, size)) + "\n").encode()))
    (d / "08.p0.ndjson.zst").write_bytes(zstandard.ZstdCompressor().compress(b"{}\n"))  # other hour: ignored
    rep = curate.run(str(tmp_path), str(tmp_path / "out"), "polymarket", DT, HOUR, local_host="rec-a")
    assert rep["read"]["book_events"]["layout"] == "local" and rep["read"]["book_events"]["files"] == 2
    rows = pq.read_table(curated_path(Root.parse(tmp_path / "out"), "book_deltas", "polymarket", DT, HOUR)).to_pylist()
    assert [(r["host"], int(r["size"])) for r in rows] == [("rec-a", 1), ("rec-a", 2)]


def test_snapshot_levels_sorted_best_first(curated):
    for r in _table(curated, "book_snapshots").to_pylist()[:50]:
        bids = [lv["price"] for lv in r["bids"]]
        asks = [lv["price"] for lv in r["asks"]]
        assert bids == sorted(bids, reverse=True)
        assert asks == sorted(asks)
        if bids and asks:
            assert bids[0] < asks[0]


def test_unknown_events_and_bad_lines_are_counted(tmp_path):
    write_raw(tmp_path, "polymarket", "book_events", [
        {"recv_ms": H0 + 10, "conn": 1, "msg": {"event_type": "brand_new", "asset_id": "1"}},
        {"recv_ms": H0 + 11, "conn": 1, "msg": {"event_type": "price_change", "market": "0xm", "timestamp": "1",
                                                "price_changes": [{"asset_id": "1", "price": "0.5", "size": "10",
                                                                   "side": "BUY", "extra_field": 1}]}},
        {"recv_ms": H0 + 12, "conn": 1, "msg": "not json"},
    ])
    rep = curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    assert rep["unknown_event_types"] == {"brand_new": 1}
    assert rep["new_keys"] == {"price_change.item": ["extra_field"]}
    assert rep["stats"]["frames.unparsed"] == 1
    assert rep["rows"]["book_deltas"] == 1


def test_long_decimals_are_rounded_not_rejected(tmp_path):
    write_raw(tmp_path, "polymarket", "book_events", [
        {"recv_ms": H0 + 1, "conn": 1, "msg": {"event_type": "last_trade_price", "market": "0xm", "asset_id": "1",
                                               "price": "0.1234567", "size": "1.0000004", "side": "SELL",
                                               "timestamp": str(H0)}},
    ])
    rep = curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    assert rep["decimals_rounded"] == 2
    r = pq.read_table(curated_path(Root.parse(tmp_path), "trades", "polymarket", DT, HOUR)).to_pylist()[0]
    assert r["price"] == Decimal("0.123457") and r["size"] == Decimal("1.000000") and r["side"] == "sell"


def test_hosts_and_parts_are_ordered(tmp_path):
    ev = lambda t, size: {"recv_ms": H0 + t, "conn": 1, "msg": {  # noqa: E731
        "event_type": "price_change", "market": "0xm", "timestamp": str(H0 + t),
        "price_changes": [{"asset_id": "1", "price": "0.5", "size": size, "side": "BUY"}]}}
    write_raw(tmp_path, "polymarket", "book_events", [ev(1, "1"), ev(2, "2")], host="rec-a", part=0)
    write_raw(tmp_path, "polymarket", "book_events", [ev(3, "3")], host="rec-a", part=1)
    write_raw(tmp_path, "polymarket", "book_events", [ev(1, "9")], host="rec-b", part=0)
    curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    rows = pq.read_table(curated_path(Root.parse(tmp_path), "book_deltas", "polymarket", DT, HOUR)).to_pylist()
    a = [(r["line"], str(r["size"])) for r in rows if r["host"] == "rec-a"]
    assert a == [(0, "1.000000"), (1, "2.000000"), (2, "3.000000")]
    assert [r["line"] for r in rows if r["host"] == "rec-b"] == [0]
