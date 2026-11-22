"""prepare: the derived tables, from synthetic raw through curate."""
import json

import pyarrow.parquet as pq
import pytest

from book_pipeline import chain_check as cc
from book_pipeline import curate, prepare
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root
from book_pipeline.venues import kalshi

from conftest import DT, H0, HOUR, write_raw

M, YES, NO = "0x" + "aa" * 32, "111", "222"
K = "KXTEST-26NOV14-A"


def gamma(**more):
    return {"conditionId": M, "clobTokenIds": json.dumps([YES, NO]), "outcomes": json.dumps(["Yes", "No"]),
            "orderPriceMinTickSize": 0.01, "feesEnabled": True, "feeType": "sports_fees_v3",
            "feeSchedule": {"rate": 0.05}, "events": [{"slug": "game-1", "seriesSlug": "nba"}],
            "sportsMarketType": "moneyline", "gameStartTime": "2026-11-14 07:30:00+00", **more}


def ws(tx, side, price, size, token=YES):
    return {"recv_ms": H0 + 2000, "conn": 1, "msg": {
        "event_type": "last_trade_price", "market": M, "asset_id": token, "side": side, "price": str(price),
        "size": str(size), "timestamp": str(H0 + 2000), "transaction_hash": tx}}


def order(tx, side, price, size, token=YES, wallet="0xa", market=M):
    return {"recv_ms": H0 + 3000, "conn": 2, "msg": {"type": "trades", "topic": "activity", "payload": {
        "transactionHash": tx, "asset": token, "conditionId": market, "proxyWallet": wallet, "side": side,
        "size": size, "price": price, "timestamp": H0 // 1000 + 2}}}


def build(root, prints, orders, record=None):
    write_raw(root, "polymarket", "book_events",
              [{"recv_ms": H0 + 1000, "conn": 1, "ctl": "connect", "assets": [YES, NO]}, *prints])
    write_raw(root, "polymarket", "activity", orders)
    write_raw(root, "polymarket", "markets", [{"ts_ms": H0 + 500, "reason": "added", "market": record or gamma()}])
    curate.run(str(root), str(root), "polymarket", DT, HOUR)


def prepared(root, rpc=None):
    info = prepare.run(str(root), DT, DT, root / "out", rpc=rpc)
    read = lambda name: pq.read_table(root / "out" / f"{name}.parquet").to_pylist()  # noqa: E731
    return info, read("markets"), {r["tx_hash"]: r for r in read("trade_reach")}


def test_reach_is_the_worst_maker_in_the_takers_terms(tmp_path):
    build(tmp_path, [
        ws("0xbuy", "BUY", 0.42, 30), ws("0xsell", "SELL", 0.39, 20), ws("0xone", "BUY", 0.50, 5),
        ws("0xpart", "BUY", 0.60, 10), ws("0xnone", "BUY", 0.70, 10), ws("0xbad", "BUY", 0.40, 4),
    ], [
        # a buy that took 0.41 on its own book and 0.44 through a buyer of NO at 0.56
        order("0xbuy", "BUY", 0.42, 30), order("0xbuy", "SELL", 0.41, 20, wallet="0xb"),
        order("0xbuy", "BUY", 0.56, 10, token=NO, wallet="0xc"),
        order("0xsell", "SELL", 0.39, 20), order("0xsell", "BUY", 0.40, 10, wallet="0xb"),
        order("0xsell", "BUY", 0.38, 10, wallet="0xc"),
        order("0xone", "SELL", 0.50, 5, wallet="0xb"),  # the taker's own row need not be there
        order("0xpart", "BUY", 0.60, 10), order("0xpart", "SELL", 0.59, 6, wallet="0xb"),  # a maker is missing
        order("0xnone", "BUY", 0.70, 10),
        order("0xbad", "SELL", 0.40, 4, wallet="0xb", market="0x" + "bb" * 32),
    ])
    info, _, reach = prepared(tmp_path)
    buy, sell, one = reach["0xbuy"], reach["0xsell"], reach["0xone"]
    assert (buy["status"], buy["first_price"], buy["reach_price"], buy["n_levels"]) == ("exact", 0.41, 0.44, 2)
    assert buy["print_price"] == 0.42 and buy["vwap"] == pytest.approx(0.42)
    assert (sell["status"], sell["first_price"], sell["reach_price"]) == ("exact", 0.40, 0.38)
    assert (one["status"], one["reach_price"], one["n_levels"]) == ("exact", 0.50, 1)
    for tx, status in (("0xpart", "partial"), ("0xnone", "no_makers"), ("0xbad", "inconsistent")):
        assert reach[tx]["status"] == status and reach[tx]["reach_price"] is None  # never the print in its place
    assert info["trade_reach"]["sweeps"] == 2 and info["trade_reach"]["sweeps_print_is_not_reach"] == 2


def test_reach_rows_join_back_to_trades(tmp_path):
    build(tmp_path, [ws("0x1", "BUY", 0.5, 1), ws("0x2", "BUY", 0.5, 1)], [order("0x1", "SELL", 0.5, 1, wallet="0xb")])
    _, _, reach = prepared(tmp_path)
    trades = pq.read_table(curated_path(Root.parse(tmp_path), "trades", "polymarket", DT, HOUR)).to_pylist()
    key = lambda r: (r["source"], r["host"], r["line"], r["item"])  # noqa: E731
    by_key = {key(t): t for t in trades}
    assert len(by_key) == len(trades)  # `line` alone would collide between the two streams
    for tx, r in reach.items():
        assert (r["dt"], r["hour"], by_key[key(r)]["tx_hash"]) == (DT, HOUR, tx)


def test_surplus_keeps_the_copy_nearest_its_block():
    fill = {"tx_hash": "0x1", "maker": "0xa", "token_id": YES, "side": "buy", "shares": 10_000_000, "block_ts": 1000}
    row = {"tx_hash": "0x1", "wallet": "0xA", "token_id": YES, "side": "buy", "size": 10, "recv_ms": 1}
    late, near = {**row, "ts_ms": 1_009_000, "line": 1}, {**row, "ts_ms": 1_000_000, "line": 2}
    assert prepare.surplus_rows([fill], [late, near], 900, 1100) == [late]
    assert prepare.surplus_rows([fill, fill], [late, near], 900, 1100) == []  # two real fills
    assert prepare.surplus_rows([], [near], 900, 1100) == [near]
    assert prepare.surplus_rows([], [near], 2000, 3000) == []  # outside the window


def test_reach_ignores_surplus_rows(tmp_path):
    build(tmp_path, [ws("0x1", "BUY", 0.5, 5)],
          [order("0x1", "SELL", 0.5, 5, wallet="0xb"),
           {**order("0x1", "SELL", 0.5, 5, wallet="0xb"), "recv_ms": H0 + 3001}])  # two copies on one socket
    _, _, reach = prepared(tmp_path)
    assert reach["0x1"]["status"] == "partial"
    copies = [r for r in pq.read_table(curated_path(Root.parse(tmp_path), "trades", "polymarket", DT, HOUR)).to_pylist()
              if r["source"] == "activity"]
    extra = [{"dt": DT, "hour": HOUR, "source": "activity", "host": copies[1]["host"], "line": copies[1]["line"],
              "item": 0, "tx_hash": "0x1"}]
    out = tmp_path / "reach.parquet"
    root = Root.parse(tmp_path)
    prepare.trade_reach(prepare.replay.connect([str(tmp_path)]), root, str(tmp_path), [DT], extra, out)
    assert pq.read_table(out).to_pylist()[0]["status"] == "exact"


class FakeChain(cc.Rpc):
    def __init__(self, logs, limit=10_000):
        super().__init__(["https://rpc.test"])
        self.logs, self.limit, self.ranges = logs, limit, []

    def head(self):
        return 250

    def block_ts(self, n):
        return H0 // 1000 + n

    def call_one(self, url, method, params):
        b, e = int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16)
        if e - b + 1 > self.limit:
            raise cc.RpcError("range too large")
        self.ranges.append((b, e))
        return [lg for lg in self.logs if b <= int(lg["blockNumber"], 16) <= e]


def resolution(market, payouts, block):
    words = [2, 64, 2, *payouts]
    return {"topics": [prepare.RESOLUTION, market], "blockNumber": hex(block), "blockTimestamp": hex(H0 // 1000 + 900),
            "data": "0x" + "".join(f"{w:064x}" for w in words)}


def test_resolutions_cover_the_whole_range_when_the_rpc_refuses_wide_ones():
    rpc = FakeChain([resolution(M, [0, 1], 5), resolution("0x" + "cc" * 32, [1, 0], 200)], limit=64)
    got = prepare.fetch_resolutions(rpc, 0, 250, {M})
    assert list(got) == [M] and got[M]["payouts"] == [0.0, 1.0]
    assert rpc.ranges[0][0] == 0 and rpc.ranges[-1][1] == 250
    assert all(b == e + 1 for (_, e), (b, _) in zip(rpc.ranges, rpc.ranges[1:]))


def test_resolutions_fail_loudly_rather_than_return_part():
    with pytest.raises(RuntimeError, match="no RPC returned"):
        prepare.fetch_resolutions(FakeChain([], limit=0), 0, 10, {M})


@pytest.mark.parametrize("payouts, outcome, source", [([0, 1], "No", "chain"), ([1, 1], None, "chain_split")])
def test_missing_outcome_comes_from_chain(tmp_path, monkeypatch, payouts, outcome, source):
    monkeypatch.setattr(cc, "block_at", lambda rpc, ts: 0)
    build(tmp_path, [ws("0x1", "BUY", 0.5, 1)], [])
    _, markets, _ = prepared(tmp_path, FakeChain([resolution(M, payouts, 7)]))
    m = markets[0]
    assert (m["outcome"], m["outcome_source"], m["resolution_source"]) == (outcome, source, "chain")
    assert m["category"] == "sports" and m["is_sports"] and m["series"] == "nba"
    assert (m["fee_type"], m["fee_rate"], m["tick_size"]) == ("sports_fees_v3", 0.05, 0.01)
    assert int(m["event_start_ts"].timestamp() * 1000) == H0 + 30 * 60_000


def test_chain_that_contradicts_the_venue_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "block_at", lambda rpc, ts: 0)
    build(tmp_path, [ws("0x1", "BUY", 0.5, 1)], [], gamma(closed=True, outcomePrices=json.dumps(["1", "0"])))
    with pytest.raises(RuntimeError, match="agree on only 0 of 1"):
        prepared(tmp_path, FakeChain([resolution(M, [0, 1], 7)]))


def test_polymarket_category():
    cat = lambda **raw: prepare.polymarket_category(raw)[0]  # noqa: E731
    assert cat(sportsMarketType="totals") == cat(feeType="sports_fees_nfl_cfb_oct26") == "sports"
    assert cat(feeType="politics_elections_fees") == "elections"
    assert cat(feeType="finance_prices_fees") == "finance"
    assert cat(feeType="crypto_fees_v2") == "crypto"
    assert cat() is None


def test_kalshi_tick_category_and_fee(tmp_path):
    record = {"ticker": K, "event_ticker": "KXTEST-26NOV14", "price_level_structure": "tapered_deci_cent",
              "price_ranges": [{"start": "0", "end": "0.1", "step": "0.0010"},
                               {"start": "0.1", "end": "0.9", "step": "0.0100"}]}
    assert str(kalshi.min_step(record)) == "0.0010"
    assert kalshi.min_step({}) is None and kalshi.min_step({"price_ranges": [{"step": "0"}]}) is None
    write_raw(tmp_path, "kalshi", "book_events", [
        {"recv_ms": H0 + 1, "conn": 1, "ctl": "connect", "assets": [K]},
        {"recv_ms": H0 + 9, "conn": 1, "msg": {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {
            "market_ticker": K, "yes_dollars_fp": [["0.4", "5"]], "no_dollars_fp": []}}}])
    write_raw(tmp_path, "kalshi", "markets", [{"ts_ms": H0, "reason": "added", "market": record}])
    write_raw(tmp_path, "kalshi", "series", [{"ts_ms": H0, "series": {"series": {
        "ticker": "KXTEST", "category": "Sports", "fee_type": "quadratic", "fee_multiplier": 1}}}])
    curate.run(str(tmp_path), str(tmp_path), "kalshi", DT, HOUR)
    curated = pq.read_table(curated_path(Root.parse(tmp_path), "markets", "kalshi", DT, HOUR)).to_pylist()
    assert float(next(r for r in curated if r["source"] == "metadata")["tick_size"]) == 0.001
    _, markets, _ = prepared(tmp_path)
    m = markets[0]
    assert (m["category"], m["tick_size"], m["fee_type"], m["fee_multiplier"]) == ("sports", 0.001, "quadratic", 1.0)


def test_polymarket_fee_type_is_curated(tmp_path):
    build(tmp_path, [], [])
    rows = pq.read_table(curated_path(Root.parse(tmp_path), "markets", "polymarket", DT, HOUR)).to_pylist()
    assert next(r for r in rows if r["source"] == "metadata")["fee_type"] == "sports_fees_v3"


def test_exclusions(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("venue,from_utc,to_utc,reason\nkalshi,2026-11-19T08:00Z,2026-11-19T10:00Z,pause\n", encoding="utf-8")
    assert prepare.read_exclusions(p, ["2026-11-19"])[0]["to_ts"] - prepare.read_exclusions(p, ["2026-11-19"])[0]["from_ts"] == 7_200_000
    assert prepare.read_exclusions(p, ["2026-11-20"]) == []
    p.write_text("venue,from_utc,to_utc,reason\nkalshi,2026-11-19T10:00Z,2026-11-19T08:00Z,backwards\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty exclusion"):
        prepare.read_exclusions(p, ["2026-11-19"])


def test_the_committed_exclusions_parse():
    path = prepare.Path(__file__).resolve().parents[2] / "docs" / "data_exclusions.csv"
    assert len(prepare.read_exclusions(path, prepare._days("2026-11-15", "2026-11-21"))) == 4
