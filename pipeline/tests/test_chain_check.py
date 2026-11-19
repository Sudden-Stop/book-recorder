from decimal import Decimal

from book_pipeline import chain_check as cc

EX = "0xe111180000d2663c0091e4f400237545b87b996b"


def _log(tx, maker, taker, side, token, maker_amt, taker_amt, fee=0, idx=0):
    words = [side, token, maker_amt, taker_amt, fee, 0, 0]
    return {"address": EX, "transactionHash": tx, "blockNumber": "0x10", "logIndex": hex(idx),
            "topics": [cc.ORDER_FILLED, "0x" + "ab" * 32, "0x" + "0" * 24 + maker[2:], "0x" + "0" * 24 + taker[2:]],
            "data": "0x" + "".join(f"{w:064x}" for w in words)}


A, B = "0x" + "a" * 40, "0x" + "b" * 40


def test_decode_buy_and_sell():
    buy = cc.decode(_log("0x01", A, B, 0, 7, 9_230_770, 19_230_770))
    assert (buy["side"], buy["shares"], buy["usdc"], buy["maker"], buy["is_taker_order"]) == ("buy", 19_230_770, 9_230_770, A, False)
    sell = cc.decode(_log("0x01", B, EX, 1, 7, 94_940_000, 3_322_900))
    assert (sell["side"], sell["shares"], sell["usdc"], sell["is_taker_order"]) == ("sell", 94_940_000, 3_322_900, True)


def _row(tx, wallet, token, side, size, price, ts):
    return {"tx_hash": tx, "wallet": wallet, "token_id": token, "side": side, "size": Decimal(size),
            "price": Decimal(price), "exchange_ts": ts * 1000, "recv_ts": ts * 1000}


def test_reconcile_counts_missing_and_pairs_by_price():
    fills = []
    for i, (usdc, idx) in enumerate(((4_000_000, 0), (5_000_000, 1), (3_000_000, 2))):
        f = cc.decode(_log("0x01", A, B, 0, 7, usdc, 10_000_000, idx=idx))
        f["block_ts"] = 100 + i
        fills.append(f)
    # the feed sends two of the three identical-size fills, in the reverse price order
    rows = [_row("0x01", A, "7", "buy", "10", "0.5", 100), _row("0x01", A, "7", "buy", "10", "0.4", 100)]
    rep = cc.reconcile(fills, rows, 100, 200, gaps=[])
    assert rep["onchain_fills"] == 3 and rep["matched"] == 2
    assert rep["missing"]["by_cause"] == {"tx_partially_in_feed": 1}
    assert rep["price_mismatches"] == 0
    assert rep["extra_activity_rows"]["n"] == 0


def test_reconcile_attributes_outage():
    f = cc.decode(_log("0x02", A, B, 0, 7, 1_000_000, 2_000_000))
    f["block_ts"] = 150
    rep = cc.reconcile([f], [], 100, 200, gaps=[{"from_ms": 140_000, "to_ms": 160_000, "disconnects": ["stalled"]}])
    assert rep["missing"]["by_cause"] == {"outage:stalled": 1}


class _FakeRpc(cc.Rpc):
    """eth_getLogs answered from a block -> logs map; refuses ranges wider than `limit` like Alchemy's free plan."""

    def __init__(self, logs_by_block, limit):
        super().__init__(["https://rpc.test/v2/key"])
        self.logs_by_block, self.limit = logs_by_block, limit

    def call_one(self, url, method, params):
        self.calls += 1
        b, e = int(params[0]["fromBlock"], 16), int(params[0]["toBlock"], 16)
        if e - b + 1 > self.limit:
            raise cc.RpcError("block range too large")
        return [lg for n in range(b, e + 1) for lg in self.logs_by_block.get(n, [])]


def test_get_logs_shrinks_range_and_keeps_every_log():
    logs = {n: [_log(f"0x{n:02x}", A, B, 0, 7, 1, 2)] for n in range(100, 160)}
    rpc = _FakeRpc(logs, limit=10)
    got = cc.get_logs(rpc, 100, 159)
    assert sorted(lg["transactionHash"] for lg in got) == sorted(f"0x{n:02x}" for n in range(100, 160))
    assert rpc.logs_range["https://rpc.test/v2/key"] == 10


def test_decode_uses_block_timestamp_from_the_log():
    lg = _log("0x01", A, B, 0, 7, 1, 2)
    lg["blockTimestamp"] = hex(1_795_003_201)
    assert cc.decode(lg)["block_ts"] == 1_795_003_201


def test_problems():
    f = cc.decode(_log("0x01", A, B, 0, 7, 1_000_000, 2_000_000))
    f["block_ts"] = 150
    rep = {**cc.reconcile([f], [], 100, 200, gaps=[]), "unknown_exchanges": []}
    assert rep["in_scope"]["completeness_excl_outages_pct"] == 0.0
    assert any("completeness" in p for p in cc.problems(rep))
    ok = {**cc.reconcile([f], [_row("0x01", A, "7", "buy", "2", "0.5", 150)], 100, 200, gaps=[]),
          "unknown_exchanges": []}
    assert cc.problems(ok) == []
    assert cc.problems({**ok, "unknown_exchanges": ["0xdead"]})


def test_nightly_runs_the_chain_check_only_when_an_rpc_is_set(tmp_path, monkeypatch):
    import shutil

    from book_pipeline import nightly
    from conftest import DT, FIXTURES, HOUR

    root = tmp_path / "bucket"
    shutil.copytree(FIXTURES / "raw", root / "raw")
    calls = []

    def fake(root_uri, dt, hours, **kw):
        calls.append((dt, hours))
        return {"ok": False, "problems": ["in-scope completeness outside outages 99.0% < 99.8%"],
                "in_scope": {}, "missing": {"by_cause": {}}}

    monkeypatch.setattr(nightly.chain_check, "run", fake)
    monkeypatch.delenv(cc.RPC_ENV, raising=False)
    s = nightly.run(str(root), DT, venues=("polymarket",), log=lambda e: None)
    assert s["ok"] and calls == []
    monkeypatch.setenv(cc.RPC_ENV, "https://rpc.test/v2/key")
    s = nightly.run(str(root), DT, venues=("polymarket",), log=lambda e: None)
    assert calls == [(DT, [HOUR])]
    assert not s["ok"] and s["failed"][0]["step"] == "chain"
