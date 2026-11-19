"""Parallel activity sockets: each fill kept once, legitimate repeats kept, hour boundary handled."""
import pyarrow.parquet as pq

from book_pipeline import curate
from book_pipeline.curate import curated_path, previous_hour
from book_pipeline.storage import Root

from conftest import DT, H0, HOUR, write_raw


def fill(tx, size=10.0, wallet="0xAbC"):
    return {"type": "trades", "topic": "activity", "payload": {
        "transactionHash": tx, "asset": "111", "conditionId": "0xm", "proxyWallet": wallet, "side": "BUY",
        "size": size, "price": 0.42, "fee": 0.01, "timestamp": H0 // 1000, "outcome": "Yes", "outcomeIndex": 0}}


def line(t, conn, msg):
    return {"recv_ms": t, "conn": conn, "msg": msg}


def trades(out):
    return pq.read_table(curated_path(Root.parse(str(out)), "trades", "polymarket", DT, HOUR)).to_pylist()


def test_copies_from_parallel_sockets_are_kept_once(tmp_path):
    a = [fill("0x1"), fill("0x2"), fill("0x3"), fill("0x3")]  # 0x3: two identical fills in one transaction
    b = [fill("0x1"), fill("0x3"), fill("0x3"), fill("0x4")]  # missed 0x2 (stalled), saw 0x4 alone
    lines = [line(H0 + 1000 + i, 1, m) for i, m in enumerate(a)] + [line(H0 + 1000 + i, 2, m) for i, m in enumerate(b)]
    lines.sort(key=lambda r: r["recv_ms"])
    write_raw(tmp_path, "polymarket", "activity", lines)
    rep = curate.run(str(tmp_path), str(tmp_path / "out"), "polymarket", DT, HOUR)

    got = sorted(r["tx_hash"] for r in trades(tmp_path / "out"))
    assert got == ["0x1", "0x2", "0x3", "0x3", "0x4"]
    assert rep["stats"]["activity.duplicates"] == 3
    assert rep["stats"]["activity.connections"] == 2
    assert rep["stats"]["activity.fills_single_conn"] == 2  # 0x2 and 0x4
    assert rep["read"]["warm:activity"]["files"] == 0


def test_a_fill_straddling_the_hour_is_kept_in_the_first_hour_only(tmp_path):
    pdt, ph = previous_hour(DT, HOUR)
    write_raw(tmp_path, "polymarket", "activity",
              [line(H0 - 120_000, 1, fill("0xold")), line(H0 - 5, 1, fill("0xedge"))], dt=pdt, hour=ph)
    write_raw(tmp_path, "polymarket", "activity",
              [line(H0 + 3, 2, fill("0xedge")), line(H0 + 10, 1, fill("0xnew")), line(H0 + 11, 2, fill("0xnew"))])
    rep = curate.run(str(tmp_path), str(tmp_path / "out"), "polymarket", DT, HOUR)
    assert [r["tx_hash"] for r in trades(tmp_path / "out")] == ["0xnew"]
    assert rep["stats"]["activity.duplicates"] == 2

    # Without warming, the late copy of the boundary fill is kept.
    curate.run(str(tmp_path), str(tmp_path / "cold"), "polymarket", DT, HOUR, warm=False)
    assert sorted(r["tx_hash"] for r in trades(tmp_path / "cold")) == ["0xedge", "0xnew"]
