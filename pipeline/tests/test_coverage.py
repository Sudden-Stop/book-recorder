import pyarrow.parquet as pq
import pytest

from book_pipeline import curate
from book_pipeline.coverage import CoverageTracker
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root

from conftest import DT, H0, HOUR, write_raw

MIN = 60_000


def pc(t, market, token, size="1", conn=1):
    return {"recv_ms": H0 + t, "conn": conn, "msg": {
        "event_type": "price_change", "market": market, "timestamp": str(H0 + t),
        "price_changes": [{"asset_id": token, "price": "0.5", "size": size, "side": "BUY"}]}}


@pytest.fixture
def covered(tmp_path):
    lines = [
        # conn 1 opens in this hour with A1, A2 (market M)
        {"recv_ms": H0, "conn": 1, "ctl": "connect", "assets": ["A1", "A2"]},
        # conn 2 was already open (its connect is in the previous hour): frames first
        pc(5_000, "N", "B1", conn=2),
        pc(MIN, "M", "A1"),
        pc(MIN + 1, "M", "A2"),
        {"recv_ms": H0 + 10 * MIN, "conn": 1, "ctl": "disconnect", "code": 1006, "reason": ""},
        {"recv_ms": H0 + 10 * MIN + 500, "conn": 1, "ctl": "connect_error", "error": "timeout"},
        {"recv_ms": H0 + 11 * MIN, "conn": 1, "ctl": "connect", "assets": ["A1", "A2"]},
        {"recv_ms": H0 + 20 * MIN, "conn": 1, "ctl": "subscribe", "assets": ["C1"]},
        pc(25 * MIN, "P", "C1"),
        {"recv_ms": H0 + 30 * MIN, "conn": 2, "ctl": "disconnect", "code": None, "reason": "stall"},
        pc(59 * MIN, "M", "A1"),
    ]
    write_raw(tmp_path, "polymarket", "book_events", lines)
    curate.run(str(tmp_path), str(tmp_path), "polymarket", DT, HOUR)
    root = Root.parse(tmp_path)
    cov = {r["market_id"]: r for r in pq.read_table(curated_path(root, "coverage", "polymarket", DT, HOUR)).to_pylist()}
    deltas = pq.read_table(curated_path(root, "book_deltas", "polymarket", DT, HOUR)).to_pylist()
    return cov, deltas


def test_reconnect_gap(covered):
    cov, _ = covered
    m = cov["M"]
    assert m["token_ids"] == ["A1", "A2"]
    assert m["subscribed_s"] == pytest.approx(3600)
    assert m["connected_s"] == pytest.approx(3600 - 60)
    assert m["gap_s"] == pytest.approx(60)
    assert m["gap_count"] == 1 and m["connects"] == 2
    assert m["start_assumed"] is False
    assert m["n_level_changes"] == 3


def test_connection_open_before_the_hour_is_assumed_up(covered):
    cov, _ = covered
    n = cov["N"]
    assert n["start_assumed"] is True
    assert n["connected_s"] == pytest.approx(1800)
    assert n["subscribed_s"] == pytest.approx(3600)
    assert n["gap_count"] == 1


def test_mid_hour_subscribe(covered):
    cov, _ = covered
    p = cov["P"]
    assert p["subscribed_s"] == pytest.approx(2400)
    assert p["connected_s"] == pytest.approx(2400)
    assert p["gap_count"] == 0


def test_disconnect_writes_gap_rows(covered):
    _, deltas = covered
    gaps = [(r["token_id"], int(r["recv_ts"].timestamp() * 1000) - H0) for r in deltas if r["action"] == "gap"]
    assert sorted(gaps) == [("A1", 10 * MIN), ("A2", 10 * MIN), ("B1", 30 * MIN)]


def test_overlapping_connections_are_not_double_counted():
    c = CoverageTracker("polymarket", H0)
    for conn in (1, 2):
        c.ctl("h", conn, H0, "connect", ["X"])
    c.ctl("h", 1, H0 + 10 * MIN, "disconnect", None)
    r = c.rows({"X": "MX"})[0]
    assert r["subscribed_s"] == pytest.approx(3600) and r["connected_s"] == pytest.approx(3600)
    assert r["gap_count"] == 1


def test_fixture_coverage(curated):
    rows = pq.read_table(curated_path(Root.parse(curated), "coverage", "polymarket", DT, HOUR)).to_pylist()
    assert len(rows) == 4
    for r in rows:
        assert len(r["token_ids"]) == 2
        assert r["n_snapshots"] > 100
        assert r["n_level_changes"] > 0
        assert r["gap_count"] == 0
        assert r["start_assumed"] is True  # the fixture hour starts mid-connection
        assert 0 < r["connected_s"] <= 3600
