"""A recorder that restarts without writing `disconnect`: the outage has to show in coverage, gaps and replay."""
import json

import pyarrow.parquet as pq
import pytest
import zstandard

from book_pipeline import curate, replay
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root

from conftest import DT, H0, HOUR, write_raw

MIN = 60_000
HOUR_MS = 60 * MIN


def pc(t, market, token, conn=1, size="1"):
    return {"recv_ms": H0 + t, "conn": conn, "msg": {
        "event_type": "price_change", "market": market, "timestamp": str(H0 + t),
        "price_changes": [{"asset_id": token, "price": "0.5", "size": size, "side": "BUY"}]}}


def ctl(t, kind, conn=1, *assets):
    rec = {"recv_ms": H0 + t, "conn": conn, "ctl": kind}
    return {**rec, "assets": list(assets)} if assets else rec


def curated(root, venue="polymarket"):
    rep = curate.run(str(root), str(root), venue, DT, HOUR)
    r = Root.parse(root)
    read = lambda table: pq.read_table(curated_path(r, table, venue, DT, HOUR)).to_pylist()  # noqa: E731
    cov = {(x["host"], x["market_id"]): x for x in read("coverage")}
    deltas = read("book_deltas")
    gaps = sorted((x["token_id"], int(x["recv_ts"].timestamp() * 1000) - H0) for x in deltas if x["action"] == "gap")
    return cov, gaps, deltas, rep["stats"].get("ctl.disconnect_inferred", 0)


def write(root, *parts, hour=HOUR, host="rec-a"):
    for n, lines in enumerate(parts):
        write_raw(root, "polymarket", "book_events", lines, hour=hour, host=host, part=n)


def later(hours=1):
    return f"{int(HOUR) + hours:02d}"


def test_restart_inside_the_hour_is_a_gap_from_the_last_line(tmp_path):
    write(tmp_path,
          [pc(MIN, "M", "A", conn=7), pc(2 * MIN, "N", "B", conn=9), pc(10 * MIN, "M", "A", conn=7)],
          [ctl(15 * MIN, "connect", 1, "A"), ctl(16 * MIN, "connect", 2, "B"), pc(20 * MIN, "M", "A", conn=1)])
    cov, gaps, deltas, inferred = curated(tmp_path)
    assert inferred == 2
    assert gaps == [("A", 10 * MIN), ("B", 10 * MIN)]
    for market, back in (("M", 15), ("N", 16)):
        row = cov["rec-a", market]
        assert row["gap_s"] == pytest.approx((back - 10) * 60)
        assert row["connected_s"] == pytest.approx(3600 - (back - 10) * 60)
        assert row["gap_count"] == 1
    keys = [(x["host"], x["line"], x["item"]) for x in deltas]
    assert len(keys) == len(set(keys))


def test_restart_that_reuses_the_connection_id(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=1)], [ctl(5 * MIN, "connect", 1, "A"), pc(6 * MIN, "M", "A", conn=1)])
    cov, gaps, _, inferred = curated(tmp_path)
    assert inferred == 1 and gaps == [("A", MIN)]
    assert cov["rec-a", "M"]["gap_s"] == pytest.approx(240)


def test_restart_whose_first_dial_fails(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=7)],
          [ctl(3 * MIN, "connect_error", 1), ctl(5 * MIN, "connect", 2, "A")])
    cov, _, _, inferred = curated(tmp_path)
    assert inferred == 1
    assert cov["rec-a", "M"]["gap_s"] == pytest.approx(240)


def test_two_restarts_in_one_hour(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=7)],
          [ctl(3 * MIN, "connect", 1, "A"), pc(10 * MIN, "M", "A", conn=1)],
          [ctl(14 * MIN, "connect", 1, "A")])
    cov, gaps, _, inferred = curated(tmp_path)
    assert inferred == 2 and gaps == [("A", MIN), ("A", 10 * MIN)]
    assert cov["rec-a", "M"]["gap_s"] == pytest.approx(120 + 240)


def test_truncated_part_is_closed_at_its_last_complete_line(tmp_path):
    p = write_raw(tmp_path, "polymarket", "book_events", [])
    z = zstandard.ZstdCompressor().compressobj()
    flushed = z.compress((json.dumps(pc(MIN, "M", "A", conn=7)) + "\n").encode())
    flushed += z.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK)  # the recorder flushes on every tick
    half = z.compress(json.dumps(pc(2 * MIN, "M", "A", conn=7))[:40].encode())
    p.write_bytes(flushed + half + z.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK))  # killed: the frame has no end
    write_raw(tmp_path, "polymarket", "book_events", [ctl(5 * MIN, "connect", 1, "A")], part=1)
    _, gaps, _, inferred = curated(tmp_path)
    assert inferred == 1 and gaps == [("A", MIN)]


def test_a_late_frame_opening_a_part_is_not_a_restart(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=7)], [pc(59 * MIN, "M", "A", conn=7)])
    cov, gaps, _, inferred = curated(tmp_path)
    assert not inferred and not gaps
    assert cov["rec-a", "M"]["gap_s"] == 0


def test_a_logged_shutdown_gets_nothing_added(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=7), ctl(2 * MIN, "disconnect", 7)], [ctl(5 * MIN, "connect", 1, "A")])
    cov, gaps, _, inferred = curated(tmp_path)
    assert not inferred and gaps == [("A", 2 * MIN)]
    assert cov["rec-a", "M"]["gap_s"] == pytest.approx(180)


def test_only_the_connections_left_open_are_closed(tmp_path):
    write(tmp_path,
          [pc(MIN, "M", "A", conn=7), pc(MIN, "N", "B", conn=9), ctl(2 * MIN, "disconnect", 7)],
          [ctl(5 * MIN, "connect", 1, "A", "B")])
    _, gaps, _, inferred = curated(tmp_path)
    assert inferred == 1 and gaps == [("A", 2 * MIN), ("B", 2 * MIN)]


@pytest.mark.parametrize("ahead", [1, 3])
def test_restart_across_the_end_of_the_hour(tmp_path, ahead):
    write(tmp_path, [pc(MIN, "M", "A", conn=7), pc(50 * MIN, "M", "A", conn=7)])
    write(tmp_path, [ctl(ahead * HOUR_MS + 5_000, "connect", 1, "A")], hour=later(ahead))
    cov, gaps, _, inferred = curated(tmp_path)
    assert inferred == 1 and gaps == [("A", 50 * MIN)]
    assert cov["rec-a", "M"]["gap_s"] == pytest.approx(600)


@pytest.mark.parametrize("next_hour", [
    None,  # nothing later: no evidence
    [pc(HOUR_MS + 5, "M", "A", conn=7)],  # carries on
    [ctl(HOUR_MS + 5, "connect", 8, "Z"), pc(HOUR_MS + 9, "M", "A", conn=7)],  # another shard reconnected first
    [ctl(HOUR_MS + 5, "disconnect", 7), ctl(HOUR_MS + 9, "connect", 8, "A")],  # an ordinary reconnect
])
def test_no_restart_at_the_end_of_the_hour(tmp_path, next_hour):
    write(tmp_path, [pc(MIN, "M", "A", conn=7), pc(50 * MIN, "M", "A", conn=7)])
    if next_hour:
        write(tmp_path, next_hour, hour=later())
    cov, gaps, _, inferred = curated(tmp_path)
    assert not inferred and not gaps
    assert cov["rec-a", "M"]["gap_s"] == 0


def test_hosts_are_judged_separately(tmp_path):
    write(tmp_path, [pc(MIN, "M", "A", conn=7)], host="rec-a")
    write(tmp_path, [pc(MIN, "M", "A", conn=7)], host="rec-b")
    write(tmp_path, [ctl(HOUR_MS + 5, "connect", 1, "A")], hour=later(), host="rec-a")
    write(tmp_path, [pc(HOUR_MS + 5, "M", "A", conn=7)], hour=later(), host="rec-b")
    cov, _, _, inferred = curated(tmp_path)
    assert inferred == 1
    assert cov["rec-a", "M"]["gap_s"] > 0
    assert cov["rec-b", "M"]["gap_s"] == 0


def test_kalshi_replay_does_not_run_across_a_restart(tmp_path):
    ticker = "KXTEST-1"

    def snap(t, conn, seq, size):
        return {"recv_ms": H0 + t, "conn": conn, "msg": {"type": "orderbook_snapshot", "sid": 1, "seq": seq, "msg": {
            "market_ticker": ticker, "yes_dollars_fp": [["0.4000", size]], "no_dollars_fp": []}}}

    # the book moved from 100 to 250 while nobody was recording
    write_raw(tmp_path, "kalshi", "book_events", [snap(1000, 283, 1, "100.00")])
    write_raw(tmp_path, "kalshi", "book_events",
              [ctl(5 * MIN, "connect", 6, ticker), snap(5 * MIN + 100, 6, 1, "250.00")], part=1)
    cov, gaps, _, inferred = curated(tmp_path, "kalshi")
    assert inferred == 1 and gaps == [(ticker, 1000)]
    assert cov["rec-a", ticker]["gap_s"] == pytest.approx(299)
    r = Root.parse(tmp_path)
    rep = replay.run([curated_path(r, "book_deltas", "kalshi", DT, HOUR)],
                     [curated_path(r, "book_snapshots", "kalshi", DT, HOUR)])
    assert rep["outcomes"]["skipped"] == 1 and rep["outcomes"]["mismatch"] == 0
