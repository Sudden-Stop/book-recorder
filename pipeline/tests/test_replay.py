from collections import Counter

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from book_pipeline import replay
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root

from conftest import DT, HOUR

S = replay.SCALE


def _paths(root_dir):
    root = Root.parse(root_dir)
    return (curated_path(root, "book_deltas", "polymarket", DT, HOUR),
            curated_path(root, "book_snapshots", "polymarket", DT, HOUR))


def test_fixture_replay_matches_snapshots(curated):
    d, s = _paths(curated)
    rep = replay.run([d], [s])
    assert rep["tokens"] == 8
    assert rep["evaluated"] >= 400
    assert rep["outcomes"]["mismatch"] == 0 and rep["outcomes"]["skipped"] == 0
    assert rep["match_rate"] == 1.0
    assert rep["duplicate_snapshots"] > 0  # the legacy snapshots repeat unchanged books


def test_corrupted_deltas_are_caught(curated_copy):
    d, s = _paths(curated_copy)
    t = pq.read_table(d)
    victim = t.filter(pc.is_valid(t["best_bid"]))["token_id"][0].as_py()
    is_victim_change = pc.and_(pc.equal(t["token_id"], victim), pc.is_valid(t["best_bid"]))
    bumped = pc.if_else(is_victim_change, pc.add(t["size"], pa.scalar(1, pa.decimal128(24, 6))), t["size"])
    t = t.set_column(t.column_names.index("size"), t.schema.field("size"), bumped.cast(pa.decimal128(24, 6)))
    pq.write_table(t, d)
    rep = replay.run([d], [s])
    bad = [r for r in rep["per_token"] if r["token_id"] == victim][0]
    assert bad.get("mismatch", 0) + bad.get("top", 0) > 0
    assert bad["examples"] or bad.get("top", 0)
    assert rep["match_rate"] < 1.0


def _book(bids, asks):
    return ({int(p * S): int(z * S) for p, z in bids}, {int(p * S): int(z * S) for p, z in asks})


def _set(t, side, p, z):
    return (t, "set", side, int(p * S), int(z * S), None)


def _run(snaps, events, window_ms=2000):
    res = replay.TokenResult("h", "tok", "m")
    replay.replay_token([(t, *_book(b, a)) for t, b, a in snaps], events, window_ms, res)
    return res.outcomes


def test_unit_exact_and_mismatch():
    s0 = (1000, [(0.40, 10)], [(0.60, 10)])
    s1 = (5000, [(0.40, 10), (0.41, 5)], [(0.60, 10)])
    assert _run([s0, s1], [_set(2000, "bid", 0.41, 5)]) == Counter(exact=1)
    assert _run([s0, s1], [_set(2000, "bid", 0.41, 6)]) == Counter(mismatch=1)
    # top of book agrees, deeper level does not
    s1b = (5000, [(0.41, 5), (0.30, 1)], [(0.60, 10)])
    assert _run([s0, s1b], [_set(2000, "bid", 0.41, 5)]) == Counter(top=1)


def test_unit_window_tolerates_late_delta():
    s0 = (1000, [(0.40, 10)], [(0.60, 10)])
    s1 = (5000, [(0.40, 10), (0.41, 5)], [(0.60, 10)])
    # the change is stamped 500 ms after the snapshot time: not exact at the boundary, but within the window
    out = _run([s0, s1], [_set(2000, "ask", 0.60, 10), _set(5500, "bid", 0.41, 5)])
    assert out == Counter(window=1)
    assert _run([s0, s1], [_set(8000, "bid", 0.41, 5)], window_ms=1000) == Counter(mismatch=1)


def test_unit_gap_then_reset():
    s0 = (1000, [(0.40, 10)], [(0.60, 10)])
    s1 = (5000, [(0.45, 1)], [(0.55, 1)])
    gap = (2000, "gap", None, None, None, None)
    assert _run([s0, s1], [gap]) == Counter(skipped=1)
    reset = (3000, "reset", None, None, None, _book([(0.45, 1)], [(0.55, 1)]))
    assert _run([s0, s1], [gap, reset]) == Counter(exact=1)
