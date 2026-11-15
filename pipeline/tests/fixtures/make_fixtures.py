"""Build the test fixtures from the legacy TypeScript recorder's output (run by hand, not by pytest).

    python pipeline/tests/fixtures/make_fixtures.py <data-ingestion>/notebooks/output/book-recorder

Source hour: dt=2026-11-14/07 (the smallest hour where book-events, snapshots, live-activity and universe all
exist). Output: `raw/venue=polymarket/stream=<s>/dt=2026-11-14/hour=07/legacy-p0.ndjson.zst`, the layout the
Rust recorder uploads.

What is real and what is converted:
- `book_events`: real market-channel frames, filtered to the four markets below (book lists and
  price_change entries for other tokens dropped, control lines kept with their asset lists trimmed).
- `book_snapshots`: the legacy recorder dumped its own in-memory book every 30 s; those dumps are rewritten
  into the CLOB REST `GET /book` shape (`market`, `asset_id`, `timestamp`, `hash`, `bids`/`asks` as
  `{price, size}` strings) wrapped as the recorder writes them: `{"asset","recv_ms","req_ms","status","msg":…}`. They are derived from the same WebSocket
  feed, so the replay test checks the replay logic, not REST-vs-WebSocket agreement.
- `activity`: real global trade feed lines for the four markets (the `name`, `pseudonym`, `bio`,
  `profileImage` fields are blanked; wallets are kept because the publish wallet switch is tested on them).
- `universe`: rewritten into the Rust recorder's shape (`selected` list).
- `markets` and `pairs`: synthetic (no such stream existed), in the Rust recorder's line shapes, built from the
  universe records; `pairs` goes under `venue=cross`.
"""
import gzip
import json
import sys
from pathlib import Path

import zstandard

DT, HOUR = "2026-11-14", "07"
MARKETS = {
    "0x44e6c9f1f4bf438e8ab4abdb17619f76d916455098fe1767d256e011b46bf26a",
    "0xbd5d3f2363478597b2a52257a8d8560986a608fee5b17aa42ac34812ba18fe8a",
    "0xb9aee520bf51ac5f00b5c43e54ef9245bc9fcdef866ce8a33025f9a441cc651d",
    "0x723822eb2b143cee54c0bd7c1efba322b21f0051984c266df8879c394f1011c0",
}
OUT = Path(__file__).parent / "raw"


def read(src: Path, stream: str):
    with gzip.open(src / stream / f"dt={DT}" / f"{HOUR}.ndjson.gz", "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def write(stream: str, rows: list[dict], venue: str = "polymarket") -> None:
    d = OUT / f"venue={venue}" / f"stream={stream}" / f"dt={DT}" / f"hour={HOUR}"
    d.mkdir(parents=True, exist_ok=True)
    data = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode()
    (d / "legacy-p0.ndjson.zst").write_bytes(zstandard.ZstdCompressor(level=19).compress(data))
    print(stream, len(rows), "lines")


def main(src: Path) -> None:
    universe = [u for u in read(src, "universe")]
    tokens: dict[str, str] = {}
    for u in universe:
        for m in u["top"]:
            if m["conditionId"] in MARKETS:
                for t in m["tokens"]:
                    tokens[t["tokenId"]] = m["conditionId"]

    events = []
    for r in read(src, "book-events"):
        if "ctl" in r:
            if "assets" in r:
                r["assets"] = [a for a in r["assets"] if a in tokens]
            events.append(r)
            continue
        msgs = r["msg"] if isinstance(r["msg"], list) else [r["msg"]]
        kept = []
        for m in msgs:
            if m.get("event_type") == "price_change":
                pcs = [c for c in m["price_changes"] if c["asset_id"] in tokens]
                if pcs:
                    kept.append({**m, "price_changes": pcs})
            elif m.get("asset_id") in tokens:
                kept.append(m)
        if kept:
            events.append({**r, "msg": kept if isinstance(r["msg"], list) else kept[0]})
    write("book_events", events)

    snaps = []
    for s in read(src, "snapshots"):
        if s["asset"] not in tokens:
            continue
        snaps.append({"asset": s["asset"], "recv_ms": s["ts_ms"], "req_ms": s["ts_ms"] - 120, "status": 200, "msg": {
            "market": s["condition_id"],
            "asset_id": s["asset"],
            "timestamp": str(s["exchange_ms"]),
            "hash": s["hash"],
            "bids": [{"price": repr(p), "size": repr(z)} for p, z in sorted(s["bids"])],
            "asks": [{"price": repr(p), "size": repr(z)} for p, z in sorted(s["asks"], reverse=True)],
            "min_order_size": "5",
            "tick_size": s["tick_size"],
            "neg_risk": s["neg_risk"],
        }})
    write("book_snapshots", snaps)

    act = []
    for r in read(src, "live-activity"):
        if "ctl" in r:
            act.append(r)
            continue
        p = r["msg"].get("payload") or {}
        if p.get("conditionId") in MARKETS:
            for k in ("name", "pseudonym", "bio", "profileImage", "icon"):
                if k in p:
                    p[k] = ""
            act.append(r)
    write("activity", act)

    uni = []
    for u in universe:
        top = [m for m in u["top"] if m["conditionId"] in MARKETS]
        uni.append({"ts_ms": u["ts_ms"], "pool": len(u["top"]), "selected": [
            {"condition_id": m["conditionId"], "slug": m["slug"], "reason": "top", "rank": m["rank"],
             "volume24hr": m["volume24hr"],
             "tokens": [{"token_id": t["tokenId"], "outcome": t["outcome"]} for t in m["tokens"]]} for m in top],
            "in_grace": [], "added": [], "removed": []})
    write("universe", uni)

    ts0 = universe[0]["ts_ms"]
    top = [m for m in universe[0]["top"] if m["conditionId"] in MARKETS]
    write("markets", [{"ts_ms": ts0 + 1000, "reason": "added", "market": {
        "conditionId": m["conditionId"],
        "slug": m["slug"],
        "question": m["question"],
        "clobTokenIds": json.dumps([t["tokenId"] for t in m["tokens"]]),
        "outcomes": json.dumps([t["outcome"] for t in m["tokens"]]),
        "endDate": "2026-11-15T00:00:00Z",
        "closed": False,
        "volume24hr": m["volume24hr"],
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": False,
        "feesEnabled": False,
        "events": [{"slug": m["slug"]}],
        "tags": [{"label": "Synthetic"}],
    }} for m in top])

    a = top[0]
    write("pairs", [{"ts_ms": ts0 + 2000, "rule": "league+date+team_codes", "confidence": "exact",
                     "pairs": [{"league": "synth", "date": DT, "teams": ["aaa", "bbb"],
                                "polymarket": {"condition_id": a["conditionId"], "slug": a["slug"],
                                               "tokens": [{"token_id": t["tokenId"], "outcome": t["outcome"]}
                                                          for t in a["tokens"]]},
                                "kalshi": {"event_ticker": "SYNTH-26NOV14",
                                           "market_tickers": ["SYNTH-26NOV14-AAA", "SYNTH-26NOV14-BBB"]}}]}],
          venue="cross")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
