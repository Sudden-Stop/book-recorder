# book-pipeline

Batch pipeline for the recorder's raw hourly files: **curate** them into typed Parquet tables, **validate**
them (schema drift, book replay against REST snapshots, coverage), and **publish** them to a Hugging Face
dataset. Table schemas live in [`../schema/`](../schema/README.md); the dataset card template is
[`../docs/dataset_card_template.md`](../docs/dataset_card_template.md).

```bash
cd pipeline
uv venv && uv pip install -e ".[test]"
pytest
```

## Commands

```bash
book-pipeline curate   --raw s3://BUCKET --out s3://BUCKET --venue polymarket --dt 2026-11-14 --hour 07
book-pipeline curate   --raw s3://BUCKET --out s3://BUCKET --venue cross --dt 2026-11-14 --hour 07   # pairs
book-pipeline validate --curated s3://BUCKET --venue polymarket --dt 2026-11-14 --strict --min-match-rate 0.99
book-pipeline publish  --curated s3://BUCKET --stage ./stage --wallets hashed            # stage only (dry run)
book-pipeline publish  --curated s3://BUCKET --stage ./stage --wallets hashed --repo-id ORG/NAME   # private push
```

Storage roots hold `raw/…` (written by the recorder), `curated/<table>/venue=<v>/dt=<d>/part-HH.parquet` and
`reports/{curate,validate}/venue=<v>/dt=<d>/hour-HH.json`. A local directory works the same as an S3 prefix.

Environment: `HF_TOKEN` (push), `BOOK_WALLET_SALT` (≥ 16 chars, for `--wallets hashed`), AWS credentials via
the usual chain (S3), optional `BOOK_SCHEMA_DIR` / `BOOK_CARD_TEMPLATE` when the package runs outside the repo
(e.g. in a container: copy `schema/` and `docs/` too), `BOOK_DUCKDB_TEMP` for DuckDB's spill directory.

## Raw input it expects

The recorder's format is described in the repo [README](../README.md#recorder). Lines are
`{"recv_ms","conn","msg":<frame as received>}` or control lines
`{"recv_ms","conn","ctl":"connect|connect_error|subscribe|unsubscribe|disconnect|seq_gap",…}`; records the
recorder builds itself carry `ts_ms`.

Files: `raw/venue=<v>/stream=<s>/dt=YYYY-MM-DD/hour=HH/<host>-pN.ndjson.zst` (S3; `.manifest.json` siblings are
ignored), or the recorder's local data dir `{venue}/{stream}/dt=YYYY-MM-DD/HH.pN.ndjson.zst` (host taken from
`--local-host`). Every part of an hour is read in order and lines are numbered per host across parts. A part cut
short by a crash (no zstd end marker) yields its complete lines; the cut part is listed in the curate report's
`truncated_files`.

| Venue | Stream | Line shape | Table(s) |
|---|---|---|---|
| `polymarket` | `book_events` | market channel: `book`, `price_change`, `last_trade_price`, `tick_size_change` | `book_deltas`, `trades`, `coverage` |
| `polymarket` | `book_snapshots` | `{asset, recv_ms, req_ms, status, msg:<REST /book body>}`; non-200 skipped and counted | `book_snapshots` (`source=rest`) |
| `polymarket` | `activity` | `{"type":"trades","topic":"activity","payload":{…}}` | `trades` (with wallet) |
| `polymarket` | `markets` | `{ts_ms, reason: added/daily/final, market:<Gamma record>}` | `markets` (`source=metadata`) |
| `polymarket` | `universe` | `{ts_ms, pool, selected:[{condition_id, slug, reason, rank, volume24hr, tokens}], in_grace, added, removed}` | `markets` (`source=universe`) |
| `kalshi` | `book_events` | one socket, split by `type`: `orderbook_snapshot`, `orderbook_delta`, `trade` (all markets), `market_lifecycle_v2` (all markets), replies `subscribed`/`ok`/`error` | `book_deltas`, `book_snapshots` (`source=ws`), `trades`, `markets` (`source=lifecycle`), `coverage` |
| `kalshi` | `markets`, `universe` | as Polymarket, with Kalshi market records / tickers | `markets` |
| `kalshi` | `series` | `{ts_ms, series:<GET /series response>}` | `markets` (`source=series`, fee type and multiplier) |
| `cross` | `pairs` | `{ts_ms, rule, confidence, pairs:[{league, date, teams, polymarket:{…}, kalshi:{event_ticker, market_tickers}}]}` | `pairs` (one row per Kalshi market) |

Kalshi specifics (see `src/book_pipeline/venues/kalshi.py`): deltas are relative, so curate first replays the
previous hour's `book_events` to know each book at the start of the hour (`--no-warm` to skip); a `seq_gap` on
the order-book subscription writes `gap` rows and drops those books until the next snapshot, while a gap on the
trade or lifecycle subscription is only counted. Replay between two socket snapshots is bounded by stream
position, not time, so a snapshot is never compared with itself. Kalshi parsing follows the recorder's code and
is **not yet checked against a recorded Kalshi hour** (`unverified_parser: true` in the curate report).

## Memory and speed (measured on this machine, largest sample hour: 134 MB gz, 5.0 M delta rows)

- `curate`: 45 s, peak 276 MB (streams one line at a time, row groups of 200 k rows).
- `validate`: 11 s, peak 815 MB with the default DuckDB cap of 512 MB (`--memory-limit 256MB`: 562 MB).

## Tests

`tests/fixtures/raw/` is one real hour (2026-11-14 07 UTC) of four Polymarket markets from the legacy
TypeScript recorder, converted by `tests/fixtures/make_fixtures.py` (see its docstring for what is real and what
is synthetic). The tests cover the schema checks, curate output, the replay match (and that corruption is
caught), coverage from control lines, validate failures, staging with each wallet mode, a private-by-default
push against a fake Hub client, crashed parts and the local layout, and the Kalshi mapping on synthetic frames
in the recorder's shapes (sequence gaps, warm start, socket-snapshot replay). Nothing touches S3 or the Hub.
