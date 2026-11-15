# book-recorder

Records order book data from **Polymarket** and **Kalshi**, ships it hourly to S3, and (in `pipeline/`)
turns it into a typed Parquet dataset for Hugging Face. The first use of the data is testing the
assumptions of Feil & Nendel, *Optimal Market Making in Prediction Markets* (arXiv 2607.17991), on
real markets.

```
recorder/   Rust binary: sockets + REST -> hourly zstd NDJSON segments -> S3
pipeline/   Python: raw segments -> Parquet tables, validation, Hugging Face publishing
schema/     one YAML per published table
infra/      Terraform for the instance, bucket, IAM and alarms
deploy/     Dockerfile and docker-compose
docs/       setup guide and dataset card template
```

## Recorder

One process, one static binary. Every inbound frame is written as received, with the local receive
time and a connection id, before anything else happens to it:

```
{"recv_ms":1794643200201,"conn":4,"msg":<frame exactly as the venue sent it>}
{"recv_ms":1794643200950,"conn":4,"ctl":"disconnect","code":1006,"reason":"…","frames":1234}
```

Control lines (`connect`, `connect_error`, `subscribe`, `unsubscribe`, `disconnect`, `seq_gap`)
bracket every gap, so coverage can be rebuilt offline. Segments are
`{data_dir}/{venue}/{stream}/dt=YYYY-MM-DD/HH.pN.ndjson.zst` (UTC); a restart opens a new part `pN`.
A segment cut short by a crash has no zstd end marker: readers must tolerate a truncated last frame.

| venue | stream | content |
|---|---|---|
| `polymarket` | `book_events` | CLOB market channel: `book`, `price_change`, `last_trade_price`, `tick_size_change`. Tokens are split over several connections (`conn` tells them apart). |
| `polymarket` | `book_snapshots` | REST `/book` of every tracked token, one per token per `BR_POLYMARKET_SNAPSHOT_PERIOD_MS`: `{recv_ms, req_ms, asset, status, msg}`. Independent reference for the replay check. |
| `polymarket` | `activity` | Real-time data socket `activity/trades`: every trade on the venue with wallet and transaction hash. |
| `polymarket` | `universe` | Each hourly selection: pool size, selected markets with reason and rank, added / removed / in grace. |
| `polymarket` | `markets` | Full Gamma record of a tracked market when added, once a day, and a `final` record when it leaves (fees, rewards, tick, end date, outcome). |
| `kalshi` | `book_events` | One signed connection: `orderbook_snapshot` / `orderbook_delta` for tracked markets, **all** public `trade`s, **all** `market_lifecycle_v2` events, plus command replies. Fresh snapshots are requested every `BR_KALSHI_SNAPSHOT_PERIOD_MS`. |
| `kalshi` | `universe`, `markets` | As for Polymarket. |
| `kalshi` | `series` | Series record of each tracked market (`fee_type`, `fee_multiplier`), refetched daily. |
| `cross` | `pairs` | Events listed on both venues, matched by league + date + both team codes. |

### Which markets

Refreshed hourly, the same rule on both venues (see `recorder/src/select.rs`):

1. **paired**: markets whose event also trades on the other venue;
2. **top volume**: the top N by 24 h volume;
3. **fee-free** (Polymarket): markets with fees disabled above a volume floor;
4. **sample**: a stratified sample of the rest, by price bucket × time to resolution, ordered by a
   hash of (UTC date, market id), so it is reproducible and not biased towards busy markets.

A market that drops out stays recorded for `BR_UNIVERSE_GRACE_MS`.

### Configuration

| variable | default | |
|---|---|---|
| `BR_DATA_DIR` | `./data` | local segment directory |
| `BR_HOST_ID` | `rec-a` | recorder name used in S3 keys |
| `BR_S3_BUCKET`, `BR_S3_REGION`, `BR_S3_PREFIX` | none, none, `raw` | without a bucket, segments stay on disk |
| `BR_LOCAL_RETAIN_HOURS` | `24` | uploaded segments are deleted locally after this |
| `BR_HEALTH_PORT` | `8080` | `/healthz` returns 503 when a stream goes silent |
| `BR_UNIVERSE_REFRESH_MS` / `BR_UNIVERSE_GRACE_MS` | 1 h / 6 h | |
| `BR_POLYMARKET_ENABLED`, `BR_POLYMARKET_ACTIVITY_ENABLED` | `true` | |
| `BR_POLYMARKET_TOP_N` / `_FEE_FREE_MAX` / `_SAMPLE_N` / `_PAIRED_MAX` | 100 / 120 / 80 / 100 | about 300 markets |
| `BR_POLYMARKET_SHARD_SIZE` | `200` | tokens per connection |
| `BR_POLYMARKET_SNAPSHOT_PERIOD_MS` | `600000` | |
| `BR_KALSHI_KEY_ID`, `BR_KALSHI_PRIVATE_KEY` or `BR_KALSHI_PRIVATE_KEY_FILE` | none | RSA key (PEM); Kalshi is enabled when both are set |
| `BR_KALSHI_TOP_N` / `_SAMPLE_N` / `_PAIRED_MAX` | 120 / 80 / 100 | about 300 markets |
| `BR_KALSHI_MAX_PAGES` | `300` | pages of 1,000 open markets scanned per refresh |
| `BR_KALSHI_SNAPSHOT_PERIOD_MS` | `600000` | |
| `BR_LOG_LEVEL` | `info` | |

S3 credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` when set, otherwise from the
EC2 instance role. In a container the instance metadata hop limit must be 2.

S3 layout: `raw/venue=<v>/stream=<s>/dt=YYYY-MM-DD/hour=HH/<host>-pN.ndjson.zst` with a sibling
`<host>-pN.manifest.json` (bytes, sha256, lines, first/last receive time).

### Build and test

```bash
cd recorder && cargo test
```

```bash
cd recorder && cargo run --release
```

## Status and known limits

- Not yet run against the live venues: unit tests and a local WebSocket test only.
- Polymarket's per-connection subscription limit is not documented; `BR_POLYMARKET_SHARD_SIZE` is a guess.
- Kalshi sequence gaps are recorded as `seq_gap` control lines, not repaired on the spot; the periodic
  snapshot request bounds the damage.
- Kalshi Ed25519 keys are not supported, only RSA.
- Cross-venue matching covers head-to-head games in NBA, NFL, NHL, MLB and US college football and
  basketball. A team whose code differs between venues is left unmatched.
