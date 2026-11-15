# {{pretty_name}}

Order book and trade data from prediction-market venues ({{venues}}), recorded continuously by
[book-recorder](https://github.com/Sudden-Stop/book-recorder) and curated into one schema across venues.

- Period covered: {{date_range}} (UTC)
- Generated: {{generated_at}}
- Wallet policy for this release: {{wallet_policy}}

> Status: private research release. Licence and venue data terms are not settled; do not redistribute.

## Configurations

Each table is its own configuration (`load_dataset(repo, "<config>")`). Files are Parquet (zstd), one per venue,
UTC date and UTC hour: `data/<table>/<venue>/<YYYY-MM-DD>/part-HH.parquet`.

{{summary_table}}

## How the data was recorded

- Market-data WebSockets are recorded frame by frame with the recorder's receive time (`recv_ts`, chrony-synced
  clock) next to the venue's own timestamp (`exchange_ts`). Nothing is dropped or merged at record time.
- Full books are also fetched from each venue's REST API on a slow rotation (`book_snapshots`), independently of
  the WebSocket. Replaying `book_deltas` between two snapshots must reproduce the second one; the measured match
  rate is below.
- Connection drops are explicit: a `gap` row in `book_deltas`, and per market per hour in `coverage`. Estimates
  should exclude or flag hours with `gap_s > 0`.
- Prices are decimals in dollars per share (probability units, 0 to 1); sizes are decimals; all times are UTC
  milliseconds.

## Quality checks

Replay of `book_deltas` against `book_snapshots`:

{{replay}}

Coverage:

{{coverage}}

## Tables

{{tables}}

## Column types

{{schema_types}}

## Known limitations

- Polymarket's activity feed reports one side of each fill with its wallet; whether that wallet was the taker
  is not established (`trades.side` for `source = activity`).
- The same trade can appear twice in `trades` (once per source); there is no cross-source deduplication.
- Kalshi parsing follows the recorder's code and is marked unverified until a real recorded hour has been
  checked. Kalshi `book_snapshots` come from the same socket as `book_deltas` (`source = ws`), so the Kalshi
  replay check tests sequence integrity, not agreement with an independent source.
- `coverage` infers the connection state at the start of an hour when the connection opened in an earlier
  hour (`start_assumed`).
