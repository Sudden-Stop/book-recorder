# Curated table schemas

One YAML file per curated table. The pipeline builds its Arrow/Parquet schemas from these files, `validate`
fails when a curated file does not match them (schema drift), and `publish` renders the column tables of the
Hugging Face dataset card from them. Change a column here first, then the code.

File layout: `curated/<table>/venue=<venue>/dt=YYYY-MM-DD/part-HH.parquet` (zstd), one file per venue, UTC date
and UTC hour. The `venue` column is also stored inside each file so published files stand alone.

## Types

| Type | Arrow / Parquet | Notes |
|---|---|---|
| `string` | `string` | |
| `bool` | `bool` | |
| `int32`, `int64` | `int32`, `int64` | |
| `float64` | `double` | only for venue-reported statistics (volume), never prices or sizes |
| `price` | `decimal128(10, 6)` | probability price in dollars per share/contract, 0–1 |
| `size` | `decimal128(24, 6)` | shares (Polymarket) or contracts (Kalshi) |
| `timestamp` | `timestamp[ms, tz=UTC]` | UTC milliseconds since the Unix epoch |
| `list<string>` | `list<string>` | |
| `levels` | `list<struct<price: price, size: size>>` | book side, best level first |
| `tokens` | `list<struct<token_id: string, outcome: string>>` | outcome tokens of a market |

`nullable: false` columns are written as Parquet `REQUIRED`.

## Identifiers across venues

| Column | Polymarket | Kalshi |
|---|---|---|
| `market_id` | condition id (`0x…`) | market ticker |
| `token_id` | CLOB asset id of one outcome token | market ticker (the YES contract; NO is its mirror) |
