# Massive downloader review guide

The code is review-only at this checkpoint. None of the commands below have been run
against Massive, and no API key is stored in the repository.

## Universe semantics

Massive's All Tickers endpoint supports both a point-in-time `date` and an `active`
filter. The project uses them as follows:

- `active=true` on every XNYS trading session is the point-in-time tradable universe.
- `active=false` on the final session is the historical security-master supplement. It
  captures delisted/former symbols without downloading the mostly unchanged inactive
  list every day.
- The generated minute-download union contains every ticker seen in a daily active
  snapshot plus any final-snapshot ticker whose `delisted_utc` falls inside the window.
- `--active both` remains available for a literal active/inactive snapshot on every
  session, but it is intentionally not the default because it costs many redundant
  free-tier requests.

Bronze discovery does not filter the Massive ticker `type`; later research layers can
select common shares (`CS`) without losing ADRs, ETFs, preferred shares, old symbols, or
other listed instruments prematurely. The current scope is U.S. listed stocks and does
not include Massive's separate OTC market.

## API strategy

| Dataset | Endpoint | Request shape |
| --- | --- | --- |
| Point-in-time tickers | `GET /v3/reference/tickers` | Daily active streams plus one final inactive stream, `limit=1000` |
| Daily bars | `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` | One full-market request per XNYS session, unadjusted, OTC excluded |
| Minute bars | `GET /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}` | One full-window stream per historical ticker, `limit=50000`, unadjusted |
| Splits | `GET /stocks/v1/splits` | One full-market date-range stream, `limit=5000` |
| Dividends | `GET /stocks/v1/dividends` | One full-market date-range stream, `limit=5000` |

The grouped daily endpoint is materially cheaper than requesting daily bars ticker by
ticker: approximately one base request per trading session instead of one stream per
ticker. Minute bars do not have an equivalent free full-market endpoint, so each ticker
uses the widest two-year request range and follows pagination rather than being split
into daily requests.

`exchange-calendars` supplies the XNYS session list, including scheduled holidays and
half days. API planning reports a lower bound because ticker and minute responses may
paginate.

## Safety properties

- `plan` never reads an API key or opens a network client.
- `download` reads only the `MASSIVE_API_KEY` environment variable.
- Authentication uses `Authorization: Bearer`; the key is never placed in a query
  string, manifest, or log.
- One provider instance enforces a global default of five requests per minute.
- HTTP 429, 408, 425, and selected 5xx responses retry with `Retry-After` or capped
  exponential backoff.
- `next_url` must remain on the configured HTTPS origin and must not contain `apiKey`.
- Successful JSON pages are gzip-compressed deterministically and written atomically.
- Every page stores raw and compressed SHA-256 checksums.
- A continuation checkpoint is committed after each page; interrupted jobs resume
  without replaying completed pages.
- Completed request manifests are verified and skipped on rerun.
- Existing Bronze and materialized files are never replaced when checksums differ.

## Storage layout

Raw responses remain request-shaped and immutable. Only the explicit offline
materialization commands reorganize them by date.

```text
DATA_ROOT/
├── bronze/massive/{dataset}/request_id={sha256}/page-00000.json.gz
├── manifests/massive/{dataset}/{sha256}.json
├── staging/universe/window={start}_{end}/
│   ├── snapshots/date=YYYY-MM-DD/status=active/tickers.parquet
│   ├── snapshots/date=YYYY-MM-DD/status=inactive/tickers.parquet
│   ├── historical_tickers.parquet
│   └── historical_tickers.txt
├── staging/minute_unadjusted/
│   └── by_ticker/ticker=AAPL/request_id={sha256}/bars.parquet
└── silver_unadjusted/minute/
    └── date=YYYY-MM-DD/bars.parquet
```

The intermediate layer intentionally uses one two-year Parquet per ticker instead of
millions of tiny `date × ticker` files. It is still directly reviewable and retains the
New York `session_date` column. The explicit compaction pass streams those ticker files
into daily partitions and then writes exactly one final file per session.

The final daily minute file is long format, not thousands of timestamp columns. One row
is one `ticker × minute` observation:

```text
session_date, timestamp_utc, ticker, open, high, low, close,
volume, vwap, transactions, otc
```

`session_date` is derived in `America/New_York`, so an after-hours bar whose UTC date is
the next day remains in the correct U.S. trading-date partition. Rows are sorted by
`timestamp_utc, ticker`. Duplicate `(ticker, timestamp_utc)` bars are counted in the
manifest and preserved at this rough aggregation stage; cleaning is a later reviewed
step.

Parquet is interoperable with both Polars and pandas:

```python
import pandas as pd

bars = pd.read_parquet(
    "silver_unadjusted/minute/date=2026-06-30/bars.parquet",
)
```

## Review workflow

All `plan` and `ame-materialize` commands are offline. Only `download` contacts Massive.

```bash
# 1. Review the exact point-in-time universe request plan.
.venv/bin/ame-massive plan \
  --dataset assets \
  --start 2024-07-01 \
  --end 2026-06-30

# 2. After an approved assets download, build snapshot Parquet and the ticker union.
.venv/bin/ame-materialize universe \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 3. Review the cheap full-market daily plan.
.venv/bin/ame-massive plan \
  --dataset daily_bars \
  --start 2024-07-01 \
  --end 2026-06-30

# 4. Review minute requests using the generated historical union.
.venv/bin/ame-massive plan \
  --dataset minute_bars \
  --ticker-file /mnt/HC_Volume_106309665/american_stocks/staging/universe/window=2024-07-01_2026-06-30/historical_tickers.txt \
  --start 2024-07-01 \
  --end 2026-06-30

# 5. After an approved minute download, parse each ticker stream into review Parquet.
.venv/bin/ame-materialize partition-minute \
  --ticker-file /path/to/reviewed-tickers.txt \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 6. Only after reviewing all partitions, explicitly create one file per day.
.venv/bin/ame-materialize compact-minute \
  --ticker-file /path/to/reviewed-tickers.txt \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

The first approved live run must still be one ticker and one completed trading day.
The 50-stock pilot and full-market history remain separate later checkpoints.

## Official references

- [REST authentication quickstart](https://massive.com/docs/rest/quickstart)
- [All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers)
- [Ticker Types](https://massive.com/docs/rest/stocks/tickers/ticker-types)
- [Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary)
- [Stocks Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
- [Splits](https://massive.com/docs/rest/stocks/corporate-actions/splits)
- [Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends)
- [REST request limits](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
