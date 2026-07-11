# Massive downloader review guide

This downloader is optimized for the Stocks Basic free tier and keeps credentials out of URLs, manifests, logs, and Git.

## API strategy

| Dataset | Endpoint | Request shape |
| --- | --- | --- |
| Point-in-time tickers | `GET /v3/reference/tickers` | One active and one inactive stream, `limit=1000` |
| Daily bars | `GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}` | One two-year stream per ticker, `limit=50000`, unadjusted |
| Minute bars | `GET /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}` | One two-year stream per ticker, `limit=50000`, unadjusted |
| Splits | `GET /stocks/v1/splits` | One full-market date-range stream, `limit=5000` |
| Dividends | `GET /stocks/v1/dividends` | One full-market date-range stream, `limit=5000` |

The adapter follows `next_url` rather than splitting bars into daily or monthly requests. This minimizes calls under the free-tier limit. Daily bars should be downloaded before minute bars so factor research can start quickly.

## Safety properties

- `plan` never reads an API key or opens a network client.
- `download` reads only the `MASSIVE_API_KEY` environment variable.
- Authentication uses `Authorization: Bearer`; the key is never placed in a query string.
- One provider instance enforces a global default of five requests per minute.
- HTTP 429, 408, 425, and selected 5xx responses retry with `Retry-After` or capped exponential backoff.
- `next_url` must remain on the configured HTTPS origin and must not contain `apiKey`.
- Successful JSON pages are gzip-compressed with deterministic output and written atomically.
- Every page stores raw and compressed SHA-256 checksums.
- A continuation checkpoint is committed after each page; interrupted jobs resume without replaying completed pages.
- Completed request manifests are verified and skipped on rerun.
- Existing Bronze files are never overwritten when their checksum differs.

## Storage layout

```text
DATA_ROOT/
├── bronze/massive/{dataset}/request_id={sha256}/page-00000.json.gz
└── manifests/massive/{dataset}/{sha256}.json
```

Manifests contain request dates, ticker, adjustment flag, adapter versions, checksums, row counts, page continuation, and lifecycle status. They never contain HTTP headers or credentials.

## Review commands

These are offline-only:

```bash
.venv/bin/pytest tests/test_massive_provider.py tests/test_bronze_downloader.py
.venv/bin/ame-massive plan --dataset assets --start 2026-06-30 --end 2026-06-30
.venv/bin/ame-massive plan --dataset daily_bars --ticker AAPL --start 2024-07-01 --end 2026-06-30
.venv/bin/ame-massive plan --dataset minute_bars --ticker AAPL --start 2024-07-01 --end 2026-06-30
.venv/bin/ame-massive plan --dataset splits --start 2024-07-01 --end 2026-06-30
.venv/bin/ame-massive plan --dataset dividends --start 2024-07-01 --end 2026-06-30
```

The first approved live run should remain one ticker and one completed trading day before expanding to the teaching sample or 50-stock pilot.

## Official references

- [REST authentication quickstart](https://massive.com/docs/rest/quickstart)
- [Stocks Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
- [All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers)
- [Splits](https://massive.com/docs/rest/stocks/corporate-actions/splits)
- [Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends)
- [REST request limits](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
