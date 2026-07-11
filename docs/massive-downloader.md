# Massive Starter hybrid downloader review guide

This checkpoint is code-only. No REST or S3 market-data request has been executed, and
no real credential is stored in Git.

## Confirmed Starter capabilities

Stocks Starter includes the `us_stocks_sip/minute_aggs_v1` and
`us_stocks_sip/day_aggs_v1` Flat File datasets. Minute aggregate files are:

- one gzip CSV object per U.S. trading day;
- end-of-day, normally finalized around 11:00 AM ET the following day;
- available for five years on Starter;
- unadjusted for splits, dividends, and other corporate actions;
- market-activity files with OHLCV, ticker, timestamp, and transaction count.

The official 2025 minute archive is about 5.4 GB compressed and 2024 is about 4.7 GB.
A rolling two-year Bronze minute archive should therefore be roughly 10–11 GB before
measuring the current files. This is much safer for the 186 GB volume than downloading
per-ticker REST pages.

## Flat Files do not solve survivorship by themselves

The Flat File schema is:

```text
ticker, volume, open, close, high, low, window_start, transactions
```

It contains neither `active` nor `inactive`, and it only represents securities with an
eligible aggregate row. Consequently:

- a ticker present in a Flat File had qualifying market activity;
- a ticker absent from a Flat File might be suspended, illiquid, newly listed, or
  otherwise have no eligible bar;
- absence must never be interpreted as delisting;
- Flat Files cannot serve as the point-in-time security master.

Massive explicitly states that its market data includes companies delisted from the
exchanges and preserves observations as they occurred on each historical date. Thus a
later-delisted company should retain its historical bars on dates when it traded. The
Flat File still does not label those rows inactive and is not a complete status table,
so the pipeline never infers listing status from Flat File membership.

## Survivorship-safe hybrid design

For every XNYS session:

1. REST `GET /v3/reference/tickers?date=...&active=true` downloads all active tickers.
2. REST `GET /v3/reference/tickers?date=...&active=false` downloads all inactive/
   delisted tickers.
3. Both paginated results are combined into one daily security master with
   `active_on_date` explicitly recorded.
4. One minute aggregate Flat File supplies the complete daily activity table.
5. Coverage QA joins activity to the security master and reports:
   - active tickers without bars;
   - inactive tickers with bars;
   - bar tickers missing from reference data.

Research selects `active_on_date=true` using the historical date, never the latest
status. A company that later delists therefore remains eligible on dates when it was
active, eliminating the usual current-constituent survivorship bias.

`active=false` is retained every day as requested, even though it repeats much of the
historical delisted list. `--active history` remains available as a cheaper diagnostic
mode, but it is not the project default.

## API and S3 responsibilities

| Source | Responsibility | Stored form |
| --- | --- | --- |
| REST All Tickers | Daily active and inactive security master | gzip JSON Bronze plus daily Parquet |
| Minute Flat Files | Full-market unadjusted minute activity | immutable daily gzip CSV Bronze |
| Day Flat Files | Full-market unadjusted daily activity | immutable daily gzip CSV Bronze |
| REST splits/dividends | Later adjustment inputs | gzip JSON Bronze |
| REST Custom Bars | Small validation samples and future execution-price supplement | gzip JSON Bronze |

The old per-ticker minute REST downloader remains available only for spot checks. It is
no longer the full-market backfill path.

Paid REST plans have unlimited request counts. The default client pace is therefore 600
requests per minute (10 per second), still well below Massive's published recommendation
to remain under 100 requests per second; 429 responses continue to back off and retry.

## VWAP limitation

Starter minute aggregate Flat Files do not contain a VWAP column. OHLCV alone cannot
reconstruct exact trade-level 09:30–10:00 VWAP. The backtest must therefore later choose
one explicitly named method:

- preferred: retrieve a targeted 30-minute aggregate VWAP through REST for execution;
- alternative: use a clearly labelled volume-weighted minute-close proxy.

The current converter does not invent a `vwap` field, so the platform cannot silently
present an approximation as an exact VWAP.

## Credentials

REST and Flat Files use different credentials:

```text
MASSIVE_API_KEY                 # REST
MASSIVE_S3_ACCESS_KEY_ID        # Dashboard Flat Files access key
MASSIVE_S3_SECRET_ACCESS_KEY    # Dashboard Flat Files secret key
```

S3 uses endpoint `https://files.massive.com` and bucket `flatfiles`. Credentials are
read only by the explicit download command and never placed in object keys, manifests,
logs, or Git.

## Storage layout

```text
DATA_ROOT/
├── bronze/massive/
│   ├── assets/request_id={sha256}/page-00000.json.gz
│   └── flatfiles/us_stocks_sip/
│       ├── minute_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
│       └── day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
├── silver_unadjusted/
│   ├── universe/date=YYYY-MM-DD/tickers.parquet
│   ├── minute/date=YYYY-MM-DD/bars.parquet
│   ├── daily/date=YYYY-MM-DD/bars.parquet
│   └── coverage/date=YYYY-MM-DD/ticker_coverage.parquet
├── manifests/
└── tmp/massive_flatfiles/
```

Downloads resume through S3 byte ranges, verify advertised size, fully decompress the
gzip stream to validate CRC and CSV headers, calculate SHA-256, and atomically publish
Bronze. Completed files are checksummed and skipped. A download is rejected if it would
leave less than the configured 40 GiB disk-safety floor; conversion applies the same
floor with a conservative temporary-space estimate.

## Review workflow

All `plan`, `convert`, `coverage`, and `ame-materialize` commands are offline. Only an
explicit `download` action contacts Massive.

```bash
# 1. Daily point-in-time reference plan: active and inactive every session.
.venv/bin/ame-massive plan \
  --dataset assets \
  --active both \
  --start 2024-07-01 \
  --end 2026-06-30

# 2. After approved REST download, build each daily security master.
.venv/bin/ame-materialize universe \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 3. Offline Flat File plan: one daily object, no S3 credentials read.
.venv/bin/ame-flatfiles plan \
  --dataset minute_aggregates \
  --start 2024-07-01 \
  --end 2026-06-30

# 4. Live S3 download only after review and server-side credential setup.
.venv/bin/ame-flatfiles download \
  --dataset minute_aggregates \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 5. After inspecting Bronze CSV files, explicitly convert them to daily Parquet.
.venv/bin/ame-flatfiles convert \
  --dataset minute_aggregates \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 6. Reconcile Flat File activity with point-in-time reference status.
.venv/bin/ame-flatfiles coverage \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

The first live checkpoint remains one trading day. Its coverage report will provide the
first account-specific evidence about inactive tickers appearing in the selected Flat
File before any two-year backfill is authorized.

## Official references

- [Stocks Flat Files overview](https://massive.com/docs/flat-files/stocks/overview)
- [Stocks Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)
- [Flat Files Quickstart](https://massive.com/docs/flat-files/quickstart)
- [REST All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers)
- [REST Stocks overview](https://massive.com/docs/rest/stocks)
- [Massive handling of delisted tickers](https://massive.com/knowledge-base/article/what-does-massive-do-with-delisted-tickers)
- [REST Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
