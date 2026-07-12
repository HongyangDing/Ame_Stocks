# Massive Advanced hybrid downloader review guide

Live downloads are explicit, resumable, and stored only under the configured data root.
No real credential is stored in Git, manifests, or logs.

## Confirmed Advanced capabilities

The Stocks Advanced access used for this archive includes the `us_stocks_sip/minute_aggs_v1` and
`us_stocks_sip/day_aggs_v1` Flat File datasets. Minute aggregate files are:

- one gzip CSV object per U.S. trading day;
- end-of-day, normally finalized around 11:00 AM ET the following day;
- downloaded for the project's ten-year window through 2026-07-09;
- unadjusted for splits, dividends, and other corporate actions;
- market-activity files with OHLCV, ticker, timestamp, and transaction count.

The completed newer five-year Bronze minute archive occupies about 25.6 GB compressed.
The older five-year extension is measured while downloading rather than estimated from a
different schema. REST and Flat File writes both refuse to reduce free space below the
40 GiB safety floor; Silver conversion remains a separate, reviewed operation.

For the catalog frozen and entitlement-probed on 2026-07-12, the completed required scope contains
29 REST datasets plus the two aggregate Flat File datasets. REST Daily Market Summary covers
2016-07-13 through 2026-07-09; the isolated legacy/deprecated combined-financials endpoint that is
currently accessible to the live key covers `filing_date` from 2009-03-29 through 2026-07-09. Its
rows are only PIT candidates, not safe PIT inputs. The three new v1 statement endpoints and
current-ratios endpoint still return HTTP 403 for the live key, so they remain optional
replacement/cross-check contracts rather than unreported holes in this date-bounded Bronze scope.

The final strict inventory audit is
`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v9.json`
(SHA-256 `a23fdd2aa4c613274dfe0dcca611e8ed1bd62153146f787ecd415c345c1a15d6`). It passes the
authoritative-plan and physical-integrity gates for 58,771 manifests and 238,814 files. The
report-level `failed` status comes only from the separate semantic-consistency gate and must not
be read as a download failure or local file corruption. REST semantic details are frozen in
`manifests/audits/rest_semantics/full-2026-07-12-v7.json` (SHA-256
`95366ec4abcdc9903b0c1aea972e2cf9f14da008f931bdfc3111523addfae301`).

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
4. One minute aggregate Flat File supplies that session's full-market activity file.
5. Coverage QA joins activity to the security master and reports:
   - active tickers without bars;
   - inactive tickers with bars;
   - bar tickers missing from reference data.

Research selects `active_on_date=true` using the signal date, never today's status and
never the set of tickers that happens to have bars. The daily reference table is the
left side of the join, so an active but halted ticker remains visible as a missing-data
case instead of silently disappearing. A company that later delists therefore remains
eligible on dates when it was active, avoiding current-constituent survivorship bias.

For example, if `XYZ` is active through January 20 and inactive from January 21, a
January 10 backtest universe may include it even if it is inactive today. It is excluded
from new selections from January 21 onward. Rebuilding January from today's active list
would erase `XYZ` before the backtest starts and bias the result upward.

The backtest will apply the two sources in this order:

1. On signal date `t`, start with the REST security master and filter
   `active_on_date=true`, supported security types, exchanges, and rules known by `t`.
2. Left-join unadjusted Flat File bars and derived features. Insufficient history, IPOs,
   halts, and missing minutes remain explicit eligibility or QA outcomes.
3. Freeze the orders at the close of `t`. At `t+1` execution, an order without the
   required 09:30–10:00 price remains unfilled/cash; the engine must not use that new
   information to drop the ticker and rerank the remaining names.
4. Treat an existing position that delists as a separate return-accounting problem.
   The inactive flag identifies a status transition but does not provide the economic
   delisting payoff. Corporate actions and ticker events will supply the preferred exit;
   a documented conservative fallback is required when that value is unavailable.

Dropping missing positions, forward-filling them forever, or interpreting
`active=false` as a zero return would each create a different bias. Those behaviors are
therefore prohibited by design and will receive small hand-calculated tests in the
backtest step.

`active=false` is retained every day as requested, even though it repeats much of the
historical delisted list. `--active history` remains available as a cheaper diagnostic
mode, but it is not the project default.

## API and S3 responsibilities

| Source | Responsibility | Stored form |
| --- | --- | --- |
| REST All Tickers | Daily active and inactive security master | gzip JSON Bronze plus daily Parquet |
| Minute Flat Files | Full-market unadjusted minute activity | immutable daily gzip CSV Bronze |
| Day Flat Files | Full-market unadjusted daily activity | immutable daily gzip CSV Bronze |
| REST Daily Market Summary | Independent full-market daily OHLCV and full-session VWAP QA from 2016-07-13 | one immutable gzip JSON response per session |
| REST splits/dividends | Later adjustment inputs | gzip JSON Bronze |
| REST legacy combined financials | Isolated legacy/deprecated statement PIT candidates from 2009-03-29; not safe PIT until Silver timing QA | annual `filing_date` gzip JSON streams |
| REST Custom Bars | Small validation samples and targeted exact provider VWAP for an explicitly requested interval | gzip JSON Bronze |

Daily Market Summary requests are fixed to `adjusted=false` and `include_otc=false`; they are an
unadjusted exchange-listed comparison product, not an adjusted-return series or an OTC universe.

The old per-ticker minute REST downloader remains available only for spot checks. It is
no longer the full-market backfill path.

Paid REST plans have unlimited request counts. The default client pace is therefore 600
requests per minute (10 per second), still well below Massive's published recommendation
to remain under 100 requests per second; 429 responses continue to back off and retry.

## Independent daily-product reconciliation

The two daily products are deliberately audited as independent sources rather than assuming one
is a copy of the other. The completed schema-v4 report is:

```text
/mnt/HC_Volume_106309665/american_stocks/manifests/audits/daily_product_crosscheck/full-2026-07-12-v4.json
SHA-256 f0588ca0b1ac54dcd2d4883c010725cafe723d0931977200f5c8b0486d34c7fe
```

It covers 2,511 sessions from 2016-07-13 through 2026-07-09, with 24,452,482 common
ticker rows, 8,356 REST-only rows, and 64 Flat-only rows. Price differences are small
relative to the common population (open 0.002883%, high 0.005995%, low 0.064910%, close
0.019564%); volume and transaction-count differences are product-level and much more common
(35.347099% and 35.535515%). These differences are retained for Silver policy rather than
silently selecting whichever value is convenient.

Time fields have different contracts: REST `t` is checked against the exchange close used by the
provider's nominal 16:00 ET daily-window end on every session, including exchange half days whose
actual close is 13:00 ET, while
Flat `window_start` is checked against midnight in `America/New_York`. The sole source-integrity
failure is 29 noncanonical Flat timestamps on 2019-08-12, which makes the report status `failed`.
A separate provider re-download has the same SHA-256, so this is a vendor object anomaly rather
than local disk damage.

## VWAP limitation

Minute aggregate Flat Files do not contain a VWAP column. OHLCV alone cannot
reconstruct exact trade-level 09:30–10:00 VWAP. The backtest must therefore later choose
one explicitly named method and must not treat them as equivalent:

- exact provider VWAP: retrieve a targeted 09:30–10:00 Custom Bars response and use its `vw`;
- non-exact alternative: use a clearly labelled volume-weighted minute-close proxy and record that
  the execution-price contract has been relaxed.

The current converter does not invent a `vwap` field, so the platform cannot silently
present an approximation as an exact VWAP. REST Daily Market Summary does contain `vw` for
24,317,162 of 24,460,838 downloaded rows, but that value describes the full trading session;
it is useful for provider QA and daily features, not as the next-day 09:30–10:00 execution price.

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
│   ├── daily_bars/request_id={sha256}/page-00000.json.gz
│   ├── legacy_financials/request_id={sha256}/page-{sequence}.json.gz
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

All three CLIs use the same date rule: `--end` is required, while omitting `--start`
defaults to the inclusive ten-calendar-year window ending on that date. Use an explicit
`--start` for a one-day or short pilot. `--years N` can request another lookback, but it
cannot be combined with `--start`.

```bash
# 1. Daily point-in-time reference plan: active and inactive every session.
.venv/bin/ame-massive plan \
  --dataset assets \
  --active both \
  --end 2026-06-30

# 2. Independent grouped daily bars: one unadjusted full-market request per session.
.venv/bin/ame-massive plan \
  --dataset daily_bars \
  --start 2016-07-13 \
  --end 2026-07-09

# 3. Isolated currently accessible legacy/deprecated financial PIT candidates:
#    chronological filing-date years; Silver timing QA is still required.
.venv/bin/ame-massive plan \
  --dataset legacy_financials \
  --start 2009-03-29 \
  --end 2026-07-09

# 4. After approved REST download, build each daily security master.
.venv/bin/ame-materialize universe \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 5. Offline Flat File plan: one daily object, no S3 credentials read.
.venv/bin/ame-flatfiles plan \
  --dataset minute_aggregates \
  --end 2026-06-30

# 6. Live S3 download only after review and server-side credential setup.
.venv/bin/ame-flatfiles download \
  --dataset minute_aggregates \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 7. After inspecting Bronze CSV files, explicitly convert them to daily Parquet.
.venv/bin/ame-flatfiles convert \
  --dataset minute_aggregates \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# 8. Reconcile Flat File activity with point-in-time reference status.
.venv/bin/ame-flatfiles coverage \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

For a one-day checkpoint, pass the same date to both `--start` and `--end`; the default
does not override an explicit start. High-volume research endpoints are divided into
calendar-year requests so they can resume independently and begin with the oldest year.

## Official references

- [Stocks Flat Files overview](https://massive.com/docs/flat-files/stocks/overview)
- [Stocks Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates)
- [Flat Files Quickstart](https://massive.com/docs/flat-files/quickstart)
- [REST All Tickers](https://massive.com/docs/rest/stocks/tickers/all-tickers)
- [REST Stocks overview](https://massive.com/docs/rest/stocks)
- [REST Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary)
- [Massive handling of delisted tickers](https://massive.com/knowledge-base/article/what-does-massive-do-with-delisted-tickers)
- [REST Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
