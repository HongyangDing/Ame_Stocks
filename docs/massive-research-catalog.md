# Massive non-trade research data catalog

This catalog defines the datasets worth preserving for daily U.S. equity factor research.
It is intentionally narrower than "every endpoint": data that is reconstructible from our
immutable bars, only describes the live market, leaks future information, or requires a
different paid expansion is not bulk-downloaded.

Account access was probed again on 2026-07-12 with one-record requests. The probe printed only
HTTP status and response size; credentials and response bodies were not logged.

The frozen catalog was independently audited on 2026-07-12. All 29 REST datasets required by this
date-bounded, exchange-listed daily-factor/Barra scope and both aggregate Flat File datasets are
present in the authoritative plan. The common research window is 2016-07-11 through 2026-07-09,
except REST Daily Market Summary begins at its observed entitlement boundary of 2016-07-13 and
the isolated legacy/deprecated financial fallback currently accessible to the live key begins on
2009-03-29. This is a completeness claim for that frozen scope and those observed entitlements,
not for every Massive product or a later catalog date. The detailed file-integrity evidence,
provider differences, and Barra-readiness boundary are recorded in
[the bounded Bronze audit](bronze-audit-2026-07-12.md).

The daily universe plan explicitly uses `locale=us, market=stocks`. Massive exposes OTC as a
separate `market=otc` universe, so the audited coverage claim is exchange-listed U.S. stocks,
including inactive listings, not OTC securities. OTC is not required for the current Barra-style
research universe; adding it would require a separate active/inactive snapshot plan and audit.

## Download catalog

| Dataset | Earliest useful request | Storage/partition | Research use |
| --- | --- | --- | --- |
| Minute aggregates | rolling ten-year cutoff | one immutable gzip CSV per session | explicitly non-exact intraday execution proxy and daily features |
| Day aggregates | rolling ten-year cutoff | one immutable gzip CSV per session | fast daily QA and bar cross-check |
| REST Daily Market Summary | 2016-07-13 observed entitlement boundary | one immutable gzip JSON response per session | independent daily OHLCV QA and provider full-session VWAP |
| Active + inactive tickers | every session | paginated gzip JSON | point-in-time universe and survivorship control |
| Splits + dividends | 2003-09-10 | one resumable stream per dataset | unadjusted-to-adjusted return construction |
| Short interest | 2017-12-29 | calendar-year gzip JSON streams | positioning and short-squeeze factors |
| Short volume | 2024-02-06 | calendar-year gzip JSON streams | daily short-sale activity factors |
| Free float | capture date only | one full-market snapshot | liquidity/position-size research; never treated as history |
| IPOs | 2008-01-01 | calendar-year gzip JSON streams | listing age, issuance, and IPO cohort effects |
| Ticker events | 2003-09-10 | one request per FIGI/CUSIP/ticker | symbol continuity and entity identity QA |
| Ticker Overview | one request per ticker/identity lifecycle | gzip JSON plus allowlisted Parquet | SIC, listing date, and identity reference inputs |
| Legacy combined financials | 2009-03-29 | annual `filing_date` gzip JSON streams, isolated from v1 | income, balance-sheet, cash-flow, and provenance PIT candidates; not safe PIT until Silver timing QA |
| Exchanges + ticker types | capture date only | two small snapshots | decode reference classifications |
| Condition codes | capture date only | one small snapshot | explain trade/quote eligibility and provider OHLCV update rules |
| EDGAR index | 2016-07-11 project window | calendar-year gzip JSON streams | authoritative filing availability timestamp |
| Forms 3 and 4 | 2016-07-11 project window | calendar-year gzip JSON streams | insider ownership and transaction factors |
| Form 13-F | 2016-07-11 project window | calendar-quarter gzip JSON streams | institutional holdings and crowding factors |
| 10-K sections | 2016-07-11 project window | calendar-year gzip JSON streams | business/risk text factors |
| 8-K text | 2016-07-11 project window | calendar-year gzip JSON streams | event-driven disclosure factors |
| 8-K disclosures | requested from 2016-07-11; provider returns from 2022-01-03 | calendar-year gzip JSON streams | standardized event-disclosure factors |
| 8-K disclosure taxonomy | capture date only | one small snapshot | decode standardized disclosure categories |
| Risk factors | 2016-07-11 project window | calendar-year gzip JSON streams | standardized disclosure-risk features |
| News | 2016-06-22 | calendar-year gzip JSON streams | point-in-time sentiment and attention features |
| Treasury yields | 1962-01-02 | one gzip JSON stream | yield-curve and rate-regime controls |
| Inflation | 1947-01-01 | one gzip JSON stream | realized inflation regime controls |
| Inflation expectations | 1982-01-01 | one gzip JSON stream | market/model inflation-risk controls |
| Labor market | 1948-01-01 | one gzip JSON stream | employment-cycle controls |
| Risk taxonomy | capture date only | one small snapshot | decode standardized SEC risk categories |

Every date-ranged REST request uses the dataset's disclosure or event date, not report-period
date. Bronze stores the provider response unchanged; later Silver jobs must deduplicate by the
provider record key and enforce publication-time lags.

Form 13-F contains a small provider-visible header-only variant: 152 HR/HR-A rows have complete
filing metadata and no holding payload. Silver must retain them as
`holdings_status=not_public_or_unavailable`, never infer zero holdings, and exclude them from the
holding fact table. Partial holding payloads remain audit failures.

## Daily-product reconciliation

The independent schema-v4 report is stored at
`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/daily_product_crosscheck/full-2026-07-12-v4.json`
(SHA-256 `f0588ca0b1ac54dcd2d4883c010725cafe723d0931977200f5c8b0486d34c7fe`). Across
2,511 sessions it compares 24,452,482 common ticker rows, plus 8,356 REST-only and 64
Flat-only rows. REST timestamps are checked against the exchange close used by the provider's
nominal daily-window end at 16:00 ET on every session, including exchange half days whose actual
close is 13:00 ET. Flat
timestamps are session starts at midnight ET. The report's only
source-integrity failure is 29 Flat rows on 2019-08-12 with noncanonical timestamps, reproduced
byte-for-byte by a provider re-download. That single anomaly makes the report status `failed`;
numerical and coverage differences are kept as explicit product differences for Silver policy,
not classified as local file corruption.

The REST `vw` field is available on 24,317,162 of 24,460,838 Daily Market Summary rows and is
the full-session VWAP. It does not satisfy the backtest contract for exact next-day
09:30–10:00 VWAP. Exact provider VWAP for that interval requires targeted Custom Bars; a
minute-price construction is only a clearly labelled non-exact proxy and requires an explicit
relaxation of the execution-price contract.

## New-v1 entitlement gap and isolated fallback

Massive's current official plan table says Stocks Advanced includes end-of-day access and all
history back to 2009-03-29 for the three statement endpoints. The live remote key nevertheless
returns HTTP 403 for all four new v1 endpoints below. This is an observed documentation-versus-live
access mismatch whose exact cause must be confirmed by Massive. No failed response body or
credential is persisted.

The legacy/deprecated `/vX/reference/financials` endpoint, currently accessible to the live key,
was therefore downloaded as a strictly isolated fallback: 377,576 rows in 3,784 pages across 18
chronological annual requests from 2009-03-29 through 2026-07-09. It supplies combined income,
balance-sheet, cash-flow, source-XPath, and derivation provenance. These are PIT candidates, not
safe PIT observations merely because they have a `filing_date`: 299,200 rows lack
`acceptance_datetime`, and 39 rows have `end_date > filing_date`. Silver must preserve lineage,
apply conservative availability timing, cross-check EDGAR, and quarantine impossible date order
before factor use. It must never present this fallback as a response from a new v1 contract.

| Dataset | Intended storage | Research use | Current action |
| --- | --- | --- | --- |
| Income statements v1 | annual `filing_date` chunks from 2009-03-29 | earnings yield, growth, profitability, weighted-share proxy | optional replacement/cross-check after entitlement is restored; legacy fallback is present |
| Balance sheets v1 | annual `filing_date` chunks from 2009-03-29 | book-to-price, leverage, balance-sheet quality | optional replacement/cross-check after entitlement is restored; legacy fallback is present |
| Cash-flow statements v1 | annual `filing_date` chunks from 2009-03-29 | cash-flow yield, accruals, quality | optional replacement/cross-check after entitlement is restored; legacy fallback is present |
| Ratios v1 | one current full-market snapshot | current cross-section QA only; not historical Barra | optional QA snapshot after entitlement is restored; historical ratios are derived locally |

Historical ratios must be recomputed point-in-time from statements and prices; the provider Ratios
endpoint explicitly has no history. Even after statements are available, weighted-average shares
are only a labelled proxy for exact period-end shares, and the safe Ticker Overview table covers
SIC for only 16,682 / 30,739 identity lifecycles. Full classic Barra therefore still requires an
explicit market-cap proxy policy and either a point-in-time industry source or a documented
coverage restriction. Price/volume styles do not depend on those blocked inputs.

As of the 2026-07-12 live-key probe, there is no additional accessible, non-oversized Massive
endpoint missing from the frozen exchange-listed daily-factor/Barra Bronze scope. The remaining
gaps are the explicitly oversized trades/quotes,
reconstructible indicators, current-only or non-point-in-time-safe products, the four HTTP-403 v1
contracts covered above, and research-policy/source questions such as exact point-in-time shares
and industry history. Those Barra limitations cannot be fixed by silently bulk-downloading another
small endpoint.

## Explicit exclusions

| Dataset | Reason not queued |
| --- | --- |
| Trades | User-excluded and roughly multi-terabyte at ten-year scale |
| Quotes | User-excluded as oversized and unnecessary for daily-factor/Barra research |
| Per-ticker aggregate bars | Duplicates full-market Flat Files; retained only for tiny validation samples |
| SMA/EMA/MACD/RSI | Deterministically reconstructed from stored bars |
| Live snapshots, movers, last trade/quote | Not historical research inputs |
| Related tickers | Current proprietary relationship graph is not point-in-time safe |
| Market status/upcoming holidays | Operational, forward-looking data; exchange calendar is versioned locally |
| OTC active/inactive universe | Outside the current exchange-listed Barra universe; add only as a separately versioned universe expansion |
| Benzinga partner feeds | Separate paid expansion, not part of the current account |

## Safety and execution rules

- Full-market range endpoints are split into chronological calendar-year requests; 13-F uses
  quarters because its 1,000-row page limit makes yearly pagination unnecessarily serial.
- Each successful page is gzip-compressed, checksummed, atomically written, and checkpointed.
- A rerun skips complete manifests and resumes incomplete pagination from the committed cursor.
- Ticker Overview is queried once per deduplicated ticker/identity lifecycle. Bronze retains the
  full response, while the stage-one Parquet allowlists identity, SIC, and listing-date fields.
  Market cap and all shares-outstanding fields remain Bronze-only.
- Stage-one Ticker Overview consumers must require `identity_match=true`; rows without a
  comparable CIK/FIGI remain visible for QA but are not approved inputs.
- REST and S3 tasks refuse writes that would leave less than 40 GiB free.
- Large text/news/ownership datasets begin with the oldest annual chunk; measured size and
  record count determine whether the remaining years start.
- No process writes outside `/mnt/HC_Volume_106309665/american_stocks` or touches Mogikabu.

## Examples

```bash
# Full-market yearly chunks, defaulting to a ten-year window.
.venv/bin/ame-massive plan --dataset news --end 2026-07-09

# Latest-only datasets require an explicit capture date.
.venv/bin/ame-massive plan \
  --dataset float \
  --start 2026-07-09 \
  --end 2026-07-09

# Condition codes are a small current dictionary, not a historical series.
.venv/bin/ame-massive plan \
  --dataset condition_codes \
  --start 2026-07-09 \
  --end 2026-07-09

# Ticker events accept exact-case tickers, CUSIPs, or Composite FIGIs.
.venv/bin/ame-massive plan \
  --dataset ticker_events \
  --ticker-file .runtime/ticker-event-identifiers.txt \
  --start 2003-09-10 \
  --end 2026-07-09

# Generate one Overview request per deduplicated ticker/identity lifecycle.
.venv/bin/ame-materialize ticker-overview-lifecycles \
  --start 2016-07-11 \
  --end 2026-07-09 \
  --data-root /mnt/HC_Volume_106309665/american_stocks

# Build the allowlisted identity/SIC/list-date table after the Bronze requests finish.
.venv/bin/ame-materialize ticker-overview-safe \
  --start 2016-07-11 \
  --end 2026-07-09 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

Large identifier lists may include symbols for which the experimental ticker-events endpoint
returns HTTP 404. Run that dataset with `--continue-on-error`: each missing identifier keeps a
retryable failed manifest while independent identifiers finish. Other datasets remain fail-fast.

Official endpoint documentation:

- [Stocks REST overview](https://massive.com/docs/rest/stocks)
- [Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary)
- [Short interest](https://massive.com/docs/rest/stocks/fundamentals/short-interest)
- [Short volume](https://massive.com/docs/rest/stocks/fundamentals/short-volume)
- [Float](https://massive.com/docs/rest/stocks/fundamentals/float)
- [IPOs](https://massive.com/docs/rest/stocks/corporate-actions)
- [Ticker events](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events)
- [Condition codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes/)
- [Income statements](https://massive.com/docs/rest/stocks/fundamentals/income-statements)
- [Balance sheets](https://massive.com/docs/rest/stocks/fundamentals/balance-sheets)
- [Cash-flow statements](https://massive.com/docs/rest/stocks/fundamentals/cash-flow-statements)
- [Ratios](https://massive.com/docs/rest/stocks/fundamentals/ratios)
- [SEC EDGAR index](https://massive.com/docs/rest/stocks/filings/index)
- [Form 4](https://massive.com/docs/rest/stocks/filings/form-4)
- [13-F](https://massive.com/docs/rest/stocks/filings/13-f-filings)
- [10-K sections](https://massive.com/docs/rest/stocks/filings/10-k-sections)
- [8-K text](https://massive.com/docs/rest/stocks/filings/8-k-text)
- [8-K disclosures](https://massive.com/docs/rest/stocks/filings/8-k-disclosures)
- [Disclosure categories](https://massive.com/docs/rest/stocks/filings/disclosure-categories)
- [Risk factors](https://massive.com/docs/rest/stocks/filings/risk-factors)
- [News](https://massive.com/docs/rest/stocks/news)
- [Economy overview](https://massive.com/docs/rest/economy/overview)
