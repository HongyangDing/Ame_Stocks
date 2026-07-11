# Massive non-trade research data catalog

This catalog defines the datasets worth preserving for daily U.S. equity factor research.
It is intentionally narrower than "every endpoint": data that is reconstructible from our
immutable bars, only describes the live market, leaks future information, or requires a
different paid expansion is not bulk-downloaded.

Account access was probed on 2026-07-11 with one-record requests. The probe printed only
HTTP status and response size; credentials and response bodies were not logged.

## Download catalog

| Dataset | Earliest useful request | Storage/partition | Research use |
| --- | --- | --- | --- |
| Minute aggregates | rolling ten-year cutoff | one immutable gzip CSV per session | intraday execution proxy and daily features |
| Day aggregates | rolling ten-year cutoff | one immutable gzip CSV per session | fast daily QA and bar cross-check |
| Active + inactive tickers | every session | paginated gzip JSON | point-in-time universe and survivorship control |
| Splits + dividends | 2003-09-10 | one resumable stream per dataset | unadjusted-to-adjusted return construction |
| Short interest | 2017-12-29 | calendar-year gzip JSON streams | positioning and short-squeeze factors |
| Short volume | 2024-02-06 | calendar-year gzip JSON streams | daily short-sale activity factors |
| Free float | capture date only | one full-market snapshot | liquidity/position-size research; never treated as history |
| IPOs | 2008-01-01 | calendar-year gzip JSON streams | listing age, issuance, and IPO cohort effects |
| Ticker events | 2003-09-10 | one request per FIGI/CUSIP/ticker | symbol continuity and entity identity QA |
| Exchanges + ticker types | capture date only | two small snapshots | decode reference classifications |
| EDGAR index | 2016-07-11 project window | calendar-year gzip JSON streams | authoritative filing availability timestamp |
| Forms 3 and 4 | 2016-07-11 project window | calendar-year gzip JSON streams | insider ownership and transaction factors |
| Form 13-F | 2016-07-11 project window | calendar-quarter gzip JSON streams | institutional holdings and crowding factors |
| 10-K sections | 2016-07-11 project window | calendar-year gzip JSON streams | business/risk text factors |
| 8-K text | 2016-07-11 project window | calendar-year gzip JSON streams | event-driven disclosure factors |
| Risk-factor taxonomy | 2016-07-11 project window | calendar-year gzip JSON streams | standardized disclosure-risk features |
| News | 2016-06-22 | calendar-year gzip JSON streams | point-in-time sentiment and attention features |
| Treasury yields | 1962-01-02 | one gzip JSON stream | yield-curve and rate-regime controls |
| Inflation | 1947-01-01 | one gzip JSON stream | realized inflation regime controls |
| Inflation expectations | 1982-01-01 | one gzip JSON stream | market/model inflation-risk controls |
| Labor market | 1948-01-01 | one gzip JSON stream | employment-cycle controls |
| Risk taxonomy | capture date only | one small snapshot | decode standardized SEC risk categories |

Every date-ranged REST request uses the dataset's disclosure or event date, not report-period
date. Bronze stores the provider response unchanged; later Silver jobs must deduplicate by the
provider record key and enforce publication-time lags.

## Explicit exclusions

| Dataset | Reason not queued |
| --- | --- |
| Trades | User-excluded and roughly multi-terabyte at ten-year scale |
| Quotes | Not included in Stocks Developer and materially larger than aggregates |
| Financial statements + ratios | Live account probe returned HTTP 403; requires Stocks Advanced or the Financials expansion |
| Per-ticker aggregate bars | Duplicates full-market Flat Files; retained only for tiny validation samples |
| SMA/EMA/MACD/RSI | Deterministically reconstructed from stored bars |
| Live snapshots, movers, last trade/quote | Not historical research inputs |
| Related tickers | Current proprietary relationship graph is not point-in-time safe |
| Market status/upcoming holidays | Operational, forward-looking data; exchange calendar is versioned locally |
| Benzinga partner feeds | Separate paid expansion, not part of the current account |

## Safety and execution rules

- Full-market range endpoints are split into chronological calendar-year requests; 13-F uses
  quarters because its 1,000-row page limit makes yearly pagination unnecessarily serial.
- Each successful page is gzip-compressed, checksummed, atomically written, and checkpointed.
- A rerun skips complete manifests and resumes incomplete pagination from the committed cursor.
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

# Ticker events accept exact-case tickers, CUSIPs, or Composite FIGIs.
.venv/bin/ame-massive plan \
  --dataset ticker_events \
  --ticker-file .runtime/ticker-event-identifiers.txt \
  --start 2003-09-10 \
  --end 2026-07-09
```

Large identifier lists may include symbols for which the experimental ticker-events endpoint
returns HTTP 404. Run that dataset with `--continue-on-error`: each missing identifier keeps a
retryable failed manifest while independent identifiers finish. Other datasets remain fail-fast.

Official endpoint documentation:

- [Stocks REST overview](https://massive.com/docs/rest/stocks)
- [Short interest](https://massive.com/docs/rest/stocks/fundamentals/short-interest)
- [Short volume](https://massive.com/docs/rest/stocks/fundamentals/short-volume)
- [Float](https://massive.com/docs/rest/stocks/fundamentals/float)
- [IPOs](https://massive.com/docs/rest/stocks/corporate-actions)
- [Ticker events](https://massive.com/docs/rest/stocks/corporate-actions/ticker-events)
- [SEC EDGAR index](https://massive.com/docs/rest/stocks/filings/index)
- [Form 4](https://massive.com/docs/rest/stocks/filings/form-4)
- [13-F](https://massive.com/docs/rest/stocks/filings/13-f-filings)
- [10-K sections](https://massive.com/docs/rest/stocks/filings/10-k-sections)
- [8-K text](https://massive.com/docs/rest/stocks/filings/8-k-text)
- [Risk factors](https://massive.com/docs/rest/stocks/filings/risk-factors)
- [News](https://massive.com/docs/rest/stocks/news)
- [Economy overview](https://massive.com/docs/rest/economy/overview)
