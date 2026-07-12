# Ame Stocks

A resume-grade U.S. equity factor research and backtesting platform. The project is built in
explicit, reviewable stages so every transition from immutable vendor data to research features
can be inspected and reproduced.

## Current milestone

The project is at the **Bronze data checkpoint** for the catalog frozen on 2026-07-12:

- ten years of full-market minute/day aggregate Flat Files and 29 required REST research
  datasets (31 dataset families in total) are stored immutably on the remote data volume;
- the REST catalog now includes one unadjusted full-market Daily Market Summary per session from
  2016-07-13 and an isolated legacy/deprecated combined-financials fallback from 2009-03-29;
- those legacy financial rows are only point-in-time candidates: they are not approved PIT factor
  inputs until Silver applies filing/acceptance-time rules, EDGAR cross-checks, and quarantines;
- every saved file is manifest-bound, checksummed, resumable, and covered by the full Bronze audit;
- full-universe Silver normalization, adjustment, and Gold factor/backtest outputs have not started.

The final strict full audit is
`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/bronze/full-2026-07-12-v9.json`
(SHA-256 `a23fdd2aa4c613274dfe0dcca611e8ed1bd62153146f787ecd415c345c1a15d6`). It verified
238,814 files and 230,783,074 REST records with `authoritative_plan=passed` and
`physical_integrity=passed`. Its overall status is `failed` only because known provider-content
findings intentionally keep `semantic_consistency=failed`; no hash, gzip/parse, byte-count, or
record-count damage was found.

Two public Python contracts remain stable across these stages:

- `DataProvider`: an asynchronous, resumable source adapter that returns immutable raw payload batches.
- `FactorSpec`: a Git-managed Polars factor plugin that emits `signal_date`, `asset_id`, and `raw_value`.

`MockProvider` remains deterministic for contract tests. The Massive downloader contacts external
services only through explicit download commands; audit and materialization commands are offline.

## Repository layout

```text
backend/                 FastAPI service
frontend/                Next.js application
infra/                   local and remote deployment notes/configuration
packages/ame_stocks_core shared provider and factor contracts
research/factors/        Git-managed factor plugins
tests/                   Python contract and service tests
worker/                  Celery worker service
```

## Python setup

Python 3.12 or 3.13 is supported.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/ruff check .
```

Run the API:

```bash
.venv/bin/uvicorn ame_stocks_api.main:app --reload --host 127.0.0.1 --port 8000
```

The service exposes:

- `GET /healthz`
- `GET /api/v1/contracts`

## Frontend setup

Node.js 20.9 or newer is required.

```bash
cd frontend
npm ci
npm run dev
```

The frontend runs at `http://127.0.0.1:3000` and checks the API at `http://127.0.0.1:8000` by default.

## Worker skeleton

The Celery application can be imported without a broker. Running a worker will require Redis, which is added to the local Compose stack in a later step.

```bash
.venv/bin/celery -A ame_stocks_worker.celery_app:celery_app worker --loglevel=INFO
```

## Configuration and secrets

Copy `.env.example` only when local overrides are needed. Planning requires no
credentials. REST downloads use an untracked `MASSIVE_API_KEY`; Flat Files use the
separate S3 access key and secret supplied in the Massive dashboard.

The active remote data root is:

```text
/mnt/HC_Volume_106309665/american_stocks
```

Runtime data and credentials remain outside Git. Deployment and progress synchronization do not
touch Caddy, domains, or the legacy Mogikabu application.

## Automatic progress synchronization

This checkout uses a versioned `post-commit` hook. After every focused commit on `main`, it:

1. refuses to proceed if task files remain uncommitted;
2. pushes `main` to `git@github.com:HongyangDing/Ame_Stocks.git`;
3. runs `git pull --ff-only origin main` in `/opt/american_stocks` on the remote server;
4. verifies that local, GitHub, and remote commit IDs are identical.

Enable the hook once per clone:

```bash
scripts/install_hooks.sh
```

If a network failure interrupts synchronization, the commit remains local. Fix the connection
and rerun `scripts/sync_progress.sh`; the script never force-pushes or resets either checkout.

## Massive Advanced hybrid downloader

Full-market minute and day backfills use Advanced Flat Files. This offline plan reads no
credentials and contacts no network service:

```bash
.venv/bin/ame-flatfiles plan \
  --dataset minute_aggregates \
  --end 2026-06-30
```

When `--start` is omitted, every market-data CLI derives a ten-calendar-year window
from `--end` (the example starts on 2016-06-30). An explicit `--start` remains available
for the required one-day and short pilot reviews.

The live S3 command is intentionally distinct and requires an explicit storage root:

```bash
.venv/bin/ame-flatfiles download \
  --dataset minute_aggregates \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

Daily survivorship-safe exchange-listed membership is downloaded separately through REST using
both `active=true` and `active=false` with `locale=us, market=stocks`; OTC is a separate optional
universe. Flat Files retain historical activity from companies
that later delist, but their rows are never treated as listing status. See
[docs/massive-downloader.md](docs/massive-downloader.md) for the evidence, storage
layout, credential separation, and resume behavior. The versioned
[research-data catalog](docs/massive-research-catalog.md) records which non-trade datasets
are queued, excluded as reconstructible, unavailable on the account, or latest-only.
The Chinese [data dictionary](DATA_README.md) inventories every downloaded dataset and
documents its observed field structure, candidate keys, timing semantics, and backtest risks.
The bounded [2026-07-12 Bronze audit](docs/bronze-audit-2026-07-12.md) records the full-file
hash/gzip/row verification, authoritative-plan reconciliation, market cross-check, semantic
differences, and the exact remaining blockers for a classic Barra implementation.

The independent schema-v4 daily-product audit is stored remotely at
`/mnt/HC_Volume_106309665/american_stocks/manifests/audits/daily_product_crosscheck/full-2026-07-12-v4.json`
(SHA-256 `f0588ca0b1ac54dcd2d4883c010725cafe723d0931977200f5c8b0486d34c7fe`). It
compares REST Daily Market Summary with Day Flat Files across 2,511 sessions and 24,452,482
common ticker rows. REST `t` is checked against the exchange close used by the provider's daily
window end at 16:00 ET on every session, including exchange half days whose actual close is
13:00 ET. Flat
`window_start` is checked against midnight ET. The report status is `failed` solely because 29
Flat rows on 2019-08-12 have noncanonical timestamps; a separate provider re-download reproduced
the same bytes, so this is not local disk corruption. REST `vw` is the full-session VWAP, not the
required next-day 09:30–10:00 execution VWAP. Exact provider VWAP for that interval requires a
targeted REST Custom Bars request; any price derived from minute OHLCV must be labelled a non-exact
proxy.

After reviewed downloads, `ame-materialize universe` builds one active/inactive security
master per date. `ame-materialize ticker-overview-lifecycles` creates one historical detail
request per deduplicated ticker/identity lifecycle, and `ame-materialize ticker-overview-safe`
builds the allowlisted identity/SIC/list-date table while keeping market-cap and share-count
fields in Bronze only. `ame-flatfiles convert` preserves each daily unadjusted CSV as Parquet,
and `ame-flatfiles coverage` reconciles bars with reference status. Materialization commands
are offline and never read credentials.
