# Ame Stocks

A resume-grade U.S. equity factor research and backtesting platform. The project is built in
explicit, reviewable stages so every transition from immutable vendor data to research features
can be inspected and reproduced.

## Current milestone

The project has passed the **Bronze data checkpoint** for the catalog frozen on 2026-07-12 and is
now at **Silver Phase 2 / S4 Assets `awaiting_review`; S1–S3 remain published**:

- ten years of full-market minute/day aggregate Flat Files and 29 required REST research
  datasets (31 dataset families in total) are stored immutably on the remote data volume;
- the REST catalog now includes one unadjusted full-market Daily Market Summary per session from
  2016-07-13 and an isolated legacy/deprecated combined-financials fallback from 2009-03-29;
- those legacy financial rows are only point-in-time candidates: they are not approved PIT factor
  inputs until Silver applies filing/acceptance-time rules, EDGAR cross-checks, and quarantines;
- every saved file is manifest-bound, checksummed, resumable, and covered by the full Bronze audit;
- the Silver S0 control plane is implemented: frozen schemas/QA rules, source inventories, immutable
  review workflows, approval-bound releases, and a release-only reader are covered by synthetic tests;
- the exact S1 `reference/exchange_dim` schema is approved; its 27-row preview and review-bound
  full build both passed all 20 QA checks with zero quarantine rows;
- S1 release `feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838`
  is published through the immutable release-only reader;
- the exact 17-field S2 `reference/ticker_type_dim` contract
  `b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d` is approved and packaged;
- its bounded manifest-bound preview and review-bound full build both accepted all 24 source rows
  into 24 output rows, with 17 columns, zero duplicate excess, zero quarantine, and all 20 QA checks
  passed; the three first-capture temporal checks are the expected 0/0;
- S2 workflow `40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97` is
  `published` at sequence 9. Full build
  `f02a6ad085e5f78ac15f3d1e26caf75079275204e7b55b58b4bb679bdfab2780` has seven immutable
  outputs, and release `11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6`
  exposes only its one verified DATA Parquet through the release-only reader;
- S3 publishes the version-preserved 29-field `condition_code_dim` and normalized 20-field
  `condition_code_data_type_bridge`: 94 source definitions became 94 Dim rows and 123 Bridge rows,
  with 27/27 and 23/23 QA passed, zero quarantine, and zero approval exceptions;
- S3 releases `9c0eb2eec54428bfa58754fc0b6f58a33b5fd804fe5917253f2a411574ab35b2`
  and `bdb5286b592dae80477cc45025f822c53aab140202f74cf41d2fc39075b86d66`
  are published and release-only verified; exact replay preserved all file SHA/metadata and both
  event chains at sequence 9. S1/S2 and Bronze remain unchanged;
- S4 has completed a read-only full profile of all 5,026 active/inactive Assets manifests,
  72,038 pages, and 69,381,182 rows. It confirmed zero active-flag mismatch and zero same-day
  active/inactive exact-ticker overlap, and refined the 4,853 duplicate groups into 2 exact,
  2,115 last-updated-only, and 2,736 delisted-plus-last-updated groups;
- the three exact S4 contracts are approved and packaged. A manifest-bound 2026-05-11 preview used
  all 37 active/inactive pages and 35,647 source rows, with S1/S2 release manifests registered as
  upstream lineage;
- the manifest-bound Assets reader and session-bounded pure transform now implement lossless daily
  observations, multi-version evidence, fail-closed selection, and the one-row-per-session/ticker
  source universe. The three preview outputs contain 35,647 observations, 82 version rows, and
  35,606 universe rows; all have zero quarantine and zero blocking QA;
- the three S4 workflows are stopped at `awaiting_review` sequence 5. Exact idempotent replay kept
  the same build and event IDs. No full build, full-run approval, published Silver directory, or
  S4 release exists; any later ten-year build requires a separately reviewed immutable
  `FullRunPlan`.

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

Formal Silver control-plane code lives in `backend/ame_stocks_api/silver/`. Its contract is
documented in [docs/silver-s0-contracts.md](docs/silver-s0-contracts.md), while the dataset-by-dataset
sequence and hard approval stops remain in
[docs/silver-processing-plan.md](docs/silver-processing-plan.md). S0 does not read Bronze or run a
transformation. S1 `exchanges`, S2 `ticker_types`, and S3 `condition_codes` are fully published.
S2's approved contract, bounded 24-row preview, review-bound full build, and immutable release
evidence are documented in
[docs/silver-s2-ticker-types-schema-review.md](docs/silver-s2-ticker-types-schema-review.md). The
approved packaged schema is
[ticker_type_dim.schema-v1.json](backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json),
loaded by [ticker_type_contract.py](backend/ame_stocks_api/silver/ticker_type_contract.py); the
manifest-bound reader and pure transform live in
[ticker_type_source.py](backend/ame_stocks_api/silver/ticker_type_source.py) and
[ticker_types.py](backend/ame_stocks_api/silver/ticker_types.py). The bounded runner in
[ticker_type_preview.py](backend/ame_stocks_api/silver/ticker_type_preview.py) processed only the
exact approved 24-row source; after separate explicit authorization, the review-bound runner in
[ticker_type_release.py](backend/ame_stocks_api/silver/ticker_type_release.py) published that exact
reviewed result. S3's paired contracts, source profile, runtime IDs, QA results, and replay evidence
are documented in
[docs/silver-s3-condition-codes-schema-review.md](docs/silver-s3-condition-codes-schema-review.md).
S3 is finished. S4's full source profile, reconstructed-membership caveat, duplicate selection rule,
approved contracts, runtime workflow IDs, and bounded-preview evidence are documented in
[docs/silver-s4-assets-schema-review.md](docs/silver-s4-assets-schema-review.md). The current hard
stop is human review of the three immutable preview builds; no full run or publish is authorized.
The approved schemas are loaded by
[asset_contract.py](backend/ame_stocks_api/silver/asset_contract.py); the manifest-bound reader and
session-bounded pure transform live in
[asset_source.py](backend/ame_stocks_api/silver/asset_source.py) and
[assets.py](backend/ame_stocks_api/silver/assets.py).

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

Read-only S0 inspection commands are available after installation:

```bash
.venv/bin/ame-silver fixed-cases
.venv/bin/ame-silver validate-contract --contract /path/to/contract.json
.venv/bin/ame-silver status --data-root /path/to/data --workflow-id <sha256>
.venv/bin/ame-silver inspect-release --data-root /path/to/data --release-id <sha256>
```

Inspect any Parquet file without truncating columns. The viewer prints complete rows in five-row
pages by default:

```bash
.venv/bin/python -m ame_stocks_api.cli.parquet_view /path/to/file.parquet --page 1 --schema
.venv/bin/python -m ame_stocks_api.cli.parquet_view /path/to/file.parquet --page 2
```

There is intentionally no S0 CLI command that downloads Bronze, runs a dataset transform, approves
a review, selects a “latest” build, or exposes unpublished data.

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
