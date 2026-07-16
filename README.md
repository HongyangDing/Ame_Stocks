# Ame Stocks

A resume-grade U.S. equity factor research and backtesting platform. The project is built in
explicit, reviewable stages so every transition from immutable vendor data to research features
can be inspected and reproduced.

## Current milestone

The project has passed the **Bronze data checkpoint** for the catalog frozen on 2026-07-12 and is
now at **Silver Phase 2 with S1–S6 published; S7 production ingress is fail-closed**:

- ten years of full-market minute/day aggregate Flat Files and 29 required REST research
  datasets (31 dataset families in total) are stored immutably on the remote data volume;
- the REST catalog now includes one unadjusted full-market Daily Market Summary per session from
  2016-07-13 and an isolated legacy/deprecated combined-financials fallback from 2009-03-29;
- those legacy financial rows are only point-in-time candidates: they are not approved PIT factor
  inputs until Silver applies filing/acceptance-time rules, EDGAR cross-checks, and quarantines;
- every saved file is manifest-bound, checksummed, resumable, and covered by the full Bronze audit;
- the Silver S0 control plane is implemented: frozen schemas/QA rules, source inventories, immutable
  review workflows, approval-bound releases, and a release-only reader are covered by synthetic tests;
- S1 exchanges, S2 ticker types, and the paired S3 condition-code tables are published through the
  immutable release-only reader with exact schema, QA, and zero-quarantine evidence;
- S4 processed all 5,026 active/inactive Assets manifests, 72,038 pages, and 69,381,182 rows. Its
  three full-scope tables were published as one atomic release set with scope
  `identity_evidence_pending_s7`; they remain `backtest_identity_eligible=false`;
- S5 binds a 15,173-identifier request inventory and 11,471 successful Bronze responses through one
  coverage receipt v2. It publishes 15,173 request-status rows and 12,895 valid ticker-change rows;
  the other 193 raw events are empty-target High quarantine records, not silently dropped rows;
- S5 releases `afc63db6850fb50295daa8e6e499c52fe1c16b8290b7932b08aea67531ff98eb`
  and `18a7eb3dd6805b94151f5b6ce0167c19dbeb328f45bec7c2f806dac42b8a6350`
  passed release-only trust-chain and artifact verification. They are evidence only and also remain
  `backtest_identity_eligible=false`;
- S6 published 30,570 retrospective Overview evidence rows plus 169 pending High quarantine rows;
  permanent identity, ticker validity intervals, and a backtestable universe still require S7;
- S7 completed one approved bounded S4 detector preview and stopped at `awaiting_review`: 19 cases,
  89 suspected rows, 50 source artifacts, and 1,471,768 physically attested rows. External review then
  confirmed that nine tickers contain same-Share-Class non-US Composite FIGIs in Massive US-locale
  records: 79 rows are foreign observations and 10 are correct inverse-case US observations. The
  revised proposal preserves all 19 cases and raw FIGI lineage, adds a separate exact-scope
  `identity_cross_market_adjudication` registry, full-sequence market-consistency QA, and immutable
  OpenFIGI/SEC/issuer evidence. Six contract candidates now await reapproval. No adjudication plan,
  market-consistency scan, four-table materialization, FullRunPlan, PublishPlan, or S7 release has run;

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
transformation. S1 `exchanges` through S6 `ticker_overview_safe` are fully published.
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
approved contracts, full-scope release-set IDs, and evidence-only boundary are documented in
[docs/silver-s4-assets-schema-review.md](docs/silver-s4-assets-schema-review.md). S5's dual source
inventories, exact date-quality decisions, QA/quarantine results, and two published releases are
documented in
[docs/silver-s5-ticker-events-schema-review.md](docs/silver-s5-ticker-events-schema-review.md). The
published S6 evidence contract, lifecycle coverage, QA/quarantine results, and release are documented
in [docs/silver-s6-ticker-overview-schema-review.md](docs/silver-s6-ticker-overview-schema-review.md).
The current hard stop is an exact S7 detector-preview plan: the revised contracts and source-bound
S4 streaming preview runner exist, but no real ticker allowlist, session range, request event, literal
approval, detector output, production membership resolver, full build, or release is authorized.
The runner cannot accept caller-supplied rows, paths, checksums, bundles, or evidence; even a successful
run stops at `awaiting_review` and cannot enter candidate/adjudication/backtest/publication paths.
The exact input binding, observed/canonical split, adjudication rules, contract digests, and approval
wording are in
[docs/silver-s7-identity-resolution-schema-review.md](docs/silver-s7-identity-resolution-schema-review.md).
For S4, the approved schemas are loaded by
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
