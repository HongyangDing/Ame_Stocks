# Ame Stocks

A resume-grade U.S. equity factor research and backtesting platform. The project is being built in explicit, reviewable stages so the complete market-data pipeline can be inspected before real data is introduced.

## Current milestone

Step 1 establishes the application skeleton and the two public Python contracts:

- `DataProvider`: an asynchronous, resumable source adapter that returns immutable raw payload batches.
- `FactorSpec`: a Git-managed Polars factor plugin that emits `signal_date`, `asset_id`, and `raw_value`.

The current `MockProvider` is deterministic and returns an empty JSON payload. A reviewed Massive downloader is also available, but it contacts the API only through the explicit `download` command. Synthetic market generation begins in Step 3.

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

Copy `.env.example` only when local overrides are needed. Planning requires no API key. An approved live download will read Massive credentials only from an untracked `MASSIVE_API_KEY` environment variable.

The future remote data root is reserved as:

```text
/mnt/HC_Volume_106309665/american_stocks
```

No remote directories, services, domains, or legacy applications are changed by Step 1.

## Massive downloader review

Planning never reads `MASSIVE_API_KEY` and never contacts Massive:

```bash
.venv/bin/ame-massive plan \
  --dataset daily_bars \
  --start 2024-07-01 \
  --end 2026-06-30
```

The live command is intentionally distinct and requires an explicit storage root:

```bash
MASSIVE_API_KEY='set-in-your-shell' .venv/bin/ame-massive download \
  --dataset minute_bars \
  --ticker-file tickers.txt \
  --start 2024-07-01 \
  --end 2026-06-30 \
  --data-root /mnt/HC_Volume_106309665/american_stocks
```

Do not run the live command until the downloader has been reviewed. See [docs/massive-downloader.md](docs/massive-downloader.md) for endpoints, rate limits, storage layout, and resume behavior.

After a reviewed download, `ame-materialize` performs offline-only conversion. It can
build the historical ticker union, parse each unadjusted ticker stream into reviewable
Parquet, and—only after a separate explicit command—compact by New York date into one
long-format file per session. It never reads `MASSIVE_API_KEY` or contacts Massive.
