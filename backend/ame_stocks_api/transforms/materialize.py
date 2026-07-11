"""Build daily point-in-time security masters from verified REST Bronze pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

import polars as pl

from ame_stocks_api.artifacts import (
    ArtifactError,
    load_reusable_manifest,
    now_utc,
    stable_digest,
    write_bytes_immutable,
    write_json_atomic,
    write_parquet_immutable,
)
from ame_stocks_api.downloads import BronzeReader, build_download_plan, market_session_dates
from ame_stocks_core import ProviderDataset, ProviderRequest

UNIVERSE_SCHEMA_VERSION = 2
_SNAPSHOT_SCHEMA = {
    "snapshot_date": pl.Date,
    "active_on_date": pl.Boolean,
    "provider_active": pl.Boolean,
    "ticker": pl.String,
    "type": pl.String,
    "name": pl.String,
    "market": pl.String,
    "locale": pl.String,
    "primary_exchange": pl.String,
    "currency_name": pl.String,
    "cik": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "delisted_utc": pl.String,
    "last_updated_utc": pl.String,
}
_HISTORICAL_SCHEMA = {
    "ticker": pl.String,
    "first_seen_date": pl.Date,
    "last_seen_date": pl.Date,
    "first_active_date": pl.Date,
    "last_active_date": pl.Date,
    "active_on_end_date": pl.Boolean,
    "ever_inactive": pl.Boolean,
    "type": pl.String,
    "name": pl.String,
    "primary_exchange": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "delisted_utc": pl.String,
}


class MaterializationError(ArtifactError):
    """Raised when reference snapshots cannot form a trustworthy daily master."""


@dataclass(frozen=True, slots=True)
class UniverseResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    ticker_file: Path
    snapshot_rows: int
    ticker_count: int
    daily_file_count: int


def materialize_universe(data_root: Path, *, start: date, end: date) -> UniverseResult:
    """Materialize active and inactive reference results for every trading session."""

    root = data_root.expanduser().resolve()
    sessions = market_session_dates(start, end)
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=start,
        end=end,
        active="both",
    )
    reader = BronzeReader(root)
    sources = [reader.source_entry(request) for request in plan.requests]
    source_digest = stable_digest(sources)
    window = f"{start.isoformat()}_{end.isoformat()}"
    staging_root = root / "staging" / "universe" / f"window={window}"
    manifest_path = root / "manifests" / "materialized" / "universe" / f"{window}.json"
    existing = load_reusable_manifest(
        root,
        manifest_path,
        source_digest=source_digest,
        schema_version=UNIVERSE_SCHEMA_VERSION,
    )
    if existing:
        return UniverseResult(
            status="skipped",
            manifest_path=manifest_path,
            ticker_file=root / existing["ticker_file"],
            snapshot_rows=int(existing["snapshot_rows"]),
            ticker_count=int(existing["ticker_count"]),
            daily_file_count=int(existing["daily_file_count"]),
        )

    request_by_key = {
        (request.start, dict(request.parameters)["active"]): request for request in plan.requests
    }
    outputs: list[dict[str, object]] = []
    historical: dict[str, dict[str, Any]] = {}
    daily_counts: list[dict[str, object]] = []
    snapshot_rows = 0
    for session in sessions:
        active = _snapshot_frame(reader, request_by_key[(session, "true")], True)
        inactive = _snapshot_frame(reader, request_by_key[(session, "false")], False)
        combined = pl.concat((active, inactive), how="vertical").sort("ticker")
        if combined["ticker"].n_unique() != combined.height:
            duplicates = (
                combined.group_by("ticker")
                .len()
                .filter(pl.col("len") > 1)["ticker"]
                .head(5)
                .to_list()
            )
            raise MaterializationError(
                f"ticker appeared in both active/inactive snapshots: {duplicates}"
            )
        mismatch = combined.filter(
            pl.col("provider_active").is_not_null()
            & (pl.col("provider_active") != pl.col("active_on_date"))
        )
        if mismatch.height:
            raise MaterializationError("provider active field contradicts the query status")
        daily_path = (
            root
            / "silver_unadjusted"
            / "universe"
            / f"date={session.isoformat()}"
            / "tickers.parquet"
        )
        outputs.append(write_parquet_immutable(root, daily_path, combined))
        snapshot_rows += combined.height
        daily_counts.append(
            {
                "active": active.height,
                "date": session.isoformat(),
                "inactive": inactive.height,
                "total": combined.height,
            }
        )
        for row in combined.iter_rows(named=True):
            _update_historical(historical, row=row, end_session=sessions[-1])

    historical_frame = pl.DataFrame(
        [value for _, value in sorted(historical.items())],
        schema=_HISTORICAL_SCHEMA,
        strict=False,
    ).sort("ticker")
    historical_path = staging_root / "historical_tickers.parquet"
    outputs.append(write_parquet_immutable(root, historical_path, historical_frame))
    ticker_file = staging_root / "historical_tickers.txt"
    ticker_text = "".join(f"{ticker}\n" for ticker in historical_frame["ticker"].to_list())
    outputs.append(write_bytes_immutable(root, ticker_file, ticker_text.encode("utf-8")))

    manifest = {
        "completed_at": now_utc(),
        "daily_counts": daily_counts,
        "daily_file_count": len(sessions),
        "kind": "daily_point_in_time_universe",
        "outputs": outputs,
        "schema_version": UNIVERSE_SCHEMA_VERSION,
        "snapshot_rows": snapshot_rows,
        "source_digest": source_digest,
        "sources": sources,
        "status": "complete",
        "ticker_count": historical_frame.height,
        "ticker_file": str(ticker_file.relative_to(root)),
        "window": {"end": end.isoformat(), "start": start.isoformat()},
    }
    write_json_atomic(manifest_path, manifest)
    return UniverseResult(
        status="materialized",
        manifest_path=manifest_path,
        ticker_file=ticker_file,
        snapshot_rows=snapshot_rows,
        ticker_count=historical_frame.height,
        daily_file_count=len(sessions),
    )


def _snapshot_frame(
    reader: BronzeReader,
    request: ProviderRequest,
    active_on_date: bool,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for page in reader.pages(request):
        results = page.document.get("results", [])
        if not isinstance(results, list):
            raise MaterializationError("ticker response results must be an array")
        for result in results:
            if not isinstance(result, dict):
                raise MaterializationError("ticker response entries must be objects")
            ticker = result.get("ticker")
            if not isinstance(ticker, str) or not ticker.strip():
                raise MaterializationError("ticker response entry is missing ticker")
            rows.append(
                {
                    "snapshot_date": request.start,
                    "active_on_date": active_on_date,
                    "provider_active": result.get("active"),
                    # Massive identifiers are case-sensitive: lowercase characters can
                    # encode preferred/special share suffixes (for example BCPC and
                    # BCpC are different securities). Preserve the provider identifier.
                    "ticker": ticker.strip(),
                    "type": result.get("type"),
                    "name": result.get("name"),
                    "market": result.get("market"),
                    "locale": result.get("locale"),
                    "primary_exchange": result.get("primary_exchange"),
                    "currency_name": result.get("currency_name"),
                    "cik": result.get("cik"),
                    "composite_figi": result.get("composite_figi"),
                    "share_class_figi": result.get("share_class_figi"),
                    "delisted_utc": result.get("delisted_utc"),
                    "last_updated_utc": result.get("last_updated_utc"),
                }
            )
    frame = pl.DataFrame(rows, schema=_SNAPSHOT_SCHEMA, strict=False).sort("ticker")
    if frame["ticker"].n_unique() != frame.height:
        raise MaterializationError("one ticker snapshot contains duplicate ticker rows")
    return frame


def _update_historical(
    state: dict[str, dict[str, Any]],
    *,
    row: dict[str, Any],
    end_session: date,
) -> None:
    ticker = str(row["ticker"])
    snapshot_date = row["snapshot_date"]
    active = bool(row["active_on_date"])
    current = state.setdefault(
        ticker,
        {
            "ticker": ticker,
            "first_seen_date": snapshot_date,
            "last_seen_date": snapshot_date,
            "first_active_date": None,
            "last_active_date": None,
            "active_on_end_date": False,
            "ever_inactive": False,
            "type": None,
            "name": None,
            "primary_exchange": None,
            "composite_figi": None,
            "share_class_figi": None,
            "delisted_utc": None,
        },
    )
    current["first_seen_date"] = min(current["first_seen_date"], snapshot_date)
    current["last_seen_date"] = max(current["last_seen_date"], snapshot_date)
    if active:
        current["first_active_date"] = _minimum_date(current["first_active_date"], snapshot_date)
        current["last_active_date"] = _maximum_date(current["last_active_date"], snapshot_date)
    else:
        current["ever_inactive"] = True
    if snapshot_date == end_session:
        current["active_on_end_date"] = active
    for key in (
        "type",
        "name",
        "primary_exchange",
        "composite_figi",
        "share_class_figi",
        "delisted_utc",
    ):
        if row.get(key) is not None:
            current[key] = row[key]


def _minimum_date(left: date | None, right: date) -> date:
    return right if left is None else min(left, right)


def _maximum_date(left: date | None, right: date) -> date:
    return right if left is None else max(left, right)
