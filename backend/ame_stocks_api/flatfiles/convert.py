"""Offline Flat File CSV-to-Parquet conversion and universe coverage QA."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

import polars as pl

from ame_stocks_api.artifacts import (
    ArtifactError,
    load_reusable_manifest,
    now_utc,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_json_atomic,
    write_parquet_immutable,
)
from ame_stocks_api.flatfiles.plan import FlatFileDataset, FlatFileObject

FLAT_FILE_CONVERT_SCHEMA_VERSION = 1
COVERAGE_SCHEMA_VERSION = 1
DEFAULT_MINIMUM_FREE_BYTES = 40 * 1024**3
_REQUIRED_COLUMNS = frozenset(
    {"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"}
)
_CSV_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.Float64,
    "open": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "window_start": pl.Int64,
    "transactions": pl.Int64,
}


@dataclass(frozen=True, slots=True)
class FlatFileConvertResult:
    status: Literal["converted", "skipped"]
    manifest_path: Path
    output_path: Path
    row_count: int
    ticker_count: int
    duplicate_count: int


@dataclass(frozen=True, slots=True)
class CoverageResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    output_path: Path
    ticker_count: int
    active_without_bars: int
    inactive_with_bars: int
    bars_without_reference: int


def convert_flat_file(
    data_root: Path,
    item: FlatFileObject,
    *,
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
) -> FlatFileConvertResult:
    """Convert one verified daily gzip CSV without cleaning or adjustment."""

    root = data_root.expanduser().resolve()
    if minimum_free_bytes < 0:
        raise ValueError("minimum_free_bytes cannot be negative")
    source = _flat_file_source(root, item)
    source_digest = stable_digest(source)
    manifest_path = (
        root
        / "manifests"
        / "materialized"
        / "flatfiles"
        / item.dataset.value
        / f"{item.session_date.isoformat()}.json"
    )
    existing = load_reusable_manifest(
        root,
        manifest_path,
        source_digest=source_digest,
        schema_version=FLAT_FILE_CONVERT_SCHEMA_VERSION,
    )
    output_path = _converted_path(root, item)
    if existing:
        return FlatFileConvertResult(
            status="skipped",
            manifest_path=manifest_path,
            output_path=output_path,
            row_count=int(existing["row_count"]),
            ticker_count=int(existing["ticker_count"]),
            duplicate_count=int(existing["duplicate_count"]),
        )
    estimated_peak_bytes = int(source["bytes"]) * 4
    if shutil.disk_usage(root).free - estimated_peak_bytes < minimum_free_bytes:
        raise ArtifactError(
            "conversion would reduce free disk space below the configured safety floor"
        )

    raw_path = safe_relative_path(root, source["path"])
    try:
        raw = pl.read_csv(
            raw_path,
            schema_overrides=_CSV_SCHEMA,
            null_values=["", "null"],
        )
    except Exception as exc:
        raise ArtifactError(f"cannot parse Massive Flat File CSV: {raw_path}") from exc
    missing_columns = sorted(_REQUIRED_COLUMNS - set(raw.columns))
    if missing_columns:
        raise ArtifactError(f"Flat File CSV is missing columns: {', '.join(missing_columns)}")
    if raw["ticker"].null_count() or raw["window_start"].null_count():
        raise ArtifactError("Flat File contains null ticker or window_start values")

    frame = (
        raw.select(list(_CSV_SCHEMA))
        .with_columns(
            pl.from_epoch("window_start", time_unit="ns")
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc"),
            pl.lit(item.session_date).cast(pl.Date).alias("session_date"),
        )
        .select(
            "session_date",
            "timestamp_utc",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "transactions",
        )
        .sort(["timestamp_utc", "ticker"])
    )
    if item.dataset is FlatFileDataset.MINUTE_AGGREGATES and frame.height:
        observed_dates = set(
            frame.select(
                pl.col("timestamp_utc")
                .dt.convert_time_zone("America/New_York")
                .dt.date()
                .alias("date")
            )["date"].unique()
        )
        if observed_dates != {item.session_date}:
            raise ArtifactError(
                "minute Flat File timestamps do not match its New York session date"
            )

    duplicate_count = _duplicate_count(frame)
    ticker_count = frame["ticker"].n_unique() if frame.height else 0
    null_counts = {
        name: int(value) for name, value in frame.null_count().row(0, named=True).items() if value
    }
    output = write_parquet_immutable(root, output_path, frame)
    manifest = {
        "completed_at": now_utc(),
        "dataset": item.dataset.value,
        "duplicate_count": duplicate_count,
        "kind": "massive_flat_file_parquet",
        "null_counts": null_counts,
        "outputs": [output],
        "row_count": frame.height,
        "schema_version": FLAT_FILE_CONVERT_SCHEMA_VERSION,
        "session_date": item.session_date.isoformat(),
        "source_digest": source_digest,
        "sources": [source],
        "status": "complete",
        "ticker_count": ticker_count,
    }
    write_json_atomic(manifest_path, manifest)
    return FlatFileConvertResult(
        status="converted",
        manifest_path=manifest_path,
        output_path=output_path,
        row_count=frame.height,
        ticker_count=ticker_count,
        duplicate_count=duplicate_count,
    )


def build_daily_coverage(data_root: Path, *, session_date: date) -> CoverageResult:
    """Join activity bars to the daily reference master without inferring listing status."""

    root = data_root.expanduser().resolve()
    universe_path = (
        root
        / "silver_unadjusted"
        / "universe"
        / f"date={session_date.isoformat()}"
        / "tickers.parquet"
    )
    bars_path = (
        root / "silver_unadjusted" / "minute" / f"date={session_date.isoformat()}" / "bars.parquet"
    )
    for path in (universe_path, bars_path):
        if not path.is_file():
            raise ArtifactError(f"coverage input is missing: {path}")
    source = {
        "bars": {
            "path": str(bars_path.relative_to(root)),
            "sha256": sha256_file(bars_path),
        },
        "universe": {
            "path": str(universe_path.relative_to(root)),
            "sha256": sha256_file(universe_path),
        },
    }
    source_digest = stable_digest(source)
    manifest_path = (
        root / "manifests" / "materialized" / "coverage" / f"{session_date.isoformat()}.json"
    )
    output_path = (
        root
        / "silver_unadjusted"
        / "coverage"
        / f"date={session_date.isoformat()}"
        / "ticker_coverage.parquet"
    )
    existing = load_reusable_manifest(
        root,
        manifest_path,
        source_digest=source_digest,
        schema_version=COVERAGE_SCHEMA_VERSION,
    )
    if existing:
        return CoverageResult(
            status="skipped",
            manifest_path=manifest_path,
            output_path=output_path,
            ticker_count=int(existing["ticker_count"]),
            active_without_bars=int(existing["active_without_bars"]),
            inactive_with_bars=int(existing["inactive_with_bars"]),
            bars_without_reference=int(existing["bars_without_reference"]),
        )

    universe = pl.read_parquet(universe_path)
    bars = pl.read_parquet(bars_path)
    required_universe = {"ticker", "active_on_date"}
    if not required_universe.issubset(universe.columns):
        raise ArtifactError("daily universe is missing ticker or active_on_date")
    if universe["ticker"].n_unique() != universe.height:
        raise ArtifactError("daily universe contains duplicate tickers")
    bar_stats = bars.group_by("ticker").agg(
        pl.len().alias("minute_count"),
        pl.col("timestamp_utc").min().alias("first_bar_utc"),
        pl.col("timestamp_utc").max().alias("last_bar_utc"),
    )
    reference = universe.select(
        "ticker",
        "active_on_date",
        *(
            column
            for column in ("type", "name", "primary_exchange", "delisted_utc")
            if column in universe.columns
        ),
    )
    coverage = (
        reference.join(bar_stats, on="ticker", how="full", coalesce=True)
        .with_columns(
            pl.col("minute_count").is_not_null().alias("has_minute_bar"),
            pl.col("active_on_date").is_null().alias("reference_missing"),
        )
        .with_columns(
            (pl.col("active_on_date").eq(True) & ~pl.col("has_minute_bar")).alias(
                "active_without_bars"
            ),
            (pl.col("active_on_date").eq(False) & pl.col("has_minute_bar")).alias(
                "inactive_with_bars"
            ),
        )
        .sort("ticker")
    )
    active_without_bars = _true_count(coverage, "active_without_bars")
    inactive_with_bars = _true_count(coverage, "inactive_with_bars")
    bars_without_reference = _true_count(coverage, "reference_missing")
    output = write_parquet_immutable(root, output_path, coverage)
    manifest = {
        "active_without_bars": active_without_bars,
        "bars_without_reference": bars_without_reference,
        "completed_at": now_utc(),
        "inactive_with_bars": inactive_with_bars,
        "kind": "daily_ticker_coverage",
        "outputs": [output],
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "session_date": session_date.isoformat(),
        "source_digest": source_digest,
        "sources": source,
        "status": "complete",
        "ticker_count": coverage.height,
    }
    write_json_atomic(manifest_path, manifest)
    return CoverageResult(
        status="materialized",
        manifest_path=manifest_path,
        output_path=output_path,
        ticker_count=coverage.height,
        active_without_bars=active_without_bars,
        inactive_with_bars=inactive_with_bars,
        bars_without_reference=bars_without_reference,
    )


def _flat_file_source(root: Path, item: FlatFileObject) -> dict[str, Any]:
    manifest_path = (
        root
        / "manifests"
        / "massive"
        / "flatfiles"
        / item.dataset.value
        / f"{item.session_date.isoformat()}.json"
    )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ArtifactError(f"Flat File manifest is missing: {manifest_path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"cannot read Flat File manifest: {manifest_path}") from exc
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise ArtifactError(f"Flat File download is not complete: {item.object_id}")
    if manifest.get("object_id") != item.object_id:
        raise ArtifactError("Flat File manifest object_id mismatch")
    output = manifest.get("output")
    if not isinstance(output, dict):
        raise ArtifactError("Flat File manifest has no output")
    path = safe_relative_path(root, output.get("path"))
    checksum = sha256_file(path)
    if checksum != output.get("sha256"):
        raise ArtifactError(f"Flat File checksum failed: {path}")
    return {
        "bytes": int(output["bytes"]),
        "object_id": item.object_id,
        "object_key": item.object_key,
        "path": str(path.relative_to(root)),
        "remote": manifest.get("remote"),
        "sha256": checksum,
    }


def _converted_path(root: Path, item: FlatFileObject) -> Path:
    layer = "minute" if item.dataset is FlatFileDataset.MINUTE_AGGREGATES else "daily"
    return (
        root
        / "silver_unadjusted"
        / layer
        / f"date={item.session_date.isoformat()}"
        / "bars.parquet"
    )


def _duplicate_count(frame: pl.DataFrame) -> int:
    if not frame.height:
        return 0
    count = (
        frame.group_by(["ticker", "timestamp_utc"])
        .len()
        .filter(pl.col("len") > 1)
        .select((pl.col("len") - 1).sum())
        .item()
    )
    return int(count or 0)


def _true_count(frame: pl.DataFrame, column: str) -> int:
    return int(frame.select(pl.col(column).fill_null(False).sum()).item() or 0)
