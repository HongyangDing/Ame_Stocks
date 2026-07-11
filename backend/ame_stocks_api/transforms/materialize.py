"""Turn verified Massive Bronze pages into unadjusted, reviewable Parquet files."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote
from uuid import uuid4

import polars as pl

from ame_stocks_api.downloads import BronzeReader, build_download_plan, market_session_dates
from ame_stocks_core import ProviderDataset, ProviderRequest

MATERIALIZATION_SCHEMA_VERSION = 1
_UNIVERSE_SCHEMA = {
    "snapshot_date": pl.Date,
    "query_active": pl.Boolean,
    "active": pl.Boolean,
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
_HISTORICAL_TICKER_SCHEMA = {
    "ticker": pl.String,
    "first_active_date": pl.Date,
    "last_active_date": pl.Date,
    "seen_in_inactive_snapshot": pl.Boolean,
    "type": pl.String,
    "name": pl.String,
    "primary_exchange": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "delisted_utc": pl.String,
}
_MINUTE_SCHEMA = {
    "session_date": pl.Date,
    "timestamp_utc": pl.Datetime("ms", "UTC"),
    "ticker": pl.String,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "vwap": pl.Float64,
    "transactions": pl.Int64,
    "otc": pl.Boolean,
}


class MaterializationError(RuntimeError):
    """Raised when offline materialization would be incomplete or non-idempotent."""


@dataclass(frozen=True, slots=True)
class UniverseResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    ticker_file: Path
    snapshot_rows: int
    ticker_count: int


@dataclass(frozen=True, slots=True)
class MinutePartitionResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    request_id: str
    row_count: int
    fragment_count: int
    session_count: int


@dataclass(frozen=True, slots=True)
class DailyMinuteResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    output_path: Path
    session_date: date
    row_count: int
    ticker_count: int
    duplicate_count: int


def materialize_universe(data_root: Path, *, start: date, end: date) -> UniverseResult:
    """Materialize daily active snapshots and a delisting-safe historical ticker union."""

    root = data_root.expanduser().resolve()
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=start,
        end=end,
        active="history",
    )
    reader = BronzeReader(root)
    sources = [reader.source_entry(request) for request in plan.requests]
    source_digest = _stable_digest(sources)
    window = f"{start.isoformat()}_{end.isoformat()}"
    output_root = root / "staging" / "universe" / f"window={window}"
    manifest_path = root / "manifests" / "materialized" / "universe" / f"{window}.json"
    existing = _load_reusable_manifest(root, manifest_path, source_digest)
    if existing:
        return UniverseResult(
            status="skipped",
            manifest_path=manifest_path,
            ticker_file=root / existing["ticker_file"],
            snapshot_rows=int(existing["snapshot_rows"]),
            ticker_count=int(existing["ticker_count"]),
        )

    outputs: list[dict[str, object]] = []
    ticker_state: dict[str, dict[str, Any]] = {}
    snapshot_rows = 0
    for request in plan.requests:
        query_active = dict(request.parameters)["active"] == "true"
        rows: list[dict[str, Any]] = []
        for page in reader.pages(request):
            results = page.document.get("results", [])
            if not isinstance(results, list):
                raise MaterializationError("ticker response results must be an array")
            for result in results:
                if not isinstance(result, dict):
                    raise MaterializationError("ticker response entries must be objects")
                ticker = _required_ticker(result)
                row = {
                    "snapshot_date": request.start,
                    "query_active": query_active,
                    "active": result.get("active"),
                    "ticker": ticker,
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
                rows.append(row)
                _update_ticker_state(
                    ticker_state,
                    ticker=ticker,
                    snapshot_date=request.start,
                    query_active=query_active,
                    result=result,
                    window_start=start,
                    window_end=end,
                )

        frame = pl.DataFrame(rows, schema=_UNIVERSE_SCHEMA, strict=False).sort("ticker")
        status = "active" if query_active else "inactive"
        output_path = (
            output_root
            / "snapshots"
            / f"date={request.start.isoformat()}"
            / f"status={status}"
            / "tickers.parquet"
        )
        outputs.append(_write_parquet(root, output_path, frame))
        snapshot_rows += frame.height

    historical_rows = [state for _, state in sorted(ticker_state.items()) if state["include"]]
    for row in historical_rows:
        row.pop("include")
    historical = pl.DataFrame(
        historical_rows,
        schema=_HISTORICAL_TICKER_SCHEMA,
        strict=False,
    ).sort("ticker")
    historical_path = output_root / "historical_tickers.parquet"
    outputs.append(_write_parquet(root, historical_path, historical))
    ticker_file = output_root / "historical_tickers.txt"
    ticker_text = "".join(f"{ticker}\n" for ticker in historical["ticker"].to_list())
    outputs.append(_write_bytes(root, ticker_file, ticker_text.encode("utf-8")))

    manifest = {
        "completed_at": _now(),
        "kind": "historical_universe",
        "materialization_schema_version": MATERIALIZATION_SCHEMA_VERSION,
        "outputs": outputs,
        "snapshot_rows": snapshot_rows,
        "source_digest": source_digest,
        "sources": sources,
        "status": "complete",
        "ticker_count": historical.height,
        "ticker_file": str(ticker_file.relative_to(root)),
        "window": {"end": end.isoformat(), "start": start.isoformat()},
    }
    _write_json(manifest_path, manifest)
    return UniverseResult(
        status="materialized",
        manifest_path=manifest_path,
        ticker_file=ticker_file,
        snapshot_rows=snapshot_rows,
        ticker_count=historical.height,
    )


def partition_minute_request(data_root: Path, request: ProviderRequest) -> MinutePartitionResult:
    """Parse one ticker request into a compact, reviewable two-year Parquet file."""

    if request.dataset is not ProviderDataset.MINUTE_BARS or len(request.asset_ids) != 1:
        raise ValueError("partition_minute_request requires one minute_bars ticker request")
    root = data_root.expanduser().resolve()
    reader = BronzeReader(root)
    source = reader.source_entry(request)
    source_digest = _stable_digest(source)
    manifest_path = (
        root / "manifests" / "materialized" / "minute_partitions" / f"{request.request_id}.json"
    )
    existing = _load_reusable_manifest(root, manifest_path, source_digest)
    if existing:
        return MinutePartitionResult(
            status="skipped",
            manifest_path=manifest_path,
            request_id=request.request_id,
            row_count=int(existing["row_count"]),
            fragment_count=int(existing["fragment_count"]),
            session_count=int(existing["session_count"]),
        )

    ticker = request.asset_ids[0]
    encoded_ticker = quote(ticker, safe="._-")
    frames: list[pl.DataFrame] = []
    for page in reader.pages(request):
        results = page.document.get("results", [])
        if not isinstance(results, list):
            raise MaterializationError("minute response results must be an array")
        frames.append(_minute_frame(results, ticker))

    frame = (
        pl.concat(frames, how="vertical").sort("timestamp_utc")
        if frames
        else pl.DataFrame(schema=_MINUTE_SCHEMA)
    )
    output_path = (
        root
        / "staging"
        / "minute_unadjusted"
        / "by_ticker"
        / f"ticker={encoded_ticker}"
        / f"request_id={request.request_id}"
        / "bars.parquet"
    )
    outputs = [
        _write_parquet(
            root,
            output_path,
            frame,
            extra={"source_page_count": len(source["artifacts"])},
        )
    ]
    session_count = frame["session_date"].n_unique() if frame.height else 0

    manifest = {
        "completed_at": _now(),
        "fragment_count": len(outputs),
        "kind": "minute_partition",
        "materialization_schema_version": MATERIALIZATION_SCHEMA_VERSION,
        "outputs": outputs,
        "request_id": request.request_id,
        "row_count": frame.height,
        "session_count": session_count,
        "source_digest": source_digest,
        "sources": [source],
        "status": "complete",
        "ticker": ticker,
    }
    _write_json(manifest_path, manifest)
    return MinutePartitionResult(
        status="materialized",
        manifest_path=manifest_path,
        request_id=request.request_id,
        row_count=frame.height,
        fragment_count=len(outputs),
        session_count=session_count,
    )


def compact_minute_days(
    data_root: Path,
    *,
    start: date,
    end: date,
    requests: tuple[ProviderRequest, ...],
) -> tuple[DailyMinuteResult, ...]:
    """Create one long-format, unadjusted Parquet file per trading day."""

    root = data_root.expanduser().resolve()
    partition_manifests = [_partition_manifest(root, request) for request in requests]
    inputs = sorted(
        (output for manifest in partition_manifests for output in manifest["outputs"]),
        key=lambda output: str(output["path"]),
    )
    source_identity = {
        "expected_request_ids": sorted(request.request_id for request in requests),
        "inputs": [{"path": item["path"], "sha256": item["sha256"]} for item in inputs],
    }
    source_digest = _stable_digest(source_identity)
    sessions = market_session_dates(start, end)
    existing_by_session: dict[date, dict[str, Any] | None] = {}
    for session in sessions:
        manifest_path = (
            root / "manifests" / "materialized" / "minute_daily" / f"{session.isoformat()}.json"
        )
        existing_by_session[session] = _load_reusable_manifest(root, manifest_path, source_digest)

    if all(existing_by_session.values()):
        return tuple(
            _daily_result_from_manifest(root, session, existing_by_session[session])
            for session in sessions
        )

    temporary_root = root / "tmp" / "minute_daily" / uuid4().hex
    temporary_partitions = temporary_root / "partitions"
    input_paths = [_safe_relative_path(root, item["path"]) for item in inputs]
    try:
        if input_paths:
            (
                pl.scan_parquet(input_paths)
                .filter(pl.col("session_date").is_between(start, end, closed="both"))
                .sink_parquet(
                    pl.PartitionBy(
                        temporary_partitions,
                        key="session_date",
                        include_key=True,
                        approximate_bytes_per_file=None,
                    ),
                    compression="zstd",
                    statistics=True,
                    mkdir=True,
                    engine="streaming",
                )
            )
        expected_sessions = set(sessions)
        actual_sessions = {
            date.fromisoformat(path.name.removeprefix("session_date="))
            for path in temporary_partitions.glob("session_date=*")
            if path.is_dir()
        }
        unexpected_sessions = sorted(actual_sessions - expected_sessions)
        if unexpected_sessions:
            unexpected = ", ".join(value.isoformat() for value in unexpected_sessions[:5])
            raise MaterializationError(f"minute data contains non-XNYS session dates: {unexpected}")

        results: list[DailyMinuteResult] = []
        for session in sessions:
            existing = existing_by_session[session]
            if existing:
                results.append(_daily_result_from_manifest(root, session, existing))
                continue
            temporary_files = sorted(
                (temporary_partitions / f"session_date={session.isoformat()}").glob("*.parquet")
            )
            frames = [pl.read_parquet(path) for path in temporary_files]
            frame = (
                pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=_MINUTE_SCHEMA)
            )
            duplicate_count = _duplicate_count(frame)
            frame = frame.sort(["timestamp_utc", "ticker"])
            output_path = _daily_output_path(root, session)
            output = _write_parquet(root, output_path, frame)
            ticker_count = frame["ticker"].n_unique() if frame.height else 0
            manifest_path = (
                root / "manifests" / "materialized" / "minute_daily" / f"{session.isoformat()}.json"
            )
            manifest = {
                "completed_at": _now(),
                "duplicate_count": duplicate_count,
                "input_count": len(inputs),
                "kind": "minute_daily_unadjusted",
                "materialization_schema_version": MATERIALIZATION_SCHEMA_VERSION,
                "outputs": [output],
                "row_count": frame.height,
                "session_date": session.isoformat(),
                "source_digest": source_digest,
                "sources": source_identity,
                "status": "complete",
                "ticker_count": ticker_count,
            }
            _write_json(manifest_path, manifest)
            results.append(
                DailyMinuteResult(
                    status="materialized",
                    manifest_path=manifest_path,
                    output_path=output_path,
                    session_date=session,
                    row_count=frame.height,
                    ticker_count=ticker_count,
                    duplicate_count=duplicate_count,
                )
            )
        return tuple(results)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)


def _minute_frame(results: list[object], ticker: str) -> pl.DataFrame:
    columns: dict[str, list[object]] = {
        "timestamp_ms": [],
        "ticker": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "vwap": [],
        "transactions": [],
        "otc": [],
    }
    for result in results:
        if not isinstance(result, dict):
            raise MaterializationError("minute response entries must be objects")
        timestamp = result.get("t")
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise MaterializationError("minute response entry is missing numeric t")
        columns["timestamp_ms"].append(int(timestamp))
        columns["ticker"].append(ticker)
        columns["open"].append(result.get("o"))
        columns["high"].append(result.get("h"))
        columns["low"].append(result.get("l"))
        columns["close"].append(result.get("c"))
        columns["volume"].append(result.get("v"))
        columns["vwap"].append(result.get("vw"))
        columns["transactions"].append(result.get("n"))
        columns["otc"].append(result.get("otc", False))

    if not results:
        return pl.DataFrame(schema=_MINUTE_SCHEMA)
    return (
        pl.DataFrame(columns, strict=False)
        .with_columns(
            pl.from_epoch("timestamp_ms", time_unit="ms")
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )
        .with_columns(
            pl.col("timestamp_utc")
            .dt.convert_time_zone("America/New_York")
            .dt.date()
            .alias("session_date")
        )
        .select(list(_MINUTE_SCHEMA))
        .cast(_MINUTE_SCHEMA, strict=False)
    )


def _daily_result_from_manifest(
    root: Path,
    session: date,
    manifest: dict[str, Any] | None,
) -> DailyMinuteResult:
    if manifest is None:
        raise MaterializationError("daily materialization manifest is unexpectedly missing")
    return DailyMinuteResult(
        status="skipped",
        manifest_path=(
            root / "manifests" / "materialized" / "minute_daily" / f"{session.isoformat()}.json"
        ),
        output_path=_daily_output_path(root, session),
        session_date=session,
        row_count=int(manifest["row_count"]),
        ticker_count=int(manifest["ticker_count"]),
        duplicate_count=int(manifest["duplicate_count"]),
    )


def _daily_output_path(root: Path, session: date) -> Path:
    return root / "silver_unadjusted" / "minute" / f"date={session.isoformat()}" / "bars.parquet"


def _update_ticker_state(
    state: dict[str, dict[str, Any]],
    *,
    ticker: str,
    snapshot_date: date,
    query_active: bool,
    result: dict[str, Any],
    window_start: date,
    window_end: date,
) -> None:
    current = state.setdefault(
        ticker,
        {
            "ticker": ticker,
            "first_active_date": None,
            "last_active_date": None,
            "seen_in_inactive_snapshot": False,
            "type": None,
            "name": None,
            "primary_exchange": None,
            "composite_figi": None,
            "share_class_figi": None,
            "delisted_utc": None,
            "include": False,
        },
    )
    if query_active:
        current["include"] = True
        current["first_active_date"] = min(
            filter(None, (current["first_active_date"], snapshot_date))
        )
        current["last_active_date"] = max(
            filter(None, (current["last_active_date"], snapshot_date))
        )
    else:
        current["seen_in_inactive_snapshot"] = True
        delisted = _parse_iso_date(result.get("delisted_utc"))
        if delisted is not None and window_start <= delisted <= window_end:
            current["include"] = True
    for key in (
        "type",
        "name",
        "primary_exchange",
        "composite_figi",
        "share_class_figi",
        "delisted_utc",
    ):
        if result.get(key) is not None:
            current[key] = result[key]


def _required_ticker(result: dict[str, Any]) -> str:
    ticker = result.get("ticker")
    if not isinstance(ticker, str) or not ticker.strip():
        raise MaterializationError("ticker response entry is missing ticker")
    return ticker.strip().upper()


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _partition_manifest(root: Path, request: ProviderRequest) -> dict[str, Any]:
    path = root / "manifests" / "materialized" / "minute_partitions" / f"{request.request_id}.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise MaterializationError(f"minute partition is missing: {request.request_id}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise MaterializationError(f"cannot read minute partition manifest: {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise MaterializationError(f"minute partition is not complete: {request.request_id}")
    if manifest.get("request_id") != request.request_id:
        raise MaterializationError("minute partition request_id mismatch")
    _verify_outputs(root, manifest.get("outputs"))
    return manifest


def _load_reusable_manifest(
    root: Path,
    path: Path,
    source_digest: str,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MaterializationError(f"cannot read materialization manifest: {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise MaterializationError(f"materialization manifest is not complete: {path}")
    if manifest.get("materialization_schema_version") != MATERIALIZATION_SCHEMA_VERSION:
        raise MaterializationError("materialization manifest schema is incompatible")
    if manifest.get("source_digest") != source_digest:
        raise MaterializationError("materialized output source set changed; refusing overwrite")
    _verify_outputs(root, manifest.get("outputs"))
    return manifest


def _verify_outputs(root: Path, outputs: object) -> None:
    if not isinstance(outputs, list):
        raise MaterializationError("materialization outputs must be an array")
    for output in outputs:
        if not isinstance(output, dict):
            raise MaterializationError("materialization output must be an object")
        path = _safe_relative_path(root, output.get("path"))
        if not path.is_file():
            raise MaterializationError(f"materialized output is missing: {path}")
        if _sha256_file(path) != output.get("sha256"):
            raise MaterializationError(f"materialized output checksum failed: {path}")


def _write_parquet(
    root: Path,
    path: Path,
    frame: pl.DataFrame,
    *,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        checksum = _sha256_file(temporary)
        size = temporary.stat().st_size
        if path.exists():
            if _sha256_file(path) != checksum:
                raise MaterializationError(f"refusing to overwrite materialized output: {path}")
        else:
            os.replace(temporary, path)
        output: dict[str, object] = {
            "bytes": size,
            "path": str(path.relative_to(root)),
            "row_count": frame.height,
            "sha256": checksum,
        }
        if extra:
            output.update(extra)
        return output
    finally:
        temporary.unlink(missing_ok=True)


def _write_bytes(root: Path, path: Path, content: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    checksum = hashlib.sha256(content).hexdigest()
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
            raise MaterializationError(f"refusing to overwrite materialized output: {path}")
    else:
        temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
        try:
            temporary.write_bytes(content)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return {
        "bytes": len(content),
        "path": str(path.relative_to(root)),
        "sha256": checksum,
    }


def _write_json(path: Path, document: dict[str, Any]) -> None:
    content = json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _safe_relative_path(root: Path, relative: object) -> Path:
    if not isinstance(relative, str):
        raise MaterializationError("materialized path must be a string")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise MaterializationError("materialized path escaped data root") from exc
    return path


def _stable_digest(value: object) -> str:
    serialized = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _duplicate_count(frame: pl.DataFrame) -> int:
    if not frame.height:
        return 0
    duplicate_rows = (
        frame.group_by(["ticker", "timestamp_utc"])
        .len()
        .filter(pl.col("len") > 1)
        .select((pl.col("len") - 1).sum())
        .item()
    )
    return int(duplicate_rows or 0)


def _now() -> str:
    return datetime.now(UTC).isoformat()
