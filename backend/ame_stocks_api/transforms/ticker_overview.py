"""Build lifecycle Ticker Overview requests and an allowlisted reference table."""

from __future__ import annotations

import json
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
    verify_outputs,
    write_bytes_immutable,
    write_json_atomic,
    write_parquet_immutable,
)
from ame_stocks_api.downloads import BronzeReader, build_download_plan
from ame_stocks_api.downloads.bronze import BronzeStorageError
from ame_stocks_api.transforms.materialize import MaterializationError
from ame_stocks_core import ProviderDataset, ProviderRequest

TICKER_LIFECYCLE_SCHEMA_VERSION = 2
TICKER_OVERVIEW_SAFE_SCHEMA_VERSION = 2

# These values are retained only in the immutable Bronze response. They are current-looking
# quantities without a sufficiently explicit point-in-time/as-of contract for factor research.
QUARANTINED_TICKER_OVERVIEW_FIELDS = frozenset(
    {
        "market_cap",
        "share_class_shares_outstanding",
        "weighted_shares_outstanding",
    }
)

_IDENTITY_FIELDS = ("share_class_figi", "composite_figi", "cik")
_LIFECYCLE_SCHEMA = {
    "lifecycle_id": pl.String,
    "ticker": pl.String,
    "first_active_date": pl.Date,
    "last_active_date": pl.Date,
    "query_date": pl.Date,
    "identity_type": pl.String,
    "identity_value": pl.String,
    "cik": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "type": pl.String,
    "name": pl.String,
    "primary_exchange": pl.String,
}
_SAFE_SCHEMA = {
    "lifecycle_id": pl.String,
    "source_request_id": pl.String,
    "query_ticker": pl.String,
    "query_date": pl.Date,
    "first_active_date": pl.Date,
    "last_active_date": pl.Date,
    "identity_type": pl.String,
    "identity_value": pl.String,
    "identity_match": pl.Boolean,
    "identity_match_basis": pl.String,
    "ticker": pl.String,
    "name": pl.String,
    "type": pl.String,
    "market": pl.String,
    "locale": pl.String,
    "active": pl.Boolean,
    "primary_exchange": pl.String,
    "currency_name": pl.String,
    "cik": pl.String,
    "composite_figi": pl.String,
    "share_class_figi": pl.String,
    "sic_code": pl.String,
    "sic_description": pl.String,
    "list_date": pl.Date,
    "delisted_utc": pl.String,
    "ticker_root": pl.String,
    "ticker_suffix": pl.String,
}


@dataclass(frozen=True, slots=True)
class TickerLifecycleResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    lifecycle_path: Path
    request_file: Path
    lifecycle_count: int
    request_count: int


@dataclass(frozen=True, slots=True)
class TickerOverviewSafeResult:
    status: Literal["materialized", "skipped"]
    manifest_path: Path
    output_path: Path
    lifecycle_count: int
    row_count: int
    failed_request_count: int


def materialize_ticker_overview_lifecycles(
    data_root: Path,
    *,
    start: date,
    end: date,
) -> TickerLifecycleResult:
    """Collapse active daily ticker snapshots into ticker/identity lifecycle requests."""

    root = data_root.expanduser().resolve()
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=start,
        end=end,
        active="true",
    )
    reader = BronzeReader(root)
    sources = [reader.source_entry(request) for request in plan.requests]
    source_digest = stable_digest(sources)
    window = f"{start.isoformat()}_{end.isoformat()}"
    schema_directory = f"schema=v{TICKER_LIFECYCLE_SCHEMA_VERSION}"
    staging_root = root / "staging" / "ticker_overview" / schema_directory / f"window={window}"
    lifecycle_path = staging_root / "lifecycles.parquet"
    request_file = staging_root / "requests.csv"
    manifest_path = (
        root
        / "manifests"
        / "materialized"
        / "ticker_overview_lifecycles"
        / schema_directory
        / f"{window}.json"
    )
    existing = load_reusable_manifest(
        root,
        manifest_path,
        source_digest=source_digest,
        schema_version=TICKER_LIFECYCLE_SCHEMA_VERSION,
    )
    if existing:
        return TickerLifecycleResult(
            status="skipped",
            manifest_path=manifest_path,
            lifecycle_path=root / str(existing["lifecycle_file"]),
            request_file=root / str(existing["request_file"]),
            lifecycle_count=int(existing["lifecycle_count"]),
            request_count=int(existing["request_count"]),
        )

    state: dict[str, list[dict[str, Any]]] = {}
    for request in plan.requests:
        for row in _active_snapshot_rows(reader, request):
            _update_lifecycle_state(state, row=row, snapshot_date=request.start)

    lifecycle_rows = [_finalize_lifecycle(item) for records in state.values() for item in records]
    lifecycle_frame = pl.DataFrame(
        lifecycle_rows,
        schema=_LIFECYCLE_SCHEMA,
        strict=False,
    ).sort("query_date", "ticker", "lifecycle_id")
    if lifecycle_frame["lifecycle_id"].n_unique() != lifecycle_frame.height:
        raise MaterializationError("ticker lifecycle identifiers are not unique")

    request_rows = sorted(
        {
            (str(row["ticker"]), row["query_date"])
            for row in lifecycle_frame.iter_rows(named=True)
        },
        key=lambda item: (item[1], item[0]),
    )
    request_content = _request_csv(request_rows)
    outputs = [
        write_parquet_immutable(root, lifecycle_path, lifecycle_frame),
        write_bytes_immutable(root, request_file, request_content),
    ]
    manifest = {
        "completed_at": now_utc(),
        "identity_priority": list(_IDENTITY_FIELDS),
        "kind": "ticker_identity_lifecycle_requests",
        "lifecycle_count": lifecycle_frame.height,
        "lifecycle_file": str(lifecycle_path.relative_to(root)),
        "outputs": outputs,
        "request_count": len(request_rows),
        "request_file": str(request_file.relative_to(root)),
        "schema_version": TICKER_LIFECYCLE_SCHEMA_VERSION,
        "source_digest": source_digest,
        "sources": sources,
        "status": "complete",
        "window": {"end": end.isoformat(), "start": start.isoformat()},
    }
    write_json_atomic(manifest_path, manifest)
    return TickerLifecycleResult(
        status="materialized",
        manifest_path=manifest_path,
        lifecycle_path=lifecycle_path,
        request_file=request_file,
        lifecycle_count=lifecycle_frame.height,
        request_count=len(request_rows),
    )


def materialize_ticker_overview_safe(
    data_root: Path,
    *,
    start: date,
    end: date,
) -> TickerOverviewSafeResult:
    """Project verified Overview Bronze into a strict identity/SIC/list-date allowlist."""

    root = data_root.expanduser().resolve()
    window = f"{start.isoformat()}_{end.isoformat()}"
    lifecycle_schema_directory = f"schema=v{TICKER_LIFECYCLE_SCHEMA_VERSION}"
    lifecycle_manifest_path = (
        root
        / "manifests"
        / "materialized"
        / "ticker_overview_lifecycles"
        / lifecycle_schema_directory
        / f"{window}.json"
    )
    lifecycle_manifest = _load_complete_artifact_manifest(root, lifecycle_manifest_path)
    lifecycle_path = root / str(lifecycle_manifest["lifecycle_file"])
    lifecycle_frame = pl.read_parquet(lifecycle_path)
    _require_columns(lifecycle_frame, set(_LIFECYCLE_SCHEMA), "ticker lifecycle")

    ticker_dates = tuple(
        (str(row["ticker"]), row["query_date"])
        for row in lifecycle_frame.select("ticker", "query_date").unique().iter_rows(named=True)
    )
    plan = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=start,
        end=end,
        ticker_dates=ticker_dates,
    )
    reader = BronzeReader(root)
    source_entries: list[dict[str, object]] = []
    response_by_key: dict[tuple[str, date], tuple[ProviderRequest, dict[str, Any]]] = {}
    failed_request_count = 0
    for request in plan.requests:
        source = _overview_source_entry(reader, request)
        source_entries.append(source)
        if source["status"] == "failed":
            failed_request_count += 1
            continue
        response_by_key[(request.asset_ids[0], request.start)] = (
            request,
            _overview_result(reader, request),
        )

    lifecycle_source = {
        "manifest": str(lifecycle_manifest_path.relative_to(root)),
        "source_digest": str(lifecycle_manifest["source_digest"]),
        "outputs": lifecycle_manifest["outputs"],
    }
    source_digest = stable_digest(
        {"lifecycle": lifecycle_source, "ticker_overview": source_entries}
    )
    output_path = (
        root
        / "silver_unadjusted"
        / "reference"
        / "ticker_overview_safe"
        / f"schema=v{TICKER_OVERVIEW_SAFE_SCHEMA_VERSION}"
        / f"window={window}"
        / "ticker_overview.parquet"
    )
    manifest_path = (
        root
        / "manifests"
        / "materialized"
        / "ticker_overview_safe"
        / f"schema=v{TICKER_OVERVIEW_SAFE_SCHEMA_VERSION}"
        / f"{window}.json"
    )
    existing = load_reusable_manifest(
        root,
        manifest_path,
        source_digest=source_digest,
        schema_version=TICKER_OVERVIEW_SAFE_SCHEMA_VERSION,
    )
    if existing:
        return TickerOverviewSafeResult(
            status="skipped",
            manifest_path=manifest_path,
            output_path=root / str(existing["output_file"]),
            lifecycle_count=int(existing["lifecycle_count"]),
            row_count=int(existing["row_count"]),
            failed_request_count=int(existing["failed_request_count"]),
        )

    safe_rows: list[dict[str, Any]] = []
    for lifecycle in lifecycle_frame.iter_rows(named=True):
        key = (str(lifecycle["ticker"]), lifecycle["query_date"])
        response = response_by_key.get(key)
        if response is None:
            continue
        request, result = response
        safe_rows.append(_safe_overview_row(lifecycle, request=request, result=result))

    safe_frame = pl.DataFrame(safe_rows, schema=_SAFE_SCHEMA, strict=False).sort(
        "query_date", "query_ticker", "lifecycle_id"
    )
    forbidden_columns = QUARANTINED_TICKER_OVERVIEW_FIELDS.intersection(safe_frame.columns)
    if forbidden_columns:
        raise MaterializationError(
            f"unsafe Ticker Overview fields escaped quarantine: {sorted(forbidden_columns)}"
        )
    output = write_parquet_immutable(root, output_path, safe_frame)
    manifest = {
        "completed_at": now_utc(),
        "failed_request_count": failed_request_count,
        "field_policy": {
            "allowlisted_output_columns": list(_SAFE_SCHEMA),
            "quarantined_bronze_only_fields": sorted(QUARANTINED_TICKER_OVERVIEW_FIELDS),
            "temporal_status": (
                "provisional reference inputs; do not treat SEC-derived historical fields as "
                "filing-time-safe without a separate availability-date validation"
            ),
        },
        "kind": "ticker_overview_allowlisted_reference",
        "lifecycle_count": lifecycle_frame.height,
        "output_file": str(output_path.relative_to(root)),
        "outputs": [output],
        "row_count": safe_frame.height,
        "schema_version": TICKER_OVERVIEW_SAFE_SCHEMA_VERSION,
        "source_digest": source_digest,
        "sources": {"lifecycle": lifecycle_source, "ticker_overview": source_entries},
        "status": "complete",
        "window": {"end": end.isoformat(), "start": start.isoformat()},
    }
    write_json_atomic(manifest_path, manifest)
    return TickerOverviewSafeResult(
        status="materialized",
        manifest_path=manifest_path,
        output_path=output_path,
        lifecycle_count=lifecycle_frame.height,
        row_count=safe_frame.height,
        failed_request_count=failed_request_count,
    )


def _active_snapshot_rows(
    reader: BronzeReader,
    request: ProviderRequest,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in reader.pages(request):
        results = page.document.get("results", [])
        if not isinstance(results, list):
            raise MaterializationError("active ticker response results must be an array")
        for result in results:
            if not isinstance(result, dict):
                raise MaterializationError("active ticker response entries must be objects")
            ticker = _clean_string(result.get("ticker"))
            if ticker is None:
                raise MaterializationError("active ticker response entry is missing ticker")
            if result.get("active") is False:
                raise MaterializationError("active=true response contains an inactive ticker")
            rows.append(
                {
                    "ticker": ticker,
                    "cik": _clean_string(result.get("cik")),
                    "composite_figi": _clean_string(result.get("composite_figi")),
                    "share_class_figi": _clean_string(result.get("share_class_figi")),
                    "type": _clean_string(result.get("type")),
                    "name": _clean_string(result.get("name")),
                    "primary_exchange": _clean_string(result.get("primary_exchange")),
                }
            )
    return rows


def _update_lifecycle_state(
    state: dict[str, list[dict[str, Any]]],
    *,
    row: dict[str, Any],
    snapshot_date: date,
) -> None:
    ticker = str(row["ticker"])
    records = state.setdefault(ticker, [])
    current = _matching_lifecycle(records, row)
    if current is None:
        current = {
            "ticker": ticker,
            "first_active_date": snapshot_date,
            "last_active_date": snapshot_date,
            **{
                field: row.get(field)
                for field in (*_IDENTITY_FIELDS, "type", "name", "primary_exchange")
            },
        }
        records.append(current)
        return

    current["last_active_date"] = snapshot_date
    for field in (*_IDENTITY_FIELDS, "type", "name", "primary_exchange"):
        if row.get(field) is not None:
            current[field] = row[field]


def _matching_lifecycle(
    records: list[dict[str, Any]],
    row: dict[str, Any],
) -> dict[str, Any] | None:
    if not records:
        return None
    populated_fields = [field for field in _IDENTITY_FIELDS if row.get(field) is not None]
    if not populated_fields:
        return max(records, key=lambda record: record["last_active_date"])

    matches: list[tuple[int, date, dict[str, Any]]] = []
    for record in records:
        if _has_identity_conflict(record, row):
            continue
        score = sum(
            weight
            for weight, field in enumerate(reversed(_IDENTITY_FIELDS), start=1)
            if row.get(field) is not None and row.get(field) == record.get(field)
        )
        if score:
            matches.append((score, record["last_active_date"], record))
    if matches:
        return max(matches, key=lambda item: (item[0], item[1]))[2]

    identityless = [
        record
        for record in records
        if all(record.get(field) is None for field in _IDENTITY_FIELDS)
    ]
    if identityless:
        return max(identityless, key=lambda record: record["last_active_date"])
    return None


def _has_identity_conflict(record: dict[str, Any], row: dict[str, Any]) -> bool:
    return any(
        record.get(field) is not None
        and row.get(field) is not None
        and record[field] != row[field]
        for field in _IDENTITY_FIELDS
    )


def _finalize_lifecycle(record: dict[str, Any]) -> dict[str, Any]:
    identity_type = next((field for field in _IDENTITY_FIELDS if record.get(field)), "ticker")
    identity_value = str(record.get(identity_type) or record["ticker"])
    lifecycle_id = stable_digest(
        {
            "first_active_date": record["first_active_date"].isoformat(),
            "identity_type": identity_type,
            "identity_value": identity_value,
            "ticker": record["ticker"],
        }
    )
    return {
        **record,
        "lifecycle_id": lifecycle_id,
        "query_date": record["last_active_date"],
        "identity_type": identity_type,
        "identity_value": identity_value,
    }


def _request_csv(rows: list[tuple[str, date]]) -> bytes:
    lines = ["ticker,query_date\n"]
    for ticker, query_date in rows:
        # Massive ticker identifiers cannot contain CSV control characters. Refuse rather than
        # produce a request file whose row boundaries differ from the reviewed lifecycle table.
        if any(character in ticker for character in (",", "\r", "\n")):
            raise MaterializationError("ticker cannot be represented in the request CSV")
        lines.append(f"{ticker},{query_date.isoformat()}\n")
    return "".join(lines).encode("utf-8")


def _overview_source_entry(
    reader: BronzeReader,
    request: ProviderRequest,
) -> dict[str, object]:
    path = reader.manifest_path(request)
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise BronzeStorageError(f"Bronze manifest is missing: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise BronzeStorageError(f"cannot read Bronze manifest: {path}") from exc
    if not isinstance(manifest, dict):
        raise BronzeStorageError("Bronze manifest root must be an object")
    if manifest.get("provider") != reader.provider_name:
        raise BronzeStorageError("Bronze manifest provider does not match reader")
    if manifest.get("request_id") != request.request_id:
        raise BronzeStorageError("Bronze manifest request_id does not match request")
    if manifest.get("request") != request.canonical_dict():
        raise BronzeStorageError("Bronze manifest request definition does not match request")
    status = manifest.get("status")
    if status == "complete":
        source = reader.source_entry(request)
        return {**source, "status": "complete"}
    if status == "failed":
        return {"request_id": request.request_id, "status": "failed"}
    raise BronzeStorageError(f"Bronze request is neither complete nor failed: {request.request_id}")


def _overview_result(reader: BronzeReader, request: ProviderRequest) -> dict[str, Any]:
    pages = reader.pages(request)
    if len(pages) != 1:
        raise MaterializationError("Ticker Overview must contain exactly one response page")
    result = pages[0].document.get("results")
    if not isinstance(result, dict):
        raise MaterializationError("Ticker Overview results must be an object")
    return result


def _safe_overview_row(
    lifecycle: dict[str, Any],
    *,
    request: ProviderRequest,
    result: dict[str, Any],
) -> dict[str, Any]:
    response_identities = {
        field: _clean_string(result.get(field)) for field in _IDENTITY_FIELDS
    }
    expected_type = str(lifecycle["identity_type"])
    expected_value = str(lifecycle["identity_value"])
    identity_match, identity_match_basis = _identity_match(
        lifecycle,
        response_identities=response_identities,
        response_ticker=_clean_string(result.get("ticker")),
    )
    return {
        "lifecycle_id": lifecycle["lifecycle_id"],
        "source_request_id": request.request_id,
        "query_ticker": lifecycle["ticker"],
        "query_date": lifecycle["query_date"],
        "first_active_date": lifecycle["first_active_date"],
        "last_active_date": lifecycle["last_active_date"],
        "identity_type": expected_type,
        "identity_value": expected_value,
        "identity_match": identity_match,
        "identity_match_basis": identity_match_basis,
        "ticker": _clean_string(result.get("ticker")),
        "name": _clean_string(result.get("name")),
        "type": _clean_string(result.get("type")),
        "market": _clean_string(result.get("market")),
        "locale": _clean_string(result.get("locale")),
        "active": result.get("active"),
        "primary_exchange": _clean_string(result.get("primary_exchange")),
        "currency_name": _clean_string(result.get("currency_name")),
        **response_identities,
        "sic_code": _clean_string(result.get("sic_code")),
        "sic_description": _clean_string(result.get("sic_description")),
        "list_date": _parse_iso_date(result.get("list_date"), field="list_date"),
        "delisted_utc": _clean_string(result.get("delisted_utc")),
        "ticker_root": _clean_string(result.get("ticker_root")),
        "ticker_suffix": _clean_string(result.get("ticker_suffix")),
    }


def _identity_match(
    lifecycle: dict[str, Any],
    *,
    response_identities: dict[str, str | None],
    response_ticker: str | None,
) -> tuple[bool, str | None]:
    if lifecycle["identity_type"] == "ticker":
        matches = response_ticker == _clean_string(lifecycle.get("ticker"))
        return matches, "ticker" if matches else None

    comparable: list[tuple[str, bool]] = []
    for field in _IDENTITY_FIELDS:
        expected = _clean_string(lifecycle.get(field))
        actual = response_identities[field]
        if expected is not None and actual is not None:
            comparable.append((field, expected == actual))
    if not comparable or any(not matches for _, matches in comparable):
        return False, None
    matching_fields = {field for field, matches in comparable if matches}
    basis = next(field for field in _IDENTITY_FIELDS if field in matching_fields)
    return True, basis


def _load_complete_artifact_manifest(root: Path, path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ArtifactError(f"artifact manifest is missing: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"cannot read artifact manifest: {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise ArtifactError(f"artifact manifest is not complete: {path}")
    if manifest.get("schema_version") != TICKER_LIFECYCLE_SCHEMA_VERSION:
        raise ArtifactError(f"artifact manifest schema is incompatible: {path}")
    verify_outputs(root, manifest.get("outputs"))
    return manifest


def _require_columns(frame: pl.DataFrame, expected: set[str], label: str) -> None:
    missing = sorted(expected - set(frame.columns))
    if missing:
        raise MaterializationError(f"{label} is missing columns: {', '.join(missing)}")


def _clean_string(value: object) -> str | None:
    if value is None:
        return None
    clean = str(value).strip()
    return clean or None


def _parse_iso_date(value: object, *, field: str) -> date | None:
    clean = _clean_string(value)
    if clean is None:
        return None
    try:
        return date.fromisoformat(clean)
    except ValueError as exc:
        raise MaterializationError(f"Ticker Overview {field} is not an ISO date") from exc
