"""Manifest-bound formal source rows for the S6 Ticker Overview table."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, write_bytes_immutable
from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)
from ame_stocks_api.silver.ticker_overview_source_profile import (
    PRODUCTION_COVERAGE_RECEIPT_NAMESPACE,
    TickerOverviewSourceProfileError,
    _safe_row,
    _verify_manifest_and_payload,
    lifecycle_plan_content,
    validate_ticker_overview_coverage_receipt,
)
from ame_stocks_core import ProviderDataset

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TickerOverviewSourceError(SilverContractError):
    """Raised when accepted S6 coverage cannot be reproduced exactly."""


@dataclass(frozen=True, slots=True)
class TickerOverviewSourceRecord:
    """One lifecycle/Overview pair plus exact manifest and page lineage."""

    values: Mapping[str, object]

    def __post_init__(self) -> None:
        detached = _detached_mapping(self.values, "source record")
        required = {
            "lifecycle_id",
            "source_request_id",
            "query_ticker",
            "query_date",
            "first_active_date",
            "last_active_date",
            "identity_type",
            "identity_value",
            "identity_match",
            "identity_match_basis",
            "identity_evidence_status",
            "ticker",
            "name",
            "type",
            "market",
            "locale",
            "active",
            "primary_exchange",
            "currency_name",
            "cik",
            "composite_figi",
            "share_class_figi",
            "sic_code",
            "sic_description",
            "list_date",
            "delisted_utc",
            "ticker_root",
            "ticker_suffix",
            "source_manifest_created_at_utc",
            "source_capture_at_utc",
            "source_manifest_path",
            "source_manifest_sha256",
            "source_artifact_path",
            "source_artifact_sha256",
            "source_artifact_raw_sha256",
            "source_page_sequence",
            "source_row_ordinal",
            "source_provider_request_id",
            "source_result_hash",
        }
        if set(detached) != required:
            raise TickerOverviewSourceError("ticker overview source record fields changed")
        object.__setattr__(self, "values", detached)

    def to_transform_input(self) -> dict[str, object]:
        return dict(self.values)


@dataclass(frozen=True, slots=True)
class TickerOverviewSourceBatch:
    records: tuple[TickerOverviewSourceRecord, ...]
    coverage_receipt_id: str
    lifecycle_plan_path: str

    def __post_init__(self) -> None:
        records = tuple(
            sorted(self.records, key=lambda item: str(item.values["source_request_id"]))
        )
        if not records:
            raise TickerOverviewSourceError("ticker overview source batch cannot be empty")
        request_ids = [str(item.values["source_request_id"]) for item in records]
        lifecycle_ids = [str(item.values["lifecycle_id"]) for item in records]
        if len(set(request_ids)) != len(records) or len(set(lifecycle_ids)) != len(records):
            raise TickerOverviewSourceError("ticker overview source keys are duplicated")
        _sha256_text(self.coverage_receipt_id, "coverage receipt ID")
        _relative_path(self.lifecycle_plan_path, "lifecycle plan path")
        object.__setattr__(self, "records", records)

    @property
    def row_count(self) -> int:
        return len(self.records)

    def iter_records(self) -> Iterator[TickerOverviewSourceRecord]:
        return iter(self.records)


def ticker_overview_coverage_receipt_path(receipt: Mapping[str, object]) -> str:
    try:
        validated = validate_ticker_overview_coverage_receipt(dict(receipt))
    except TickerOverviewSourceProfileError as exc:
        raise TickerOverviewSourceError(str(exc)) from exc
    return (
        f"{PRODUCTION_COVERAGE_RECEIPT_NAMESPACE}/coverage-{validated['coverage_receipt_id']}.json"
    )


def load_ticker_overview_coverage_receipt(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
) -> dict[str, object]:
    root = data_root.expanduser().resolve()
    _sha256_text(coverage_receipt_sha256, "coverage receipt SHA-256")
    relative = Path(coverage_receipt_path)
    if (
        relative.is_absolute()
        or relative.parent.as_posix() != PRODUCTION_COVERAGE_RECEIPT_NAMESPACE
    ):
        raise TickerOverviewSourceError("ticker overview coverage receipt path is not canonical")
    try:
        content = safe_relative_path(root, coverage_receipt_path).read_bytes()
        document = json.loads(content, parse_constant=_reject_constant)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise TickerOverviewSourceError("cannot read ticker overview coverage receipt") from exc
    if hashlib.sha256(content).hexdigest() != coverage_receipt_sha256:
        raise TickerOverviewSourceError("ticker overview coverage receipt checksum mismatch")
    try:
        receipt = validate_ticker_overview_coverage_receipt(document)
    except TickerOverviewSourceProfileError as exc:
        raise TickerOverviewSourceError(str(exc)) from exc
    if relative.name != f"coverage-{receipt['coverage_receipt_id']}.json":
        raise TickerOverviewSourceError("ticker overview coverage receipt filename/ID mismatch")
    return receipt


def ticker_overview_lifecycle_plan_bytes(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
) -> bytes:
    """Rebuild the one-artifact lifecycle carrier from receipt-bound v2 Parquet."""

    root = data_root.expanduser().resolve()
    receipt = load_ticker_overview_coverage_receipt(
        root,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
    )
    lifecycle = _mapping(receipt["lifecycle"], "lifecycle")
    parquet_ref = _mapping(lifecycle["parquet"], "lifecycle parquet")
    content = _verified_content(root, parquet_ref, label="lifecycle parquet")
    try:
        rows = pq.ParquetFile(pa.BufferReader(content)).read().to_pylist()
    except pa.ArrowException as exc:
        raise TickerOverviewSourceError("cannot read receipt-bound lifecycle Parquet") from exc
    plan_content = lifecycle_plan_content(rows)
    plan = _mapping(receipt["lifecycle_plan"], "lifecycle plan")
    if (
        len(plan_content) != plan.get("bytes")
        or hashlib.sha256(plan_content).hexdigest() != plan.get("sha256")
        or len(rows) != plan.get("row_count")
    ):
        raise TickerOverviewSourceError("reconstructed lifecycle plan differs from receipt")
    return plan_content


def build_ticker_overview_lifecycle_source_inventory(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
    git_commit: str,
) -> SourceInventory:
    """Register the sole 30,739-row direct build input under manifests/plans."""

    root = data_root.expanduser().resolve()
    receipt = load_ticker_overview_coverage_receipt(
        root,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
    )
    plan = _mapping(receipt["lifecycle_plan"], "lifecycle plan")
    plan_content = ticker_overview_lifecycle_plan_bytes(
        root,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
    )
    stored = write_bytes_immutable(root, root / str(plan["path"]), plan_content)
    if stored["sha256"] != plan.get("sha256") or stored["bytes"] != plan.get("bytes"):
        raise TickerOverviewSourceError("stored lifecycle plan differs from receipt")
    _verified_content(root, plan, label="lifecycle plan")
    return SourceInventory(
        source_dataset="ticker_overview",
        source_layer=SourceLayer.CONTROL_MANIFEST,
        git_commit=git_commit,
        upstream_manifests=(
            UpstreamManifestRef(path=coverage_receipt_path, sha256=coverage_receipt_sha256),
        ),
        artifacts=(
            SourceInventoryItem(
                path=str(plan["path"]),
                sha256=str(plan["sha256"]),
                bytes=_native_int(plan["bytes"], "lifecycle plan bytes"),
                row_count=_native_int(plan["row_count"], "lifecycle plan rows"),
                media_type="text/plain",
            ),
        ),
    )


def build_ticker_overview_source_inventory(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
    git_commit: str,
) -> SourceInventory:
    """Build an audit-only inventory of every receipt-bound Bronze page."""

    root = data_root.expanduser().resolve()
    receipt = load_ticker_overview_coverage_receipt(
        root,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
    )
    artifacts = tuple(
        SourceInventoryItem(
            path=str(item["path"]),
            sha256=str(item["sha256"]),
            bytes=_native_int(item["bytes"], "Bronze bytes"),
            row_count=_native_int(item["row_count"], "Bronze rows"),
            media_type="application/gzip+json",
        )
        for item in (_mapping(value, "Bronze artifact") for value in receipt["artifacts"])
    )
    manifest_refs = tuple(
        UpstreamManifestRef(
            path=str(item["path"]),
            sha256=str(item["sha256"]),
        )
        for item in (
            _mapping(value, "Bronze manifest ref") for value in receipt["manifest_refs"]
        )
    )
    if len(manifest_refs) != len(artifacts):
        raise TickerOverviewSourceError("ticker overview manifest/artifact coverage changed")
    for item in artifacts:
        content = safe_relative_path(root, item.path).read_bytes()
        if len(content) != item.bytes or hashlib.sha256(content).hexdigest() != item.sha256:
            raise TickerOverviewSourceError("ticker overview Bronze inventory checksum changed")
    return SourceInventory(
        source_dataset="ticker_overview",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=(
            UpstreamManifestRef(path=coverage_receipt_path, sha256=coverage_receipt_sha256),
            *manifest_refs,
        ),
        artifacts=artifacts,
    )


def read_ticker_overview_source_inventory(
    data_root: Path,
    overview_inventory: SourceInventory,
    *,
    lifecycle_inventory: SourceInventory,
) -> TickerOverviewSourceBatch:
    """Reverify plan, manifests and pages and return detached transform inputs."""

    root = data_root.expanduser().resolve()
    _require_inventory_pair(overview_inventory, lifecycle_inventory)
    upstream = lifecycle_inventory.upstream_manifests[0]
    receipt = load_ticker_overview_coverage_receipt(
        root,
        coverage_receipt_path=upstream.path,
        coverage_receipt_sha256=upstream.sha256,
    )
    plan_item = lifecycle_inventory.artifacts[0]
    plan_content = safe_relative_path(root, plan_item.path).read_bytes()
    if (
        len(plan_content) != plan_item.bytes
        or hashlib.sha256(plan_content).hexdigest() != plan_item.sha256
    ):
        raise TickerOverviewSourceError("ticker overview lifecycle plan checksum changed")
    lifecycle_rows = _parse_lifecycle_plan(plan_content)
    plan = _mapping(receipt["lifecycle_plan"], "lifecycle plan")
    if len(lifecycle_rows) != plan.get("row_count"):
        raise TickerOverviewSourceError("ticker overview lifecycle plan row count changed")
    window = _mapping(receipt["window"], "window")
    try:
        start = date.fromisoformat(str(window["start"]))
        end = date.fromisoformat(str(window["end"]))
    except (KeyError, ValueError) as exc:
        raise TickerOverviewSourceError("ticker overview receipt window is invalid") from exc
    ticker_dates = tuple((str(row["ticker"]), row["query_date"]) for row in lifecycle_rows)
    requests = {
        request.request_id: request
        for request in build_download_plan(
            dataset=ProviderDataset.TICKER_OVERVIEW,
            start=start,
            end=end,
            ticker_dates=ticker_dates,
        ).requests
    }
    refs = receipt["manifest_refs"]
    if not isinstance(refs, list) or len(refs) != len(requests):
        raise TickerOverviewSourceError("ticker overview receipt manifest cardinality changed")
    lifecycle_by_pair = {(str(row["ticker"]), row["query_date"]): row for row in lifecycle_rows}
    records: list[TickerOverviewSourceRecord] = []
    for raw_ref in refs:
        ref = _mapping(raw_ref, "manifest ref")
        request_id = str(ref["request_id"])
        try:
            request = requests[request_id]
        except KeyError as exc:
            raise TickerOverviewSourceError(
                "receipt request ID differs from lifecycle plan"
            ) from exc
        verified = _verify_manifest_and_payload(
            root,
            safe_relative_path(root, str(ref["path"])),
            request_id=request_id,
            expected_request=request.canonical_dict(),
        )
        expected_ref = {
            "artifact": verified["artifact"],
            "completed_at": verified["completed_at"],
            "created_at": verified["created_at"],
            "path": verified["manifest_path"],
            "query_date": request.start.isoformat(),
            "query_ticker": request.asset_ids[0],
            "request_id": request_id,
            "sha256": verified["manifest_sha256"],
            "updated_at": verified["updated_at"],
        }
        if expected_ref != ref:
            raise TickerOverviewSourceError("raw ticker overview source differs from receipt")
        lifecycle = lifecycle_by_pair[(request.asset_ids[0], request.start)]
        safe = _safe_row(lifecycle, request_id=request_id, result=verified["result"])
        artifact = _mapping(verified["artifact"], "verified artifact")
        values = {
            **safe,
            "source_manifest_created_at_utc": verified["created_at"],
            "source_capture_at_utc": verified["completed_at"],
            "source_manifest_path": verified["manifest_path"],
            "source_manifest_sha256": verified["manifest_sha256"],
            "source_artifact_path": artifact["path"],
            "source_artifact_sha256": artifact["sha256"],
            "source_artifact_raw_sha256": artifact["raw_sha256"],
            "source_page_sequence": 0,
            "source_row_ordinal": 0,
            "source_provider_request_id": verified["provider_request_id"],
            "source_result_hash": verified["result_hash"],
        }
        records.append(TickerOverviewSourceRecord(values=values))
    batch = TickerOverviewSourceBatch(
        records=tuple(records),
        coverage_receipt_id=str(receipt["coverage_receipt_id"]),
        lifecycle_plan_path=plan_item.path,
    )
    if batch.row_count != len(lifecycle_rows):
        raise TickerOverviewSourceError("verified ticker overview batch cardinality changed")
    return batch


def ticker_overview_transform_inputs(
    batch: TickerOverviewSourceBatch,
) -> tuple[dict[str, object], ...]:
    return tuple(record.to_transform_input() for record in batch.records)


def _require_inventory_pair(overview: SourceInventory, lifecycle: SourceInventory) -> None:
    lifecycle_receipts = set(lifecycle.upstream_manifests)
    overview_upstreams = set(overview.upstream_manifests)
    if (
        overview.source_dataset != "ticker_overview"
        or lifecycle.source_dataset != "ticker_overview"
        or overview.source_layer is not SourceLayer.BRONZE
        or lifecycle.source_layer is not SourceLayer.CONTROL_MANIFEST
        or overview.git_commit != lifecycle.git_commit
        or len(lifecycle.upstream_manifests) != 1
        or not lifecycle_receipts.issubset(overview_upstreams)
        or len(overview_upstreams) != len(overview.artifacts) + 1
        or len(lifecycle.artifacts) != 1
        or lifecycle.artifacts[0].media_type != "text/plain"
    ):
        raise TickerOverviewSourceError("ticker overview inventories do not share exact lineage")
    declared = {(item.path, item.sha256, item.bytes, item.row_count) for item in overview.artifacts}
    if len(declared) != len(overview.artifacts) or any(
        item.media_type != "application/gzip+json" or item.row_count != 1
        for item in overview.artifacts
    ):
        raise TickerOverviewSourceError("ticker overview Bronze inventory grain changed")


def _parse_lifecycle_plan(content: bytes) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    try:
        lines = content.decode("utf-8").splitlines()
        for line in lines:
            raw = json.loads(line, parse_constant=_reject_constant)
            if not isinstance(raw, dict):
                raise ValueError("row")
            for field in ("first_active_date", "last_active_date", "query_date"):
                raw[field] = date.fromisoformat(str(raw[field]))
            rows.append(raw)
    except (UnicodeDecodeError, KeyError, TypeError, ValueError) as exc:
        raise TickerOverviewSourceError("ticker overview lifecycle plan is invalid") from exc
    if not rows or len({str(row["lifecycle_id"]) for row in rows}) != len(rows):
        raise TickerOverviewSourceError("ticker overview lifecycle plan keys are invalid")
    return rows


def _verified_content(root: Path, ref: dict[str, object], *, label: str) -> bytes:
    path = str(ref.get("path"))
    content = safe_relative_path(root, path).read_bytes()
    if len(content) != ref.get("bytes") or hashlib.sha256(content).hexdigest() != ref.get("sha256"):
        raise TickerOverviewSourceError(f"ticker overview {label} checksum changed")
    return content


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TickerOverviewSourceError(f"ticker overview {label} must be an object")
    return dict(value)


def _detached_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TickerOverviewSourceError(f"ticker overview {label} must be an object")
    detached = dict(value)
    if any(isinstance(item, (dict, list, set, tuple)) for item in detached.values()):
        raise TickerOverviewSourceError(f"ticker overview {label} contains nested values")
    return MappingProxyType(detached)


def _relative_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or Path(value).is_absolute()
        or value != value.strip()
    ):
        raise TickerOverviewSourceError(f"ticker overview {label} is invalid")
    return value


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise TickerOverviewSourceError(f"ticker overview {label} is not a SHA-256")
    return value


def _native_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise TickerOverviewSourceError(f"ticker overview {label} is invalid")
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


__all__ = [
    "TickerOverviewSourceBatch",
    "TickerOverviewSourceError",
    "TickerOverviewSourceRecord",
    "build_ticker_overview_lifecycle_source_inventory",
    "build_ticker_overview_source_inventory",
    "load_ticker_overview_coverage_receipt",
    "read_ticker_overview_source_inventory",
    "ticker_overview_coverage_receipt_path",
    "ticker_overview_lifecycle_plan_bytes",
    "ticker_overview_transform_inputs",
]
