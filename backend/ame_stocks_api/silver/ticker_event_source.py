"""Accepted-coverage, manifest-bound inputs for the S5 ticker-event transforms."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ame_stocks_api.artifacts import safe_relative_path
from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)
from ame_stocks_api.silver.ticker_event_source_profile import (
    PRODUCTION_COVERAGE_RECEIPT_NAMESPACE,
    TickerEventSourceProfileError,
    _date_target_quality,
    _verify_manifest_and_payload,
    validate_ticker_event_coverage_receipt,
)
from ame_stocks_core import ProviderDataset

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TickerEventSourceError(SilverContractError):
    """Raised before S5 transformation when accepted coverage cannot be reproduced."""


@dataclass(frozen=True, slots=True)
class TickerEventCoverageReceipt:
    """One immutable accepted-status receipt binding all formal terminal requests."""

    source_path: str
    source_sha256: str
    coverage_receipt_id: str
    formal_identifier_receipt_path: str
    formal_identifier_receipt_sha256: str
    pilot_identifier_receipt_path: str
    pilot_identifier_receipt_sha256: str
    request_start: date
    request_end: date
    manifest_refs: tuple[Mapping[str, object], ...]
    artifacts: tuple[Mapping[str, object], ...]
    diagnostics: Mapping[str, object]

    def __post_init__(self) -> None:
        _relative_path(self.source_path, "coverage receipt path")
        _sha256_text(self.source_sha256, "coverage receipt SHA-256")
        _sha256_text(self.coverage_receipt_id, "coverage receipt ID")
        _relative_path(self.formal_identifier_receipt_path, "formal identifier receipt path")
        _sha256_text(
            self.formal_identifier_receipt_sha256,
            "formal identifier receipt SHA-256",
        )
        _relative_path(self.pilot_identifier_receipt_path, "pilot identifier receipt path")
        _sha256_text(
            self.pilot_identifier_receipt_sha256,
            "pilot identifier receipt SHA-256",
        )
        if type(self.request_start) is not date or type(self.request_end) is not date:
            raise TickerEventSourceError("ticker-event receipt request dates are invalid")
        if self.request_start > self.request_end:
            raise TickerEventSourceError("ticker-event receipt request window is reversed")
        manifests = tuple(
            _detached_mapping(item, "coverage manifest ref") for item in self.manifest_refs
        )
        artifacts = tuple(_detached_mapping(item, "coverage artifact") for item in self.artifacts)
        diagnostics = _detached_mapping(self.diagnostics, "coverage diagnostics")
        if not manifests or not artifacts:
            raise TickerEventSourceError("ticker-event receipt cannot have empty formal coverage")
        object.__setattr__(self, "manifest_refs", manifests)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "diagnostics", diagnostics)

    @property
    def request_count(self) -> int:
        return len(self.manifest_refs)

    @property
    def complete_count(self) -> int:
        return sum(item["status"] == "complete" for item in self.manifest_refs)

    @property
    def not_found_count(self) -> int:
        return sum(item["status"] == "not_found_404" for item in self.manifest_refs)

    @property
    def event_count(self) -> int:
        return sum(int(item["row_count"]) for item in self.artifacts)


@dataclass(frozen=True, slots=True)
class TickerEventRequestStatus:
    """One formal request outcome, including reviewed terminal HTTP 404 coverage."""

    requested_identifier: str
    source_request_id: str
    source_manifest_path: str
    source_manifest_sha256: str
    source_created_at_utc: datetime
    source_updated_at_utc: datetime
    source_capture_at_utc: datetime | None
    outcome: str
    provider_status_code: int | None
    event_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.requested_identifier, str) or not self.requested_identifier:
            raise TickerEventSourceError("ticker-event requested identifier is missing")
        _sha256_text(self.source_request_id, "request ID")
        _relative_path(self.source_manifest_path, "manifest path")
        _sha256_text(self.source_manifest_sha256, "manifest SHA-256")
        for label in ("source_created_at_utc", "source_updated_at_utc"):
            value = getattr(self, label)
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TickerEventSourceError(f"ticker-event {label} must be timezone-aware")
            object.__setattr__(self, label, value.astimezone(UTC))
        captured = self.source_capture_at_utc
        if captured is not None:
            if not isinstance(captured, datetime) or captured.tzinfo is None:
                raise TickerEventSourceError("ticker-event capture time must be timezone-aware")
            object.__setattr__(self, "source_capture_at_utc", captured.astimezone(UTC))
        if self.outcome not in {"complete", "not_found_404"}:
            raise TickerEventSourceError("ticker-event request outcome is not accepted")
        if self.outcome == "complete":
            if self.provider_status_code is not None or captured is None:
                raise TickerEventSourceError("complete ticker-event status fields are inconsistent")
        elif self.provider_status_code != 404 or captured is not None:
            raise TickerEventSourceError("404 ticker-event status fields are inconsistent")
        _native_nonnegative_int(self.event_count, "event_count")
        if self.outcome == "not_found_404" and self.event_count != 0:
            raise TickerEventSourceError("404 ticker-event request cannot have events")


@dataclass(frozen=True, slots=True)
class TickerEventSourcePage:
    """One fully verified Massive response and its detached event rows."""

    source_path: str
    source_artifact_sha256: str
    raw_sha256: str
    sequence: int
    compressed_bytes: int
    raw_bytes: int
    record_count: int
    source_provider_request_id: str
    result_name: str
    result_cik: str | None
    result_composite_figi: str
    result_hash: str
    rows: tuple[Mapping[str, object], ...]

    def __post_init__(self) -> None:
        _relative_path(self.source_path, "source page path")
        _sha256_text(self.source_artifact_sha256, "source artifact SHA-256")
        _sha256_text(self.raw_sha256, "raw SHA-256")
        _sha256_text(self.result_hash, "result hash")
        _native_nonnegative_int(self.sequence, "page sequence")
        _native_nonnegative_int(self.compressed_bytes, "compressed_bytes")
        _native_nonnegative_int(self.raw_bytes, "raw_bytes")
        _native_nonnegative_int(self.record_count, "record_count")
        for label, value in (
            ("provider request ID", self.source_provider_request_id),
            ("result name", self.result_name),
            ("result Composite FIGI", self.result_composite_figi),
        ):
            if not isinstance(value, str) or not value or value != value.strip():
                raise TickerEventSourceError(f"ticker-event {label} is invalid")
        if self.result_cik is not None and (
            not isinstance(self.result_cik, str) or not self.result_cik
        ):
            raise TickerEventSourceError("ticker-event result CIK is invalid")
        rows = tuple(_detached_mapping(row, "ticker-event row") for row in self.rows)
        if len(rows) != self.record_count:
            raise TickerEventSourceError("ticker-event page row count changed")
        object.__setattr__(self, "rows", rows)


@dataclass(frozen=True, slots=True)
class TickerEventSourceSnapshot:
    """One successful formal Composite-FIGI request."""

    requested_identifier: str
    source_request_id: str
    source_manifest_path: str
    source_manifest_sha256: str
    source_created_at_utc: datetime
    source_capture_at_utc: datetime
    source_updated_at_utc: datetime
    pages: tuple[TickerEventSourcePage, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.requested_identifier, str) or not self.requested_identifier:
            raise TickerEventSourceError("ticker-event requested identifier is missing")
        _sha256_text(self.source_request_id, "request ID")
        _relative_path(self.source_manifest_path, "manifest path")
        _sha256_text(self.source_manifest_sha256, "manifest SHA-256")
        for label in (
            "source_created_at_utc",
            "source_capture_at_utc",
            "source_updated_at_utc",
        ):
            value = getattr(self, label)
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TickerEventSourceError(f"ticker-event {label} must be timezone-aware")
            object.__setattr__(self, label, value.astimezone(UTC))
        pages = tuple(self.pages)
        if len(pages) != 1 or pages[0].sequence != 0:
            raise TickerEventSourceError("ticker-event snapshot requires one sequence-zero page")
        if pages[0].result_composite_figi != self.requested_identifier:
            raise TickerEventSourceError("ticker-event snapshot FIGI differs from request")
        object.__setattr__(self, "pages", pages)


@dataclass(frozen=True, slots=True)
class TickerEventSourceRecord:
    """One event plus complete response/manifest lineage."""

    requested_identifier: str
    source_request_id: str
    source_manifest_path: str
    source_manifest_sha256: str
    source_created_at_utc: datetime
    source_capture_at_utc: datetime
    source_updated_at_utc: datetime
    source_artifact_path: str
    source_artifact_sha256: str
    source_page_sequence: int
    source_row_ordinal: int
    source_provider_request_id: str
    result_name: str
    result_cik: str | None
    result_composite_figi: str
    result_hash: str
    row: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TickerEventSourceBatch:
    """The complete accepted formal scope: successes plus terminal 404 status rows."""

    snapshots: tuple[TickerEventSourceSnapshot, ...]
    request_statuses: tuple[TickerEventRequestStatus, ...]
    request_start: date
    request_end: date

    def __post_init__(self) -> None:
        snapshots = tuple(sorted(self.snapshots, key=lambda item: item.source_request_id))
        statuses = tuple(sorted(self.request_statuses, key=lambda item: item.source_request_id))
        if not snapshots or not statuses:
            raise TickerEventSourceError("ticker-event source batch cannot be empty")
        if len({item.source_request_id for item in statuses}) != len(statuses):
            raise TickerEventSourceError("ticker-event request statuses must be unique")
        complete_ids = {item.source_request_id for item in statuses if item.outcome == "complete"}
        if complete_ids != {item.source_request_id for item in snapshots}:
            raise TickerEventSourceError("ticker-event snapshots do not match complete statuses")
        object.__setattr__(self, "snapshots", snapshots)
        object.__setattr__(self, "request_statuses", statuses)

    @property
    def request_count(self) -> int:
        return len(self.request_statuses)

    @property
    def not_found_count(self) -> int:
        return sum(item.outcome == "not_found_404" for item in self.request_statuses)

    @property
    def page_count(self) -> int:
        return sum(len(snapshot.pages) for snapshot in self.snapshots)

    @property
    def row_count(self) -> int:
        return sum(page.record_count for snapshot in self.snapshots for page in snapshot.pages)

    @property
    def source_object_count(self) -> int:
        return self.request_count + self.page_count

    def iter_records(self) -> Iterator[TickerEventSourceRecord]:
        """Yield every event in deterministic request/page/provider order."""

        for snapshot in self.snapshots:
            for page in snapshot.pages:
                for ordinal, row in enumerate(page.rows):
                    yield TickerEventSourceRecord(
                        requested_identifier=snapshot.requested_identifier,
                        source_request_id=snapshot.source_request_id,
                        source_manifest_path=snapshot.source_manifest_path,
                        source_manifest_sha256=snapshot.source_manifest_sha256,
                        source_created_at_utc=snapshot.source_created_at_utc,
                        source_capture_at_utc=snapshot.source_capture_at_utc,
                        source_updated_at_utc=snapshot.source_updated_at_utc,
                        source_artifact_path=page.source_path,
                        source_artifact_sha256=page.source_artifact_sha256,
                        source_page_sequence=page.sequence,
                        source_row_ordinal=ordinal,
                        source_provider_request_id=page.source_provider_request_id,
                        result_name=page.result_name,
                        result_cik=page.result_cik,
                        result_composite_figi=page.result_composite_figi,
                        result_hash=page.result_hash,
                        row=row,
                    )


def ticker_event_coverage_receipt_path(receipt: Mapping[str, object]) -> str:
    """Return the canonical immutable path for a validated accepted receipt."""

    try:
        validated = validate_ticker_event_coverage_receipt(receipt)
    except TickerEventSourceProfileError as exc:
        raise TickerEventSourceError(str(exc)) from exc
    return (
        f"{PRODUCTION_COVERAGE_RECEIPT_NAMESPACE}/coverage-{validated['coverage_receipt_id']}.json"
    )


def load_ticker_event_coverage_receipt(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
) -> TickerEventCoverageReceipt:
    """Load one exact accepted receipt; pilot/raw manifests are not upstream substitutes."""

    root = data_root.expanduser().resolve()
    _sha256_text(coverage_receipt_sha256, "coverage receipt SHA-256")
    relative = Path(coverage_receipt_path)
    if (
        relative.is_absolute()
        or relative.parent.as_posix() != PRODUCTION_COVERAGE_RECEIPT_NAMESPACE
    ):
        raise TickerEventSourceError("ticker-event coverage receipt path is not canonical")
    try:
        content = safe_relative_path(root, coverage_receipt_path).read_bytes()
        document = json.loads(content, parse_constant=_reject_constant)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise TickerEventSourceError("cannot read ticker-event coverage receipt") from exc
    if hashlib.sha256(content).hexdigest() != coverage_receipt_sha256:
        raise TickerEventSourceError("ticker-event coverage receipt checksum mismatch")
    try:
        validated = validate_ticker_event_coverage_receipt(document)
    except TickerEventSourceProfileError as exc:
        raise TickerEventSourceError(str(exc)) from exc
    expected_name = f"coverage-{validated['coverage_receipt_id']}.json"
    if relative.name != expected_name:
        raise TickerEventSourceError("ticker-event coverage receipt filename/ID mismatch")
    formal = _mapping(validated["formal_identifier_receipt"], "formal receipt")
    pilot = _mapping(validated["pilot_exclusion"], "pilot exclusion")
    scope = _mapping(validated["request_scope"], "request scope")
    try:
        start = date.fromisoformat(str(scope["start_label_not_provider_filter"]))
        end = date.fromisoformat(str(scope["end_label_not_provider_filter"]))
    except (KeyError, ValueError) as exc:
        raise TickerEventSourceError("ticker-event receipt request dates are invalid") from exc
    return TickerEventCoverageReceipt(
        source_path=coverage_receipt_path,
        source_sha256=coverage_receipt_sha256,
        coverage_receipt_id=str(validated["coverage_receipt_id"]),
        formal_identifier_receipt_path=str(formal["path"]),
        formal_identifier_receipt_sha256=str(formal["sha256"]),
        pilot_identifier_receipt_path=str(pilot["path"]),
        pilot_identifier_receipt_sha256=str(pilot["sha256"]),
        request_start=start,
        request_end=end,
        manifest_refs=tuple(validated["formal_manifest_refs"]),
        artifacts=tuple(validated["artifacts"]),
        diagnostics=validated["diagnostics"],
    )


def build_ticker_event_source_inventory(
    data_root: Path,
    *,
    coverage_receipt_path: str,
    coverage_receipt_sha256: str,
    git_commit: str,
) -> SourceInventory:
    """Build the production S5 inventory from exactly one accepted coverage receipt.

    There is deliberately no ``manifest_paths`` fallback.  Raw successful/404 manifests are
    children bound by the accepted receipt, never independent accepted upstream manifests.
    """

    receipt = load_ticker_event_coverage_receipt(
        data_root,
        coverage_receipt_path=coverage_receipt_path,
        coverage_receipt_sha256=coverage_receipt_sha256,
    )
    inventory = _declared_inventory(receipt, git_commit=git_commit)
    root = data_root.expanduser().resolve()
    for item in inventory.artifacts:
        try:
            content = safe_relative_path(root, item.path).read_bytes()
        except OSError as exc:
            raise TickerEventSourceError(
                f"cannot read ticker-event Bronze page: {item.path}"
            ) from exc
        if len(content) != item.bytes:
            raise TickerEventSourceError("ticker-event Bronze compressed byte count mismatch")
        if hashlib.sha256(content).hexdigest() != item.sha256:
            raise TickerEventSourceError("ticker-event Bronze stored checksum mismatch")
    return inventory


def read_ticker_event_source_inventory(
    data_root: Path,
    inventory: SourceInventory,
) -> TickerEventSourceBatch:
    """Reverify the receipt, every formal manifest and every successful page, then detach rows."""

    if (
        inventory.source_dataset != "ticker_events"
        or inventory.source_layer is not SourceLayer.BRONZE
        or len(inventory.upstream_manifests) != 1
    ):
        raise TickerEventSourceError(
            "ticker-event input must use one accepted Bronze coverage receipt"
        )
    upstream = inventory.upstream_manifests[0]
    rebuilt = build_ticker_event_source_inventory(
        data_root,
        coverage_receipt_path=upstream.path,
        coverage_receipt_sha256=upstream.sha256,
        git_commit=inventory.git_commit,
    )
    if rebuilt.to_dict() != inventory.to_dict():
        raise TickerEventSourceError(
            "ticker-event source inventory differs from accepted coverage receipt"
        )
    receipt = load_ticker_event_coverage_receipt(
        data_root,
        coverage_receipt_path=upstream.path,
        coverage_receipt_sha256=upstream.sha256,
    )
    return _build_verified_batch(data_root.expanduser().resolve(), receipt)


def ticker_event_request_transform_inputs(
    batch: TickerEventSourceBatch,
) -> tuple[dict[str, object], ...]:
    """Adapt every formal request outcome, including terminal 404s, for the status table."""

    snapshots = {item.source_request_id: item for item in batch.snapshots}
    return tuple(
        {
            "event_count": item.event_count,
            "outcome": item.outcome,
            "provider_status_code": item.provider_status_code,
            "requested_identifier": item.requested_identifier,
            "result_cik": (
                None
                if item.outcome == "not_found_404"
                else snapshots[item.source_request_id].pages[0].result_cik
            ),
            "result_composite_figi": (
                None
                if item.outcome == "not_found_404"
                else snapshots[item.source_request_id].pages[0].result_composite_figi
            ),
            "result_name": (
                None
                if item.outcome == "not_found_404"
                else snapshots[item.source_request_id].pages[0].result_name
            ),
            "source_artifact_sha256": (
                None
                if item.outcome == "not_found_404"
                else snapshots[item.source_request_id].pages[0].source_artifact_sha256
            ),
            "source_capture_at_utc": (
                None
                if item.source_capture_at_utc is None
                else item.source_capture_at_utc.isoformat()
            ),
            "source_created_at_utc": item.source_created_at_utc.isoformat(),
            "source_manifest_path": item.source_manifest_path,
            "source_manifest_sha256": item.source_manifest_sha256,
            "source_page_count": 0 if item.outcome == "not_found_404" else 1,
            "source_provider_request_id": (
                None
                if item.outcome == "not_found_404"
                else snapshots[item.source_request_id].pages[0].source_provider_request_id
            ),
            "source_request_id": item.source_request_id,
            "source_updated_at_utc": item.source_updated_at_utc.isoformat(),
        }
        for item in batch.request_statuses
    )


def ticker_event_occurrence_transform_inputs(
    batch: TickerEventSourceBatch,
) -> tuple[dict[str, object], ...]:
    """Adapt verified event occurrences to stable dictionaries for the event table."""

    output: list[dict[str, object]] = []
    for record in batch.iter_records():
        event_date = str(record.row["date"])
        ticker_change = _mapping(record.row["ticker_change"], "ticker_change")
        target = str(ticker_change["ticker"])
        quality = _date_target_quality(
            event_date,
            target,
            request_start=batch.request_start,
            request_end=batch.request_end,
        )
        output.append(
            {
                "date_quality": quality,
                "event_date_raw": event_date,
                "event_type": record.row["type"],
                "requested_identifier": record.requested_identifier,
                "result_cik": record.result_cik,
                "result_composite_figi": record.result_composite_figi,
                "result_name": record.result_name,
                "source_artifact_path": record.source_artifact_path,
                "source_artifact_sha256": record.source_artifact_sha256,
                "source_capture_at_utc": record.source_capture_at_utc.isoformat(),
                "source_manifest_path": record.source_manifest_path,
                "source_manifest_sha256": record.source_manifest_sha256,
                "source_page_sequence": record.source_page_sequence,
                "source_provider_request_id": record.source_provider_request_id,
                "source_request_id": record.source_request_id,
                "source_row_ordinal": record.source_row_ordinal,
                "source_event_hash": record.row["source_event_hash"],
                "source_result_hash": record.result_hash,
                "target_ticker_raw": target,
            }
        )
    return tuple(output)


def ticker_event_transform_inputs(
    batch: TickerEventSourceBatch,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    """Return ``(request_status_inputs, event_occurrence_inputs)`` for the pure transform."""

    return (
        ticker_event_request_transform_inputs(batch),
        ticker_event_occurrence_transform_inputs(batch),
    )


def _declared_inventory(
    receipt: TickerEventCoverageReceipt,
    *,
    git_commit: str,
) -> SourceInventory:
    return SourceInventory(
        source_dataset="ticker_events",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=(
            UpstreamManifestRef(path=receipt.source_path, sha256=receipt.source_sha256),
        ),
        artifacts=tuple(
            SourceInventoryItem(
                path=str(item["path"]),
                sha256=str(item["sha256"]),
                bytes=_native_nonnegative_int(item["bytes"], "artifact bytes"),
                row_count=_native_nonnegative_int(item["row_count"], "artifact row count"),
                media_type="application/gzip+json",
            )
            for item in receipt.artifacts
        ),
    )


def _build_verified_batch(
    root: Path, receipt: TickerEventCoverageReceipt
) -> TickerEventSourceBatch:
    identifiers = tuple(str(item["identifier"]) for item in receipt.manifest_refs)
    plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=receipt.request_start,
        end=receipt.request_end,
        tickers=identifiers,
    )
    requests = {request.request_id: request for request in plan.requests}
    if len(requests) != receipt.request_count:
        raise TickerEventSourceError("coverage receipt identifiers are not a unique formal plan")
    snapshots: list[TickerEventSourceSnapshot] = []
    statuses: list[TickerEventRequestStatus] = []
    seen_artifacts: set[str] = set()
    for raw_ref in receipt.manifest_refs:
        ref = _mapping(raw_ref, "coverage manifest ref")
        request_id = str(ref["request_id"])
        try:
            request = requests[request_id]
        except KeyError as exc:
            raise TickerEventSourceError(
                "coverage receipt request ID differs from identifier plan"
            ) from exc
        if request.asset_ids != (ref["identifier"],):
            raise TickerEventSourceError("coverage receipt identifier/request mismatch")
        try:
            verified = _verify_manifest_and_payload(
                root,
                safe_relative_path(root, str(ref["path"])),
                request_id=request_id,
                expected_request=request.canonical_dict(),
                scope="formal",
            )
        except TickerEventSourceProfileError as exc:
            raise TickerEventSourceError(str(exc)) from exc
        _match_verified_ref(verified, ref)
        created = _datetime(str(verified["created_at"]), "created_at")
        updated = _datetime(str(verified["updated_at"]), "updated_at")
        if verified["status"] == "not_found_404":
            statuses.append(
                TickerEventRequestStatus(
                    requested_identifier=str(ref["identifier"]),
                    source_request_id=request_id,
                    source_manifest_path=str(ref["path"]),
                    source_manifest_sha256=str(ref["sha256"]),
                    source_created_at_utc=created,
                    source_updated_at_utc=updated,
                    source_capture_at_utc=None,
                    outcome="not_found_404",
                    provider_status_code=404,
                    event_count=0,
                )
            )
            continue
        capture = _datetime(str(verified["completed_at"]), "completed_at")
        artifact = _mapping(verified["artifact"], "verified artifact")
        if str(artifact["path"]) in seen_artifacts:
            raise TickerEventSourceError("coverage receipt repeats a verified artifact")
        seen_artifacts.add(str(artifact["path"]))
        rows = tuple(
            {
                "date": item["date"],
                "source_event_hash": item["source_event_hash"],
                "ticker_change": {"ticker": item["target"]},
                "type": item["type"],
            }
            for item in verified["events"]
        )
        page = TickerEventSourcePage(
            source_path=str(artifact["path"]),
            source_artifact_sha256=str(artifact["sha256"]),
            raw_sha256=str(artifact["raw_sha256"]),
            sequence=0,
            compressed_bytes=int(artifact["bytes"]),
            raw_bytes=int(artifact["raw_bytes"]),
            record_count=int(artifact["row_count"]),
            source_provider_request_id=str(verified["provider_request_id"]),
            result_name=str(verified["returned_name"]),
            result_cik=(
                None if verified["returned_cik"] is None else str(verified["returned_cik"])
            ),
            result_composite_figi=str(verified["returned_composite_figi"]),
            result_hash=str(verified["result_hash"]),
            rows=rows,
        )
        snapshot = TickerEventSourceSnapshot(
            requested_identifier=str(ref["identifier"]),
            source_request_id=request_id,
            source_manifest_path=str(ref["path"]),
            source_manifest_sha256=str(ref["sha256"]),
            source_created_at_utc=created,
            source_capture_at_utc=capture,
            source_updated_at_utc=updated,
            pages=(page,),
        )
        snapshots.append(snapshot)
        statuses.append(
            TickerEventRequestStatus(
                requested_identifier=snapshot.requested_identifier,
                source_request_id=request_id,
                source_manifest_path=snapshot.source_manifest_path,
                source_manifest_sha256=snapshot.source_manifest_sha256,
                source_created_at_utc=created,
                source_updated_at_utc=updated,
                source_capture_at_utc=capture,
                outcome="complete",
                provider_status_code=None,
                event_count=page.record_count,
            )
        )
    batch = TickerEventSourceBatch(
        snapshots=tuple(snapshots),
        request_statuses=tuple(statuses),
        request_start=receipt.request_start,
        request_end=receipt.request_end,
    )
    if (
        batch.request_count != receipt.request_count
        or batch.page_count != receipt.complete_count
        or batch.not_found_count != receipt.not_found_count
        or batch.row_count != receipt.event_count
    ):
        raise TickerEventSourceError("verified ticker-event batch differs from coverage receipt")
    return batch


def _match_verified_ref(verified: Mapping[str, object], ref: Mapping[str, object]) -> None:
    expected = {
        "artifact": verified["artifact"],
        "completed_at": verified["completed_at"],
        "created_at": verified["created_at"],
        "event_count": verified["event_count"],
        "identifier": ref["identifier"],
        "path": verified["manifest_path"],
        "request_id": ref["request_id"],
        "sha256": verified["manifest_sha256"],
        "status": verified["status"],
        "updated_at": verified["updated_at"],
    }
    if expected != dict(ref):
        raise TickerEventSourceError(
            "raw ticker-event manifest/artifact differs from accepted coverage receipt"
        )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TickerEventSourceError(f"ticker-event {label} must be an object")
    return dict(value)


def _detached_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TickerEventSourceError(f"ticker-event {label} must be an object")
    try:
        detached = json.loads(
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
        )
    except (TypeError, ValueError) as exc:
        raise TickerEventSourceError(f"ticker-event {label} is not safe JSON") from exc
    if not isinstance(detached, dict):  # pragma: no cover - Mapping serialized above
        raise TickerEventSourceError(f"ticker-event {label} must remain an object")
    return MappingProxyType(detached)


def _relative_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or Path(value).is_absolute()
    ):
        raise TickerEventSourceError(f"ticker-event {label} is invalid")
    return value


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise TickerEventSourceError(f"ticker-event {label} is not a lowercase SHA-256")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise TickerEventSourceError(f"ticker-event {label} must be a nonnegative native int")
    return value


def _datetime(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TickerEventSourceError(f"ticker-event {label} is not ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TickerEventSourceError(f"ticker-event {label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "TickerEventCoverageReceipt",
    "TickerEventRequestStatus",
    "TickerEventSourceBatch",
    "TickerEventSourceError",
    "TickerEventSourcePage",
    "TickerEventSourceRecord",
    "TickerEventSourceSnapshot",
    "build_ticker_event_source_inventory",
    "load_ticker_event_coverage_receipt",
    "read_ticker_event_source_inventory",
    "ticker_event_coverage_receipt_path",
    "ticker_event_occurrence_transform_inputs",
    "ticker_event_request_transform_inputs",
    "ticker_event_transform_inputs",
]
