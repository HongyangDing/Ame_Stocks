"""Read-only, manifest-bound inputs for the approved exchange_dim transform."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ame_stocks_api.artifacts import safe_relative_path
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MANIFEST_PREFIX = "manifests/massive/exchanges"
_ARTIFACT_PREFIX = "bronze/massive/exchanges"


class ExchangeSourceError(SilverContractError):
    """Raised before transformation when manifest-bound exchange input is unsafe."""


@dataclass(frozen=True, slots=True)
class ExchangeSourcePage:
    """One verified provider response page within an exchange snapshot."""

    source_path: str
    source_artifact_sha256: str
    sequence: int
    source_provider_request_id: str
    rows: tuple[Mapping[str, object], ...]

    def __post_init__(self) -> None:
        if not self.source_path or Path(self.source_path).is_absolute():
            raise ExchangeSourceError("exchange source page path must be relative")
        if not _SHA256.fullmatch(self.source_artifact_sha256):
            raise ExchangeSourceError("exchange source page SHA-256 is invalid")
        if type(self.sequence) is not int or self.sequence < 0:
            raise ExchangeSourceError("exchange source page sequence must be nonnegative")
        if not isinstance(self.source_provider_request_id, str) or not (
            self.source_provider_request_id.strip()
        ):
            raise ExchangeSourceError("exchange provider request ID is missing")
        normalized: list[Mapping[str, object]] = []
        for row in self.rows:
            if not isinstance(row, Mapping):
                raise ExchangeSourceError("exchange result rows must be objects")
            try:
                detached = json.loads(
                    json.dumps(row, allow_nan=False, separators=(",", ":"), sort_keys=True)
                )
            except (TypeError, ValueError) as exc:
                raise ExchangeSourceError("exchange result row is not safe JSON") from exc
            if not isinstance(detached, dict):  # pragma: no cover - Mapping serialized above
                raise ExchangeSourceError("exchange result row is not an object")
            normalized.append(MappingProxyType(detached))
        object.__setattr__(self, "rows", tuple(normalized))


@dataclass(frozen=True, slots=True)
class ExchangeSourceSnapshot:
    """One immutable latest-only request observed at a specific instant."""

    source_request_id: str
    source_capture_at_utc: datetime
    pages: tuple[ExchangeSourcePage, ...]

    def __post_init__(self) -> None:
        if not _SHA256.fullmatch(self.source_request_id):
            raise ExchangeSourceError("exchange source request ID is invalid")
        captured = self.source_capture_at_utc
        if not isinstance(captured, datetime) or captured.tzinfo is None:
            raise ExchangeSourceError("exchange capture time must be timezone-aware")
        object.__setattr__(self, "source_capture_at_utc", captured.astimezone(UTC))
        pages = tuple(self.pages)
        if not pages:
            raise ExchangeSourceError("exchange source snapshot requires at least one page")
        if tuple(page.sequence for page in pages) != tuple(range(len(pages))):
            raise ExchangeSourceError("exchange source pages must be contiguous and ordered")
        if len({page.source_path for page in pages}) != len(pages):
            raise ExchangeSourceError("exchange source page paths must be unique")
        object.__setattr__(self, "pages", pages)


@dataclass(frozen=True, slots=True)
class ExchangeSourceBatch:
    """Verified snapshots ready for the pure exchange_dim transformation."""

    snapshots: tuple[ExchangeSourceSnapshot, ...]

    def __post_init__(self) -> None:
        snapshots = tuple(self.snapshots)
        if not snapshots:
            raise ExchangeSourceError("exchange source batch cannot be empty")
        if len({item.source_request_id for item in snapshots}) != len(snapshots):
            raise ExchangeSourceError("exchange source request IDs must be unique")
        object.__setattr__(
            self,
            "snapshots",
            tuple(
                sorted(
                    snapshots,
                    key=lambda item: (
                        item.source_capture_at_utc,
                        item.source_request_id,
                    ),
                )
            ),
        )

    @property
    def page_count(self) -> int:
        return sum(len(snapshot.pages) for snapshot in self.snapshots)

    @property
    def row_count(self) -> int:
        return sum(len(page.rows) for snapshot in self.snapshots for page in snapshot.pages)

    @property
    def source_object_count(self) -> int:
        return len(self.snapshots) + self.page_count


def build_exchange_source_inventory(
    data_root: Path,
    *,
    manifest_paths: Iterable[str],
    git_commit: str,
) -> SourceInventory:
    """Build, but do not register, an exact manifest-bound Bronze inventory."""

    root = data_root.expanduser().resolve()
    paths = tuple(sorted(set(manifest_paths)))
    if not paths:
        raise ExchangeSourceError("exchange inventory requires at least one manifest")
    upstream: list[UpstreamManifestRef] = []
    artifacts: list[SourceInventoryItem] = []
    seen_artifacts: set[str] = set()
    for relative_manifest in paths:
        document, content, _ = _load_exchange_manifest(root, relative_manifest)
        upstream.append(
            UpstreamManifestRef(
                path=relative_manifest,
                sha256=hashlib.sha256(content).hexdigest(),
            )
        )
        for artifact in _manifest_artifacts(document):
            relative_artifact = _required_text(artifact, "path")
            if relative_artifact in seen_artifacts:
                raise ExchangeSourceError("exchange manifests bind the same artifact twice")
            seen_artifacts.add(relative_artifact)
            artifact_path = safe_relative_path(root, relative_artifact)
            try:
                compressed = artifact_path.read_bytes()
            except OSError as exc:
                raise ExchangeSourceError(
                    f"cannot read exchange Bronze artifact: {relative_artifact}"
                ) from exc
            expected_bytes = _native_nonnegative_int(artifact.get("compressed_bytes"), "bytes")
            expected_sha = _sha256_text(artifact.get("stored_sha256"), "stored_sha256")
            if len(compressed) != expected_bytes:
                raise ExchangeSourceError("exchange Bronze artifact byte count mismatch")
            if hashlib.sha256(compressed).hexdigest() != expected_sha:
                raise ExchangeSourceError("exchange Bronze artifact checksum mismatch")
            artifacts.append(
                SourceInventoryItem(
                    path=relative_artifact,
                    sha256=expected_sha,
                    bytes=expected_bytes,
                    row_count=_native_nonnegative_int(
                        artifact.get("record_count"),
                        "record_count",
                    ),
                    media_type="application/gzip+json",
                )
            )
    return SourceInventory(
        source_dataset="exchanges",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=tuple(upstream),
        artifacts=tuple(artifacts),
    )


def read_exchange_source_inventory(
    data_root: Path,
    inventory: SourceInventory,
) -> ExchangeSourceBatch:
    """Verify the full inventory preimage, decompress its pages, and return no other files."""

    if inventory.source_dataset != "exchanges" or inventory.source_layer is not SourceLayer.BRONZE:
        raise ExchangeSourceError("exchange input must be a Bronze exchanges inventory")
    manifest_paths = tuple(item.path for item in inventory.upstream_manifests)
    rebuilt = build_exchange_source_inventory(
        data_root,
        manifest_paths=manifest_paths,
        git_commit=inventory.git_commit,
    )
    if rebuilt.to_dict() != inventory.to_dict():
        raise ExchangeSourceError("exchange source inventory differs from current immutable bytes")

    root = data_root.expanduser().resolve()
    inventory_items = {item.path: item for item in inventory.artifacts}
    upstream_sha_by_path = {
        item.path: item.sha256 for item in inventory.upstream_manifests
    }
    snapshots: list[ExchangeSourceSnapshot] = []
    for relative_manifest in sorted(manifest_paths):
        document, manifest_content, capture_at = _load_exchange_manifest(root, relative_manifest)
        if hashlib.sha256(manifest_content).hexdigest() != upstream_sha_by_path[relative_manifest]:
            raise ExchangeSourceError("exchange manifest changed while being read")
        request_id = _sha256_text(document.get("request_id"), "request_id")
        pages: list[ExchangeSourcePage] = []
        for artifact in _manifest_artifacts(document):
            relative_artifact = _required_text(artifact, "path")
            item = inventory_items.get(relative_artifact)
            if item is None:
                raise ExchangeSourceError("manifest page is absent from exchange inventory")
            try:
                compressed = safe_relative_path(root, relative_artifact).read_bytes()
            except OSError as exc:
                raise ExchangeSourceError("cannot reread exchange Bronze artifact") from exc
            if len(compressed) != item.bytes:
                raise ExchangeSourceError("exchange Bronze page changed byte count while reading")
            if hashlib.sha256(compressed).hexdigest() != item.sha256:
                raise ExchangeSourceError("exchange Bronze page changed checksum while reading")
            try:
                raw = gzip.decompress(compressed)
            except (OSError, gzip.BadGzipFile) as exc:
                raise ExchangeSourceError("exchange Bronze page is not valid gzip") from exc
            expected_raw_bytes = _native_nonnegative_int(
                artifact.get("raw_bytes"),
                "raw_bytes",
            )
            if len(raw) != expected_raw_bytes:
                raise ExchangeSourceError("exchange Bronze raw byte count mismatch")
            expected_raw = _sha256_text(artifact.get("raw_sha256"), "raw_sha256")
            if hashlib.sha256(raw).hexdigest() != expected_raw:
                raise ExchangeSourceError("exchange Bronze raw checksum mismatch")
            try:
                response = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ExchangeSourceError("exchange Bronze page is not valid JSON") from exc
            provider_request_id, rows = _validate_response_envelope(response)
            if len(rows) != item.row_count:
                raise ExchangeSourceError("exchange response rows differ from manifest")
            pages.append(
                ExchangeSourcePage(
                    source_path=relative_artifact,
                    source_artifact_sha256=item.sha256,
                    sequence=_native_nonnegative_int(artifact.get("sequence"), "sequence"),
                    source_provider_request_id=provider_request_id,
                    rows=tuple(rows),
                )
            )
        snapshots.append(
            ExchangeSourceSnapshot(
                source_request_id=request_id,
                source_capture_at_utc=capture_at,
                pages=tuple(pages),
            )
        )
    return ExchangeSourceBatch(tuple(snapshots))


def _load_exchange_manifest(
    root: Path,
    relative_path: str,
) -> tuple[dict[str, Any], bytes, datetime]:
    if not isinstance(relative_path, str) or not relative_path.startswith(
        f"{_MANIFEST_PREFIX}/"
    ):
        raise ExchangeSourceError("exchange manifest is outside its canonical namespace")
    relative_manifest = Path(relative_path)
    if relative_manifest.parent.as_posix() != _MANIFEST_PREFIX:
        raise ExchangeSourceError("exchange manifest must use the canonical dataset directory")
    path = safe_relative_path(root, relative_path)
    try:
        content = path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExchangeSourceError(f"cannot read exchange manifest: {relative_path}") from exc
    if not isinstance(document, dict):
        raise ExchangeSourceError("exchange manifest root must be an object")
    request_id = _sha256_text(document.get("request_id"), "request_id")
    if (
        document.get("status") != "complete"
        or document.get("provider") != "massive"
        or document.get("dataset") != "exchanges"
    ):
        raise ExchangeSourceError("exchange manifest identity or terminal status is invalid")
    expected_name = f"{request_id}.json"
    if path.name != expected_name:
        raise ExchangeSourceError("exchange manifest filename does not match request ID")
    request = document.get("request")
    if not isinstance(request, dict):
        raise ExchangeSourceError("exchange manifest request is missing")
    start = request.get("start")
    end = request.get("end")
    if (
        request.get("dataset") != "exchanges"
        or not isinstance(start, str)
        or start != end
        or request.get("asset_ids") != []
        or request.get("parameters") != {}
        or request.get("adjusted") is not False
    ):
        raise ExchangeSourceError("exchange manifest is not the approved latest-only request")
    try:
        date.fromisoformat(start)
    except ValueError as exc:
        raise ExchangeSourceError("exchange request date label is invalid") from exc
    created_at = _utc_datetime(document.get("created_at"), "created_at")
    capture_at = _utc_datetime(document.get("completed_at"), "completed_at")
    if created_at > capture_at:
        raise ExchangeSourceError("exchange manifest completion precedes creation")
    artifacts = _manifest_artifacts(document)
    expected_sequences = tuple(range(len(artifacts)))
    actual_sequences = tuple(
        _native_nonnegative_int(item.get("sequence"), "sequence") for item in artifacts
    )
    if actual_sequences != expected_sequences:
        raise ExchangeSourceError("exchange manifest page sequences are not contiguous")
    for artifact in artifacts:
        artifact_path = _required_text(artifact, "path")
        sequence = _native_nonnegative_int(artifact.get("sequence"), "sequence")
        expected_parent = f"{_ARTIFACT_PREFIX}/request_id={request_id}"
        relative_artifact = Path(artifact_path)
        if relative_artifact.parent.as_posix() != expected_parent:
            raise ExchangeSourceError("exchange artifact path does not match its request")
        if relative_artifact.name != f"page-{sequence:05d}.json.gz":
            raise ExchangeSourceError("exchange artifact filename does not match its sequence")
        if artifact.get("content_type") != "application/json":
            raise ExchangeSourceError("exchange artifact content type is not JSON")
        _native_nonnegative_int(artifact.get("compressed_bytes"), "compressed_bytes")
        _native_nonnegative_int(artifact.get("raw_bytes"), "raw_bytes")
        _native_nonnegative_int(artifact.get("record_count"), "record_count")
        _sha256_text(artifact.get("stored_sha256"), "stored_sha256")
        _sha256_text(artifact.get("raw_sha256"), "raw_sha256")
        if artifact.get("is_last") is not (sequence == len(artifacts) - 1):
            raise ExchangeSourceError("exchange manifest page termination marker is invalid")
    return document, content, capture_at


def _manifest_artifacts(document: Mapping[str, object]) -> tuple[dict[str, Any], ...]:
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ExchangeSourceError("complete exchange manifest has no artifacts")
    if not all(isinstance(item, dict) for item in artifacts):
        raise ExchangeSourceError("exchange manifest artifacts must be objects")
    return tuple(artifacts)


def _validate_response_envelope(response: object) -> tuple[str, tuple[dict[str, Any], ...]]:
    if not isinstance(response, dict) or response.get("status") != "OK":
        raise ExchangeSourceError("exchange response envelope status is invalid")
    provider_request_id = response.get("request_id")
    if not isinstance(provider_request_id, str) or not provider_request_id.strip():
        raise ExchangeSourceError("exchange response request ID is missing")
    if provider_request_id != provider_request_id.strip():
        raise ExchangeSourceError("exchange response request ID has surrounding whitespace")
    results = response.get("results")
    if not isinstance(results, list) or not all(isinstance(item, dict) for item in results):
        raise ExchangeSourceError("exchange response results must be an array of objects")
    count = response.get("count")
    if count is not None and (
        type(count) is not int or count < 0 or count != len(results)
    ):
        raise ExchangeSourceError("exchange response count does not match results")
    return provider_request_id, tuple(results)


def _required_text(document: Mapping[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ExchangeSourceError(f"exchange source {key} must be a nonempty string")
    return value


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ExchangeSourceError(f"exchange {label} must be a lowercase SHA-256")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ExchangeSourceError(f"exchange {label} must be a nonnegative native int")
    return value


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ExchangeSourceError(f"exchange {label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExchangeSourceError(f"exchange {label} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExchangeSourceError(f"exchange {label} must be timezone-aware")
    return parsed.astimezone(UTC)


__all__ = [
    "ExchangeSourceBatch",
    "ExchangeSourceError",
    "ExchangeSourcePage",
    "ExchangeSourceSnapshot",
    "build_exchange_source_inventory",
    "read_exchange_source_inventory",
]
