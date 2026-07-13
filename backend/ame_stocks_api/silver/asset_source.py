"""Read-only, manifest-bound streaming inputs for S4 asset transforms."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit

from ame_stocks_api.artifacts import safe_relative_path
from ame_stocks_api.providers.massive import MASSIVE_BASE_URL
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MANIFEST_PREFIX = "manifests/massive/assets"
_ARTIFACT_PREFIX = "bronze/massive/assets"


class AssetSourceError(SilverContractError):
    """Raised before transformation when a manifest-bound assets input is unsafe."""


@dataclass(frozen=True, slots=True)
class AssetSourcePage:
    """Manifest-declared metadata for one immutable Massive response page."""

    source_path: str
    source_artifact_sha256: str
    raw_sha256: str
    sequence: int
    compressed_bytes: int
    raw_bytes: int
    record_count: int
    next_continuation: str | None = None

    def __post_init__(self) -> None:
        if not self.source_path or Path(self.source_path).is_absolute():
            raise AssetSourceError("asset source page path must be relative")
        _sha256_text(self.source_artifact_sha256, "stored_sha256")
        _sha256_text(self.raw_sha256, "raw_sha256")
        _native_nonnegative_int(self.sequence, "sequence")
        _native_nonnegative_int(self.compressed_bytes, "compressed_bytes")
        _native_nonnegative_int(self.raw_bytes, "raw_bytes")
        _native_nonnegative_int(self.record_count, "record_count")
        if self.next_continuation is not None and (
            not isinstance(self.next_continuation, str)
            or not self.next_continuation
            or self.next_continuation != self.next_continuation.strip()
        ):
            raise AssetSourceError("asset page continuation must be null or trimmed text")


@dataclass(frozen=True, slots=True)
class AssetSourceRequest:
    """One complete active or inactive request for a historical session."""

    session_date: date
    requested_active: bool
    source_request_id: str
    source_manifest_path: str
    source_manifest_sha256: str
    source_created_at_utc: datetime
    source_capture_at_utc: datetime
    source_updated_at_utc: datetime
    pages: tuple[AssetSourcePage, ...]

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise AssetSourceError("asset source session date must be a date")
        if type(self.requested_active) is not bool:
            raise AssetSourceError("asset requested_active must be a native bool")
        _sha256_text(self.source_request_id, "request_id")
        _sha256_text(self.source_manifest_sha256, "manifest SHA-256")
        if not self.source_manifest_path or Path(self.source_manifest_path).is_absolute():
            raise AssetSourceError("asset manifest path must be relative")
        for label in (
            "source_created_at_utc",
            "source_capture_at_utc",
            "source_updated_at_utc",
        ):
            value = getattr(self, label)
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise AssetSourceError(f"asset {label} must be timezone-aware")
            object.__setattr__(self, label, value.astimezone(UTC))
        pages = tuple(self.pages)
        if not pages:
            raise AssetSourceError("asset source request requires at least one page")
        if tuple(page.sequence for page in pages) != tuple(range(len(pages))):
            raise AssetSourceError("asset source pages must be contiguous and ordered")
        if len({page.source_path for page in pages}) != len(pages):
            raise AssetSourceError("asset source page paths must be unique")
        object.__setattr__(self, "pages", pages)

    @property
    def page_count(self) -> int:
        return len(self.pages)

    @property
    def declared_row_count(self) -> int:
        return sum(page.record_count for page in self.pages)


@dataclass(frozen=True, slots=True)
class AssetSourceSession:
    """The complete active/inactive manifest pair for one session date."""

    session_date: date
    active_request: AssetSourceRequest
    inactive_request: AssetSourceRequest

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise AssetSourceError("asset source session date must be a date")
        requests = (self.active_request, self.inactive_request)
        if any(item.session_date != self.session_date for item in requests):
            raise AssetSourceError("asset source pair has inconsistent session dates")
        if self.active_request.requested_active is not True:
            raise AssetSourceError("asset source pair is missing its active request")
        if self.inactive_request.requested_active is not False:
            raise AssetSourceError("asset source pair is missing its inactive request")
        if self.active_request.source_request_id == self.inactive_request.source_request_id:
            raise AssetSourceError("asset source pair request IDs must be distinct")

    @property
    def requests(self) -> tuple[AssetSourceRequest, AssetSourceRequest]:
        """Return the pair in stable active-then-inactive order."""

        return (self.active_request, self.inactive_request)

    @property
    def capture_completed_at_utc(self) -> datetime:
        return max(item.source_capture_at_utc for item in self.requests)

    @property
    def page_count(self) -> int:
        return sum(item.page_count for item in self.requests)

    @property
    def declared_row_count(self) -> int:
        return sum(item.declared_row_count for item in self.requests)


@dataclass(frozen=True, slots=True)
class AssetSourceRecord:
    """One verified provider row plus its complete immutable source pointer."""

    session_date: date
    requested_active: bool
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
    row: Mapping[str, object]


class AssetSourceReader:
    """Re-iterable page-streaming reader over exact manifest-bound session pairs.

    Construct this through :func:`read_asset_source_inventory`. Manifest structure and the
    inventory preimage are checked eagerly. Stored bytes, gzip, raw bytes, JSON envelopes and
    rows are checked page by page while an iterator is consumed, so memory stays bounded by one
    provider page.
    """

    def __init__(
        self,
        data_root: Path,
        inventory: SourceInventory,
        sessions: tuple[AssetSourceSession, ...],
    ) -> None:
        self._root = data_root.expanduser().resolve()
        self._inventory = inventory
        self._sessions = tuple(sessions)
        self._session_by_date = {item.session_date: item for item in self._sessions}
        self._inventory_items = {item.path: item for item in inventory.artifacts}

    @property
    def inventory(self) -> SourceInventory:
        return self._inventory

    @property
    def sessions(self) -> tuple[AssetSourceSession, ...]:
        return self._sessions

    @property
    def session_count(self) -> int:
        return len(self._sessions)

    @property
    def request_count(self) -> int:
        return self.session_count * 2

    @property
    def page_count(self) -> int:
        return sum(item.page_count for item in self._sessions)

    @property
    def declared_row_count(self) -> int:
        return sum(item.declared_row_count for item in self._sessions)

    def iter_sessions(self) -> Iterator[AssetSourceSession]:
        """Iterate immutable session-pair metadata in ascending session order."""

        yield from self._sessions

    def iter_session_records(self, session_date: date | str) -> Iterator[AssetSourceRecord]:
        """Stream and fully verify both requests for one selected session."""

        normalized = _session_date(session_date)
        try:
            session = self._session_by_date[normalized]
        except KeyError as exc:
            raise AssetSourceError(
                f"asset source session is absent from inventory: {normalized.isoformat()}"
            ) from exc
        for request in session.requests:
            yield from self._iter_request(request)

    def iter_records(self) -> Iterator[AssetSourceRecord]:
        """Stream every verified row in session, active request and page order."""

        for session in self._sessions:
            for request in session.requests:
                yield from self._iter_request(request)

    def _iter_request(self, request: AssetSourceRequest) -> Iterator[AssetSourceRecord]:
        manifest_path = safe_relative_path(self._root, request.source_manifest_path)
        try:
            current_manifest = manifest_path.read_bytes()
        except OSError as exc:
            raise AssetSourceError(
                f"cannot reread asset manifest: {request.source_manifest_path}"
            ) from exc
        if hashlib.sha256(current_manifest).hexdigest() != request.source_manifest_sha256:
            raise AssetSourceError("asset manifest changed while being streamed")

        for page in request.pages:
            item = self._inventory_items.get(page.source_path)
            if item is None:  # protected by inventory preimage verification
                raise AssetSourceError("asset manifest page is absent from source inventory")
            try:
                compressed = safe_relative_path(self._root, page.source_path).read_bytes()
            except OSError as exc:
                raise AssetSourceError(
                    f"cannot read asset Bronze page: {page.source_path}"
                ) from exc
            if len(compressed) != page.compressed_bytes or len(compressed) != item.bytes:
                raise AssetSourceError("asset Bronze page compressed byte count mismatch")
            stored_sha256 = hashlib.sha256(compressed).hexdigest()
            if stored_sha256 != page.source_artifact_sha256 or stored_sha256 != item.sha256:
                raise AssetSourceError("asset Bronze page stored checksum mismatch")
            try:
                raw = gzip.decompress(compressed)
            except (EOFError, OSError) as exc:
                raise AssetSourceError("asset Bronze page is not valid gzip") from exc
            if len(raw) != page.raw_bytes:
                raise AssetSourceError("asset Bronze page raw byte count mismatch")
            if hashlib.sha256(raw).hexdigest() != page.raw_sha256:
                raise AssetSourceError("asset Bronze page raw checksum mismatch")
            try:
                response = json.loads(raw, parse_constant=_reject_json_constant)
            except (UnicodeDecodeError, ValueError) as exc:
                raise AssetSourceError("asset Bronze page is not valid JSON") from exc
            provider_request_id, rows = _validate_response_envelope(
                response,
                expected_continuation=page.next_continuation,
            )
            if len(rows) != page.record_count or len(rows) != item.row_count:
                raise AssetSourceError("asset response rows differ from manifest")
            for row_ordinal, row in enumerate(rows):
                provider_active = row.get("active")
                if (
                    type(provider_active) is not bool
                    or provider_active is not request.requested_active
                ):
                    raise AssetSourceError(
                        "asset provider active flag does not match the manifest request"
                    )
                yield AssetSourceRecord(
                    session_date=request.session_date,
                    requested_active=request.requested_active,
                    source_request_id=request.source_request_id,
                    source_manifest_path=request.source_manifest_path,
                    source_manifest_sha256=request.source_manifest_sha256,
                    source_created_at_utc=request.source_created_at_utc,
                    source_capture_at_utc=request.source_capture_at_utc,
                    source_updated_at_utc=request.source_updated_at_utc,
                    source_artifact_path=page.source_path,
                    source_artifact_sha256=page.source_artifact_sha256,
                    source_page_sequence=page.sequence,
                    source_row_ordinal=row_ordinal,
                    source_provider_request_id=provider_request_id,
                    row=MappingProxyType(dict(row)),
                )


def build_asset_source_inventory(
    data_root: Path,
    *,
    manifest_paths: Iterable[str],
    git_commit: str,
) -> SourceInventory:
    """Build, but never register, a complete-pair Bronze assets inventory.

    Manifest structure and active/inactive pairing are verified before any page is read. Every
    listed compressed page then has its physical byte count and stored SHA-256 checked. Raw-page
    and response validation remains streaming and is performed by :class:`AssetSourceReader`.
    """

    root = data_root.expanduser().resolve()
    sessions = _load_asset_sessions(root, manifest_paths)
    inventory = _declared_inventory(sessions, git_commit=git_commit)
    for item in inventory.artifacts:
        try:
            content = safe_relative_path(root, item.path).read_bytes()
        except OSError as exc:
            raise AssetSourceError(f"cannot read asset Bronze page: {item.path}") from exc
        if len(content) != item.bytes:
            raise AssetSourceError("asset Bronze page compressed byte count mismatch")
        if hashlib.sha256(content).hexdigest() != item.sha256:
            raise AssetSourceError("asset Bronze page stored checksum mismatch")
    return inventory


def read_asset_source_inventory(
    data_root: Path,
    inventory: SourceInventory,
) -> AssetSourceReader:
    """Verify an ephemeral inventory preimage and return a read-only streaming reader."""

    if inventory.source_dataset != "assets" or inventory.source_layer is not SourceLayer.BRONZE:
        raise AssetSourceError("asset input must be a Bronze assets inventory")
    manifest_paths = tuple(item.path for item in inventory.upstream_manifests)
    sessions = _load_asset_sessions(data_root.expanduser().resolve(), manifest_paths)
    declared = _declared_inventory(sessions, git_commit=inventory.git_commit)
    if declared.to_dict() != inventory.to_dict():
        raise AssetSourceError("asset source inventory differs from manifest-declared bytes")
    return AssetSourceReader(data_root, inventory, sessions)


def _load_asset_sessions(
    root: Path,
    manifest_paths: Iterable[str],
) -> tuple[AssetSourceSession, ...]:
    paths = _normalize_manifest_paths(manifest_paths)
    requests = tuple(_load_asset_manifest(root, path) for path in paths)
    if len({item.source_request_id for item in requests}) != len(requests):
        raise AssetSourceError("asset source request IDs must be unique")
    page_paths = [page.source_path for request in requests for page in request.pages]
    if len(set(page_paths)) != len(page_paths):
        raise AssetSourceError("asset manifests bind the same page more than once")

    grouped: defaultdict[date, dict[bool, AssetSourceRequest]] = defaultdict(dict)
    for request in requests:
        group = grouped[request.session_date]
        if request.requested_active in group:
            raise AssetSourceError("asset session has duplicate active request scope")
        group[request.requested_active] = request
    sessions: list[AssetSourceSession] = []
    for session_date, pair in sorted(grouped.items()):
        if set(pair) != {False, True}:
            raise AssetSourceError(
                f"asset session requires one active and one inactive request: {session_date}"
            )
        sessions.append(
            AssetSourceSession(
                session_date=session_date,
                active_request=pair[True],
                inactive_request=pair[False],
            )
        )
    return tuple(sessions)


def _normalize_manifest_paths(manifest_paths: Iterable[str]) -> tuple[str, ...]:
    try:
        paths = tuple(manifest_paths)
    except TypeError as exc:
        raise AssetSourceError("asset manifest paths must be iterable") from exc
    if not paths:
        raise AssetSourceError("asset inventory requires at least one manifest pair")
    if not all(isinstance(path, str) and path for path in paths):
        raise AssetSourceError("asset manifest paths must be nonempty strings")
    if len(set(paths)) != len(paths):
        raise AssetSourceError("asset manifest paths must be unique")
    return tuple(sorted(paths))


def _load_asset_manifest(root: Path, relative_path: str) -> AssetSourceRequest:
    relative = Path(relative_path)
    if relative.is_absolute() or relative.parent.as_posix() != _MANIFEST_PREFIX:
        raise AssetSourceError("asset manifest is outside its canonical namespace")
    path = safe_relative_path(root, relative_path)
    try:
        content = path.read_bytes()
        document = json.loads(content, parse_constant=_reject_json_constant)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise AssetSourceError(f"cannot read asset manifest: {relative_path}") from exc
    if not isinstance(document, dict):
        raise AssetSourceError("asset manifest root must be an object")
    request_id = _sha256_text(document.get("request_id"), "request_id")
    if path.name != f"{request_id}.json":
        raise AssetSourceError("asset manifest filename does not match request ID")
    if (
        document.get("manifest_schema_version") != 1
        or document.get("provider") != "massive"
        or document.get("dataset") != "assets"
        or document.get("status") != "complete"
    ):
        raise AssetSourceError("asset manifest identity, schema or terminal status is invalid")
    if "checkpoint" not in document or document.get("checkpoint") is not None:
        raise AssetSourceError("complete asset manifest must have a null checkpoint")

    request = document.get("request")
    if not isinstance(request, dict):
        raise AssetSourceError("asset manifest request is missing")
    start = request.get("start")
    end = request.get("end")
    parameters = request.get("parameters")
    active_parameter = parameters.get("active") if isinstance(parameters, dict) else None
    if (
        request.get("dataset") != "assets"
        or not isinstance(start, str)
        or start != end
        or request.get("asset_ids") != []
        or request.get("adjusted") is not False
        or not isinstance(parameters, dict)
        or set(parameters) != {"active"}
        or not isinstance(active_parameter, str)
        or active_parameter not in ("true", "false")
    ):
        raise AssetSourceError("asset manifest request scope is invalid")
    if not _ISO_DATE.fullmatch(start):
        raise AssetSourceError("asset request date is not an ISO date")
    try:
        session_date = date.fromisoformat(start)
    except ValueError as exc:
        raise AssetSourceError("asset request date is not an ISO date") from exc
    requested_active = active_parameter == "true"

    created_at = _utc_datetime(document.get("created_at"), "created_at")
    capture_at = _utc_datetime(document.get("completed_at"), "completed_at")
    updated_at = _utc_datetime(document.get("updated_at"), "updated_at")
    if created_at > capture_at:
        raise AssetSourceError("asset manifest completion precedes creation")
    if capture_at > updated_at:
        raise AssetSourceError("asset manifest update precedes completion")

    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise AssetSourceError("complete asset manifest has no artifacts")
    if not all(isinstance(item, dict) for item in artifacts):
        raise AssetSourceError("asset manifest artifacts must be objects")
    pages: list[AssetSourcePage] = []
    for expected_sequence, artifact in enumerate(artifacts):
        sequence = _native_nonnegative_int(artifact.get("sequence"), "sequence")
        if sequence != expected_sequence:
            raise AssetSourceError("asset manifest page sequences are not contiguous")
        source_path = _required_text(artifact, "path")
        relative_artifact = Path(source_path)
        expected_parent = f"{_ARTIFACT_PREFIX}/request_id={request_id}"
        if (
            relative_artifact.is_absolute()
            or relative_artifact.parent.as_posix() != expected_parent
        ):
            raise AssetSourceError("asset page path does not match its request")
        if relative_artifact.name != f"page-{sequence:05d}.json.gz":
            raise AssetSourceError("asset page filename does not match its sequence")
        if artifact.get("content_type") != "application/json":
            raise AssetSourceError("asset page content type is not JSON")
        is_last = sequence == len(artifacts) - 1
        if artifact.get("is_last") is not is_last:
            raise AssetSourceError("asset manifest page termination marker is invalid")
        continuation = artifact.get("next_continuation")
        if (is_last and continuation is not None) or (
            not is_last
            and (
                not isinstance(continuation, str)
                or not continuation
                or continuation != continuation.strip()
            )
        ):
            raise AssetSourceError("asset manifest page continuation is invalid")
        pages.append(
            AssetSourcePage(
                source_path=source_path,
                source_artifact_sha256=_sha256_text(artifact.get("stored_sha256"), "stored_sha256"),
                raw_sha256=_sha256_text(artifact.get("raw_sha256"), "raw_sha256"),
                sequence=sequence,
                compressed_bytes=_native_nonnegative_int(
                    artifact.get("compressed_bytes"), "compressed_bytes"
                ),
                raw_bytes=_native_nonnegative_int(artifact.get("raw_bytes"), "raw_bytes"),
                record_count=_native_nonnegative_int(artifact.get("record_count"), "record_count"),
                next_continuation=continuation,
            )
        )
    continuations = [page.next_continuation for page in pages if page.next_continuation]
    if len(continuations) != len(set(continuations)):
        raise AssetSourceError("asset manifest repeats a page continuation")
    return AssetSourceRequest(
        session_date=session_date,
        requested_active=requested_active,
        source_request_id=request_id,
        source_manifest_path=relative_path,
        source_manifest_sha256=hashlib.sha256(content).hexdigest(),
        source_created_at_utc=created_at,
        source_capture_at_utc=capture_at,
        source_updated_at_utc=updated_at,
        pages=tuple(pages),
    )


def _declared_inventory(
    sessions: tuple[AssetSourceSession, ...],
    *,
    git_commit: str,
) -> SourceInventory:
    requests = [request for session in sessions for request in session.requests]
    return SourceInventory(
        source_dataset="assets",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=tuple(
            UpstreamManifestRef(
                path=request.source_manifest_path,
                sha256=request.source_manifest_sha256,
            )
            for request in requests
        ),
        artifacts=tuple(
            SourceInventoryItem(
                path=page.source_path,
                sha256=page.source_artifact_sha256,
                bytes=page.compressed_bytes,
                row_count=page.record_count,
                media_type="application/gzip+json",
            )
            for request in requests
            for page in request.pages
        ),
    )


def _validate_response_envelope(
    response: object,
    *,
    expected_continuation: str | None,
) -> tuple[str, tuple[dict[str, Any], ...]]:
    if not isinstance(response, dict) or response.get("status") != "OK":
        raise AssetSourceError("asset response envelope status is invalid")
    provider_request_id = response.get("request_id")
    if (
        not isinstance(provider_request_id, str)
        or not provider_request_id
        or provider_request_id != provider_request_id.strip()
    ):
        raise AssetSourceError("asset response request ID is missing or invalid")
    results = response.get("results")
    if not isinstance(results, list) or not all(isinstance(item, dict) for item in results):
        raise AssetSourceError("asset response results must be an array of objects")
    count = response.get("count")
    if type(count) is not int or count < 0 or count != len(results):
        raise AssetSourceError("asset response count does not match results")
    if _safe_continuation(response.get("next_url")) != expected_continuation:
        raise AssetSourceError("asset response next URL differs from its manifest continuation")
    return provider_request_id, tuple(results)


def _safe_continuation(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise AssetSourceError("asset response next URL must be text or null")
    absolute = urljoin(f"{MASSIVE_BASE_URL}/", value)
    parsed = urlsplit(absolute)
    provider_origin = urlsplit(MASSIVE_BASE_URL)
    if (parsed.scheme.lower(), parsed.netloc.lower()) != (
        provider_origin.scheme.lower(),
        provider_origin.netloc.lower(),
    ):
        raise AssetSourceError("asset response next URL changed provider origin")
    if parsed.fragment or any(key.lower() == "apikey" for key, _ in parse_qsl(parsed.query)):
        raise AssetSourceError("asset response next URL is unsafe")
    return urlunsplit(("", "", parsed.path, parsed.query, ""))


def _required_text(document: Mapping[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        raise AssetSourceError(f"asset source {key} must be a trimmed nonempty string")
    return value


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise AssetSourceError(f"asset {label} must be a lowercase SHA-256")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise AssetSourceError(f"asset {label} must be a nonnegative native int")
    return value


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise AssetSourceError(f"asset {label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AssetSourceError(f"asset {label} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AssetSourceError(f"asset {label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _session_date(value: date | str) -> date:
    if type(value) is date:
        return value
    if isinstance(value, str):
        if not _ISO_DATE.fullmatch(value):
            raise AssetSourceError("asset requested session is not an ISO date")
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise AssetSourceError("asset requested session is not an ISO date") from exc
    raise AssetSourceError("asset requested session must be a date or ISO date string")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


__all__ = [
    "AssetSourceError",
    "AssetSourcePage",
    "AssetSourceReader",
    "AssetSourceRecord",
    "AssetSourceRequest",
    "AssetSourceSession",
    "build_asset_source_inventory",
    "read_asset_source_inventory",
]
