"""Bounded external FIGI resolution for provider rows with no source FIGI.

The provider observation remains immutable.  This module captures OpenFIGI and
Nasdaq symbol-directory evidence and exposes a separate canonical overlay for
active common-stock rows whose observed Composite and Share Class FIGIs are
both null.  It never mutates the four S7.5 tables and grants no publication or
tradability authority.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, Protocol, Self

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.identity_resolution import canonical_asset_id
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_migration_io import (
    readback_i3_migration_parquet_exact,
)
from ame_stocks_api.silver.incremental_identity import canonical_share_class_id

EXTERNAL_FIGI_RESOLUTION_RULE_VERSION: Final = "s7_5_external_figi_null_observation_resolution_v1"
EXTERNAL_FIGI_EVIDENCE_RULE_VERSION: Final = "openfigi_nasdaq_exact_capture_v1"
OPENFIGI_MAPPING_URL: Final = "https://api.openfigi.com/v3/mapping"
NASDAQ_LISTED_URL: Final = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL: Final = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
EXTERNAL_FIGI_RELEASE_ROOT: Final = "manifests/silver/identity/external-figi-resolutions"
EXTERNAL_FIGI_EVIDENCE_ROOT: Final = "bronze/external/openfigi/s7-5-missing-figi"

_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SUPPORTED_MICS: Final = frozenset({"XNAS", "XNYS", "XASE"})
_MIC_TO_NASDAQ_EXCHANGE: Final = {"XNYS": "N", "XASE": "A"}
_RETRIABLE_STATUS: Final = frozenset({429, 500, 502, 503, 504})
_RESPONSE_HEADER_ALLOWLIST: Final = frozenset(
    {
        "content-length",
        "content-type",
        "date",
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
    }
)
_OPENFIGI_RESPONSE_BYTES_CAP: Final = 8 * 1024 * 1024
_NASDAQ_RESPONSE_BYTES_CAP: Final = 16 * 1024 * 1024
_HTTP_ATTEMPTS: Final = 5
_HTTP_TIMEOUT_SECONDS: Final = 45.0
_PROVIDER: Final = "massive"
_LOCALE: Final = "us"
_RESOLVED: Final = "resolved_unique_corroborated"
_AMBIGUOUS: Final = "openfigi_ambiguous"
_NO_RESULT: Final = "openfigi_no_result"
_NO_MATCH: Final = "openfigi_no_matching_common_stock"
_LISTING_MISMATCH: Final = "listing_not_corroborated"
_DISPOSITIONS: Final = frozenset({_RESOLVED, _AMBIGUOUS, _NO_RESULT, _NO_MATCH, _LISTING_MISMATCH})


class ExternalFigiResolutionError(RuntimeError):
    """Raised when external identity evidence is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class ExternalHttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]
    final_url: str


class ExternalHttpTransport(Protocol):
    def __call__(
        self,
        *,
        method: str,
        url: str,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> ExternalHttpResponse: ...


@dataclass(frozen=True, slots=True)
class ExternalFigiResolutionAttempt:
    resolution_key_id: str
    source_partition: ArtifactPin
    source_partition_row_count: int
    source_session: date
    source_record_id: str
    ticker: str
    primary_exchange_mic: str
    observed_cik_normalized: str
    evidence_artifact: ArtifactPin
    evidence_id: str
    batch_index: int
    job_index: int
    disposition: str
    canonical_composite_figi: str | None
    canonical_share_class_figi: str | None
    canonical_asset_id: str | None
    canonical_share_class_id: str | None
    evidence_available_session: date

    def __post_init__(self) -> None:
        _digest(self.resolution_key_id, "resolution key ID")
        if not isinstance(self.source_partition, ArtifactPin):
            raise ExternalFigiResolutionError("source partition pin is invalid")
        _positive_int(self.source_partition_row_count, "source partition row count")
        _native_date(self.source_session, "source session")
        _digest(self.source_record_id, "source record ID")
        _ticker(self.ticker)
        if self.primary_exchange_mic not in _SUPPORTED_MICS:
            raise ExternalFigiResolutionError("attempt MIC is unsupported")
        if not _CIK.fullmatch(self.observed_cik_normalized):
            raise ExternalFigiResolutionError("attempt observed CIK is invalid")
        if not isinstance(self.evidence_artifact, ArtifactPin):
            raise ExternalFigiResolutionError("attempt evidence pin is invalid")
        _digest(self.evidence_id, "evidence ID")
        _nonnegative_int(self.batch_index, "batch index")
        _nonnegative_int(self.job_index, "job index")
        if self.disposition not in _DISPOSITIONS:
            raise ExternalFigiResolutionError("attempt disposition is invalid")
        _native_date(self.evidence_available_session, "evidence availability")
        expected_key = external_figi_resolution_key(
            ticker=self.ticker,
            primary_exchange_mic=self.primary_exchange_mic,
            observed_cik_normalized=self.observed_cik_normalized,
        )
        if self.resolution_key_id != expected_key:
            raise ExternalFigiResolutionError("attempt resolution key does not reproduce")
        values = (
            self.canonical_composite_figi,
            self.canonical_share_class_figi,
            self.canonical_asset_id,
            self.canonical_share_class_id,
        )
        if self.disposition == _RESOLVED:
            if any(not isinstance(value, str) for value in values):
                raise ExternalFigiResolutionError("resolved attempt lacks canonical identity")
            if not _FIGI.fullmatch(str(self.canonical_composite_figi)):
                raise ExternalFigiResolutionError("resolved Composite FIGI is invalid")
            if not _FIGI.fullmatch(str(self.canonical_share_class_figi)):
                raise ExternalFigiResolutionError("resolved Share Class FIGI is invalid")
            if self.canonical_asset_id != canonical_asset_id(str(self.canonical_composite_figi)):
                raise ExternalFigiResolutionError("resolved asset ID does not reproduce")
            if self.canonical_share_class_id != canonical_share_class_id(
                str(self.canonical_share_class_figi)
            ):
                raise ExternalFigiResolutionError("resolved Share Class ID does not reproduce")
        elif any(value is not None for value in values):
            raise ExternalFigiResolutionError("unresolved attempt carries canonical identity")

    @property
    def attempt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "batch_index": self.batch_index,
            "canonical_asset_id": self.canonical_asset_id,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "canonical_share_class_id": self.canonical_share_class_id,
            "disposition": self.disposition,
            "evidence_artifact": self.evidence_artifact.to_dict(),
            "evidence_available_session": self.evidence_available_session.isoformat(),
            "evidence_id": self.evidence_id,
            "job_index": self.job_index,
            "observed_cik_normalized": self.observed_cik_normalized,
            "primary_exchange_mic": self.primary_exchange_mic,
            "resolution_key_id": self.resolution_key_id,
            "source_partition": self.source_partition.to_dict(),
            "source_partition_row_count": self.source_partition_row_count,
            "source_record_id": self.source_record_id,
            "source_session": self.source_session.isoformat(),
            "ticker": self.ticker,
        }

    def to_dict(self) -> dict[str, object]:
        return {"attempt_id": self.attempt_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(
            value,
            {
                "attempt_id",
                "batch_index",
                "canonical_asset_id",
                "canonical_composite_figi",
                "canonical_share_class_figi",
                "canonical_share_class_id",
                "disposition",
                "evidence_artifact",
                "evidence_available_session",
                "evidence_id",
                "job_index",
                "observed_cik_normalized",
                "primary_exchange_mic",
                "resolution_key_id",
                "source_partition",
                "source_partition_row_count",
                "source_record_id",
                "source_session",
                "ticker",
            },
            "external FIGI attempt",
        )
        result = cls(
            resolution_key_id=_text(item["resolution_key_id"], "resolution key ID"),
            source_partition=_artifact(item["source_partition"]),
            source_partition_row_count=_int(
                item["source_partition_row_count"], "source partition row count"
            ),
            source_session=_date(item["source_session"], "source session"),
            source_record_id=_text(item["source_record_id"], "source record ID"),
            ticker=_text(item["ticker"], "ticker"),
            primary_exchange_mic=_text(item["primary_exchange_mic"], "MIC"),
            observed_cik_normalized=_text(item["observed_cik_normalized"], "observed CIK"),
            evidence_artifact=_artifact(item["evidence_artifact"]),
            evidence_id=_text(item["evidence_id"], "evidence ID"),
            batch_index=_int(item["batch_index"], "batch index"),
            job_index=_int(item["job_index"], "job index"),
            disposition=_text(item["disposition"], "disposition"),
            canonical_composite_figi=_optional_text(item["canonical_composite_figi"]),
            canonical_share_class_figi=_optional_text(item["canonical_share_class_figi"]),
            canonical_asset_id=_optional_text(item["canonical_asset_id"]),
            canonical_share_class_id=_optional_text(item["canonical_share_class_id"]),
            evidence_available_session=_date(
                item["evidence_available_session"], "evidence availability"
            ),
        )
        if item["attempt_id"] != result.attempt_id:
            raise ExternalFigiResolutionError("external FIGI attempt ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class ExternalFigiResolutionRelease:
    release_available_session: date
    attempts: tuple[ExternalFigiResolutionAttempt, ...]
    evidence_artifacts: tuple[ArtifactPin, ...]
    parent_release_artifact: ArtifactPin | None = None

    def __post_init__(self) -> None:
        _native_date(self.release_available_session, "release availability")
        if not self.attempts:
            raise ExternalFigiResolutionError("external FIGI release cannot be empty")
        keys = tuple(item.resolution_key_id for item in self.attempts)
        if keys != tuple(sorted(set(keys))):
            raise ExternalFigiResolutionError("external FIGI attempts are not key-sorted unique")
        evidence = tuple(item.path for item in self.evidence_artifacts)
        if evidence != tuple(sorted(set(evidence))):
            raise ExternalFigiResolutionError("evidence pins are not path-sorted unique")
        evidence_by_path = {item.path: item for item in self.evidence_artifacts}
        for attempt in self.attempts:
            if evidence_by_path.get(attempt.evidence_artifact.path) != attempt.evidence_artifact:
                raise ExternalFigiResolutionError("attempt evidence is absent from release")
            if attempt.evidence_available_session > self.release_available_session:
                raise ExternalFigiResolutionError("release predates attempt evidence")
        if self.parent_release_artifact is not None and not isinstance(
            self.parent_release_artifact, ArtifactPin
        ):
            raise ExternalFigiResolutionError("parent release pin is invalid")

    @property
    def release_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def resolved_count(self) -> int:
        return sum(item.disposition == _RESOLVED for item in self.attempts)

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_external_figi_resolution_release",
            "attempts": [item.to_dict() for item in self.attempts],
            "evidence_artifacts": [item.to_dict() for item in self.evidence_artifacts],
            "locale": _LOCALE,
            "parent_release_artifact": (
                None
                if self.parent_release_artifact is None
                else self.parent_release_artifact.to_dict()
            ),
            "provider": _PROVIDER,
            "release_available_session": self.release_available_session.isoformat(),
            "rule_version": EXTERNAL_FIGI_RESOLUTION_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "release_id": self.release_id,
            "resolved_count": self.resolved_count,
            "attempt_count": len(self.attempts),
            **self.logical_payload(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(
            value,
            {
                "artifact_type",
                "attempt_count",
                "attempts",
                "evidence_artifacts",
                "locale",
                "parent_release_artifact",
                "provider",
                "release_available_session",
                "release_id",
                "resolved_count",
                "rule_version",
            },
            "external FIGI release",
        )
        if (
            item["artifact_type"] != "s7_5_external_figi_resolution_release"
            or item["provider"] != _PROVIDER
            or item["locale"] != _LOCALE
            or item["rule_version"] != EXTERNAL_FIGI_RESOLUTION_RULE_VERSION
        ):
            raise ExternalFigiResolutionError("external FIGI release identity differs")
        attempts_value = item["attempts"]
        evidence_value = item["evidence_artifacts"]
        if not isinstance(attempts_value, list) or not isinstance(evidence_value, list):
            raise ExternalFigiResolutionError("external FIGI release arrays are invalid")
        result = cls(
            release_available_session=_date(
                item["release_available_session"], "release availability"
            ),
            attempts=tuple(
                ExternalFigiResolutionAttempt.from_dict(value) for value in attempts_value
            ),
            evidence_artifacts=tuple(_artifact(value) for value in evidence_value),
            parent_release_artifact=(
                None
                if item["parent_release_artifact"] is None
                else _artifact(item["parent_release_artifact"])
            ),
        )
        if (
            item["release_id"] != result.release_id
            or item["attempt_count"] != len(result.attempts)
            or item["resolved_count"] != result.resolved_count
        ):
            raise ExternalFigiResolutionError("external FIGI release summary does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class ExternalFigiApplicationSummary:
    active_cs_missing_source_figi_rows: int
    externally_resolved_rows: int
    remaining_rows: int
    missing_cik_rows: int
    unsupported_mic_rows: int

    def to_dict(self) -> dict[str, object]:
        return {
            "active_cs_missing_source_figi_rows": self.active_cs_missing_source_figi_rows,
            "externally_resolved_rows": self.externally_resolved_rows,
            "missing_cik_rows": self.missing_cik_rows,
            "remaining_rows": self.remaining_rows,
            "unsupported_mic_rows": self.unsupported_mic_rows,
        }


@dataclass(frozen=True, slots=True)
class ExternalFigiBackfillResult:
    release: ExternalFigiResolutionRelease
    release_artifact: ArtifactPin
    application_summary: ExternalFigiApplicationSummary
    reused: bool


@dataclass(frozen=True, slots=True)
class EffectiveExternalIdentity:
    resolution_key_id: str
    canonical_composite_figi: str
    canonical_share_class_figi: str
    canonical_asset_id: str
    canonical_share_class_id: str
    evidence_available_session: date


def external_figi_resolution_key(
    *,
    ticker: str,
    primary_exchange_mic: str,
    observed_cik_normalized: str,
) -> str:
    _ticker(ticker)
    if primary_exchange_mic not in _SUPPORTED_MICS:
        raise ExternalFigiResolutionError("resolution key MIC is unsupported")
    if not _CIK.fullmatch(observed_cik_normalized):
        raise ExternalFigiResolutionError("resolution key CIK is invalid")
    return stable_digest(
        {
            "locale": _LOCALE,
            "observed_cik_normalized": observed_cik_normalized,
            "primary_exchange_mic": primary_exchange_mic,
            "provider": _PROVIDER,
            "rule_version": EXTERNAL_FIGI_RESOLUTION_RULE_VERSION,
            "ticker": ticker,
        }
    )


def load_external_figi_resolution_release_exact(
    data_root: Path,
    artifact: ArtifactPin,
) -> ExternalFigiResolutionRelease:
    root = data_root.expanduser().resolve()
    content = _read_exact(root, artifact, "external FIGI release")
    value = _json(content, "external FIGI release")
    if _canonical(value) != content:
        raise ExternalFigiResolutionError("external FIGI release is not canonical JSON")
    release = ExternalFigiResolutionRelease.from_dict(value)
    expected = f"{EXTERNAL_FIGI_RELEASE_ROOT}/release_id={release.release_id}/manifest.json"
    if artifact.path != expected:
        raise ExternalFigiResolutionError("external FIGI release path is not canonical")
    return release


def verify_external_figi_resolution_release(
    data_root: Path,
    artifact: ArtifactPin,
    *,
    current_target_artifact: ArtifactPin,
    current_target_row_count: int,
    current_terminal_session: date,
    calendar: object,
) -> tuple[ExternalFigiResolutionRelease, ExternalFigiApplicationSummary]:
    """Replay evidence, source rows, and the current overlay application."""

    root = data_root.expanduser().resolve()
    release = load_external_figi_resolution_release_exact(root, artifact)
    if release.parent_release_artifact is not None:
        parent = load_external_figi_resolution_release_exact(root, release.parent_release_artifact)
        current_attempts = {item.resolution_key_id: item.to_dict() for item in release.attempts}
        for parent_attempt in parent.attempts:
            child_attempt = current_attempts.get(parent_attempt.resolution_key_id)
            if child_attempt is None:
                raise ExternalFigiResolutionError("external FIGI parent attempt was dropped")
            if (
                parent_attempt.disposition == _RESOLVED
                and child_attempt != parent_attempt.to_dict()
            ):
                raise ExternalFigiResolutionError(
                    "resolved external FIGI parent attempt was rewritten"
                )
        if parent.release_available_session > release.release_available_session:
            raise ExternalFigiResolutionError("external FIGI child release predates parent")

    evidence_by_id: dict[str, dict[str, object]] = {}
    evidence_available: dict[str, date] = {}
    for evidence_pin in release.evidence_artifacts:
        document = _load_evidence(root, evidence_pin, calendar=calendar)
        evidence_id = _text(document["evidence_id"], "evidence ID")
        if evidence_id in evidence_by_id:
            raise ExternalFigiResolutionError("duplicate external FIGI evidence ID")
        evidence_by_id[evidence_id] = document
        evidence_available[evidence_id] = _date(
            document["evidence_available_session"], "evidence availability"
        )

    source_rows_by_pin: dict[ArtifactPin, dict[str, dict[str, object]]] = {}
    for attempt in release.attempts:
        source_rows = source_rows_by_pin.get(attempt.source_partition)
        if source_rows is None:
            table = readback_i3_migration_parquet_exact(
                data_root=root,
                artifact=attempt.source_partition,
                table_name="universe_daily",
                row_count=attempt.source_partition_row_count,
                session_date=attempt.source_session,
            )
            values = table.to_pylist()
            source_rows = {}
            for row in values:
                source_id = row.get("selected_source_record_id")
                if not isinstance(source_id, str) or source_id in source_rows:
                    raise ExternalFigiResolutionError(
                        "external FIGI source partition record IDs are invalid"
                    )
                source_rows[source_id] = row
            source_rows_by_pin[attempt.source_partition] = source_rows
        row = source_rows.get(attempt.source_record_id)
        if row is None:
            raise ExternalFigiResolutionError("external FIGI source row is missing")
        _require_attempt_source_row(attempt, row)
        evidence = evidence_by_id.get(attempt.evidence_id)
        if evidence is None or attempt.evidence_artifact not in release.evidence_artifacts:
            raise ExternalFigiResolutionError("external FIGI attempt evidence is missing")
        if evidence_available[attempt.evidence_id] != attempt.evidence_available_session:
            raise ExternalFigiResolutionError("attempt evidence availability differs")
        expected = _attempt_from_evidence(
            attempt=attempt,
            evidence=evidence,
        )
        if attempt.to_dict() != expected.to_dict():
            raise ExternalFigiResolutionError("external FIGI attempt does not replay")

    table = readback_i3_migration_parquet_exact(
        data_root=root,
        artifact=current_target_artifact,
        table_name="universe_daily",
        row_count=current_target_row_count,
        session_date=current_terminal_session,
    )
    rows = table.to_pylist()
    summary = summarize_external_figi_application(rows, release)
    return release, summary


def summarize_external_figi_application(
    rows: Sequence[Mapping[str, object]],
    release: ExternalFigiResolutionRelease | None,
) -> ExternalFigiApplicationSummary:
    resolved = {
        item.resolution_key_id: item
        for item in (() if release is None else release.attempts)
        if item.disposition == _RESOLVED
    }
    missing = 0
    applied = 0
    missing_cik = 0
    unsupported_mic = 0
    seen_tickers: set[str] = set()
    for row in rows:
        ticker = row.get("ticker")
        if not isinstance(ticker, str) or not ticker or ticker in seen_tickers:
            raise ExternalFigiResolutionError("target partition ticker grain is invalid")
        seen_tickers.add(ticker)
        if not _is_missing_figi_active_cs(row):
            continue
        missing += 1
        cik = row.get("observed_cik_normalized")
        mic = row.get("primary_exchange_mic")
        if not isinstance(cik, str) or not _CIK.fullmatch(cik):
            missing_cik += 1
            continue
        if mic not in _SUPPORTED_MICS:
            unsupported_mic += 1
            continue
        key = external_figi_resolution_key(
            ticker=ticker,
            primary_exchange_mic=str(mic),
            observed_cik_normalized=cik,
        )
        applied += int(key in resolved)
    return ExternalFigiApplicationSummary(
        active_cs_missing_source_figi_rows=missing,
        externally_resolved_rows=applied,
        remaining_rows=missing - applied,
        missing_cik_rows=missing_cik,
        unsupported_mic_rows=unsupported_mic,
    )


def effective_external_identity(
    row: Mapping[str, object],
    release: ExternalFigiResolutionRelease | None,
) -> EffectiveExternalIdentity | None:
    """Return the overlay identity without mutating the observed row."""

    if release is None or not _is_missing_figi_active_cs(row):
        return None
    ticker = row.get("ticker")
    mic = row.get("primary_exchange_mic")
    cik = row.get("observed_cik_normalized")
    if (
        not isinstance(ticker, str)
        or mic not in _SUPPORTED_MICS
        or not isinstance(cik, str)
        or not _CIK.fullmatch(cik)
    ):
        return None
    key = external_figi_resolution_key(
        ticker=ticker,
        primary_exchange_mic=str(mic),
        observed_cik_normalized=cik,
    )
    attempt = next(
        (
            item
            for item in release.attempts
            if item.resolution_key_id == key and item.disposition == _RESOLVED
        ),
        None,
    )
    if attempt is None:
        return None
    return EffectiveExternalIdentity(
        resolution_key_id=key,
        canonical_composite_figi=str(attempt.canonical_composite_figi),
        canonical_share_class_figi=str(attempt.canonical_share_class_figi),
        canonical_asset_id=str(attempt.canonical_asset_id),
        canonical_share_class_id=str(attempt.canonical_share_class_id),
        evidence_available_session=attempt.evidence_available_session,
    )


def capture_external_figi_resolution(
    data_root: Path,
    *,
    target_artifact: ArtifactPin,
    target_row_count: int,
    target_session: date,
    calendar: object,
    existing_release_artifact: ArtifactPin | None = None,
    api_key: str | None = None,
    refresh_unresolved: bool = False,
    transport: ExternalHttpTransport | None = None,
    clock: Callable[[], datetime] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> ExternalFigiBackfillResult:
    """Capture unresolved rows and create a self-contained immutable release."""

    root = data_root.expanduser().resolve()
    _positive_int(target_row_count, "target row count")
    _native_date(target_session, "target session")
    table = readback_i3_migration_parquet_exact(
        data_root=root,
        artifact=target_artifact,
        table_name="universe_daily",
        row_count=target_row_count,
        session_date=target_session,
    )
    rows = table.to_pylist()
    existing: ExternalFigiResolutionRelease | None = None
    if existing_release_artifact is not None:
        existing, _ = verify_external_figi_resolution_release(
            root,
            existing_release_artifact,
            current_target_artifact=target_artifact,
            current_target_row_count=target_row_count,
            current_terminal_session=target_session,
            calendar=calendar,
        )
    existing_by_key = {
        item.resolution_key_id: item for item in (() if existing is None else existing.attempts)
    }
    candidates = _queryable_candidates(
        rows,
        target_artifact=target_artifact,
        target_row_count=target_row_count,
        target_session=target_session,
    )
    pending = [
        item
        for item in candidates
        if item["resolution_key_id"] not in existing_by_key
        or (
            refresh_unresolved
            and existing_by_key[item["resolution_key_id"]].disposition != _RESOLVED
        )
    ]
    if not pending:
        if existing is None or existing_release_artifact is None:
            raise ExternalFigiResolutionError(
                "no queryable missing-FIGI common-stock rows were found"
            )
        return ExternalFigiBackfillResult(
            release=existing,
            release_artifact=existing_release_artifact,
            application_summary=summarize_external_figi_application(rows, existing),
            reused=True,
        )

    if api_key is not None and (
        not isinstance(api_key, str) or not api_key or api_key != api_key.strip()
    ):
        raise ExternalFigiResolutionError("OpenFIGI API key must be trimmed nonempty text")
    actual_transport = transport or _default_transport
    actual_clock = clock or (lambda: datetime.now(UTC))
    actual_sleeper = sleeper or time.sleep
    auth_mode = "api_key" if api_key else "anonymous"
    batch_size = 100 if api_key else 5
    cadence = 0.25 if api_key else 2.5
    source_set_id = stable_digest(
        {
            "auth_mode": auth_mode,
            "candidates": [item["resolution_key_id"] for item in pending],
            "rule_version": EXTERNAL_FIGI_EVIDENCE_RULE_VERSION,
            "target_artifact": target_artifact.to_dict(),
            "target_session": target_session.isoformat(),
        }
    )
    workspace = safe_relative_path(
        root,
        f"tmp/s7-5-external-figi/source_set_id={source_set_id}",
    )
    workspace.mkdir(parents=True, exist_ok=True)
    cache_paths: list[Path] = []
    snapshots: list[dict[str, object]] = []
    for dataset, url in (
        ("nasdaqlisted", NASDAQ_LISTED_URL),
        ("otherlisted", NASDAQ_OTHER_LISTED_URL),
    ):
        cache = workspace / f"{dataset}.json"
        cache_paths.append(cache)
        response = _cached_fetch(
            cache,
            method="GET",
            url=url,
            body=None,
            headers={"Accept": "text/plain", "User-Agent": "ame-stocks/0.1"},
            size_cap=_NASDAQ_RESPONSE_BYTES_CAP,
            transport=actual_transport,
            clock=actual_clock,
            sleeper=actual_sleeper,
        )
        try:
            _pipe_rows(_decode_body(response, f"{dataset} snapshot"))
        except ExternalFigiResolutionError:
            cache.unlink(missing_ok=True)
            raise
        snapshots.append(
            {
                "dataset": dataset,
                **response,
            }
        )
    batches: list[dict[str, object]] = []
    for batch_index, offset in enumerate(range(0, len(pending), batch_size)):
        selected = pending[offset : offset + batch_size]
        queries = [
            {
                "job": {
                    "exchCode": "US",
                    "idType": "TICKER",
                    "idValue": item["ticker"],
                    "marketSecDes": "Equity",
                },
                "resolution_key_id": item["resolution_key_id"],
            }
            for item in selected
        ]
        request_body = _canonical([item["job"] for item in queries])[:-1]
        cache = workspace / f"openfigi-batch-{batch_index:05d}.json"
        cache_paths.append(cache)
        if batch_index and not cache.is_file():
            actual_sleeper(cadence)
        response = _cached_fetch(
            cache,
            method="POST",
            url=OPENFIGI_MAPPING_URL,
            body=request_body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "ame-stocks/0.1",
                **({"X-OPENFIGI-APIKEY": api_key} if api_key else {}),
            },
            size_cap=_OPENFIGI_RESPONSE_BYTES_CAP,
            transport=actual_transport,
            clock=actual_clock,
            sleeper=actual_sleeper,
        )
        try:
            _require_openfigi_response_cardinality(response, len(queries))
        except ExternalFigiResolutionError:
            cache.unlink(missing_ok=True)
            raise
        batches.append(
            {
                "batch_index": batch_index,
                "queries": queries,
                "request_bytes": len(request_body),
                "request_sha256": hashlib.sha256(request_body).hexdigest(),
                **response,
            }
        )

    completed_at = _utc(actual_clock(), "capture completion")
    timestamps = [completed_at]
    timestamps.extend(
        _datetime(item["received_at_utc"], "response timestamp") for item in snapshots
    )
    timestamps.extend(_datetime(item["received_at_utc"], "response timestamp") for item in batches)
    controlling_time = max(timestamps)
    try:
        evidence_available, _ = calendar.first_open_after(controlling_time)
    except Exception as exc:
        raise ExternalFigiResolutionError(
            "bound XNYS calendar cannot derive evidence availability"
        ) from exc
    evidence_body: dict[str, object] = {
        "artifact_type": "s7_5_external_figi_evidence",
        "auth_mode": auth_mode,
        "batches": batches,
        "captured_completed_at_utc": completed_at.isoformat(),
        "evidence_available_session": evidence_available.isoformat(),
        "nasdaq_snapshots": snapshots,
        "openfigi_mapping_url": OPENFIGI_MAPPING_URL,
        "rule_version": EXTERNAL_FIGI_EVIDENCE_RULE_VERSION,
        "source_set_id": source_set_id,
        "target_artifact": target_artifact.to_dict(),
        "target_row_count": target_row_count,
        "target_session": target_session.isoformat(),
    }
    evidence_id = stable_digest(evidence_body)
    evidence_document = {"evidence_id": evidence_id, **evidence_body}
    evidence_pin = _write(
        root,
        f"{EXTERNAL_FIGI_EVIDENCE_ROOT}/capture_id={evidence_id}/evidence.json",
        _canonical(evidence_document),
    )
    new_attempts = tuple(
        _attempt_from_capture_candidate(
            candidate=item,
            evidence_artifact=evidence_pin,
            evidence=evidence_document,
        )
        for item in pending
    )
    merged = dict(existing_by_key)
    for attempt in new_attempts:
        prior = merged.get(attempt.resolution_key_id)
        if prior is None or prior.disposition != _RESOLVED:
            merged[attempt.resolution_key_id] = attempt
    evidence_pins = {
        item.path: item for item in (() if existing is None else existing.evidence_artifacts)
    }
    evidence_pins[evidence_pin.path] = evidence_pin
    release = ExternalFigiResolutionRelease(
        release_available_session=max(
            evidence_available,
            date.min if existing is None else existing.release_available_session,
        ),
        attempts=tuple(merged[key] for key in sorted(merged)),
        evidence_artifacts=tuple(evidence_pins[key] for key in sorted(evidence_pins)),
        parent_release_artifact=existing_release_artifact,
    )
    release_pin = _write(
        root,
        f"{EXTERNAL_FIGI_RELEASE_ROOT}/release_id={release.release_id}/manifest.json",
        release.canonical_bytes(),
    )
    verified, summary = verify_external_figi_resolution_release(
        root,
        release_pin,
        current_target_artifact=target_artifact,
        current_target_row_count=target_row_count,
        current_terminal_session=target_session,
        calendar=calendar,
    )
    if verified != release:
        raise ExternalFigiResolutionError("stored external FIGI release differs after replay")
    _cleanup_workspace(cache_paths, workspace)
    return ExternalFigiBackfillResult(
        release=release,
        release_artifact=release_pin,
        application_summary=summary,
        reused=False,
    )


def _queryable_candidates(
    rows: Sequence[Mapping[str, object]],
    *,
    target_artifact: ArtifactPin,
    target_row_count: int,
    target_session: date,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if not _is_missing_figi_active_cs(row):
            continue
        ticker = row.get("ticker")
        mic = row.get("primary_exchange_mic")
        cik = row.get("observed_cik_normalized")
        source_record = row.get("selected_source_record_id")
        if (
            not isinstance(ticker, str)
            or mic not in _SUPPORTED_MICS
            or not isinstance(cik, str)
            or not _CIK.fullmatch(cik)
        ):
            continue
        _digest(source_record, "selected source record ID")
        key = external_figi_resolution_key(
            ticker=ticker,
            primary_exchange_mic=str(mic),
            observed_cik_normalized=cik,
        )
        if key in seen:
            raise ExternalFigiResolutionError("target has duplicate external resolution key")
        seen.add(key)
        candidates.append(
            {
                "observed_cik_normalized": cik,
                "primary_exchange_mic": mic,
                "resolution_key_id": key,
                "source_partition": target_artifact,
                "source_partition_row_count": target_row_count,
                "source_record_id": source_record,
                "source_session": target_session,
                "ticker": ticker,
            }
        )
    return sorted(candidates, key=lambda item: str(item["resolution_key_id"]))


def _attempt_from_capture_candidate(
    *,
    candidate: Mapping[str, object],
    evidence_artifact: ArtifactPin,
    evidence: Mapping[str, object],
) -> ExternalFigiResolutionAttempt:
    batches = evidence.get("batches")
    if not isinstance(batches, list):
        raise ExternalFigiResolutionError("evidence batches are invalid")
    key = candidate["resolution_key_id"]
    for batch in batches:
        if not isinstance(batch, dict) or not isinstance(batch.get("queries"), list):
            raise ExternalFigiResolutionError("evidence batch is invalid")
        for job_index, query in enumerate(batch["queries"]):
            if isinstance(query, dict) and query.get("resolution_key_id") == key:
                disposition, composite, share = _classify_query(
                    evidence=evidence,
                    batch=batch,
                    job_index=job_index,
                    ticker=str(candidate["ticker"]),
                    mic=str(candidate["primary_exchange_mic"]),
                )
                return ExternalFigiResolutionAttempt(
                    resolution_key_id=str(key),
                    source_partition=candidate["source_partition"],  # type: ignore[arg-type]
                    source_partition_row_count=int(candidate["source_partition_row_count"]),
                    source_session=candidate["source_session"],  # type: ignore[arg-type]
                    source_record_id=str(candidate["source_record_id"]),
                    ticker=str(candidate["ticker"]),
                    primary_exchange_mic=str(candidate["primary_exchange_mic"]),
                    observed_cik_normalized=str(candidate["observed_cik_normalized"]),
                    evidence_artifact=evidence_artifact,
                    evidence_id=str(evidence["evidence_id"]),
                    batch_index=int(batch["batch_index"]),
                    job_index=job_index,
                    disposition=disposition,
                    canonical_composite_figi=composite,
                    canonical_share_class_figi=share,
                    canonical_asset_id=(
                        None if composite is None else canonical_asset_id(composite)
                    ),
                    canonical_share_class_id=(
                        None if share is None else canonical_share_class_id(share)
                    ),
                    evidence_available_session=_date(
                        evidence["evidence_available_session"], "evidence availability"
                    ),
                )
    raise ExternalFigiResolutionError("candidate query is absent from evidence")


def _attempt_from_evidence(
    *,
    attempt: ExternalFigiResolutionAttempt,
    evidence: Mapping[str, object],
) -> ExternalFigiResolutionAttempt:
    batches = evidence.get("batches")
    if not isinstance(batches, list) or attempt.batch_index >= len(batches):
        raise ExternalFigiResolutionError("attempt batch locator is invalid")
    batch = batches[attempt.batch_index]
    if not isinstance(batch, dict) or batch.get("batch_index") != attempt.batch_index:
        raise ExternalFigiResolutionError("attempt batch identity differs")
    queries = batch.get("queries")
    if not isinstance(queries, list) or attempt.job_index >= len(queries):
        raise ExternalFigiResolutionError("attempt job locator is invalid")
    query = queries[attempt.job_index]
    if not isinstance(query, dict) or query.get("resolution_key_id") != attempt.resolution_key_id:
        raise ExternalFigiResolutionError("attempt query key differs")
    disposition, composite, share = _classify_query(
        evidence=evidence,
        batch=batch,
        job_index=attempt.job_index,
        ticker=attempt.ticker,
        mic=attempt.primary_exchange_mic,
    )
    return ExternalFigiResolutionAttempt(
        resolution_key_id=attempt.resolution_key_id,
        source_partition=attempt.source_partition,
        source_partition_row_count=attempt.source_partition_row_count,
        source_session=attempt.source_session,
        source_record_id=attempt.source_record_id,
        ticker=attempt.ticker,
        primary_exchange_mic=attempt.primary_exchange_mic,
        observed_cik_normalized=attempt.observed_cik_normalized,
        evidence_artifact=attempt.evidence_artifact,
        evidence_id=attempt.evidence_id,
        batch_index=attempt.batch_index,
        job_index=attempt.job_index,
        disposition=disposition,
        canonical_composite_figi=composite,
        canonical_share_class_figi=share,
        canonical_asset_id=None if composite is None else canonical_asset_id(composite),
        canonical_share_class_id=None if share is None else canonical_share_class_id(share),
        evidence_available_session=attempt.evidence_available_session,
    )


def _classify_query(
    *,
    evidence: Mapping[str, object],
    batch: Mapping[str, object],
    job_index: int,
    ticker: str,
    mic: str,
) -> tuple[str, str | None, str | None]:
    body = _decode_body(batch, "OpenFIGI batch")
    response = _json(body, "OpenFIGI response")
    if not isinstance(response, list):
        raise ExternalFigiResolutionError("OpenFIGI response must be an array")
    queries = batch.get("queries")
    if not isinstance(queries, list) or len(response) != len(queries):
        raise ExternalFigiResolutionError("OpenFIGI response cardinality differs")
    item = response[job_index]
    if not isinstance(item, dict):
        raise ExternalFigiResolutionError("OpenFIGI result envelope is invalid")
    data = item.get("data")
    if data is None:
        if "error" in item or "warning" in item:
            return _NO_RESULT, None, None
        raise ExternalFigiResolutionError("OpenFIGI result lacks data or error")
    if not isinstance(data, list):
        raise ExternalFigiResolutionError("OpenFIGI result data is invalid")
    pairs: set[tuple[str, str]] = set()
    for result in data:
        if not isinstance(result, dict):
            raise ExternalFigiResolutionError("OpenFIGI data row is invalid")
        composite = result.get("compositeFIGI")
        share = result.get("shareClassFIGI")
        if (
            result.get("ticker") == ticker
            and result.get("marketSector") == "Equity"
            and result.get("securityType2") == "Common Stock"
            and isinstance(composite, str)
            and isinstance(share, str)
            and _FIGI.fullmatch(composite)
            and _FIGI.fullmatch(share)
        ):
            pairs.add((composite, share))
    if not pairs:
        return (_NO_RESULT if not data else _NO_MATCH), None, None
    if len(pairs) != 1:
        return _AMBIGUOUS, None, None
    composite, share = next(iter(pairs))
    if not _listing_corroborates(evidence, ticker=ticker, mic=mic):
        return _LISTING_MISMATCH, None, None
    return _RESOLVED, composite, share


def _listing_corroborates(
    evidence: Mapping[str, object],
    *,
    ticker: str,
    mic: str,
) -> bool:
    snapshots = evidence.get("nasdaq_snapshots")
    if not isinstance(snapshots, list) or len(snapshots) != 2:
        raise ExternalFigiResolutionError("Nasdaq snapshots are incomplete")
    by_dataset = {item.get("dataset"): item for item in snapshots if isinstance(item, dict)}
    if set(by_dataset) != {"nasdaqlisted", "otherlisted"}:
        raise ExternalFigiResolutionError("Nasdaq snapshot datasets differ")
    if mic == "XNAS":
        rows = _pipe_rows(_decode_body(by_dataset["nasdaqlisted"], "nasdaqlisted"))
        return any(row.get("Symbol") == ticker and row.get("Test Issue") == "N" for row in rows)
    rows = _pipe_rows(_decode_body(by_dataset["otherlisted"], "otherlisted"))
    exchange = _MIC_TO_NASDAQ_EXCHANGE[mic]
    return any(
        row.get("ACT Symbol") == ticker
        and row.get("Exchange") == exchange
        and row.get("Test Issue") == "N"
        for row in rows
    )


def _pipe_rows(content: bytes) -> list[dict[str, str]]:
    try:
        lines = content.decode("utf-8-sig").splitlines()
    except UnicodeDecodeError as exc:
        raise ExternalFigiResolutionError("Nasdaq symbol directory is not UTF-8") from exc
    if len(lines) < 2 or "|" not in lines[0]:
        raise ExternalFigiResolutionError("Nasdaq symbol directory is malformed")
    header = lines[0].split("|")
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        if not line or line.startswith("File Creation Time"):
            continue
        fields = line.split("|")
        if len(fields) != len(header):
            raise ExternalFigiResolutionError("Nasdaq symbol directory row width differs")
        rows.append(dict(zip(header, fields, strict=True)))
    return rows


def _load_evidence(
    root: Path,
    artifact: ArtifactPin,
    *,
    calendar: object,
) -> dict[str, object]:
    content = _read_exact(root, artifact, "external FIGI evidence")
    value = _json(content, "external FIGI evidence")
    if _canonical(value) != content:
        raise ExternalFigiResolutionError("external FIGI evidence is not canonical JSON")
    item = _closed(
        value,
        {
            "artifact_type",
            "auth_mode",
            "batches",
            "captured_completed_at_utc",
            "evidence_available_session",
            "evidence_id",
            "nasdaq_snapshots",
            "openfigi_mapping_url",
            "rule_version",
            "source_set_id",
            "target_artifact",
            "target_row_count",
            "target_session",
        },
        "external FIGI evidence",
    )
    if (
        item["artifact_type"] != "s7_5_external_figi_evidence"
        or item["rule_version"] != EXTERNAL_FIGI_EVIDENCE_RULE_VERSION
        or item["openfigi_mapping_url"] != OPENFIGI_MAPPING_URL
        or item["auth_mode"] not in {"anonymous", "api_key"}
    ):
        raise ExternalFigiResolutionError("external FIGI evidence identity differs")
    body = dict(item)
    evidence_id = body.pop("evidence_id")
    if evidence_id != stable_digest(body):
        raise ExternalFigiResolutionError("external FIGI evidence ID does not reproduce")
    expected_path = f"{EXTERNAL_FIGI_EVIDENCE_ROOT}/capture_id={evidence_id}/evidence.json"
    if artifact.path != expected_path:
        raise ExternalFigiResolutionError("external FIGI evidence path is not canonical")
    batches = item["batches"]
    snapshots = item["nasdaq_snapshots"]
    if not isinstance(batches, list) or not batches:
        raise ExternalFigiResolutionError("external FIGI evidence has no batches")
    if not isinstance(snapshots, list) or len(snapshots) != 2:
        raise ExternalFigiResolutionError("external FIGI evidence snapshots differ")
    received: list[datetime] = [_datetime(item["captured_completed_at_utc"], "capture completion")]
    for expected_index, batch in enumerate(batches):
        if not isinstance(batch, dict) or set(batch) != {
            "batch_index",
            "body_base64",
            "body_bytes",
            "body_sha256",
            "final_url",
            "queries",
            "received_at_utc",
            "request_bytes",
            "request_sha256",
            "response_headers",
            "status",
        }:
            raise ExternalFigiResolutionError("OpenFIGI evidence batch fields differ")
        if batch["batch_index"] != expected_index:
            raise ExternalFigiResolutionError("OpenFIGI batch indices are not contiguous")
        queries = batch["queries"]
        if not isinstance(queries, list) or not queries:
            raise ExternalFigiResolutionError("OpenFIGI batch queries are invalid")
        maximum = 100 if item["auth_mode"] == "api_key" else 5
        if len(queries) > maximum:
            raise ExternalFigiResolutionError("OpenFIGI batch exceeds the authenticated mode")
        jobs: list[object] = []
        for query in queries:
            query_item = _closed(query, {"job", "resolution_key_id"}, "OpenFIGI query")
            _digest(query_item["resolution_key_id"], "resolution key ID")
            job = _closed(
                query_item["job"],
                {"exchCode", "idType", "idValue", "marketSecDes"},
                "OpenFIGI job",
            )
            if (
                job["exchCode"] != "US"
                or job["idType"] != "TICKER"
                or job["marketSecDes"] != "Equity"
            ):
                raise ExternalFigiResolutionError("OpenFIGI job semantics differ")
            _ticker(job["idValue"])
            jobs.append(job)
        request = _canonical(jobs)[:-1]
        if (
            batch["request_bytes"] != len(request)
            or batch["request_sha256"] != hashlib.sha256(request).hexdigest()
        ):
            raise ExternalFigiResolutionError("OpenFIGI request bytes do not reproduce")
        _verify_response_envelope(batch, expected_url=OPENFIGI_MAPPING_URL)
        received.append(_datetime(batch["received_at_utc"], "response timestamp"))
    datasets: set[str] = set()
    for snapshot in snapshots:
        snapshot_item = _closed(
            snapshot,
            {
                "body_base64",
                "body_bytes",
                "body_sha256",
                "dataset",
                "final_url",
                "received_at_utc",
                "response_headers",
                "status",
            },
            "Nasdaq snapshot",
        )
        dataset = snapshot_item["dataset"]
        if dataset not in {"nasdaqlisted", "otherlisted"} or dataset in datasets:
            raise ExternalFigiResolutionError("Nasdaq snapshot dataset is invalid")
        datasets.add(str(dataset))
        expected_url = NASDAQ_LISTED_URL if dataset == "nasdaqlisted" else NASDAQ_OTHER_LISTED_URL
        _verify_response_envelope(snapshot_item, expected_url=expected_url)
        _pipe_rows(_decode_body(snapshot_item, "Nasdaq snapshot"))
        received.append(_datetime(snapshot_item["received_at_utc"], "response timestamp"))
    controlling_time = max(received)
    try:
        expected_available, _ = calendar.first_open_after(controlling_time)
    except Exception as exc:
        raise ExternalFigiResolutionError(
            "bound XNYS calendar cannot replay evidence availability"
        ) from exc
    if item["evidence_available_session"] != expected_available.isoformat():
        raise ExternalFigiResolutionError("external FIGI evidence availability differs")
    return item


def _require_attempt_source_row(
    attempt: ExternalFigiResolutionAttempt,
    row: Mapping[str, object],
) -> None:
    if (
        row.get("session_date") != attempt.source_session
        or row.get("ticker") != attempt.ticker
        or row.get("primary_exchange_mic") != attempt.primary_exchange_mic
        or row.get("observed_cik_normalized") != attempt.observed_cik_normalized
        or row.get("selected_source_record_id") != attempt.source_record_id
        or not _is_missing_figi_active_cs(row)
    ):
        raise ExternalFigiResolutionError("external FIGI source row identity differs")


def _is_missing_figi_active_cs(row: Mapping[str, object]) -> bool:
    return (
        row.get("active_on_date") is True
        and row.get("type_code") == "CS"
        and row.get("observed_composite_figi") is None
        and row.get("observed_share_class_figi") is None
        and row.get("canonical_composite_figi") is None
        and row.get("canonical_share_class_figi") is None
        and row.get("asset_id") is None
        and row.get("share_class_id") is None
    )


def _require_openfigi_response_cardinality(
    response_envelope: Mapping[str, object],
    expected_count: int,
) -> None:
    response = _json(_decode_body(response_envelope, "OpenFIGI batch"), "OpenFIGI response")
    if not isinstance(response, list) or len(response) != expected_count:
        raise ExternalFigiResolutionError("OpenFIGI response cardinality differs")


def _cached_fetch(
    cache_path: Path,
    *,
    method: str,
    url: str,
    body: bytes | None,
    headers: Mapping[str, str],
    size_cap: int,
    transport: ExternalHttpTransport,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
) -> dict[str, object]:
    request_identity = stable_digest(
        {
            "body_sha256": None if body is None else hashlib.sha256(body).hexdigest(),
            "method": method,
            "url": url,
        }
    )
    if cache_path.is_file() and not cache_path.is_symlink():
        cached_content = cache_path.read_bytes()
        cached = _json(cached_content, "external HTTP resume cache")
        if _canonical(cached) != cached_content:
            raise ExternalFigiResolutionError("external HTTP resume cache is not canonical")
        item = _closed(cached, {"request_identity", "response"}, "HTTP resume cache")
        if item["request_identity"] != request_identity or not isinstance(item["response"], dict):
            raise ExternalFigiResolutionError("external HTTP resume cache request differs")
        _verify_response_envelope(item["response"], expected_url=url)
        return item["response"]
    response: ExternalHttpResponse | None = None
    for attempt in range(_HTTP_ATTEMPTS):
        try:
            response = transport(
                method=method,
                url=url,
                body=body,
                headers=headers,
                timeout_seconds=_HTTP_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            if attempt + 1 == _HTTP_ATTEMPTS:
                raise ExternalFigiResolutionError(f"external request failed: {url}") from exc
            sleeper(min(2**attempt, 30))
            continue
        if response.final_url != url:
            raise ExternalFigiResolutionError("external request redirected")
        if len(response.body) > size_cap:
            raise ExternalFigiResolutionError("external response exceeds byte cap")
        if response.status == 200:
            break
        if response.status not in _RETRIABLE_STATUS or attempt + 1 == _HTTP_ATTEMPTS:
            raise ExternalFigiResolutionError(
                f"external request returned HTTP {response.status}: {url}"
            )
        sleeper(_retry_delay(response.headers, attempt))
    if response is None or response.status != 200:  # pragma: no cover - loop guard
        raise ExternalFigiResolutionError("external request did not complete")
    received = _utc(clock(), "external response timestamp")
    result = {
        "body_base64": base64.b64encode(response.body).decode("ascii"),
        "body_bytes": len(response.body),
        "body_sha256": hashlib.sha256(response.body).hexdigest(),
        "final_url": response.final_url,
        "received_at_utc": received.isoformat(),
        "response_headers": _allowlisted_headers(response.headers),
        "status": response.status,
    }
    _atomic_write(
        cache_path, _canonical({"request_identity": request_identity, "response": result})
    )
    return result


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _default_transport(
    *,
    method: str,
    url: str,
    body: bytes | None,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> ExternalHttpResponse:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout_seconds) as handle:
            response_body = handle.read(
                max(_OPENFIGI_RESPONSE_BYTES_CAP, _NASDAQ_RESPONSE_BYTES_CAP) + 1
            )
            return ExternalHttpResponse(
                status=int(handle.status),
                body=response_body,
                headers={key: value for key, value in handle.headers.items()},
                final_url=str(handle.geturl()),
            )
    except urllib.error.HTTPError as exc:
        return ExternalHttpResponse(
            status=int(exc.code),
            body=exc.read(max(_OPENFIGI_RESPONSE_BYTES_CAP, _NASDAQ_RESPONSE_BYTES_CAP) + 1),
            headers={key: value for key, value in exc.headers.items()},
            final_url=str(exc.geturl()),
        )


def _allowlisted_headers(headers: Mapping[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in _RESPONSE_HEADER_ALLOWLIST:
            if not isinstance(value, str) or "\r" in value or "\n" in value:
                raise ExternalFigiResolutionError("external response header is invalid")
            result[lowered] = value
    return {key: result[key] for key in sorted(result)}


def _retry_delay(headers: Mapping[str, str], attempt: int) -> float:
    value = next(
        (item for key, item in headers.items() if key.lower() == "retry-after"),
        None,
    )
    if value is not None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = -1
        if 0 <= parsed <= 60:
            return parsed
    return float(min(2**attempt, 30))


def _verify_response_envelope(value: Mapping[str, object], *, expected_url: str) -> None:
    if value.get("status") != 200 or value.get("final_url") != expected_url:
        raise ExternalFigiResolutionError("external response envelope differs")
    body = _decode_body(value, "external response")
    if (
        value.get("body_bytes") != len(body)
        or value.get("body_sha256") != hashlib.sha256(body).hexdigest()
    ):
        raise ExternalFigiResolutionError("external response bytes do not reproduce")
    _datetime(value.get("received_at_utc"), "external response timestamp")
    headers = value.get("response_headers")
    if not isinstance(headers, dict) or headers != _allowlisted_headers(headers):
        raise ExternalFigiResolutionError("stored response headers are not allowlisted")


def _decode_body(value: Mapping[str, object], label: str) -> bytes:
    encoded = value.get("body_base64")
    if not isinstance(encoded, str):
        raise ExternalFigiResolutionError(f"{label} body encoding is invalid")
    try:
        return base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ExternalFigiResolutionError(f"{label} body encoding is invalid") from exc


def _cleanup_workspace(cache_paths: Sequence[Path], workspace: Path) -> None:
    for path in cache_paths:
        if path.is_symlink():
            raise ExternalFigiResolutionError("refusing to clean a symlinked capture cache")
        path.unlink(missing_ok=True)
    try:
        workspace.rmdir()
        workspace.parent.rmdir()
    except OSError:
        # Preserve unrelated or diagnostic files; never recursively delete a shared tree.
        pass


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        content,
    )
    return ArtifactPin(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _read_exact(root: Path, artifact: ArtifactPin, label: str) -> bytes:
    path = safe_relative_path(root, artifact.path)
    if not path.is_file() or path.is_symlink():
        raise ExternalFigiResolutionError(f"{label} is missing")
    content = path.read_bytes()
    if len(content) != artifact.bytes or hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise ExternalFigiResolutionError(f"{label} exact pin differs")
    return content


def _artifact(value: object) -> ArtifactPin:
    item = _closed(value, {"bytes", "path", "sha256"}, "artifact pin")
    return ArtifactPin(
        path=_text(item["path"], "artifact path"),
        sha256=_text(item["sha256"], "artifact SHA-256"),
        bytes=_int(item["bytes"], "artifact bytes"),
    )


def _closed(value: object, expected: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ExternalFigiResolutionError(f"{label} fields differ")
    return value


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _json(content: bytes, label: str) -> object:
    try:
        return json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalFigiResolutionError(f"{label} is not valid JSON") from exc


def _date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise ExternalFigiResolutionError(f"{label} is invalid")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise ExternalFigiResolutionError(f"{label} is invalid") from exc
    if result.isoformat() != value:
        raise ExternalFigiResolutionError(f"{label} is not canonical")
    return result


def _datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ExternalFigiResolutionError(f"{label} is invalid")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ExternalFigiResolutionError(f"{label} is invalid") from exc
    return _utc(result, label)


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ExternalFigiResolutionError(f"{label} must be timezone-aware")
    result = value.astimezone(UTC)
    if value != result or value.isoformat() != result.isoformat():
        raise ExternalFigiResolutionError(f"{label} must be canonical UTC")
    return result


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExternalFigiResolutionError(f"{label} must be trimmed nonempty text")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return _text(value, "optional text")


def _int(value: object, label: str) -> int:
    if type(value) is not int:
        raise ExternalFigiResolutionError(f"{label} must be a native integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _int(value, label)
    if result <= 0:
        raise ExternalFigiResolutionError(f"{label} must be positive")
    return result


def _nonnegative_int(value: object, label: str) -> int:
    result = _int(value, label)
    if result < 0:
        raise ExternalFigiResolutionError(f"{label} must be nonnegative")
    return result


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise ExternalFigiResolutionError(f"{label} must be a native date")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ExternalFigiResolutionError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExternalFigiResolutionError("ticker must be trimmed nonempty text")
    return value


__all__ = [
    "EXTERNAL_FIGI_RESOLUTION_RULE_VERSION",
    "EffectiveExternalIdentity",
    "ExternalFigiApplicationSummary",
    "ExternalFigiBackfillResult",
    "ExternalFigiResolutionAttempt",
    "ExternalFigiResolutionError",
    "ExternalFigiResolutionRelease",
    "ExternalHttpResponse",
    "capture_external_figi_resolution",
    "effective_external_identity",
    "external_figi_resolution_key",
    "load_external_figi_resolution_release_exact",
    "summarize_external_figi_application",
    "verify_external_figi_resolution_release",
]
