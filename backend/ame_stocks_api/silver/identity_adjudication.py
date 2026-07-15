"""Immutable S7 external-evidence and identity-adjudication control plane.

This module deliberately does not detect FIGI bounces or resolve market rows.  It freezes
optional external evidence, decision plans, row-specific human review receipts, and the
approved decision records consumed by the cutoff-bound resolver.  Provider observations are
never rewritten here and an unapproved proposal is never exposed as an effective override.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import IO
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ensure_json_safe, thaw_json
from ame_stocks_api.silver.identity_bounce import (
    IdentityBounceError,
    IdentityCaseCandidateManifest,
    read_identity_case_candidate_manifest,
)
from ame_stocks_api.silver.identity_source import (
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
)

EXTERNAL_EVIDENCE_MANIFEST_VERSION = 1
ADJUDICATION_PLAN_VERSION = 1
ADJUDICATION_REVIEW_RECEIPT_VERSION = 1
APPROVED_IDENTITY_DECISION_VERSION = 1
ADJUDICATION_REGISTRY_RELEASE_VERSION = 1

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_MEDIA_TYPE = re.compile(r"^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$")
_FIELD = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "key",
    "password",
    "secret",
    "signature",
    "token",
    "x-amz-signature",
}
_OUTCOME_FIELD_TOKENS = ("backtest", "factor", "portfolio", "return", "sharpe")


class IdentityAdjudicationError(RuntimeError):
    """Raised when an S7 identity control artifact is unsafe or inconsistent."""


class ExternalAuthorityClass(StrEnum):
    REGULATOR_OFFICIAL = "regulator_official"
    EXCHANGE_OR_SRO_OFFICIAL = "exchange_or_sro_official"
    ISSUER_OFFICIAL = "issuer_official"
    IDENTIFIER_REFERENCE_REVIEWED = "identifier_reference_reviewed"


class EvidenceSourceType(StrEnum):
    PINNED_MASSIVE_RELEASE = "pinned_massive_release"
    EXTERNAL_IMMUTABLE_SNAPSHOT = "external_immutable_snapshot"


class IdentityDisposition(StrEnum):
    CONFIRMED_GENUINE_TRANSITION = "confirmed_genuine_transition"
    CONFIRMED_PROVIDER_CONTAMINATION = "confirmed_provider_contamination"
    ADJUDICATED_UNRESOLVED = "adjudicated_unresolved"


class AdjudicationReviewDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"


class AdjudicationControlState(StrEnum):
    PROPOSED = "proposed"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class StoredIdentityDocument:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class ExternalEvidenceCapture:
    """Exact bytes and source metadata supplied by a reviewer-controlled capture step."""

    identity_case_id: str
    source_authority_class: ExternalAuthorityClass
    source_name: str
    source_url: str
    source_published_at_utc: datetime
    observed_at_utc: datetime
    as_of_at_utc: datetime
    captured_at_utc: datetime
    source_available_session: date
    asserted_fields: tuple[str, ...]
    assertion: Mapping[str, object]
    media_type: str
    license_name: str
    captured_content: bytes = field(repr=False)
    license_url: str | None = None

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity_case_id")
        if not isinstance(self.source_authority_class, ExternalAuthorityClass):
            raise IdentityAdjudicationError("external source authority class is invalid")
        _text(self.source_name, "source_name", maximum=300)
        object.__setattr__(self, "source_url", _normalized_public_url(self.source_url))
        published = _utc_datetime(self.source_published_at_utc, "source_published_at_utc")
        observed = _utc_datetime(self.observed_at_utc, "observed_at_utc")
        as_of = _utc_datetime(self.as_of_at_utc, "as_of_at_utc")
        captured = _utc_datetime(self.captured_at_utc, "captured_at_utc")
        if not as_of <= published <= observed <= captured:
            raise IdentityAdjudicationError(
                "external evidence timestamps must satisfy "
                "as_of <= published <= observed <= captured"
            )
        for name, value in (
            ("source_published_at_utc", published),
            ("observed_at_utc", observed),
            ("as_of_at_utc", as_of),
            ("captured_at_utc", captured),
        ):
            object.__setattr__(self, name, value)
        if not isinstance(self.source_available_session, date) or isinstance(
            self.source_available_session, datetime
        ):
            raise IdentityAdjudicationError("source_available_session must be a date")
        if self.source_available_session < max(published.date(), captured.date()):
            raise IdentityAdjudicationError(
                "external evidence availability cannot precede publication or capture"
            )
        fields = tuple(sorted(set(self.asserted_fields)))
        if not fields or any(not _FIELD.fullmatch(item) for item in fields):
            raise IdentityAdjudicationError("asserted_fields must contain safe field identifiers")
        if any(token in item for item in fields for token in _OUTCOME_FIELD_TOKENS):
            raise IdentityAdjudicationError(
                "external identity evidence cannot assert outcome or backtest fields"
            )
        object.__setattr__(self, "asserted_fields", fields)
        assertion = ensure_json_safe(self.assertion, label="external evidence assertion")
        if _contains_outcome_or_backtest_token(assertion):
            raise IdentityAdjudicationError(
                "external identity evidence assertion cannot contain outcome or backtest tokens"
            )
        object.__setattr__(self, "assertion", assertion)
        if not _MEDIA_TYPE.fullmatch(self.media_type):
            raise IdentityAdjudicationError("external evidence media_type is invalid")
        _text(self.license_name, "license_name", maximum=500)
        if self.license_url is not None:
            object.__setattr__(self, "license_url", _normalized_public_url(self.license_url))
        if not isinstance(self.captured_content, bytes) or not self.captured_content:
            raise IdentityAdjudicationError(
                "external evidence requires non-empty immutable captured bytes"
            )
        if len(self.captured_content) > 64 * 1024 * 1024:
            raise IdentityAdjudicationError("one external evidence capture exceeds 64 MiB")


@dataclass(frozen=True, slots=True)
class ExternalEvidenceRecord:
    external_evidence_id: str
    identity_case_id: str
    source_authority_class: ExternalAuthorityClass
    source_name: str
    normalized_url: str
    source_published_at_utc: datetime
    observed_at_utc: datetime
    as_of_at_utc: datetime
    captured_at_utc: datetime
    source_available_session: date
    asserted_fields: tuple[str, ...]
    assertion: Mapping[str, object]
    media_type: str
    license_name: str
    license_url: str | None
    archived_artifact_path: str
    archived_artifact_sha256: str
    archived_artifact_bytes: int

    def __post_init__(self) -> None:
        self._validate()

    @classmethod
    def from_capture(cls, capture: ExternalEvidenceCapture) -> ExternalEvidenceRecord:
        content_sha = hashlib.sha256(capture.captured_content).hexdigest()
        artifact_path = (
            f"manifests/silver/identity/external-evidence/artifacts/sha256={content_sha}/content"
        )
        payload = {
            "archived_artifact_bytes": len(capture.captured_content),
            "archived_artifact_path": artifact_path,
            "archived_artifact_sha256": content_sha,
            "as_of_at_utc": _utc_text(capture.as_of_at_utc),
            "asserted_fields": list(capture.asserted_fields),
            "assertion": thaw_json(capture.assertion),
            "captured_at_utc": _utc_text(capture.captured_at_utc),
            "identity_case_id": capture.identity_case_id,
            "license_name": capture.license_name,
            "license_url": capture.license_url,
            "media_type": capture.media_type,
            "namespace": "ame_stocks.identity.external_evidence",
            "normalized_url": capture.source_url,
            "observed_at_utc": _utc_text(capture.observed_at_utc),
            "rule_version": "s7_identity_external_evidence_id_v1",
            "source_authority_class": capture.source_authority_class.value,
            "source_available_session": capture.source_available_session.isoformat(),
            "source_name": capture.source_name,
            "source_published_at_utc": _utc_text(capture.source_published_at_utc),
        }
        return cls(external_evidence_id=stable_digest(payload), **_record_kwargs(payload))

    def to_dict(self) -> dict[str, object]:
        return {
            "archived_artifact_bytes": self.archived_artifact_bytes,
            "archived_artifact_path": self.archived_artifact_path,
            "archived_artifact_sha256": self.archived_artifact_sha256,
            "as_of_at_utc": _utc_text(self.as_of_at_utc),
            "asserted_fields": list(self.asserted_fields),
            "assertion": thaw_json(self.assertion),
            "captured_at_utc": _utc_text(self.captured_at_utc),
            "external_evidence_id": self.external_evidence_id,
            "identity_case_id": self.identity_case_id,
            "license_name": self.license_name,
            "license_url": self.license_url,
            "media_type": self.media_type,
            "normalized_url": self.normalized_url,
            "observed_at_utc": _utc_text(self.observed_at_utc),
            "source_authority_class": self.source_authority_class.value,
            "source_available_session": self.source_available_session.isoformat(),
            "source_name": self.source_name,
            "source_published_at_utc": _utc_text(self.source_published_at_utc),
        }

    @classmethod
    def from_dict(cls, value: object) -> ExternalEvidenceRecord:
        item = _mapping(value, "external evidence record")
        record = cls(
            external_evidence_id=_string(item, "external_evidence_id"),
            identity_case_id=_string(item, "identity_case_id"),
            source_authority_class=ExternalAuthorityClass(_string(item, "source_authority_class")),
            source_name=_string(item, "source_name"),
            normalized_url=_string(item, "normalized_url"),
            source_published_at_utc=_parse_utc(_string(item, "source_published_at_utc")),
            observed_at_utc=_parse_utc(_string(item, "observed_at_utc")),
            as_of_at_utc=_parse_utc(_string(item, "as_of_at_utc")),
            captured_at_utc=_parse_utc(_string(item, "captured_at_utc")),
            source_available_session=date.fromisoformat(_string(item, "source_available_session")),
            asserted_fields=tuple(_string_list(item, "asserted_fields")),
            assertion=ensure_json_safe(item.get("assertion"), label="external assertion"),
            media_type=_string(item, "media_type"),
            license_name=_string(item, "license_name"),
            license_url=_optional_string(item, "license_url"),
            archived_artifact_path=_string(item, "archived_artifact_path"),
            archived_artifact_sha256=_string(item, "archived_artifact_sha256"),
            archived_artifact_bytes=_positive_int(item.get("archived_artifact_bytes"), "bytes"),
        )
        record._validate()
        return record

    def _validate(self) -> None:
        _digest(self.external_evidence_id, "external_evidence_id")
        _digest(self.identity_case_id, "identity_case_id")
        _digest(self.archived_artifact_sha256, "archived_artifact_sha256")
        if not isinstance(self.source_authority_class, ExternalAuthorityClass):
            raise IdentityAdjudicationError("external source authority class is invalid")
        _text(self.source_name, "source_name", maximum=300)
        if _normalized_public_url(self.normalized_url) != self.normalized_url:
            raise IdentityAdjudicationError("external evidence URL is not normalized")
        as_of = _utc_datetime(self.as_of_at_utc, "as_of_at_utc")
        published = _utc_datetime(self.source_published_at_utc, "source_published_at_utc")
        observed = _utc_datetime(self.observed_at_utc, "observed_at_utc")
        captured = _utc_datetime(self.captured_at_utc, "captured_at_utc")
        if not as_of <= published <= observed <= captured:
            raise IdentityAdjudicationError("external evidence timestamp chain is invalid")
        if self.source_available_session < max(published.date(), captured.date()):
            raise IdentityAdjudicationError("external evidence availability is backdated")
        if not self.asserted_fields or any(
            not _FIELD.fullmatch(item) for item in self.asserted_fields
        ):
            raise IdentityAdjudicationError("external asserted fields are invalid")
        if any(token in item for item in self.asserted_fields for token in _OUTCOME_FIELD_TOKENS):
            raise IdentityAdjudicationError(
                "external identity evidence cannot assert outcome or backtest fields"
            )
        if tuple(sorted(set(self.asserted_fields))) != self.asserted_fields:
            raise IdentityAdjudicationError("external asserted fields are not canonical")
        assertion = ensure_json_safe(self.assertion, label="external evidence assertion")
        if _contains_outcome_or_backtest_token(assertion):
            raise IdentityAdjudicationError(
                "external identity evidence assertion cannot contain outcome or backtest tokens"
            )
        if not _MEDIA_TYPE.fullmatch(self.media_type):
            raise IdentityAdjudicationError("external evidence media type is invalid")
        _text(self.license_name, "license_name", maximum=500)
        if self.license_url is not None and (
            _normalized_public_url(self.license_url) != self.license_url
        ):
            raise IdentityAdjudicationError("external evidence license URL is not normalized")
        _relative_path_text(self.archived_artifact_path, "archived artifact path")
        expected_path = (
            "manifests/silver/identity/external-evidence/artifacts/"
            f"sha256={self.archived_artifact_sha256}/content"
        )
        if self.archived_artifact_path != expected_path:
            raise IdentityAdjudicationError("external archive path is not content addressed")
        if type(self.archived_artifact_bytes) is not int or self.archived_artifact_bytes <= 0:
            raise IdentityAdjudicationError("external archived bytes must be positive")
        payload = self.to_dict()
        payload.pop("external_evidence_id")
        payload.update(
            {
                "namespace": "ame_stocks.identity.external_evidence",
                "rule_version": "s7_identity_external_evidence_id_v1",
            }
        )
        if stable_digest(payload) != self.external_evidence_id:
            raise IdentityAdjudicationError("external evidence ID recomputation failed")

    def to_evidence_ref(self, manifest_id: str) -> AdjudicationEvidenceRef:
        return AdjudicationEvidenceRef(
            evidence_ref=self.external_evidence_id,
            source_type=EvidenceSourceType.EXTERNAL_IMMUTABLE_SNAPSHOT,
            source_available_session=self.source_available_session,
            source={
                "external_evidence_id": self.external_evidence_id,
                "identity_case_id": self.identity_case_id,
                "source_external_evidence_manifest_id": manifest_id,
            },
        )


@dataclass(frozen=True, slots=True)
class ExternalEvidenceManifest:
    identity_case_id: str
    six_release_binding_id: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    records: tuple[ExternalEvidenceRecord, ...]

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity_case_id")
        _digest(self.six_release_binding_id, "six_release_binding_id")
        _text(self.availability_calendar_id, "availability_calendar_id", maximum=500)
        _digest(self.availability_calendar_sha256, "availability_calendar_sha256")
        records = tuple(sorted(self.records, key=lambda item: item.external_evidence_id))
        if not records or len(records) > 1_000:
            raise IdentityAdjudicationError("external manifest must contain 1..1000 records")
        if len({item.external_evidence_id for item in records}) != len(records):
            raise IdentityAdjudicationError("external manifest contains duplicate evidence")
        if any(item.identity_case_id != self.identity_case_id for item in records):
            raise IdentityAdjudicationError("external evidence case binding differs")
        object.__setattr__(self, "records", records)

    @property
    def manifest_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "identity_case_id": self.identity_case_id,
            "namespace": "ame_stocks.identity.external_evidence_manifest",
            "records": [item.to_dict() for item in self.records],
            "rule_version": "s7_identity_external_evidence_manifest_id_v1",
            "six_release_binding_id": self.six_release_binding_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "external_evidence_record_count": len(self.records),
            "identity_external_evidence_manifest_id": self.manifest_id,
            "identity_external_evidence_manifest_version": (EXTERNAL_EVIDENCE_MANIFEST_VERSION),
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ExternalEvidenceManifest:
        document = _mapping(value, "external evidence manifest")
        if document.get("identity_external_evidence_manifest_version") != 1:
            raise IdentityAdjudicationError("unsupported external evidence manifest version")
        raw_records = document.get("records")
        if not isinstance(raw_records, list):
            raise IdentityAdjudicationError("external evidence records must be an array")
        manifest = cls(
            identity_case_id=_string(document, "identity_case_id"),
            six_release_binding_id=_string(document, "six_release_binding_id"),
            availability_calendar_id=_string(document, "availability_calendar_id"),
            availability_calendar_sha256=_string(document, "availability_calendar_sha256"),
            records=tuple(ExternalEvidenceRecord.from_dict(item) for item in raw_records),
        )
        if document.get("external_evidence_record_count") != len(manifest.records):
            raise IdentityAdjudicationError("external evidence count does not reconcile")
        if document.get("identity_external_evidence_manifest_id") != manifest.manifest_id:
            raise IdentityAdjudicationError("external manifest ID recomputation failed")
        return manifest


@dataclass(frozen=True, slots=True)
class CandidateManifestBinding:
    manifest_id: str
    manifest_sha256: str
    path: str

    def __post_init__(self) -> None:
        _digest(self.manifest_id, "candidate manifest ID")
        _digest(self.manifest_sha256, "candidate manifest SHA-256")
        _relative_path_text(self.path, "candidate manifest path")

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "manifest_sha256": self.manifest_sha256,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, value: object) -> CandidateManifestBinding:
        item = _mapping(value, "candidate manifest binding")
        return cls(
            manifest_id=_string(item, "manifest_id"),
            manifest_sha256=_string(item, "manifest_sha256"),
            path=_string(item, "path"),
        )


@dataclass(frozen=True, slots=True)
class IdentityCaseReference:
    identity_case_id: str
    identity_case_available_session: date
    observed_ticker: str
    observed_composite_figi: str
    left_outer_composite_figi: str
    right_outer_composite_figi: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_count: int
    episode_source_record_set_digest: str
    detector_rule_version: str = "s7_provider_figi_bounce_detector_v1"

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity_case_id")
        _text(self.observed_ticker, "observed_ticker", maximum=100)
        for name in (
            "observed_composite_figi",
            "left_outer_composite_figi",
            "right_outer_composite_figi",
        ):
            if not _FIGI.fullmatch(getattr(self, name)):
                raise IdentityAdjudicationError(f"{name} must be a valid Composite FIGI")
        if self.left_outer_composite_figi != self.right_outer_composite_figi:
            raise IdentityAdjudicationError("bounce outer Composite FIGIs must agree")
        if self.observed_composite_figi == self.left_outer_composite_figi:
            raise IdentityAdjudicationError("bounce middle Composite FIGI must differ")
        if self.episode_valid_from_session > self.episode_valid_through_session:
            raise IdentityAdjudicationError("episode session bounds are reversed")
        if self.identity_case_available_session <= self.episode_valid_through_session:
            raise IdentityAdjudicationError("case availability must follow the middle episode")
        if type(self.episode_source_record_count) is not int or not (
            1 <= self.episode_source_record_count <= 20
        ):
            raise IdentityAdjudicationError("episode source-record count must be 1..20")
        _digest(self.episode_source_record_set_digest, "episode source-record-set digest")
        if self.detector_rule_version != "s7_provider_figi_bounce_detector_v1":
            raise IdentityAdjudicationError("unsupported identity bounce detector version")

    def to_dict(self) -> dict[str, object]:
        return {
            "detector_rule_version": self.detector_rule_version,
            "episode_source_record_count": self.episode_source_record_count,
            "episode_source_record_set_digest": self.episode_source_record_set_digest,
            "episode_valid_from_session": self.episode_valid_from_session.isoformat(),
            "episode_valid_through_session": self.episode_valid_through_session.isoformat(),
            "identity_case_available_session": self.identity_case_available_session.isoformat(),
            "identity_case_id": self.identity_case_id,
            "left_outer_composite_figi": self.left_outer_composite_figi,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_ticker": self.observed_ticker,
            "right_outer_composite_figi": self.right_outer_composite_figi,
        }

    @classmethod
    def from_dict(cls, value: object) -> IdentityCaseReference:
        item = _mapping(value, "identity case reference")
        return cls(
            identity_case_id=_string(item, "identity_case_id"),
            identity_case_available_session=date.fromisoformat(
                _string(item, "identity_case_available_session")
            ),
            observed_ticker=_string(item, "observed_ticker"),
            observed_composite_figi=_string(item, "observed_composite_figi"),
            left_outer_composite_figi=_string(item, "left_outer_composite_figi"),
            right_outer_composite_figi=_string(item, "right_outer_composite_figi"),
            episode_valid_from_session=date.fromisoformat(
                _string(item, "episode_valid_from_session")
            ),
            episode_valid_through_session=date.fromisoformat(
                _string(item, "episode_valid_through_session")
            ),
            episode_source_record_count=_positive_int(
                item.get("episode_source_record_count"), "episode_source_record_count"
            ),
            episode_source_record_set_digest=_string(item, "episode_source_record_set_digest"),
            detector_rule_version=_string(item, "detector_rule_version"),
        )


@dataclass(frozen=True, slots=True)
class AdjudicationEvidenceRef:
    evidence_ref: str
    source_type: EvidenceSourceType
    source_available_session: date
    source: Mapping[str, object]

    def __post_init__(self) -> None:
        _text(self.evidence_ref, "evidence_ref", maximum=500)
        if not isinstance(self.source_type, EvidenceSourceType):
            raise IdentityAdjudicationError("evidence source type is invalid")
        if not isinstance(self.source_available_session, date) or isinstance(
            self.source_available_session, datetime
        ):
            raise IdentityAdjudicationError("evidence availability must be a date")
        source = ensure_json_safe(self.source, label="adjudication evidence source")
        object.__setattr__(self, "source", source)
        if self.source_type is EvidenceSourceType.PINNED_MASSIVE_RELEASE:
            required = {"dataset", "release_id", "source_record_id"}
        else:
            required = {
                "external_evidence_id",
                "identity_case_id",
                "source_external_evidence_manifest_id",
            }
        if not required.issubset(source):
            raise IdentityAdjudicationError(
                f"{self.source_type.value} evidence lacks required source keys"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "evidence_ref": self.evidence_ref,
            "source": thaw_json(self.source),
            "source_available_session": self.source_available_session.isoformat(),
            "source_type": self.source_type.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> AdjudicationEvidenceRef:
        item = _mapping(value, "adjudication evidence ref")
        return cls(
            evidence_ref=_string(item, "evidence_ref"),
            source_type=EvidenceSourceType(_string(item, "source_type")),
            source_available_session=date.fromisoformat(_string(item, "source_available_session")),
            source=ensure_json_safe(item.get("source"), label="adjudication evidence source"),
        )


@dataclass(frozen=True, slots=True)
class IdentityAdjudicationProposal:
    case: IdentityCaseReference
    decision_version: int
    disposition: IdentityDisposition
    canonical_composite_figi: str | None
    reason_code: str
    reason_detail: str
    evidence_refs: tuple[AdjudicationEvidenceRef, ...]
    supersedes_identity_adjudication_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.decision_version) is not int or self.decision_version <= 0:
            raise IdentityAdjudicationError("decision_version must be a positive native int")
        if not isinstance(self.disposition, IdentityDisposition):
            raise IdentityAdjudicationError("identity disposition is invalid")
        if self.decision_version == 1 and self.supersedes_identity_adjudication_id is not None:
            raise IdentityAdjudicationError("decision version 1 cannot have a predecessor")
        if self.decision_version > 1:
            if self.supersedes_identity_adjudication_id is None:
                raise IdentityAdjudicationError("successor decision requires a predecessor")
            _digest(self.supersedes_identity_adjudication_id, "superseded adjudication ID")
        canonical = self.canonical_composite_figi
        if self.disposition is IdentityDisposition.CONFIRMED_GENUINE_TRANSITION:
            if canonical != self.case.observed_composite_figi:
                raise IdentityAdjudicationError("genuine transition must retain observed FIGI")
        elif self.disposition is IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION:
            if canonical != self.case.left_outer_composite_figi:
                raise IdentityAdjudicationError(
                    "provider contamination must map to the independently reviewed outer FIGI"
                )
        elif canonical is not None:
            raise IdentityAdjudicationError("adjudicated unresolved must have null canonical FIGI")
        _field_text(self.reason_code, "reason_code")
        _text(self.reason_detail, "reason_detail", maximum=4_000)
        forbidden = ("return", "factor", "backtest", "portfolio", "majority", "recent")
        reason = f"{self.reason_code} {self.reason_detail}".lower()
        if any(word in reason for word in forbidden):
            raise IdentityAdjudicationError("outcome or heuristic-based adjudication is forbidden")
        refs = tuple(sorted(self.evidence_refs, key=lambda item: item.evidence_ref))
        if not refs or len(refs) > 1_000:
            raise IdentityAdjudicationError("adjudication requires 1..1000 evidence refs")
        if len({item.evidence_ref for item in refs}) != len(refs):
            raise IdentityAdjudicationError("adjudication evidence refs are not unique")
        object.__setattr__(self, "evidence_refs", refs)

    @property
    def adjudication_series_id(self) -> str:
        return stable_digest(
            {
                "identity_case_id": self.case.identity_case_id,
                "namespace": "ame_stocks.identity.adjudication_series",
                "rule_version": "s7_identity_adjudication_series_id_v1",
            }
        )

    @property
    def evidence_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.evidence_refs])

    @property
    def evidence_cutoff_session(self) -> date:
        return max(item.source_available_session for item in self.evidence_refs)

    @property
    def external_evidence_record_count(self) -> int:
        return sum(
            item.source_type is EvidenceSourceType.EXTERNAL_IMMUTABLE_SNAPSHOT
            for item in self.evidence_refs
        )

    @property
    def canonical_asset_id(self) -> str | None:
        if self.canonical_composite_figi is None:
            return None
        return stable_digest(
            {
                "anchor_type": "composite_figi",
                "anchor_value": self.canonical_composite_figi,
                "namespace": "ame_stocks.identity.asset",
                "rule_version": "ame_stocks_asset_id_from_composite_figi_v1",
            }
        )

    @property
    def identity_adjudication_id(self) -> str:
        return stable_digest(
            {
                "adjudication_series_id": self.adjudication_series_id,
                "canonical_asset_id": self.canonical_asset_id,
                "canonical_composite_figi": self.canonical_composite_figi,
                "decision_version": self.decision_version,
                "disposition": self.disposition.value,
                "evidence_digest": self.evidence_digest,
                "identity_case_id": self.case.identity_case_id,
                "namespace": "ame_stocks.identity.adjudication",
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "rule_version": "s7_identity_adjudication_id_v1",
                "supersedes_identity_adjudication_id": (self.supersedes_identity_adjudication_id),
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "adjudication_series_id": self.adjudication_series_id,
            "canonical_asset_id": self.canonical_asset_id,
            "canonical_composite_figi": self.canonical_composite_figi,
            "case": self.case.to_dict(),
            "decision_version": self.decision_version,
            "disposition": self.disposition.value,
            "evidence_cutoff_session": self.evidence_cutoff_session.isoformat(),
            "evidence_digest": self.evidence_digest,
            "evidence_refs": [item.to_dict() for item in self.evidence_refs],
            "external_evidence_record_count": self.external_evidence_record_count,
            "identity_adjudication_id": self.identity_adjudication_id,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "supersedes_identity_adjudication_id": (self.supersedes_identity_adjudication_id),
        }

    @classmethod
    def from_dict(cls, value: object) -> IdentityAdjudicationProposal:
        item = _mapping(value, "identity adjudication proposal")
        raw_refs = item.get("evidence_refs")
        if not isinstance(raw_refs, list):
            raise IdentityAdjudicationError("proposal evidence_refs must be an array")
        proposal = cls(
            case=IdentityCaseReference.from_dict(item.get("case")),
            decision_version=_positive_int(item.get("decision_version"), "decision_version"),
            disposition=IdentityDisposition(_string(item, "disposition")),
            canonical_composite_figi=_optional_string(item, "canonical_composite_figi"),
            reason_code=_string(item, "reason_code"),
            reason_detail=_string(item, "reason_detail"),
            evidence_refs=tuple(AdjudicationEvidenceRef.from_dict(ref) for ref in raw_refs),
            supersedes_identity_adjudication_id=_optional_string(
                item, "supersedes_identity_adjudication_id"
            ),
        )
        expected = proposal.to_dict()
        for name in (
            "adjudication_series_id",
            "canonical_asset_id",
            "evidence_cutoff_session",
            "evidence_digest",
            "external_evidence_record_count",
            "identity_adjudication_id",
        ):
            if item.get(name) != expected[name]:
                raise IdentityAdjudicationError(f"proposal {name} recomputation failed")
        return proposal


@dataclass(frozen=True, slots=True)
class IdentityAdjudicationPlan:
    candidate_manifest: CandidateManifestBinding
    six_release_binding_id: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    proposed_by: str
    proposed_at_utc: datetime
    proposals: tuple[IdentityAdjudicationProposal, ...]
    external_evidence_manifest_id: str | None = None
    external_evidence_manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        _digest(self.six_release_binding_id, "six_release_binding_id")
        _text(self.availability_calendar_id, "availability_calendar_id", maximum=500)
        _digest(self.availability_calendar_sha256, "availability_calendar_sha256")
        _text(self.proposed_by, "proposed_by", maximum=200)
        object.__setattr__(
            self,
            "proposed_at_utc",
            _utc_datetime(self.proposed_at_utc, "proposed_at_utc"),
        )
        proposals = tuple(sorted(self.proposals, key=lambda item: item.identity_adjudication_id))
        if not proposals or len(proposals) > 1_000:
            raise IdentityAdjudicationError("decision plan must contain 1..1000 proposals")
        if len({item.identity_adjudication_id for item in proposals}) != len(proposals):
            raise IdentityAdjudicationError("decision plan contains duplicate proposals")
        object.__setattr__(self, "proposals", proposals)
        if self.six_release_binding_id == S7_SIX_RELEASE_BINDING_ID:
            for proposal in proposals:
                for evidence in proposal.evidence_refs:
                    if evidence.source_type is not EvidenceSourceType.PINNED_MASSIVE_RELEASE:
                        continue
                    source = thaw_json(evidence.source)
                    dataset = source.get("dataset")
                    release_id = source.get("release_id")
                    pin = S7_SOURCE_PINS.get(dataset) if isinstance(dataset, str) else None
                    if pin is None or release_id != pin.release_id:
                        raise IdentityAdjudicationError(
                            "production provider evidence is outside the exact six-release binding"
                        )
        paired = (
            self.external_evidence_manifest_id,
            self.external_evidence_manifest_sha256,
        )
        if (paired[0] is None) != (paired[1] is None):
            raise IdentityAdjudicationError("external manifest ID and SHA must be jointly null")
        if paired[0] is not None:
            _digest(paired[0], "external manifest ID")
            _digest(paired[1], "external manifest SHA-256")
        if any(item.external_evidence_record_count for item in proposals) != (
            paired[0] is not None
        ):
            raise IdentityAdjudicationError(
                "external evidence refs and external manifest binding disagree"
            )

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate_manifest": self.candidate_manifest.to_dict(),
            "external_evidence_manifest_id": self.external_evidence_manifest_id,
            "external_evidence_manifest_sha256": self.external_evidence_manifest_sha256,
            "namespace": "ame_stocks.identity.adjudication_plan",
            "proposals": [item.to_dict() for item in self.proposals],
            "proposed_at_utc": _utc_text(self.proposed_at_utc),
            "proposed_by": self.proposed_by,
            "rule_version": "s7_identity_adjudication_plan_id_v1",
            "six_release_binding_id": self.six_release_binding_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "identity_adjudication_plan_version": ADJUDICATION_PLAN_VERSION,
            "plan_id": self.plan_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> IdentityAdjudicationPlan:
        item = _mapping(value, "identity adjudication plan")
        if item.get("identity_adjudication_plan_version") != 1:
            raise IdentityAdjudicationError("unsupported identity adjudication plan version")
        raw = item.get("proposals")
        if not isinstance(raw, list):
            raise IdentityAdjudicationError("decision plan proposals must be an array")
        plan = cls(
            candidate_manifest=CandidateManifestBinding.from_dict(item.get("candidate_manifest")),
            six_release_binding_id=_string(item, "six_release_binding_id"),
            availability_calendar_id=_string(item, "availability_calendar_id"),
            availability_calendar_sha256=_string(item, "availability_calendar_sha256"),
            proposed_by=_string(item, "proposed_by"),
            proposed_at_utc=_parse_utc(_string(item, "proposed_at_utc")),
            proposals=tuple(IdentityAdjudicationProposal.from_dict(row) for row in raw),
            external_evidence_manifest_id=_optional_string(item, "external_evidence_manifest_id"),
            external_evidence_manifest_sha256=_optional_string(
                item, "external_evidence_manifest_sha256"
            ),
        )
        if item.get("plan_id") != plan.plan_id:
            raise IdentityAdjudicationError("decision plan ID recomputation failed")
        return plan


@dataclass(frozen=True, slots=True)
class AdjudicationReviewReceipt:
    plan_id: str
    plan_path: str
    plan_sha256: str
    candidate_manifest_id: str
    candidate_manifest_sha256: str
    identity_adjudication_id: str
    decision: AdjudicationReviewDecision
    reviewed_by: str
    reviewed_at_utc: datetime
    review_reason: str
    approval_available_session: date | None
    adjudication_available_session: date | None

    def __post_init__(self) -> None:
        for value, label in (
            (self.plan_id, "plan_id"),
            (self.plan_sha256, "plan_sha256"),
            (self.candidate_manifest_id, "candidate_manifest_id"),
            (self.candidate_manifest_sha256, "candidate_manifest_sha256"),
            (self.identity_adjudication_id, "identity_adjudication_id"),
        ):
            _digest(value, label)
        _relative_path_text(self.plan_path, "plan_path")
        if not isinstance(self.decision, AdjudicationReviewDecision):
            raise IdentityAdjudicationError("review decision is invalid")
        _text(self.reviewed_by, "reviewed_by", maximum=200)
        object.__setattr__(
            self,
            "reviewed_at_utc",
            _utc_datetime(self.reviewed_at_utc, "reviewed_at_utc"),
        )
        _text(self.review_reason, "review_reason", maximum=4_000)
        if self.decision is AdjudicationReviewDecision.APPROVED:
            if self.approval_available_session is None or (
                self.adjudication_available_session is None
            ):
                raise IdentityAdjudicationError("approved receipt requires availability")
            if self.adjudication_available_session < self.approval_available_session:
                raise IdentityAdjudicationError(
                    "adjudication availability cannot precede approval availability"
                )
        elif (
            self.approval_available_session is not None
            or self.adjudication_available_session is not None
        ):
            raise IdentityAdjudicationError("rejected receipt cannot expose availability")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def approval_id(self) -> str | None:
        return self.receipt_id if self.decision is AdjudicationReviewDecision.APPROVED else None

    def logical_payload(self) -> dict[str, object]:
        return {
            "adjudication_available_session": _date_text(self.adjudication_available_session),
            "approval_available_session": _date_text(self.approval_available_session),
            "candidate_manifest_id": self.candidate_manifest_id,
            "candidate_manifest_sha256": self.candidate_manifest_sha256,
            "decision": self.decision.value,
            "identity_adjudication_id": self.identity_adjudication_id,
            "namespace": "ame_stocks.identity.adjudication_review_receipt",
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "review_reason": self.review_reason,
            "reviewed_at_utc": _utc_text(self.reviewed_at_utc),
            "reviewed_by": self.reviewed_by,
            "rule_version": "s7_identity_adjudication_review_receipt_id_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "adjudication_review_receipt_version": ADJUDICATION_REVIEW_RECEIPT_VERSION,
            "approval_id": self.approval_id,
            "receipt_id": self.receipt_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> AdjudicationReviewReceipt:
        item = _mapping(value, "adjudication review receipt")
        if item.get("adjudication_review_receipt_version") != 1:
            raise IdentityAdjudicationError("unsupported adjudication receipt version")
        decision = AdjudicationReviewDecision(_string(item, "decision"))
        receipt = cls(
            plan_id=_string(item, "plan_id"),
            plan_path=_string(item, "plan_path"),
            plan_sha256=_string(item, "plan_sha256"),
            candidate_manifest_id=_string(item, "candidate_manifest_id"),
            candidate_manifest_sha256=_string(item, "candidate_manifest_sha256"),
            identity_adjudication_id=_string(item, "identity_adjudication_id"),
            decision=decision,
            reviewed_by=_string(item, "reviewed_by"),
            reviewed_at_utc=_parse_utc(_string(item, "reviewed_at_utc")),
            review_reason=_string(item, "review_reason"),
            approval_available_session=_optional_date(item.get("approval_available_session")),
            adjudication_available_session=_optional_date(
                item.get("adjudication_available_session")
            ),
        )
        if item.get("receipt_id") != receipt.receipt_id:
            raise IdentityAdjudicationError("review receipt ID recomputation failed")
        if item.get("approval_id") != receipt.approval_id:
            raise IdentityAdjudicationError("review receipt approval ID is invalid")
        return receipt


@dataclass(frozen=True, slots=True)
class ApprovedIdentityDecision:
    """Stable resolver-facing view of one explicitly approved decision version."""

    identity_case_id: str
    identity_adjudication_id: str
    adjudication_series_id: str
    decision_version: int
    observed_ticker: str
    observed_composite_figi: str
    disposition: IdentityDisposition
    canonical_composite_figi: str | None
    canonical_asset_id: str | None
    canonical_override: bool
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_set_digest: str
    identity_case_available_session: date
    evidence_cutoff_session: date
    approval_available_session: date
    adjudication_available_session: date
    approval_status: str
    approval_id: str
    approval_receipt_path: str
    approval_receipt_sha256: str
    approved_by: str
    approved_at_utc: datetime
    supersedes_identity_adjudication_id: str | None
    source_identity_case_candidate_manifest_id: str
    source_identity_case_candidate_manifest_sha256: str
    source_decision_plan_id: str
    source_decision_plan_path: str
    source_decision_plan_sha256: str
    outcome_or_backtest_evidence_used: bool

    def __post_init__(self) -> None:
        for value, label in (
            (self.identity_case_id, "identity_case_id"),
            (self.identity_adjudication_id, "identity_adjudication_id"),
            (self.adjudication_series_id, "adjudication_series_id"),
            (self.episode_source_record_set_digest, "episode record-set digest"),
            (self.approval_id, "approval_id"),
            (self.approval_receipt_sha256, "approval receipt SHA-256"),
            (
                self.source_identity_case_candidate_manifest_id,
                "candidate manifest ID",
            ),
            (
                self.source_identity_case_candidate_manifest_sha256,
                "candidate manifest SHA-256",
            ),
            (self.source_decision_plan_id, "decision plan ID"),
            (self.source_decision_plan_sha256, "decision plan SHA-256"),
        ):
            _digest(value, label)
        _text(self.observed_ticker, "observed_ticker", maximum=100)
        if not _FIGI.fullmatch(self.observed_composite_figi):
            raise IdentityAdjudicationError("approved observed Composite FIGI is invalid")
        if self.approval_status != "approved":
            raise IdentityAdjudicationError("approved decision status must be approved")
        if type(self.canonical_override) is not bool:
            raise IdentityAdjudicationError("canonical_override must be a native bool")
        if type(self.outcome_or_backtest_evidence_used) is not bool or (
            self.outcome_or_backtest_evidence_used
        ):
            raise IdentityAdjudicationError(
                "approved identity decision cannot use outcome or backtest evidence"
            )
        if self.disposition is IdentityDisposition.CONFIRMED_GENUINE_TRANSITION:
            valid = (
                self.canonical_composite_figi == self.observed_composite_figi
                and self.canonical_asset_id is not None
                and not self.canonical_override
            )
        elif self.disposition is IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION:
            valid = (
                self.canonical_composite_figi is not None
                and self.canonical_composite_figi != self.observed_composite_figi
                and self.canonical_asset_id is not None
                and self.canonical_override
            )
        else:
            valid = (
                self.canonical_composite_figi is None
                and self.canonical_asset_id is None
                and not self.canonical_override
            )
        if not valid:
            raise IdentityAdjudicationError("approved identity disposition matrix is invalid")

    @classmethod
    def create(
        cls,
        proposal: IdentityAdjudicationProposal,
        plan: IdentityAdjudicationPlan,
        plan_document: StoredIdentityDocument,
        receipt: AdjudicationReviewReceipt,
        receipt_document: StoredIdentityDocument,
    ) -> ApprovedIdentityDecision:
        if receipt.decision is not AdjudicationReviewDecision.APPROVED:
            raise IdentityAdjudicationError("only approved receipts produce registry records")
        expected_receipt_path = (
            f"manifests/silver/identity/adjudication-receipts/receipt_id={receipt.receipt_id}.json"
        )
        if (
            receipt.plan_id != plan.plan_id
            or receipt.plan_path != plan_document.path
            or receipt.plan_sha256 != plan_document.sha256
            or receipt.candidate_manifest_id != plan.candidate_manifest.manifest_id
            or receipt.candidate_manifest_sha256 != plan.candidate_manifest.manifest_sha256
            or receipt.identity_adjudication_id != proposal.identity_adjudication_id
            or receipt_document.path != expected_receipt_path
        ):
            raise IdentityAdjudicationError(
                "approved receipt is not exactly bound to its plan and proposal"
            )
        expected_available = max(
            proposal.case.identity_case_available_session,
            proposal.evidence_cutoff_session,
            receipt.approval_available_session,
        )
        if receipt.adjudication_available_session != expected_available:
            raise IdentityAdjudicationError("adjudication availability recomputation failed")
        return cls(
            identity_case_id=proposal.case.identity_case_id,
            identity_adjudication_id=proposal.identity_adjudication_id,
            adjudication_series_id=proposal.adjudication_series_id,
            decision_version=proposal.decision_version,
            observed_ticker=proposal.case.observed_ticker,
            observed_composite_figi=proposal.case.observed_composite_figi,
            disposition=proposal.disposition,
            canonical_composite_figi=proposal.canonical_composite_figi,
            canonical_asset_id=proposal.canonical_asset_id,
            canonical_override=(
                proposal.disposition is IdentityDisposition.CONFIRMED_PROVIDER_CONTAMINATION
            ),
            episode_valid_from_session=proposal.case.episode_valid_from_session,
            episode_valid_through_session=proposal.case.episode_valid_through_session,
            episode_source_record_set_digest=(proposal.case.episode_source_record_set_digest),
            identity_case_available_session=proposal.case.identity_case_available_session,
            evidence_cutoff_session=proposal.evidence_cutoff_session,
            approval_available_session=receipt.approval_available_session,
            adjudication_available_session=receipt.adjudication_available_session,
            approval_status="approved",
            approval_id=receipt.receipt_id,
            approval_receipt_path=receipt_document.path,
            approval_receipt_sha256=receipt_document.sha256,
            approved_by=receipt.reviewed_by,
            approved_at_utc=receipt.reviewed_at_utc,
            supersedes_identity_adjudication_id=(proposal.supersedes_identity_adjudication_id),
            source_identity_case_candidate_manifest_id=(plan.candidate_manifest.manifest_id),
            source_identity_case_candidate_manifest_sha256=(
                plan.candidate_manifest.manifest_sha256
            ),
            source_decision_plan_id=plan.plan_id,
            source_decision_plan_path=plan_document.path,
            source_decision_plan_sha256=plan_document.sha256,
            outcome_or_backtest_evidence_used=False,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "approved_identity_decision_version": APPROVED_IDENTITY_DECISION_VERSION,
            "adjudication_available_session": self.adjudication_available_session.isoformat(),
            "adjudication_series_id": self.adjudication_series_id,
            "approval_available_session": self.approval_available_session.isoformat(),
            "approval_id": self.approval_id,
            "approval_receipt_path": self.approval_receipt_path,
            "approval_receipt_sha256": self.approval_receipt_sha256,
            "approval_status": self.approval_status,
            "approved_at_utc": _utc_text(self.approved_at_utc),
            "approved_by": self.approved_by,
            "canonical_asset_id": self.canonical_asset_id,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_override": self.canonical_override,
            "decision_version": self.decision_version,
            "disposition": self.disposition.value,
            "episode_source_record_set_digest": self.episode_source_record_set_digest,
            "episode_valid_from_session": self.episode_valid_from_session.isoformat(),
            "episode_valid_through_session": self.episode_valid_through_session.isoformat(),
            "evidence_cutoff_session": self.evidence_cutoff_session.isoformat(),
            "identity_adjudication_id": self.identity_adjudication_id,
            "identity_case_available_session": self.identity_case_available_session.isoformat(),
            "identity_case_id": self.identity_case_id,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_ticker": self.observed_ticker,
            "outcome_or_backtest_evidence_used": self.outcome_or_backtest_evidence_used,
            "source_decision_plan_id": self.source_decision_plan_id,
            "source_decision_plan_path": self.source_decision_plan_path,
            "source_decision_plan_sha256": self.source_decision_plan_sha256,
            "source_identity_case_candidate_manifest_id": (
                self.source_identity_case_candidate_manifest_id
            ),
            "source_identity_case_candidate_manifest_sha256": (
                self.source_identity_case_candidate_manifest_sha256
            ),
            "supersedes_identity_adjudication_id": (self.supersedes_identity_adjudication_id),
        }

    @classmethod
    def from_dict(cls, value: object) -> ApprovedIdentityDecision:
        item = _mapping(value, "approved identity decision")
        if item.get("approved_identity_decision_version") != 1:
            raise IdentityAdjudicationError("unsupported approved identity decision version")
        return cls(
            identity_case_id=_string(item, "identity_case_id"),
            identity_adjudication_id=_string(item, "identity_adjudication_id"),
            adjudication_series_id=_string(item, "adjudication_series_id"),
            decision_version=_positive_int(item.get("decision_version"), "decision_version"),
            observed_ticker=_string(item, "observed_ticker"),
            observed_composite_figi=_string(item, "observed_composite_figi"),
            disposition=IdentityDisposition(_string(item, "disposition")),
            canonical_composite_figi=_optional_string(item, "canonical_composite_figi"),
            canonical_asset_id=_optional_string(item, "canonical_asset_id"),
            canonical_override=_native_bool(item.get("canonical_override"), "canonical_override"),
            episode_valid_from_session=date.fromisoformat(
                _string(item, "episode_valid_from_session")
            ),
            episode_valid_through_session=date.fromisoformat(
                _string(item, "episode_valid_through_session")
            ),
            episode_source_record_set_digest=_string(item, "episode_source_record_set_digest"),
            identity_case_available_session=date.fromisoformat(
                _string(item, "identity_case_available_session")
            ),
            evidence_cutoff_session=date.fromisoformat(_string(item, "evidence_cutoff_session")),
            approval_available_session=date.fromisoformat(
                _string(item, "approval_available_session")
            ),
            adjudication_available_session=date.fromisoformat(
                _string(item, "adjudication_available_session")
            ),
            approval_status=_string(item, "approval_status"),
            approval_id=_string(item, "approval_id"),
            approval_receipt_path=_string(item, "approval_receipt_path"),
            approval_receipt_sha256=_string(item, "approval_receipt_sha256"),
            approved_by=_string(item, "approved_by"),
            approved_at_utc=_parse_utc(_string(item, "approved_at_utc")),
            supersedes_identity_adjudication_id=_optional_string(
                item, "supersedes_identity_adjudication_id"
            ),
            source_identity_case_candidate_manifest_id=_string(
                item, "source_identity_case_candidate_manifest_id"
            ),
            source_identity_case_candidate_manifest_sha256=_string(
                item, "source_identity_case_candidate_manifest_sha256"
            ),
            source_decision_plan_id=_string(item, "source_decision_plan_id"),
            source_decision_plan_path=_string(item, "source_decision_plan_path"),
            source_decision_plan_sha256=_string(item, "source_decision_plan_sha256"),
            outcome_or_backtest_evidence_used=_native_bool(
                item.get("outcome_or_backtest_evidence_used"),
                "outcome_or_backtest_evidence_used",
            ),
        )


@dataclass(frozen=True, slots=True)
class AdjudicationControlRecord:
    identity_adjudication_id: str
    plan_id: str
    state: AdjudicationControlState
    decision_version: int
    adjudication_series_id: str
    receipt_id: str | None


@dataclass(frozen=True, slots=True)
class AdjudicationReviewResult:
    receipt: AdjudicationReviewReceipt
    receipt_document: StoredIdentityDocument
    approved_decision: ApprovedIdentityDecision | None
    approved_decision_document: StoredIdentityDocument | None


@dataclass(frozen=True, slots=True)
class ApprovedDecisionArtifactRef:
    identity_adjudication_id: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        _digest(self.identity_adjudication_id, "identity adjudication ID")
        _relative_path_text(self.path, "approved decision path")
        _digest(self.sha256, "approved decision SHA-256")

    def to_dict(self) -> dict[str, object]:
        return {
            "identity_adjudication_id": self.identity_adjudication_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> ApprovedDecisionArtifactRef:
        item = _mapping(value, "approved decision artifact ref")
        return cls(
            identity_adjudication_id=_string(item, "identity_adjudication_id"),
            path=_string(item, "path"),
            sha256=_string(item, "sha256"),
        )


@dataclass(frozen=True, slots=True)
class IdentityAdjudicationRegistryRelease:
    """One exact content-addressed snapshot of approved immutable revisions."""

    six_release_binding_id: str
    candidate_manifest_id: str
    candidate_manifest_sha256: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    published_at_utc: datetime
    release_available_session: date
    decisions: tuple[ApprovedDecisionArtifactRef, ...]

    def __post_init__(self) -> None:
        _digest(self.six_release_binding_id, "six-release binding ID")
        _digest(self.candidate_manifest_id, "candidate manifest ID")
        _digest(self.candidate_manifest_sha256, "candidate manifest SHA-256")
        _text(self.availability_calendar_id, "availability calendar ID", maximum=500)
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        published = _utc_datetime(self.published_at_utc, "published_at_utc")
        object.__setattr__(self, "published_at_utc", published)
        if not isinstance(self.release_available_session, date) or isinstance(
            self.release_available_session, datetime
        ):
            raise IdentityAdjudicationError("registry release availability must be a date")
        if self.release_available_session < published.date():
            raise IdentityAdjudicationError("registry release availability is backdated")
        decisions = tuple(sorted(self.decisions, key=lambda item: item.identity_adjudication_id))
        if len(decisions) > 100_000:
            raise IdentityAdjudicationError("registry release exceeds 100000 decisions")
        if len({item.identity_adjudication_id for item in decisions}) != len(decisions):
            raise IdentityAdjudicationError("registry release decision IDs are not unique")
        object.__setattr__(self, "decisions", decisions)

    @property
    def release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate_manifest_id": self.candidate_manifest_id,
            "candidate_manifest_sha256": self.candidate_manifest_sha256,
            "decisions": [item.to_dict() for item in self.decisions],
            "namespace": "ame_stocks.identity.adjudication_registry_release",
            "published_at_utc": _utc_text(self.published_at_utc),
            "release_available_session": self.release_available_session.isoformat(),
            "rule_version": "s7_identity_adjudication_registry_release_id_v1",
            "six_release_binding_id": self.six_release_binding_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "adjudication_registry_release_version": (ADJUDICATION_REGISTRY_RELEASE_VERSION),
            "decision_count": len(self.decisions),
            "release_id": self.release_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> IdentityAdjudicationRegistryRelease:
        item = _mapping(value, "identity adjudication registry release")
        if item.get("adjudication_registry_release_version") != 1:
            raise IdentityAdjudicationError("unsupported adjudication registry release")
        raw = item.get("decisions")
        if not isinstance(raw, list):
            raise IdentityAdjudicationError("registry release decisions must be an array")
        release = cls(
            six_release_binding_id=_string(item, "six_release_binding_id"),
            candidate_manifest_id=_string(item, "candidate_manifest_id"),
            candidate_manifest_sha256=_string(item, "candidate_manifest_sha256"),
            availability_calendar_id=_string(item, "availability_calendar_id"),
            availability_calendar_sha256=_string(item, "availability_calendar_sha256"),
            published_at_utc=_parse_utc(_string(item, "published_at_utc")),
            release_available_session=date.fromisoformat(
                _string(item, "release_available_session")
            ),
            decisions=tuple(ApprovedDecisionArtifactRef.from_dict(row) for row in raw),
        )
        if item.get("decision_count") != len(release.decisions):
            raise IdentityAdjudicationError("registry release decision count differs")
        if item.get("release_id") != release.release_id:
            raise IdentityAdjudicationError("registry release ID recomputation failed")
        return release


@dataclass(frozen=True, slots=True)
class LoadedIdentityAdjudicationRegistryRelease:
    release: IdentityAdjudicationRegistryRelease
    release_document: StoredIdentityDocument
    candidate_manifest: IdentityCaseCandidateManifest
    decisions: tuple[ApprovedIdentityDecision, ...]


class IdentityAdjudicationStore:
    """Fail-closed, append-only storage for S7 identity review controls."""

    def __init__(self, data_root: Path) -> None:
        self.root = data_root.expanduser().resolve()

    def _load_availability_calendar(
        self,
        availability_calendar_id: str,
        availability_calendar_sha256: str,
    ) -> XNYSCalendarArtifact:
        """Load one exact frozen calendar; directory or latest discovery is forbidden."""

        try:
            return load_xnys_calendar_artifact(
                self.root,
                calendar_artifact_id=availability_calendar_id,
                expected_sha256=availability_calendar_sha256,
            )
        except (ArtifactError, XNYSCalendarArtifactError) as exc:
            raise IdentityAdjudicationError(
                "availability calendar ID/SHA trust chain failed"
            ) from exc

    @staticmethod
    def _validate_external_availability(
        calendar: XNYSCalendarArtifact,
        record: ExternalEvidenceCapture | ExternalEvidenceRecord,
    ) -> None:
        try:
            calendar.require_session_open_after(
                max(record.source_published_at_utc, record.captured_at_utc),
                record.source_available_session,
                label="external evidence availability",
            )
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc

    @staticmethod
    def _validate_receipt_calendar(
        receipt: AdjudicationReviewReceipt,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        try:
            if receipt.decision is AdjudicationReviewDecision.APPROVED:
                assert receipt.approval_available_session is not None
                assert receipt.adjudication_available_session is not None
                calendar.require_first_open_session(
                    receipt.reviewed_at_utc,
                    receipt.approval_available_session,
                    label="approval availability",
                )
                calendar.market_open(receipt.adjudication_available_session)
            else:
                calendar.first_open_after(receipt.reviewed_at_utc)
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc

    @staticmethod
    def _validate_decision_calendar(
        decision: ApprovedIdentityDecision,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        try:
            calendar.require_first_open_session(
                decision.approved_at_utc,
                decision.approval_available_session,
                label="approval availability",
            )
            for session in (
                decision.identity_case_available_session,
                decision.evidence_cutoff_session,
                decision.adjudication_available_session,
            ):
                calendar.market_open(session)
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc

    @staticmethod
    def _validate_registry_release_calendar(
        release: IdentityAdjudicationRegistryRelease,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        try:
            calendar.require_first_open_session(
                release.published_at_utc,
                release.release_available_session,
                label="registry release availability",
            )
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc

    def capture_external_evidence(
        self,
        captures: Sequence[ExternalEvidenceCapture],
        *,
        six_release_binding_id: str,
        availability_calendar_id: str,
        availability_calendar_sha256: str,
    ) -> tuple[ExternalEvidenceManifest, StoredIdentityDocument]:
        captures = tuple(captures)
        calendar = self._load_availability_calendar(
            availability_calendar_id,
            availability_calendar_sha256,
        )
        for capture in captures:
            self._validate_external_availability(calendar, capture)
        records = tuple(ExternalEvidenceRecord.from_capture(item) for item in captures)
        manifest = ExternalEvidenceManifest(
            identity_case_id=records[0].identity_case_id if records else "",
            six_release_binding_id=six_release_binding_id,
            availability_calendar_id=availability_calendar_id,
            availability_calendar_sha256=availability_calendar_sha256,
            records=records,
        )
        for capture, record in zip(captures, records, strict=True):
            stored = write_bytes_immutable(
                self.root,
                self.root / record.archived_artifact_path,
                capture.captured_content,
            )
            if (
                stored["sha256"] != record.archived_artifact_sha256
                or stored["bytes"] != record.archived_artifact_bytes
            ):
                raise IdentityAdjudicationError("captured artifact receipt differs")
        path = (
            self.root
            / "manifests/silver/identity/external-evidence/manifests"
            / f"manifest_id={manifest.manifest_id}.json"
        )
        return manifest, _write_document(self.root, path, manifest.to_dict())

    def load_external_evidence(
        self, manifest_id: str
    ) -> tuple[ExternalEvidenceManifest, StoredIdentityDocument]:
        _digest(manifest_id, "external evidence manifest ID")
        path = (
            self.root
            / "manifests/silver/identity/external-evidence/manifests"
            / f"manifest_id={manifest_id}.json"
        )
        document, stored = _read_document(self.root, path)
        manifest = ExternalEvidenceManifest.from_dict(document)
        if manifest.manifest_id != manifest_id:
            raise IdentityAdjudicationError("external manifest path identity differs")
        calendar = self._load_availability_calendar(
            manifest.availability_calendar_id,
            manifest.availability_calendar_sha256,
        )
        for record in manifest.records:
            self._validate_external_availability(calendar, record)
            artifact = safe_relative_path(self.root, record.archived_artifact_path)
            if (
                not artifact.is_file()
                or artifact.stat().st_size != record.archived_artifact_bytes
                or sha256_file(artifact) != record.archived_artifact_sha256
            ):
                raise IdentityAdjudicationError("external archived artifact trust chain failed")
        return manifest, stored

    def store_plan(self, plan: IdentityAdjudicationPlan) -> StoredIdentityDocument:
        candidate = self._verify_candidate_binding(plan.candidate_manifest)
        self._verify_production_candidate_source_binding(
            plan.six_release_binding_id,
            candidate,
        )
        calendar = self._load_availability_calendar(
            plan.availability_calendar_id,
            plan.availability_calendar_sha256,
        )
        self._verify_plan_candidate_cases(plan, candidate, calendar)
        self._verify_plan_external_evidence(plan, calendar)
        path = (
            self.root
            / "manifests/silver/identity/adjudication-plans"
            / f"plan_id={plan.plan_id}"
            / "manifest.json"
        )
        return _write_document(self.root, path, plan.to_dict())

    def load_plan(self, plan_id: str) -> tuple[IdentityAdjudicationPlan, StoredIdentityDocument]:
        _digest(plan_id, "decision plan ID")
        path = (
            self.root
            / "manifests/silver/identity/adjudication-plans"
            / f"plan_id={plan_id}"
            / "manifest.json"
        )
        document, stored = _read_document(self.root, path)
        plan = IdentityAdjudicationPlan.from_dict(document)
        if plan.plan_id != plan_id:
            raise IdentityAdjudicationError("decision plan path identity differs")
        candidate = self._verify_candidate_binding(plan.candidate_manifest)
        self._verify_production_candidate_source_binding(
            plan.six_release_binding_id,
            candidate,
        )
        calendar = self._load_availability_calendar(
            plan.availability_calendar_id,
            plan.availability_calendar_sha256,
        )
        self._verify_plan_candidate_cases(plan, candidate, calendar)
        self._verify_plan_external_evidence(plan, calendar)
        return plan, stored

    def review(
        self,
        plan_id: str,
        identity_adjudication_id: str,
        *,
        decision: AdjudicationReviewDecision,
        reviewed_by: str,
        reviewed_at_utc: datetime,
        review_reason: str,
        approval_available_session: date | None = None,
    ) -> AdjudicationReviewResult:
        plan, plan_document = self.load_plan(plan_id)
        calendar = self._load_availability_calendar(
            plan.availability_calendar_id,
            plan.availability_calendar_sha256,
        )
        proposal = _proposal_by_id(plan, identity_adjudication_id)
        with _adjudication_lock(self.root, proposal.adjudication_series_id):
            return self._review_locked(
                plan,
                plan_document,
                proposal,
                calendar,
                decision=decision,
                reviewed_by=reviewed_by,
                reviewed_at_utc=reviewed_at_utc,
                review_reason=review_reason,
                approval_available_session=approval_available_session,
            )

    def _review_locked(
        self,
        plan: IdentityAdjudicationPlan,
        plan_document: StoredIdentityDocument,
        proposal: IdentityAdjudicationProposal,
        calendar: XNYSCalendarArtifact,
        *,
        decision: AdjudicationReviewDecision,
        reviewed_by: str,
        reviewed_at_utc: datetime,
        review_reason: str,
        approval_available_session: date | None,
    ) -> AdjudicationReviewResult:
        reviewed_at = _utc_datetime(reviewed_at_utc, "reviewed_at_utc")
        if reviewed_at < plan.proposed_at_utc:
            raise IdentityAdjudicationError(
                "review timestamp cannot precede the decision plan proposal"
            )
        identity_adjudication_id = proposal.identity_adjudication_id
        existing = self._receipt_for_decision(identity_adjudication_id)
        if existing is not None:
            raise IdentityAdjudicationError("adjudication proposal already has a review receipt")
        available = None
        if decision is AdjudicationReviewDecision.APPROVED:
            if approval_available_session is None:
                raise IdentityAdjudicationError("approval requires approval_available_session")
            try:
                calendar.require_first_open_session(
                    reviewed_at,
                    approval_available_session,
                    label="approval availability",
                )
            except XNYSCalendarArtifactError as exc:
                raise IdentityAdjudicationError(str(exc)) from exc
            available = max(
                proposal.case.identity_case_available_session,
                proposal.evidence_cutoff_session,
                approval_available_session,
            )
            self._validate_approval_chain(
                proposal,
                calendar=calendar,
                reviewed_at_utc=reviewed_at,
                approval_available_session=approval_available_session,
                adjudication_available_session=available,
            )
        else:
            try:
                calendar.first_open_after(reviewed_at)
            except XNYSCalendarArtifactError as exc:
                raise IdentityAdjudicationError(str(exc)) from exc
        receipt = AdjudicationReviewReceipt(
            plan_id=plan.plan_id,
            plan_path=plan_document.path,
            plan_sha256=plan_document.sha256,
            candidate_manifest_id=plan.candidate_manifest.manifest_id,
            candidate_manifest_sha256=plan.candidate_manifest.manifest_sha256,
            identity_adjudication_id=identity_adjudication_id,
            decision=decision,
            reviewed_by=reviewed_by,
            reviewed_at_utc=reviewed_at,
            review_reason=review_reason,
            approval_available_session=approval_available_session,
            adjudication_available_session=available,
        )
        self._validate_receipt_calendar(receipt, calendar)
        receipt_path = (
            self.root
            / "manifests/silver/identity/adjudication-receipts"
            / f"receipt_id={receipt.receipt_id}.json"
        )
        receipt_document = _write_document(self.root, receipt_path, receipt.to_dict())
        if decision is AdjudicationReviewDecision.REJECTED:
            return AdjudicationReviewResult(receipt, receipt_document, None, None)
        approved = ApprovedIdentityDecision.create(
            proposal,
            plan,
            plan_document,
            receipt,
            receipt_document,
        )
        registry_path = (
            self.root
            / "manifests/silver/identity/adjudication-registry"
            / f"identity_adjudication_id={approved.identity_adjudication_id}"
            / "approved.json"
        )
        approved_document = _write_document(self.root, registry_path, approved.to_dict())
        return AdjudicationReviewResult(
            receipt,
            receipt_document,
            approved,
            approved_document,
        )

    def write_registry_release(
        self,
        identity_adjudication_ids: Sequence[str],
        *,
        candidate_manifest: CandidateManifestBinding,
        six_release_binding_id: str,
        availability_calendar_id: str,
        availability_calendar_sha256: str,
        published_at_utc: datetime,
        release_available_session: date,
    ) -> tuple[IdentityAdjudicationRegistryRelease, StoredIdentityDocument]:
        """Freeze an explicit approved-decision set; directory discovery is forbidden."""

        ids = tuple(sorted(identity_adjudication_ids))
        if len(ids) != len(set(ids)):
            raise IdentityAdjudicationError("registry release decision IDs are not unique")
        calendar = self._load_availability_calendar(
            availability_calendar_id,
            availability_calendar_sha256,
        )
        candidate = self._verify_candidate_binding(candidate_manifest)
        self._verify_production_candidate_source_binding(
            six_release_binding_id,
            candidate,
        )
        if candidate.document.get("six_release_binding_id") != six_release_binding_id:
            raise IdentityAdjudicationError(
                "registry release and candidate six-release bindings differ"
            )
        decisions: list[ApprovedIdentityDecision] = []
        refs: list[ApprovedDecisionArtifactRef] = []
        for identity_adjudication_id in ids:
            decision, stored = self.load_approved_decision(identity_adjudication_id)
            plan, _ = self.load_plan(decision.source_decision_plan_id)
            if (
                plan.candidate_manifest != candidate_manifest
                or plan.six_release_binding_id != six_release_binding_id
                or plan.availability_calendar_id != availability_calendar_id
                or plan.availability_calendar_sha256 != availability_calendar_sha256
            ):
                raise IdentityAdjudicationError(
                    "approved decision bindings differ from the registry release"
                )
            decisions.append(decision)
            self._validate_decision_calendar(decision, calendar)
            refs.append(
                ApprovedDecisionArtifactRef(
                    identity_adjudication_id=identity_adjudication_id,
                    path=stored.path,
                    sha256=stored.sha256,
                )
            )
        _validate_loaded_chains(decisions)
        published_at = _utc_datetime(published_at_utc, "published_at_utc")
        self._verify_registry_chronology(
            candidate,
            decisions,
            calendar=calendar,
            published_at_utc=published_at,
            release_available_session=release_available_session,
        )
        if decisions and release_available_session < max(
            item.adjudication_available_session for item in decisions
        ):
            raise IdentityAdjudicationError(
                "registry release availability precedes an included decision"
            )
        release = IdentityAdjudicationRegistryRelease(
            six_release_binding_id=six_release_binding_id,
            candidate_manifest_id=candidate_manifest.manifest_id,
            candidate_manifest_sha256=candidate_manifest.manifest_sha256,
            availability_calendar_id=availability_calendar_id,
            availability_calendar_sha256=availability_calendar_sha256,
            published_at_utc=published_at,
            release_available_session=release_available_session,
            decisions=tuple(refs),
        )
        self._validate_registry_release_calendar(release, calendar)
        path = (
            self.root
            / "manifests/silver/identity/adjudication-registry-releases"
            / f"release_id={release.release_id}.json"
        )
        return release, _write_document(self.root, path, release.to_dict())

    def load_registry_release(self, release_id: str) -> LoadedIdentityAdjudicationRegistryRelease:
        """Load one exact pinned registry snapshot and revalidate its complete chain."""

        _digest(release_id, "adjudication registry release ID")
        path = (
            self.root
            / "manifests/silver/identity/adjudication-registry-releases"
            / f"release_id={release_id}.json"
        )
        document, stored = _read_document(self.root, path)
        release = IdentityAdjudicationRegistryRelease.from_dict(document)
        if release.release_id != release_id:
            raise IdentityAdjudicationError("registry release path identity differs")
        calendar = self._load_availability_calendar(
            release.availability_calendar_id,
            release.availability_calendar_sha256,
        )
        self._validate_registry_release_calendar(release, calendar)
        candidate_path = (
            "manifests/silver/identity-case-candidates/"
            f"candidate_manifest_id={release.candidate_manifest_id}.json"
        )
        candidate_manifest = self._verify_candidate_binding(
            CandidateManifestBinding(
                manifest_id=release.candidate_manifest_id,
                manifest_sha256=release.candidate_manifest_sha256,
                path=candidate_path,
            )
        )
        if (
            candidate_manifest.document.get("six_release_binding_id")
            != release.six_release_binding_id
        ):
            raise IdentityAdjudicationError(
                "registry release and candidate six-release bindings differ"
            )
        self._verify_production_candidate_source_binding(
            release.six_release_binding_id,
            candidate_manifest,
        )
        decisions: list[ApprovedIdentityDecision] = []
        for ref in release.decisions:
            expected_path = self._approved_decision_path(ref.identity_adjudication_id)
            if ref.path != str(expected_path.relative_to(self.root)):
                raise IdentityAdjudicationError(
                    "registry decision ref does not use its canonical path"
                )
            decision, decision_stored = self.load_approved_decision(ref.identity_adjudication_id)
            if decision_stored.sha256 != ref.sha256:
                raise IdentityAdjudicationError("registry decision SHA binding differs")
            plan, _ = self.load_plan(decision.source_decision_plan_id)
            if (
                plan.six_release_binding_id != release.six_release_binding_id
                or plan.candidate_manifest.manifest_id != release.candidate_manifest_id
                or plan.candidate_manifest.manifest_sha256 != release.candidate_manifest_sha256
                or plan.availability_calendar_id != release.availability_calendar_id
                or plan.availability_calendar_sha256 != release.availability_calendar_sha256
            ):
                raise IdentityAdjudicationError(
                    "registry decision lineage differs from its pinned release"
                )
            decisions.append(decision)
            self._validate_decision_calendar(decision, calendar)
        _validate_loaded_chains(decisions)
        self._verify_registry_chronology(
            candidate_manifest,
            decisions,
            calendar=calendar,
            published_at_utc=release.published_at_utc,
            release_available_session=release.release_available_session,
        )
        if decisions and release.release_available_session < max(
            item.adjudication_available_session for item in decisions
        ):
            raise IdentityAdjudicationError(
                "registry release availability precedes an included decision"
            )
        return LoadedIdentityAdjudicationRegistryRelease(
            release=release,
            release_document=stored,
            candidate_manifest=candidate_manifest,
            decisions=tuple(
                sorted(
                    decisions,
                    key=lambda item: (
                        item.adjudication_series_id,
                        item.decision_version,
                    ),
                )
            ),
        )

    def load_approved_decision(
        self, identity_adjudication_id: str
    ) -> tuple[ApprovedIdentityDecision, StoredIdentityDocument]:
        _digest(identity_adjudication_id, "identity adjudication ID")
        path = self._approved_decision_path(identity_adjudication_id)
        document, stored = _read_document(self.root, path)
        decision = ApprovedIdentityDecision.from_dict(document)
        if decision.identity_adjudication_id != identity_adjudication_id:
            raise IdentityAdjudicationError("approved decision path identity differs")
        self._verify_approved_decision(decision)
        return decision, stored

    def load_approved_decisions(self) -> tuple[ApprovedIdentityDecision, ...]:
        base = self.root / "manifests/silver/identity/adjudication-registry"
        decisions: list[ApprovedIdentityDecision] = []
        for path in sorted(base.glob("identity_adjudication_id=*/approved.json")):
            document, _ = _read_document(self.root, path)
            item = ApprovedIdentityDecision.from_dict(document)
            self._verify_approved_decision(item)
            decisions.append(item)
        _validate_loaded_chains(decisions)
        return tuple(
            sorted(
                decisions,
                key=lambda item: (item.adjudication_series_id, item.decision_version),
            )
        )

    def require_approved_decision(self, identity_adjudication_id: str) -> ApprovedIdentityDecision:
        try:
            return self.load_approved_decision(identity_adjudication_id)[0]
        except (IdentityAdjudicationError, FileNotFoundError) as exc:
            raise IdentityAdjudicationError(
                "canonical identity override has no unique approved registry decision"
            ) from exc

    def list_control_records(self) -> tuple[AdjudicationControlRecord, ...]:
        approved = {item.identity_adjudication_id: item for item in self.load_approved_decisions()}
        superseded = {
            item.supersedes_identity_adjudication_id
            for item in approved.values()
            if item.supersedes_identity_adjudication_id is not None
        }
        receipts = {receipt.identity_adjudication_id: receipt for receipt in self._load_receipts()}
        records: list[AdjudicationControlRecord] = []
        base = self.root / "manifests/silver/identity/adjudication-plans"
        seen: set[str] = set()
        for path in sorted(base.glob("plan_id=*/manifest.json")):
            directory = path.parent.name
            if not directory.startswith("plan_id="):
                raise IdentityAdjudicationError("decision plan path is not canonical")
            plan_id = directory.removeprefix("plan_id=")
            plan, stored = self.load_plan(plan_id)
            if stored.path != str(path.relative_to(self.root)):
                raise IdentityAdjudicationError("decision plan path binding differs")
            for proposal in plan.proposals:
                item_id = proposal.identity_adjudication_id
                if item_id in seen:
                    continue
                seen.add(item_id)
                receipt = receipts.get(item_id)
                if item_id in superseded:
                    state = AdjudicationControlState.SUPERSEDED
                elif item_id in approved:
                    state = AdjudicationControlState.APPROVED
                elif receipt is not None:
                    state = AdjudicationControlState.REJECTED
                else:
                    state = AdjudicationControlState.PROPOSED
                records.append(
                    AdjudicationControlRecord(
                        identity_adjudication_id=item_id,
                        plan_id=plan.plan_id,
                        state=state,
                        decision_version=proposal.decision_version,
                        adjudication_series_id=proposal.adjudication_series_id,
                        receipt_id=None if receipt is None else receipt.receipt_id,
                    )
                )
        return tuple(sorted(records, key=lambda item: item.identity_adjudication_id))

    def _load_receipts(self) -> tuple[AdjudicationReviewReceipt, ...]:
        base = self.root / "manifests/silver/identity/adjudication-receipts"
        receipts: list[AdjudicationReviewReceipt] = []
        for path in sorted(base.glob("receipt_id=*.json")):
            document, stored = _read_document(self.root, path)
            receipt = AdjudicationReviewReceipt.from_dict(document)
            expected_path = (
                "manifests/silver/identity/adjudication-receipts/"
                f"receipt_id={receipt.receipt_id}.json"
            )
            if stored.path != expected_path:
                raise IdentityAdjudicationError("review receipt path binding differs")
            plan, plan_document = self.load_plan(receipt.plan_id)
            if (
                receipt.plan_path != plan_document.path
                or receipt.plan_sha256 != plan_document.sha256
                or receipt.candidate_manifest_id != plan.candidate_manifest.manifest_id
                or receipt.candidate_manifest_sha256 != plan.candidate_manifest.manifest_sha256
                or receipt.reviewed_at_utc < plan.proposed_at_utc
            ):
                raise IdentityAdjudicationError("review receipt plan binding differs")
            proposal = _proposal_by_id(plan, receipt.identity_adjudication_id)
            if receipt.decision is AdjudicationReviewDecision.APPROVED:
                expected_available = max(
                    proposal.case.identity_case_available_session,
                    proposal.evidence_cutoff_session,
                    receipt.approval_available_session,
                )
                if receipt.adjudication_available_session != expected_available:
                    raise IdentityAdjudicationError(
                        "review receipt adjudication availability differs"
                    )
            calendar = self._load_availability_calendar(
                plan.availability_calendar_id,
                plan.availability_calendar_sha256,
            )
            self._validate_receipt_calendar(receipt, calendar)
            receipts.append(receipt)
        ids = [item.identity_adjudication_id for item in receipts]
        if len(ids) != len(set(ids)):
            raise IdentityAdjudicationError("one adjudication has conflicting review receipts")
        return tuple(receipts)

    def _receipt_for_decision(
        self, identity_adjudication_id: str
    ) -> AdjudicationReviewReceipt | None:
        return next(
            (
                item
                for item in self._load_receipts()
                if item.identity_adjudication_id == identity_adjudication_id
            ),
            None,
        )

    def _validate_approval_chain(
        self,
        proposal: IdentityAdjudicationProposal,
        *,
        calendar: XNYSCalendarArtifact,
        reviewed_at_utc: datetime,
        approval_available_session: date,
        adjudication_available_session: date,
    ) -> None:
        approved = self.load_approved_decisions()
        same_series = [
            item
            for item in approved
            if item.adjudication_series_id == proposal.adjudication_series_id
        ]
        for item in same_series:
            self._validate_decision_calendar(item, calendar)
        if proposal.decision_version == 1:
            if same_series:
                raise IdentityAdjudicationError("adjudication series already has version 1")
            return
        predecessors = [
            item
            for item in same_series
            if item.identity_adjudication_id == proposal.supersedes_identity_adjudication_id
        ]
        if len(predecessors) != 1 or (
            predecessors[0].decision_version != proposal.decision_version - 1
        ):
            raise IdentityAdjudicationError("adjudication successor chain is incomplete")
        predecessor = predecessors[0]
        if (
            reviewed_at_utc < predecessor.approved_at_utc
            or approval_available_session < predecessor.approval_available_session
            or adjudication_available_session < predecessor.adjudication_available_session
        ):
            raise IdentityAdjudicationError(
                "adjudication successor chronology precedes its predecessor"
            )
        if any(item.decision_version >= proposal.decision_version for item in same_series):
            raise IdentityAdjudicationError("adjudication decision version already exists")

    def _verify_candidate_binding(
        self, binding: CandidateManifestBinding
    ) -> IdentityCaseCandidateManifest:
        try:
            manifest = read_identity_case_candidate_manifest(
                self.root,
                candidate_manifest_id=binding.manifest_id,
                expected_sha256=binding.manifest_sha256,
            )
        except IdentityBounceError as exc:
            raise IdentityAdjudicationError("candidate manifest ID/SHA trust chain failed") from exc
        if binding.path != manifest.relative_path:
            raise IdentityAdjudicationError("candidate manifest path is not canonical")
        return manifest

    @staticmethod
    def _verify_production_candidate_source_binding(
        six_release_binding_id: str,
        manifest: IdentityCaseCandidateManifest,
    ) -> None:
        if (
            six_release_binding_id == S7_SIX_RELEASE_BINDING_ID
            and manifest.document.get("source_verification") is None
        ):
            raise IdentityAdjudicationError(
                "production candidate manifest lacks exact source-bundle verification"
            )

    def _verify_plan_candidate_cases(
        self,
        plan: IdentityAdjudicationPlan,
        manifest: IdentityCaseCandidateManifest,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        document = manifest.document
        if document.get("six_release_binding_id") != plan.six_release_binding_id:
            raise IdentityAdjudicationError(
                "candidate manifest six-release binding differs from the plan"
            )
        created_at = _parse_utc(_string(document, "created_at_utc"))
        candidate_available = date.fromisoformat(
            _string(document, "candidate_manifest_available_session")
        )
        if plan.proposed_at_utc < created_at:
            raise IdentityAdjudicationError(
                "decision plan proposal cannot precede candidate manifest creation"
            )
        try:
            calendar.require_first_open_session(
                created_at,
                candidate_available,
                label="candidate manifest availability",
            )
            calendar.require_timestamp_at_or_after_open(
                plan.proposed_at_utc,
                candidate_available,
                label="decision plan candidate availability",
            )
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc
        raw_cases = document.get("cases")
        if not isinstance(raw_cases, list):
            raise IdentityAdjudicationError("candidate manifest cases are unavailable")
        cases = {
            row.get("identity_case_id"): row
            for row in raw_cases
            if isinstance(row, dict) and isinstance(row.get("identity_case_id"), str)
        }
        for proposal in plan.proposals:
            controlling_session = max(
                proposal.case.identity_case_available_session,
                proposal.evidence_cutoff_session,
            )
            try:
                calendar.market_open(proposal.case.identity_case_available_session)
                for evidence in proposal.evidence_refs:
                    calendar.market_open(evidence.source_available_session)
                calendar.require_timestamp_at_or_after_open(
                    plan.proposed_at_utc,
                    controlling_session,
                    label="decision plan evidence availability",
                )
            except XNYSCalendarArtifactError as exc:
                raise IdentityAdjudicationError(str(exc)) from exc
            case = proposal.case
            row = cases.get(case.identity_case_id)
            expected = {
                "detector_rule_version": case.detector_rule_version,
                "episode_source_record_set_digest": (case.episode_source_record_set_digest),
                "episode_valid_from_session": case.episode_valid_from_session.isoformat(),
                "episode_valid_through_session": (case.episode_valid_through_session.isoformat()),
                "identity_case_available_session": (
                    case.identity_case_available_session.isoformat()
                ),
                "identity_case_id": case.identity_case_id,
                "left_outer_composite_figi": case.left_outer_composite_figi,
                "middle_observed_composite_figi": case.observed_composite_figi,
                "middle_session_count": case.episode_source_record_count,
                "right_outer_composite_figi": case.right_outer_composite_figi,
                "ticker": case.observed_ticker,
            }
            if row is None or any(row.get(key) != value for key, value in expected.items()):
                raise IdentityAdjudicationError(
                    "proposal case does not exactly match the candidate manifest"
                )
            admitted_provider_ids = {
                row.get("left_outer_source_record_id"),
                row.get("right_outer_source_record_id"),
                *row.get("episode_source_record_ids", []),
                *row.get("s5_source_record_ids", []),
                *row.get("s6_source_record_ids", []),
                *row.get("hierarchy_source_record_ids", []),
            }
            for evidence in proposal.evidence_refs:
                if evidence.source_type is not EvidenceSourceType.PINNED_MASSIVE_RELEASE:
                    continue
                source = thaw_json(evidence.source)
                if source.get("source_record_id") not in admitted_provider_ids:
                    raise IdentityAdjudicationError(
                        "provider evidence record is outside exact candidate lineage"
                    )

    def _verify_external_refs(
        self,
        plan: IdentityAdjudicationPlan,
        external: ExternalEvidenceManifest,
    ) -> None:
        admitted = {item.external_evidence_id: item for item in external.records}
        for proposal in plan.proposals:
            for ref in proposal.evidence_refs:
                if ref.source_type is not EvidenceSourceType.EXTERNAL_IMMUTABLE_SNAPSHOT:
                    continue
                source = thaw_json(ref.source)
                record = admitted.get(ref.evidence_ref)
                if (
                    record is None
                    or record.identity_case_id != proposal.case.identity_case_id
                    or source.get("external_evidence_id") != ref.evidence_ref
                    or source.get("identity_case_id") != proposal.case.identity_case_id
                    or source.get("source_external_evidence_manifest_id") != external.manifest_id
                    or ref.source_available_session != record.source_available_session
                ):
                    raise IdentityAdjudicationError("external evidence ref is not admitted")
                if plan.proposed_at_utc < record.captured_at_utc:
                    raise IdentityAdjudicationError(
                        "decision plan proposal cannot precede external evidence capture"
                    )

    def _verify_plan_external_evidence(
        self,
        plan: IdentityAdjudicationPlan,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        if plan.external_evidence_manifest_id is None:
            return
        external, stored = self.load_external_evidence(plan.external_evidence_manifest_id)
        if stored.sha256 != plan.external_evidence_manifest_sha256:
            raise IdentityAdjudicationError("external manifest SHA binding differs")
        if (
            external.six_release_binding_id != plan.six_release_binding_id
            or external.availability_calendar_id != plan.availability_calendar_id
            or external.availability_calendar_sha256 != plan.availability_calendar_sha256
        ):
            raise IdentityAdjudicationError(
                "external manifest release/calendar binding differs from the plan"
            )
        for record in external.records:
            self._validate_external_availability(calendar, record)
        self._verify_external_refs(plan, external)

    @staticmethod
    def _verify_registry_chronology(
        candidate: IdentityCaseCandidateManifest,
        decisions: Sequence[ApprovedIdentityDecision],
        *,
        calendar: XNYSCalendarArtifact,
        published_at_utc: datetime,
        release_available_session: date,
    ) -> None:
        document = candidate.document
        candidate_created = _parse_utc(_string(document, "created_at_utc"))
        candidate_available = date.fromisoformat(
            _string(document, "candidate_manifest_available_session")
        )
        try:
            calendar.require_first_open_session(
                candidate_created,
                candidate_available,
                label="candidate manifest availability",
            )
            calendar.require_first_open_session(
                published_at_utc,
                release_available_session,
                label="registry release availability",
            )
        except XNYSCalendarArtifactError as exc:
            raise IdentityAdjudicationError(str(exc)) from exc
        if published_at_utc < candidate_created:
            raise IdentityAdjudicationError(
                "registry publication cannot precede candidate manifest creation"
            )
        if decisions and published_at_utc < max(item.approved_at_utc for item in decisions):
            raise IdentityAdjudicationError(
                "registry publication cannot precede an included approval"
            )
        if release_available_session < candidate_available:
            raise IdentityAdjudicationError(
                "registry release availability precedes the candidate manifest"
            )

    def _verify_approved_decision(self, item: ApprovedIdentityDecision) -> None:
        if item.approval_status != "approved":
            raise IdentityAdjudicationError("registry contains a non-approved decision")
        plan, plan_document = self.load_plan(item.source_decision_plan_id)
        calendar = self._load_availability_calendar(
            plan.availability_calendar_id,
            plan.availability_calendar_sha256,
        )
        self._validate_decision_calendar(item, calendar)
        if (
            plan_document.path != item.source_decision_plan_path
            or plan_document.sha256 != item.source_decision_plan_sha256
        ):
            raise IdentityAdjudicationError("approved decision plan binding differs")
        proposal = _proposal_by_id(plan, item.identity_adjudication_id)
        receipt_path = safe_relative_path(self.root, item.approval_receipt_path)
        document, stored = _read_document(self.root, receipt_path)
        receipt = AdjudicationReviewReceipt.from_dict(document)
        self._validate_receipt_calendar(receipt, calendar)
        if (
            receipt.receipt_id != item.approval_id
            or stored.sha256 != item.approval_receipt_sha256
            or receipt.decision is not AdjudicationReviewDecision.APPROVED
        ):
            raise IdentityAdjudicationError("approved decision receipt binding differs")
        reproduced = ApprovedIdentityDecision.create(
            proposal,
            plan,
            plan_document,
            receipt,
            stored,
        )
        if reproduced != item:
            raise IdentityAdjudicationError("approved registry record recomputation failed")

    def _approved_decision_path(self, identity_adjudication_id: str) -> Path:
        _digest(identity_adjudication_id, "identity adjudication ID")
        return (
            self.root
            / "manifests/silver/identity/adjudication-registry"
            / f"identity_adjudication_id={identity_adjudication_id}"
            / "approved.json"
        )


def _validate_loaded_chains(decisions: Sequence[ApprovedIdentityDecision]) -> None:
    by_id = {item.identity_adjudication_id: item for item in decisions}
    if len(by_id) != len(decisions):
        raise IdentityAdjudicationError("approved registry has duplicate decision IDs")
    version_keys = [(item.adjudication_series_id, item.decision_version) for item in decisions]
    if len(version_keys) != len(set(version_keys)):
        raise IdentityAdjudicationError("approved registry has duplicate series versions")
    for item in decisions:
        if item.decision_version == 1:
            if item.supersedes_identity_adjudication_id is not None:
                raise IdentityAdjudicationError("registry version 1 has a predecessor")
            continue
        predecessor = by_id.get(item.supersedes_identity_adjudication_id or "")
        if (
            predecessor is None
            or predecessor.adjudication_series_id != item.adjudication_series_id
            or predecessor.decision_version != item.decision_version - 1
        ):
            raise IdentityAdjudicationError("approved registry chain is incomplete")
        if (
            item.approved_at_utc < predecessor.approved_at_utc
            or item.approval_available_session < predecessor.approval_available_session
            or item.adjudication_available_session < predecessor.adjudication_available_session
        ):
            raise IdentityAdjudicationError("approved registry successor chronology moves backward")


@contextmanager
def _adjudication_lock(root: Path, adjudication_series_id: str) -> Iterator[IO[str]]:
    _digest(adjudication_series_id, "adjudication series ID")
    relative = (
        f"manifests/silver/identity/adjudication-locks/series_id={adjudication_series_id}.lock"
    )
    path = safe_relative_path(root, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield handle
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _proposal_by_id(
    plan: IdentityAdjudicationPlan, identity_adjudication_id: str
) -> IdentityAdjudicationProposal:
    matches = [
        item for item in plan.proposals if item.identity_adjudication_id == identity_adjudication_id
    ]
    if len(matches) != 1:
        raise IdentityAdjudicationError("decision plan has no unique requested proposal")
    return matches[0]


def _record_kwargs(payload: Mapping[str, object]) -> dict[str, object]:
    return {
        "identity_case_id": payload["identity_case_id"],
        "source_authority_class": ExternalAuthorityClass(payload["source_authority_class"]),
        "source_name": payload["source_name"],
        "normalized_url": payload["normalized_url"],
        "source_published_at_utc": _parse_utc(payload["source_published_at_utc"]),
        "observed_at_utc": _parse_utc(payload["observed_at_utc"]),
        "as_of_at_utc": _parse_utc(payload["as_of_at_utc"]),
        "captured_at_utc": _parse_utc(payload["captured_at_utc"]),
        "source_available_session": date.fromisoformat(payload["source_available_session"]),
        "asserted_fields": tuple(payload["asserted_fields"]),
        "assertion": ensure_json_safe(payload["assertion"], label="external assertion"),
        "media_type": payload["media_type"],
        "license_name": payload["license_name"],
        "license_url": payload["license_url"],
        "archived_artifact_path": payload["archived_artifact_path"],
        "archived_artifact_sha256": payload["archived_artifact_sha256"],
        "archived_artifact_bytes": payload["archived_artifact_bytes"],
    }


def _write_document(root: Path, path: Path, document: dict[str, object]) -> StoredIdentityDocument:
    content = _canonical_bytes(document)
    stored = write_bytes_immutable(root, path, content)
    return StoredIdentityDocument(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _read_document(root: Path, path: Path) -> tuple[dict[str, object], StoredIdentityDocument]:
    if not path.is_file() or path.is_symlink():
        raise IdentityAdjudicationError(f"identity control artifact is missing: {path}")
    document = _load_json(path, "identity control artifact")
    content = path.read_bytes()
    if content != _canonical_bytes(document):
        raise IdentityAdjudicationError("identity control artifact bytes are not canonical")
    return document, StoredIdentityDocument(
        path=str(path.relative_to(root)),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _canonical_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IdentityAdjudicationError(f"cannot read {label}: {path}") from exc
    if not isinstance(value, dict):
        raise IdentityAdjudicationError(f"{label} must be a JSON object")
    return value


def _normalized_public_url(value: str) -> str:
    _text(value, "source URL", maximum=4_000)
    parsed = urlsplit(value)
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise IdentityAdjudicationError("external evidence URL must be public HTTPS")
    if parsed.fragment:
        raise IdentityAdjudicationError("external evidence URL cannot contain a fragment")
    try:
        port = parsed.port
    except ValueError as exc:
        raise IdentityAdjudicationError("external evidence URL port is invalid") from exc
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    if query_keys.intersection(_SENSITIVE_QUERY_KEYS):
        raise IdentityAdjudicationError("external evidence URL contains a sensitive query")
    host = parsed.hostname.lower()
    netloc = host if port in {None, 443} else f"{host}:{port}"
    return urlunsplit(("https", netloc, parsed.path or "/", parsed.query, ""))


def _contains_outcome_or_backtest_token(value: object) -> bool:
    """Reject outcome leakage anywhere in a recursively frozen identity assertion."""

    if isinstance(value, Mapping):
        return any(
            any(token in str(key).lower() for token in _OUTCOME_FIELD_TOKENS)
            or _contains_outcome_or_backtest_token(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_outcome_or_backtest_token(item) for item in value)
    return isinstance(value, str) and any(token in value.lower() for token in _OUTCOME_FIELD_TOKENS)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise IdentityAdjudicationError(f"{label} must be an object")
    return value


def _string(item: Mapping[str, object], key: str) -> str:
    value = item.get(key)
    if not isinstance(value, str):
        raise IdentityAdjudicationError(f"{key} must be a string")
    return value


def _optional_string(item: Mapping[str, object], key: str) -> str | None:
    value = item.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise IdentityAdjudicationError(f"{key} must be a string or null")
    return value


def _string_list(item: Mapping[str, object], key: str) -> list[str]:
    value = item.get(key)
    if not isinstance(value, list) or any(not isinstance(row, str) for row in value):
        raise IdentityAdjudicationError(f"{key} must be a string array")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityAdjudicationError(f"{label} must be a positive native int")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityAdjudicationError(f"{label} must be a native bool")
    return value


def _digest(value: object, label: str) -> None:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise IdentityAdjudicationError(f"{label} must be a lowercase SHA-256")


def _text(value: object, label: str, *, maximum: int) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise IdentityAdjudicationError(f"{label} must be non-empty and <= {maximum} chars")
    if any(ord(character) < 32 for character in value):
        raise IdentityAdjudicationError(f"{label} contains control characters")


def _field_text(value: object, label: str) -> None:
    if not isinstance(value, str) or not _FIELD.fullmatch(value):
        raise IdentityAdjudicationError(f"{label} must be a safe field identifier")


def _relative_path_text(value: object, label: str) -> None:
    _text(value, label, maximum=2_000)
    path = Path(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise IdentityAdjudicationError(f"{label} must be a normalized relative path")


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise IdentityAdjudicationError(f"{label} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset() != UTC.utcoffset(value):
        raise IdentityAdjudicationError(f"{label} must be explicitly UTC")
    return normalized


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise IdentityAdjudicationError("UTC timestamp must be a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IdentityAdjudicationError("UTC timestamp is invalid") from exc
    return _utc_datetime(parsed, "UTC timestamp")


def _date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise IdentityAdjudicationError("optional date must be an ISO string or null")
    return date.fromisoformat(value)
