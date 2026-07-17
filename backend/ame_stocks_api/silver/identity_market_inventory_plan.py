"""Fail-closed control artifacts for the S7 Composite-inventory prerequisite.

This module has no inventory runner, market classifier, network client,
adjudication, derived-table materializer, or publication capability.  It only
records the exact schema/evidence approval already supplied by the user and
freezes one immutable Gate-A plan plus its human approval request.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentitySourcePin,
)

SCHEMA_EVIDENCE_APPROVAL_SCHEMA_VERSION: Final = 1
MARKET_INVENTORY_PLAN_SCHEMA_VERSION: Final = 1
MARKET_INVENTORY_REQUEST_SCHEMA_VERSION: Final = 1

SCHEMA_EVIDENCE_APPROVAL_RULE_VERSION: Final = "s7_schema_evidence_bundle_approval_v1"
MARKET_INVENTORY_PLAN_RULE_VERSION: Final = "s7_composite_inventory_plan_v1"
MARKET_INVENTORY_REQUEST_RULE_VERSION: Final = "s7_composite_inventory_request_v1"
MARKET_INVENTORY_LITERAL_VERSION: Final = "s7_composite_inventory_approval_literal_v1"

MARKET_INVENTORY_PLAN_STATE: Final = "awaiting_exact_plan_approval"
MARKET_INVENTORY_REQUEST_STATE: Final = "awaiting_literal_human_approval"
MARKET_INVENTORY_AUTHORIZED_ACTION: Final = (
    "execute_exact_s4_full_history_composite_inventory_once_to_awaiting_review"
)
MARKET_INVENTORY_SCOPE: Final = (
    "full_s4_composite_inventory_candidate_only_no_market_classification_"
    "no_adjudication_no_materialization"
)

APPROVED_SUBJECT_GIT_COMMIT: Final = "04540a68bc86a3be2cff1e38f43e022aabe8e482"
APPROVED_SUBJECT_GIT_TREE: Final = "6af952541dc17c1738b4e881abdf2902b5ad5bb8"

APPROVAL_TEXT: Final = (
    "批准 S7 schema/evidence package\uff0c"
    "仅批准以下六份 schema contracts 与 external evidence manifest\uff1b"
    "不授权 identity_market_consistency scan、任何 adjudication plan、registry release、"
    "四表 materialization、FullRunPlan 或 PublishPlan。\n\n"
    """identity_adjudication
Contract ID: 6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e
Candidate SHA-256: eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231

identity_cross_market_adjudication
Contract ID: ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8
Candidate SHA-256: a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0

asset_master
Contract ID: 959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149
Candidate SHA-256: bfb31004df41c4556e71beb379bb36e07063f36298d329c887be48c005b02fa5

ticker_alias
Contract ID: 39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce
Candidate SHA-256: 8bf758af5c358c79477ff40177aab5f3b7c8d26f7f0882e261f7d844a66a1f95

issuer_master
Contract ID: 2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408
Candidate SHA-256: adee0a5457ac32356a0ec9b9a28c692fcebdacc4ba9cccedd1237e8c66b722b7

universe_daily
Contract ID: 38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3
Candidate SHA-256: c0923508dafa0d56de4be6b8ff43187a581627dd1d64e964cf5f506f5ce8ea0b

external evidence manifest
Manifest ID: 2ae779168e3e56887a5b0ae557bb928b6006c1b96392fe1606c201e1649ff848
Candidate SHA-256: 9544537ac7e6817c1b8f946c9ae2d5afb65399b1b553c3fe233a298614b375ab"""
)
EXPECTED_APPROVAL_TEXT_SHA256: Final = (
    "ceb0160c00aef8a69f09570266a55648cfec3ff044acf451334c26ee374c00b9"
)
APPROVAL_TEXT_SHA256: Final = EXPECTED_APPROVAL_TEXT_SHA256
if hashlib.sha256(APPROVAL_TEXT.encode("utf-8")).hexdigest() != APPROVAL_TEXT_SHA256:
    raise RuntimeError("embedded exact S7 schema/evidence approval text changed")

EXTERNAL_EVIDENCE_MANIFEST_PATH: Final = (
    "docs/silver/evidence/s7-cross-market/"
    "identity-cross-market-external-evidence-manifest.candidate.json"
)
EXTERNAL_EVIDENCE_MANIFEST_ID: Final = (
    "2ae779168e3e56887a5b0ae557bb928b6006c1b96392fe1606c201e1649ff848"
)
EXTERNAL_EVIDENCE_MANIFEST_SHA256: Final = (
    "9544537ac7e6817c1b8f946c9ae2d5afb65399b1b553c3fe233a298614b375ab"
)

INVENTORY_START_SESSION: Final = date(2016, 7, 11)
INVENTORY_END_SESSION: Final = date(2026, 7, 9)
INVENTORY_SESSION_COUNT: Final = 2_513
INVENTORY_CALENDAR_ARTIFACT_ID: Final = (
    "31cc575ae55542a580ee17e09aa242159bbcaedd0a001fd2184021a541b734bd"
)
INVENTORY_CALENDAR_ARTIFACT_SHA256: Final = (
    "3f026761a9f752d1e00c89c9f72383e7d8c0a7f7dcb2cdf8ef82e5831dfc0da7"
)
DAILY_SOURCE_ARTIFACT_COUNT: Final = 5_026
DAILY_SOURCE_ROW_COUNT: Final = 138_757_511
DAILY_SOURCE_BYTES: Final = 15_910_278_169

PREVIEW_ARTIFACT_ID: Final = "306543f5fc1d30f868482392aaafdc781daf9f36f30d3f12504024c10f865c70"
PREVIEW_ARTIFACT_SHA256: Final = "daf902fd23c11993aac42998d3676ad8defeb9e2884fe30bea0e74c71fc79700"
PREVIEW_COMPLETION_ID: Final = "7a1e2386e18428aecf50a9ce322eaaf6b3035307b4a704939584288f131c6b9d"
PREVIEW_COMPLETION_SHA256: Final = (
    "2d57dffb3602f8ae77f0f733ac11dbd88dc610fcc233b773d5eb3a3ce5a081bf"
)
PREVIEW_CASE_EVIDENCE_SET_DIGEST: Final = (
    "d19f8a1abbf83a4aacf50844792d9bb2eaca741fbe8e2d010381eb3b7619b907"
)
PREVIEW_CASE_COUNT: Final = 19
PREVIEW_SUSPECTED_ROW_COUNT: Final = 89
PREVIEW_PLAN_ID: Final = "b0cccdd8303b25a1af9a7f145dd3f95356d16d5e05fa527c8fd5cb22f7fd4fa8"
PREVIEW_APPROVAL_ID: Final = "b941f839bdd524fc901f7db26c1a4fd1dfe523efa97f09ab14c3986586cdd306"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class IdentityMarketInventoryPlanError(RuntimeError):
    """Raised when an S7 Gate-A control artifact is unsafe or inconsistent."""


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityMarketInventoryPlanError(f"{label} must be an object")
    return dict(value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise IdentityMarketInventoryPlanError(f"{label} must be an array")
    return list(value)


def _expect_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(document) != expected:
        raise IdentityMarketInventoryPlanError(
            f"{label} schema is not exact: "
            f"missing={sorted(expected - set(document))}, "
            f"extra={sorted(set(document) - expected)}"
        )


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketInventoryPlanError(f"{label} must be text")
    return value


def _identifier(value: object, label: str) -> str:
    text = _string(value, label)
    if not re.fullmatch(r"[a-z][a-z0-9_]*", text):
        raise IdentityMarketInventoryPlanError(f"{label} is invalid")
    return text


def _digest(value: object, label: str) -> str:
    text = _string(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityMarketInventoryPlanError(f"{label} must be lowercase 64-hex")
    return text


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityMarketInventoryPlanError(f"{label} must be a positive native int")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityMarketInventoryPlanError(f"{label} must be a nonnegative native int")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityMarketInventoryPlanError(f"{label} must be bool")
    return value


def _safe_text(value: object, label: str, maximum: int) -> str:
    text = _string(value, label)
    if (
        not text
        or len(text) > maximum
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise IdentityMarketInventoryPlanError(f"{label} is unsafe")
    return text


def _relative_path(value: object, label: str) -> str:
    text = _string(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityMarketInventoryPlanError(f"{label} is not a safe relative path")
    return text


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityMarketInventoryPlanError(f"{label} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset().total_seconds() != 0:
        raise IdentityMarketInventoryPlanError(f"{label} must be UTC")
    return normalized


def _utc_text(value: datetime) -> str:
    return _utc_datetime(value, "UTC datetime").isoformat()


def _parse_utc(value: object, label: str) -> datetime:
    text = _string(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityMarketInventoryPlanError(f"{label} is not ISO-8601") from exc
    normalized = _utc_datetime(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityMarketInventoryPlanError(f"{label} is not canonical UTC")
    return normalized


@dataclass(frozen=True, slots=True)
class StoredIdentityMarketInventoryDocument:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored document path")
        _digest(self.sha256, "stored document SHA-256")
        _positive_int(self.bytes, "stored document bytes")


@dataclass(frozen=True, slots=True, order=True)
class S7ApprovedContractPin:
    table: str
    domain: str
    candidate_path: str
    contract_id: str
    schema_digest: str
    candidate_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.table, "contract table")
        _identifier(self.domain, "contract domain")
        _relative_path(self.candidate_path, "contract candidate path")
        _digest(self.contract_id, "contract ID")
        _digest(self.schema_digest, "contract schema digest")
        _digest(self.candidate_sha256, "contract candidate SHA-256")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_path": self.candidate_path,
            "candidate_sha256": self.candidate_sha256,
            "contract_id": self.contract_id,
            "domain": self.domain,
            "schema_digest": self.schema_digest,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ApprovedContractPin:
        document = _mapping(value, "approved contract pin")
        _expect_keys(document, set(_CONTRACT_PIN_KEYS), "approved contract pin")
        return cls(**{key: _string(document[key], key) for key in _CONTRACT_PIN_KEYS})


_CONTRACT_PIN_KEYS = (
    "table",
    "domain",
    "candidate_path",
    "contract_id",
    "schema_digest",
    "candidate_sha256",
)

APPROVED_CONTRACT_PINS: Final = tuple(
    sorted(
        (
            S7ApprovedContractPin(
                "identity_adjudication",
                "identity",
                "docs/silver/contracts/identity/identity_adjudication.schema-v1.candidate.json",
                "6423cc01b952498cc78d55e93a349d7afe408bd30003e4f7be59f211102f2d5e",
                "e5082a8611bedb6913f79da506f1f5cc19c94507b9e27d04edfb88566033575f",
                "eb5e9d1746ad2014d7b0e4a9a56ffa29e4f36cf1e1d18d348634a058f0d22231",
            ),
            S7ApprovedContractPin(
                "identity_cross_market_adjudication",
                "identity",
                "docs/silver/contracts/identity/identity_cross_market_adjudication.schema-v1.candidate.json",
                "ae91c7b1bfc27bde82e5f5a39afdc5a3c2c9929d075486cb081836b6798e14e8",
                "96fe9108cd246919a9a00855d04d9f4057c439b6043d4d67178beb1c32d7a0fe",
                "a7308e22c07e8243a8587bfc7eab7ae45b2f232fe9bba310d084916d722f56d0",
            ),
            S7ApprovedContractPin(
                "asset_master",
                "identity",
                "docs/silver/contracts/identity/asset_master.schema-v1.candidate.json",
                "959c5f7bf464eed59fd32a7008349f60ebcfd3cf9e892c9c3d7f00080eae2149",
                "5ef86bbe8e3e0219e795ed9f8c5c9eca35ebc7b16ff21a903901765b3e7d53d3",
                "bfb31004df41c4556e71beb379bb36e07063f36298d329c887be48c005b02fa5",
            ),
            S7ApprovedContractPin(
                "ticker_alias",
                "identity",
                "docs/silver/contracts/identity/ticker_alias.schema-v1.candidate.json",
                "39dbf6ef89ed4c2d466fa0be2e47d2840a90f1a97f6a47670af05df3e15513ce",
                "2f857bc07319426e48494901571a570b1abf622c16c9e429ab8185c08af2d743",
                "8bf758af5c358c79477ff40177aab5f3b7c8d26f7f0882e261f7d844a66a1f95",
            ),
            S7ApprovedContractPin(
                "issuer_master",
                "identity",
                "docs/silver/contracts/identity/issuer_master.schema-v1.candidate.json",
                "2faa8d4d2e10e4a065b10b9ae851e53ac517db7e69af4fd59d5f6edc677aa408",
                "dac9dbe43450cf094c8170d8e88db1742fb035052df9d1b78b7ced02cc4282d2",
                "adee0a5457ac32356a0ec9b9a28c692fcebdacc4ba9cccedd1237e8c66b722b7",
            ),
            S7ApprovedContractPin(
                "universe_daily",
                "reference",
                "docs/silver/contracts/reference/universe_daily.schema-v1.candidate.json",
                "38cd59c4e4b04de8444ba99ed93e6fd8c7a78aec24f01205d7df7494bcfd33d3",
                "80902539df5dc822dc43a88cf7325b16f4fdc2c4c6786c78ea93434116e6e25a",
                "c0923508dafa0d56de4be6b8ff43187a581627dd1d64e964cf5f506f5ce8ea0b",
            ),
        )
    )
)


@dataclass(frozen=True, slots=True)
class S7SchemaEvidenceApprovalBundle:
    recorded_by: str
    recorded_at_utc: datetime
    approval_text_sha256: str
    contract_pins: tuple[S7ApprovedContractPin, ...] = APPROVED_CONTRACT_PINS
    evidence_manifest_id: str = EXTERNAL_EVIDENCE_MANIFEST_ID
    evidence_manifest_path: str = EXTERNAL_EVIDENCE_MANIFEST_PATH
    evidence_manifest_sha256: str = EXTERNAL_EVIDENCE_MANIFEST_SHA256
    subject_git_commit: str = APPROVED_SUBJECT_GIT_COMMIT
    subject_git_tree: str = APPROVED_SUBJECT_GIT_TREE

    def __post_init__(self) -> None:
        _safe_text(self.recorded_by, "recorded_by", 200)
        object.__setattr__(
            self, "recorded_at_utc", _utc_datetime(self.recorded_at_utc, "recorded_at_utc")
        )
        if self.approval_text_sha256 != APPROVAL_TEXT_SHA256:
            raise IdentityMarketInventoryPlanError("approval text SHA-256 is not exact")
        if tuple(self.contract_pins) != APPROVED_CONTRACT_PINS:
            raise IdentityMarketInventoryPlanError("approved contract package is not exact")
        _digest(self.evidence_manifest_id, "evidence manifest ID")
        _relative_path(self.evidence_manifest_path, "evidence manifest path")
        _digest(self.evidence_manifest_sha256, "evidence manifest SHA-256")
        if (
            self.evidence_manifest_id != EXTERNAL_EVIDENCE_MANIFEST_ID
            or self.evidence_manifest_path != EXTERNAL_EVIDENCE_MANIFEST_PATH
            or self.evidence_manifest_sha256 != EXTERNAL_EVIDENCE_MANIFEST_SHA256
        ):
            raise IdentityMarketInventoryPlanError("approved evidence manifest is not exact")
        if self.subject_git_commit != APPROVED_SUBJECT_GIT_COMMIT:
            raise IdentityMarketInventoryPlanError("subject Git commit provenance differs")
        if self.subject_git_tree != APPROVED_SUBJECT_GIT_TREE:
            raise IdentityMarketInventoryPlanError("subject Git tree provenance differs")

    @classmethod
    def create(
        cls,
        *,
        recorded_by: str,
        recorded_at_utc: datetime,
        exact_approval_text: str,
    ) -> S7SchemaEvidenceApprovalBundle:
        """Record only the byte-exact schema/evidence approval already supplied."""

        if not isinstance(exact_approval_text, str) or exact_approval_text != APPROVAL_TEXT:
            raise IdentityMarketInventoryPlanError("approval text is not byte-for-byte exact")
        return cls(
            recorded_by=recorded_by,
            recorded_at_utc=recorded_at_utc,
            approval_text_sha256=hashlib.sha256(exact_approval_text.encode("utf-8")).hexdigest(),
        )

    @property
    def package_digest(self) -> str:
        return stable_digest(
            {
                "contracts": [item.to_dict() for item in self.contract_pins],
                "evidence_manifest_id": self.evidence_manifest_id,
                "evidence_manifest_path": self.evidence_manifest_path,
                "evidence_manifest_sha256": self.evidence_manifest_sha256,
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_rule_version": SCHEMA_EVIDENCE_APPROVAL_RULE_VERSION,
            "approval_scope": {
                "adjudication_plan_authorized": False,
                "composite_inventory_execution_authorized": False,
                "external_evidence_admitted_for_schema_review": True,
                "full_run_authorized": False,
                "identity_market_consistency_scan_authorized": False,
                "publish_authorized": False,
                "registry_release_authorized": False,
                "schema_contracts_approved": True,
                "table_materialization_authorized": False,
            },
            "approval_text": APPROVAL_TEXT,
            "approval_text_sha256": self.approval_text_sha256,
            "artifact_type": "s7_schema_evidence_approval_bundle",
            "contracts": [item.to_dict() for item in self.contract_pins],
            "evidence_manifest": {
                "manifest_id": self.evidence_manifest_id,
                "path": self.evidence_manifest_path,
                "sha256": self.evidence_manifest_sha256,
            },
            "package_digest": self.package_digest,
            "recorded_at_utc": _utc_text(self.recorded_at_utc),
            "recorded_by": self.recorded_by,
            "schema_version": SCHEMA_EVIDENCE_APPROVAL_SCHEMA_VERSION,
            "subject_git_provenance": {
                "commit": self.subject_git_commit,
                "commit_short": self.subject_git_commit[:7],
                "tree": self.subject_git_tree,
            },
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "approval_id": self.approval_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return schema_evidence_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7SchemaEvidenceApprovalBundle:
        document = _mapping(value, "schema/evidence approval bundle")
        _expect_keys(
            document,
            {
                "approval_id",
                "approval_rule_version",
                "approval_scope",
                "approval_text",
                "approval_text_sha256",
                "artifact_type",
                "contracts",
                "evidence_manifest",
                "package_digest",
                "recorded_at_utc",
                "recorded_by",
                "schema_version",
                "subject_git_provenance",
            },
            "schema/evidence approval bundle",
        )
        pins = tuple(
            S7ApprovedContractPin.from_dict(item)
            for item in _list(document.get("contracts"), "contracts")
        )
        evidence = _mapping(document.get("evidence_manifest"), "evidence manifest")
        _expect_keys(evidence, {"manifest_id", "path", "sha256"}, "evidence manifest")
        provenance = _mapping(document.get("subject_git_provenance"), "subject Git provenance")
        _expect_keys(provenance, {"commit", "commit_short", "tree"}, "subject Git provenance")
        bundle = cls(
            recorded_by=_string(document.get("recorded_by"), "recorded_by"),
            recorded_at_utc=_parse_utc(document.get("recorded_at_utc"), "recorded_at_utc"),
            approval_text_sha256=_string(
                document.get("approval_text_sha256"), "approval_text_sha256"
            ),
            contract_pins=pins,
            evidence_manifest_id=_string(evidence.get("manifest_id"), "manifest_id"),
            evidence_manifest_path=_string(evidence.get("path"), "evidence path"),
            evidence_manifest_sha256=_string(evidence.get("sha256"), "evidence SHA-256"),
            subject_git_commit=_string(provenance.get("commit"), "subject commit"),
            subject_git_tree=_string(provenance.get("tree"), "subject tree"),
        )
        if document.get("artifact_type") != "s7_schema_evidence_approval_bundle":
            raise IdentityMarketInventoryPlanError("approval artifact type is invalid")
        if document.get("schema_version") != SCHEMA_EVIDENCE_APPROVAL_SCHEMA_VERSION:
            raise IdentityMarketInventoryPlanError("approval schema version is invalid")
        if document.get("approval_rule_version") != SCHEMA_EVIDENCE_APPROVAL_RULE_VERSION:
            raise IdentityMarketInventoryPlanError("approval rule version is invalid")
        if document.get("approval_text") != APPROVAL_TEXT:
            raise IdentityMarketInventoryPlanError("approval text differs")
        if provenance.get("commit_short") != APPROVED_SUBJECT_GIT_COMMIT[:7]:
            raise IdentityMarketInventoryPlanError("subject short commit provenance differs")
        if document.get("package_digest") != bundle.package_digest:
            raise IdentityMarketInventoryPlanError("approval package digest differs")
        if document.get("approval_scope") != bundle.logical_payload()["approval_scope"]:
            raise IdentityMarketInventoryPlanError("approval scope differs")
        if document.get("approval_id") != bundle.approval_id:
            raise IdentityMarketInventoryPlanError("approval ID does not reproduce")
        return bundle


@dataclass(frozen=True, slots=True, order=True)
class S7MarketInventorySourcePin:
    table: str
    release_id: str
    release_manifest_sha256: str
    build_id: str
    artifact_count: int
    row_count: int
    evidence_only_s4: bool

    @classmethod
    def from_source_pin(cls, pin: IdentitySourcePin) -> S7MarketInventorySourcePin:
        return cls(
            pin.table,
            pin.release_id,
            pin.release_manifest_sha256,
            pin.build_id,
            pin.artifact_count,
            pin.row_count,
            pin.evidence_only_s4,
        )

    def __post_init__(self) -> None:
        _identifier(self.table, "source table")
        _digest(self.release_id, "source release ID")
        _digest(self.release_manifest_sha256, "source manifest SHA-256")
        _digest(self.build_id, "source build ID")
        _positive_int(self.artifact_count, "source artifact count")
        _nonnegative_int(self.row_count, "source row count")
        if type(self.evidence_only_s4) is not bool:
            raise IdentityMarketInventoryPlanError("source evidence_only_s4 must be bool")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_count": self.artifact_count,
            "build_id": self.build_id,
            "evidence_only_s4": self.evidence_only_s4,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7MarketInventorySourcePin:
        d = _mapping(value, "source pin")
        _expect_keys(
            d,
            {
                "artifact_count",
                "build_id",
                "evidence_only_s4",
                "release_id",
                "release_manifest_sha256",
                "row_count",
                "table",
            },
            "source pin",
        )
        return cls(
            table=_string(d.get("table"), "source table"),
            release_id=_string(d.get("release_id"), "source release ID"),
            release_manifest_sha256=_string(
                d.get("release_manifest_sha256"), "source manifest SHA"
            ),
            build_id=_string(d.get("build_id"), "source build ID"),
            artifact_count=_positive_int(d.get("artifact_count"), "source artifact count"),
            row_count=_nonnegative_int(d.get("row_count"), "source row count"),
            evidence_only_s4=_native_bool(d.get("evidence_only_s4"), "evidence_only_s4"),
        )


EXACT_SOURCE_PINS: Final = tuple(
    sorted(S7MarketInventorySourcePin.from_source_pin(pin) for pin in S7_SOURCE_PINS.values())
)


@dataclass(frozen=True, slots=True)
class S7MarketInventoryResourceCaps:
    scanned_artifact_cap: int = DAILY_SOURCE_ARTIFACT_COUNT
    scanned_row_cap: int = DAILY_SOURCE_ROW_COUNT
    source_bytes_cap: int = DAILY_SOURCE_BYTES
    distinct_composite_cap: int = 100_000
    composite_share_class_pair_cap: int = 250_000
    output_bytes_cap: int = 256 * 1024 * 1024
    tmp_bytes_cap: int = 4 * 1024 * 1024 * 1024
    rss_bytes_cap: int = 2 * 1024 * 1024 * 1024
    batch_size: int = 65_536
    worker_count: int = 1
    wall_clock_seconds_cap: int = 14_400

    def __post_init__(self) -> None:
        expected = {
            "batch_size": 65_536,
            "composite_share_class_pair_cap": 250_000,
            "distinct_composite_cap": 100_000,
            "output_bytes_cap": 256 * 1024 * 1024,
            "rss_bytes_cap": 2 * 1024 * 1024 * 1024,
            "scanned_artifact_cap": DAILY_SOURCE_ARTIFACT_COUNT,
            "scanned_row_cap": DAILY_SOURCE_ROW_COUNT,
            "source_bytes_cap": DAILY_SOURCE_BYTES,
            "tmp_bytes_cap": 4 * 1024 * 1024 * 1024,
            "wall_clock_seconds_cap": 14_400,
            "worker_count": 1,
        }
        if self.to_dict() != expected:
            raise IdentityMarketInventoryPlanError(
                "Gate A resource caps differ from the reviewed set"
            )

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_size": self.batch_size,
            "composite_share_class_pair_cap": self.composite_share_class_pair_cap,
            "distinct_composite_cap": self.distinct_composite_cap,
            "output_bytes_cap": self.output_bytes_cap,
            "rss_bytes_cap": self.rss_bytes_cap,
            "scanned_artifact_cap": self.scanned_artifact_cap,
            "scanned_row_cap": self.scanned_row_cap,
            "source_bytes_cap": self.source_bytes_cap,
            "tmp_bytes_cap": self.tmp_bytes_cap,
            "wall_clock_seconds_cap": self.wall_clock_seconds_cap,
            "worker_count": self.worker_count,
        }

    @property
    def digest(self) -> str:
        return stable_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> S7MarketInventoryResourceCaps:
        document = _mapping(value, "market inventory resource caps")
        _expect_keys(document, set(cls().to_dict()), "market inventory resource caps")
        return cls(**{key: _positive_int(document.get(key), key) for key in cls().to_dict()})


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryPlan:
    created_by: str
    created_at_utc: datetime
    git_commit: str
    schema_approval_id: str
    schema_approval_path: str
    schema_approval_sha256: str
    schema_package_digest: str
    calendar_artifact_id: str
    calendar_artifact_sha256: str
    source_pins: tuple[S7MarketInventorySourcePin, ...] = EXACT_SOURCE_PINS
    resource_caps: S7MarketInventoryResourceCaps = S7MarketInventoryResourceCaps()
    execution_scope: str = MARKET_INVENTORY_SCOPE
    plan_state: str = MARKET_INVENTORY_PLAN_STATE

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "created_by", 200)
        object.__setattr__(
            self, "created_at_utc", _utc_datetime(self.created_at_utc, "created_at_utc")
        )
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise IdentityMarketInventoryPlanError("git_commit must be lowercase 40-hex")
        _digest(self.schema_approval_id, "schema approval ID")
        if self.schema_approval_path != schema_evidence_approval_path(self.schema_approval_id):
            raise IdentityMarketInventoryPlanError("schema approval path is not canonical")
        _digest(self.schema_approval_sha256, "schema approval SHA-256")
        _digest(self.schema_package_digest, "schema package digest")
        _digest(self.calendar_artifact_id, "calendar artifact ID")
        _digest(self.calendar_artifact_sha256, "calendar artifact SHA-256")
        if (
            self.calendar_artifact_id != INVENTORY_CALENDAR_ARTIFACT_ID
            or self.calendar_artifact_sha256 != INVENTORY_CALENDAR_ARTIFACT_SHA256
        ):
            raise IdentityMarketInventoryPlanError(
                "calendar binding differs from the exact reviewed artifact"
            )
        if tuple(self.source_pins) != EXACT_SOURCE_PINS:
            raise IdentityMarketInventoryPlanError("source pins differ from exact six releases")
        if not isinstance(self.resource_caps, S7MarketInventoryResourceCaps):
            raise IdentityMarketInventoryPlanError("resource_caps has wrong type")
        if self.execution_scope != MARKET_INVENTORY_SCOPE:
            raise IdentityMarketInventoryPlanError("inventory scope is too broad")
        if self.plan_state != MARKET_INVENTORY_PLAN_STATE:
            raise IdentityMarketInventoryPlanError("inventory plan state is invalid")

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        git_commit: str,
        approval: S7SchemaEvidenceApprovalBundle,
        approval_receipt: StoredIdentityMarketInventoryDocument,
        calendar_artifact_id: str,
        calendar_artifact_sha256: str,
    ) -> S7CompositeInventoryPlan:
        if not isinstance(approval, S7SchemaEvidenceApprovalBundle):
            raise IdentityMarketInventoryPlanError("schema/evidence approval bundle has wrong type")
        _verify_receipt(
            approval_receipt, approval.relative_path, approval.sha256, len(approval.content)
        )
        return cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            git_commit=git_commit,
            schema_approval_id=approval.approval_id,
            schema_approval_path=approval_receipt.path,
            schema_approval_sha256=approval_receipt.sha256,
            schema_package_digest=approval.package_digest,
            calendar_artifact_id=calendar_artifact_id,
            calendar_artifact_sha256=calendar_artifact_sha256,
        )

    @property
    def source_binding_digest(self) -> str:
        return stable_digest(self._source_binding(include_digest=False))

    def _calendar_binding(self) -> dict[str, object]:
        return {
            "calendar_artifact_id": self.calendar_artifact_id,
            "calendar_artifact_sha256": self.calendar_artifact_sha256,
            "calendar_name": "XNYS",
        }

    def _capabilities(self) -> dict[str, bool]:
        return {
            "adjudication_authorized": False,
            "backtest_eligibility_authorized": False,
            "canonical_override_authorized": False,
            "forced_liquidation_authorized": False,
            "full_run_authorized": False,
            "market_classification_authorized": False,
            "membership_mutation_authorized": False,
            "network_access_authorized": False,
            "publish_authorized": False,
            "registry_release_authorized": False,
            "table_materialization_authorized": False,
        }

    def _output_contract(self) -> dict[str, object]:
        return {
            "actual_distinct_count_unknown_until_execution": True,
            "artifact_type": "s4_composite_inventory_candidate",
            "contains_backtest_eligibility": False,
            "contains_canonical_identity": False,
            "contains_market_classification": False,
            "denominator_role": "valid_distinct_provider_observed_composite_figi_domain",
            "inventory_grain": "one_row_per_valid_distinct_observed_composite_figi",
            "inventory_columns": [
                "observed_composite_figi",
                "observed_share_class_figis",
                "share_class_conflict",
                "first_session",
                "last_session",
                "active_row_count",
                "inactive_row_count",
                "session_count",
                "ticker_count",
                "provider_locale_count",
                "provider_market_count",
                "primary_exchange_count",
                "parent_table_count",
                "source_release_count",
                "source_record_lineage_digest",
            ],
            "inventory_row_hard_cap": self.resource_caps.distinct_composite_cap,
            "invalid_identifier_qa_required": True,
            "parent_row_counts_are_qa_not_inventory_cardinality": True,
            "reconciliation_rows_are_not_inventory_observations": True,
            "status_after_success": "awaiting_review",
        }

    def _preview_lineage(self) -> dict[str, object]:
        return {
            "case_count": PREVIEW_CASE_COUNT,
            "case_evidence_set_digest": PREVIEW_CASE_EVIDENCE_SET_DIGEST,
            "completion_id": PREVIEW_COMPLETION_ID,
            "completion_sha256": PREVIEW_COMPLETION_SHA256,
            "preview_approval_id": PREVIEW_APPROVAL_ID,
            "preview_artifact_id": PREVIEW_ARTIFACT_ID,
            "preview_artifact_sha256": PREVIEW_ARTIFACT_SHA256,
            "preview_plan_id": PREVIEW_PLAN_ID,
            "preview_rewritten": False,
            "suspected_row_count": PREVIEW_SUSPECTED_ROW_COUNT,
        }

    def _selection(self) -> dict[str, object]:
        return {
            "caller_date_filter_allowed": False,
            "caller_ticker_filter_allowed": False,
            "end_session": INVENTORY_END_SESSION.isoformat(),
            "locale": "us",
            "market": "stocks",
            "session_count": INVENTORY_SESSION_COUNT,
            "start_session": INVENTORY_START_SESSION.isoformat(),
            "ticker_scope": "all_provider_tickers_active_and_inactive",
        }

    def _source_binding(self, *, include_digest: bool) -> dict[str, object]:
        binding: dict[str, object] = {
            "daily_physical_scan_tables": [
                "asset_observation_daily",
                "universe_source_daily",
            ],
            "daily_source_totals": {
                "artifact_count": DAILY_SOURCE_ARTIFACT_COUNT,
                "row_count": DAILY_SOURCE_ROW_COUNT,
                "stored_bytes": DAILY_SOURCE_BYTES,
            },
            "s4_release_set_id": S7_S4_RELEASE_SET_ID,
            "s4_release_set_manifest_sha256": S7_S4_RELEASE_SET_MANIFEST_SHA256,
            "inventory_authority_table": "asset_observation_daily",
            "reconciliation_only_table": "universe_source_daily",
            "six_release_binding_id": S7_SIX_RELEASE_BINDING_ID,
            "source_pins": [item.to_dict() for item in self.source_pins],
        }
        if include_digest:
            binding["source_binding_digest"] = self.source_binding_digest
        return binding

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "calendar_binding": self._calendar_binding(),
                "capabilities": self._capabilities(),
                "git_binding": {
                    "clean_checkout_required": True,
                    "git_commit": self.git_commit,
                },
                "output_contract": self._output_contract(),
                "preview_lineage": self._preview_lineage(),
                "schema_approval_binding": {
                    "approval_id": self.schema_approval_id,
                    "approval_path": self.schema_approval_path,
                    "approval_sha256": self.schema_approval_sha256,
                    "package_digest": self.schema_package_digest,
                },
                "selection": self._selection(),
                "source_binding": self._source_binding(include_digest=True),
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_composite_inventory_plan",
            "calendar_binding": self._calendar_binding(),
            "capabilities": self._capabilities(),
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_scope": self.execution_scope,
            "forbidden_outputs": [
                "adjudication_plan",
                "canonical_override",
                "composite_market_classification",
                "derived_identity_tables",
                "full_run_plan",
                "publish_plan",
                "registry_release",
            ],
            "git_binding": {"clean_checkout_required": True, "git_commit": self.git_commit},
            "input_binding_digest": self.input_binding_digest,
            "output_contract": self._output_contract(),
            "plan_rule_version": MARKET_INVENTORY_PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "preview_lineage": self._preview_lineage(),
            "resource_caps": self.resource_caps.to_dict(),
            "schema_approval_binding": {
                "approval_id": self.schema_approval_id,
                "approval_path": self.schema_approval_path,
                "approval_sha256": self.schema_approval_sha256,
                "package_digest": self.schema_package_digest,
            },
            "schema_version": MARKET_INVENTORY_PLAN_SCHEMA_VERSION,
            "selection": self._selection(),
            "source_binding": self._source_binding(include_digest=True),
        }

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "plan_id": self.plan_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return composite_inventory_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryPlan:
        d = _mapping(value, "Composite inventory plan")
        approval = _mapping(d.get("schema_approval_binding"), "schema approval binding")
        calendar = _mapping(d.get("calendar_binding"), "calendar binding")
        git = _mapping(d.get("git_binding"), "git binding")
        source = _mapping(d.get("source_binding"), "source binding")
        plan = cls(
            created_by=_string(d.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(d.get("created_at_utc"), "created_at_utc"),
            git_commit=_string(git.get("git_commit"), "git commit"),
            schema_approval_id=_string(approval.get("approval_id"), "approval ID"),
            schema_approval_path=_string(approval.get("approval_path"), "approval path"),
            schema_approval_sha256=_string(approval.get("approval_sha256"), "approval SHA"),
            schema_package_digest=_string(approval.get("package_digest"), "package digest"),
            calendar_artifact_id=_string(calendar.get("calendar_artifact_id"), "calendar ID"),
            calendar_artifact_sha256=_string(
                calendar.get("calendar_artifact_sha256"), "calendar SHA"
            ),
            source_pins=tuple(
                S7MarketInventorySourcePin.from_dict(x)
                for x in _list(source.get("source_pins"), "source pins")
            ),
            resource_caps=S7MarketInventoryResourceCaps.from_dict(d.get("resource_caps")),
            execution_scope=_string(d.get("execution_scope"), "execution scope"),
            plan_state=_string(d.get("plan_state"), "plan state"),
        )
        if d.get("artifact_type") != "s7_composite_inventory_plan":
            raise IdentityMarketInventoryPlanError("plan artifact type is invalid")
        if d.get("schema_version") != MARKET_INVENTORY_PLAN_SCHEMA_VERSION:
            raise IdentityMarketInventoryPlanError("plan schema version is invalid")
        if d.get("plan_rule_version") != MARKET_INVENTORY_PLAN_RULE_VERSION:
            raise IdentityMarketInventoryPlanError("plan rule version is invalid")
        if d.get("plan_id") != plan.plan_id or _canonical_bytes(d) != plan.content:
            raise IdentityMarketInventoryPlanError("plan does not reproduce canonical bytes")
        return plan


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryApprovalRequest:
    plan_id: str
    plan_path: str
    plan_sha256: str
    resource_caps_digest: str
    input_binding_digest: str
    created_by: str
    created_at_utc: datetime
    authorized_action: str = MARKET_INVENTORY_AUTHORIZED_ACTION
    execution_scope: str = MARKET_INVENTORY_SCOPE
    request_state: str = MARKET_INVENTORY_REQUEST_STATE

    def __post_init__(self) -> None:
        _digest(self.plan_id, "plan ID")
        if self.plan_path != composite_inventory_plan_path(self.plan_id):
            raise IdentityMarketInventoryPlanError("request plan path is not canonical")
        _digest(self.plan_sha256, "plan SHA-256")
        _digest(self.resource_caps_digest, "resource caps digest")
        _digest(self.input_binding_digest, "input binding digest")
        _safe_text(self.created_by, "request created_by", 200)
        object.__setattr__(
            self, "created_at_utc", _utc_datetime(self.created_at_utc, "request created_at_utc")
        )
        if self.authorized_action != MARKET_INVENTORY_AUTHORIZED_ACTION:
            raise IdentityMarketInventoryPlanError("request action is too broad")
        if self.execution_scope != MARKET_INVENTORY_SCOPE:
            raise IdentityMarketInventoryPlanError("request scope is too broad")
        if self.request_state != MARKET_INVENTORY_REQUEST_STATE:
            raise IdentityMarketInventoryPlanError("request state is invalid")

    @classmethod
    def create(
        cls,
        plan: S7CompositeInventoryPlan,
        plan_receipt: StoredIdentityMarketInventoryDocument,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7CompositeInventoryApprovalRequest:
        if not isinstance(plan, S7CompositeInventoryPlan):
            raise IdentityMarketInventoryPlanError(
                "Composite inventory request plan has wrong type"
            )
        _verify_receipt(plan_receipt, plan.relative_path, plan.sha256, len(plan.content))
        created = _utc_datetime(created_at_utc, "request created_at_utc")
        if created < plan.created_at_utc:
            raise IdentityMarketInventoryPlanError("request cannot predate plan")
        return cls(
            plan_id=plan.plan_id,
            plan_path=plan_receipt.path,
            plan_sha256=plan_receipt.sha256,
            resource_caps_digest=plan.resource_caps.digest,
            input_binding_digest=plan.input_binding_digest,
            created_by=created_by,
            created_at_utc=created,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_composite_inventory_approval_request",
            "authorized_action": self.authorized_action,
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_scope": self.execution_scope,
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "request_rule_version": MARKET_INVENTORY_REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "schema_version": MARKET_INVENTORY_REQUEST_SCHEMA_VERSION,
        }

    @property
    def request_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "request_event_id": self.request_event_id}
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return composite_inventory_approval_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return json.dumps(
            {
                "authorized_action": self.authorized_action,
                "input_binding_digest": self.input_binding_digest,
                "literal_version": MARKET_INVENTORY_LITERAL_VERSION,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.sha256,
                "resource_caps_digest": self.resource_caps_digest,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryApprovalRequest:
        d = _mapping(value, "Composite inventory approval request")
        request = cls(
            plan_id=_string(d.get("plan_id"), "plan ID"),
            plan_path=_string(d.get("plan_path"), "plan path"),
            plan_sha256=_string(d.get("plan_sha256"), "plan SHA"),
            resource_caps_digest=_string(d.get("resource_caps_digest"), "caps digest"),
            input_binding_digest=_string(d.get("input_binding_digest"), "input digest"),
            created_by=_string(d.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(d.get("created_at_utc"), "created_at_utc"),
            authorized_action=_string(d.get("authorized_action"), "authorized action"),
            execution_scope=_string(d.get("execution_scope"), "execution scope"),
            request_state=_string(d.get("request_state"), "request state"),
        )
        if d.get("artifact_type") != "s7_composite_inventory_approval_request":
            raise IdentityMarketInventoryPlanError("request artifact type is invalid")
        if d.get("schema_version") != MARKET_INVENTORY_REQUEST_SCHEMA_VERSION:
            raise IdentityMarketInventoryPlanError("request schema version is invalid")
        if d.get("request_rule_version") != MARKET_INVENTORY_REQUEST_RULE_VERSION:
            raise IdentityMarketInventoryPlanError("request rule version is invalid")
        if (
            d.get("request_event_id") != request.request_event_id
            or _canonical_bytes(d) != request.content
        ):
            raise IdentityMarketInventoryPlanError("request does not reproduce canonical bytes")
        return request


class IdentityMarketInventoryPlanStore:
    """Explicit ID/SHA store; latest lookup is deliberately absent."""

    def __init__(self, data_root: Path) -> None:
        expanded = data_root.expanduser()
        if expanded.is_symlink():
            raise IdentityMarketInventoryPlanError("data_root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityMarketInventoryPlanError(
                "data_root must be an existing non-symlink directory"
            )

    def store_schema_evidence_bundle(
        self, value: S7SchemaEvidenceApprovalBundle
    ) -> StoredIdentityMarketInventoryDocument:
        if not isinstance(value, S7SchemaEvidenceApprovalBundle):
            raise IdentityMarketInventoryPlanError("schema/evidence bundle has wrong type")
        return self._store(value.relative_path, value.content)

    def store_schema_approval(
        self, value: S7SchemaEvidenceApprovalBundle
    ) -> StoredIdentityMarketInventoryDocument:
        """Backward-compatible spelling for the atomic bundle store."""

        return self.store_schema_evidence_bundle(value)

    def store_plan(self, value: S7CompositeInventoryPlan) -> StoredIdentityMarketInventoryDocument:
        if not isinstance(value, S7CompositeInventoryPlan):
            raise IdentityMarketInventoryPlanError("Composite inventory plan has wrong type")
        self._verify_plan_dependencies(value)
        return self._store(value.relative_path, value.content)

    def store_approval_request(
        self, value: S7CompositeInventoryApprovalRequest
    ) -> StoredIdentityMarketInventoryDocument:
        if not isinstance(value, S7CompositeInventoryApprovalRequest):
            raise IdentityMarketInventoryPlanError("Composite inventory request has wrong type")
        plan, _ = self.load_plan(value.plan_id, expected_sha256=value.plan_sha256)
        self._verify_request_plan(value, plan)
        return self._store(value.relative_path, value.content)

    def load_schema_evidence_bundle(
        self,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7SchemaEvidenceApprovalBundle, StoredIdentityMarketInventoryDocument]:
        _digest(approval_id, "schema/evidence approval ID")
        return self._load(
            schema_evidence_approval_path(approval_id),
            expected_sha256,
            S7SchemaEvidenceApprovalBundle.from_dict,
        )

    def load_schema_approval(
        self,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7SchemaEvidenceApprovalBundle, StoredIdentityMarketInventoryDocument]:
        """Backward-compatible spelling for the atomic bundle loader."""

        return self.load_schema_evidence_bundle(
            approval_id,
            expected_sha256=expected_sha256,
        )

    def load_plan(
        self,
        plan_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7CompositeInventoryPlan, StoredIdentityMarketInventoryDocument]:
        _digest(plan_id, "Composite inventory plan ID")
        plan, stored = self._load(
            composite_inventory_plan_path(plan_id),
            expected_sha256,
            S7CompositeInventoryPlan.from_dict,
        )
        self._verify_plan_dependencies(plan)
        return plan, stored

    def load_approval_request(
        self,
        request_event_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[S7CompositeInventoryApprovalRequest, StoredIdentityMarketInventoryDocument]:
        _digest(request_event_id, "Composite inventory request event ID")
        request, stored = self._load(
            composite_inventory_approval_request_path(request_event_id),
            expected_sha256,
            S7CompositeInventoryApprovalRequest.from_dict,
        )
        plan, _ = self.load_plan(request.plan_id, expected_sha256=request.plan_sha256)
        self._verify_request_plan(request, plan)
        return request, stored

    def _verify_plan_dependencies(self, plan: S7CompositeInventoryPlan) -> None:
        approval, stored = self.load_schema_evidence_bundle(
            plan.schema_approval_id,
            expected_sha256=plan.schema_approval_sha256,
        )
        if (
            stored.path != plan.schema_approval_path
            or approval.package_digest != plan.schema_package_digest
        ):
            raise IdentityMarketInventoryPlanError("plan schema/evidence approval binding differs")
        try:
            calendar = load_xnys_calendar_artifact(
                self.root,
                calendar_artifact_id=plan.calendar_artifact_id,
                expected_sha256=plan.calendar_artifact_sha256,
            )
        except (ArtifactError, XNYSCalendarArtifactError) as exc:
            raise IdentityMarketInventoryPlanError(
                "plan calendar binding cannot be verified"
            ) from exc
        selected = tuple(
            item.session_date
            for item in calendar.sessions
            if INVENTORY_START_SESSION <= item.session_date <= INVENTORY_END_SESSION
        )
        if (
            len(selected) != INVENTORY_SESSION_COUNT
            or selected[0] != INVENTORY_START_SESSION
            or selected[-1] != INVENTORY_END_SESSION
        ):
            raise IdentityMarketInventoryPlanError(
                "calendar differs from the exact 2,513-session inventory range"
            )

    @staticmethod
    def _verify_request_plan(
        request: S7CompositeInventoryApprovalRequest,
        plan: S7CompositeInventoryPlan,
    ) -> None:
        if (
            request.plan_id != plan.plan_id
            or request.plan_path != plan.relative_path
            or request.plan_sha256 != plan.sha256
            or request.resource_caps_digest != plan.resource_caps.digest
            or request.input_binding_digest != plan.input_binding_digest
        ):
            raise IdentityMarketInventoryPlanError(
                "request does not bind the exact plan, inputs, and caps"
            )
        if request.created_at_utc < plan.created_at_utc:
            raise IdentityMarketInventoryPlanError("request cannot predate plan")

    def _store(self, relative: str, content: bytes) -> StoredIdentityMarketInventoryDocument:
        try:
            path = safe_relative_path(self.root, relative)
            receipt = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityMarketInventoryPlanError(str(exc)) from exc
        return StoredIdentityMarketInventoryDocument(
            str(receipt["path"]),
            str(receipt["sha256"]),
            int(receipt["bytes"]),
        )

    def _load(
        self,
        relative: str,
        expected_sha256: str,
        parser: Any,
    ) -> tuple[Any, StoredIdentityMarketInventoryDocument]:
        _digest(expected_sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityMarketInventoryPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink():
            raise IdentityMarketInventoryPlanError(f"control document is missing: {relative}")
        content = path.read_bytes()
        if sha256_file(path) != expected_sha256:
            raise IdentityMarketInventoryPlanError(f"control document SHA-256 differs: {relative}")
        document = _decode_json(content, relative)
        if _canonical_bytes(document) != content:
            raise IdentityMarketInventoryPlanError(
                f"control document is not canonical JSON: {relative}"
            )
        value = parser(document)
        if (
            value.relative_path != relative
            or value.sha256 != expected_sha256
            or value.content != content
        ):
            raise IdentityMarketInventoryPlanError(
                f"control document path/bytes binding differs: {relative}"
            )
        receipt = StoredIdentityMarketInventoryDocument(relative, expected_sha256, len(content))
        return value, receipt


def schema_evidence_approval_path(approval_id: str) -> str:
    _digest(approval_id, "approval ID")
    return (
        "manifests/silver/identity/schema-evidence-approval-bundles/"
        f"approval_id={approval_id}/manifest.json"
    )


def composite_inventory_plan_path(plan_id: str) -> str:
    _digest(plan_id, "plan ID")
    return f"manifests/silver/identity/composite-inventory-plans/plan_id={plan_id}/manifest.json"


def composite_inventory_approval_request_path(request_event_id: str) -> str:
    _digest(request_event_id, "request event ID")
    return (
        "manifests/silver/identity/composite-inventory-approval-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )


def _verify_receipt(
    receipt: StoredIdentityMarketInventoryDocument, path: str, sha256: str, size: int
) -> None:
    if not isinstance(receipt, StoredIdentityMarketInventoryDocument):
        raise IdentityMarketInventoryPlanError("stored document receipt has wrong type")
    if receipt.path != path or receipt.sha256 != sha256 or receipt.bytes != size:
        raise IdentityMarketInventoryPlanError("stored document receipt differs")


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _decode_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        document: dict[str, Any] = {}
        for key, value in pairs:
            if key in document:
                raise IdentityMarketInventoryPlanError(f"{label} contains duplicate JSON keys")
            document[key] = value
        return document

    try:
        value = json.loads(content, object_pairs_hook=reject_duplicates)
    except IdentityMarketInventoryPlanError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryPlanError(f"{label} is not canonical JSON") from exc
    return _mapping(value, label)


__all__ = [
    "APPROVAL_TEXT",
    "APPROVAL_TEXT_SHA256",
    "APPROVED_CONTRACT_PINS",
    "APPROVED_SUBJECT_GIT_COMMIT",
    "APPROVED_SUBJECT_GIT_TREE",
    "DAILY_SOURCE_ARTIFACT_COUNT",
    "DAILY_SOURCE_BYTES",
    "DAILY_SOURCE_ROW_COUNT",
    "EXACT_SOURCE_PINS",
    "EXPECTED_APPROVAL_TEXT_SHA256",
    "EXTERNAL_EVIDENCE_MANIFEST_ID",
    "EXTERNAL_EVIDENCE_MANIFEST_PATH",
    "EXTERNAL_EVIDENCE_MANIFEST_SHA256",
    "INVENTORY_CALENDAR_ARTIFACT_ID",
    "INVENTORY_CALENDAR_ARTIFACT_SHA256",
    "INVENTORY_END_SESSION",
    "INVENTORY_SESSION_COUNT",
    "INVENTORY_START_SESSION",
    "MARKET_INVENTORY_AUTHORIZED_ACTION",
    "MARKET_INVENTORY_LITERAL_VERSION",
    "MARKET_INVENTORY_SCOPE",
    "PREVIEW_APPROVAL_ID",
    "PREVIEW_ARTIFACT_ID",
    "PREVIEW_ARTIFACT_SHA256",
    "PREVIEW_CASE_COUNT",
    "PREVIEW_CASE_EVIDENCE_SET_DIGEST",
    "PREVIEW_COMPLETION_ID",
    "PREVIEW_COMPLETION_SHA256",
    "PREVIEW_PLAN_ID",
    "PREVIEW_SUSPECTED_ROW_COUNT",
    "IdentityMarketInventoryPlanError",
    "IdentityMarketInventoryPlanStore",
    "S7ApprovedContractPin",
    "S7CompositeInventoryApprovalRequest",
    "S7CompositeInventoryPlan",
    "S7MarketInventoryResourceCaps",
    "S7MarketInventorySourcePin",
    "S7SchemaEvidenceApprovalBundle",
    "StoredIdentityMarketInventoryDocument",
    "composite_inventory_approval_request_path",
    "composite_inventory_plan_path",
    "schema_evidence_approval_path",
]
