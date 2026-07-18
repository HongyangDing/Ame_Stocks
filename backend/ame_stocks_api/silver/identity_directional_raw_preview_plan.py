"""Non-executing controls for the exact S7 directional raw-preview scope.

This module can only freeze a review scope and a preparation plan for a future,
separately implemented and separately approved executable package.  It has no
Parquet reader, network client, approval recorder, runner, adjudicator,
materializer, or publisher.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_CAPABILITIES,
    DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
    directional_raw_preview_registry_exclusivity_semantics,
)

SCOPE_SCHEMA_VERSION: Final = 1
PLAN_SCHEMA_VERSION: Final = 1
SCOPE_RULE_VERSION: Final = "s7_directional_raw_preview_exact_scope_v1"
PLAN_RULE_VERSION: Final = "s7_directional_raw_preview_preparation_plan_v1"
PLAN_STATE: Final = "awaiting_non_execution_control_review"
AUTHORIZED_ACTION: Final = (
    "prepare_and_freeze_exact_s7_directional_raw_preview_future_executable_package_"
    "without_data_read_or_execution"
)
PREPARATION_SCOPE: Final = (
    "exact_11_pair_control_design_only_no_data_read_no_approval_no_runner_no_execution"
)

CALENDAR_ARTIFACT_ID: Final = "31cc575ae55542a580ee17e09aa242159bbcaedd0a001fd2184021a541b734bd"
CALENDAR_ARTIFACT_SHA256: Final = "3f026761a9f752d1e00c89c9f72383e7d8c0a7f7dcb2cdf8ef82e5831dfc0da7"
S4_RELEASE_SET_ID: Final = "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
S4_RELEASE_SET_MANIFEST_SHA256: Final = (
    "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
)
SIX_RELEASE_BINDING_ID: Final = "49f3d20725f2609b43d6736df78993b2975c9f1b71947af93190dc0658366c64"
S4_SOURCE_PINS: Final = (
    MappingProxyType(
        {
            "artifact_count": 2_513,
            "build_id": "9e3b5df531c01d1bcdd73cbd9cdf747bd30cdff459481b262e1ed7a23f40acc4",
            "evidence_only_s4": True,
            "release_id": "26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c",
            "release_manifest_sha256": (
                "f5fb26e75f44382caddf980e8fdf88a77903465b55bfd367f8d9029852848084"
            ),
            "row_count": 69_381_182,
            "table": "asset_observation_daily",
        }
    ),
    MappingProxyType(
        {
            "artifact_count": 2_513,
            "build_id": "21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185",
            "evidence_only_s4": True,
            "release_id": "c7e0d9a75857cbca130ba8873a737411ccb2f11d3e711ee0c0b0d9d0e2f5c614",
            "release_manifest_sha256": (
                "6b2c6ca1b612c4c38ddc8e359c1402c177a4f19b0295604d42b78bcd5804596d"
            ),
            "row_count": 69_376_329,
            "table": "universe_source_daily",
        }
    ),
)

INVENTORY_COMPLETION_ID: Final = "4472b730bbf5e77b19253c0f6bfc4b78df3135bc2f46424262fff7f735cdce15"
INVENTORY_CANDIDATE_ID: Final = "b35dc51b5798db2f8cf7783a1f2953990898bc5dde539107beabe53d85a57044"
INVENTORY_CANDIDATE_MANIFEST_SHA256: Final = (
    "11fa38df8aaa07a781e80e80d0844213bf7d859cba3826ef26c693d735697970"
)
INVENTORY_CANDIDATE_DATA_SHA256: Final = (
    "2225aacfca90676b4cb3555b37bc956955ea28b336c5ceefec74fc8ec0b02ceb"
)
INVENTORY_INPUT_BINDING_DIGEST: Final = (
    "b229ed18a000062d8c9a8f2cbec6bcd1f77d9cca5078adae3d2b8f82e4fe854a"
)
INVENTORY_CONTRACT_ID: Final = "66ac429ccc2f76bbb2a474679e83a9cf68a0f52a52c662c76905e2e4221241e8"
INVENTORY_SCHEMA_DIGEST: Final = "cc7a98521e72d88f840ec489ee290cf824d2b882ad8b479826bc7290f5e1f3e9"
INVENTORY_SOURCE_ARTIFACT_SET_DIGEST: Final = (
    "cb4a0e7cb73a59edcc74d2a8601c26d167dd1f9eed7b9821010040ddb0abcaaf"
)
INVENTORY_EXECUTION_PLAN_ID: Final = (
    "57dcfe2cd7431105e0b664163a75e76a42a023e777055bad935b548f41935eb5"
)
INVENTORY_EXECUTION_PLAN_SHA256: Final = (
    "b0d0a7987e75ed3ca366f4305d5d1260fc7b2b3b3ec6414b31ae1bcab29e4dc0"
)

SLOT_CONTRACT_CANDIDATE_PATH: Final = (
    "docs/silver/contracts/identity/identity_directional_raw_preview_slot.schema-v1.candidate.json"
)
SLOT_CONTRACT_RESOURCE_PATH: Final = (
    "backend/ame_stocks_api/silver/schema_resources/"
    "identity_directional_raw_preview_slot.schema-v1.json"
)

REQUIRED_PREPARATION_RUNTIME_PATHS: Final = frozenset(
    {
        "pyproject.toml",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_request.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_request.py",
        "docs/silver-s7-directional-raw-preview-design.md",
        SLOT_CONTRACT_CANDIDATE_PATH,
        SLOT_CONTRACT_RESOURCE_PATH,
    }
)
REQUIRED_PREPARATION_VERIFICATION_PATHS: Final = frozenset(
    {
        "tests/test_silver_identity_directional_raw_preview_contract.py",
        "tests/test_silver_identity_directional_raw_preview_plan.py",
        "tests/test_silver_identity_directional_raw_preview_request.py",
    }
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")


class IdentityDirectionalRawPreviewPlanError(RuntimeError):
    """Raised when a non-executing control artifact is not exact."""


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


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be an object")
    return dict(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityDirectionalRawPreviewPlanError(
            f"{label} schema is not exact: missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be text")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if not _GIT_OBJECT.fullmatch(text):
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be lowercase 40-hex")
    return text


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be a positive native int")
    return value


def _safe_text(value: object, label: str) -> str:
    text = _text(value, label)
    lowered = text.casefold()
    if (
        not text
        or len(text) > 200
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
        or any(token in lowered for token in ("api_key", "password", "secret", "token="))
    ):
        raise IdentityDirectionalRawPreviewPlanError(f"{label} is unsafe")
    return text


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} is not a safe relative path")
    return text


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityDirectionalRawPreviewPlanError(f"{label} must be canonical UTC")
    return normalized


@dataclass(frozen=True, slots=True, order=True)
class S7DirectionalRawPreviewControlFilePin:
    """Exact tracked file bytes included in the preparation control subject."""

    path: str
    git_blob: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "file pin path")
        _git_object(self.git_blob, "file pin Git blob")
        _digest(self.sha256, "file pin SHA-256")
        _positive_int(self.bytes, "file pin bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "git_blob": self.git_blob,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewControlFilePin:
        item = _mapping(value, "control file pin")
        _expect_keys(item, {"bytes", "git_blob", "path", "sha256"}, "control file pin")
        return cls(
            path=_text(item["path"], "file pin path"),
            git_blob=_text(item["git_blob"], "file pin Git blob"),
            sha256=_text(item["sha256"], "file pin SHA-256"),
            bytes=_positive_int(item["bytes"], "file pin bytes"),
        )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewPreparationCaps:
    """Frozen hard ceilings for the future package; no execution is authorized."""

    expected_physical_artifact_count: int = 22
    scanned_asset_row_hard_cap: int = 500_000
    scanned_universe_row_hard_cap: int = 500_000
    scanned_total_row_hard_cap: int = 1_000_000
    source_bytes_hard_cap: int = 256 * 1024 * 1024
    output_slot_row_cap: int = 11
    selected_asset_row_cap: int = 128
    selected_universe_row_cap: int = 11
    selected_total_source_row_cap: int = 139
    output_bytes_hard_cap: int = 8 * 1024 * 1024
    temporary_bytes_hard_cap: int = 64 * 1024 * 1024
    rss_bytes_hard_cap: int = 1024 * 1024 * 1024
    batch_size: int = 8_192
    worker_count: int = 1
    wall_clock_seconds_hard_cap: int = 1_800
    disk_free_floor_bytes: int = 40 * 1024 * 1024 * 1024
    disk_free_warning_bytes: int = 60 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        expected = {
            "batch_size": 8_192,
            "disk_free_floor_bytes": 40 * 1024 * 1024 * 1024,
            "disk_free_warning_bytes": 60 * 1024 * 1024 * 1024,
            "expected_physical_artifact_count": (
                DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT
            ),
            "output_bytes_hard_cap": 8 * 1024 * 1024,
            "output_slot_row_cap": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
            "rss_bytes_hard_cap": 1024 * 1024 * 1024,
            "scanned_asset_row_hard_cap": 500_000,
            "scanned_total_row_hard_cap": 1_000_000,
            "scanned_universe_row_hard_cap": 500_000,
            "selected_asset_row_cap": 128,
            "selected_total_source_row_cap": 139,
            "selected_universe_row_cap": 11,
            "source_bytes_hard_cap": 256 * 1024 * 1024,
            "temporary_bytes_hard_cap": 64 * 1024 * 1024,
            "wall_clock_seconds_hard_cap": 1_800,
            "worker_count": 1,
        }
        if self.to_dict() != expected:
            raise IdentityDirectionalRawPreviewPlanError("preparation resource caps changed")

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_size": self.batch_size,
            "disk_free_floor_bytes": self.disk_free_floor_bytes,
            "disk_free_warning_bytes": self.disk_free_warning_bytes,
            "expected_physical_artifact_count": self.expected_physical_artifact_count,
            "output_bytes_hard_cap": self.output_bytes_hard_cap,
            "output_slot_row_cap": self.output_slot_row_cap,
            "rss_bytes_hard_cap": self.rss_bytes_hard_cap,
            "scanned_asset_row_hard_cap": self.scanned_asset_row_hard_cap,
            "scanned_total_row_hard_cap": self.scanned_total_row_hard_cap,
            "scanned_universe_row_hard_cap": self.scanned_universe_row_hard_cap,
            "selected_asset_row_cap": self.selected_asset_row_cap,
            "selected_total_source_row_cap": self.selected_total_source_row_cap,
            "selected_universe_row_cap": self.selected_universe_row_cap,
            "source_bytes_hard_cap": self.source_bytes_hard_cap,
            "temporary_bytes_hard_cap": self.temporary_bytes_hard_cap,
            "wall_clock_seconds_hard_cap": self.wall_clock_seconds_hard_cap,
            "worker_count": self.worker_count,
        }

    @property
    def digest(self) -> str:
        return stable_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewPreparationCaps:
        item = _mapping(value, "preparation caps")
        expected = cls().to_dict()
        _expect_keys(item, set(expected), "preparation caps")
        if item != expected:
            raise IdentityDirectionalRawPreviewPlanError("preparation resource caps changed")
        return cls()


def _fixed_cases() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS:
        subject = {
            "inventory_anchor_composite_figi": DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS[ticker],
            "provider_id": "massive",
            "provider_locale": "us",
            "provider_market": "stocks",
            "sessions": [item.isoformat() for item in sessions],
            "ticker": ticker,
        }
        result.append({**subject, "review_case_id": stable_digest(subject)})
    return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewScopeSet:
    """The one immutable eleven-pair scope; callers cannot supply dates or tickers."""

    created_by: str
    created_at_utc: datetime

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "scope created_by")
        object.__setattr__(
            self,
            "created_at_utc",
            _utc(self.created_at_utc, "scope created_at_utc"),
        )

    @classmethod
    def create(
        cls, *, created_by: str, created_at_utc: datetime
    ) -> S7DirectionalRawPreviewScopeSet:
        return cls(created_by=created_by, created_at_utc=created_at_utc)

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_scope_set",
            "cases": _fixed_cases(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "expected_physical_artifact_count": (
                DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT
            ),
            "fixed_contract_scope_digest": DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
            "pair_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
            "rule_version": SCOPE_RULE_VERSION,
            "schema_version": SCOPE_SCHEMA_VERSION,
            "selection_rule": (
                "exact_case_sensitive_ticker_session_pairs_only_no_range_no_cartesian_product_"
                "no_whitespace_or_case_normalization"
            ),
            "unique_session_count": DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
        }

    @property
    def scope_set_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "scope_set_id": self.scope_set_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_raw_preview_scope_path(self.scope_set_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewScopeSet:
        document = _mapping(value, "directional raw-preview scope")
        scope = cls(
            created_by=_text(document.get("created_by"), "scope created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "scope created_at_utc"),
        )
        if _canonical_bytes(document) != scope.content:
            raise IdentityDirectionalRawPreviewPlanError(
                "directional raw-preview scope differs from the exact eleven pairs"
            )
        return scope


@dataclass(frozen=True, slots=True)
class StoredDirectionalRawPreviewControl:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored control path")
        _digest(self.sha256, "stored control SHA-256")
        _positive_int(self.bytes, "stored control bytes")


def _source_binding() -> dict[str, object]:
    return {
        "calendar_artifact_id": CALENDAR_ARTIFACT_ID,
        "calendar_artifact_sha256": CALENDAR_ARTIFACT_SHA256,
        "exact_daily_artifact_refs_state": "pending_manifest_only_future_executable_preflight",
        "expected_daily_artifact_count": DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT,
        "s4_release_set_id": S4_RELEASE_SET_ID,
        "s4_release_set_manifest_sha256": S4_RELEASE_SET_MANIFEST_SHA256,
        "six_release_binding_id": SIX_RELEASE_BINDING_ID,
        "source_pins": [dict(item) for item in S4_SOURCE_PINS],
    }


def _inventory_lineage() -> dict[str, object]:
    return {
        "candidate_data_sha256": INVENTORY_CANDIDATE_DATA_SHA256,
        "candidate_id": INVENTORY_CANDIDATE_ID,
        "candidate_manifest_sha256": INVENTORY_CANDIDATE_MANIFEST_SHA256,
        "completion_id": INVENTORY_COMPLETION_ID,
        "completion_manifest_sha256_state": "pending_manifest_only_future_executable_preflight",
        "input_binding_digest": INVENTORY_INPUT_BINDING_DIGEST,
        "inventory_contract_id": INVENTORY_CONTRACT_ID,
        "inventory_data_read_authorized": False,
        "inventory_reexecuted": False,
        "inventory_schema_digest": INVENTORY_SCHEMA_DIGEST,
        "inventory_v2_plan_id": INVENTORY_EXECUTION_PLAN_ID,
        "inventory_v2_plan_sha256": INVENTORY_EXECUTION_PLAN_SHA256,
        "lineage_role": "audit_origin_only_not_selection_or_execution_authority",
        "source_artifact_set_digest": INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    }


def _contract_binding() -> dict[str, object]:
    return {
        "candidate_path": SLOT_CONTRACT_CANDIDATE_PATH,
        "candidate_sha256": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
        "contract_id": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
        "qa_semantics_digest": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST,
        "resource_path": SLOT_CONTRACT_RESOURCE_PATH,
        "resource_sha256": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
        "schema_digest": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
        "table": "identity_directional_raw_preview_slot",
    }


def preparation_design_semantics() -> dict[str, object]:
    """Machine-frozen design only; it is not an executable algorithm."""

    return {
        "all_matching_asset_versions_retained": True,
        "exact_pair_filter": "session_date_and_case_sensitive_ticker_only",
        "future_output_state": "awaiting_review",
        "inventory_anchor_is_filter": False,
        "missing_membership_slot_retained": True,
        "no_interval_inference": True,
        "no_observed_value_rewrite": True,
        "physical_source_tables": ["asset_observation_daily", "universe_source_daily"],
        "registry_evaluation_state": "not_evaluated",
        "registry_exclusivity_semantics": directional_raw_preview_registry_exclusivity_semantics(),
        "registry_exclusivity_semantics_digest": (
            DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        ),
        "selected_parent_reconciliation_required": True,
        "slot_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    }


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewPreparationPlan:
    """Content-addressed, deliberately non-executable preparation plan."""

    created_by: str
    created_at_utc: datetime
    git_commit: str
    git_tree: str
    runtime_files: tuple[S7DirectionalRawPreviewControlFilePin, ...]
    verification_files: tuple[S7DirectionalRawPreviewControlFilePin, ...]
    scope_set_id: str
    scope_set_path: str
    scope_set_sha256: str
    resource_caps: S7DirectionalRawPreviewPreparationCaps = S7DirectionalRawPreviewPreparationCaps()
    authorized_action: str = field(default=AUTHORIZED_ACTION, init=False)
    preparation_scope: str = field(default=PREPARATION_SCOPE, init=False)
    plan_state: str = field(default=PLAN_STATE, init=False)

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "plan created_by")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        _git_object(self.git_commit, "plan Git commit")
        _git_object(self.git_tree, "plan Git tree")
        runtime = tuple(sorted(self.runtime_files))
        verification = tuple(sorted(self.verification_files))
        if not runtime or len({item.path for item in runtime}) != len(runtime):
            raise IdentityDirectionalRawPreviewPlanError("runtime file pins are incomplete")
        if not verification or len({item.path for item in verification}) != len(verification):
            raise IdentityDirectionalRawPreviewPlanError("verification file pins are incomplete")
        if not REQUIRED_PREPARATION_RUNTIME_PATHS.issubset({item.path for item in runtime}):
            raise IdentityDirectionalRawPreviewPlanError("runtime file set misses control inputs")
        if not REQUIRED_PREPARATION_VERIFICATION_PATHS.issubset(
            {item.path for item in verification}
        ):
            raise IdentityDirectionalRawPreviewPlanError("verification file set misses tests")
        object.__setattr__(self, "runtime_files", runtime)
        object.__setattr__(self, "verification_files", verification)
        _digest(self.scope_set_id, "scope-set ID")
        if self.scope_set_path != directional_raw_preview_scope_path(self.scope_set_id):
            raise IdentityDirectionalRawPreviewPlanError("scope-set path is not canonical")
        _digest(self.scope_set_sha256, "scope-set SHA-256")
        if not isinstance(self.resource_caps, S7DirectionalRawPreviewPreparationCaps):
            raise IdentityDirectionalRawPreviewPlanError("resource caps have wrong type")
        if (
            self.authorized_action != AUTHORIZED_ACTION
            or self.preparation_scope != PREPARATION_SCOPE
            or self.plan_state != PLAN_STATE
        ):
            raise IdentityDirectionalRawPreviewPlanError("non-executing plan boundary changed")

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        git_commit: str,
        git_tree: str,
        runtime_files: tuple[S7DirectionalRawPreviewControlFilePin, ...],
        verification_files: tuple[S7DirectionalRawPreviewControlFilePin, ...],
        scope: S7DirectionalRawPreviewScopeSet,
        stored_scope: StoredDirectionalRawPreviewControl,
    ) -> S7DirectionalRawPreviewPreparationPlan:
        if not isinstance(scope, S7DirectionalRawPreviewScopeSet):
            raise IdentityDirectionalRawPreviewPlanError("scope has wrong type")
        created = _utc(created_at_utc, "created_at_utc")
        if created < scope.created_at_utc:
            raise IdentityDirectionalRawPreviewPlanError("plan cannot predate its exact scope")
        if (
            stored_scope.path != scope.relative_path
            or stored_scope.sha256 != scope.sha256
            or stored_scope.bytes != len(scope.content)
        ):
            raise IdentityDirectionalRawPreviewPlanError("stored scope receipt differs")
        return cls(
            created_by=created_by,
            created_at_utc=created,
            git_commit=git_commit,
            git_tree=git_tree,
            runtime_files=runtime_files,
            verification_files=verification_files,
            scope_set_id=scope.scope_set_id,
            scope_set_path=stored_scope.path,
            scope_set_sha256=stored_scope.sha256,
        )

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

    @property
    def preparation_design_digest(self) -> str:
        return stable_digest(preparation_design_semantics())

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "contract_binding": _contract_binding(),
                "git_binding": self._git_binding(),
                "inventory_lineage": _inventory_lineage(),
                "preparation_design_digest": self.preparation_design_digest,
                "registry_semantics_digest": (
                    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
                ),
                "resource_caps": self.resource_caps.to_dict(),
                "scope_binding": self._scope_binding(),
                "source_binding": _source_binding(),
                "verification_binding": self._verification_binding(),
            }
        )

    def _git_binding(self) -> dict[str, object]:
        return {
            "clean_checkout_required": True,
            "git_commit": self.git_commit,
            "git_tree": self.git_tree,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "runtime_files": [item.to_dict() for item in self.runtime_files],
        }

    def _verification_binding(self) -> dict[str, object]:
        return {
            "verification_file_set_digest": self.verification_file_set_digest,
            "verification_files": [item.to_dict() for item in self.verification_files],
        }

    def _scope_binding(self) -> dict[str, object]:
        return {
            "pair_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
            "scope_set_id": self.scope_set_id,
            "scope_set_path": self.scope_set_path,
            "scope_set_sha256": self.scope_set_sha256,
            "unique_session_count": DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
        }

    def logical_payload(self) -> dict[str, object]:
        false_capabilities = {
            **dict(DIRECTIONAL_RAW_PREVIEW_CAPABILITIES),
            "approval_receipt_creation": False,
            "data_read": False,
            "network_access": False,
            "parquet_read": False,
            "runner": False,
        }
        if any(false_capabilities.values()):
            raise IdentityDirectionalRawPreviewPlanError("preparation capabilities changed")
        return {
            "artifact_type": "s7_directional_raw_preview_preparation_plan",
            "authorized_action": self.authorized_action,
            "capabilities": false_capabilities,
            "contract_binding": _contract_binding(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "future_executable_package": {
                "approval_recorder_bound": False,
                "completion_manifest_sha256_bound": False,
                "exact_daily_artifact_refs_bound": False,
                "new_exact_execution_plan_and_literal_required": True,
                "run_cli_bound": False,
                "runner_bound": False,
                "this_plan_is_executable": False,
            },
            "git_binding": self._git_binding(),
            "input_binding_digest": self.input_binding_digest,
            "inventory_lineage": _inventory_lineage(),
            "plan_rule_version": PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "preparation_design": {
                "digest": self.preparation_design_digest,
                "semantics": preparation_design_semantics(),
            },
            "preparation_scope": self.preparation_scope,
            "resource_caps": self.resource_caps.to_dict(),
            "schema_version": PLAN_SCHEMA_VERSION,
            "scope_binding": self._scope_binding(),
            "source_binding": _source_binding(),
            "verification_binding": self._verification_binding(),
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
        return directional_raw_preview_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewPreparationPlan:
        document = _mapping(value, "directional raw-preview preparation plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        scope = _mapping(document.get("scope_binding"), "scope binding")
        plan = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            git_commit=_text(git.get("git_commit"), "git_commit"),
            git_tree=_text(git.get("git_tree"), "git_tree"),
            runtime_files=tuple(
                S7DirectionalRawPreviewControlFilePin.from_dict(item)
                for item in git.get("runtime_files", [])
            ),
            verification_files=tuple(
                S7DirectionalRawPreviewControlFilePin.from_dict(item)
                for item in verification.get("verification_files", [])
            ),
            scope_set_id=_text(scope.get("scope_set_id"), "scope_set_id"),
            scope_set_path=_text(scope.get("scope_set_path"), "scope_set_path"),
            scope_set_sha256=_text(scope.get("scope_set_sha256"), "scope_set_sha256"),
            resource_caps=S7DirectionalRawPreviewPreparationCaps.from_dict(
                document.get("resource_caps")
            ),
        )
        if _canonical_bytes(document) != plan.content:
            raise IdentityDirectionalRawPreviewPlanError(
                "preparation plan does not reproduce canonical bytes"
            )
        return plan


class IdentityDirectionalRawPreviewPlanStore:
    """Immutable store for scope and preparation plan only."""

    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise IdentityDirectionalRawPreviewPlanError("control root must be a Path")
        expanded = root.expanduser()
        if expanded.is_symlink():
            raise IdentityDirectionalRawPreviewPlanError("control root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityDirectionalRawPreviewPlanError("control root must exist")

    def store_scope(
        self, scope: S7DirectionalRawPreviewScopeSet
    ) -> StoredDirectionalRawPreviewControl:
        if not isinstance(scope, S7DirectionalRawPreviewScopeSet):
            raise IdentityDirectionalRawPreviewPlanError("scope has wrong type")
        return self._write(scope.relative_path, scope.content)

    def load_scope(
        self, scope_set_id: str, *, expected_sha256: str
    ) -> tuple[S7DirectionalRawPreviewScopeSet, StoredDirectionalRawPreviewControl]:
        scope, receipt = self._read(
            directional_raw_preview_scope_path(scope_set_id),
            expected_sha256,
            S7DirectionalRawPreviewScopeSet.from_dict,
        )
        if scope.scope_set_id != scope_set_id:
            raise IdentityDirectionalRawPreviewPlanError("scope ID/path binding differs")
        return scope, receipt

    def store_plan(
        self, plan: S7DirectionalRawPreviewPreparationPlan
    ) -> StoredDirectionalRawPreviewControl:
        if not isinstance(plan, S7DirectionalRawPreviewPreparationPlan):
            raise IdentityDirectionalRawPreviewPlanError("plan has wrong type")
        self.load_scope(plan.scope_set_id, expected_sha256=plan.scope_set_sha256)
        return self._write(plan.relative_path, plan.content)

    def load_plan(
        self, plan_id: str, *, expected_sha256: str
    ) -> tuple[S7DirectionalRawPreviewPreparationPlan, StoredDirectionalRawPreviewControl]:
        plan, receipt = self._read(
            directional_raw_preview_plan_path(plan_id),
            expected_sha256,
            S7DirectionalRawPreviewPreparationPlan.from_dict,
        )
        if plan.plan_id != plan_id:
            raise IdentityDirectionalRawPreviewPlanError("plan ID/path binding differs")
        self.load_scope(plan.scope_set_id, expected_sha256=plan.scope_set_sha256)
        return plan, receipt

    def _write(self, relative: str, content: bytes) -> StoredDirectionalRawPreviewControl:
        try:
            path = safe_relative_path(self.root, relative)
            receipt = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewPlanError(str(exc)) from exc
        return StoredDirectionalRawPreviewControl(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _read(
        self, relative: str, expected_sha256: str, parser: Any
    ) -> tuple[Any, StoredDirectionalRawPreviewControl]:
        _digest(expected_sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityDirectionalRawPreviewPlanError("control artifact is missing or altered")
        content = path.read_bytes()
        try:
            document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityDirectionalRawPreviewPlanError("control artifact is not JSON") from exc
        if not isinstance(document, dict) or _canonical_bytes(document) != content:
            raise IdentityDirectionalRawPreviewPlanError("control artifact is not canonical JSON")
        value = parser(document)
        if value.relative_path != relative or value.sha256 != expected_sha256:
            raise IdentityDirectionalRawPreviewPlanError("control path or bytes differ")
        return value, StoredDirectionalRawPreviewControl(relative, expected_sha256, len(content))


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewPlanError("control JSON contains duplicate keys")
        result[key] = value
    return result


def directional_raw_preview_scope_path(scope_set_id: str) -> str:
    _digest(scope_set_id, "scope-set ID")
    return (
        "manifests/silver/identity/directional-raw-preview-control-scopes/"
        f"scope_set_id={scope_set_id}/manifest.json"
    )


def directional_raw_preview_plan_path(plan_id: str) -> str:
    _digest(plan_id, "plan ID")
    return (
        "manifests/silver/identity/directional-raw-preview-preparation-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


__all__ = [
    "AUTHORIZED_ACTION",
    "INVENTORY_CANDIDATE_DATA_SHA256",
    "INVENTORY_CANDIDATE_ID",
    "INVENTORY_CANDIDATE_MANIFEST_SHA256",
    "INVENTORY_COMPLETION_ID",
    "PREPARATION_SCOPE",
    "REQUIRED_PREPARATION_RUNTIME_PATHS",
    "REQUIRED_PREPARATION_VERIFICATION_PATHS",
    "IdentityDirectionalRawPreviewPlanError",
    "IdentityDirectionalRawPreviewPlanStore",
    "S7DirectionalRawPreviewControlFilePin",
    "S7DirectionalRawPreviewPreparationCaps",
    "S7DirectionalRawPreviewPreparationPlan",
    "S7DirectionalRawPreviewScopeSet",
    "StoredDirectionalRawPreviewControl",
    "directional_raw_preview_plan_path",
    "directional_raw_preview_scope_path",
    "preparation_design_semantics",
]
