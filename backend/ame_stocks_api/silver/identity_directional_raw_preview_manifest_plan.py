"""Fail-closed controls for the S7 manifest-only source-binding preflight.

The preparation approval recorded here authorizes only construction of this
future control package.  Neither this module nor its companion request module
opens a release manifest or Parquet file.  A later, exact human literal is
required before a manifest-only reader may inspect the two pinned S4 release
manifests, metadata for the twenty-two selected daily artifacts, or the two
inventory JSON manifests.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from importlib import import_module
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
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    INVENTORY_CANDIDATE_DATA_SHA256,
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_CONTRACT_ID,
    INVENTORY_EXECUTION_PLAN_ID,
    INVENTORY_EXECUTION_PLAN_SHA256,
    INVENTORY_INPUT_BINDING_DIGEST,
    INVENTORY_SCHEMA_DIGEST,
    INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    S4_SOURCE_PINS,
)

PREPARATION_SCOPE_SET_ID: Final = "7b23b5c9b2afd68ced0ccd3f77eb979fc86f73f8f75fa18c4463ef3f0b1bfd8b"
PREPARATION_SCOPE_SET_SHA256: Final = (
    "1a167931eee746b31c98cf8eb472dd9d306624473e5c4a5f2cacbc7bd9036d7c"
)
PREPARATION_PLAN_ID: Final = "ce9d1c8ee427271c33a87a21d0fa0dc87c17d438818eb8666d9bae2d579a30a3"
PREPARATION_PLAN_SHA256: Final = "e7fefc712ee2eef4be78545aa24ced5eefd06622fcf8351592b51e0a2d9f617a"
PREPARATION_REQUEST_EVENT_ID: Final = (
    "7dd2526734436c3cfa0dd77a786957fc7390a17e0cbcda575df7e6b31e3e2581"
)
PREPARATION_REQUEST_EVENT_SHA256: Final = (
    "094c996cb289bcdc3ae130a831a6742400811bd7499e9fb7733308ea560ec6c0"
)
PREPARATION_LITERAL_SHA256: Final = (
    "a11916a7a26f98c3a17139be57d220a9614629af29e3ffde79a1da92639a6491"
)
PREPARATION_BOUND_GIT_COMMIT: Final = "57a5d92e68dc78c9f4d434fa58712605cb6b4758"
PREPARATION_BOUND_GIT_TREE: Final = "e62955b6118aca5fb412cb10619a4ab59cb7acff"
PREPARATION_AUTHORIZED_ACTION: Final = (
    "prepare_and_freeze_exact_s7_directional_raw_preview_future_executable_package_"
    "without_data_read_or_execution"
)
PREPARATION_LITERAL_VERSION: Final = "s7_directional_raw_preview_preparation_approval_literal_v1"
PREPARATION_LITERAL: Final = json.dumps(
    {
        "authorized_action": PREPARATION_AUTHORIZED_ACTION,
        "contract_candidate_sha256": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
        "contract_id": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
        "contract_schema_digest": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
        "input_binding_digest": (
            "a475c10fc3fdb6e2eef3f75476c9f066c0ebc822ad5757f26198fdc8c65a546b"
        ),
        "literal_version": PREPARATION_LITERAL_VERSION,
        "plan_id": PREPARATION_PLAN_ID,
        "plan_sha256": PREPARATION_PLAN_SHA256,
        "preparation_design_digest": (
            "94ef0f1df34920747c556e287ab835965c41436e373b2955d036f65ca95a4479"
        ),
        "registry_semantics_digest": (
            "d2edbfe9420da8ceca4fe40b6b5a12df381fece7198763dba94658242ceb9d5d"
        ),
        "request_event_id": PREPARATION_REQUEST_EVENT_ID,
        "request_event_sha256": PREPARATION_REQUEST_EVENT_SHA256,
        "resource_caps_digest": (
            "cea553ef157aaf95eeac5a0ee8d51ca9741e038e9a3a9631568266fdd9262040"
        ),
        "runtime_file_set_digest": (
            "b5ffabed4debd1abaa5637e9378141fdb8b3cd7b51ec71391e9e9dc1a931f720"
        ),
        "scope_set_id": PREPARATION_SCOPE_SET_ID,
        "scope_set_sha256": PREPARATION_SCOPE_SET_SHA256,
        "verification_file_set_digest": (
            "51307dea310c9be1f8f770c960c206e118445810899a8d9a0013d01aefebdd45"
        ),
    },
    ensure_ascii=False,
    separators=(",", ":"),
    sort_keys=True,
)
if hashlib.sha256(PREPARATION_LITERAL.encode()).hexdigest() != PREPARATION_LITERAL_SHA256:
    raise RuntimeError("approved S7 directional preparation literal changed")

MANIFEST_PLAN_SCHEMA_VERSION: Final = 1
MANIFEST_PLAN_RULE_VERSION: Final = "s7_directional_manifest_preflight_plan_v1"
MANIFEST_PLAN_STATE: Final = "awaiting_exact_manifest_only_read_approval"
MANIFEST_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_directional_manifest_only_source_binding_preflight_once_to_awaiting_review"
)
MANIFEST_LITERAL_VERSION: Final = "s7_directional_manifest_preflight_approval_literal_v1"
MANIFEST_EXECUTION_DATA_ROOT: Final = "/mnt/HC_Volume_106309665/american_stocks"
MANIFEST_SCOPE: Final = (
    "two_pinned_s4_release_json_manifests_twenty_two_artifact_lstats_"
    "inventory_completion_and_candidate_json_only"
)

REQUIRED_MANIFEST_RUNTIME_PATHS: Final = frozenset(
    {
        "pyproject.toml",
        "backend/ame_stocks_api/__init__.py",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/cli/__init__.py",
        "backend/ame_stocks_api/silver/__init__.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_runner.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_runner.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_run.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_run.py",
        "docs/silver-s7-directional-raw-preview-design.md",
        "docs/silver/contracts/identity/identity_directional_raw_preview_slot.schema-v1.candidate.json",
        "backend/ame_stocks_api/silver/schema_resources/identity_directional_raw_preview_slot.schema-v1.json",
    }
)
REQUIRED_MANIFEST_VERIFICATION_PATHS: Final = frozenset(
    {
        "tests/test_silver_identity_directional_raw_preview_contract.py",
        "tests/test_silver_identity_directional_raw_preview_plan.py",
        "tests/test_silver_identity_directional_raw_preview_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_plan.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_approval.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_runner.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_run.py",
        "tests/test_silver_identity_directional_raw_preview_execution_plan.py",
        "tests/test_silver_identity_directional_raw_preview_approval.py",
        "tests/test_silver_identity_directional_raw_preview_runner.py",
        "tests/test_silver_identity_directional_raw_preview_run.py",
    }
)

# The manifest-only literal must bind the complete downstream executable
# package, not merely the metadata reader that produces its source binding.
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (  # noqa: E402
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
)

REQUIRED_MANIFEST_RUNTIME_PATHS = frozenset(
    set(REQUIRED_MANIFEST_RUNTIME_PATHS) | set(REQUIRED_EXECUTION_RUNTIME_PATHS)
)
REQUIRED_MANIFEST_VERIFICATION_PATHS = frozenset(
    set(REQUIRED_MANIFEST_VERIFICATION_PATHS) | set(REQUIRED_EXECUTION_VERIFICATION_PATHS)
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_APPROVAL_DIRECTORY = re.compile(r"^approval_id=([0-9a-f]{64})$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_TABLES = ("asset_observation_daily", "universe_source_daily")
_SOURCE_CONTRACTS = MappingProxyType(
    {
        ASSET_OBSERVATION_DAILY_CONTRACT.table: ASSET_OBSERVATION_DAILY_CONTRACT,
        UNIVERSE_SOURCE_DAILY_CONTRACT.table: UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
)
_FIXED_SESSIONS = tuple(
    sorted(
        {
            session
            for _, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in sessions
        }
    )
)


class IdentityDirectionalRawPreviewManifestPlanError(RuntimeError):
    """Raised when manifest-only preflight controls cross their frozen boundary."""


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
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be an object")
    return dict(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityDirectionalRawPreviewManifestPlanError(
            f"{label} schema is not exact: missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be text")
    return value


def _safe_text(value: object, label: str) -> str:
    text = _text(value, label)
    if (
        not text
        or len(text) > 200
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} is unsafe")
    return text


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if not _GIT_OBJECT.fullmatch(text):
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be lowercase 40-hex")
    return text


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityDirectionalRawPreviewManifestPlanError(
            f"{label} must be a nonnegative native int"
        )
    return value


def _positive_int(value: object, label: str) -> int:
    result = _nonnegative_int(value, label)
    if result == 0:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be positive")
    return result


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be bool")
    return value


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} is unsafe")
    return text


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must include UTC")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} is not ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} is not canonical UTC")
    return normalized


def _native_date(value: object, label: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise IdentityDirectionalRawPreviewManifestPlanError(f"{label} must be a native date")
    return value


@dataclass(frozen=True, slots=True, order=True)
class S7DirectionalRawPreviewManifestFilePin:
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
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestFilePin:
        item = _mapping(value, "manifest file pin")
        _expect_keys(item, {"bytes", "git_blob", "path", "sha256"}, "manifest file pin")
        return cls(
            path=_text(item["path"], "file pin path"),
            git_blob=_text(item["git_blob"], "file pin Git blob"),
            sha256=_text(item["sha256"], "file pin SHA-256"),
            bytes=_positive_int(item["bytes"], "file pin bytes"),
        )


@dataclass(frozen=True, slots=True)
class StoredDirectionalRawPreviewManifestControl:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored control path")
        _digest(self.sha256, "stored control SHA-256")
        _positive_int(self.bytes, "stored control bytes")


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewPreparationAuthorizationReceipt:
    """Audit receipt for package preparation; never a data-read approval."""

    recorded_by: str
    recorded_at_utc: datetime

    def __post_init__(self) -> None:
        _safe_text(self.recorded_by, "receipt recorded_by")
        object.__setattr__(self, "recorded_at_utc", _utc(self.recorded_at_utc, "recorded_at_utc"))

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_preparation_authorization_receipt",
            "authorization_boundary": {
                "data_read_authorized": False,
                "manifest_read_authorized": False,
                "network_access_authorized": False,
                "parquet_read_authorized": False,
                "preview_execution_authorized": False,
                "source_binding_creation_authorized": False,
                "source_package_preparation_authorized": True,
            },
            "approved_literal": PREPARATION_LITERAL,
            "approved_literal_sha256": PREPARATION_LITERAL_SHA256,
            "approved_literal_version": PREPARATION_LITERAL_VERSION,
            "preparation_controls": preparation_control_lineage(),
            "receipt_rule_version": "s7_directional_preparation_authorization_receipt_v1",
            "recorded_at_utc": self.recorded_at_utc.isoformat(),
            "recorded_by": self.recorded_by,
            "schema_version": 1,
            "state": "package_preparation_authorized_no_data_authority",
        }

    @property
    def authorization_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "authorization_id": self.authorization_id}
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_manifest_preparation_authorization_path(self.authorization_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewPreparationAuthorizationReceipt:
        document = _mapping(value, "preparation authorization receipt")
        result = cls(
            recorded_by=_text(document.get("recorded_by"), "recorded_by"),
            recorded_at_utc=_parse_utc(document.get("recorded_at_utc"), "recorded_at_utc"),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "preparation authorization receipt is not canonical or exact"
            )
        return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestCaps:
    release_manifest_count: int = 2
    inventory_manifest_count: int = 2
    source_artifact_metadata_count: int = 22
    parquet_lstat_count: int = 22
    parquet_content_read_bytes: int = 0
    json_manifest_bytes_hard_cap: int = 32 * 1024 * 1024
    output_json_count: int = 5
    output_bytes_hard_cap: int = 16 * 1024 * 1024
    rss_bytes_hard_cap: int = 512 * 1024 * 1024
    wall_clock_seconds_hard_cap: int = 600
    network_request_count: int = 0

    def __post_init__(self) -> None:
        expected = {
            "inventory_manifest_count": 2,
            "json_manifest_bytes_hard_cap": 32 * 1024 * 1024,
            "network_request_count": 0,
            "output_bytes_hard_cap": 16 * 1024 * 1024,
            "output_json_count": 5,
            "parquet_content_read_bytes": 0,
            "parquet_lstat_count": 22,
            "release_manifest_count": 2,
            "rss_bytes_hard_cap": 512 * 1024 * 1024,
            "source_artifact_metadata_count": 22,
            "wall_clock_seconds_hard_cap": 600,
        }
        if self.to_dict() != expected:
            raise IdentityDirectionalRawPreviewManifestPlanError("manifest-only caps changed")

    def to_dict(self) -> dict[str, int]:
        return {
            "inventory_manifest_count": self.inventory_manifest_count,
            "json_manifest_bytes_hard_cap": self.json_manifest_bytes_hard_cap,
            "network_request_count": self.network_request_count,
            "output_bytes_hard_cap": self.output_bytes_hard_cap,
            "output_json_count": self.output_json_count,
            "parquet_content_read_bytes": self.parquet_content_read_bytes,
            "parquet_lstat_count": self.parquet_lstat_count,
            "release_manifest_count": self.release_manifest_count,
            "rss_bytes_hard_cap": self.rss_bytes_hard_cap,
            "source_artifact_metadata_count": self.source_artifact_metadata_count,
            "wall_clock_seconds_hard_cap": self.wall_clock_seconds_hard_cap,
        }

    @property
    def digest(self) -> str:
        return stable_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestCaps:
        item = _mapping(value, "manifest-only caps")
        expected = cls().to_dict()
        _expect_keys(item, set(expected), "manifest-only caps")
        if item != expected:
            raise IdentityDirectionalRawPreviewManifestPlanError("manifest-only caps changed")
        return cls()


def preparation_control_lineage() -> dict[str, object]:
    return {
        "bound_git_commit": PREPARATION_BOUND_GIT_COMMIT,
        "bound_git_tree": PREPARATION_BOUND_GIT_TREE,
        "approved_literal_sha256": PREPARATION_LITERAL_SHA256,
        "plan_id": PREPARATION_PLAN_ID,
        "plan_sha256": PREPARATION_PLAN_SHA256,
        "request_event_id": PREPARATION_REQUEST_EVENT_ID,
        "request_event_sha256": PREPARATION_REQUEST_EVENT_SHA256,
        "scope_set_id": PREPARATION_SCOPE_SET_ID,
        "scope_set_sha256": PREPARATION_SCOPE_SET_SHA256,
    }


def exact_source_selection_semantics() -> dict[str, object]:
    return {
        "artifact_content_hashing": "forbidden_parquet_bytes_not_opened",
        "artifact_disk_check": "lstat_only_exists_regular_not_symlink_size_equals_manifest",
        "artifact_selection": "exact_table_and_unique_session_partition",
        "expected_artifact_count": 22,
        "fixed_pair_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
        "fixed_case_ticker_sessions": [
            {
                "sessions": [session.isoformat() for session in sessions],
                "ticker": ticker,
            }
            for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
        ],
        "fixed_scope_digest": DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
        "fixed_sessions": [item.isoformat() for item in _FIXED_SESSIONS],
        "inventory_candidate_data_read": False,
        "inventory_completion_discovery": {
            "directory": (
                "manifests/silver/identity/composite-inventory-execution-completions/"
                f"plan_id={INVENTORY_EXECUTION_PLAN_ID}"
            ),
            "rule": (
                "directory_metadata_only_require_exactly_one_non_symlink_"
                "approval_id_64hex_manifest_json"
            ),
            "selected_completion_id": INVENTORY_COMPLETION_ID,
        },
        "membership_filter": "no_ticker_or_composite_filter_during_manifest_selection",
        "release_manifest_selection": "two_exact_release_ids_no_latest_discovery",
        "session_count": DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
        "table_count": 2,
    }


MANIFEST_SELECTION_SEMANTICS_DIGEST: Final = stable_digest(exact_source_selection_semantics())


def inventory_manifest_lineage() -> dict[str, object]:
    return {
        "candidate_data_sha256": INVENTORY_CANDIDATE_DATA_SHA256,
        "candidate_id": INVENTORY_CANDIDATE_ID,
        "candidate_manifest_expected_sha256": INVENTORY_CANDIDATE_MANIFEST_SHA256,
        "candidate_manifest_path": (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
        ),
        "completion_id": INVENTORY_COMPLETION_ID,
        "completion_manifest_path_state": "bounded_discovery_pending_exact_literal",
        "contract_id": INVENTORY_CONTRACT_ID,
        "execution_plan_id": INVENTORY_EXECUTION_PLAN_ID,
        "execution_plan_sha256": INVENTORY_EXECUTION_PLAN_SHA256,
        "input_binding_digest": INVENTORY_INPUT_BINDING_DIGEST,
        "schema_digest": INVENTORY_SCHEMA_DIGEST,
        "source_artifact_set_digest": INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    }


def s4_release_lineage() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for pin in S4_SOURCE_PINS:
        contract = _SOURCE_CONTRACTS[str(pin["table"])]
        result.append(
            {
                **dict(pin),
                "contract_id": contract.contract_id,
                "manifest_path": (f"manifests/silver/releases/release_id={pin['release_id']}.json"),
                "schema_digest": contract.schema_digest,
            }
        )
    return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestPreflightPlan:
    created_by: str
    created_at_utc: datetime
    future_manifest_reader_actor: str
    future_execution_plan_actor: str
    future_execution_request_actor: str
    git_commit: str
    git_tree: str
    execution_data_root: str
    runtime_files: tuple[S7DirectionalRawPreviewManifestFilePin, ...]
    verification_files: tuple[S7DirectionalRawPreviewManifestFilePin, ...]
    preparation_authorization_id: str
    preparation_authorization_path: str
    preparation_authorization_sha256: str
    resource_caps: S7DirectionalRawPreviewManifestCaps = S7DirectionalRawPreviewManifestCaps()
    authorized_action: str = field(default=MANIFEST_AUTHORIZED_ACTION, init=False)
    plan_state: str = field(default=MANIFEST_PLAN_STATE, init=False)

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "plan created_by")
        actors = (
            self.created_by,
            _safe_text(self.future_manifest_reader_actor, "future manifest reader actor"),
            _safe_text(self.future_execution_plan_actor, "future execution plan actor"),
            _safe_text(self.future_execution_request_actor, "future execution request actor"),
        )
        if len(set(actors)) != len(actors):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest plan and future downstream actors must be distinct"
            )
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        _git_object(self.git_commit, "Git commit")
        _git_object(self.git_tree, "Git tree")
        if self.execution_data_root != MANIFEST_EXECUTION_DATA_ROOT:
            raise IdentityDirectionalRawPreviewManifestPlanError("execution data root changed")
        runtime = tuple(sorted(self.runtime_files))
        verification = tuple(sorted(self.verification_files))
        if (
            not runtime
            or len({item.path for item in runtime}) != len(runtime)
            or not REQUIRED_MANIFEST_RUNTIME_PATHS.issubset({item.path for item in runtime})
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError("runtime file pins are incomplete")
        if (
            not verification
            or len({item.path for item in verification}) != len(verification)
            or not REQUIRED_MANIFEST_VERIFICATION_PATHS.issubset(
                {item.path for item in verification}
            )
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "verification file pins are incomplete"
            )
        object.__setattr__(self, "runtime_files", runtime)
        object.__setattr__(self, "verification_files", verification)
        _digest(self.preparation_authorization_id, "preparation authorization ID")
        expected_authorization_path = directional_manifest_preparation_authorization_path(
            self.preparation_authorization_id
        )
        if self.preparation_authorization_path != expected_authorization_path:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "preparation authorization path is not canonical"
            )
        _digest(self.preparation_authorization_sha256, "preparation authorization SHA-256")
        if not isinstance(self.resource_caps, S7DirectionalRawPreviewManifestCaps):
            raise IdentityDirectionalRawPreviewManifestPlanError("manifest caps have wrong type")

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        future_manifest_reader_actor: str,
        future_execution_plan_actor: str,
        future_execution_request_actor: str,
        git_commit: str,
        git_tree: str,
        execution_data_root: str,
        runtime_files: tuple[S7DirectionalRawPreviewManifestFilePin, ...],
        verification_files: tuple[S7DirectionalRawPreviewManifestFilePin, ...],
        preparation_authorization: S7DirectionalRawPreviewPreparationAuthorizationReceipt,
        preparation_authorization_receipt: StoredDirectionalRawPreviewManifestControl,
    ) -> S7DirectionalRawPreviewManifestPreflightPlan:
        if not isinstance(
            preparation_authorization, S7DirectionalRawPreviewPreparationAuthorizationReceipt
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "preparation authorization has wrong type"
            )
        if (
            preparation_authorization_receipt.path != preparation_authorization.relative_path
            or preparation_authorization_receipt.sha256 != preparation_authorization.sha256
            or preparation_authorization_receipt.bytes != len(preparation_authorization.content)
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "preparation authorization receipt differs"
            )
        created = _utc(created_at_utc, "created_at_utc")
        if created < preparation_authorization.recorded_at_utc:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest plan predates preparation authorization"
            )
        return cls(
            created_by=created_by,
            created_at_utc=created,
            future_manifest_reader_actor=future_manifest_reader_actor,
            future_execution_plan_actor=future_execution_plan_actor,
            future_execution_request_actor=future_execution_request_actor,
            git_commit=git_commit,
            git_tree=git_tree,
            execution_data_root=execution_data_root,
            runtime_files=runtime_files,
            verification_files=verification_files,
            preparation_authorization_id=preparation_authorization.authorization_id,
            preparation_authorization_path=preparation_authorization_receipt.path,
            preparation_authorization_sha256=preparation_authorization_receipt.sha256,
        )

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "execution_data_root": self.execution_data_root,
                "future_execution_plan_actor": self.future_execution_plan_actor,
                "future_execution_request_actor": self.future_execution_request_actor,
                "future_manifest_reader_actor": self.future_manifest_reader_actor,
                "git_binding": self._git_binding(),
                "inventory_lineage": inventory_manifest_lineage(),
                "manifest_selection_semantics_digest": MANIFEST_SELECTION_SEMANTICS_DIGEST,
                "preparation_authorization": self._preparation_authorization_binding(),
                "preparation_control_lineage": preparation_control_lineage(),
                "resource_caps": self.resource_caps.to_dict(),
                "s4_release_lineage": s4_release_lineage(),
                "scope_set_id": PREPARATION_SCOPE_SET_ID,
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

    def _preparation_authorization_binding(self) -> dict[str, object]:
        return {
            "authorization_id": self.preparation_authorization_id,
            "path": self.preparation_authorization_path,
            "sha256": self.preparation_authorization_sha256,
        }

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_manifest_preflight_plan",
            "authorized_action": self.authorized_action,
            "capabilities_before_exact_literal": {
                "adjudication": False,
                "approval_receipt_creation": False,
                "full_run": False,
                "manifest_read": False,
                "network_access": False,
                "parquet_content_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
                "runner": False,
                "source_binding_creation": False,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "future_manifest_reader_actor": self.future_manifest_reader_actor,
            "future_execution_plan_actor": self.future_execution_plan_actor,
            "future_execution_request_actor": self.future_execution_request_actor,
            "future_outputs_after_exact_literal": {
                "artifact_count": 5,
                "artifacts": [
                    "manifest_preflight_run_intent",
                    "source_binding_manifest",
                    "exact_directional_preview_execution_plan",
                    "exact_directional_preview_execution_request",
                    "manifest_preflight_completion",
                ],
                "immutable_canonical_json": True,
                "state": "awaiting_review",
            },
            "git_binding": self._git_binding(),
            "input_binding_digest": self.input_binding_digest,
            "inventory_lineage": inventory_manifest_lineage(),
            "manifest_scope": MANIFEST_SCOPE,
            "plan_rule_version": MANIFEST_PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "preparation_authorization": self._preparation_authorization_binding(),
            "preparation_control_lineage": preparation_control_lineage(),
            "resource_caps": self.resource_caps.to_dict(),
            "s4_release_lineage": s4_release_lineage(),
            "schema_version": MANIFEST_PLAN_SCHEMA_VERSION,
            "selection_semantics": {
                "digest": MANIFEST_SELECTION_SEMANTICS_DIGEST,
                "semantics": exact_source_selection_semantics(),
            },
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
        return directional_manifest_preflight_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestPreflightPlan:
        document = _mapping(value, "manifest preflight plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        authorization = _mapping(
            document.get("preparation_authorization"), "preparation authorization"
        )
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            future_manifest_reader_actor=_text(
                document.get("future_manifest_reader_actor"), "future_manifest_reader_actor"
            ),
            future_execution_plan_actor=_text(
                document.get("future_execution_plan_actor"), "future_execution_plan_actor"
            ),
            future_execution_request_actor=_text(
                document.get("future_execution_request_actor"),
                "future_execution_request_actor",
            ),
            git_commit=_text(git.get("git_commit"), "git_commit"),
            git_tree=_text(git.get("git_tree"), "git_tree"),
            execution_data_root=_text(document.get("execution_data_root"), "execution_data_root"),
            runtime_files=tuple(
                S7DirectionalRawPreviewManifestFilePin.from_dict(item)
                for item in git.get("runtime_files", [])
            ),
            verification_files=tuple(
                S7DirectionalRawPreviewManifestFilePin.from_dict(item)
                for item in verification.get("verification_files", [])
            ),
            preparation_authorization_id=_text(
                authorization.get("authorization_id"), "authorization_id"
            ),
            preparation_authorization_path=_text(authorization.get("path"), "authorization path"),
            preparation_authorization_sha256=_text(
                authorization.get("sha256"), "authorization SHA-256"
            ),
            resource_caps=S7DirectionalRawPreviewManifestCaps.from_dict(
                document.get("resource_caps")
            ),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest preflight plan is not canonical or exact"
            )
        return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestRunIntent:
    """Single immutable claim written before any source or Parquet metadata read."""

    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_request_event_id: str
    manifest_request_event_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    approval_literal_sha256: str
    input_binding_digest: str
    execution_data_root: str
    source_binding_created_by: str
    source_binding_created_at_utc: datetime
    execution_plan_created_by: str
    execution_request_created_by: str

    def __post_init__(self) -> None:
        for label, value in (
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA-256", self.manifest_plan_sha256),
            ("manifest request event ID", self.manifest_request_event_id),
            ("manifest request event SHA-256", self.manifest_request_event_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA-256", self.manifest_approval_sha256),
            ("approval literal SHA-256", self.approval_literal_sha256),
            ("input binding digest", self.input_binding_digest),
        ):
            _digest(value, label)
        if self.execution_data_root != MANIFEST_EXECUTION_DATA_ROOT:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "run intent execution data root changed"
            )
        actors = (
            _safe_text(self.source_binding_created_by, "source-binding actor"),
            _safe_text(self.execution_plan_created_by, "execution-plan actor"),
            _safe_text(self.execution_request_created_by, "execution-request actor"),
        )
        if len(set(actors)) != len(actors):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "run-intent downstream actors must be distinct"
            )
        object.__setattr__(
            self,
            "source_binding_created_at_utc",
            _utc(self.source_binding_created_at_utc, "source_binding_created_at_utc"),
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal_sha256": self.approval_literal_sha256,
            "artifact_type": "s7_directional_raw_preview_manifest_preflight_run_intent",
            "capabilities": {
                "adjudication": False,
                "full_run": False,
                "network_access": False,
                "parquet_content_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
            },
            "execution_data_root": self.execution_data_root,
            "execution_plan_created_by": self.execution_plan_created_by,
            "execution_request_created_by": self.execution_request_created_by,
            "input_binding_digest": self.input_binding_digest,
            "manifest_approval_id": self.manifest_approval_id,
            "manifest_approval_sha256": self.manifest_approval_sha256,
            "manifest_plan_id": self.manifest_plan_id,
            "manifest_plan_sha256": self.manifest_plan_sha256,
            "manifest_request_event_id": self.manifest_request_event_id,
            "manifest_request_event_sha256": self.manifest_request_event_sha256,
            "schema_version": 1,
            "source_binding_created_at_utc": self.source_binding_created_at_utc.isoformat(),
            "source_binding_created_by": self.source_binding_created_by,
            "state": "claimed_before_source_read",
        }

    @property
    def intent_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "intent_id": self.intent_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_manifest_run_intent_path(
            self.manifest_plan_id, self.manifest_approval_id
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestRunIntent:
        document = _mapping(value, "manifest run intent")
        result = cls(
            manifest_plan_id=_text(document.get("manifest_plan_id"), "manifest plan ID"),
            manifest_plan_sha256=_text(
                document.get("manifest_plan_sha256"), "manifest plan SHA-256"
            ),
            manifest_request_event_id=_text(
                document.get("manifest_request_event_id"), "manifest request event ID"
            ),
            manifest_request_event_sha256=_text(
                document.get("manifest_request_event_sha256"),
                "manifest request event SHA-256",
            ),
            manifest_approval_id=_text(
                document.get("manifest_approval_id"), "manifest approval ID"
            ),
            manifest_approval_sha256=_text(
                document.get("manifest_approval_sha256"), "manifest approval SHA-256"
            ),
            approval_literal_sha256=_text(
                document.get("approval_literal_sha256"), "approval literal SHA-256"
            ),
            input_binding_digest=_text(
                document.get("input_binding_digest"), "input binding digest"
            ),
            execution_data_root=_text(
                document.get("execution_data_root"), "execution data root"
            ),
            source_binding_created_by=_text(
                document.get("source_binding_created_by"), "source-binding actor"
            ),
            source_binding_created_at_utc=_parse_utc(
                document.get("source_binding_created_at_utc"),
                "source_binding_created_at_utc",
            ),
            execution_plan_created_by=_text(
                document.get("execution_plan_created_by"), "execution-plan actor"
            ),
            execution_request_created_by=_text(
                document.get("execution_request_created_by"), "execution-request actor"
            ),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest run intent is not canonical or exact"
            )
        return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestPreflightCompletion:
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    run_intent_id: str
    run_intent_path: str
    run_intent_sha256: str
    run_intent_bytes: int
    source_binding_id: str
    source_binding_path: str
    source_binding_sha256: str
    source_binding_bytes: int
    execution_plan_id: str
    execution_plan_path: str
    execution_plan_sha256: str
    execution_plan_bytes: int
    execution_request_event_id: str
    execution_request_path: str
    execution_request_sha256: str
    execution_request_bytes: int
    completed_at_utc: datetime

    def __post_init__(self) -> None:
        for label, value in (
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA-256", self.manifest_plan_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA-256", self.manifest_approval_sha256),
            ("run intent ID", self.run_intent_id),
            ("run intent SHA-256", self.run_intent_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA-256", self.source_binding_sha256),
            ("execution plan ID", self.execution_plan_id),
            ("execution plan SHA-256", self.execution_plan_sha256),
            ("execution request event ID", self.execution_request_event_id),
            ("execution request SHA-256", self.execution_request_sha256),
        ):
            _digest(value, label)
        for label, value in (
            ("run intent path", self.run_intent_path),
            ("source binding path", self.source_binding_path),
            ("execution plan path", self.execution_plan_path),
            ("execution request path", self.execution_request_path),
        ):
            _relative_path(value, label)
        for label, value in (
            ("run intent bytes", self.run_intent_bytes),
            ("source binding bytes", self.source_binding_bytes),
            ("execution plan bytes", self.execution_plan_bytes),
            ("execution request bytes", self.execution_request_bytes),
        ):
            _positive_int(value, label)
        if self.run_intent_path != directional_manifest_run_intent_path(
            self.manifest_plan_id, self.manifest_approval_id
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "completion run-intent path is not canonical"
            )
        object.__setattr__(
            self,
            "completed_at_utc",
            _utc(self.completed_at_utc, "completed_at_utc"),
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_manifest_preflight_completion",
            "capabilities": {
                "adjudication": False,
                "full_run": False,
                "network_access": False,
                "parquet_content_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
            },
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "execution_plan": {
                "bytes": self.execution_plan_bytes,
                "path": self.execution_plan_path,
                "plan_id": self.execution_plan_id,
                "sha256": self.execution_plan_sha256,
            },
            "execution_request": {
                "bytes": self.execution_request_bytes,
                "path": self.execution_request_path,
                "request_event_id": self.execution_request_event_id,
                "sha256": self.execution_request_sha256,
            },
            "manifest_approval_id": self.manifest_approval_id,
            "manifest_approval_sha256": self.manifest_approval_sha256,
            "manifest_plan_id": self.manifest_plan_id,
            "manifest_plan_sha256": self.manifest_plan_sha256,
            "operation_counts": {
                "json_manifest_reads": 4,
                "parquet_content_read_bytes": 0,
                "parquet_lstats": 22,
            },
            "run_intent": {
                "bytes": self.run_intent_bytes,
                "intent_id": self.run_intent_id,
                "path": self.run_intent_path,
                "sha256": self.run_intent_sha256,
            },
            "schema_version": 1,
            "source_binding": {
                "bytes": self.source_binding_bytes,
                "path": self.source_binding_path,
                "sha256": self.source_binding_sha256,
                "source_binding_id": self.source_binding_id,
            },
            "state": "awaiting_review",
        }

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "completion_id": self.completion_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_manifest_preflight_completion_path(
            self.manifest_plan_id, self.manifest_approval_id
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestPreflightCompletion:
        document = _mapping(value, "manifest preflight completion")
        intent = _mapping(document.get("run_intent"), "completion run intent")
        binding = _mapping(document.get("source_binding"), "completion source binding")
        plan = _mapping(document.get("execution_plan"), "completion execution plan")
        request = _mapping(document.get("execution_request"), "completion execution request")
        result = cls(
            manifest_plan_id=_text(document.get("manifest_plan_id"), "manifest plan ID"),
            manifest_plan_sha256=_text(
                document.get("manifest_plan_sha256"), "manifest plan SHA-256"
            ),
            manifest_approval_id=_text(
                document.get("manifest_approval_id"), "manifest approval ID"
            ),
            manifest_approval_sha256=_text(
                document.get("manifest_approval_sha256"), "manifest approval SHA-256"
            ),
            run_intent_id=_text(intent.get("intent_id"), "run intent ID"),
            run_intent_path=_text(intent.get("path"), "run intent path"),
            run_intent_sha256=_text(intent.get("sha256"), "run intent SHA-256"),
            run_intent_bytes=_positive_int(intent.get("bytes"), "run intent bytes"),
            source_binding_id=_text(binding.get("source_binding_id"), "source binding ID"),
            source_binding_path=_text(binding.get("path"), "source binding path"),
            source_binding_sha256=_text(binding.get("sha256"), "source binding SHA-256"),
            source_binding_bytes=_positive_int(
                binding.get("bytes"), "source binding bytes"
            ),
            execution_plan_id=_text(plan.get("plan_id"), "execution plan ID"),
            execution_plan_path=_text(plan.get("path"), "execution plan path"),
            execution_plan_sha256=_text(plan.get("sha256"), "execution plan SHA-256"),
            execution_plan_bytes=_positive_int(plan.get("bytes"), "execution plan bytes"),
            execution_request_event_id=_text(
                request.get("request_event_id"), "execution request event ID"
            ),
            execution_request_path=_text(request.get("path"), "execution request path"),
            execution_request_sha256=_text(
                request.get("sha256"), "execution request SHA-256"
            ),
            execution_request_bytes=_positive_int(
                request.get("bytes"), "execution request bytes"
            ),
            completed_at_utc=_parse_utc(
                document.get("completed_at_utc"), "completed_at_utc"
            ),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest preflight completion is not canonical or exact"
            )
        return result


@dataclass(frozen=True, slots=True, order=True)
class S7DirectionalRawPreviewSourceArtifactRef:
    """Manifest metadata plus lstat facts; never proof that Parquet bytes were read."""

    table: str
    session_date: date
    release_id: str
    release_manifest_sha256: str
    source_contract_id: str
    source_schema_digest: str
    path: str
    sha256: str
    bytes: int
    row_count: int
    disk_size_bytes: int
    role: str = "data"
    media_type: str = "application/vnd.apache.parquet"
    disk_is_regular_file: bool = True
    disk_is_symlink: bool = False
    content_opened: bool = False

    def __post_init__(self) -> None:
        if self.table not in _TABLES:
            raise IdentityDirectionalRawPreviewManifestPlanError("source table is not exact")
        session = _native_date(self.session_date, "source session")
        if session not in _FIXED_SESSIONS:
            raise IdentityDirectionalRawPreviewManifestPlanError("source session escapes scope")
        pin = next(item for item in S4_SOURCE_PINS if item["table"] == self.table)
        contract = _SOURCE_CONTRACTS[self.table]
        if (
            self.release_id != pin["release_id"]
            or self.release_manifest_sha256 != pin["release_manifest_sha256"]
            or self.source_contract_id != contract.contract_id
            or self.source_schema_digest != contract.schema_digest
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError("source lineage differs")
        for label, value in (
            ("release ID", self.release_id),
            ("release manifest SHA-256", self.release_manifest_sha256),
            ("source contract ID", self.source_contract_id),
            ("source schema digest", self.source_schema_digest),
            ("source SHA-256", self.sha256),
        ):
            _digest(value, label)
        path = _relative_path(self.path, "source path")
        match = _SESSION_PARTITION.search(path)
        if match is None or date.fromisoformat(match.group(1)) != session:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source path does not contain its exact session"
            )
        _positive_int(self.bytes, "source bytes")
        _nonnegative_int(self.row_count, "source row count")
        if self.disk_size_bytes != self.bytes:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source lstat size differs from manifest bytes"
            )
        if (
            self.role != "data"
            or self.media_type != "application/vnd.apache.parquet"
            or self.disk_is_regular_file is not True
            or self.disk_is_symlink is not False
            or self.content_opened is not False
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source metadata crosses manifest-only boundary"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "content_opened": self.content_opened,
            "disk_is_regular_file": self.disk_is_regular_file,
            "disk_is_symlink": self.disk_is_symlink,
            "disk_size_bytes": self.disk_size_bytes,
            "media_type": self.media_type,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "role": self.role,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "source_contract_id": self.source_contract_id,
            "source_schema_digest": self.source_schema_digest,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewSourceArtifactRef:
        item = _mapping(value, "source artifact ref")
        expected = {
            "bytes",
            "content_opened",
            "disk_is_regular_file",
            "disk_is_symlink",
            "disk_size_bytes",
            "media_type",
            "path",
            "release_id",
            "release_manifest_sha256",
            "role",
            "row_count",
            "session_date",
            "sha256",
            "source_contract_id",
            "source_schema_digest",
            "table",
        }
        _expect_keys(item, expected, "source artifact ref")
        try:
            session = date.fromisoformat(_text(item["session_date"], "session_date"))
        except ValueError as exc:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source session is not ISO date"
            ) from exc
        return cls(
            table=_text(item["table"], "table"),
            session_date=session,
            release_id=_text(item["release_id"], "release_id"),
            release_manifest_sha256=_text(
                item["release_manifest_sha256"], "release_manifest_sha256"
            ),
            source_contract_id=_text(item["source_contract_id"], "source_contract_id"),
            source_schema_digest=_text(item["source_schema_digest"], "source_schema_digest"),
            path=_text(item["path"], "path"),
            sha256=_text(item["sha256"], "sha256"),
            bytes=_positive_int(item["bytes"], "bytes"),
            row_count=_nonnegative_int(item["row_count"], "row_count"),
            disk_size_bytes=_positive_int(item["disk_size_bytes"], "disk_size_bytes"),
            role=_text(item["role"], "role"),
            media_type=_text(item["media_type"], "media_type"),
            disk_is_regular_file=_native_bool(item["disk_is_regular_file"], "disk_is_regular_file"),
            disk_is_symlink=_native_bool(item["disk_is_symlink"], "disk_is_symlink"),
            content_opened=_native_bool(item["content_opened"], "content_opened"),
        )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestDocumentRef:
    kind: str
    logical_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if self.kind not in {
            "asset_release_manifest",
            "universe_release_manifest",
            "inventory_completion_manifest",
            "inventory_candidate_manifest",
        }:
            raise IdentityDirectionalRawPreviewManifestPlanError("manifest ref kind is invalid")
        _digest(self.logical_id, "manifest logical ID")
        _relative_path(self.path, "manifest path")
        _digest(self.sha256, "manifest SHA-256")
        _positive_int(self.bytes, "manifest bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "kind": self.kind,
            "logical_id": self.logical_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestDocumentRef:
        item = _mapping(value, "manifest document ref")
        _expect_keys(item, {"bytes", "kind", "logical_id", "path", "sha256"}, "manifest ref")
        return cls(
            kind=_text(item["kind"], "kind"),
            logical_id=_text(item["logical_id"], "logical_id"),
            path=_text(item["path"], "path"),
            sha256=_text(item["sha256"], "sha256"),
            bytes=_positive_int(item["bytes"], "bytes"),
        )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewSourceBinding:
    """Future manifest-only result consumed by the exact execution control plane."""

    created_by: str
    created_at_utc: datetime
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_request_event_id: str
    manifest_request_event_sha256: str
    manifest_literal_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    manifest_run_intent_id: str
    manifest_run_intent_path: str
    manifest_run_intent_sha256: str
    manifest_run_intent_execution_plan_created_by: str
    manifest_run_intent_execution_request_created_by: str
    source_artifacts: tuple[S7DirectionalRawPreviewSourceArtifactRef, ...]
    manifest_documents: tuple[S7DirectionalRawPreviewManifestDocumentRef, ...]
    resource_caps: S7DirectionalRawPreviewManifestCaps = S7DirectionalRawPreviewManifestCaps()

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "source binding created_by")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        for label, value in (
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA-256", self.manifest_plan_sha256),
            ("manifest request event ID", self.manifest_request_event_id),
            ("manifest request event SHA-256", self.manifest_request_event_sha256),
            ("manifest literal SHA-256", self.manifest_literal_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA-256", self.manifest_approval_sha256),
            ("manifest run intent ID", self.manifest_run_intent_id),
            ("manifest run intent SHA-256", self.manifest_run_intent_sha256),
        ):
            _digest(value, label)
        if self.manifest_run_intent_path != directional_manifest_run_intent_path(
            self.manifest_plan_id, self.manifest_approval_id
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest run-intent path is not canonical"
            )
        intent_actors = (
            self.created_by,
            _safe_text(
                self.manifest_run_intent_execution_plan_created_by,
                "manifest run-intent execution-plan actor",
            ),
            _safe_text(
                self.manifest_run_intent_execution_request_created_by,
                "manifest run-intent execution-request actor",
            ),
        )
        if len(set(intent_actors)) != len(intent_actors):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source-binding and run-intent actors must be distinct"
            )
        sources = tuple(sorted(self.source_artifacts))
        expected_pairs = {(table, session) for table in _TABLES for session in _FIXED_SESSIONS}
        actual_pairs = {(item.table, item.session_date) for item in sources}
        if (
            len(sources) != 22
            or actual_pairs != expected_pairs
            or len(actual_pairs) != len(sources)
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding does not contain the exact twenty-two artifacts"
            )
        object.__setattr__(self, "source_artifacts", sources)
        manifests = tuple(sorted(self.manifest_documents, key=lambda item: (item.kind, item.path)))
        if len(manifests) != 4 or len({item.kind for item in manifests}) != 4:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding must contain exactly four JSON manifest refs"
            )
        by_kind = {item.kind: item for item in manifests}
        if (
            by_kind["inventory_candidate_manifest"].logical_id != INVENTORY_CANDIDATE_ID
            or by_kind["inventory_candidate_manifest"].sha256 != INVENTORY_CANDIDATE_MANIFEST_SHA256
            or by_kind["inventory_completion_manifest"].logical_id != INVENTORY_COMPLETION_ID
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "inventory manifest refs differ from approved lineage"
            )
        expected_candidate_path = (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
        )
        completion_prefix = f"{inventory_completion_discovery_directory()}/approval_id="
        completion_path = by_kind["inventory_completion_manifest"].path
        if (
            by_kind["inventory_candidate_manifest"].path != expected_candidate_path
            or not completion_path.startswith(completion_prefix)
            or not completion_path.endswith("/manifest.json")
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "inventory manifest paths are not canonical"
            )
        approval_fragment = completion_path[
            len(inventory_completion_discovery_directory()) + 1 : -len("/manifest.json")
        ]
        parse_inventory_approval_directory(approval_fragment)
        for table, kind in (
            ("asset_observation_daily", "asset_release_manifest"),
            ("universe_source_daily", "universe_release_manifest"),
        ):
            pin = next(item for item in S4_SOURCE_PINS if item["table"] == table)
            if (
                by_kind[kind].logical_id != pin["release_id"]
                or by_kind[kind].sha256 != pin["release_manifest_sha256"]
                or by_kind[kind].path
                != f"manifests/silver/releases/release_id={pin['release_id']}.json"
            ):
                raise IdentityDirectionalRawPreviewManifestPlanError(
                    "release manifest ref differs from approved lineage"
                )
        object.__setattr__(self, "manifest_documents", manifests)
        if not isinstance(self.resource_caps, S7DirectionalRawPreviewManifestCaps):
            raise IdentityDirectionalRawPreviewManifestPlanError("source caps have wrong type")

    @property
    def source_artifact_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.source_artifacts])

    @property
    def source_manifest_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.manifest_documents])

    @property
    def inventory_completion_ref(self) -> S7DirectionalRawPreviewManifestDocumentRef:
        return next(
            item for item in self.manifest_documents if item.kind == "inventory_completion_manifest"
        )

    @property
    def inventory_candidate_ref(self) -> S7DirectionalRawPreviewManifestDocumentRef:
        return next(
            item for item in self.manifest_documents if item.kind == "inventory_candidate_manifest"
        )

    @property
    def release_manifest_refs(self) -> tuple[S7DirectionalRawPreviewManifestDocumentRef, ...]:
        return tuple(
            item for item in self.manifest_documents if item.kind.endswith("release_manifest")
        )

    @property
    def source_caps(self) -> S7DirectionalRawPreviewManifestCaps:
        return self.resource_caps

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_source_binding",
            "capabilities": {
                "adjudication": False,
                "canonical_identity_output": False,
                "full_run": False,
                "network_access": False,
                "parquet_content_read": False,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "inventory_lineage": inventory_manifest_lineage(),
            "manifest_controls": {
                "literal_sha256": self.manifest_literal_sha256,
                "approval_id": self.manifest_approval_id,
                "approval_sha256": self.manifest_approval_sha256,
                "run_intent_id": self.manifest_run_intent_id,
                "run_intent_path": self.manifest_run_intent_path,
                "run_intent_sha256": self.manifest_run_intent_sha256,
                "run_intent_execution_plan_created_by": (
                    self.manifest_run_intent_execution_plan_created_by
                ),
                "run_intent_execution_request_created_by": (
                    self.manifest_run_intent_execution_request_created_by
                ),
                "plan_id": self.manifest_plan_id,
                "plan_sha256": self.manifest_plan_sha256,
                "request_event_id": self.manifest_request_event_id,
                "request_event_sha256": self.manifest_request_event_sha256,
            },
            "manifest_documents": [item.to_dict() for item in self.manifest_documents],
            "preparation_control_lineage": preparation_control_lineage(),
            "resource_caps": self.resource_caps.to_dict(),
            "schema_version": 1,
            "selection_semantics_digest": MANIFEST_SELECTION_SEMANTICS_DIGEST,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "source_manifest_set_digest": self.source_manifest_set_digest,
            "state": "awaiting_exact_preview_execution_review",
        }

    @property
    def source_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "source_binding_id": self.source_binding_id}
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        # The intent-addressed location is discoverable after a crash without
        # scanning an unbounded content-addressed directory.  The binding ID
        # remains the digest of the complete immutable payload.
        return directional_source_binding_path(self.manifest_run_intent_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewSourceBinding:
        document = _mapping(value, "source binding")
        controls = _mapping(document.get("manifest_controls"), "manifest controls")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            manifest_plan_id=_text(controls.get("plan_id"), "manifest plan ID"),
            manifest_plan_sha256=_text(controls.get("plan_sha256"), "manifest plan SHA-256"),
            manifest_request_event_id=_text(
                controls.get("request_event_id"), "manifest request event ID"
            ),
            manifest_request_event_sha256=_text(
                controls.get("request_event_sha256"), "manifest request event SHA-256"
            ),
            manifest_literal_sha256=_text(
                controls.get("literal_sha256"), "manifest literal SHA-256"
            ),
            manifest_approval_id=_text(controls.get("approval_id"), "manifest approval ID"),
            manifest_approval_sha256=_text(
                controls.get("approval_sha256"), "manifest approval SHA-256"
            ),
            manifest_run_intent_id=_text(
                controls.get("run_intent_id"), "manifest run intent ID"
            ),
            manifest_run_intent_path=_text(
                controls.get("run_intent_path"), "manifest run intent path"
            ),
            manifest_run_intent_sha256=_text(
                controls.get("run_intent_sha256"), "manifest run intent SHA-256"
            ),
            manifest_run_intent_execution_plan_created_by=_text(
                controls.get("run_intent_execution_plan_created_by"),
                "manifest run-intent execution-plan actor",
            ),
            manifest_run_intent_execution_request_created_by=_text(
                controls.get("run_intent_execution_request_created_by"),
                "manifest run-intent execution-request actor",
            ),
            source_artifacts=tuple(
                S7DirectionalRawPreviewSourceArtifactRef.from_dict(item)
                for item in document.get("source_artifacts", [])
            ),
            manifest_documents=tuple(
                S7DirectionalRawPreviewManifestDocumentRef.from_dict(item)
                for item in document.get("manifest_documents", [])
            ),
            resource_caps=S7DirectionalRawPreviewManifestCaps.from_dict(
                document.get("resource_caps")
            ),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding is not canonical or exact"
            )
        return result


class IdentityDirectionalRawPreviewManifestStore:
    """Immutable JSON-only store.  It has no source-discovery or Parquet API."""

    def __init__(self, control_root: Path) -> None:
        if not isinstance(control_root, Path):
            raise IdentityDirectionalRawPreviewManifestPlanError("control_root must be a Path")
        expanded = control_root.expanduser()
        if expanded.is_symlink():
            raise IdentityDirectionalRawPreviewManifestPlanError("control_root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityDirectionalRawPreviewManifestPlanError("control_root must exist")

    def store_preparation_authorization(
        self, value: S7DirectionalRawPreviewPreparationAuthorizationReceipt
    ) -> StoredDirectionalRawPreviewManifestControl:
        return self._write(value.relative_path, value.content)

    def load_preparation_authorization(
        self, authorization_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewPreparationAuthorizationReceipt,
        StoredDirectionalRawPreviewManifestControl,
    ]:
        return self._read(
            directional_manifest_preparation_authorization_path(authorization_id),
            expected_sha256,
            S7DirectionalRawPreviewPreparationAuthorizationReceipt.from_dict,
        )

    def store_plan(
        self, value: S7DirectionalRawPreviewManifestPreflightPlan
    ) -> StoredDirectionalRawPreviewManifestControl:
        self.load_preparation_authorization(
            value.preparation_authorization_id,
            expected_sha256=value.preparation_authorization_sha256,
        )
        return self._write(value.relative_path, value.content)

    def load_plan(
        self, plan_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewManifestPreflightPlan,
        StoredDirectionalRawPreviewManifestControl,
    ]:
        plan, receipt = self._read(
            directional_manifest_preflight_plan_path(plan_id),
            expected_sha256,
            S7DirectionalRawPreviewManifestPreflightPlan.from_dict,
        )
        self.load_preparation_authorization(
            plan.preparation_authorization_id,
            expected_sha256=plan.preparation_authorization_sha256,
        )
        return plan, receipt

    def store_run_intent(
        self, value: S7DirectionalRawPreviewManifestRunIntent
    ) -> StoredDirectionalRawPreviewManifestControl:
        self._verify_run_intent_controls(value)
        return self._write(value.relative_path, value.content)

    def load_run_intent(
        self,
        plan_id: str,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[
        S7DirectionalRawPreviewManifestRunIntent,
        StoredDirectionalRawPreviewManifestControl,
    ]:
        intent, receipt = self._read(
            directional_manifest_run_intent_path(plan_id, approval_id),
            expected_sha256,
            S7DirectionalRawPreviewManifestRunIntent.from_dict,
        )
        if intent.manifest_plan_id != plan_id or intent.manifest_approval_id != approval_id:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest run intent crosses its canonical claim path"
            )
        self._verify_run_intent_controls(intent)
        return intent, receipt

    def _verify_run_intent_controls(
        self, intent: S7DirectionalRawPreviewManifestRunIntent
    ) -> None:
        plan, _ = self.load_plan(
            intent.manifest_plan_id,
            expected_sha256=intent.manifest_plan_sha256,
        )
        request_module = import_module(
            "ame_stocks_api.silver.identity_directional_raw_preview_manifest_request"
        )
        request, _ = request_module.load_manifest_preflight_request(
            self.root,
            intent.manifest_request_event_id,
            expected_sha256=intent.manifest_request_event_sha256,
        )
        approval_module = import_module(
            "ame_stocks_api.silver.identity_directional_raw_preview_manifest_" + "approval"
        )
        approval, _ = approval_module.DirectionalRawPreviewManifestApprovalStore(
            self.root
        ).load_approval(
            intent.manifest_approval_id,
            expected_sha256=intent.manifest_approval_sha256,
        )
        actors = {
            plan.created_by,
            request.created_by,
            approval.approved_by,
            intent.source_binding_created_by,
            intent.execution_plan_created_by,
            intent.execution_request_created_by,
        }
        if (
            len(actors) != 6
            or request.plan_id != plan.plan_id
            or approval.plan_id != plan.plan_id
            or approval.request_event_id != request.request_event_id
            or approval.approval_literal_sha256 != intent.approval_literal_sha256
            or intent.input_binding_digest != plan.input_binding_digest
            or intent.execution_data_root != plan.execution_data_root
            or intent.source_binding_created_by != plan.future_manifest_reader_actor
            or intent.execution_plan_created_by != plan.future_execution_plan_actor
            or intent.execution_request_created_by != plan.future_execution_request_actor
            or intent.source_binding_created_at_utc <= approval.approved_at_utc
            or intent.source_binding_created_at_utc > datetime.now(UTC)
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest run intent crosses controls, time, or actor gates"
            )

    def store_source_binding(
        self, value: S7DirectionalRawPreviewSourceBinding
    ) -> StoredDirectionalRawPreviewManifestControl:
        self._verify_source_binding_intent(value)
        return self._write(value.relative_path, value.content)

    def load_source_binding(
        self,
        run_intent_id: str,
        *,
        expected_source_binding_id: str,
        expected_sha256: str,
    ) -> tuple[S7DirectionalRawPreviewSourceBinding, StoredDirectionalRawPreviewManifestControl]:
        binding, receipt = self.load_source_binding_for_intent(
            run_intent_id, expected_sha256=expected_sha256
        )
        if (
            binding.source_binding_id != expected_source_binding_id
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding crosses its immutable run-intent location"
            )
        return binding, receipt

    def load_source_binding_for_intent(
        self,
        run_intent_id: str,
        *,
        expected_sha256: str | None = None,
    ) -> tuple[S7DirectionalRawPreviewSourceBinding, StoredDirectionalRawPreviewManifestControl]:
        relative = directional_source_binding_path(run_intent_id)
        if expected_sha256 is None:
            try:
                path = safe_relative_path(self.root, relative)
            except ArtifactError as exc:
                raise IdentityDirectionalRawPreviewManifestPlanError(str(exc)) from exc
            if not path.is_file() or path.is_symlink():
                raise IdentityDirectionalRawPreviewManifestPlanError(
                    "source binding is missing or unsafe"
                )
            expected_sha256 = sha256_file(path)
        binding, receipt = self._read(
            relative,
            expected_sha256,
            S7DirectionalRawPreviewSourceBinding.from_dict,
        )
        if binding.manifest_run_intent_id != run_intent_id:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding crosses its immutable run-intent location"
            )
        self._verify_source_binding_intent(binding)
        return binding, receipt

    def _verify_source_binding_intent(
        self, value: S7DirectionalRawPreviewSourceBinding
    ) -> None:
        intent, receipt = self._read(
            value.manifest_run_intent_path,
            value.manifest_run_intent_sha256,
            S7DirectionalRawPreviewManifestRunIntent.from_dict,
        )
        if (
            intent.manifest_plan_id != value.manifest_plan_id
            or intent.manifest_approval_id != value.manifest_approval_id
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding run-intent claim path differs"
            )
        if (
            intent.intent_id != value.manifest_run_intent_id
            or receipt.path != value.manifest_run_intent_path
            or intent.manifest_plan_sha256 != value.manifest_plan_sha256
            or intent.manifest_request_event_id != value.manifest_request_event_id
            or intent.manifest_request_event_sha256 != value.manifest_request_event_sha256
            or intent.manifest_approval_sha256 != value.manifest_approval_sha256
            or intent.approval_literal_sha256 != value.manifest_literal_sha256
            or intent.source_binding_created_by != value.created_by
            or intent.source_binding_created_at_utc != value.created_at_utc
            or intent.execution_plan_created_by
            != value.manifest_run_intent_execution_plan_created_by
            or intent.execution_request_created_by
            != value.manifest_run_intent_execution_request_created_by
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "source binding differs from immutable manifest run intent"
            )

    def store_preflight_completion(
        self, value: S7DirectionalRawPreviewManifestPreflightCompletion
    ) -> StoredDirectionalRawPreviewManifestControl:
        self._verify_completion_intent(value)
        return self._write(value.relative_path, value.content)

    def load_preflight_completion(
        self,
        plan_id: str,
        approval_id: str,
        *,
        expected_sha256: str,
    ) -> tuple[
        S7DirectionalRawPreviewManifestPreflightCompletion,
        StoredDirectionalRawPreviewManifestControl,
    ]:
        completion, receipt = self._read(
            directional_manifest_preflight_completion_path(plan_id, approval_id),
            expected_sha256,
            S7DirectionalRawPreviewManifestPreflightCompletion.from_dict,
        )
        if (
            completion.manifest_plan_id != plan_id
            or completion.manifest_approval_id != approval_id
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest completion crosses its canonical path"
            )
        self._verify_completion_intent(completion)
        return completion, receipt

    def _verify_completion_intent(
        self, value: S7DirectionalRawPreviewManifestPreflightCompletion
    ) -> None:
        intent, receipt = self.load_run_intent(
            value.manifest_plan_id,
            value.manifest_approval_id,
            expected_sha256=value.run_intent_sha256,
        )
        if (
            intent.intent_id != value.run_intent_id
            or receipt.path != value.run_intent_path
            or receipt.bytes != value.run_intent_bytes
            or intent.manifest_plan_sha256 != value.manifest_plan_sha256
            or intent.manifest_approval_sha256 != value.manifest_approval_sha256
            or value.completed_at_utc < intent.source_binding_created_at_utc
            or value.completed_at_utc > datetime.now(UTC)
        ):
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest completion differs from immutable run intent"
            )

    def _write(self, relative: str, content: bytes) -> StoredDirectionalRawPreviewManifestControl:
        try:
            path = safe_relative_path(self.root, relative)
            receipt = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewManifestPlanError(str(exc)) from exc
        return StoredDirectionalRawPreviewManifestControl(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _read(self, relative: str, expected_sha256: str, parser: Any) -> tuple[Any, Any]:
        _digest(expected_sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewManifestPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                f"manifest control is missing or unsafe: {relative}"
            )
        content = path.read_bytes()
        try:
            document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                f"manifest control is not JSON: {relative}"
            ) from exc
        if not isinstance(document, dict) or _canonical_bytes(document) != content:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                f"manifest control is not canonical JSON: {relative}"
            )
        value = parser(document)
        if value.relative_path != relative or value.sha256 != expected_sha256:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                f"manifest control path or bytes differ: {relative}"
            )
        return value, StoredDirectionalRawPreviewManifestControl(
            relative, expected_sha256, len(content)
        )


def directional_manifest_preparation_authorization_path(authorization_id: str) -> str:
    _digest(authorization_id, "preparation authorization ID")
    return (
        "manifests/silver/identity/directional-raw-preview-preparation-authorizations/"
        f"authorization_id={authorization_id}/manifest.json"
    )


def directional_manifest_preflight_plan_path(plan_id: str) -> str:
    _digest(plan_id, "manifest preflight plan ID")
    return (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


def directional_manifest_run_intent_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "manifest preflight plan ID")
    _digest(approval_id, "manifest preflight approval ID")
    return (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-run-intents/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def directional_manifest_preflight_completion_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "manifest preflight plan ID")
    _digest(approval_id, "manifest preflight approval ID")
    return (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def directional_source_binding_path(run_intent_id: str) -> str:
    _digest(run_intent_id, "manifest run intent ID")
    return (
        "manifests/silver/identity/directional-raw-preview-source-bindings/"
        f"run_intent_id={run_intent_id}/manifest.json"
    )


def inventory_completion_discovery_directory() -> str:
    return (
        "manifests/silver/identity/composite-inventory-execution-completions/"
        f"plan_id={INVENTORY_EXECUTION_PLAN_ID}"
    )


def parse_inventory_approval_directory(value: str) -> str:
    match = _APPROVAL_DIRECTORY.fullmatch(value)
    if match is None:
        raise IdentityDirectionalRawPreviewManifestPlanError(
            "inventory completion approval directory is invalid"
        )
    return match.group(1)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewManifestPlanError(
                "manifest control contains duplicate JSON keys"
            )
        result[key] = value
    return result


__all__ = [
    "MANIFEST_AUTHORIZED_ACTION",
    "MANIFEST_EXECUTION_DATA_ROOT",
    "MANIFEST_LITERAL_VERSION",
    "MANIFEST_SELECTION_SEMANTICS_DIGEST",
    "PREPARATION_LITERAL_SHA256",
    "REQUIRED_MANIFEST_RUNTIME_PATHS",
    "REQUIRED_MANIFEST_VERIFICATION_PATHS",
    "IdentityDirectionalRawPreviewManifestPlanError",
    "IdentityDirectionalRawPreviewManifestStore",
    "S7DirectionalRawPreviewManifestCaps",
    "S7DirectionalRawPreviewManifestDocumentRef",
    "S7DirectionalRawPreviewManifestFilePin",
    "S7DirectionalRawPreviewManifestPreflightCompletion",
    "S7DirectionalRawPreviewManifestPreflightPlan",
    "S7DirectionalRawPreviewManifestRunIntent",
    "S7DirectionalRawPreviewPreparationAuthorizationReceipt",
    "S7DirectionalRawPreviewSourceArtifactRef",
    "S7DirectionalRawPreviewSourceBinding",
    "StoredDirectionalRawPreviewManifestControl",
    "directional_manifest_preflight_completion_path",
    "directional_manifest_preflight_plan_path",
    "directional_manifest_run_intent_path",
    "directional_source_binding_path",
    "exact_source_selection_semantics",
    "inventory_completion_discovery_directory",
    "inventory_manifest_lineage",
    "preparation_control_lineage",
    "s4_release_lineage",
]
