"""Future executable controls for the exact S7 directional raw preview.

This module freezes an execution-complete Plan and Request only after a separate
manifest-only source-binding artifact has identified the exact twenty-two S4
daily Parquet artifacts.  It never discovers source files, opens Parquet,
records an approval, executes the preview, evaluates a registry, adjudicates an
identity, materializes a research table, or publishes a result.

The source refs are copied into the Plan as immutable facts.  A future runner
must consume those refs and may not accept ticker, date, range, or path
overrides from its caller.
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
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
    DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    INVENTORY_CANDIDATE_DATA_SHA256,
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    SLOT_CONTRACT_CANDIDATE_PATH,
    SLOT_CONTRACT_RESOURCE_PATH,
    S7DirectionalRawPreviewControlFilePin,
    S7DirectionalRawPreviewPreparationCaps,
)

EXECUTION_PLAN_SCHEMA_VERSION: Final = 1
EXECUTION_PLAN_RULE_VERSION: Final = "s7_directional_raw_preview_execution_plan_v1"
EXECUTION_PLAN_STATE: Final = "awaiting_exact_execution_approval"
EXECUTION_REQUEST_SCHEMA_VERSION: Final = 1
EXECUTION_REQUEST_RULE_VERSION: Final = "s7_directional_raw_preview_execution_request_v1"
EXECUTION_REQUEST_STATE: Final = "awaiting_literal_human_approval"
EXECUTION_LITERAL_VERSION: Final = "s7_directional_raw_preview_execution_approval_literal_v1"
EXECUTION_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_directional_raw_preview_once_to_awaiting_review"
)
EXECUTION_SCOPE: Final = (
    "exact_11_pair_22_artifact_directional_raw_preview_candidate_once_to_"
    "awaiting_review_no_registry_no_adjudication_no_full_no_publish"
)

PREPARATION_PLAN_ID: Final = "ce9d1c8ee427271c33a87a21d0fa0dc87c17d438818eb8666d9bae2d579a30a3"
PREPARATION_PLAN_SHA256: Final = "e7fefc712ee2eef4be78545aa24ced5eefd06622fcf8351592b51e0a2d9f617a"
PREPARATION_REQUEST_EVENT_ID: Final = (
    "7dd2526734436c3cfa0dd77a786957fc7390a17e0cbcda575df7e6b31e3e2581"
)
PREPARATION_REQUEST_EVENT_SHA256: Final = (
    "094c996cb289bcdc3ae130a831a6742400811bd7499e9fb7733308ea560ec6c0"
)
PREPARATION_APPROVAL_LITERAL_SHA256: Final = (
    "a11916a7a26f98c3a17139be57d220a9614629af29e3ffde79a1da92639a6491"
)
PREPARATION_INPUT_BINDING_DIGEST: Final = (
    "a475c10fc3fdb6e2eef3f75476c9f066c0ebc822ad5757f26198fdc8c65a546b"
)
PREPARATION_DESIGN_DIGEST: Final = (
    "94ef0f1df34920747c556e287ab835965c41436e373b2955d036f65ca95a4479"
)
PREPARATION_RESOURCE_CAPS_DIGEST: Final = (
    "cea553ef157aaf95eeac5a0ee8d51ca9741e038e9a3a9631568266fdd9262040"
)
PREPARATION_RUNTIME_FILE_SET_DIGEST: Final = (
    "b5ffabed4debd1abaa5637e9378141fdb8b3cd7b51ec71391e9e9dc1a931f720"
)
PREPARATION_VERIFICATION_FILE_SET_DIGEST: Final = (
    "51307dea310c9be1f8f770c960c206e118445810899a8d9a0013d01aefebdd45"
)
PREPARATION_SCOPE_SET_ID: Final = "7b23b5c9b2afd68ced0ccd3f77eb979fc86f73f8f75fa18c4463ef3f0b1bfd8b"
PREPARATION_SCOPE_SET_SHA256: Final = (
    "1a167931eee746b31c98cf8eb472dd9d306624473e5c4a5f2cacbc7bd9036d7c"
)

REQUIRED_EXECUTION_RUNTIME_PATHS: Final = frozenset(
    {
        "pyproject.toml",
        "backend/ame_stocks_api/__init__.py",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/cli/__init__.py",
        "backend/ame_stocks_api/silver/__init__.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        "backend/ame_stocks_api/silver/asset_full_run_plan.py",
        "backend/ame_stocks_api/silver/asset_publish_plan.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/asset_source.py",
        "backend/ame_stocks_api/silver/assets.py",
        "backend/ame_stocks_api/silver/calendar_artifact.py",
        "backend/ame_stocks_api/silver/exchange_contract.py",
        "backend/ame_stocks_api/silver/fixed_cases.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/identity_source.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
        "backend/ame_stocks_api/silver/identity_provider_evidence.py",
        "backend/ame_stocks_api/silver/identity_bounce.py",
        "backend/ame_stocks_api/silver/identity_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_streaming_preview.py",
        "backend/ame_stocks_api/silver/availability.py",
        "backend/ame_stocks_api/silver/ticker_event_contract.py",
        "backend/ame_stocks_api/silver/ticker_type_contract.py",
        "backend/ame_stocks_api/silver/ticker_overview_contract.py",
        "backend/ame_stocks_api/providers/massive.py",
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
        SLOT_CONTRACT_CANDIDATE_PATH,
        SLOT_CONTRACT_RESOURCE_PATH,
        "docs/silver-s7-directional-raw-preview-design.md",
    }
)
REQUIRED_EXECUTION_VERIFICATION_PATHS: Final = frozenset(
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
        "tests/test_silver_identity_provider_evidence.py",
        "tests/test_silver_identity_source.py",
        "tests/test_silver_identity_market_inventory_engine.py",
        "tests/test_silver_identity_bounce.py",
        "tests/test_silver_identity_preview_plan.py",
        "tests/test_silver_identity_streaming_preview.py",
        "tests/test_silver_asset_contracts.py",
        "tests/test_silver_asset_full_run_plan.py",
        "tests/test_silver_asset_publish_plan.py",
        "tests/test_silver_asset_release_set.py",
        "tests/test_silver_asset_source.py",
        "tests/test_silver_assets.py",
        "tests/test_silver_calendar_artifact.py",
        "tests/test_silver_exchange_contract.py",
        "tests/test_silver_ticker_event_contracts.py",
        "tests/test_silver_ticker_type_contract.py",
        "tests/test_silver_ticker_overview_contract.py",
        "tests/test_massive_provider.py",
        "tests/test_silver_lazy_imports.py",
    }
)

CANONICAL_EXECUTION_PATHS: Final = MappingProxyType(
    {
        "approval": (
            "manifests/silver/identity/directional-raw-preview-execution-approvals/"
            "approval_id={approval_id}/manifest.json"
        ),
        "candidate": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/manifest.json"
        ),
        "candidate_slots": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/data/review-slots.parquet"
        ),
        "case_evidence": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/evidence/review_case_id={review_case_id}/manifest.json"
        ),
        "completion": (
            "manifests/silver/identity/directional-raw-preview-execution-completions/"
            "plan_id={plan_id}/approval_id={approval_id}/manifest.json"
        ),
        "directional_review": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/review/directional-sequences.json"
        ),
        "examples": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/examples/review-anomalies.json"
        ),
        "qa": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            "candidate_id={candidate_id}/qa/qa.json"
        ),
        "staging": (
            "tmp/silver/identity/directional-raw-preview/plan_id={plan_id}/"
            "approval_id={approval_id}"
        ),
    }
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")


class IdentityDirectionalRawPreviewExecutionPlanError(RuntimeError):
    """Raised when a future exact execution control is not trustworthy."""


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
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be an object")
    return dict(value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be text")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if _DIGEST.fullmatch(text) is None:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if _GIT_OBJECT.fullmatch(text) is None:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be lowercase 40-hex")
    return text


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            f"{label} must be a positive native int"
        )
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            f"{label} must be a nonnegative native int"
        )
    return value


def _safe_text(value: object, label: str, *, maximum: int = 200) -> str:
    text = _text(value, label)
    lowered = text.casefold()
    if (
        not text
        or len(text) > maximum
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
        or any(token in lowered for token in ("api_key", "password", "secret", "token="))
    ):
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} is unsafe")
    return text


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            f"{label} is not a safe relative path"
        )
    return text


def _absolute_root(value: object) -> str:
    text = _text(value, "execution_data_root")
    path = Path(text)
    if not path.is_absolute() or str(path) != text or ".." in path.parts:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "execution_data_root is not canonical and absolute"
        )
    return text


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be canonical UTC")
    return normalized


def _date_text(value: object, label: str) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    text = _text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            f"{label} must be an ISO date"
        ) from exc
    if parsed.isoformat() != text:
        raise IdentityDirectionalRawPreviewExecutionPlanError(f"{label} must be canonical")
    return text


def _attribute(value: object, name: str) -> object:
    try:
        return getattr(value, name)
    except AttributeError as exc:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            f"source-binding artifact misses {name}"
        ) from exc


@dataclass(frozen=True, slots=True)
class StoredDirectionalRawPreviewExecutionDocument:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored document path")
        _digest(self.sha256, "stored document SHA-256")
        _positive(self.bytes, "stored document bytes")


@dataclass(frozen=True, slots=True, order=True)
class S7DirectionalRawPreviewExecutionSourcePin:
    table: str
    session_date: str
    release_id: str
    release_manifest_sha256: str
    path: str
    sha256: str
    bytes: int
    row_count: int
    source_contract_id: str
    schema_digest: str

    def __post_init__(self) -> None:
        if self.table not in DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES:
            raise IdentityDirectionalRawPreviewExecutionPlanError("source table is outside scope")
        object.__setattr__(
            self,
            "session_date",
            _date_text(self.session_date, "source session_date"),
        )
        _digest(self.release_id, "source release ID")
        _digest(self.release_manifest_sha256, "source release manifest SHA-256")
        _relative_path(self.path, "source artifact path")
        _digest(self.sha256, "source artifact SHA-256")
        _positive(self.bytes, "source artifact bytes")
        _nonnegative(self.row_count, "source artifact rows")
        _digest(self.source_contract_id, "source contract ID")
        _digest(self.schema_digest, "source schema digest")

    @classmethod
    def from_source_ref(cls, value: object) -> S7DirectionalRawPreviewExecutionSourcePin:
        return cls(
            table=_text(_attribute(value, "table"), "source table"),
            session_date=_date_text(_attribute(value, "session_date"), "source session_date"),
            release_id=_text(_attribute(value, "release_id"), "source release ID"),
            release_manifest_sha256=_text(
                _attribute(value, "release_manifest_sha256"),
                "source release manifest SHA-256",
            ),
            path=_text(_attribute(value, "path"), "source path"),
            sha256=_text(_attribute(value, "sha256"), "source SHA-256"),
            bytes=_positive(_attribute(value, "bytes"), "source bytes"),
            row_count=_nonnegative(_attribute(value, "row_count"), "source rows"),
            source_contract_id=_text(_attribute(value, "source_contract_id"), "source contract ID"),
            schema_digest=_text(_attribute(value, "source_schema_digest"), "source schema digest"),
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewExecutionSourcePin:
        item = _mapping(value, "source artifact pin")
        return cls(
            table=_text(item.get("table"), "source table"),
            session_date=_text(item.get("session_date"), "source session_date"),
            release_id=_text(item.get("release_id"), "source release ID"),
            release_manifest_sha256=_text(
                item.get("release_manifest_sha256"), "source release manifest SHA-256"
            ),
            path=_text(item.get("path"), "source path"),
            sha256=_text(item.get("sha256"), "source SHA-256"),
            bytes=_positive(item.get("bytes"), "source bytes"),
            row_count=_nonnegative(item.get("row_count"), "source rows"),
            source_contract_id=_text(item.get("source_contract_id"), "source contract ID"),
            schema_digest=_text(item.get("schema_digest"), "source schema digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": self.session_date,
            "sha256": self.sha256,
            "source_contract_id": self.source_contract_id,
            "table": self.table,
        }


def _exact_source_pairs() -> set[tuple[str, str]]:
    sessions = {
        session.isoformat()
        for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
        for session in case_sessions
    }
    return {
        (table, session)
        for table in DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES
        for session in sessions
    }


def _validate_source_pins(
    values: tuple[S7DirectionalRawPreviewExecutionSourcePin, ...],
    caps: S7DirectionalRawPreviewPreparationCaps,
) -> tuple[S7DirectionalRawPreviewExecutionSourcePin, ...]:
    pins = tuple(sorted(values))
    if len(pins) != DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT:
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "execution plan requires exactly twenty-two source artifacts"
        )
    pairs = {(item.table, item.session_date) for item in pins}
    if pairs != _exact_source_pairs() or len(pairs) != len(pins):
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "source artifacts do not reproduce the exact table/session set"
        )
    if len({item.path for item in pins}) != len(pins):
        raise IdentityDirectionalRawPreviewExecutionPlanError("source artifact paths repeat")
    asset_rows = sum(item.row_count for item in pins if item.table == "asset_observation_daily")
    universe_rows = sum(item.row_count for item in pins if item.table == "universe_source_daily")
    total_bytes = sum(item.bytes for item in pins)
    if (
        asset_rows > caps.scanned_asset_row_hard_cap
        or universe_rows > caps.scanned_universe_row_hard_cap
        or asset_rows + universe_rows > caps.scanned_total_row_hard_cap
        or total_bytes > caps.source_bytes_hard_cap
    ):
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "source-binding totals exceed the frozen execution caps"
        )
    return pins


def directional_raw_preview_algorithm_spec() -> dict[str, object]:
    return {
        "attestation_schema_version": 2,
        "candidate_serialization": {
            "compression": "zstd",
            "format": "parquet",
            "ordered_by": ["ticker", "session_date"],
        },
        "directional_sequence": {
            "exact_effective_interval_proven": False,
            "requested_sessions_only": True,
            "sampled_gaps_explicit": True,
        },
        "filter": (
            "exact_case_sensitive_ticker_and_session_pair_only_no_composite_or_share_class_filter"
        ),
        "membership": {
            "absent_slot_retained": True,
            "active_and_inactive_retained": True,
            "missing_is_not_inactive": True,
        },
        "parent_reconciliation": (
            "selected_source_record_id_exactly_one_asset_observation_parent_"
            "all_nonselected_versions_retained"
        ),
        "physical_scan": (
            "scan_every_row_of_each_manifest_bound_artifact_and_reconcile_"
            "physical_rows_bytes_sha_schema"
        ),
        "registry_evaluation": "forbidden_not_evaluated",
        "rule_version": "s7_directional_raw_preview_exact_execution_v1",
        "slot_cardinality": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
        "source_artifact_cardinality": DIRECTIONAL_RAW_PREVIEW_EXPECTED_PHYSICAL_ARTIFACT_COUNT,
        "source_artifact_discovery": "forbidden_use_plan_embedded_refs_only",
        "source_tables": list(DIRECTIONAL_RAW_PREVIEW_PHYSICAL_SOURCE_TABLES),
        "state_after_success": "awaiting_review",
    }


DIRECTIONAL_RAW_PREVIEW_ALGORITHM_DIGEST: Final = stable_digest(
    directional_raw_preview_algorithm_spec()
)


def directional_raw_preview_qa_semantics() -> tuple[dict[str, object], ...]:
    return tuple(rule.to_dict() for rule in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.qa_rules)


DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST: Final = stable_digest(
    list(directional_raw_preview_qa_semantics())
)
if (
    DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST
    != IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_QA_SEMANTICS_DIGEST
):
    raise RuntimeError("directional raw-preview QA semantics changed")


def canonical_directional_raw_preview_execution_paths() -> dict[str, str]:
    return dict(CANONICAL_EXECUTION_PATHS)


def preparation_authorization_lineage() -> dict[str, str]:
    return {
        "approved_literal_sha256": PREPARATION_APPROVAL_LITERAL_SHA256,
        "input_binding_digest": PREPARATION_INPUT_BINDING_DIGEST,
        "plan_id": PREPARATION_PLAN_ID,
        "plan_sha256": PREPARATION_PLAN_SHA256,
        "preparation_design_digest": PREPARATION_DESIGN_DIGEST,
        "request_event_id": PREPARATION_REQUEST_EVENT_ID,
        "request_event_sha256": PREPARATION_REQUEST_EVENT_SHA256,
        "resource_caps_digest": PREPARATION_RESOURCE_CAPS_DIGEST,
        "runtime_file_set_digest": PREPARATION_RUNTIME_FILE_SET_DIGEST,
        "scope_set_id": PREPARATION_SCOPE_SET_ID,
        "scope_set_sha256": PREPARATION_SCOPE_SET_SHA256,
        "verification_file_set_digest": PREPARATION_VERIFICATION_FILE_SET_DIGEST,
    }


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewExecutionPlan:
    created_by: str
    created_at_utc: datetime
    execution_git_commit: str
    execution_git_tree: str
    execution_data_root: str
    runtime_files: tuple[S7DirectionalRawPreviewControlFilePin, ...]
    verification_files: tuple[S7DirectionalRawPreviewControlFilePin, ...]
    source_binding_manifest_id: str
    source_binding_manifest_path: str
    source_binding_manifest_sha256: str
    manifest_preflight_approval_id: str
    manifest_preflight_approval_sha256: str
    manifest_preflight_intent_id: str
    manifest_preflight_intent_path: str
    manifest_preflight_intent_sha256: str
    manifest_preflight_intent_request_created_by: str
    source_binding_created_by: str
    source_binding_created_at_utc: datetime
    source_artifacts: tuple[S7DirectionalRawPreviewExecutionSourcePin, ...]
    inventory_completion_id: str
    inventory_completion_path: str
    inventory_completion_sha256: str
    inventory_candidate_id: str
    inventory_candidate_path: str
    inventory_candidate_manifest_sha256: str
    inventory_candidate_data_sha256: str
    resource_caps: S7DirectionalRawPreviewPreparationCaps = field(
        default_factory=S7DirectionalRawPreviewPreparationCaps
    )
    execution_scope: str = field(default=EXECUTION_SCOPE, init=False)
    plan_state: str = field(default=EXECUTION_PLAN_STATE, init=False)

    def __post_init__(self) -> None:
        from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
            S7DirectionalRawPreviewManifestFilePin,
        )

        _safe_text(self.created_by, "plan created_by")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "plan created_at_utc"))
        _git_object(self.execution_git_commit, "execution Git commit")
        _git_object(self.execution_git_tree, "execution Git tree")
        _absolute_root(self.execution_data_root)
        allowed_file_pin_types = (
            S7DirectionalRawPreviewControlFilePin,
            S7DirectionalRawPreviewManifestFilePin,
        )
        if any(
            not isinstance(item, allowed_file_pin_types)
            for item in (*self.runtime_files, *self.verification_files)
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution file pin has an untrusted type"
            )
        runtime = tuple(
            sorted(
                S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
                for item in self.runtime_files
            )
        )
        verification = tuple(
            sorted(
                S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
                for item in self.verification_files
            )
        )
        if len({item.path for item in runtime}) != len(runtime) or not {
            item.path for item in runtime
        }.issuperset(REQUIRED_EXECUTION_RUNTIME_PATHS):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "runtime file set misses future executable inputs"
            )
        if len({item.path for item in verification}) != len(verification) or not {
            item.path for item in verification
        }.issuperset(REQUIRED_EXECUTION_VERIFICATION_PATHS):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "verification file set misses future executable tests"
            )
        object.__setattr__(self, "runtime_files", runtime)
        object.__setattr__(self, "verification_files", verification)
        _digest(self.source_binding_manifest_id, "source-binding manifest ID")
        _relative_path(self.source_binding_manifest_path, "source-binding manifest path")
        _digest(self.source_binding_manifest_sha256, "source-binding manifest SHA-256")
        _digest(self.manifest_preflight_approval_id, "manifest preflight approval ID")
        _digest(self.manifest_preflight_approval_sha256, "manifest preflight approval SHA")
        _digest(self.manifest_preflight_intent_id, "manifest preflight intent ID")
        _relative_path(self.manifest_preflight_intent_path, "manifest preflight intent path")
        _digest(self.manifest_preflight_intent_sha256, "manifest preflight intent SHA")
        _safe_text(
            self.manifest_preflight_intent_request_created_by,
            "manifest preflight intent request actor",
        )
        _safe_text(self.source_binding_created_by, "source-binding created_by")
        object.__setattr__(
            self,
            "source_binding_created_at_utc",
            _utc(self.source_binding_created_at_utc, "source-binding created_at_utc"),
        )
        if self.created_at_utc < self.source_binding_created_at_utc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan predates source binding"
            )
        if not isinstance(self.resource_caps, S7DirectionalRawPreviewPreparationCaps):
            raise IdentityDirectionalRawPreviewExecutionPlanError("resource caps type changed")
        object.__setattr__(
            self,
            "source_artifacts",
            _validate_source_pins(tuple(self.source_artifacts), self.resource_caps),
        )
        if self.inventory_completion_id != INVENTORY_COMPLETION_ID:
            raise IdentityDirectionalRawPreviewExecutionPlanError("inventory completion ID changed")
        _relative_path(self.inventory_completion_path, "inventory completion path")
        _digest(self.inventory_completion_sha256, "inventory completion SHA-256")
        if (
            self.inventory_candidate_id != INVENTORY_CANDIDATE_ID
            or self.inventory_candidate_manifest_sha256 != INVENTORY_CANDIDATE_MANIFEST_SHA256
            or self.inventory_candidate_data_sha256 != INVENTORY_CANDIDATE_DATA_SHA256
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "inventory candidate lineage changed"
            )
        _relative_path(self.inventory_candidate_path, "inventory candidate path")
        if self.execution_scope != EXECUTION_SCOPE or self.plan_state != EXECUTION_PLAN_STATE:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan scope or state changed"
            )

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        execution_git_commit: str,
        execution_git_tree: str,
        execution_data_root: str,
        runtime_files: tuple[S7DirectionalRawPreviewControlFilePin, ...],
        verification_files: tuple[S7DirectionalRawPreviewControlFilePin, ...],
        source_binding: object,
        source_binding_receipt: StoredDirectionalRawPreviewExecutionDocument,
    ) -> S7DirectionalRawPreviewExecutionPlan:
        from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
            IdentityDirectionalRawPreviewManifestPlanError,
            IdentityDirectionalRawPreviewManifestStore,
            S7DirectionalRawPreviewSourceBinding,
            StoredDirectionalRawPreviewManifestControl,
        )

        if not isinstance(source_binding, S7DirectionalRawPreviewSourceBinding) or not isinstance(
            source_binding_receipt, StoredDirectionalRawPreviewManifestControl
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan requires the exact manifest source-binding type and receipt"
            )
        if (
            source_binding.source_binding_id != stable_digest(source_binding.logical_payload())
            or source_binding.sha256 != hashlib.sha256(source_binding.content).hexdigest()
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "source-binding canonical identity does not reproduce"
            )
        try:
            manifest_store = IdentityDirectionalRawPreviewManifestStore(
                Path(_absolute_root(execution_data_root))
            )
            loaded_binding, loaded_receipt = manifest_store.load_source_binding(
                source_binding.manifest_run_intent_id,
                expected_source_binding_id=source_binding.source_binding_id,
                expected_sha256=source_binding.sha256,
            )
            manifest_plan, _ = manifest_store.load_plan(
                source_binding.manifest_plan_id,
                expected_sha256=source_binding.manifest_plan_sha256,
            )
        except IdentityDirectionalRawPreviewManifestPlanError as exc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan requires a Store-loaded immutable source binding"
            ) from exc
        if loaded_binding != source_binding or loaded_receipt != source_binding_receipt:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "Store-loaded source binding or receipt differs"
            )
        _verify_manifest_plan_execution_projection(
            manifest_plan,
            execution_git_commit=execution_git_commit,
            execution_git_tree=execution_git_tree,
            execution_data_root=execution_data_root,
            runtime_files=runtime_files,
            verification_files=verification_files,
        )
        if (
            created_by != source_binding.manifest_run_intent_execution_plan_created_by
            or _utc(created_at_utc, "plan created_at_utc")
            != source_binding.created_at_utc
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan differs from immutable manifest run intent"
            )
        source_binding_id = _text(
            _attribute(source_binding, "source_binding_id"), "source-binding ID"
        )
        source_binding_path = _text(
            _attribute(source_binding, "relative_path"), "source-binding path"
        )
        source_binding_sha = _text(_attribute(source_binding, "sha256"), "source-binding SHA-256")
        source_binding_content = _attribute(source_binding, "content")
        if not isinstance(source_binding_content, bytes):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "source-binding content must be canonical bytes"
            )
        _verify_receipt(
            source_binding_receipt,
            source_binding_path,
            source_binding_sha,
            len(source_binding_content),
        )
        _verify_source_binding_preparation_lineage(source_binding)
        manifests = {item.kind: item for item in source_binding.manifest_documents}
        if set(manifests) != {
            "asset_release_manifest",
            "universe_release_manifest",
            "inventory_completion_manifest",
            "inventory_candidate_manifest",
        }:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "source-binding manifest document set is not exact"
            )
        completion = manifests["inventory_completion_manifest"]
        candidate = manifests["inventory_candidate_manifest"]
        pins = tuple(
            S7DirectionalRawPreviewExecutionSourcePin.from_source_ref(item)
            for item in _attribute(source_binding, "source_artifacts")
        )
        return cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            execution_git_commit=execution_git_commit,
            execution_git_tree=execution_git_tree,
            execution_data_root=execution_data_root,
            runtime_files=runtime_files,
            verification_files=verification_files,
            source_binding_manifest_id=source_binding_id,
            source_binding_manifest_path=source_binding_path,
            source_binding_manifest_sha256=source_binding_receipt.sha256,
            manifest_preflight_approval_id=_text(
                _attribute(source_binding, "manifest_approval_id"),
                "manifest preflight approval ID",
            ),
            manifest_preflight_approval_sha256=_text(
                _attribute(source_binding, "manifest_approval_sha256"),
                "manifest preflight approval SHA",
            ),
            manifest_preflight_intent_id=_text(
                _attribute(source_binding, "manifest_run_intent_id"),
                "manifest preflight intent ID",
            ),
            manifest_preflight_intent_path=_text(
                _attribute(source_binding, "manifest_run_intent_path"),
                "manifest preflight intent path",
            ),
            manifest_preflight_intent_sha256=_text(
                _attribute(source_binding, "manifest_run_intent_sha256"),
                "manifest preflight intent SHA",
            ),
            manifest_preflight_intent_request_created_by=_text(
                _attribute(
                    source_binding,
                    "manifest_run_intent_execution_request_created_by",
                ),
                "manifest preflight intent request actor",
            ),
            source_binding_created_by=_text(
                _attribute(source_binding, "created_by"), "source-binding created_by"
            ),
            source_binding_created_at_utc=_attribute(source_binding, "created_at_utc"),
            source_artifacts=pins,
            inventory_completion_id=_text(
                _attribute(completion, "logical_id"), "inventory completion ID"
            ),
            inventory_completion_path=_text(
                _attribute(completion, "path"), "inventory completion path"
            ),
            inventory_completion_sha256=_text(
                _attribute(completion, "sha256"), "inventory completion SHA-256"
            ),
            inventory_candidate_id=_text(
                _attribute(candidate, "logical_id"), "inventory candidate ID"
            ),
            inventory_candidate_path=_text(
                _attribute(candidate, "path"), "inventory candidate path"
            ),
            inventory_candidate_manifest_sha256=_text(
                _attribute(candidate, "sha256"), "inventory candidate SHA-256"
            ),
            inventory_candidate_data_sha256=INVENTORY_CANDIDATE_DATA_SHA256,
        )

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

    @property
    def source_artifact_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.source_artifacts])

    @property
    def algorithm_digest(self) -> str:
        return DIRECTIONAL_RAW_PREVIEW_ALGORITHM_DIGEST

    @property
    def qa_semantics_digest(self) -> str:
        return DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST

    @property
    def scope_set_id(self) -> str:
        return PREPARATION_SCOPE_SET_ID

    @property
    def scope_set_sha256(self) -> str:
        return PREPARATION_SCOPE_SET_SHA256

    @property
    def contract_id(self) -> str:
        return IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID

    @property
    def contract_schema_digest(self) -> str:
        return IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST

    @property
    def contract_candidate_sha256(self) -> str:
        return IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256

    def _git_binding(self) -> dict[str, object]:
        return {
            "clean_checkout_required": True,
            "execution_git_commit": self.execution_git_commit,
            "execution_git_tree": self.execution_git_tree,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "runtime_files": [item.to_dict() for item in self.runtime_files],
        }

    def _verification_binding(self) -> dict[str, object]:
        return {
            "verification_file_set_digest": self.verification_file_set_digest,
            "verification_files": [item.to_dict() for item in self.verification_files],
        }

    def _source_binding(self) -> dict[str, object]:
        return {
            "artifact_count": len(self.source_artifacts),
            "manifest_id": self.source_binding_manifest_id,
            "manifest_path": self.source_binding_manifest_path,
            "manifest_sha256": self.source_binding_manifest_sha256,
            "manifest_preflight_approval_id": self.manifest_preflight_approval_id,
            "manifest_preflight_approval_sha256": self.manifest_preflight_approval_sha256,
            "manifest_preflight_intent_id": self.manifest_preflight_intent_id,
            "manifest_preflight_intent_path": self.manifest_preflight_intent_path,
            "manifest_preflight_intent_sha256": self.manifest_preflight_intent_sha256,
            "manifest_preflight_intent_request_created_by": (
                self.manifest_preflight_intent_request_created_by
            ),
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "total_bytes": sum(item.bytes for item in self.source_artifacts),
            "total_rows": sum(item.row_count for item in self.source_artifacts),
        }

    def _inventory_binding(self) -> dict[str, str]:
        return {
            "candidate_data_sha256": self.inventory_candidate_data_sha256,
            "candidate_id": self.inventory_candidate_id,
            "candidate_manifest_path": self.inventory_candidate_path,
            "candidate_manifest_sha256": self.inventory_candidate_manifest_sha256,
            "completion_id": self.inventory_completion_id,
            "completion_manifest_path": self.inventory_completion_path,
            "completion_manifest_sha256": self.inventory_completion_sha256,
        }

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "algorithm_digest": self.algorithm_digest,
                "canonical_paths": canonical_directional_raw_preview_execution_paths(),
                "contract": {
                    "candidate_sha256": self.contract_candidate_sha256,
                    "contract_id": self.contract_id,
                    "schema_digest": self.contract_schema_digest,
                },
                "execution_data_root": self.execution_data_root,
                "git_binding": self._git_binding(),
                "inventory_binding": self._inventory_binding(),
                "preparation_authorization": preparation_authorization_lineage(),
                "qa_semantics_digest": self.qa_semantics_digest,
                "registry_semantics_digest": (
                    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
                ),
                "resource_caps": self.resource_caps.to_dict(),
                "scope_binding": {
                    "pair_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
                    "scope_set_id": self.scope_set_id,
                    "scope_set_sha256": self.scope_set_sha256,
                    "unique_session_count": DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
                },
                "source_binding": self._source_binding(),
                "verification_binding": self._verification_binding(),
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm": {
                "digest": self.algorithm_digest,
                "semantics": directional_raw_preview_algorithm_spec(),
            },
            "artifact_type": "s7_directional_raw_preview_execution_plan",
            "canonical_paths": canonical_directional_raw_preview_execution_paths(),
            "capabilities": {
                "approval_receipt_creation_authorized_by_plan": False,
                "data_read_authorized_by_plan": False,
                "exact_group_history_read_authorized": False,
                "external_evidence_capture_authorized": False,
                "full_run_authorized": False,
                "network_access_authorized": False,
                "preview_execution_authorized_by_plan": False,
                "publication_authorized": False,
                "registry_evaluation_authorized": False,
                "research_identity_materialization_authorized": False,
            },
            "contract_binding": {
                "candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "schema_digest": self.contract_schema_digest,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "git_binding": self._git_binding(),
            "input_binding_digest": self.input_binding_digest,
            "inventory_binding": self._inventory_binding(),
            "output_contract": {
                "contains_canonical_identity": False,
                "contains_registry_decision": False,
                "exact_slot_rows": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
                "slot_contract_id": self.contract_id,
                "slot_schema_digest": self.contract_schema_digest,
                "state_after_success": "awaiting_review",
            },
            "plan_rule_version": EXECUTION_PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "preparation_authorization": preparation_authorization_lineage(),
            "qa": {
                "semantics": list(directional_raw_preview_qa_semantics()),
                "semantics_digest": self.qa_semantics_digest,
            },
            "registry_semantics_digest": (
                DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
            ),
            "resource_caps": self.resource_caps.to_dict(),
            "schema_version": EXECUTION_PLAN_SCHEMA_VERSION,
            "scope_binding": {
                "pair_count": DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
                "unique_session_count": DIRECTIONAL_RAW_PREVIEW_FIXED_SESSION_COUNT,
            },
            "source_binding": self._source_binding(),
            "source_binding_created_at_utc": self.source_binding_created_at_utc.isoformat(),
            "source_binding_created_by": self.source_binding_created_by,
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
        return directional_raw_preview_execution_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewExecutionPlan:
        document = _mapping(value, "directional raw-preview execution plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        source = _mapping(document.get("source_binding"), "source binding")
        inventory = _mapping(document.get("inventory_binding"), "inventory binding")
        plan = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            execution_git_commit=_text(git.get("execution_git_commit"), "execution Git commit"),
            execution_git_tree=_text(git.get("execution_git_tree"), "execution Git tree"),
            execution_data_root=_text(document.get("execution_data_root"), "execution root"),
            runtime_files=tuple(
                S7DirectionalRawPreviewControlFilePin.from_dict(item)
                for item in git.get("runtime_files", [])
            ),
            verification_files=tuple(
                S7DirectionalRawPreviewControlFilePin.from_dict(item)
                for item in verification.get("verification_files", [])
            ),
            source_binding_manifest_id=_text(source.get("manifest_id"), "source manifest ID"),
            source_binding_manifest_path=_text(source.get("manifest_path"), "source manifest path"),
            source_binding_manifest_sha256=_text(
                source.get("manifest_sha256"), "source manifest SHA"
            ),
            manifest_preflight_approval_id=_text(
                source.get("manifest_preflight_approval_id"),
                "manifest preflight approval ID",
            ),
            manifest_preflight_approval_sha256=_text(
                source.get("manifest_preflight_approval_sha256"),
                "manifest preflight approval SHA",
            ),
            manifest_preflight_intent_id=_text(
                source.get("manifest_preflight_intent_id"),
                "manifest preflight intent ID",
            ),
            manifest_preflight_intent_path=_text(
                source.get("manifest_preflight_intent_path"),
                "manifest preflight intent path",
            ),
            manifest_preflight_intent_sha256=_text(
                source.get("manifest_preflight_intent_sha256"),
                "manifest preflight intent SHA",
            ),
            manifest_preflight_intent_request_created_by=_text(
                source.get("manifest_preflight_intent_request_created_by"),
                "manifest preflight intent request actor",
            ),
            source_binding_created_by=_text(
                document.get("source_binding_created_by"), "source created_by"
            ),
            source_binding_created_at_utc=_parse_utc(
                document.get("source_binding_created_at_utc"), "source created_at"
            ),
            source_artifacts=tuple(
                S7DirectionalRawPreviewExecutionSourcePin.from_dict(item)
                for item in source.get("source_artifacts", [])
            ),
            inventory_completion_id=_text(
                inventory.get("completion_id"), "inventory completion ID"
            ),
            inventory_completion_path=_text(
                inventory.get("completion_manifest_path"), "inventory completion path"
            ),
            inventory_completion_sha256=_text(
                inventory.get("completion_manifest_sha256"), "inventory completion SHA"
            ),
            inventory_candidate_id=_text(inventory.get("candidate_id"), "inventory candidate ID"),
            inventory_candidate_path=_text(
                inventory.get("candidate_manifest_path"), "inventory candidate path"
            ),
            inventory_candidate_manifest_sha256=_text(
                inventory.get("candidate_manifest_sha256"), "inventory candidate SHA"
            ),
            inventory_candidate_data_sha256=_text(
                inventory.get("candidate_data_sha256"), "inventory DATA SHA"
            ),
            resource_caps=S7DirectionalRawPreviewPreparationCaps.from_dict(
                document.get("resource_caps")
            ),
        )
        if _canonical_bytes(document) != plan.content:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution plan does not reproduce canonical bytes"
            )
        return plan


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewExecutionRequest:
    plan_id: str
    plan_path: str
    plan_sha256: str
    execution_data_root: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    source_binding_manifest_id: str
    source_binding_manifest_sha256: str
    manifest_preflight_intent_id: str
    manifest_preflight_intent_path: str
    manifest_preflight_intent_sha256: str
    source_artifact_set_digest: str
    inventory_completion_id: str
    inventory_completion_sha256: str
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    algorithm_digest: str
    qa_semantics_digest: str
    registry_semantics_digest: str
    preparation_request_event_id: str
    preparation_request_event_sha256: str
    preparation_approval_literal_sha256: str
    created_by: str
    created_at_utc: datetime
    authorized_action: str = EXECUTION_AUTHORIZED_ACTION
    execution_scope: str = EXECUTION_SCOPE
    request_state: str = EXECUTION_REQUEST_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file set digest", self.runtime_file_set_digest),
            ("verification file set digest", self.verification_file_set_digest),
            ("source-binding ID", self.source_binding_manifest_id),
            ("source-binding SHA", self.source_binding_manifest_sha256),
            ("manifest preflight intent ID", self.manifest_preflight_intent_id),
            ("manifest preflight intent SHA", self.manifest_preflight_intent_sha256),
            ("source artifact-set digest", self.source_artifact_set_digest),
            ("inventory completion ID", self.inventory_completion_id),
            ("inventory completion SHA", self.inventory_completion_sha256),
            ("scope-set ID", self.scope_set_id),
            ("scope-set SHA", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA", self.contract_candidate_sha256),
            ("algorithm digest", self.algorithm_digest),
            ("QA semantics digest", self.qa_semantics_digest),
            ("registry semantics digest", self.registry_semantics_digest),
            ("preparation request ID", self.preparation_request_event_id),
            ("preparation request SHA", self.preparation_request_event_sha256),
            ("preparation literal SHA", self.preparation_approval_literal_sha256),
        ):
            _digest(value, label)
        if self.plan_path != directional_raw_preview_execution_plan_path(self.plan_id):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "request plan path is not canonical"
            )
        _relative_path(self.manifest_preflight_intent_path, "manifest preflight intent path")
        _absolute_root(self.execution_data_root)
        _safe_text(self.created_by, "request created_by")
        object.__setattr__(
            self, "created_at_utc", _utc(self.created_at_utc, "request created_at_utc")
        )
        if (
            self.authorized_action != EXECUTION_AUTHORIZED_ACTION
            or self.execution_scope != EXECUTION_SCOPE
            or self.request_state != EXECUTION_REQUEST_STATE
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution request scope or state changed"
            )

    @classmethod
    def create(
        cls,
        plan: S7DirectionalRawPreviewExecutionPlan,
        plan_receipt: StoredDirectionalRawPreviewExecutionDocument,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7DirectionalRawPreviewExecutionRequest:
        if not isinstance(plan, S7DirectionalRawPreviewExecutionPlan):
            raise IdentityDirectionalRawPreviewExecutionPlanError("request plan type changed")
        _verify_receipt(plan_receipt, plan.relative_path, plan.sha256, len(plan.content))
        created = _utc(created_at_utc, "request created_at_utc")
        if (
            created != plan.created_at_utc
            or created_by != plan.manifest_preflight_intent_request_created_by
            or created_by in {plan.created_by, plan.source_binding_created_by}
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "request must follow the plan and use a separate actor"
            )
        return cls(
            plan_id=plan.plan_id,
            plan_path=plan.relative_path,
            plan_sha256=plan.sha256,
            execution_data_root=plan.execution_data_root,
            input_binding_digest=plan.input_binding_digest,
            resource_caps_digest=plan.resource_caps.digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            source_binding_manifest_id=plan.source_binding_manifest_id,
            source_binding_manifest_sha256=plan.source_binding_manifest_sha256,
            manifest_preflight_intent_id=plan.manifest_preflight_intent_id,
            manifest_preflight_intent_path=plan.manifest_preflight_intent_path,
            manifest_preflight_intent_sha256=plan.manifest_preflight_intent_sha256,
            source_artifact_set_digest=plan.source_artifact_set_digest,
            inventory_completion_id=plan.inventory_completion_id,
            inventory_completion_sha256=plan.inventory_completion_sha256,
            scope_set_id=plan.scope_set_id,
            scope_set_sha256=plan.scope_set_sha256,
            contract_id=plan.contract_id,
            contract_schema_digest=plan.contract_schema_digest,
            contract_candidate_sha256=plan.contract_candidate_sha256,
            algorithm_digest=plan.algorithm_digest,
            qa_semantics_digest=plan.qa_semantics_digest,
            registry_semantics_digest=(
                DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
            ),
            preparation_request_event_id=PREPARATION_REQUEST_EVENT_ID,
            preparation_request_event_sha256=PREPARATION_REQUEST_EVENT_SHA256,
            preparation_approval_literal_sha256=PREPARATION_APPROVAL_LITERAL_SHA256,
            created_by=created_by,
            created_at_utc=created,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm_digest": self.algorithm_digest,
            "artifact_type": "s7_directional_raw_preview_execution_request",
            "authorized_action": self.authorized_action,
            "contract_candidate_sha256": self.contract_candidate_sha256,
            "contract_id": self.contract_id,
            "contract_schema_digest": self.contract_schema_digest,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "input_binding_digest": self.input_binding_digest,
            "inventory_completion_id": self.inventory_completion_id,
            "inventory_completion_sha256": self.inventory_completion_sha256,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "preparation_approval_literal_sha256": (self.preparation_approval_literal_sha256),
            "preparation_request_event_id": self.preparation_request_event_id,
            "preparation_request_event_sha256": self.preparation_request_event_sha256,
            "qa_semantics_digest": self.qa_semantics_digest,
            "registry_semantics_digest": self.registry_semantics_digest,
            "request_rule_version": EXECUTION_REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": EXECUTION_REQUEST_SCHEMA_VERSION,
            "scope_set_id": self.scope_set_id,
            "scope_set_sha256": self.scope_set_sha256,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_binding_manifest_id": self.source_binding_manifest_id,
            "source_binding_manifest_sha256": self.source_binding_manifest_sha256,
            "manifest_preflight_intent_id": self.manifest_preflight_intent_id,
            "manifest_preflight_intent_path": self.manifest_preflight_intent_path,
            "manifest_preflight_intent_sha256": self.manifest_preflight_intent_sha256,
            "verification_file_set_digest": self.verification_file_set_digest,
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
        return directional_raw_preview_execution_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return json.dumps(
            {
                "algorithm_digest": self.algorithm_digest,
                "authorized_action": self.authorized_action,
                "contract_candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "contract_schema_digest": self.contract_schema_digest,
                "execution_data_root": self.execution_data_root,
                "input_binding_digest": self.input_binding_digest,
                "inventory_completion_id": self.inventory_completion_id,
                "inventory_completion_sha256": self.inventory_completion_sha256,
                "literal_version": EXECUTION_LITERAL_VERSION,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "preparation_approval_literal_sha256": (self.preparation_approval_literal_sha256),
                "qa_semantics_digest": self.qa_semantics_digest,
                "registry_semantics_digest": self.registry_semantics_digest,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.sha256,
                "resource_caps_digest": self.resource_caps_digest,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
                "source_artifact_set_digest": self.source_artifact_set_digest,
                "source_binding_manifest_id": self.source_binding_manifest_id,
                "source_binding_manifest_sha256": self.source_binding_manifest_sha256,
                "manifest_preflight_intent_id": self.manifest_preflight_intent_id,
                "manifest_preflight_intent_path": self.manifest_preflight_intent_path,
                "manifest_preflight_intent_sha256": self.manifest_preflight_intent_sha256,
                "verification_file_set_digest": self.verification_file_set_digest,
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewExecutionRequest:
        document = _mapping(value, "directional raw-preview execution request")
        request = cls(
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_path=_text(document.get("plan_path"), "plan path"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            execution_data_root=_text(document.get("execution_data_root"), "execution root"),
            input_binding_digest=_text(
                document.get("input_binding_digest"), "input binding digest"
            ),
            resource_caps_digest=_text(
                document.get("resource_caps_digest"), "resource caps digest"
            ),
            runtime_file_set_digest=_text(
                document.get("runtime_file_set_digest"), "runtime file-set digest"
            ),
            verification_file_set_digest=_text(
                document.get("verification_file_set_digest"),
                "verification file-set digest",
            ),
            source_binding_manifest_id=_text(
                document.get("source_binding_manifest_id"), "source-binding ID"
            ),
            source_binding_manifest_sha256=_text(
                document.get("source_binding_manifest_sha256"), "source-binding SHA"
            ),
            manifest_preflight_intent_id=_text(
                document.get("manifest_preflight_intent_id"),
                "manifest preflight intent ID",
            ),
            manifest_preflight_intent_path=_text(
                document.get("manifest_preflight_intent_path"),
                "manifest preflight intent path",
            ),
            manifest_preflight_intent_sha256=_text(
                document.get("manifest_preflight_intent_sha256"),
                "manifest preflight intent SHA",
            ),
            source_artifact_set_digest=_text(
                document.get("source_artifact_set_digest"), "source artifact-set digest"
            ),
            inventory_completion_id=_text(
                document.get("inventory_completion_id"), "inventory completion ID"
            ),
            inventory_completion_sha256=_text(
                document.get("inventory_completion_sha256"), "inventory completion SHA"
            ),
            scope_set_id=_text(document.get("scope_set_id"), "scope-set ID"),
            scope_set_sha256=_text(document.get("scope_set_sha256"), "scope-set SHA"),
            contract_id=_text(document.get("contract_id"), "contract ID"),
            contract_schema_digest=_text(
                document.get("contract_schema_digest"), "contract schema digest"
            ),
            contract_candidate_sha256=_text(
                document.get("contract_candidate_sha256"), "contract candidate SHA"
            ),
            algorithm_digest=_text(document.get("algorithm_digest"), "algorithm digest"),
            qa_semantics_digest=_text(document.get("qa_semantics_digest"), "QA semantics digest"),
            registry_semantics_digest=_text(
                document.get("registry_semantics_digest"), "registry semantics digest"
            ),
            preparation_request_event_id=_text(
                document.get("preparation_request_event_id"), "preparation request ID"
            ),
            preparation_request_event_sha256=_text(
                document.get("preparation_request_event_sha256"), "preparation request SHA"
            ),
            preparation_approval_literal_sha256=_text(
                document.get("preparation_approval_literal_sha256"),
                "preparation literal SHA",
            ),
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            authorized_action=_text(document.get("authorized_action"), "authorized action"),
            execution_scope=_text(document.get("execution_scope"), "execution scope"),
            request_state=_text(document.get("request_state"), "request state"),
        )
        if _canonical_bytes(document) != request.content:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "execution request does not reproduce canonical bytes"
            )
        return request


class DirectionalRawPreviewExecutionPlanStore:
    """Immutable JSON control store; it never opens a source data artifact."""

    def __init__(self, data_root: Path) -> None:
        if not isinstance(data_root, Path):
            raise IdentityDirectionalRawPreviewExecutionPlanError("data_root must be a Path")
        expanded = data_root.expanduser()
        if expanded.is_symlink():
            raise IdentityDirectionalRawPreviewExecutionPlanError("data_root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityDirectionalRawPreviewExecutionPlanError("data_root must exist")

    def store_execution_plan(
        self, value: S7DirectionalRawPreviewExecutionPlan
    ) -> StoredDirectionalRawPreviewExecutionDocument:
        if value.execution_data_root != str(self.root):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "plan execution root differs from store"
            )
        self._verify_source_binding(value)
        return self._write(value.relative_path, value.content)

    def load_execution_plan(
        self, plan_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewExecutionPlan,
        StoredDirectionalRawPreviewExecutionDocument,
    ]:
        value, receipt = self._read(
            directional_raw_preview_execution_plan_path(plan_id),
            expected_sha256,
            S7DirectionalRawPreviewExecutionPlan.from_dict,
        )
        if value.execution_data_root != str(self.root):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "plan execution root differs from store"
            )
        self._verify_source_binding(value)
        return value, receipt

    def store_execution_request(
        self, value: S7DirectionalRawPreviewExecutionRequest
    ) -> StoredDirectionalRawPreviewExecutionDocument:
        plan, _ = self.load_execution_plan(value.plan_id, expected_sha256=value.plan_sha256)
        _verify_request_plan(value, plan)
        return self._write(value.relative_path, value.content)

    def load_execution_request(
        self, request_event_id: str, *, expected_sha256: str
    ) -> tuple[
        S7DirectionalRawPreviewExecutionRequest,
        StoredDirectionalRawPreviewExecutionDocument,
    ]:
        value, receipt = self._read(
            directional_raw_preview_execution_request_path(request_event_id),
            expected_sha256,
            S7DirectionalRawPreviewExecutionRequest.from_dict,
        )
        plan, _ = self.load_execution_plan(value.plan_id, expected_sha256=value.plan_sha256)
        _verify_request_plan(value, plan)
        return value, receipt

    def _verify_source_binding(self, plan: S7DirectionalRawPreviewExecutionPlan) -> None:
        from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
            IdentityDirectionalRawPreviewManifestPlanError,
            IdentityDirectionalRawPreviewManifestStore,
        )

        try:
            source_binding, receipt = IdentityDirectionalRawPreviewManifestStore(
                self.root
            ).load_source_binding(
                plan.manifest_preflight_intent_id,
                expected_source_binding_id=plan.source_binding_manifest_id,
                expected_sha256=plan.source_binding_manifest_sha256,
            )
        except IdentityDirectionalRawPreviewManifestPlanError as exc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "source-binding manifest is missing or altered"
            ) from exc
        normalized = tuple(
            S7DirectionalRawPreviewExecutionSourcePin.from_source_ref(item)
            for item in source_binding.source_artifacts
        )
        manifests = {item.kind: item for item in source_binding.manifest_documents}
        manifest_plan, _ = IdentityDirectionalRawPreviewManifestStore(self.root).load_plan(
            source_binding.manifest_plan_id,
            expected_sha256=source_binding.manifest_plan_sha256,
        )
        _verify_manifest_plan_execution_projection(
            manifest_plan,
            execution_git_commit=plan.execution_git_commit,
            execution_git_tree=plan.execution_git_tree,
            execution_data_root=plan.execution_data_root,
            runtime_files=plan.runtime_files,
            verification_files=plan.verification_files,
        )
        approval_module = import_module(
            "ame_stocks_api.silver.identity_directional_raw_preview_manifest_" + "approval"
        )
        approval, _ = approval_module.DirectionalRawPreviewManifestApprovalStore(
            self.root
        ).load_approval(
            source_binding.manifest_approval_id,
            expected_sha256=source_binding.manifest_approval_sha256,
        )
        intent, intent_receipt = IdentityDirectionalRawPreviewManifestStore(
            self.root
        ).load_run_intent(
            source_binding.manifest_plan_id,
            source_binding.manifest_approval_id,
            expected_sha256=source_binding.manifest_run_intent_sha256,
        )
        completion = manifests.get("inventory_completion_manifest")
        candidate = manifests.get("inventory_candidate_manifest")
        if (
            receipt.path != plan.source_binding_manifest_path
            or source_binding.created_by != plan.source_binding_created_by
            or source_binding.created_at_utc != plan.source_binding_created_at_utc
            or source_binding.manifest_approval_id != plan.manifest_preflight_approval_id
            or source_binding.manifest_approval_sha256 != plan.manifest_preflight_approval_sha256
            or source_binding.manifest_run_intent_id != plan.manifest_preflight_intent_id
            or source_binding.manifest_run_intent_path != plan.manifest_preflight_intent_path
            or source_binding.manifest_run_intent_sha256
            != plan.manifest_preflight_intent_sha256
            or intent.intent_id != source_binding.manifest_run_intent_id
            or intent_receipt.path != source_binding.manifest_run_intent_path
            or intent.manifest_plan_sha256 != source_binding.manifest_plan_sha256
            or intent.manifest_request_event_id != source_binding.manifest_request_event_id
            or intent.manifest_request_event_sha256
            != source_binding.manifest_request_event_sha256
            or intent.manifest_approval_sha256 != source_binding.manifest_approval_sha256
            or intent.approval_literal_sha256 != source_binding.manifest_literal_sha256
            or intent.source_binding_created_by != source_binding.created_by
            or intent.source_binding_created_at_utc != source_binding.created_at_utc
            or intent.execution_plan_created_by != plan.created_by
            or intent.execution_request_created_by
            != plan.manifest_preflight_intent_request_created_by
            or intent.source_binding_created_at_utc != plan.created_at_utc
            or approval.plan_id != source_binding.manifest_plan_id
            or approval.plan_sha256 != source_binding.manifest_plan_sha256
            or approval.request_event_id != source_binding.manifest_request_event_id
            or approval.request_event_sha256 != source_binding.manifest_request_event_sha256
            or approval.approval_literal_sha256 != source_binding.manifest_literal_sha256
            or approval.approval_id != source_binding.manifest_approval_id
            or approval.sha256 != source_binding.manifest_approval_sha256
            or source_binding.created_at_utc <= approval.approved_at_utc
            or normalized != plan.source_artifacts
            or completion is None
            or candidate is None
            or completion.logical_id != plan.inventory_completion_id
            or completion.path != plan.inventory_completion_path
            or completion.sha256 != plan.inventory_completion_sha256
            or candidate.logical_id != plan.inventory_candidate_id
            or candidate.path != plan.inventory_candidate_path
            or candidate.sha256 != plan.inventory_candidate_manifest_sha256
        ):
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "source-binding manifest projection differs from execution plan"
            )
        _verify_source_binding_preparation_lineage(source_binding)

    def _write(self, relative: str, content: bytes) -> StoredDirectionalRawPreviewExecutionDocument:
        try:
            path = safe_relative_path(self.root, relative)
            receipt = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(str(exc)) from exc
        return StoredDirectionalRawPreviewExecutionDocument(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _read(
        self, relative: str, expected_sha256: str, parser: Any
    ) -> tuple[Any, StoredDirectionalRawPreviewExecutionDocument]:
        _digest(expected_sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                f"control document is missing or altered: {relative}"
            )
        content = path.read_bytes()
        try:
            document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                f"control document is not JSON: {relative}"
            ) from exc
        if not isinstance(document, dict) or _canonical_bytes(document) != content:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                f"control document is not canonical JSON: {relative}"
            )
        value = parser(document)
        if value.relative_path != relative or value.sha256 != expected_sha256:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                f"control document identity differs: {relative}"
            )
        return value, StoredDirectionalRawPreviewExecutionDocument(
            relative, expected_sha256, len(content)
        )


def directional_raw_preview_execution_plan_path(plan_id: str) -> str:
    _digest(plan_id, "execution plan ID")
    return (
        "manifests/silver/identity/directional-raw-preview-execution-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


def directional_raw_preview_execution_request_path(request_event_id: str) -> str:
    _digest(request_event_id, "execution request event ID")
    return (
        "manifests/silver/identity/directional-raw-preview-execution-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )


def _verify_receipt(
    receipt: object,
    path: str,
    checksum: str,
    size: int,
) -> None:
    if (
        _attribute(receipt, "path") != path
        or _attribute(receipt, "sha256") != checksum
        or _attribute(receipt, "bytes") != size
    ):
        raise IdentityDirectionalRawPreviewExecutionPlanError("stored receipt differs")


def _verify_manifest_plan_execution_projection(
    manifest_plan: object,
    *,
    execution_git_commit: str,
    execution_git_tree: str,
    execution_data_root: str,
    runtime_files: tuple[object, ...],
    verification_files: tuple[object, ...],
) -> None:
    expected_runtime = tuple(
        sorted(
            S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
            for item in _attribute(manifest_plan, "runtime_files")
        )
    )
    expected_verification = tuple(
        sorted(
            S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
            for item in _attribute(manifest_plan, "verification_files")
        )
    )
    actual_runtime = tuple(
        sorted(
            S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
            for item in runtime_files
        )
    )
    actual_verification = tuple(
        sorted(
            S7DirectionalRawPreviewControlFilePin.from_dict(item.to_dict())
            for item in verification_files
        )
    )
    if (
        execution_git_commit != _attribute(manifest_plan, "git_commit")
        or execution_git_tree != _attribute(manifest_plan, "git_tree")
        or execution_data_root != _attribute(manifest_plan, "execution_data_root")
        or actual_runtime != expected_runtime
        or actual_verification != expected_verification
    ):
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "execution Git or file pins differ from approved manifest plan"
        )


def _verify_source_binding_preparation_lineage(source_binding: object) -> None:
    expected = preparation_authorization_lineage()
    document = _mapping(_attribute(source_binding, "document"), "source-binding document")
    lineage = _mapping(document.get("preparation_control_lineage"), "preparation control lineage")
    for name in (
        "approved_literal_sha256",
        "plan_id",
        "plan_sha256",
        "request_event_id",
        "request_event_sha256",
        "scope_set_id",
        "scope_set_sha256",
    ):
        if lineage.get(name) != expected[name]:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                f"source-binding preparation {name} differs from approved lineage"
            )


def _verify_request_plan(
    request: S7DirectionalRawPreviewExecutionRequest,
    plan: S7DirectionalRawPreviewExecutionPlan,
) -> None:
    if (
        request.plan_id != plan.plan_id
        or request.plan_path != plan.relative_path
        or request.plan_sha256 != plan.sha256
        or request.execution_data_root != plan.execution_data_root
        or request.input_binding_digest != plan.input_binding_digest
        or request.resource_caps_digest != plan.resource_caps.digest
        or request.runtime_file_set_digest != plan.runtime_file_set_digest
        or request.verification_file_set_digest != plan.verification_file_set_digest
        or request.source_binding_manifest_id != plan.source_binding_manifest_id
        or request.source_binding_manifest_sha256 != plan.source_binding_manifest_sha256
        or request.manifest_preflight_intent_id != plan.manifest_preflight_intent_id
        or request.manifest_preflight_intent_path != plan.manifest_preflight_intent_path
        or request.manifest_preflight_intent_sha256
        != plan.manifest_preflight_intent_sha256
        or request.source_artifact_set_digest != plan.source_artifact_set_digest
        or request.inventory_completion_id != plan.inventory_completion_id
        or request.inventory_completion_sha256 != plan.inventory_completion_sha256
        or request.scope_set_id != plan.scope_set_id
        or request.scope_set_sha256 != plan.scope_set_sha256
        or request.contract_id != plan.contract_id
        or request.contract_schema_digest != plan.contract_schema_digest
        or request.contract_candidate_sha256 != plan.contract_candidate_sha256
        or request.algorithm_digest != plan.algorithm_digest
        or request.qa_semantics_digest != plan.qa_semantics_digest
        or request.registry_semantics_digest
        != DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        or request.preparation_request_event_id != PREPARATION_REQUEST_EVENT_ID
        or request.preparation_request_event_sha256 != PREPARATION_REQUEST_EVENT_SHA256
        or request.preparation_approval_literal_sha256 != PREPARATION_APPROVAL_LITERAL_SHA256
        or request.created_at_utc != plan.created_at_utc
        or request.created_by != plan.manifest_preflight_intent_request_created_by
        or request.created_by in {plan.created_by, plan.source_binding_created_by}
    ):
        raise IdentityDirectionalRawPreviewExecutionPlanError(
            "execution request crosses plan bindings"
        )


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise IdentityDirectionalRawPreviewExecutionPlanError(
                "control document contains duplicate JSON keys"
            )
        output[key] = value
    return output


__all__ = [
    "CANONICAL_EXECUTION_PATHS",
    "DIRECTIONAL_RAW_PREVIEW_ALGORITHM_DIGEST",
    "DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST",
    "EXECUTION_AUTHORIZED_ACTION",
    "EXECUTION_LITERAL_VERSION",
    "EXECUTION_SCOPE",
    "PREPARATION_APPROVAL_LITERAL_SHA256",
    "REQUIRED_EXECUTION_RUNTIME_PATHS",
    "REQUIRED_EXECUTION_VERIFICATION_PATHS",
    "DirectionalRawPreviewExecutionPlanStore",
    "IdentityDirectionalRawPreviewExecutionPlanError",
    "S7DirectionalRawPreviewExecutionPlan",
    "S7DirectionalRawPreviewExecutionRequest",
    "S7DirectionalRawPreviewExecutionSourcePin",
    "StoredDirectionalRawPreviewExecutionDocument",
    "canonical_directional_raw_preview_execution_paths",
    "directional_raw_preview_algorithm_spec",
    "directional_raw_preview_execution_plan_path",
    "directional_raw_preview_execution_request_path",
    "directional_raw_preview_qa_semantics",
    "preparation_authorization_lineage",
]
