"""Superseding, execution-complete S7 Composite-inventory control plane.

The first Gate-A plan correctly froze its source scope and resource envelope, but
it bound a Git commit that intentionally contained no approval recorder, runner,
candidate contract, or completion serializer.  Its output contract also named
columns without freezing their physical schema or aggregation semantics.  The
exact v1 literal is therefore preserved as audit evidence but cannot safely be
turned into an approval receipt.

This module is deliberately control-plane only.  It can:

* load the immutable v1 plan and request by exact ID/SHA;
* record one immutable audit event explaining why v1 execution was blocked; and
* freeze a v2 plan/request that binds executable Git bytes, a candidate contract,
  deterministic aggregation/QA semantics, canonical paths, and resource caps.

It cannot create an execution approval, read Parquet, run the inventory, access a
network, classify a market, adjudicate identity, materialize research tables, or
publish anything.
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
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT,
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_plan import (
    DAILY_SOURCE_ARTIFACT_COUNT,
    DAILY_SOURCE_BYTES,
    DAILY_SOURCE_ROW_COUNT,
    EXACT_SOURCE_PINS,
    INVENTORY_CALENDAR_ARTIFACT_ID,
    INVENTORY_CALENDAR_ARTIFACT_SHA256,
    INVENTORY_END_SESSION,
    INVENTORY_SESSION_COUNT,
    INVENTORY_START_SESSION,
    PREVIEW_APPROVAL_ID,
    PREVIEW_ARTIFACT_ID,
    PREVIEW_ARTIFACT_SHA256,
    PREVIEW_CASE_COUNT,
    PREVIEW_CASE_EVIDENCE_SET_DIGEST,
    PREVIEW_COMPLETION_ID,
    PREVIEW_COMPLETION_SHA256,
    PREVIEW_PLAN_ID,
    PREVIEW_SUSPECTED_ROW_COUNT,
    IdentityMarketInventoryPlanError,
    IdentityMarketInventoryPlanStore,
    S7CompositeInventoryApprovalRequest,
    S7CompositeInventoryPlan,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
)

V1_PLAN_ID: Final = "563ac40a43a1bb979bbd71d399f2516f142a12067ad68c9e5308a709ffff45f0"
V1_PLAN_SHA256: Final = "8aea58f744bf5a4449471de948fc921d8a079200e5248a322ce23cccbeb0f1e1"
V1_REQUEST_EVENT_ID: Final = "e0bb7c9f5c81e19925e51f62f93c82bfc89881a03a37884dd17a5ec3cab1cd14"
V1_REQUEST_EVENT_SHA256: Final = "3e42c32c17037f554303b547db37cc94f6ae415d543b1cc0ba55b05ff73b88dd"
V1_INPUT_BINDING_DIGEST: Final = "1a36d4b3c813f8d7281658e66c8f19af2fbf22a0c4eb298f223c47c1a38608b7"
V1_RESOURCE_CAPS_DIGEST: Final = "3b246d6018c28b78b3e4515592a6fb130eb7a8ff5a404f542cdf38005a7871e0"
V1_BOUND_GIT_COMMIT: Final = "84057ffa9e16f97ab3960c514d843f8627eadf73"
V1_BOUND_GIT_TREE: Final = "938abd27a792d449ecf8e0084c29072f17636a00"
V1_LITERAL_VERSION: Final = "s7_composite_inventory_approval_literal_v1"
V1_AUTHORIZED_ACTION: Final = (
    "execute_exact_s4_full_history_composite_inventory_once_to_awaiting_review"
)
V1_LITERAL: Final = json.dumps(
    {
        "authorized_action": V1_AUTHORIZED_ACTION,
        "input_binding_digest": V1_INPUT_BINDING_DIGEST,
        "literal_version": V1_LITERAL_VERSION,
        "plan_id": V1_PLAN_ID,
        "plan_sha256": V1_PLAN_SHA256,
        "request_event_id": V1_REQUEST_EVENT_ID,
        "request_event_sha256": V1_REQUEST_EVENT_SHA256,
        "resource_caps_digest": V1_RESOURCE_CAPS_DIGEST,
    },
    ensure_ascii=False,
    separators=(",", ":"),
    sort_keys=True,
)
V1_LITERAL_SHA256: Final = hashlib.sha256(V1_LITERAL.encode("utf-8")).hexdigest()

V1_BLOCKED_REASONS: Final = (
    "runner_absent_at_bound_commit",
    "output_contract_underdefined",
)
V1_MISSING_RUNTIME_PATHS: Final = (
    "backend/ame_stocks_api/silver/identity_market_inventory_approval.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_contract.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_runner.py",
)
V1_OUTPUT_CONTRACT_GAPS: Final = (
    "aggregation_semantics_not_bound",
    "candidate_contract_id_not_bound",
    "candidate_schema_digest_not_bound",
    "candidate_types_and_nullability_not_bound",
    "identifier_validation_semantics_not_bound",
    "lineage_digest_algorithm_not_bound",
    "output_serialization_and_sort_order_not_bound",
    "qa_semantics_not_bound",
)

BLOCKED_EVENT_SCHEMA_VERSION: Final = 1
BLOCKED_EVENT_RULE_VERSION: Final = "s7_v1_composite_inventory_execution_blocked_v1"
BLOCKED_EVENT_STATE: Final = "execution_blocked_superseded"

EXECUTION_PLAN_SCHEMA_VERSION: Final = 2
EXECUTION_PLAN_RULE_VERSION: Final = "s7_composite_inventory_execution_plan_v2"
EXECUTION_PLAN_STATE: Final = "awaiting_exact_execution_approval"
EXECUTION_REQUEST_SCHEMA_VERSION: Final = 2
EXECUTION_REQUEST_RULE_VERSION: Final = "s7_composite_inventory_execution_request_v2"
EXECUTION_REQUEST_STATE: Final = "awaiting_literal_human_approval"
EXECUTION_LITERAL_VERSION: Final = "s7_composite_inventory_approval_literal_v2"
EXECUTION_AUTHORIZED_ACTION: Final = (
    "execute_exact_s4_full_history_composite_inventory_v2_once_to_awaiting_review"
)
EXECUTION_SCOPE: Final = (
    "full_s4_composite_inventory_candidate_only_no_market_classification_"
    "no_adjudication_no_materialization_no_release"
)

INVENTORY_CONTRACT_TABLE: Final = "composite_figi_inventory"
INVENTORY_CONTRACT_DOMAIN: Final = "identity"
INVENTORY_CONTRACT_CANDIDATE_PATH: Final = (
    "docs/silver/contracts/identity/composite_figi_inventory.schema-v1.candidate.json"
)
INVENTORY_CONTRACT_RESOURCE_PATH: Final = (
    "backend/ame_stocks_api/silver/schema_resources/composite_figi_inventory.schema-v1.json"
)

INVENTORY_ALGORITHM_RULE_VERSION: Final = "s7_s4_composite_inventory_streaming_aggregation_v2"
LINEAGE_DIGEST_RULE_VERSION: Final = "s7_composite_inventory_source_record_lineage_v1"

FIGI_REASON_PRECEDENCE: Final = (
    "null",
    "empty",
    "whitespace_only",
    "surrounding_whitespace",
    "length_not_12",
    "non_upper_ascii_alnum",
    "prefix_not_BBG",
)

UNIVERSE_PARENT_RECONCILIATION_PROJECTION: Final = (
    "session_year",
    "session_date",
    "requested_active_to_active_on_date",
    "ticker",
    "provider_active_to_active_on_date",
    "type_code",
    "name",
    "market",
    "locale",
    "primary_exchange_mic",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "delisted_at_utc",
    "last_updated_at_utc",
    "reference_time_scope",
    "metadata_time_scope",
    "source_capture_at_utc_to_selected_source_capture_at_utc",
    "source_availability_quality",
    "source_record_id_to_selected_source_record_id",
    "source_request_id",
    "source_provider_request_id",
    "source_artifact_sha256",
    "source_page_sequence",
    "source_row_ordinal",
    "source_row_hash",
)

INVENTORY_OUTPUT_COLUMNS: Final = (
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
)

UNIVERSE_PAIR_DERIVED_FIELDS_EXCLUDED_FROM_PARENT_EQUALITY: Final = (
    "source_available_session",
    "source_available_at_utc",
    "source_availability_rule",
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
        "backend/ame_stocks_api/silver/calendar_artifact.py",
        "backend/ame_stocks_api/silver/fixed_cases.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_plan.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_request.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_contract.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_approval.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_runner.py",
        "backend/ame_stocks_api/silver/identity_source.py",
        ("backend/ame_stocks_api/cli/silver_identity_market_inventory_execution_request.py"),
        "backend/ame_stocks_api/cli/silver_identity_market_inventory_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_market_inventory_run.py",
    }
)

REQUIRED_EXECUTION_VERIFICATION_PATHS: Final = frozenset(
    {
        "tests/test_silver_identity_market_inventory_plan.py",
        "tests/test_silver_identity_market_inventory_request.py",
        "tests/test_silver_identity_market_inventory_execution_plan.py",
        "tests/test_silver_identity_market_inventory_execution_request.py",
        "tests/test_silver_identity_market_inventory_contract.py",
        "tests/test_silver_identity_market_inventory_engine.py",
        "tests/test_silver_identity_market_inventory_approval.py",
        "tests/test_silver_identity_market_inventory_runner.py",
        "tests/test_silver_identity_market_inventory_run.py",
        "tests/test_silver_lazy_imports.py",
    }
)

CANONICAL_EXECUTION_PATHS: Final = MappingProxyType(
    {
        "approval": (
            "manifests/silver/identity/composite-inventory-execution-approvals/"
            "approval_id={approval_id}/manifest.json"
        ),
        "candidate": (
            "manifests/silver/identity/composite-inventory-candidates/"
            "candidate_id={candidate_id}/manifest.json"
        ),
        "candidate_data": (
            "manifests/silver/identity/composite-inventory-candidates/"
            "candidate_id={candidate_id}/data/part-00000.parquet"
        ),
        "completion": (
            "manifests/silver/identity/composite-inventory-execution-completions/"
            "plan_id={plan_id}/approval_id={approval_id}/manifest.json"
        ),
        "lock": ("manifests/silver/identity/locks/composite-inventory-run_id={run_id}.lock"),
        "staging": "tmp/silver-identity-composite-inventory/run_id={run_id}",
    }
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class IdentityMarketInventoryExecutionPlanError(RuntimeError):
    """Raised when the superseding execution control chain is not exact."""


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be an array")
    return list(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityMarketInventoryExecutionPlanError(
            f"{label} schema is not exact: missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be text")
    return value


def _digest(value: object, label: str) -> str:
    text = _string(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _string(value, label)
    if not _GIT_OBJECT.fullmatch(text):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be lowercase 40-hex")
    return text


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be a positive native int")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be bool")
    return value


def _safe_text(value: object, label: str, maximum: int = 200) -> str:
    text = _string(value, label)
    if (
        not text
        or len(text) > maximum
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise IdentityMarketInventoryExecutionPlanError(f"{label} is unsafe")
    return text


def _relative_path(value: object, label: str) -> str:
    text = _string(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} is not a safe relative path")
    return text


def _absolute_data_root(value: object, label: str = "execution_data_root") -> str:
    text = _string(value, label)
    path = Path(text)
    if (
        not path.is_absolute()
        or text == "/"
        or path.as_posix() != text
        or str(path) != text
        or ".." in path.parts
    ):
        raise IdentityMarketInventoryExecutionPlanError(
            f"{label} must be a canonical absolute non-root path"
        )
    return text


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _utc(value, "UTC datetime").isoformat()


def _parse_utc(value: object, label: str) -> datetime:
    text = _string(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} is not ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} is not canonical UTC")
    return normalized


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
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise IdentityMarketInventoryExecutionPlanError(
                    f"{label} contains duplicate JSON keys"
                )
            result[key] = value
        return result

    try:
        value = json.loads(content, object_pairs_hook=reject_duplicates)
    except IdentityMarketInventoryExecutionPlanError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryExecutionPlanError(f"{label} is not JSON") from exc
    return _mapping(value, label)


@dataclass(frozen=True, slots=True)
class StoredInventoryExecutionDocument:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "stored path")
        _digest(self.sha256, "stored SHA-256")
        _positive_int(self.bytes, "stored bytes")


@dataclass(frozen=True, slots=True)
class S7V1InventoryControlLineage:
    """Exact immutable v1 Plan/Request and received-literal binding."""

    plan_id: str = V1_PLAN_ID
    plan_sha256: str = V1_PLAN_SHA256
    request_event_id: str = V1_REQUEST_EVENT_ID
    request_event_sha256: str = V1_REQUEST_EVENT_SHA256
    input_binding_digest: str = V1_INPUT_BINDING_DIGEST
    resource_caps_digest: str = V1_RESOURCE_CAPS_DIGEST
    bound_git_commit: str = V1_BOUND_GIT_COMMIT
    bound_git_tree: str = V1_BOUND_GIT_TREE
    literal_sha256: str = V1_LITERAL_SHA256

    def __post_init__(self) -> None:
        expected = (
            V1_PLAN_ID,
            V1_PLAN_SHA256,
            V1_REQUEST_EVENT_ID,
            V1_REQUEST_EVENT_SHA256,
            V1_INPUT_BINDING_DIGEST,
            V1_RESOURCE_CAPS_DIGEST,
            V1_BOUND_GIT_COMMIT,
            V1_BOUND_GIT_TREE,
            V1_LITERAL_SHA256,
        )
        if (
            self.plan_id,
            self.plan_sha256,
            self.request_event_id,
            self.request_event_sha256,
            self.input_binding_digest,
            self.resource_caps_digest,
            self.bound_git_commit,
            self.bound_git_tree,
            self.literal_sha256,
        ) != expected:
            raise IdentityMarketInventoryExecutionPlanError("v1 control lineage is not exact")

    @property
    def plan_path(self) -> str:
        return (
            "manifests/silver/identity/composite-inventory-plans/"
            f"plan_id={self.plan_id}/manifest.json"
        )

    @property
    def request_path(self) -> str:
        return (
            "manifests/silver/identity/composite-inventory-approval-requests/"
            f"request_event_id={self.request_event_id}/manifest.json"
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "bound_git_commit": self.bound_git_commit,
            "bound_git_tree": self.bound_git_tree,
            "input_binding_digest": self.input_binding_digest,
            "literal": V1_LITERAL,
            "literal_received": True,
            "literal_sha256": self.literal_sha256,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_path": self.request_path,
            "request_event_sha256": self.request_event_sha256,
            "resource_caps_digest": self.resource_caps_digest,
        }


@dataclass(frozen=True, slots=True)
class S7V1InventoryExecutionBlockedEvent:
    """Audit-only proof that the received v1 literal was not executed."""

    recorded_by: str
    recorded_at_utc: datetime
    v1_lineage: S7V1InventoryControlLineage = S7V1InventoryControlLineage()
    state: str = field(default=BLOCKED_EVENT_STATE, init=False)

    def __post_init__(self) -> None:
        _safe_text(self.recorded_by, "recorded_by")
        object.__setattr__(
            self,
            "recorded_at_utc",
            _utc(self.recorded_at_utc, "recorded_at_utc"),
        )
        if not isinstance(self.v1_lineage, S7V1InventoryControlLineage):
            raise IdentityMarketInventoryExecutionPlanError("v1 lineage has wrong type")
        if self.state != BLOCKED_EVENT_STATE:
            raise IdentityMarketInventoryExecutionPlanError("blocked event state changed")

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_v1_composite_inventory_execution_blocked_event",
            "blocking_reasons": list(V1_BLOCKED_REASONS),
            "bound_commit_audit": {
                "git_commit": V1_BOUND_GIT_COMMIT,
                "git_tree": V1_BOUND_GIT_TREE,
                "missing_runtime_paths": list(V1_MISSING_RUNTIME_PATHS),
                "output_contract_gaps": list(V1_OUTPUT_CONTRACT_GAPS),
            },
            "capabilities": {
                "approval_receipt_creation_authorized": False,
                "inventory_execution_authorized": False,
                "market_classification_authorized": False,
                "network_access_authorized": False,
                "parquet_read_authorized": False,
                "publication_authorized": False,
                "runner_authorized": False,
            },
            "execution_facts": {
                "approval_receipt_created": False,
                "candidate_written": False,
                "completion_written": False,
                "data_run_started": False,
                "parquet_opened": False,
                "remote_data_changed": False,
            },
            "recorded_at_utc": _utc_text(self.recorded_at_utc),
            "recorded_by": self.recorded_by,
            "rule_version": BLOCKED_EVENT_RULE_VERSION,
            "schema_version": BLOCKED_EVENT_SCHEMA_VERSION,
            "state": self.state,
            "supersession_policy": {
                "new_exact_literal_required": True,
                "v1_controls_remain_immutable": True,
                "v1_literal_received_but_not_converted_to_approval": True,
            },
            "v1_control_lineage": self.v1_lineage.to_dict(),
        }

    @property
    def event_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "event_id": self.event_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return v1_execution_blocked_event_path(self.event_id)

    @classmethod
    def from_dict(cls, value: object) -> S7V1InventoryExecutionBlockedEvent:
        document = _mapping(value, "v1 execution blocked event")
        recorded = cls(
            recorded_by=_string(document.get("recorded_by"), "recorded_by"),
            recorded_at_utc=_parse_utc(document.get("recorded_at_utc"), "recorded_at_utc"),
        )
        if _canonical_bytes(document) != recorded.content:
            raise IdentityMarketInventoryExecutionPlanError(
                "v1 blocked event does not reproduce canonical bytes"
            )
        return recorded


@dataclass(frozen=True, slots=True, order=True)
class S7InventoryRuntimeFilePin:
    path: str
    git_blob: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "runtime file path")
        _git_object(self.git_blob, "runtime Git blob")
        _digest(self.sha256, "runtime file SHA-256")
        _positive_int(self.bytes, "runtime file bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "git_blob": self.git_blob,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7InventoryRuntimeFilePin:
        item = _mapping(value, "runtime file pin")
        _expect_keys(item, {"bytes", "git_blob", "path", "sha256"}, "runtime file pin")
        return cls(
            path=_string(item["path"], "runtime path"),
            git_blob=_string(item["git_blob"], "runtime Git blob"),
            sha256=_string(item["sha256"], "runtime SHA-256"),
            bytes=_positive_int(item["bytes"], "runtime bytes"),
        )


@dataclass(frozen=True, slots=True)
class S7InventoryCandidateContractPin:
    contract_id: str
    schema_digest: str
    candidate_sha256: str
    resource_sha256: str
    candidate_path: str = INVENTORY_CONTRACT_CANDIDATE_PATH
    resource_path: str = INVENTORY_CONTRACT_RESOURCE_PATH
    table: str = INVENTORY_CONTRACT_TABLE
    domain: str = INVENTORY_CONTRACT_DOMAIN
    schema_version: int = 1

    def __post_init__(self) -> None:
        _digest(self.contract_id, "inventory contract ID")
        _digest(self.schema_digest, "inventory schema digest")
        _digest(self.candidate_sha256, "inventory candidate SHA-256")
        _digest(self.resource_sha256, "inventory packaged resource SHA-256")
        if (
            self.candidate_path != INVENTORY_CONTRACT_CANDIDATE_PATH
            or self.resource_path != INVENTORY_CONTRACT_RESOURCE_PATH
            or self.resource_sha256 != self.candidate_sha256
            or self.contract_id != COMPOSITE_FIGI_INVENTORY_CONTRACT_ID
            or self.schema_digest != COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
            or self.candidate_sha256 != COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256
            or self.table != INVENTORY_CONTRACT_TABLE
            or self.domain != INVENTORY_CONTRACT_DOMAIN
            or self.schema_version != 1
        ):
            raise IdentityMarketInventoryExecutionPlanError(
                "inventory candidate contract identity changed"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_path": self.candidate_path,
            "candidate_sha256": self.candidate_sha256,
            "contract_id": self.contract_id,
            "domain": self.domain,
            "resource_path": self.resource_path,
            "resource_sha256": self.resource_sha256,
            "schema_digest": self.schema_digest,
            "schema_version": self.schema_version,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7InventoryCandidateContractPin:
        item = _mapping(value, "inventory candidate contract pin")
        _expect_keys(
            item,
            {
                "candidate_path",
                "candidate_sha256",
                "contract_id",
                "domain",
                "resource_path",
                "resource_sha256",
                "schema_digest",
                "schema_version",
                "table",
            },
            "inventory candidate contract pin",
        )
        if type(item["schema_version"]) is not int:
            raise IdentityMarketInventoryExecutionPlanError("schema version must be int")
        return cls(
            contract_id=_string(item["contract_id"], "contract ID"),
            schema_digest=_string(item["schema_digest"], "schema digest"),
            candidate_sha256=_string(item["candidate_sha256"], "candidate SHA"),
            resource_sha256=_string(item["resource_sha256"], "resource SHA"),
            candidate_path=_string(item["candidate_path"], "candidate path"),
            resource_path=_string(item["resource_path"], "resource path"),
            table=_string(item["table"], "contract table"),
            domain=_string(item["domain"], "contract domain"),
            schema_version=item["schema_version"],
        )


@dataclass(frozen=True, slots=True)
class S7InventoryExecutionResourceCaps:
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
    disk_free_floor_bytes: int = 40 * 1024 * 1024 * 1024
    disk_free_warning_bytes: int = 60 * 1024 * 1024 * 1024
    bounded_example_cap: int = 20
    resource_check_interval_batches: int = 1

    def __post_init__(self) -> None:
        expected = {
            "batch_size": 65_536,
            "bounded_example_cap": 20,
            "composite_share_class_pair_cap": 250_000,
            "disk_free_floor_bytes": 40 * 1024 * 1024 * 1024,
            "disk_free_warning_bytes": 60 * 1024 * 1024 * 1024,
            "distinct_composite_cap": 100_000,
            "output_bytes_cap": 256 * 1024 * 1024,
            "resource_check_interval_batches": 1,
            "rss_bytes_cap": 2 * 1024 * 1024 * 1024,
            "scanned_artifact_cap": DAILY_SOURCE_ARTIFACT_COUNT,
            "scanned_row_cap": DAILY_SOURCE_ROW_COUNT,
            "source_bytes_cap": DAILY_SOURCE_BYTES,
            "tmp_bytes_cap": 4 * 1024 * 1024 * 1024,
            "wall_clock_seconds_cap": 14_400,
            "worker_count": 1,
        }
        if self.to_dict() != expected:
            raise IdentityMarketInventoryExecutionPlanError("v2 execution resource caps changed")

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_size": self.batch_size,
            "bounded_example_cap": self.bounded_example_cap,
            "composite_share_class_pair_cap": self.composite_share_class_pair_cap,
            "disk_free_floor_bytes": self.disk_free_floor_bytes,
            "disk_free_warning_bytes": self.disk_free_warning_bytes,
            "distinct_composite_cap": self.distinct_composite_cap,
            "output_bytes_cap": self.output_bytes_cap,
            "resource_check_interval_batches": self.resource_check_interval_batches,
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
    def from_dict(cls, value: object) -> S7InventoryExecutionResourceCaps:
        item = _mapping(value, "v2 resource caps")
        expected = set(cls().to_dict())
        _expect_keys(item, expected, "v2 resource caps")
        return cls(**{key: _positive_int(item[key], key) for key in expected})


def inventory_algorithm_spec() -> dict[str, object]:
    """Return the exact deterministic semantics the executable runner must implement."""

    return {
        "active_count_field": "provider_active_after_required_requested_active_equality",
        "authority_table": "asset_observation_daily",
        "duplicate_key_scope": {
            "asset_source_record_id": "unique_within_each_session",
            "universe_selected_source_record_id": "unique_within_each_session",
        },
        "candidate_sort": ["observed_composite_figi_ASC_bytewise"],
        "composite_validity": "exact_12_char_upper_ascii_alnum_with_literal_BBG_prefix",
        "figi_reason_partition": {
            "composite": [f"composite_figi_{item}" for item in FIGI_REASON_PRECEDENCE],
            "precedence": list(FIGI_REASON_PRECEDENCE),
            "share_class": [f"share_class_figi_{item}" for item in FIGI_REASON_PRECEDENCE],
            "semantics": "first_matching_reason_exactly_one_or_valid_no_normalization",
        },
        "distinct_semantics": {
            "counts": "exact_distinct_raw_non_null_values_no_case_or_space_normalization",
            "observed_share_class_figis": "valid_figi_only_sorted_unique_bytewise",
            "share_class_conflict": "more_than_one_distinct_valid_share_class_figi",
        },
        "invalid_composite_policy": (
            "exclude_from_identifier_inventory_preserve_exact_reason_counts_and_bounded_examples"
        ),
        "lineage_digest": {
            "initialization": "hashlib_sha256_then_update_exact_seed_without_newline",
            "record_order": "artifact_path_asc,row_group_asc,row_index_asc",
            "rule_version": LINEAGE_DIGEST_RULE_VERSION,
            "seed": {
                "json": {
                    "parent_table": "asset_observation_daily",
                    "release_id": (
                        "26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c"
                    ),
                    "rule_version": LINEAGE_DIGEST_RULE_VERSION,
                    "scan_order": "artifact_path_asc,row_group_asc,row_index_asc",
                },
                "serialization": (
                    "json.dumps_allow_nan_false_ensure_ascii_false_"
                    "separators_comma_colon_sort_keys_true_utf8_no_newline"
                ),
            },
            "update_per_authority_occurrence": ("hash_update_bytes_fromhex_source_record_id"),
        },
        "null_distinct_value_policy": "null_values_do_not_increment_distinct_counts",
        "parent_table_count": "constant_one_asset_observation_daily_authority_only",
        "reconciliation_only_table": "universe_source_daily",
        "reconciliation_projection": list(UNIVERSE_PARENT_RECONCILIATION_PROJECTION),
        "reconciliation_projection_excludes_pair_derived_fields": list(
            UNIVERSE_PAIR_DERIVED_FIELDS_EXCLUDED_FROM_PARENT_EQUALITY
        ),
        "reconciliation_selected_key": (
            "selected_source_record_id_equals_source_record_id_within_same_session"
        ),
        "row_count_authority": "asset_observation_daily_only",
        "rule_version": INVENTORY_ALGORITHM_RULE_VERSION,
        "session_count": "exact_distinct_session_date_per_valid_composite",
        "source_release_count": "constant_one_asset_observation_daily_release_only",
        "ticker_count": "exact_distinct_raw_ticker_per_valid_composite",
        "candidate_serialization": {
            "compression": "zstd",
            "compression_level": 9,
            "data_file_count": 1,
            "data_page_version": "2.0",
            "format": "parquet",
            "parquet_version": "2.6",
            "pyarrow_version": "25.0.0",
            "row_group_size": 100_000,
            "store_schema": True,
            "use_dictionary": False,
            "use_threads": False,
            "write_statistics": True,
        },
        "universe_rows_are_inventory_observations": False,
    }


INVENTORY_ALGORITHM_DIGEST: Final = stable_digest(inventory_algorithm_spec())


def inventory_qa_semantics() -> tuple[dict[str, object], ...]:
    """Return exact Critical/High review semantics for the Gate-A candidate."""

    return tuple(rule.to_dict() for rule in COMPOSITE_FIGI_INVENTORY_CONTRACT.qa_rules)


INVENTORY_QA_SEMANTICS_DIGEST: Final = stable_digest(list(inventory_qa_semantics()))


def canonical_execution_paths() -> dict[str, str]:
    return dict(CANONICAL_EXECUTION_PATHS)


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryExecutionPlanV2:
    created_by: str
    created_at_utc: datetime
    execution_git_commit: str
    execution_git_tree: str
    execution_data_root: str
    runtime_files: tuple[S7InventoryRuntimeFilePin, ...]
    verification_files: tuple[S7InventoryRuntimeFilePin, ...]
    inventory_contract: S7InventoryCandidateContractPin
    blocked_event_id: str
    blocked_event_path: str
    blocked_event_sha256: str
    resource_caps: S7InventoryExecutionResourceCaps = S7InventoryExecutionResourceCaps()
    execution_scope: str = EXECUTION_SCOPE
    plan_state: str = EXECUTION_PLAN_STATE

    def __post_init__(self) -> None:
        _safe_text(self.created_by, "created_by")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        _git_object(self.execution_git_commit, "execution Git commit")
        _git_object(self.execution_git_tree, "execution Git tree")
        _absolute_data_root(self.execution_data_root)
        runtime = tuple(sorted(self.runtime_files))
        if not runtime or len({item.path for item in runtime}) != len(runtime):
            raise IdentityMarketInventoryExecutionPlanError("runtime file pins are incomplete")
        if any(not isinstance(item, S7InventoryRuntimeFilePin) for item in runtime):
            raise IdentityMarketInventoryExecutionPlanError("runtime file pin has wrong type")
        paths = {item.path for item in runtime}
        required = set(REQUIRED_EXECUTION_RUNTIME_PATHS) | {
            self.inventory_contract.candidate_path,
            self.inventory_contract.resource_path,
        }
        if not required.issubset(paths):
            raise IdentityMarketInventoryExecutionPlanError(
                f"runtime file set misses executable inputs: {sorted(required - paths)}"
            )
        object.__setattr__(self, "runtime_files", runtime)
        verification = tuple(sorted(self.verification_files))
        if not verification or len({item.path for item in verification}) != len(verification):
            raise IdentityMarketInventoryExecutionPlanError("verification file pins are incomplete")
        if any(not isinstance(item, S7InventoryRuntimeFilePin) for item in verification):
            raise IdentityMarketInventoryExecutionPlanError("verification file pin has wrong type")
        verification_paths = {item.path for item in verification}
        if not REQUIRED_EXECUTION_VERIFICATION_PATHS.issubset(verification_paths):
            raise IdentityMarketInventoryExecutionPlanError(
                "verification file set misses required regression tests: "
                f"{sorted(REQUIRED_EXECUTION_VERIFICATION_PATHS - verification_paths)}"
            )
        object.__setattr__(self, "verification_files", verification)
        if not isinstance(self.inventory_contract, S7InventoryCandidateContractPin):
            raise IdentityMarketInventoryExecutionPlanError("inventory contract has wrong type")
        _digest(self.blocked_event_id, "blocked event ID")
        if self.blocked_event_path != v1_execution_blocked_event_path(self.blocked_event_id):
            raise IdentityMarketInventoryExecutionPlanError("blocked event path is not canonical")
        _digest(self.blocked_event_sha256, "blocked event SHA-256")
        if not isinstance(self.resource_caps, S7InventoryExecutionResourceCaps):
            raise IdentityMarketInventoryExecutionPlanError("resource caps have wrong type")
        if self.execution_scope != EXECUTION_SCOPE or self.plan_state != EXECUTION_PLAN_STATE:
            raise IdentityMarketInventoryExecutionPlanError("v2 plan scope or state changed")

    @classmethod
    def create(
        cls,
        *,
        created_by: str,
        created_at_utc: datetime,
        execution_git_commit: str,
        execution_git_tree: str,
        execution_data_root: str,
        runtime_files: tuple[S7InventoryRuntimeFilePin, ...],
        verification_files: tuple[S7InventoryRuntimeFilePin, ...],
        inventory_contract: S7InventoryCandidateContractPin,
        blocked_event: S7V1InventoryExecutionBlockedEvent,
        blocked_event_receipt: StoredInventoryExecutionDocument,
    ) -> S7CompositeInventoryExecutionPlanV2:
        if not isinstance(blocked_event, S7V1InventoryExecutionBlockedEvent):
            raise IdentityMarketInventoryExecutionPlanError("blocked event has wrong type")
        _verify_receipt(
            blocked_event_receipt,
            blocked_event.relative_path,
            blocked_event.sha256,
            len(blocked_event.content),
        )
        created = _utc(created_at_utc, "created_at_utc")
        if created < blocked_event.recorded_at_utc:
            raise IdentityMarketInventoryExecutionPlanError("v2 plan predates blocked event")
        return cls(
            created_by=created_by,
            created_at_utc=created,
            execution_git_commit=execution_git_commit,
            execution_git_tree=execution_git_tree,
            execution_data_root=_absolute_data_root(execution_data_root),
            runtime_files=runtime_files,
            verification_files=verification_files,
            inventory_contract=inventory_contract,
            blocked_event_id=blocked_event.event_id,
            blocked_event_path=blocked_event_receipt.path,
            blocked_event_sha256=blocked_event_receipt.sha256,
        )

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

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
            "calendar_artifact_id": INVENTORY_CALENDAR_ARTIFACT_ID,
            "calendar_artifact_sha256": INVENTORY_CALENDAR_ARTIFACT_SHA256,
            "daily_physical_scan_tables": [
                "asset_observation_daily",
                "universe_source_daily",
            ],
            "daily_source_totals": {
                "artifact_count": DAILY_SOURCE_ARTIFACT_COUNT,
                "row_count": DAILY_SOURCE_ROW_COUNT,
                "stored_bytes": DAILY_SOURCE_BYTES,
            },
            "end_session": INVENTORY_END_SESSION.isoformat(),
            "s4_release_set_id": S7_S4_RELEASE_SET_ID,
            "s4_release_set_manifest_sha256": S7_S4_RELEASE_SET_MANIFEST_SHA256,
            "session_count": INVENTORY_SESSION_COUNT,
            "six_release_binding_id": S7_SIX_RELEASE_BINDING_ID,
            "source_pins": [item.to_dict() for item in EXACT_SOURCE_PINS],
            "start_session": INVENTORY_START_SESSION.isoformat(),
        }

    def _v1_lineage(self) -> dict[str, object]:
        return {
            "blocked_event_id": self.blocked_event_id,
            "blocked_event_path": self.blocked_event_path,
            "blocked_event_sha256": self.blocked_event_sha256,
            "prior_preview": {
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
            },
            "v1_controls": S7V1InventoryControlLineage().to_dict(),
        }

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "algorithm_digest": INVENTORY_ALGORITHM_DIGEST,
                "candidate_contract": self.inventory_contract.to_dict(),
                "canonical_paths": canonical_execution_paths(),
                "execution_data_root": self.execution_data_root,
                "git_binding": self._git_binding(),
                "qa_semantics_digest": INVENTORY_QA_SEMANTICS_DIGEST,
                "resource_caps": self.resource_caps.to_dict(),
                "source_binding": self._source_binding(),
                "v1_lineage": self._v1_lineage(),
                "verification_binding": self._verification_binding(),
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm": {
                "digest": INVENTORY_ALGORITHM_DIGEST,
                "rule_version": INVENTORY_ALGORITHM_RULE_VERSION,
                "semantics": inventory_algorithm_spec(),
            },
            "artifact_type": "s7_composite_inventory_execution_plan_v2",
            "candidate_contract": self.inventory_contract.to_dict(),
            "canonical_paths": canonical_execution_paths(),
            "capabilities": {
                "approval_receipt_creation_authorized_by_plan": False,
                "inventory_execution_authorized_by_plan": False,
                "market_classification_authorized": False,
                "network_access_authorized": False,
                "publication_authorized": False,
                "registry_release_authorized": False,
                "research_table_materialization_authorized": False,
            },
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "execution_scope": self.execution_scope,
            "git_binding": self._git_binding(),
            "input_binding_digest": self.input_binding_digest,
            "output_contract": {
                "actual_cardinality_unknown_until_execution": True,
                "candidate_contract_id": self.inventory_contract.contract_id,
                "candidate_schema_digest": self.inventory_contract.schema_digest,
                "contains_backtest_eligibility": False,
                "contains_canonical_identity": False,
                "contains_market_classification": False,
                "inventory_row_hard_cap": self.resource_caps.distinct_composite_cap,
                "ordered_columns": list(INVENTORY_OUTPUT_COLUMNS),
                "physical_serialization": inventory_algorithm_spec()["candidate_serialization"],
                "status_after_success": "awaiting_review",
            },
            "plan_rule_version": EXECUTION_PLAN_RULE_VERSION,
            "plan_state": self.plan_state,
            "qa": {
                "semantics": list(inventory_qa_semantics()),
                "semantics_digest": INVENTORY_QA_SEMANTICS_DIGEST,
            },
            "resource_caps": self.resource_caps.to_dict(),
            "resource_measurement": {
                "disk": "shutil_disk_usage_before_each_stable_write_and_batch",
                "disk_free_warning": (
                    "record_true_when_precommit_minimum_is_below_warning_threshold"
                ),
                "output_bytes_scope": "candidate_plus_completion_stable_bytes",
                "post_commit_cap_check": (
                    "all_hard_caps_rechecked_after_completion_link_before_success"
                ),
                "rss": "resource_getrusage_RUSAGE_SELF_ru_maxrss_platform_normalized",
                "tmp_bytes_scope": "dedicated_run_staging_tree",
                "wall_clock": ("precommit_snapshot_plus_post_completion_link_hard_cap_enforcement"),
            },
            "schema_version": EXECUTION_PLAN_SCHEMA_VERSION,
            "single_use_policy": {
                "existing_candidate_without_completion": "fail_closed",
                "existing_completion": "read_and_revalidate_without_rescan",
                "immutable_candidate_and_completion": True,
                "one_logical_run_per_plan_and_approval": True,
                "parallel_runner": "exclusive_nonblocking_plan_approval_lock",
                "stale_staging": "fail_closed_no_implicit_delete",
            },
            "source_binding": self._source_binding(),
            "v1_lineage": self._v1_lineage(),
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
        return execution_plan_v2_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryExecutionPlanV2:
        document = _mapping(value, "v2 execution plan")
        git = _mapping(document.get("git_binding"), "v2 Git binding")
        lineage = _mapping(document.get("v1_lineage"), "v1 lineage")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        plan = cls(
            created_by=_string(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            execution_git_commit=_string(git.get("execution_git_commit"), "execution Git commit"),
            execution_git_tree=_string(git.get("execution_git_tree"), "execution Git tree"),
            execution_data_root=_absolute_data_root(document.get("execution_data_root")),
            runtime_files=tuple(
                S7InventoryRuntimeFilePin.from_dict(item)
                for item in _array(git.get("runtime_files"), "runtime files")
            ),
            verification_files=tuple(
                S7InventoryRuntimeFilePin.from_dict(item)
                for item in _array(verification.get("verification_files"), "verification files")
            ),
            inventory_contract=S7InventoryCandidateContractPin.from_dict(
                document.get("candidate_contract")
            ),
            blocked_event_id=_string(lineage.get("blocked_event_id"), "blocked event ID"),
            blocked_event_path=_string(lineage.get("blocked_event_path"), "blocked event path"),
            blocked_event_sha256=_string(lineage.get("blocked_event_sha256"), "blocked event SHA"),
            resource_caps=S7InventoryExecutionResourceCaps.from_dict(document.get("resource_caps")),
            execution_scope=_string(document.get("execution_scope"), "execution scope"),
            plan_state=_string(document.get("plan_state"), "plan state"),
        )
        if _canonical_bytes(document) != plan.content:
            raise IdentityMarketInventoryExecutionPlanError(
                "v2 execution plan does not reproduce canonical bytes"
            )
        return plan


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryExecutionRequestV2:
    plan_id: str
    plan_path: str
    plan_sha256: str
    execution_data_root: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    inventory_contract_id: str
    inventory_candidate_sha256: str
    inventory_schema_digest: str
    algorithm_digest: str
    qa_semantics_digest: str
    blocked_event_id: str
    blocked_event_sha256: str
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
            ("inventory contract ID", self.inventory_contract_id),
            ("inventory candidate SHA-256", self.inventory_candidate_sha256),
            ("inventory schema digest", self.inventory_schema_digest),
            ("algorithm digest", self.algorithm_digest),
            ("QA semantics digest", self.qa_semantics_digest),
            ("blocked event ID", self.blocked_event_id),
            ("blocked event SHA-256", self.blocked_event_sha256),
        ):
            _digest(value, label)
        if self.plan_path != execution_plan_v2_path(self.plan_id):
            raise IdentityMarketInventoryExecutionPlanError("request plan path is not canonical")
        _absolute_data_root(self.execution_data_root)
        _safe_text(self.created_by, "request created_by")
        object.__setattr__(
            self,
            "created_at_utc",
            _utc(self.created_at_utc, "request created_at_utc"),
        )
        if (
            self.authorized_action != EXECUTION_AUTHORIZED_ACTION
            or self.execution_scope != EXECUTION_SCOPE
            or self.request_state != EXECUTION_REQUEST_STATE
        ):
            raise IdentityMarketInventoryExecutionPlanError("v2 request scope or state changed")

    @classmethod
    def create(
        cls,
        plan: S7CompositeInventoryExecutionPlanV2,
        plan_receipt: StoredInventoryExecutionDocument,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7CompositeInventoryExecutionRequestV2:
        if not isinstance(plan, S7CompositeInventoryExecutionPlanV2):
            raise IdentityMarketInventoryExecutionPlanError("v2 request plan has wrong type")
        _verify_receipt(plan_receipt, plan.relative_path, plan.sha256, len(plan.content))
        created = _utc(created_at_utc, "request created_at_utc")
        if created < plan.created_at_utc:
            raise IdentityMarketInventoryExecutionPlanError("v2 request predates plan")
        return cls(
            plan_id=plan.plan_id,
            plan_path=plan_receipt.path,
            plan_sha256=plan_receipt.sha256,
            execution_data_root=plan.execution_data_root,
            input_binding_digest=plan.input_binding_digest,
            resource_caps_digest=plan.resource_caps.digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            inventory_contract_id=plan.inventory_contract.contract_id,
            inventory_candidate_sha256=plan.inventory_contract.candidate_sha256,
            inventory_schema_digest=plan.inventory_contract.schema_digest,
            algorithm_digest=INVENTORY_ALGORITHM_DIGEST,
            qa_semantics_digest=INVENTORY_QA_SEMANTICS_DIGEST,
            blocked_event_id=plan.blocked_event_id,
            blocked_event_sha256=plan.blocked_event_sha256,
            created_by=created_by,
            created_at_utc=created,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm_digest": self.algorithm_digest,
            "artifact_type": "s7_composite_inventory_execution_request_v2",
            "authorized_action": self.authorized_action,
            "blocked_event_id": self.blocked_event_id,
            "blocked_event_sha256": self.blocked_event_sha256,
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "execution_scope": self.execution_scope,
            "execution_data_root": self.execution_data_root,
            "input_binding_digest": self.input_binding_digest,
            "inventory_candidate_sha256": self.inventory_candidate_sha256,
            "inventory_contract_id": self.inventory_contract_id,
            "inventory_schema_digest": self.inventory_schema_digest,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "qa_semantics_digest": self.qa_semantics_digest,
            "request_rule_version": EXECUTION_REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "verification_file_set_digest": self.verification_file_set_digest,
            "schema_version": EXECUTION_REQUEST_SCHEMA_VERSION,
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
        return execution_request_v2_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return json.dumps(
            {
                "algorithm_digest": self.algorithm_digest,
                "authorized_action": self.authorized_action,
                "blocked_event_id": self.blocked_event_id,
                "execution_data_root": self.execution_data_root,
                "input_binding_digest": self.input_binding_digest,
                "inventory_contract_id": self.inventory_contract_id,
                "inventory_schema_digest": self.inventory_schema_digest,
                "literal_version": EXECUTION_LITERAL_VERSION,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "qa_semantics_digest": self.qa_semantics_digest,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.sha256,
                "resource_caps_digest": self.resource_caps_digest,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "verification_file_set_digest": self.verification_file_set_digest,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryExecutionRequestV2:
        document = _mapping(value, "v2 execution request")
        request = cls(
            plan_id=_string(document.get("plan_id"), "plan ID"),
            plan_path=_string(document.get("plan_path"), "plan path"),
            plan_sha256=_string(document.get("plan_sha256"), "plan SHA"),
            execution_data_root=_absolute_data_root(document.get("execution_data_root")),
            input_binding_digest=_string(
                document.get("input_binding_digest"), "input binding digest"
            ),
            resource_caps_digest=_string(
                document.get("resource_caps_digest"), "resource caps digest"
            ),
            runtime_file_set_digest=_string(
                document.get("runtime_file_set_digest"), "runtime file set digest"
            ),
            verification_file_set_digest=_string(
                document.get("verification_file_set_digest"),
                "verification file set digest",
            ),
            inventory_contract_id=_string(
                document.get("inventory_contract_id"), "inventory contract ID"
            ),
            inventory_candidate_sha256=_string(
                document.get("inventory_candidate_sha256"), "candidate SHA"
            ),
            inventory_schema_digest=_string(
                document.get("inventory_schema_digest"), "inventory schema digest"
            ),
            algorithm_digest=_string(document.get("algorithm_digest"), "algorithm digest"),
            qa_semantics_digest=_string(document.get("qa_semantics_digest"), "QA semantics digest"),
            blocked_event_id=_string(document.get("blocked_event_id"), "blocked event ID"),
            blocked_event_sha256=_string(document.get("blocked_event_sha256"), "blocked event SHA"),
            created_by=_string(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            authorized_action=_string(document.get("authorized_action"), "authorized action"),
            execution_scope=_string(document.get("execution_scope"), "execution scope"),
            request_state=_string(document.get("request_state"), "request state"),
        )
        if _canonical_bytes(document) != request.content:
            raise IdentityMarketInventoryExecutionPlanError(
                "v2 execution request does not reproduce canonical bytes"
            )
        return request


class IdentityMarketInventoryExecutionPlanStore:
    """Content-addressed audit/v2 control store with no approval or runner API."""

    def __init__(self, data_root: Path) -> None:
        if not isinstance(data_root, Path):
            raise IdentityMarketInventoryExecutionPlanError("data_root must be a Path")
        expanded = data_root.expanduser()
        if expanded.is_symlink():
            raise IdentityMarketInventoryExecutionPlanError("data_root cannot be a symlink")
        self.root = expanded.resolve()
        if not self.root.is_dir():
            raise IdentityMarketInventoryExecutionPlanError("data_root must exist")

    def load_exact_v1_controls(
        self,
    ) -> tuple[S7CompositeInventoryPlan, S7CompositeInventoryApprovalRequest]:
        """Load and byte-verify the exact immutable v1 Plan/Request without mutation."""

        try:
            v1 = IdentityMarketInventoryPlanStore(self.root)
            plan, plan_receipt = v1.load_plan(V1_PLAN_ID, expected_sha256=V1_PLAN_SHA256)
            request, request_receipt = v1.load_approval_request(
                V1_REQUEST_EVENT_ID,
                expected_sha256=V1_REQUEST_EVENT_SHA256,
            )
        except IdentityMarketInventoryPlanError as exc:
            raise IdentityMarketInventoryExecutionPlanError(
                "exact immutable v1 controls cannot be loaded"
            ) from exc
        if (
            plan_receipt.path != S7V1InventoryControlLineage().plan_path
            or request_receipt.path != S7V1InventoryControlLineage().request_path
            or plan.git_commit != V1_BOUND_GIT_COMMIT
            or plan.input_binding_digest != V1_INPUT_BINDING_DIGEST
            or plan.resource_caps.digest != V1_RESOURCE_CAPS_DIGEST
            or request.plan_id != plan.plan_id
            or request.plan_sha256 != plan.sha256
            or request.canonical_approval_literal != V1_LITERAL
        ):
            raise IdentityMarketInventoryExecutionPlanError("v1 controls differ from literal")
        return plan, request

    def store_blocked_event(
        self, value: S7V1InventoryExecutionBlockedEvent
    ) -> StoredInventoryExecutionDocument:
        if not isinstance(value, S7V1InventoryExecutionBlockedEvent):
            raise IdentityMarketInventoryExecutionPlanError("blocked event has wrong type")
        _, request = self.load_exact_v1_controls()
        if value.recorded_at_utc < request.created_at_utc:
            raise IdentityMarketInventoryExecutionPlanError(
                "blocked event predates the exact received-literal request"
            )
        return self._write(value.relative_path, value.content)

    def load_blocked_event(
        self, event_id: str, *, expected_sha256: str
    ) -> tuple[S7V1InventoryExecutionBlockedEvent, StoredInventoryExecutionDocument]:
        value, receipt = self._read(
            v1_execution_blocked_event_path(event_id),
            expected_sha256,
            S7V1InventoryExecutionBlockedEvent.from_dict,
        )
        _, request = self.load_exact_v1_controls()
        if value.recorded_at_utc < request.created_at_utc:
            raise IdentityMarketInventoryExecutionPlanError(
                "blocked event predates the exact received-literal request"
            )
        return value, receipt

    def store_execution_plan_v2(
        self, value: S7CompositeInventoryExecutionPlanV2
    ) -> StoredInventoryExecutionDocument:
        if not isinstance(value, S7CompositeInventoryExecutionPlanV2):
            raise IdentityMarketInventoryExecutionPlanError("v2 plan has wrong type")
        if value.execution_data_root != str(self.root):
            raise IdentityMarketInventoryExecutionPlanError(
                "v2 plan execution_data_root differs from this immutable store"
            )
        self.load_blocked_event(
            value.blocked_event_id,
            expected_sha256=value.blocked_event_sha256,
        )
        return self._write(value.relative_path, value.content)

    def load_execution_plan_v2(
        self, plan_id: str, *, expected_sha256: str
    ) -> tuple[S7CompositeInventoryExecutionPlanV2, StoredInventoryExecutionDocument]:
        value, receipt = self._read(
            execution_plan_v2_path(plan_id),
            expected_sha256,
            S7CompositeInventoryExecutionPlanV2.from_dict,
        )
        if value.execution_data_root != str(self.root):
            raise IdentityMarketInventoryExecutionPlanError(
                "v2 plan execution_data_root differs from this immutable store"
            )
        self.load_blocked_event(
            value.blocked_event_id,
            expected_sha256=value.blocked_event_sha256,
        )
        return value, receipt

    def store_execution_request_v2(
        self, value: S7CompositeInventoryExecutionRequestV2
    ) -> StoredInventoryExecutionDocument:
        if not isinstance(value, S7CompositeInventoryExecutionRequestV2):
            raise IdentityMarketInventoryExecutionPlanError("v2 request has wrong type")
        plan, _ = self.load_execution_plan_v2(
            value.plan_id,
            expected_sha256=value.plan_sha256,
        )
        _verify_request_plan(value, plan)
        return self._write(value.relative_path, value.content)

    def load_execution_request_v2(
        self, request_event_id: str, *, expected_sha256: str
    ) -> tuple[S7CompositeInventoryExecutionRequestV2, StoredInventoryExecutionDocument]:
        value, receipt = self._read(
            execution_request_v2_path(request_event_id),
            expected_sha256,
            S7CompositeInventoryExecutionRequestV2.from_dict,
        )
        plan, _ = self.load_execution_plan_v2(
            value.plan_id,
            expected_sha256=value.plan_sha256,
        )
        _verify_request_plan(value, plan)
        return value, receipt

    def _write(self, relative: str, content: bytes) -> StoredInventoryExecutionDocument:
        try:
            path = safe_relative_path(self.root, relative)
            receipt = write_bytes_immutable(self.root, path, content)
        except ArtifactError as exc:
            raise IdentityMarketInventoryExecutionPlanError(str(exc)) from exc
        return StoredInventoryExecutionDocument(
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _read(
        self,
        relative: str,
        expected_sha256: str,
        parser: Any,
    ) -> tuple[Any, StoredInventoryExecutionDocument]:
        _digest(expected_sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityMarketInventoryExecutionPlanError(str(exc)) from exc
        if not path.is_file() or path.is_symlink():
            raise IdentityMarketInventoryExecutionPlanError(
                f"control document is missing or unsafe: {relative}"
            )
        content = path.read_bytes()
        if sha256_file(path) != expected_sha256:
            raise IdentityMarketInventoryExecutionPlanError(
                f"control document SHA-256 differs: {relative}"
            )
        document = _decode_json(content, relative)
        if _canonical_bytes(document) != content:
            raise IdentityMarketInventoryExecutionPlanError(
                f"control document is not canonical JSON: {relative}"
            )
        value = parser(document)
        if (
            value.relative_path != relative
            or value.sha256 != expected_sha256
            or value.content != content
        ):
            raise IdentityMarketInventoryExecutionPlanError(
                f"control document path or bytes differ: {relative}"
            )
        return value, StoredInventoryExecutionDocument(relative, expected_sha256, len(content))


def v1_execution_blocked_event_path(event_id: str) -> str:
    _digest(event_id, "blocked event ID")
    return (
        "manifests/silver/identity/composite-inventory-execution-blocked-events/"
        f"event_id={event_id}/manifest.json"
    )


def execution_plan_v2_path(plan_id: str) -> str:
    _digest(plan_id, "v2 plan ID")
    return (
        "manifests/silver/identity/composite-inventory-execution-plans-v2/"
        f"plan_id={plan_id}/manifest.json"
    )


def execution_request_v2_path(request_event_id: str) -> str:
    _digest(request_event_id, "v2 request event ID")
    return (
        "manifests/silver/identity/composite-inventory-execution-requests-v2/"
        f"request_event_id={request_event_id}/manifest.json"
    )


def _verify_receipt(
    receipt: StoredInventoryExecutionDocument,
    path: str,
    sha256: str,
    size: int,
) -> None:
    if not isinstance(receipt, StoredInventoryExecutionDocument) or (
        receipt.path != path or receipt.sha256 != sha256 or receipt.bytes != size
    ):
        raise IdentityMarketInventoryExecutionPlanError("stored receipt differs")


def _verify_request_plan(
    request: S7CompositeInventoryExecutionRequestV2,
    plan: S7CompositeInventoryExecutionPlanV2,
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
        or request.inventory_contract_id != plan.inventory_contract.contract_id
        or request.inventory_candidate_sha256 != plan.inventory_contract.candidate_sha256
        or request.inventory_schema_digest != plan.inventory_contract.schema_digest
        or request.algorithm_digest != INVENTORY_ALGORITHM_DIGEST
        or request.qa_semantics_digest != INVENTORY_QA_SEMANTICS_DIGEST
        or request.blocked_event_id != plan.blocked_event_id
        or request.blocked_event_sha256 != plan.blocked_event_sha256
        or request.created_at_utc < plan.created_at_utc
    ):
        raise IdentityMarketInventoryExecutionPlanError("v2 request crosses plan bindings")


__all__ = [
    "BLOCKED_EVENT_STATE",
    "CANONICAL_EXECUTION_PATHS",
    "EXECUTION_AUTHORIZED_ACTION",
    "EXECUTION_LITERAL_VERSION",
    "EXECUTION_SCOPE",
    "INVENTORY_ALGORITHM_DIGEST",
    "INVENTORY_ALGORITHM_RULE_VERSION",
    "INVENTORY_CONTRACT_CANDIDATE_PATH",
    "INVENTORY_CONTRACT_DOMAIN",
    "INVENTORY_CONTRACT_RESOURCE_PATH",
    "INVENTORY_CONTRACT_TABLE",
    "INVENTORY_QA_SEMANTICS_DIGEST",
    "REQUIRED_EXECUTION_RUNTIME_PATHS",
    "REQUIRED_EXECUTION_VERIFICATION_PATHS",
    "V1_BLOCKED_REASONS",
    "V1_BOUND_GIT_COMMIT",
    "V1_BOUND_GIT_TREE",
    "V1_LITERAL",
    "V1_LITERAL_SHA256",
    "V1_PLAN_ID",
    "V1_PLAN_SHA256",
    "V1_REQUEST_EVENT_ID",
    "V1_REQUEST_EVENT_SHA256",
    "IdentityMarketInventoryExecutionPlanError",
    "IdentityMarketInventoryExecutionPlanStore",
    "S7CompositeInventoryExecutionPlanV2",
    "S7CompositeInventoryExecutionRequestV2",
    "S7InventoryCandidateContractPin",
    "S7InventoryExecutionResourceCaps",
    "S7InventoryRuntimeFilePin",
    "S7V1InventoryControlLineage",
    "S7V1InventoryExecutionBlockedEvent",
    "StoredInventoryExecutionDocument",
    "canonical_execution_paths",
    "execution_plan_v2_path",
    "execution_request_v2_path",
    "inventory_algorithm_spec",
    "inventory_qa_semantics",
    "v1_execution_blocked_event_path",
]
