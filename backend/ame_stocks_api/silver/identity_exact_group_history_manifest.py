"""Manifest-only control plane for the S7 three-group history review.

The only data-root operation authorized by this module is one exact preflight:
seven pinned JSON documents are read and the 5,026 source Parquet paths are
checked with ``lstat``.  Parquet bytes are never opened.  A successful run
produces a raw 16-field source binding, its normalized 10-field execution pins,
a future execution Plan/Request, and an awaiting-review completion.  The future
request still requires a separate exact human approval.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import resource
import stat
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
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
from ame_stocks_api.silver.identity_exact_group_history_plan import (
    ASSET_RELEASE_ID,
    ASSET_RELEASE_SHA256,
    CALENDAR_ARTIFACT_ID,
    CALENDAR_ARTIFACT_SHA256,
    DEFAULT_RUNTIME_PATHS,
    DEFAULT_VERIFICATION_PATHS,
    DIRECTIONAL_CANDIDATE_ID,
    DIRECTIONAL_CANDIDATE_SHA256,
    DIRECTIONAL_COMPLETION_ID,
    DIRECTIONAL_COMPLETION_SHA256,
    END_SESSION,
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_COMPLETION_SHA256,
    INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    PREPARATION_AUTHORIZED_ACTION,
    PREPARATION_LITERAL_VERSION,
    SESSION_COUNT,
    SOURCE_ARTIFACT_COUNT,
    SOURCE_BYTES,
    SOURCE_ROW_COUNT,
    START_SESSION,
    UNIVERSE_RELEASE_ID,
    UNIVERSE_RELEASE_SHA256,
    ExactGroupHistoryFilePin,
    ExactGroupHistoryPlanStore,
    IdentityExactGroupHistoryPlanError,
    S7ExactGroupHistoryExecutionCaps,
    S7ExactGroupHistoryPreparationPlan,
    S7ExactGroupHistoryPreparationRequest,
    S7ExactGroupHistoryScopeSet,
    canonical_bytes,
    verify_preparation_request_binding,
)
from ame_stocks_api.silver.store import ArtifactRole, ReleaseManifest

MANIFEST_PLAN_RULE_VERSION: Final = "s7_exact_group_history_manifest_preflight_plan_v1"
MANIFEST_REQUEST_RULE_VERSION: Final = "s7_exact_group_history_manifest_preflight_request_v1"
MANIFEST_APPROVAL_RULE_VERSION: Final = "s7_exact_group_history_manifest_preflight_approval_v1"
MANIFEST_RUN_RULE_VERSION: Final = "s7_exact_group_history_manifest_preflight_run_v1"
PREPARATION_AUTHORIZATION_RULE_VERSION: Final = (
    "s7_exact_group_history_preparation_authorization_slot_v1"
)
MANIFEST_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_three_group_history_manifest_only_source_binding_preflight_"
    "once_to_awaiting_review"
)
MANIFEST_LITERAL_VERSION: Final = "s7_exact_group_history_manifest_preflight_literal_v1"
EXECUTION_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_three_group_full_s4_history_once_to_awaiting_review"
)
EXECUTION_LITERAL_VERSION: Final = "s7_exact_group_history_execution_literal_v1"
EXECUTION_DATA_ROOT: Final = "/mnt/HC_Volume_106309665/american_stocks"
EXPECTED_MANIFEST_INPUT_COUNT: Final = 7
EXPECTED_FUTURE_OUTPUT_COUNT: Final = 5

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_SESSION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_TABLES = ("asset_observation_daily", "universe_source_daily")
_CONTRACTS = MappingProxyType(
    {
        "asset_observation_daily": ASSET_OBSERVATION_DAILY_CONTRACT,
        "universe_source_daily": UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
)


class IdentityExactGroupHistoryManifestError(RuntimeError):
    """Raised when a manifest-only control or operation crosses its boundary."""


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityExactGroupHistoryManifestError(f"{label} must be an object")
    return dict(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityExactGroupHistoryManifestError(
            f"{label} schema differs: missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be non-empty text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise IdentityExactGroupHistoryManifestError(f"{label} contains controls")
    return value


def _review_note(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) > 2_000 or value.strip() != value:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be bounded canonical text")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise IdentityExactGroupHistoryManifestError(f"{label} contains controls")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityExactGroupHistoryManifestError(f"{label} must be lowercase 64-hex")
    return text


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if not _GIT_OBJECT.fullmatch(text):
        raise IdentityExactGroupHistoryManifestError(f"{label} must be lowercase 40-hex")
    return text


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be a positive native int")
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be a nonnegative native int")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be a native bool")
    return value


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise IdentityExactGroupHistoryManifestError(f"{label} is not a safe relative path")
    return text


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(_text(value, label))
    except ValueError as exc:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be ISO-8601") from exc
    normalized = _utc(result, label)
    if normalized.isoformat() != value:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be canonical UTC")
    return normalized


def _date(value: object, label: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(_text(value, label))
    except ValueError as exc:
        raise IdentityExactGroupHistoryManifestError(f"{label} must be an ISO date") from exc


def _false_capabilities() -> dict[str, bool]:
    return {
        "adjudication": False,
        "canonical_identity_output": False,
        "full_run": False,
        "network_access": False,
        "parquet_content_read": False,
        "publication": False,
        "registry_evaluation": False,
    }


def _require_default_manifest_file_pin_paths(
    runtime_files: tuple[ExactGroupHistoryFilePin, ...],
    verification_files: tuple[ExactGroupHistoryFilePin, ...],
) -> None:
    runtime_paths = {item.path for item in runtime_files}
    verification_paths = {item.path for item in verification_files}
    missing_runtime = sorted(DEFAULT_RUNTIME_PATHS - runtime_paths)
    missing_verification = sorted(DEFAULT_VERIFICATION_PATHS - verification_paths)
    if missing_runtime or missing_verification:
        raise IdentityExactGroupHistoryManifestError(
            "required manifest file pins are missing: "
            f"runtime={missing_runtime}, verification={missing_verification}"
        )


@dataclass(frozen=True, slots=True, order=True)
class S7ExactGroupHistoryManifestInputPin:
    kind: str
    logical_id: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        if self.kind not in {
            "asset_release_manifest",
            "universe_release_manifest",
            "inventory_candidate_manifest",
            "inventory_completion_manifest",
            "directional_candidate_manifest",
            "directional_completion_manifest",
            "xnys_calendar_manifest",
        }:
            raise IdentityExactGroupHistoryManifestError("manifest input kind differs")
        _digest(self.logical_id, "manifest logical ID")
        _relative(self.path, "manifest path")
        _digest(self.sha256, "manifest SHA-256")

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "logical_id": self.logical_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestInputPin:
        item = _mapping(value, "manifest input")
        _expect_keys(item, {"kind", "logical_id", "path", "sha256"}, "manifest input")
        return cls(**{key: _text(item[key], key) for key in item})


def canonical_manifest_inputs(
    *,
    inventory_completion_path: str,
    directional_completion_path: str,
) -> tuple[S7ExactGroupHistoryManifestInputPin, ...]:
    values = (
        S7ExactGroupHistoryManifestInputPin(
            "asset_release_manifest",
            ASSET_RELEASE_ID,
            f"manifests/silver/releases/release_id={ASSET_RELEASE_ID}.json",
            ASSET_RELEASE_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "universe_release_manifest",
            UNIVERSE_RELEASE_ID,
            f"manifests/silver/releases/release_id={UNIVERSE_RELEASE_ID}.json",
            UNIVERSE_RELEASE_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "inventory_candidate_manifest",
            INVENTORY_CANDIDATE_ID,
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json",
            INVENTORY_CANDIDATE_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "inventory_completion_manifest",
            INVENTORY_COMPLETION_ID,
            inventory_completion_path,
            INVENTORY_COMPLETION_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "directional_candidate_manifest",
            DIRECTIONAL_CANDIDATE_ID,
            "manifests/silver/identity/directional-raw-preview-candidates/"
            f"candidate_id={DIRECTIONAL_CANDIDATE_ID}/manifest.json",
            DIRECTIONAL_CANDIDATE_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "directional_completion_manifest",
            DIRECTIONAL_COMPLETION_ID,
            directional_completion_path,
            DIRECTIONAL_COMPLETION_SHA256,
        ),
        S7ExactGroupHistoryManifestInputPin(
            "xnys_calendar_manifest",
            CALENDAR_ARTIFACT_ID,
            f"manifests/silver/xnys-calendars/calendar_artifact_id={CALENDAR_ARTIFACT_ID}.json",
            CALENDAR_ARTIFACT_SHA256,
        ),
    )
    return _validate_manifest_inputs(values)


def _validate_manifest_inputs(
    values: tuple[S7ExactGroupHistoryManifestInputPin, ...],
) -> tuple[S7ExactGroupHistoryManifestInputPin, ...]:
    pins = tuple(sorted(values))
    if len(pins) != EXPECTED_MANIFEST_INPUT_COUNT or len({item.kind for item in pins}) != len(pins):
        raise IdentityExactGroupHistoryManifestError("exactly seven manifest inputs are required")
    expected = {
        "asset_release_manifest": (ASSET_RELEASE_ID, ASSET_RELEASE_SHA256),
        "universe_release_manifest": (UNIVERSE_RELEASE_ID, UNIVERSE_RELEASE_SHA256),
        "inventory_candidate_manifest": (INVENTORY_CANDIDATE_ID, INVENTORY_CANDIDATE_SHA256),
        "inventory_completion_manifest": (INVENTORY_COMPLETION_ID, INVENTORY_COMPLETION_SHA256),
        "directional_candidate_manifest": (
            DIRECTIONAL_CANDIDATE_ID,
            DIRECTIONAL_CANDIDATE_SHA256,
        ),
        "directional_completion_manifest": (
            DIRECTIONAL_COMPLETION_ID,
            DIRECTIONAL_COMPLETION_SHA256,
        ),
        "xnys_calendar_manifest": (CALENDAR_ARTIFACT_ID, CALENDAR_ARTIFACT_SHA256),
    }
    if {item.kind: (item.logical_id, item.sha256) for item in pins} != expected:
        raise IdentityExactGroupHistoryManifestError("manifest input lineage differs")
    by_kind = {item.kind: item for item in pins}
    fixed_paths = {
        "asset_release_manifest": (f"manifests/silver/releases/release_id={ASSET_RELEASE_ID}.json"),
        "universe_release_manifest": (
            f"manifests/silver/releases/release_id={UNIVERSE_RELEASE_ID}.json"
        ),
        "inventory_candidate_manifest": (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
        ),
        "directional_candidate_manifest": (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            f"candidate_id={DIRECTIONAL_CANDIDATE_ID}/manifest.json"
        ),
        "xnys_calendar_manifest": (
            f"manifests/silver/xnys-calendars/calendar_artifact_id={CALENDAR_ARTIFACT_ID}.json"
        ),
    }
    if any(by_kind[kind].path != path for kind, path in fixed_paths.items()):
        raise IdentityExactGroupHistoryManifestError("manifest input path differs")
    completion_patterns = {
        "inventory_completion_manifest": re.compile(
            r"^manifests/silver/identity/composite-inventory-execution-completions/"
            r"plan_id=[0-9a-f]{64}/approval_id=[0-9a-f]{64}/manifest\.json$"
        ),
        "directional_completion_manifest": re.compile(
            r"^manifests/silver/identity/directional-raw-preview-execution-completions/"
            r"plan_id=[0-9a-f]{64}/approval_id=[0-9a-f]{64}/manifest\.json$"
        ),
    }
    if any(
        pattern.fullmatch(by_kind[kind].path) is None
        for kind, pattern in completion_patterns.items()
    ):
        raise IdentityExactGroupHistoryManifestError("completion manifest path differs")
    return pins


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryPreparationAuthorization:
    plan_id: str
    plan_sha256: str
    request_event_id: str
    request_event_sha256: str
    approval_literal: str
    approved_by: str
    approved_at_utc: datetime
    review_note: str = ""

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA-256", self.request_event_sha256),
        ):
            _digest(value, label)
        _text(self.approval_literal, "preparation approval literal")
        _text(self.approved_by, "preparation approver")
        _review_note(self.review_note, "preparation review note")
        object.__setattr__(self, "approved_at_utc", _utc(self.approved_at_utc, "approved_at_utc"))
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityExactGroupHistoryManifestError(
                "preparation approval cannot be in the future"
            )

    @property
    def approval_literal_sha256(self) -> str:
        return hashlib.sha256(self.approval_literal.encode()).hexdigest()

    def slot_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_preparation_authorization_slot",
            "authorized_action": PREPARATION_AUTHORIZED_ACTION,
            "literal_version": PREPARATION_LITERAL_VERSION,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "rule_version": PREPARATION_AUTHORIZATION_RULE_VERSION,
            "approval_literal_sha256": self.approval_literal_sha256,
        }

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approved_at_utc": self.approved_at_utc.isoformat(),
            "approved_by": self.approved_by,
            "artifact_type": "s7_exact_group_history_preparation_authorization",
            "authorized_action": PREPARATION_AUTHORIZED_ACTION,
            "capabilities": _false_capabilities(),
            "literal_version": PREPARATION_LITERAL_VERSION,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "review_note": self.review_note,
            "rule_version": PREPARATION_AUTHORIZATION_RULE_VERSION,
            "schema_version": 1,
        }

    @property
    def authorization_id(self) -> str:
        return stable_digest(self.slot_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "authorization_id": self.authorization_id}
        )

    @property
    def content(self) -> bytes:
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return preparation_authorization_path(self.authorization_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryPreparationAuthorization:
        document = _mapping(value, "preparation authorization")
        result = cls(
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            request_event_id=_text(document.get("request_event_id"), "request event ID"),
            request_event_sha256=_text(document.get("request_event_sha256"), "request event SHA"),
            approval_literal=_text(document.get("approval_literal"), "approval literal"),
            approved_by=_text(document.get("approved_by"), "approved_by"),
            approved_at_utc=_parse_utc(document.get("approved_at_utc"), "approved_at_utc"),
            review_note=_review_note(document.get("review_note"), "review note"),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError(
                "preparation authorization is not canonical"
            )
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestCaps:
    json_manifest_bytes_hard_cap: int = 128 * 1024 * 1024
    source_artifact_count_hard_cap: int = SOURCE_ARTIFACT_COUNT
    source_bytes_hard_cap: int = SOURCE_BYTES
    source_rows_hard_cap: int = SOURCE_ROW_COUNT
    lstat_hard_cap: int = SOURCE_ARTIFACT_COUNT
    rss_bytes_hard_cap: int = 2 * 1024 * 1024 * 1024
    wall_clock_seconds_hard_cap: int = 1_800

    def __post_init__(self) -> None:
        for label, value in self.to_dict().items():
            _positive(value, label)
        if self.to_dict() != {
            "json_manifest_bytes_hard_cap": 128 * 1024 * 1024,
            "lstat_hard_cap": SOURCE_ARTIFACT_COUNT,
            "rss_bytes_hard_cap": 2 * 1024 * 1024 * 1024,
            "source_artifact_count_hard_cap": SOURCE_ARTIFACT_COUNT,
            "source_bytes_hard_cap": SOURCE_BYTES,
            "source_rows_hard_cap": SOURCE_ROW_COUNT,
            "wall_clock_seconds_hard_cap": 1_800,
        }:
            raise IdentityExactGroupHistoryManifestError("source caps are not exact")

    def to_dict(self) -> dict[str, int]:
        return {
            "json_manifest_bytes_hard_cap": self.json_manifest_bytes_hard_cap,
            "lstat_hard_cap": self.lstat_hard_cap,
            "rss_bytes_hard_cap": self.rss_bytes_hard_cap,
            "source_artifact_count_hard_cap": self.source_artifact_count_hard_cap,
            "source_bytes_hard_cap": self.source_bytes_hard_cap,
            "source_rows_hard_cap": self.source_rows_hard_cap,
            "wall_clock_seconds_hard_cap": self.wall_clock_seconds_hard_cap,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestCaps:
        item = _mapping(value, "manifest caps")
        return cls(**{key: _positive(raw, key) for key, raw in item.items()})


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestPlan:
    created_by: str
    created_at_utc: datetime
    execution_data_root: str
    git_commit: str
    git_tree: str
    runtime_files: tuple[ExactGroupHistoryFilePin, ...]
    verification_files: tuple[ExactGroupHistoryFilePin, ...]
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    execution_resource_caps_digest: str
    preparation_authorization_id: str
    preparation_authorization_sha256: str
    preparation_authorization_path: str
    preparation_plan_id: str
    preparation_plan_sha256: str
    preparation_request_event_id: str
    preparation_request_event_sha256: str
    manifest_inputs: tuple[S7ExactGroupHistoryManifestInputPin, ...]
    future_manifest_reader_actor: str
    future_execution_plan_actor: str
    future_execution_request_actor: str
    resource_caps: S7ExactGroupHistoryManifestCaps = S7ExactGroupHistoryManifestCaps()

    def __post_init__(self) -> None:
        _text(self.created_by, "manifest plan actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        if self.execution_data_root != EXECUTION_DATA_ROOT:
            raise IdentityExactGroupHistoryManifestError("execution data root differs")
        _git_object(self.git_commit, "Git commit")
        _git_object(self.git_tree, "Git tree")
        for label, value in (
            ("scope set ID", self.scope_set_id),
            ("scope set SHA-256", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA-256", self.contract_candidate_sha256),
            ("execution resource caps digest", self.execution_resource_caps_digest),
            ("preparation authorization ID", self.preparation_authorization_id),
            ("preparation authorization SHA-256", self.preparation_authorization_sha256),
            ("preparation plan ID", self.preparation_plan_id),
            ("preparation plan SHA-256", self.preparation_plan_sha256),
            ("preparation request event ID", self.preparation_request_event_id),
            ("preparation request event SHA-256", self.preparation_request_event_sha256),
        ):
            _digest(value, label)
        _relative(self.preparation_authorization_path, "authorization path")
        if self.execution_resource_caps_digest != S7ExactGroupHistoryExecutionCaps().digest:
            raise IdentityExactGroupHistoryManifestError("execution resource caps digest differs")
        runtime = tuple(sorted(self.runtime_files))
        verification = tuple(sorted(self.verification_files))
        if not runtime or not verification:
            raise IdentityExactGroupHistoryManifestError("manifest file pins are empty")
        if {item.path for item in runtime} & {item.path for item in verification}:
            raise IdentityExactGroupHistoryManifestError("manifest file pin paths overlap")
        _require_default_manifest_file_pin_paths(runtime, verification)
        object.__setattr__(self, "runtime_files", runtime)
        object.__setattr__(self, "verification_files", verification)
        object.__setattr__(self, "manifest_inputs", _validate_manifest_inputs(self.manifest_inputs))
        actors = (
            self.created_by,
            _text(self.future_manifest_reader_actor, "manifest reader actor"),
            _text(self.future_execution_plan_actor, "execution plan actor"),
            _text(self.future_execution_request_actor, "execution request actor"),
        )
        if len(set(actors)) != 4:
            raise IdentityExactGroupHistoryManifestError("manifest actors must be distinct")

    @property
    def runtime_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.runtime_files])

    @property
    def verification_file_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.verification_files])

    @property
    def manifest_input_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.manifest_inputs])

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "contract": [
                    self.contract_id,
                    self.contract_schema_digest,
                    self.contract_candidate_sha256,
                ],
                "manifest_input_set_digest": self.manifest_input_set_digest,
                "preparation_authorization": [
                    self.preparation_authorization_id,
                    self.preparation_authorization_sha256,
                ],
                "scope": [self.scope_set_id, self.scope_set_sha256],
                "execution_resource_caps_digest": self.execution_resource_caps_digest,
            }
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_manifest_preflight_plan",
            "authorized_action": MANIFEST_AUTHORIZED_ACTION,
            "capabilities_before_exact_literal": {
                **_false_capabilities(),
                "approval_receipt_creation": False,
                "manifest_read": False,
                "source_binding_creation": False,
                "source_lstat": False,
            },
            "contract": {
                "candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "schema_digest": self.contract_schema_digest,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "future_actors": {
                "execution_plan": self.future_execution_plan_actor,
                "execution_request": self.future_execution_request_actor,
                "manifest_reader": self.future_manifest_reader_actor,
            },
            "future_outputs": {
                "artifact_count": EXPECTED_FUTURE_OUTPUT_COUNT,
                "artifacts": [
                    "manifest_preflight_run_intent",
                    "source_binding",
                    "future_execution_plan",
                    "future_execution_request",
                    "manifest_preflight_completion",
                ],
                "state": "awaiting_review",
            },
            "git_binding": {
                "git_commit": self.git_commit,
                "git_tree": self.git_tree,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "runtime_files": [item.to_dict() for item in self.runtime_files],
            },
            "input_binding_digest": self.input_binding_digest,
            "manifest_input_set_digest": self.manifest_input_set_digest,
            "manifest_inputs": [item.to_dict() for item in self.manifest_inputs],
            "plan_rule_version": MANIFEST_PLAN_RULE_VERSION,
            "plan_state": "awaiting_exact_manifest_only_approval",
            "execution_resource_caps_digest": self.execution_resource_caps_digest,
            "preparation": {
                "authorization_id": self.preparation_authorization_id,
                "authorization_path": self.preparation_authorization_path,
                "authorization_sha256": self.preparation_authorization_sha256,
                "plan_id": self.preparation_plan_id,
                "plan_sha256": self.preparation_plan_sha256,
                "request_event_id": self.preparation_request_event_id,
                "request_event_sha256": self.preparation_request_event_sha256,
            },
            "resource_caps": self.resource_caps.to_dict(),
            "schema_version": 1,
            "scope_binding": {
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
            },
            "verification_binding": {
                "verification_file_set_digest": self.verification_file_set_digest,
                "verification_files": [item.to_dict() for item in self.verification_files],
            },
        }

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "plan_id": self.plan_id})

    @property
    def content(self) -> bytes:
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return manifest_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestPlan:
        document = _mapping(value, "manifest plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        verification = _mapping(document.get("verification_binding"), "verification binding")
        scope = _mapping(document.get("scope_binding"), "scope binding")
        contract = _mapping(document.get("contract"), "contract")
        preparation = _mapping(document.get("preparation"), "preparation")
        actors = _mapping(document.get("future_actors"), "future actors")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            execution_data_root=_text(document.get("execution_data_root"), "execution data root"),
            git_commit=_text(git.get("git_commit"), "Git commit"),
            git_tree=_text(git.get("git_tree"), "Git tree"),
            runtime_files=tuple(
                ExactGroupHistoryFilePin.from_dict(item) for item in git.get("runtime_files", [])
            ),
            verification_files=tuple(
                ExactGroupHistoryFilePin.from_dict(item)
                for item in verification.get("verification_files", [])
            ),
            scope_set_id=_text(scope.get("scope_set_id"), "scope set ID"),
            scope_set_sha256=_text(scope.get("scope_set_sha256"), "scope set SHA"),
            contract_id=_text(contract.get("contract_id"), "contract ID"),
            contract_schema_digest=_text(contract.get("schema_digest"), "schema digest"),
            contract_candidate_sha256=_text(contract.get("candidate_sha256"), "candidate SHA-256"),
            execution_resource_caps_digest=_text(
                document.get("execution_resource_caps_digest"),
                "execution resource caps digest",
            ),
            preparation_authorization_id=_text(
                preparation.get("authorization_id"), "authorization ID"
            ),
            preparation_authorization_sha256=_text(
                preparation.get("authorization_sha256"), "authorization SHA"
            ),
            preparation_authorization_path=_text(
                preparation.get("authorization_path"), "authorization path"
            ),
            preparation_plan_id=_text(preparation.get("plan_id"), "preparation plan ID"),
            preparation_plan_sha256=_text(preparation.get("plan_sha256"), "preparation plan SHA"),
            preparation_request_event_id=_text(
                preparation.get("request_event_id"), "preparation request ID"
            ),
            preparation_request_event_sha256=_text(
                preparation.get("request_event_sha256"), "preparation request SHA"
            ),
            manifest_inputs=tuple(
                S7ExactGroupHistoryManifestInputPin.from_dict(item)
                for item in document.get("manifest_inputs", [])
            ),
            future_manifest_reader_actor=_text(actors.get("manifest_reader"), "reader actor"),
            future_execution_plan_actor=_text(actors.get("execution_plan"), "execution plan actor"),
            future_execution_request_actor=_text(
                actors.get("execution_request"), "execution request actor"
            ),
            resource_caps=S7ExactGroupHistoryManifestCaps.from_dict(document.get("resource_caps")),
        )
        _require_default_manifest_file_pin_paths(result.runtime_files, result.verification_files)
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("manifest plan is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestRequest:
    created_by: str
    created_at_utc: datetime
    plan_id: str
    plan_sha256: str
    input_binding_digest: str
    manifest_input_set_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    execution_resource_caps_digest: str
    execution_data_root: str
    future_manifest_reader_actor: str
    future_execution_plan_actor: str
    future_execution_request_actor: str

    def __post_init__(self) -> None:
        _text(self.created_by, "manifest request actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("manifest input set digest", self.manifest_input_set_digest),
            ("runtime file set digest", self.runtime_file_set_digest),
            ("verification file set digest", self.verification_file_set_digest),
            ("execution resource caps digest", self.execution_resource_caps_digest),
        ):
            _digest(value, label)
        if self.execution_data_root != EXECUTION_DATA_ROOT:
            raise IdentityExactGroupHistoryManifestError("execution data root differs")
        actors = (
            self.created_by,
            _text(self.future_manifest_reader_actor, "reader actor"),
            _text(self.future_execution_plan_actor, "plan actor"),
            _text(self.future_execution_request_actor, "request actor"),
        )
        if len(set(actors)) != 4:
            raise IdentityExactGroupHistoryManifestError("request actors must be distinct")

    @classmethod
    def create(
        cls,
        plan: S7ExactGroupHistoryManifestPlan,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7ExactGroupHistoryManifestRequest:
        if (
            created_by
            in {
                plan.created_by,
                plan.future_manifest_reader_actor,
                plan.future_execution_plan_actor,
                plan.future_execution_request_actor,
            }
            or created_at_utc <= plan.created_at_utc
        ):
            raise IdentityExactGroupHistoryManifestError("manifest request actor/time differs")
        return cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            input_binding_digest=plan.input_binding_digest,
            manifest_input_set_digest=plan.manifest_input_set_digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            execution_resource_caps_digest=plan.execution_resource_caps_digest,
            execution_data_root=plan.execution_data_root,
            future_manifest_reader_actor=plan.future_manifest_reader_actor,
            future_execution_plan_actor=plan.future_execution_plan_actor,
            future_execution_request_actor=plan.future_execution_request_actor,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_manifest_preflight_request",
            "authorized_action": MANIFEST_AUTHORIZED_ACTION,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "execution_resource_caps_digest": self.execution_resource_caps_digest,
            "expected_manifest_input_count": EXPECTED_MANIFEST_INPUT_COUNT,
            "expected_output_json_count": EXPECTED_FUTURE_OUTPUT_COUNT,
            "future_execution_plan_actor": self.future_execution_plan_actor,
            "future_execution_request_actor": self.future_execution_request_actor,
            "future_manifest_reader_actor": self.future_manifest_reader_actor,
            "input_binding_digest": self.input_binding_digest,
            "literal_version": MANIFEST_LITERAL_VERSION,
            "manifest_input_set_digest": self.manifest_input_set_digest,
            "parquet_content_read": False,
            "parquet_lstat_count": SOURCE_ARTIFACT_COUNT,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_rule_version": MANIFEST_REQUEST_RULE_VERSION,
            "request_state": "awaiting_literal_human_approval",
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": 1,
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return manifest_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        excluded = {
            "artifact_type",
            "created_at_utc",
            "created_by",
            "request_rule_version",
            "request_state",
            "schema_version",
        }
        payload = {key: value for key, value in self.document.items() if key not in excluded}
        payload["request_event_sha256"] = self.sha256
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestRequest:
        document = _mapping(value, "manifest request")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            input_binding_digest=_text(
                document.get("input_binding_digest"), "input binding digest"
            ),
            manifest_input_set_digest=_text(
                document.get("manifest_input_set_digest"), "manifest input set digest"
            ),
            runtime_file_set_digest=_text(
                document.get("runtime_file_set_digest"), "runtime set digest"
            ),
            verification_file_set_digest=_text(
                document.get("verification_file_set_digest"), "verification set digest"
            ),
            execution_resource_caps_digest=_text(
                document.get("execution_resource_caps_digest"),
                "execution resource caps digest",
            ),
            execution_data_root=_text(document.get("execution_data_root"), "execution data root"),
            future_manifest_reader_actor=_text(
                document.get("future_manifest_reader_actor"), "reader actor"
            ),
            future_execution_plan_actor=_text(
                document.get("future_execution_plan_actor"), "plan actor"
            ),
            future_execution_request_actor=_text(
                document.get("future_execution_request_actor"), "request actor"
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("manifest request is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestApproval:
    plan_id: str
    plan_sha256: str
    request_event_id: str
    request_event_sha256: str
    approval_literal: str
    approved_by: str
    approved_at_utc: datetime
    review_note: str = ""

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("request ID", self.request_event_id),
            ("request SHA-256", self.request_event_sha256),
        ):
            _digest(value, label)
        _text(self.approval_literal, "approval literal")
        _text(self.approved_by, "approval actor")
        _review_note(self.review_note, "manifest review note")
        object.__setattr__(self, "approved_at_utc", _utc(self.approved_at_utc, "approved_at_utc"))
        if self.approved_at_utc > datetime.now(UTC):
            raise IdentityExactGroupHistoryManifestError(
                "manifest approval cannot be in the future"
            )

    @property
    def approval_literal_sha256(self) -> str:
        return hashlib.sha256(self.approval_literal.encode()).hexdigest()

    def slot_payload(self) -> dict[str, object]:
        return {
            "approval_literal_sha256": self.approval_literal_sha256,
            "artifact_type": "s7_exact_group_history_manifest_preflight_approval_slot",
            "authorized_action": MANIFEST_AUTHORIZED_ACTION,
            "literal_version": MANIFEST_LITERAL_VERSION,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "rule_version": MANIFEST_APPROVAL_RULE_VERSION,
        }

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal": self.approval_literal,
            "approval_literal_sha256": self.approval_literal_sha256,
            "approved_at_utc": self.approved_at_utc.isoformat(),
            "approved_by": self.approved_by,
            "artifact_type": "s7_exact_group_history_manifest_preflight_approval",
            "authorized_action": MANIFEST_AUTHORIZED_ACTION,
            "capabilities": {
                **_false_capabilities(),
                "manifest_read": True,
                "source_binding_creation": True,
                "source_lstat": True,
            },
            "literal_version": MANIFEST_LITERAL_VERSION,
            "once_to_awaiting_review": True,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "review_note": self.review_note,
            "rule_version": MANIFEST_APPROVAL_RULE_VERSION,
            "schema_version": 1,
        }

    @property
    def approval_id(self) -> str:
        return stable_digest(self.slot_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "approval_id": self.approval_id})

    @property
    def content(self) -> bytes:
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return manifest_approval_path(self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestApproval:
        document = _mapping(value, "manifest approval")
        result = cls(
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            request_event_id=_text(document.get("request_event_id"), "request ID"),
            request_event_sha256=_text(document.get("request_event_sha256"), "request SHA"),
            approval_literal=_text(document.get("approval_literal"), "approval literal"),
            approved_by=_text(document.get("approved_by"), "approved_by"),
            approved_at_utc=_parse_utc(document.get("approved_at_utc"), "approved_at_utc"),
            review_note=_review_note(document.get("review_note"), "review note"),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("manifest approval is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestRunIntent:
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
            ("manifest plan SHA", self.manifest_plan_sha256),
            ("manifest request ID", self.manifest_request_event_id),
            ("manifest request SHA", self.manifest_request_event_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA", self.manifest_approval_sha256),
            ("approval literal SHA", self.approval_literal_sha256),
            ("input binding digest", self.input_binding_digest),
        ):
            _digest(value, label)
        if self.execution_data_root != EXECUTION_DATA_ROOT:
            raise IdentityExactGroupHistoryManifestError("run-intent data root differs")
        actors = (
            _text(self.source_binding_created_by, "binding actor"),
            _text(self.execution_plan_created_by, "plan actor"),
            _text(self.execution_request_created_by, "request actor"),
        )
        if len(set(actors)) != 3:
            raise IdentityExactGroupHistoryManifestError("run-intent actors overlap")
        object.__setattr__(
            self,
            "source_binding_created_at_utc",
            _utc(self.source_binding_created_at_utc, "source_binding_created_at_utc"),
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_literal_sha256": self.approval_literal_sha256,
            "artifact_type": "s7_exact_group_history_manifest_preflight_run_intent",
            "capabilities": _false_capabilities(),
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
            "rule_version": MANIFEST_RUN_RULE_VERSION,
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return manifest_run_intent_path(self.manifest_plan_id, self.manifest_approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestRunIntent:
        document = _mapping(value, "run intent")
        result = cls(
            manifest_plan_id=_text(document.get("manifest_plan_id"), "plan ID"),
            manifest_plan_sha256=_text(document.get("manifest_plan_sha256"), "plan SHA"),
            manifest_request_event_id=_text(
                document.get("manifest_request_event_id"), "request ID"
            ),
            manifest_request_event_sha256=_text(
                document.get("manifest_request_event_sha256"), "request SHA"
            ),
            manifest_approval_id=_text(document.get("manifest_approval_id"), "approval ID"),
            manifest_approval_sha256=_text(
                document.get("manifest_approval_sha256"), "approval SHA"
            ),
            approval_literal_sha256=_text(document.get("approval_literal_sha256"), "literal SHA"),
            input_binding_digest=_text(document.get("input_binding_digest"), "input binding"),
            execution_data_root=_text(document.get("execution_data_root"), "data root"),
            source_binding_created_by=_text(
                document.get("source_binding_created_by"), "binding actor"
            ),
            source_binding_created_at_utc=_parse_utc(
                document.get("source_binding_created_at_utc"), "binding time"
            ),
            execution_plan_created_by=_text(
                document.get("execution_plan_created_by"), "plan actor"
            ),
            execution_request_created_by=_text(
                document.get("execution_request_created_by"), "request actor"
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("run intent is not canonical")
        return result


@dataclass(frozen=True, slots=True, order=True)
class S7ExactGroupHistoryRawSourceArtifactRef:
    """Raw 16-field release-manifest plus lstat projection."""

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
            raise IdentityExactGroupHistoryManifestError("source table differs")
        session = _date(self.session_date, "source session")
        if session < START_SESSION or session > END_SESSION:
            raise IdentityExactGroupHistoryManifestError("source session is outside release")
        object.__setattr__(self, "session_date", session)
        release = (
            (ASSET_RELEASE_ID, ASSET_RELEASE_SHA256)
            if self.table == "asset_observation_daily"
            else (UNIVERSE_RELEASE_ID, UNIVERSE_RELEASE_SHA256)
        )
        contract = _CONTRACTS[self.table]
        if (
            (self.release_id, self.release_manifest_sha256) != release
            or self.source_contract_id != contract.contract_id
            or self.source_schema_digest != contract.schema_digest
        ):
            raise IdentityExactGroupHistoryManifestError("source lineage differs")
        for label, value in (
            ("release ID", self.release_id),
            ("release SHA", self.release_manifest_sha256),
            ("contract ID", self.source_contract_id),
            ("schema digest", self.source_schema_digest),
            ("source SHA", self.sha256),
        ):
            _digest(value, label)
        path = _relative(self.path, "source path")
        match = _SESSION.search(path)
        if match is None or date.fromisoformat(match.group(1)) != session:
            raise IdentityExactGroupHistoryManifestError("source path/session differs")
        _positive(self.bytes, "source bytes")
        _nonnegative(self.row_count, "source rows")
        if self.disk_size_bytes != self.bytes:
            raise IdentityExactGroupHistoryManifestError("lstat size differs from manifest")
        if (
            self.role != "data"
            or self.media_type != "application/vnd.apache.parquet"
            or self.disk_is_regular_file is not True
            or self.disk_is_symlink is not False
            or self.content_opened is not False
        ):
            raise IdentityExactGroupHistoryManifestError("raw source crosses metadata boundary")

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

    def project_inventory_8(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryRawSourceArtifactRef:
        item = _mapping(value, "raw source artifact")
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
        _expect_keys(item, expected, "raw source artifact")
        return cls(
            table=_text(item["table"], "table"),
            session_date=_date(item["session_date"], "session date"),
            release_id=_text(item["release_id"], "release ID"),
            release_manifest_sha256=_text(item["release_manifest_sha256"], "release manifest SHA"),
            source_contract_id=_text(item["source_contract_id"], "source contract ID"),
            source_schema_digest=_text(item["source_schema_digest"], "source schema digest"),
            path=_text(item["path"], "path"),
            sha256=_text(item["sha256"], "SHA"),
            bytes=_positive(item["bytes"], "bytes"),
            row_count=_nonnegative(item["row_count"], "row count"),
            disk_size_bytes=_positive(item["disk_size_bytes"], "disk bytes"),
            role=_text(item["role"], "role"),
            media_type=_text(item["media_type"], "media type"),
            disk_is_regular_file=_boolean(item["disk_is_regular_file"], "regular"),
            disk_is_symlink=_boolean(item["disk_is_symlink"], "symlink"),
            content_opened=_boolean(item["content_opened"], "content opened"),
        )


@dataclass(frozen=True, slots=True, order=True)
class S7ExactGroupHistoryExecutionSourcePin:
    """Normalized 10-field execution projection, distinct from the raw digest domain."""

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
        if self.table not in _TABLES:
            raise IdentityExactGroupHistoryManifestError("execution source table differs")
        session = _date(self.session_date, "execution source session")
        if session < START_SESSION or session > END_SESSION:
            raise IdentityExactGroupHistoryManifestError("execution source session differs")
        object.__setattr__(self, "session_date", session.isoformat())
        for label, value in (
            ("release ID", self.release_id),
            ("release manifest SHA", self.release_manifest_sha256),
            ("source SHA", self.sha256),
            ("source contract ID", self.source_contract_id),
            ("schema digest", self.schema_digest),
        ):
            _digest(value, label)
        _relative(self.path, "source path")
        _positive(self.bytes, "source bytes")
        _nonnegative(self.row_count, "source rows")

    @classmethod
    def from_raw(
        cls, value: S7ExactGroupHistoryRawSourceArtifactRef
    ) -> S7ExactGroupHistoryExecutionSourcePin:
        return cls(
            table=value.table,
            session_date=value.session_date.isoformat(),
            release_id=value.release_id,
            release_manifest_sha256=value.release_manifest_sha256,
            path=value.path,
            sha256=value.sha256,
            bytes=value.bytes,
            row_count=value.row_count,
            source_contract_id=value.source_contract_id,
            schema_digest=value.source_schema_digest,
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

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionSourcePin:
        item = _mapping(value, "execution source pin")
        expected = {
            "bytes",
            "path",
            "release_id",
            "release_manifest_sha256",
            "row_count",
            "schema_digest",
            "session_date",
            "sha256",
            "source_contract_id",
            "table",
        }
        _expect_keys(item, expected, "execution source pin")
        return cls(
            table=_text(item["table"], "table"),
            session_date=_text(item["session_date"], "session date"),
            release_id=_text(item["release_id"], "release ID"),
            release_manifest_sha256=_text(item["release_manifest_sha256"], "release SHA"),
            path=_text(item["path"], "path"),
            sha256=_text(item["sha256"], "SHA"),
            bytes=_positive(item["bytes"], "bytes"),
            row_count=_nonnegative(item["row_count"], "rows"),
            source_contract_id=_text(item["source_contract_id"], "contract ID"),
            schema_digest=_text(item["schema_digest"], "schema digest"),
        )


@dataclass(frozen=True, slots=True, order=True)
class S7ExactGroupHistoryManifestDocumentRef:
    kind: str
    logical_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        if self.kind not in {item.kind for item in _expected_input_shell()}:
            raise IdentityExactGroupHistoryManifestError("manifest ref kind differs")
        _digest(self.logical_id, "manifest logical ID")
        _relative(self.path, "manifest ref path")
        _digest(self.sha256, "manifest ref SHA")
        _positive(self.bytes, "manifest ref bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "kind": self.kind,
            "logical_id": self.logical_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestDocumentRef:
        item = _mapping(value, "manifest ref")
        _expect_keys(item, {"bytes", "kind", "logical_id", "path", "sha256"}, "manifest ref")
        return cls(
            kind=_text(item["kind"], "kind"),
            logical_id=_text(item["logical_id"], "logical ID"),
            path=_text(item["path"], "path"),
            sha256=_text(item["sha256"], "SHA"),
            bytes=_positive(item["bytes"], "bytes"),
        )


def _expected_input_shell() -> tuple[S7ExactGroupHistoryManifestInputPin, ...]:
    # Completion paths are irrelevant to the kind set used by document refs.
    return canonical_manifest_inputs(
        inventory_completion_path=(
            "manifests/silver/identity/composite-inventory-execution-completions/"
            f"plan_id={'0' * 64}/approval_id={'1' * 64}/manifest.json"
        ),
        directional_completion_path=(
            "manifests/silver/identity/directional-raw-preview-execution-completions/"
            f"plan_id={'0' * 64}/approval_id={'1' * 64}/manifest.json"
        ),
    )


def normalize_raw_sources(
    values: tuple[S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> tuple[S7ExactGroupHistoryExecutionSourcePin, ...]:
    return tuple(sorted(S7ExactGroupHistoryExecutionSourcePin.from_raw(item) for item in values))


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistorySourceBinding:
    created_by: str
    created_at_utc: datetime
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_request_event_id: str
    manifest_request_event_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    manifest_literal_sha256: str
    run_intent_id: str
    run_intent_path: str
    run_intent_sha256: str
    source_artifacts: tuple[S7ExactGroupHistoryRawSourceArtifactRef, ...]
    execution_source_pins: tuple[S7ExactGroupHistoryExecutionSourcePin, ...]
    manifest_documents: tuple[S7ExactGroupHistoryManifestDocumentRef, ...]

    def __post_init__(self) -> None:
        _text(self.created_by, "source-binding actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        for label, value in (
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA", self.manifest_plan_sha256),
            ("manifest request ID", self.manifest_request_event_id),
            ("manifest request SHA", self.manifest_request_event_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA", self.manifest_approval_sha256),
            ("manifest literal SHA", self.manifest_literal_sha256),
            ("run intent ID", self.run_intent_id),
            ("run intent SHA", self.run_intent_sha256),
        ):
            _digest(value, label)
        if self.run_intent_path != manifest_run_intent_path(
            self.manifest_plan_id, self.manifest_approval_id
        ):
            raise IdentityExactGroupHistoryManifestError("run-intent path differs")
        sources = tuple(sorted(self.source_artifacts))
        if len(sources) != SOURCE_ARTIFACT_COUNT:
            raise IdentityExactGroupHistoryManifestError("raw source count differs")
        expected_pairs = {
            (table, session)
            for table in _TABLES
            for session in _calendar_span(START_SESSION, END_SESSION)
        }
        # Calendar sessions are not every civil date, so pair exactness is checked
        # by per-table uniqueness and the frozen 2,513 count; calendar content is
        # independently bound and read during the preflight.
        del expected_pairs
        pairs = {(item.table, item.session_date) for item in sources}
        if len(pairs) != SOURCE_ARTIFACT_COUNT or any(
            sum(item.table == table for item in sources) != SESSION_COUNT for table in _TABLES
        ):
            raise IdentityExactGroupHistoryManifestError("raw source table/session pairs differ")
        if len({item.path for item in sources}) != SOURCE_ARTIFACT_COUNT:
            raise IdentityExactGroupHistoryManifestError("raw source paths repeat")
        if (
            sum(item.row_count for item in sources) != SOURCE_ROW_COUNT
            or sum(item.bytes for item in sources) != SOURCE_BYTES
        ):
            raise IdentityExactGroupHistoryManifestError("raw source totals differ")
        if (
            stable_digest([item.project_inventory_8() for item in sources])
            != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        ):
            raise IdentityExactGroupHistoryManifestError("raw-to-inventory projection differs")
        normalized = tuple(sorted(self.execution_source_pins))
        if normalized != normalize_raw_sources(sources):
            raise IdentityExactGroupHistoryManifestError(
                "normalize(raw) differs from execution pins"
            )
        documents = tuple(sorted(self.manifest_documents))
        if len(documents) != EXPECTED_MANIFEST_INPUT_COUNT or len(
            {item.kind for item in documents}
        ) != len(documents):
            raise IdentityExactGroupHistoryManifestError("manifest document refs differ")
        object.__setattr__(self, "source_artifacts", sources)
        object.__setattr__(self, "execution_source_pins", normalized)
        object.__setattr__(self, "manifest_documents", documents)

    @property
    def raw_source_artifact_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.source_artifacts])

    @property
    def inventory_projection_set_digest(self) -> str:
        return stable_digest([item.project_inventory_8() for item in self.source_artifacts])

    @property
    def normalized_source_artifact_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.execution_source_pins])

    @property
    def manifest_document_set_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.manifest_documents])

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_source_binding",
            "capabilities": _false_capabilities(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_source_pins": [item.to_dict() for item in self.execution_source_pins],
            "inventory_projection_set_digest": self.inventory_projection_set_digest,
            "manifest_controls": {
                "approval_id": self.manifest_approval_id,
                "approval_sha256": self.manifest_approval_sha256,
                "literal_sha256": self.manifest_literal_sha256,
                "plan_id": self.manifest_plan_id,
                "plan_sha256": self.manifest_plan_sha256,
                "request_event_id": self.manifest_request_event_id,
                "request_event_sha256": self.manifest_request_event_sha256,
                "run_intent_id": self.run_intent_id,
                "run_intent_path": self.run_intent_path,
                "run_intent_sha256": self.run_intent_sha256,
            },
            "manifest_document_set_digest": self.manifest_document_set_digest,
            "manifest_documents": [item.to_dict() for item in self.manifest_documents],
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "raw_source_artifact_set_digest": self.raw_source_artifact_set_digest,
            "schema_version": 1,
            "source_artifact_count": SOURCE_ARTIFACT_COUNT,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "source_bytes": SOURCE_BYTES,
            "source_row_count": SOURCE_ROW_COUNT,
            "state": "awaiting_exact_execution_approval",
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return source_binding_path(self.run_intent_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistorySourceBinding:
        document = _mapping(value, "source binding")
        controls = _mapping(document.get("manifest_controls"), "manifest controls")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            manifest_plan_id=_text(controls.get("plan_id"), "plan ID"),
            manifest_plan_sha256=_text(controls.get("plan_sha256"), "plan SHA"),
            manifest_request_event_id=_text(controls.get("request_event_id"), "request event ID"),
            manifest_request_event_sha256=_text(
                controls.get("request_event_sha256"), "request event SHA"
            ),
            manifest_approval_id=_text(controls.get("approval_id"), "approval ID"),
            manifest_approval_sha256=_text(controls.get("approval_sha256"), "approval SHA"),
            manifest_literal_sha256=_text(controls.get("literal_sha256"), "literal SHA"),
            run_intent_id=_text(controls.get("run_intent_id"), "run intent ID"),
            run_intent_path=_text(controls.get("run_intent_path"), "run intent path"),
            run_intent_sha256=_text(controls.get("run_intent_sha256"), "run intent SHA"),
            source_artifacts=tuple(
                S7ExactGroupHistoryRawSourceArtifactRef.from_dict(item)
                for item in document.get("source_artifacts", [])
            ),
            execution_source_pins=tuple(
                S7ExactGroupHistoryExecutionSourcePin.from_dict(item)
                for item in document.get("execution_source_pins", [])
            ),
            manifest_documents=tuple(
                S7ExactGroupHistoryManifestDocumentRef.from_dict(item)
                for item in document.get("manifest_documents", [])
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("source binding is not canonical")
        return result


def _calendar_span(start: date, end: date) -> tuple[date, ...]:
    # Used only as a bounded helper; exact exchange sessions come from the
    # calendar manifest and are verified by the runner.
    return (start, end)


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryExecutionPlan:
    created_by: str
    created_at_utc: datetime
    git_commit: str
    git_tree: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    scope_set_id: str
    scope_set_sha256: str
    contract_id: str
    contract_schema_digest: str
    contract_candidate_sha256: str
    execution_resource_caps: S7ExactGroupHistoryExecutionCaps
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    source_binding_id: str
    source_binding_path: str
    source_binding_sha256: str
    source_binding_created_by: str
    raw_source_artifact_set_digest: str
    inventory_projection_set_digest: str
    normalized_source_artifact_set_digest: str
    source_artifacts: tuple[S7ExactGroupHistoryExecutionSourcePin, ...]

    def __post_init__(self) -> None:
        _text(self.created_by, "execution plan actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        _git_object(self.git_commit, "Git commit")
        _git_object(self.git_tree, "Git tree")
        for label, value in (
            ("runtime set digest", self.runtime_file_set_digest),
            ("verification set digest", self.verification_file_set_digest),
            ("scope set ID", self.scope_set_id),
            ("scope set SHA", self.scope_set_sha256),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("contract candidate SHA", self.contract_candidate_sha256),
            ("execution resource caps digest", self.execution_resource_caps.digest),
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA", self.manifest_plan_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA", self.manifest_approval_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA", self.source_binding_sha256),
            ("raw source digest", self.raw_source_artifact_set_digest),
            ("inventory projection digest", self.inventory_projection_set_digest),
            ("normalized source digest", self.normalized_source_artifact_set_digest),
        ):
            _digest(value, label)
        _relative(self.source_binding_path, "source binding path")
        _text(self.source_binding_created_by, "source binding actor")
        if self.source_binding_created_by == self.created_by:
            raise IdentityExactGroupHistoryManifestError("source and plan actors overlap")
        sources = tuple(sorted(self.source_artifacts))
        if (
            len(sources) != SOURCE_ARTIFACT_COUNT
            or stable_digest([item.to_dict() for item in sources])
            != self.normalized_source_artifact_set_digest
            or self.inventory_projection_set_digest != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        ):
            raise IdentityExactGroupHistoryManifestError("execution source projection differs")
        object.__setattr__(self, "source_artifacts", sources)
        if not isinstance(self.execution_resource_caps, S7ExactGroupHistoryExecutionCaps):
            raise IdentityExactGroupHistoryManifestError("execution resource caps type differs")

    @property
    def input_binding_digest(self) -> str:
        return stable_digest(
            {
                "contract": [
                    self.contract_id,
                    self.contract_schema_digest,
                    self.contract_candidate_sha256,
                ],
                "source": [
                    self.source_binding_id,
                    self.source_binding_sha256,
                    self.raw_source_artifact_set_digest,
                    self.inventory_projection_set_digest,
                    self.normalized_source_artifact_set_digest,
                ],
                "scope": [self.scope_set_id, self.scope_set_sha256],
                "resource_caps_digest": self.execution_resource_caps.digest,
            }
        )

    @property
    def source_artifact_set_digest(self) -> str:
        """Compatibility alias for the raw 16-field digest domain."""

        return self.raw_source_artifact_set_digest

    @property
    def execution_data_root(self) -> str:
        return EXECUTION_DATA_ROOT

    @property
    def inventory_completion_id(self) -> str:
        return INVENTORY_COMPLETION_ID

    @property
    def directional_preview_candidate_id(self) -> str:
        return DIRECTIONAL_CANDIDATE_ID

    @property
    def directional_preview_completion_id(self) -> str:
        return DIRECTIONAL_COMPLETION_ID

    @property
    def rss_bytes_hard_cap(self) -> int:
        return self.execution_resource_caps.rss_bytes_hard_cap

    @property
    def wall_clock_seconds_hard_cap(self) -> int:
        return self.execution_resource_caps.wall_clock_seconds_hard_cap

    @property
    def disk_free_floor_bytes(self) -> int:
        return self.execution_resource_caps.disk_free_bytes_hard_floor

    @property
    def temporary_bytes_hard_cap(self) -> int:
        return self.execution_resource_caps.tmp_bytes_hard_cap

    @property
    def output_bytes_hard_cap(self) -> int:
        return self.execution_resource_caps.output_bytes_hard_cap

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_execution_plan",
            "authorized_action": EXECUTION_AUTHORIZED_ACTION,
            "capabilities_before_exact_literal": {
                **_false_capabilities(),
                "exact_group_history_read": False,
            },
            "contract": {
                "candidate_sha256": self.contract_candidate_sha256,
                "contract_id": self.contract_id,
                "schema_digest": self.contract_schema_digest,
            },
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_semantics": {
                "exact_group_history_scan_once_after_separate_approval": True,
                "filter_fields": [
                    "provider",
                    "market",
                    "locale",
                    "ticker",
                    "observed_composite_figi",
                ],
                "physical_artifact_count": SOURCE_ARTIFACT_COUNT,
                "share_class_is_not_a_filter": True,
            },
            "git_binding": {
                "git_commit": self.git_commit,
                "git_tree": self.git_tree,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "verification_file_set_digest": self.verification_file_set_digest,
            },
            "input_binding_digest": self.input_binding_digest,
            "manifest_controls": {
                "approval_id": self.manifest_approval_id,
                "approval_sha256": self.manifest_approval_sha256,
                "plan_id": self.manifest_plan_id,
                "plan_sha256": self.manifest_plan_sha256,
            },
            "plan_state": "awaiting_exact_execution_approval",
            "resource_caps": self.execution_resource_caps.to_dict(),
            "resource_caps_digest": self.execution_resource_caps.digest,
            "schema_version": 1,
            "scope_binding": {
                "scope_set_id": self.scope_set_id,
                "scope_set_sha256": self.scope_set_sha256,
            },
            "source_binding": {
                "created_by": self.source_binding_created_by,
                "inventory_projection_set_digest": self.inventory_projection_set_digest,
                "normalized_source_artifact_set_digest": (
                    self.normalized_source_artifact_set_digest
                ),
                "path": self.source_binding_path,
                "raw_source_artifact_set_digest": self.raw_source_artifact_set_digest,
                "sha256": self.source_binding_sha256,
                "source_binding_id": self.source_binding_id,
            },
            "source_artifact_count": SOURCE_ARTIFACT_COUNT,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
        }

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "plan_id": self.plan_id})

    @property
    def content(self) -> bytes:
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return execution_plan_path(self.plan_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionPlan:
        document = _mapping(value, "execution plan")
        git = _mapping(document.get("git_binding"), "Git binding")
        scope = _mapping(document.get("scope_binding"), "scope binding")
        contract = _mapping(document.get("contract"), "contract")
        controls = _mapping(document.get("manifest_controls"), "manifest controls")
        source = _mapping(document.get("source_binding"), "source binding")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            git_commit=_text(git.get("git_commit"), "Git commit"),
            git_tree=_text(git.get("git_tree"), "Git tree"),
            runtime_file_set_digest=_text(git.get("runtime_file_set_digest"), "runtime set digest"),
            verification_file_set_digest=_text(
                git.get("verification_file_set_digest"), "verification set digest"
            ),
            scope_set_id=_text(scope.get("scope_set_id"), "scope set ID"),
            scope_set_sha256=_text(scope.get("scope_set_sha256"), "scope set SHA"),
            contract_id=_text(contract.get("contract_id"), "contract ID"),
            contract_schema_digest=_text(contract.get("schema_digest"), "schema digest"),
            contract_candidate_sha256=_text(contract.get("candidate_sha256"), "candidate SHA"),
            execution_resource_caps=S7ExactGroupHistoryExecutionCaps.from_dict(
                document.get("resource_caps")
            ),
            manifest_plan_id=_text(controls.get("plan_id"), "manifest plan ID"),
            manifest_plan_sha256=_text(controls.get("plan_sha256"), "manifest plan SHA"),
            manifest_approval_id=_text(controls.get("approval_id"), "approval ID"),
            manifest_approval_sha256=_text(controls.get("approval_sha256"), "approval SHA"),
            source_binding_id=_text(source.get("source_binding_id"), "binding ID"),
            source_binding_path=_text(source.get("path"), "binding path"),
            source_binding_sha256=_text(source.get("sha256"), "binding SHA"),
            source_binding_created_by=_text(source.get("created_by"), "binding actor"),
            raw_source_artifact_set_digest=_text(
                source.get("raw_source_artifact_set_digest"), "raw digest"
            ),
            inventory_projection_set_digest=_text(
                source.get("inventory_projection_set_digest"), "inventory digest"
            ),
            normalized_source_artifact_set_digest=_text(
                source.get("normalized_source_artifact_set_digest"), "normalized digest"
            ),
            source_artifacts=tuple(
                S7ExactGroupHistoryExecutionSourcePin.from_dict(item)
                for item in document.get("source_artifacts", [])
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("execution plan is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryExecutionRequest:
    created_by: str
    created_at_utc: datetime
    plan_id: str
    plan_sha256: str
    input_binding_digest: str
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    source_binding_id: str
    source_binding_sha256: str
    raw_source_artifact_set_digest: str
    inventory_projection_set_digest: str
    normalized_source_artifact_set_digest: str
    resource_caps_digest: str

    def __post_init__(self) -> None:
        _text(self.created_by, "execution request actor")
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("input binding digest", self.input_binding_digest),
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA", self.manifest_plan_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA", self.manifest_approval_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA", self.source_binding_sha256),
            ("raw digest", self.raw_source_artifact_set_digest),
            ("inventory projection digest", self.inventory_projection_set_digest),
            ("normalized digest", self.normalized_source_artifact_set_digest),
            ("resource caps digest", self.resource_caps_digest),
        ):
            _digest(value, label)

    @classmethod
    def create(
        cls,
        plan: S7ExactGroupHistoryExecutionPlan,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7ExactGroupHistoryExecutionRequest:
        if created_by == plan.created_by or created_at_utc <= plan.created_at_utc:
            raise IdentityExactGroupHistoryManifestError("execution request actor/time differs")
        return cls(
            created_by=created_by,
            created_at_utc=created_at_utc,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            input_binding_digest=plan.input_binding_digest,
            manifest_plan_id=plan.manifest_plan_id,
            manifest_plan_sha256=plan.manifest_plan_sha256,
            manifest_approval_id=plan.manifest_approval_id,
            manifest_approval_sha256=plan.manifest_approval_sha256,
            source_binding_id=plan.source_binding_id,
            source_binding_sha256=plan.source_binding_sha256,
            raw_source_artifact_set_digest=plan.raw_source_artifact_set_digest,
            inventory_projection_set_digest=plan.inventory_projection_set_digest,
            normalized_source_artifact_set_digest=plan.normalized_source_artifact_set_digest,
            resource_caps_digest=plan.execution_resource_caps.digest,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_exact_group_history_execution_request",
            "authorized_action": EXECUTION_AUTHORIZED_ACTION,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "input_binding_digest": self.input_binding_digest,
            "inventory_projection_set_digest": self.inventory_projection_set_digest,
            "literal_version": EXECUTION_LITERAL_VERSION,
            "manifest_approval_id": self.manifest_approval_id,
            "manifest_approval_sha256": self.manifest_approval_sha256,
            "manifest_plan_id": self.manifest_plan_id,
            "manifest_plan_sha256": self.manifest_plan_sha256,
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "raw_source_artifact_set_digest": self.raw_source_artifact_set_digest,
            "request_state": "awaiting_literal_human_approval",
            "resource_caps_digest": self.resource_caps_digest,
            "schema_version": 1,
            "source_artifact_count": SOURCE_ARTIFACT_COUNT,
            "source_binding_id": self.source_binding_id,
            "source_binding_sha256": self.source_binding_sha256,
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return execution_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        excluded = {
            "artifact_type",
            "created_at_utc",
            "created_by",
            "request_state",
            "schema_version",
        }
        payload = {key: value for key, value in self.document.items() if key not in excluded}
        payload["request_event_sha256"] = self.sha256
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionRequest:
        document = _mapping(value, "execution request")
        result = cls(
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            plan_id=_text(document.get("plan_id"), "plan ID"),
            plan_sha256=_text(document.get("plan_sha256"), "plan SHA"),
            input_binding_digest=_text(document.get("input_binding_digest"), "input binding"),
            manifest_plan_id=_text(document.get("manifest_plan_id"), "manifest plan ID"),
            manifest_plan_sha256=_text(document.get("manifest_plan_sha256"), "manifest plan SHA"),
            manifest_approval_id=_text(
                document.get("manifest_approval_id"), "manifest approval ID"
            ),
            manifest_approval_sha256=_text(
                document.get("manifest_approval_sha256"), "manifest approval SHA"
            ),
            source_binding_id=_text(document.get("source_binding_id"), "binding ID"),
            source_binding_sha256=_text(document.get("source_binding_sha256"), "binding SHA"),
            raw_source_artifact_set_digest=_text(
                document.get("raw_source_artifact_set_digest"), "raw digest"
            ),
            inventory_projection_set_digest=_text(
                document.get("inventory_projection_set_digest"), "inventory digest"
            ),
            normalized_source_artifact_set_digest=_text(
                document.get("normalized_source_artifact_set_digest"), "normalized digest"
            ),
            resource_caps_digest=_text(
                document.get("resource_caps_digest"), "resource caps digest"
            ),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("execution request is not canonical")
        return result


@dataclass(frozen=True, slots=True, order=True)
class StoredExactGroupHistoryManifestDocument:
    logical_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _digest(self.logical_id, "stored document logical ID")
        _relative(self.path, "stored document path")
        _digest(self.sha256, "stored document SHA-256")
        _positive(self.bytes, "stored document bytes")


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestCompletion:
    manifest_plan_id: str
    manifest_plan_sha256: str
    manifest_approval_id: str
    manifest_approval_sha256: str
    run_intent: StoredExactGroupHistoryManifestDocument
    source_binding: StoredExactGroupHistoryManifestDocument
    execution_plan: StoredExactGroupHistoryManifestDocument
    execution_request: StoredExactGroupHistoryManifestDocument
    source_json_read_count: int
    parquet_lstat_count: int
    parquet_content_bytes_read: int
    completed_at_utc: datetime

    def __post_init__(self) -> None:
        for label, value in (
            ("manifest plan ID", self.manifest_plan_id),
            ("manifest plan SHA", self.manifest_plan_sha256),
            ("manifest approval ID", self.manifest_approval_id),
            ("manifest approval SHA", self.manifest_approval_sha256),
        ):
            _digest(value, label)
        if self.source_json_read_count != EXPECTED_MANIFEST_INPUT_COUNT:
            raise IdentityExactGroupHistoryManifestError("source JSON read count differs")
        if self.parquet_lstat_count != SOURCE_ARTIFACT_COUNT:
            raise IdentityExactGroupHistoryManifestError("Parquet lstat count differs")
        if self.parquet_content_bytes_read != 0:
            raise IdentityExactGroupHistoryManifestError("Parquet content was read")
        object.__setattr__(
            self, "completed_at_utc", _utc(self.completed_at_utc, "completed_at_utc")
        )

    def logical_payload(self) -> dict[str, object]:
        def ref(value: StoredExactGroupHistoryManifestDocument) -> dict[str, object]:
            return {
                "bytes": value.bytes,
                "logical_id": value.logical_id,
                "path": value.path,
                "sha256": value.sha256,
            }

        return {
            "artifact_type": "s7_exact_group_history_manifest_preflight_completion",
            "capabilities": _false_capabilities(),
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "execution_plan": ref(self.execution_plan),
            "execution_request": ref(self.execution_request),
            "manifest_approval_id": self.manifest_approval_id,
            "manifest_approval_sha256": self.manifest_approval_sha256,
            "manifest_plan_id": self.manifest_plan_id,
            "manifest_plan_sha256": self.manifest_plan_sha256,
            "operation_counts": {
                "parquet_content_bytes_read": self.parquet_content_bytes_read,
                "parquet_lstat_count": self.parquet_lstat_count,
                "source_json_read_count": self.source_json_read_count,
            },
            "run_intent": ref(self.run_intent),
            "schema_version": 1,
            "source_binding": ref(self.source_binding),
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
        return canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return manifest_completion_path(self.manifest_plan_id, self.manifest_approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryManifestCompletion:
        document = _mapping(value, "manifest completion")

        def parsed_ref(key: str) -> StoredExactGroupHistoryManifestDocument:
            item = _mapping(document.get(key), key)
            return StoredExactGroupHistoryManifestDocument(
                logical_id=_text(item.get("logical_id"), f"{key} logical ID"),
                path=_text(item.get("path"), f"{key} path"),
                sha256=_text(item.get("sha256"), f"{key} SHA"),
                bytes=_positive(item.get("bytes"), f"{key} bytes"),
            )

        counts = _mapping(document.get("operation_counts"), "operation counts")
        result = cls(
            manifest_plan_id=_text(document.get("manifest_plan_id"), "plan ID"),
            manifest_plan_sha256=_text(document.get("manifest_plan_sha256"), "plan SHA"),
            manifest_approval_id=_text(document.get("manifest_approval_id"), "approval ID"),
            manifest_approval_sha256=_text(
                document.get("manifest_approval_sha256"), "approval SHA"
            ),
            run_intent=parsed_ref("run_intent"),
            source_binding=parsed_ref("source_binding"),
            execution_plan=parsed_ref("execution_plan"),
            execution_request=parsed_ref("execution_request"),
            source_json_read_count=_nonnegative(
                counts.get("source_json_read_count"), "source JSON reads"
            ),
            parquet_lstat_count=_nonnegative(counts.get("parquet_lstat_count"), "Parquet lstats"),
            parquet_content_bytes_read=_nonnegative(
                counts.get("parquet_content_bytes_read"), "Parquet bytes"
            ),
            completed_at_utc=_parse_utc(document.get("completed_at_utc"), "completed_at_utc"),
        )
        if result.content != canonical_bytes(document):
            raise IdentityExactGroupHistoryManifestError("manifest completion is not canonical")
        return result


class ExactGroupHistoryManifestStore:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def _store(
        self, logical_id: str, relative: str, content: bytes
    ) -> StoredExactGroupHistoryManifestDocument:
        try:
            receipt = write_bytes_immutable(
                self.root, safe_relative_path(self.root, relative), content
            )
        except ArtifactError as exc:
            raise IdentityExactGroupHistoryManifestError(str(exc)) from exc
        return StoredExactGroupHistoryManifestDocument(
            logical_id=logical_id,
            path=str(receipt["path"]),
            sha256=str(receipt["sha256"]),
            bytes=int(receipt["bytes"]),
        )

    def _load(self, relative: str, sha256: str, parser: Any) -> Any:
        _digest(sha256, "expected SHA-256")
        try:
            path = safe_relative_path(self.root, relative)
        except ArtifactError as exc:
            raise IdentityExactGroupHistoryManifestError(str(exc)) from exc
        if not path.is_file() or path.is_symlink():
            raise IdentityExactGroupHistoryManifestError(f"control missing or unsafe: {relative}")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != sha256:
            raise IdentityExactGroupHistoryManifestError(f"control SHA differs: {relative}")
        try:
            document = json.loads(content, object_pairs_hook=_reject_duplicates)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IdentityExactGroupHistoryManifestError(
                f"control JSON invalid: {relative}"
            ) from exc
        value = parser(document)
        if value.content != content:
            raise IdentityExactGroupHistoryManifestError(f"control bytes differ: {relative}")
        return value

    def store_preparation_authorization(
        self, value: S7ExactGroupHistoryPreparationAuthorization
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.authorization_id, value.relative_path, value.content)

    def load_preparation_authorization(
        self, authorization_id: str, sha256: str
    ) -> S7ExactGroupHistoryPreparationAuthorization:
        return self._load(
            preparation_authorization_path(authorization_id),
            sha256,
            S7ExactGroupHistoryPreparationAuthorization.from_dict,
        )

    def store_manifest_plan(
        self, value: S7ExactGroupHistoryManifestPlan
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.plan_id, value.relative_path, value.content)

    def load_manifest_plan(self, plan_id: str, sha256: str) -> S7ExactGroupHistoryManifestPlan:
        return self._load(
            manifest_plan_path(plan_id), sha256, S7ExactGroupHistoryManifestPlan.from_dict
        )

    def store_manifest_request(
        self, value: S7ExactGroupHistoryManifestRequest
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.request_event_id, value.relative_path, value.content)

    def load_manifest_request(
        self, request_id: str, sha256: str
    ) -> S7ExactGroupHistoryManifestRequest:
        return self._load(
            manifest_request_path(request_id), sha256, S7ExactGroupHistoryManifestRequest.from_dict
        )

    def store_manifest_approval(
        self, value: S7ExactGroupHistoryManifestApproval
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.approval_id, value.relative_path, value.content)

    def load_manifest_approval(
        self, approval_id: str, sha256: str
    ) -> S7ExactGroupHistoryManifestApproval:
        return self._load(
            manifest_approval_path(approval_id),
            sha256,
            S7ExactGroupHistoryManifestApproval.from_dict,
        )

    def store_run_intent(
        self, value: S7ExactGroupHistoryManifestRunIntent
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.intent_id, value.relative_path, value.content)

    def load_run_intent(
        self, plan_id: str, approval_id: str, sha256: str
    ) -> S7ExactGroupHistoryManifestRunIntent:
        return self._load(
            manifest_run_intent_path(plan_id, approval_id),
            sha256,
            S7ExactGroupHistoryManifestRunIntent.from_dict,
        )

    def store_source_binding(
        self, value: S7ExactGroupHistorySourceBinding
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.source_binding_id, value.relative_path, value.content)

    def load_source_binding(
        self, run_intent_id: str, sha256: str
    ) -> S7ExactGroupHistorySourceBinding:
        return self._load(
            source_binding_path(run_intent_id), sha256, S7ExactGroupHistorySourceBinding.from_dict
        )

    def store_execution_plan(
        self, value: S7ExactGroupHistoryExecutionPlan
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.plan_id, value.relative_path, value.content)

    def load_execution_plan(
        self,
        plan_id: str,
        expected_sha256: str,
    ) -> tuple[
        S7ExactGroupHistoryExecutionPlan,
        StoredExactGroupHistoryManifestDocument,
    ]:
        value = self._load(
            execution_plan_path(plan_id),
            expected_sha256,
            S7ExactGroupHistoryExecutionPlan.from_dict,
        )
        return value, StoredExactGroupHistoryManifestDocument(
            value.plan_id, value.relative_path, value.sha256, len(value.content)
        )

    def store_execution_request(
        self, value: S7ExactGroupHistoryExecutionRequest
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.request_event_id, value.relative_path, value.content)

    def load_execution_request(
        self,
        request_id: str,
        expected_sha256: str,
    ) -> tuple[
        S7ExactGroupHistoryExecutionRequest,
        StoredExactGroupHistoryManifestDocument,
    ]:
        value = self._load(
            execution_request_path(request_id),
            expected_sha256,
            S7ExactGroupHistoryExecutionRequest.from_dict,
        )
        return value, StoredExactGroupHistoryManifestDocument(
            value.request_event_id, value.relative_path, value.sha256, len(value.content)
        )

    def store_completion(
        self, value: S7ExactGroupHistoryManifestCompletion
    ) -> StoredExactGroupHistoryManifestDocument:
        return self._store(value.completion_id, value.relative_path, value.content)

    def load_completion(
        self, plan_id: str, approval_id: str, sha256: str
    ) -> S7ExactGroupHistoryManifestCompletion:
        return self._load(
            manifest_completion_path(plan_id, approval_id),
            sha256,
            S7ExactGroupHistoryManifestCompletion.from_dict,
        )


def verify_exact_group_history_cross_bindings(
    *,
    scope: S7ExactGroupHistoryScopeSet,
    preparation_plan: S7ExactGroupHistoryPreparationPlan,
    preparation_request: S7ExactGroupHistoryPreparationRequest,
    preparation_authorization: S7ExactGroupHistoryPreparationAuthorization,
    manifest_plan: S7ExactGroupHistoryManifestPlan,
    manifest_request: S7ExactGroupHistoryManifestRequest,
    manifest_approval: S7ExactGroupHistoryManifestApproval | None = None,
) -> None:
    """Verify every immutable control projection, actor, and timestamp in one place."""

    try:
        verify_preparation_request_binding(preparation_plan, preparation_request)
    except IdentityExactGroupHistoryPlanError as exc:
        raise IdentityExactGroupHistoryManifestError(str(exc)) from exc

    authorization_expected = (
        preparation_plan.plan_id,
        preparation_plan.sha256,
        preparation_request.request_event_id,
        preparation_request.sha256,
        preparation_request.canonical_approval_literal,
    )
    authorization_actual = (
        preparation_authorization.plan_id,
        preparation_authorization.plan_sha256,
        preparation_authorization.request_event_id,
        preparation_authorization.request_event_sha256,
        preparation_authorization.approval_literal,
    )
    manifest_plan_expected = (
        preparation_authorization.authorization_id,
        preparation_authorization.sha256,
        preparation_authorization.relative_path,
        preparation_plan.plan_id,
        preparation_plan.sha256,
        preparation_request.request_event_id,
        preparation_request.sha256,
        preparation_plan.git_commit,
        preparation_plan.git_tree,
        preparation_plan.runtime_files,
        preparation_plan.verification_files,
        preparation_plan.scope_set_id,
        preparation_plan.scope_set_sha256,
        preparation_plan.contract_id,
        preparation_plan.contract_schema_digest,
        preparation_plan.contract_candidate_sha256,
        preparation_plan.execution_resource_caps.digest,
    )
    manifest_plan_actual = (
        manifest_plan.preparation_authorization_id,
        manifest_plan.preparation_authorization_sha256,
        manifest_plan.preparation_authorization_path,
        manifest_plan.preparation_plan_id,
        manifest_plan.preparation_plan_sha256,
        manifest_plan.preparation_request_event_id,
        manifest_plan.preparation_request_event_sha256,
        manifest_plan.git_commit,
        manifest_plan.git_tree,
        manifest_plan.runtime_files,
        manifest_plan.verification_files,
        manifest_plan.scope_set_id,
        manifest_plan.scope_set_sha256,
        manifest_plan.contract_id,
        manifest_plan.contract_schema_digest,
        manifest_plan.contract_candidate_sha256,
        manifest_plan.execution_resource_caps_digest,
    )
    manifest_request_expected = (
        manifest_plan.plan_id,
        manifest_plan.sha256,
        manifest_plan.input_binding_digest,
        manifest_plan.manifest_input_set_digest,
        manifest_plan.runtime_file_set_digest,
        manifest_plan.verification_file_set_digest,
        manifest_plan.execution_resource_caps_digest,
        manifest_plan.execution_data_root,
        manifest_plan.future_manifest_reader_actor,
        manifest_plan.future_execution_plan_actor,
        manifest_plan.future_execution_request_actor,
    )
    manifest_request_actual = (
        manifest_request.plan_id,
        manifest_request.plan_sha256,
        manifest_request.input_binding_digest,
        manifest_request.manifest_input_set_digest,
        manifest_request.runtime_file_set_digest,
        manifest_request.verification_file_set_digest,
        manifest_request.execution_resource_caps_digest,
        manifest_request.execution_data_root,
        manifest_request.future_manifest_reader_actor,
        manifest_request.future_execution_plan_actor,
        manifest_request.future_execution_request_actor,
    )
    if (
        preparation_plan.scope_set_id != scope.scope_set_id
        or preparation_plan.scope_set_sha256 != scope.sha256
        or authorization_actual != authorization_expected
        or manifest_plan_actual != manifest_plan_expected
        or manifest_request_actual != manifest_request_expected
    ):
        raise IdentityExactGroupHistoryManifestError("exact control cross-binding differs")

    actors = [
        scope.created_by,
        preparation_plan.created_by,
        preparation_request.created_by,
        preparation_authorization.approved_by,
        manifest_plan.created_by,
        manifest_request.created_by,
        manifest_plan.future_manifest_reader_actor,
        manifest_plan.future_execution_plan_actor,
        manifest_plan.future_execution_request_actor,
    ]
    times = [
        scope.created_at_utc,
        preparation_plan.created_at_utc,
        preparation_request.created_at_utc,
        preparation_authorization.approved_at_utc,
        manifest_plan.created_at_utc,
        manifest_request.created_at_utc,
    ]
    if manifest_approval is not None:
        if (
            manifest_approval.plan_id != manifest_plan.plan_id
            or manifest_approval.plan_sha256 != manifest_plan.sha256
            or manifest_approval.request_event_id != manifest_request.request_event_id
            or manifest_approval.request_event_sha256 != manifest_request.sha256
            or manifest_approval.approval_literal != manifest_request.canonical_approval_literal
        ):
            raise IdentityExactGroupHistoryManifestError("manifest approval cross-binding differs")
        actors.append(manifest_approval.approved_by)
        times.append(manifest_approval.approved_at_utc)
    if len(actors) != len(set(actors)):
        raise IdentityExactGroupHistoryManifestError("control actors are not globally unique")
    if any(later <= earlier for earlier, later in pairwise(times)):
        raise IdentityExactGroupHistoryManifestError("control timestamps do not strictly increase")
    if times[-1] > datetime.now(UTC):
        raise IdentityExactGroupHistoryManifestError("control timestamp cannot be in the future")


def _load_preparation_cross_binding_inputs(
    root: Path,
    manifest_plan: S7ExactGroupHistoryManifestPlan,
) -> tuple[
    S7ExactGroupHistoryScopeSet,
    S7ExactGroupHistoryPreparationPlan,
    S7ExactGroupHistoryPreparationRequest,
    S7ExactGroupHistoryPreparationAuthorization,
]:
    plan_store = ExactGroupHistoryPlanStore(root)
    preparation_plan = plan_store.load_plan(
        manifest_plan.preparation_plan_id, manifest_plan.preparation_plan_sha256
    )
    preparation_request = plan_store.load_request(
        manifest_plan.preparation_request_event_id,
        manifest_plan.preparation_request_event_sha256,
    )
    scope = plan_store.load_scope(manifest_plan.scope_set_id, manifest_plan.scope_set_sha256)
    authorization = ExactGroupHistoryManifestStore(root).load_preparation_authorization(
        manifest_plan.preparation_authorization_id,
        manifest_plan.preparation_authorization_sha256,
    )
    return scope, preparation_plan, preparation_request, authorization


def build_exact_group_history_manifest_controls(
    *,
    control_root: Path,
    preparation_plan_id: str,
    preparation_plan_sha256: str,
    preparation_request_event_id: str,
    preparation_request_event_sha256: str,
    approved_preparation_literal: str,
    preparation_approved_by: str,
    preparation_approved_at_utc: datetime,
    manifest_plan_created_by: str,
    manifest_plan_created_at_utc: datetime,
    manifest_request_created_by: str,
    manifest_request_created_at_utc: datetime,
    inventory_completion_path: str,
    directional_completion_path: str,
    future_manifest_reader_actor: str,
    future_execution_plan_actor: str,
    future_execution_request_actor: str,
    preparation_review_note: str = "",
) -> tuple[
    StoredExactGroupHistoryManifestDocument,
    StoredExactGroupHistoryManifestDocument,
    StoredExactGroupHistoryManifestDocument,
    S7ExactGroupHistoryManifestRequest,
]:
    plan_store = ExactGroupHistoryPlanStore(control_root)
    preparation_plan = plan_store.load_plan(preparation_plan_id, preparation_plan_sha256)
    preparation_request = plan_store.load_request(
        preparation_request_event_id, preparation_request_event_sha256
    )
    scope = plan_store.load_scope(preparation_plan.scope_set_id, preparation_plan.scope_set_sha256)
    authorization = S7ExactGroupHistoryPreparationAuthorization(
        plan_id=preparation_plan.plan_id,
        plan_sha256=preparation_plan.sha256,
        request_event_id=preparation_request.request_event_id,
        request_event_sha256=preparation_request.sha256,
        approval_literal=approved_preparation_literal,
        approved_by=preparation_approved_by,
        approved_at_utc=preparation_approved_at_utc,
        review_note=preparation_review_note,
    )
    manifest_plan = S7ExactGroupHistoryManifestPlan(
        created_by=manifest_plan_created_by,
        created_at_utc=manifest_plan_created_at_utc,
        execution_data_root=EXECUTION_DATA_ROOT,
        git_commit=preparation_plan.git_commit,
        git_tree=preparation_plan.git_tree,
        runtime_files=preparation_plan.runtime_files,
        verification_files=preparation_plan.verification_files,
        scope_set_id=preparation_plan.scope_set_id,
        scope_set_sha256=preparation_plan.scope_set_sha256,
        contract_id=preparation_plan.contract_id,
        contract_schema_digest=preparation_plan.contract_schema_digest,
        contract_candidate_sha256=preparation_plan.contract_candidate_sha256,
        execution_resource_caps_digest=preparation_plan.execution_resource_caps.digest,
        preparation_authorization_id=authorization.authorization_id,
        preparation_authorization_sha256=authorization.sha256,
        preparation_authorization_path=authorization.relative_path,
        preparation_plan_id=preparation_plan.plan_id,
        preparation_plan_sha256=preparation_plan.sha256,
        preparation_request_event_id=preparation_request.request_event_id,
        preparation_request_event_sha256=preparation_request.sha256,
        manifest_inputs=canonical_manifest_inputs(
            inventory_completion_path=inventory_completion_path,
            directional_completion_path=directional_completion_path,
        ),
        future_manifest_reader_actor=future_manifest_reader_actor,
        future_execution_plan_actor=future_execution_plan_actor,
        future_execution_request_actor=future_execution_request_actor,
    )
    manifest_request = S7ExactGroupHistoryManifestRequest.create(
        manifest_plan,
        created_by=manifest_request_created_by,
        created_at_utc=manifest_request_created_at_utc,
    )
    verify_exact_group_history_cross_bindings(
        scope=scope,
        preparation_plan=preparation_plan,
        preparation_request=preparation_request,
        preparation_authorization=authorization,
        manifest_plan=manifest_plan,
        manifest_request=manifest_request,
    )
    store = ExactGroupHistoryManifestStore(control_root)
    authorization_receipt = store.store_preparation_authorization(authorization)
    return (
        authorization_receipt,
        store.store_manifest_plan(manifest_plan),
        store.store_manifest_request(manifest_request),
        manifest_request,
    )


def record_exact_group_history_manifest_approval(
    *,
    control_root: Path,
    manifest_plan_id: str,
    manifest_plan_sha256: str,
    manifest_request_event_id: str,
    manifest_request_event_sha256: str,
    approval_literal: str,
    approved_by: str,
    approved_at_utc: datetime,
    review_note: str = "",
) -> StoredExactGroupHistoryManifestDocument:
    store = ExactGroupHistoryManifestStore(control_root)
    plan = store.load_manifest_plan(manifest_plan_id, manifest_plan_sha256)
    request = store.load_manifest_request(manifest_request_event_id, manifest_request_event_sha256)
    scope, preparation_plan, preparation_request, authorization = (
        _load_preparation_cross_binding_inputs(control_root, plan)
    )
    approval = S7ExactGroupHistoryManifestApproval(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        request_event_sha256=request.sha256,
        approval_literal=approval_literal,
        approved_by=approved_by,
        approved_at_utc=approved_at_utc,
        review_note=review_note,
    )
    verify_exact_group_history_cross_bindings(
        scope=scope,
        preparation_plan=preparation_plan,
        preparation_request=preparation_request,
        preparation_authorization=authorization,
        manifest_plan=plan,
        manifest_request=request,
        manifest_approval=approval,
    )
    return store.store_manifest_approval(approval)


def build_execution_controls(
    source_binding: S7ExactGroupHistorySourceBinding,
    manifest_plan: S7ExactGroupHistoryManifestPlan,
    *,
    created_at_utc: datetime,
    plan_actor: str,
    request_actor: str,
) -> tuple[S7ExactGroupHistoryExecutionPlan, S7ExactGroupHistoryExecutionRequest]:
    if (
        source_binding.manifest_plan_id != manifest_plan.plan_id
        or source_binding.manifest_plan_sha256 != manifest_plan.sha256
        or source_binding.created_by != manifest_plan.future_manifest_reader_actor
        or plan_actor != manifest_plan.future_execution_plan_actor
        or request_actor != manifest_plan.future_execution_request_actor
        or created_at_utc <= source_binding.created_at_utc
    ):
        raise IdentityExactGroupHistoryManifestError("execution control projection differs")
    plan = S7ExactGroupHistoryExecutionPlan(
        created_by=plan_actor,
        created_at_utc=created_at_utc,
        git_commit=manifest_plan.git_commit,
        git_tree=manifest_plan.git_tree,
        runtime_file_set_digest=manifest_plan.runtime_file_set_digest,
        verification_file_set_digest=manifest_plan.verification_file_set_digest,
        scope_set_id=manifest_plan.scope_set_id,
        scope_set_sha256=manifest_plan.scope_set_sha256,
        contract_id=manifest_plan.contract_id,
        contract_schema_digest=manifest_plan.contract_schema_digest,
        contract_candidate_sha256=manifest_plan.contract_candidate_sha256,
        execution_resource_caps=S7ExactGroupHistoryExecutionCaps(),
        manifest_plan_id=manifest_plan.plan_id,
        manifest_plan_sha256=manifest_plan.sha256,
        manifest_approval_id=source_binding.manifest_approval_id,
        manifest_approval_sha256=source_binding.manifest_approval_sha256,
        source_binding_id=source_binding.source_binding_id,
        source_binding_path=source_binding.relative_path,
        source_binding_sha256=source_binding.sha256,
        source_binding_created_by=source_binding.created_by,
        raw_source_artifact_set_digest=source_binding.raw_source_artifact_set_digest,
        inventory_projection_set_digest=source_binding.inventory_projection_set_digest,
        normalized_source_artifact_set_digest=(
            source_binding.normalized_source_artifact_set_digest
        ),
        source_artifacts=source_binding.execution_source_pins,
    )
    request = S7ExactGroupHistoryExecutionRequest.create(
        plan,
        created_by=request_actor,
        created_at_utc=created_at_utc + timedelta(microseconds=1),
    )
    return plan, request


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryManifestRun:
    completion: S7ExactGroupHistoryManifestCompletion
    completion_receipt: StoredExactGroupHistoryManifestDocument
    attempt_source_json_reads: int
    attempt_parquet_lstats: int
    attempt_parquet_content_bytes_read: int
    recovered: bool


def run_exact_group_history_manifest_preflight(
    *,
    data_root: Path,
    repository_root: Path,
    manifest_plan_id: str,
    manifest_plan_sha256: str,
    manifest_approval_id: str,
    manifest_approval_sha256: str,
    source_binding_created_at_utc: datetime,
) -> S7ExactGroupHistoryManifestRun:
    root = data_root.expanduser().resolve()
    repository = repository_root.expanduser().resolve()
    store = ExactGroupHistoryManifestStore(root)
    plan = store.load_manifest_plan(manifest_plan_id, manifest_plan_sha256)
    approval = store.load_manifest_approval(manifest_approval_id, manifest_approval_sha256)
    request = store.load_manifest_request(approval.request_event_id, approval.request_event_sha256)
    _verify_manifest_controls(plan, request, approval, root)
    _verify_repository(repository, plan)
    source_time = _utc(source_binding_created_at_utc, "source_binding_created_at_utc")
    if source_time <= approval.approved_at_utc or source_time > datetime.now(UTC):
        raise IdentityExactGroupHistoryManifestError("source time must follow approval")
    lock = _acquire_lock(root, plan.plan_id, approval.approval_id)
    try:
        return _run_manifest_preflight_locked(
            root=root,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=source_time,
        )
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        os.close(lock)


def _run_manifest_preflight_locked(
    *,
    root: Path,
    store: ExactGroupHistoryManifestStore,
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
    source_time: datetime,
) -> S7ExactGroupHistoryManifestRun:
    completion_relative = manifest_completion_path(plan.plan_id, approval.approval_id)
    existing_completion = _load_existing(
        root, completion_relative, S7ExactGroupHistoryManifestCompletion.from_dict
    )
    if existing_completion is not None:
        _verify_completion_chain(root, existing_completion, plan, request, approval)
        receipt = _receipt(
            existing_completion.completion_id, completion_relative, existing_completion.content
        )
        return S7ExactGroupHistoryManifestRun(existing_completion, receipt, 0, 0, 0, True)

    intent_relative = manifest_run_intent_path(plan.plan_id, approval.approval_id)
    existing_intent = _load_existing(
        root, intent_relative, S7ExactGroupHistoryManifestRunIntent.from_dict
    )
    if existing_intent is None:
        intent = S7ExactGroupHistoryManifestRunIntent(
            manifest_plan_id=plan.plan_id,
            manifest_plan_sha256=plan.sha256,
            manifest_request_event_id=request.request_event_id,
            manifest_request_event_sha256=request.sha256,
            manifest_approval_id=approval.approval_id,
            manifest_approval_sha256=approval.sha256,
            approval_literal_sha256=approval.approval_literal_sha256,
            input_binding_digest=plan.input_binding_digest,
            execution_data_root=plan.execution_data_root,
            source_binding_created_by=plan.future_manifest_reader_actor,
            source_binding_created_at_utc=source_time,
            execution_plan_created_by=plan.future_execution_plan_actor,
            execution_request_created_by=plan.future_execution_request_actor,
        )
        intent_receipt = store.store_run_intent(intent)
    else:
        intent = existing_intent
        _verify_intent(intent, plan, request, approval)
        intent_receipt = _receipt(intent.intent_id, intent.relative_path, intent.content)

    binding_relative = source_binding_path(intent.intent_id)
    existing_binding = _load_existing(
        root, binding_relative, S7ExactGroupHistorySourceBinding.from_dict
    )
    if existing_intent is not None and existing_binding is None:
        raise IdentityExactGroupHistoryManifestError(
            "run intent exists without a complete source binding; fail closed"
        )
    if existing_binding is not None:
        _verify_binding_controls(existing_binding, intent, plan, request, approval)
        binding_receipt = _receipt(
            existing_binding.source_binding_id,
            existing_binding.relative_path,
            existing_binding.content,
        )
        return _recover_from_binding(
            root=root,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            intent=intent,
            intent_receipt=intent_receipt,
            binding=existing_binding,
            binding_receipt=binding_receipt,
            source_reads=0,
            lstats=0,
            recovered=True,
        )

    started = time.monotonic()
    counts = {
        "json_reads": 0,
        "lstats": 0,
        "parquet_bytes": 0,
        "json_bytes": 0,
        "source_lstat_bytes": 0,
        "source_rows": 0,
    }
    documents, raw_sources = _read_and_bind_sources(root, plan, counts, started)
    _check_caps(plan, counts, started)
    binding = S7ExactGroupHistorySourceBinding(
        created_by=intent.source_binding_created_by,
        created_at_utc=intent.source_binding_created_at_utc,
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_request_event_id=request.request_event_id,
        manifest_request_event_sha256=request.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        manifest_literal_sha256=approval.approval_literal_sha256,
        run_intent_id=intent.intent_id,
        run_intent_path=intent.relative_path,
        run_intent_sha256=intent.sha256,
        source_artifacts=raw_sources,
        execution_source_pins=normalize_raw_sources(raw_sources),
        manifest_documents=documents,
    )
    binding_receipt = store.store_source_binding(binding)
    return _recover_from_binding(
        root=root,
        store=store,
        plan=plan,
        request=request,
        approval=approval,
        intent=intent,
        intent_receipt=intent_receipt,
        binding=binding,
        binding_receipt=binding_receipt,
        source_reads=counts["json_reads"],
        lstats=counts["lstats"],
        recovered=False,
    )


def _recover_from_binding(
    *,
    root: Path,
    store: ExactGroupHistoryManifestStore,
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
    intent: S7ExactGroupHistoryManifestRunIntent,
    intent_receipt: StoredExactGroupHistoryManifestDocument,
    binding: S7ExactGroupHistorySourceBinding,
    binding_receipt: StoredExactGroupHistoryManifestDocument,
    source_reads: int,
    lstats: int,
    recovered: bool,
) -> S7ExactGroupHistoryManifestRun:
    execution_time = intent.source_binding_created_at_utc + timedelta(microseconds=1)
    execution_plan, execution_request = build_execution_controls(
        binding,
        plan,
        created_at_utc=execution_time,
        plan_actor=intent.execution_plan_created_by,
        request_actor=intent.execution_request_created_by,
    )
    plan_receipt = store.store_execution_plan(execution_plan)
    request_receipt = store.store_execution_request(execution_request)
    completion = S7ExactGroupHistoryManifestCompletion(
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        run_intent=intent_receipt,
        source_binding=binding_receipt,
        execution_plan=plan_receipt,
        execution_request=request_receipt,
        source_json_read_count=EXPECTED_MANIFEST_INPUT_COUNT,
        parquet_lstat_count=SOURCE_ARTIFACT_COUNT,
        parquet_content_bytes_read=0,
        completed_at_utc=execution_time + timedelta(microseconds=2),
    )
    completion_receipt = store.store_completion(completion)
    _verify_completion_chain(root, completion, plan, request, approval)
    return S7ExactGroupHistoryManifestRun(
        completion,
        completion_receipt,
        source_reads,
        lstats,
        0,
        recovered,
    )


def _read_and_bind_sources(
    root: Path,
    plan: S7ExactGroupHistoryManifestPlan,
    counts: dict[str, int],
    started: float,
) -> tuple[
    tuple[S7ExactGroupHistoryManifestDocumentRef, ...],
    tuple[S7ExactGroupHistoryRawSourceArtifactRef, ...],
]:
    by_kind: dict[str, tuple[bytes, dict[str, Any], S7ExactGroupHistoryManifestInputPin]] = {}
    refs = []
    for pin in plan.manifest_inputs:
        _check_live_caps(plan, counts, started)
        content, document = _read_exact_json(root, pin, counts, plan, started)
        by_kind[pin.kind] = (content, document, pin)
        refs.append(
            S7ExactGroupHistoryManifestDocumentRef(
                kind=pin.kind,
                logical_id=pin.logical_id,
                path=pin.path,
                sha256=pin.sha256,
                bytes=len(content),
            )
        )
    calendar = by_kind["xnys_calendar_manifest"][1]
    projected_sessions = _project_calendar_sessions(calendar)
    session_set = set(projected_sessions)

    inventory_candidate = by_kind["inventory_candidate_manifest"][1]
    inventory_completion = by_kind["inventory_completion_manifest"][1]
    directional_candidate = by_kind["directional_candidate_manifest"][1]
    directional_completion = by_kind["directional_completion_manifest"][1]
    inventory_refs = inventory_candidate.get("source_artifacts")
    if (
        inventory_candidate.get("candidate_id") != INVENTORY_CANDIDATE_ID
        or inventory_candidate.get("candidate_state") != "awaiting_review"
        or inventory_candidate.get("source_artifact_set_digest")
        != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        or not isinstance(inventory_refs, list)
        or len(inventory_refs) != SOURCE_ARTIFACT_COUNT
        or stable_digest(inventory_refs) != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        or inventory_completion.get("completion_id") != INVENTORY_COMPLETION_ID
        or inventory_completion.get("completion_state") != "awaiting_review"
        or directional_candidate.get("candidate_id") != DIRECTIONAL_CANDIDATE_ID
        or directional_candidate.get("candidate_state") != "awaiting_review"
        or directional_completion.get("completion_id") != DIRECTIONAL_COMPLETION_ID
        or directional_completion.get("completion_state") != "awaiting_review"
    ):
        raise IdentityExactGroupHistoryManifestError("inventory/directional lineage differs")
    for document in (
        inventory_candidate,
        inventory_completion,
        directional_candidate,
        directional_completion,
    ):
        capabilities = document.get("capabilities")
        if not isinstance(capabilities, dict) or any(capabilities.values()):
            raise IdentityExactGroupHistoryManifestError("upstream capabilities are unsafe")

    release_outputs: dict[tuple[str, date], Any] = {}
    for table, kind in (
        ("asset_observation_daily", "asset_release_manifest"),
        ("universe_source_daily", "universe_release_manifest"),
    ):
        release = ReleaseManifest.from_dict(by_kind[kind][1])
        expected_id = (
            ASSET_RELEASE_ID if table == "asset_observation_daily" else UNIVERSE_RELEASE_ID
        )
        if release.release_id != expected_id or release.table != table:
            raise IdentityExactGroupHistoryManifestError("release identity differs")
        data_outputs = [
            item
            for item in release.outputs
            if item.role is ArtifactRole.DATA and item.table == table
        ]
        if len(data_outputs) != SESSION_COUNT:
            raise IdentityExactGroupHistoryManifestError("release artifact count differs")
        for output in data_outputs:
            match = _SESSION.search(output.path)
            if match is None or output.row_count is None:
                raise IdentityExactGroupHistoryManifestError("release output metadata differs")
            key = (table, date.fromisoformat(match.group(1)))
            if key in release_outputs:
                raise IdentityExactGroupHistoryManifestError("release table/session repeats")
            release_outputs[key] = output
    for table in _TABLES:
        table_sessions = {
            session
            for release_table, session in release_outputs
            if release_table == table
        }
        if table_sessions != session_set:
            raise IdentityExactGroupHistoryManifestError(
                f"{table} release/calendar sessions differ"
            )

    raw_sources = []
    for inventory_raw in inventory_refs:
        item = _mapping(inventory_raw, "inventory source ref")
        expected_keys = {
            "bytes",
            "path",
            "release_id",
            "release_manifest_sha256",
            "row_count",
            "session_date",
            "sha256",
            "table",
        }
        _expect_keys(item, expected_keys, "inventory source ref")
        table = _text(item["table"], "inventory table")
        session = _date(item["session_date"], "inventory session")
        output = release_outputs.get((table, session))
        if (
            output is None
            or {
                "bytes": output.bytes,
                "path": output.path,
                "release_id": item["release_id"],
                "release_manifest_sha256": item["release_manifest_sha256"],
                "row_count": output.row_count,
                "session_date": session.isoformat(),
                "sha256": output.sha256,
                "table": table,
            }
            != item
        ):
            raise IdentityExactGroupHistoryManifestError("inventory/release projection differs")
        info = _checked_source_lstat(
            root,
            output.path,
            expected_row_count=output.row_count,
            plan=plan,
            counts=counts,
            started=started,
        )
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != output.bytes
        ):
            raise IdentityExactGroupHistoryManifestError("source lstat differs")
        contract = _CONTRACTS[table]
        raw_sources.append(
            S7ExactGroupHistoryRawSourceArtifactRef(
                table=table,
                session_date=session,
                release_id=_text(item["release_id"], "release ID"),
                release_manifest_sha256=_text(item["release_manifest_sha256"], "release SHA"),
                source_contract_id=contract.contract_id,
                source_schema_digest=contract.schema_digest,
                path=output.path,
                sha256=output.sha256,
                bytes=output.bytes,
                row_count=output.row_count,
                disk_size_bytes=info.st_size,
            )
        )
    sources = tuple(sorted(raw_sources))
    if (
        len(sources) != SOURCE_ARTIFACT_COUNT
        or stable_digest([item.project_inventory_8() for item in sources])
        != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        or sum(item.row_count for item in sources) != SOURCE_ROW_COUNT
        or sum(item.bytes for item in sources) != SOURCE_BYTES
        or counts["lstats"] != SOURCE_ARTIFACT_COUNT
        or counts["parquet_bytes"] != 0
    ):
        raise IdentityExactGroupHistoryManifestError("source-binding totals differ")
    return tuple(sorted(refs)), sources


def _project_calendar_sessions(calendar: Mapping[str, object]) -> tuple[date, ...]:
    """Validate a pinned calendar superset and select the exact S7 source interval."""

    sessions_raw = calendar.get("sessions")
    session_count = calendar.get("session_count")
    if (
        calendar.get("calendar_artifact_id") != CALENDAR_ARTIFACT_ID
        or not isinstance(sessions_raw, list)
        or not sessions_raw
        or type(session_count) is not int
        or session_count != len(sessions_raw)
    ):
        raise IdentityExactGroupHistoryManifestError("calendar lineage differs")

    calendar_start = _date(calendar.get("start_session"), "calendar start session")
    calendar_end = _date(calendar.get("end_session"), "calendar end session")
    sessions = tuple(
        _date(
            _mapping(item, f"calendar session {index}").get("session_date"),
            f"calendar session {index}",
        )
        for index, item in enumerate(sessions_raw)
    )
    if len(set(sessions)) != len(sessions):
        raise IdentityExactGroupHistoryManifestError("calendar sessions repeat")
    if any(left >= right for left, right in pairwise(sessions)):
        raise IdentityExactGroupHistoryManifestError(
            "calendar sessions are not strictly ordered"
        )
    if sessions[0] != calendar_start or sessions[-1] != calendar_end:
        raise IdentityExactGroupHistoryManifestError(
            "calendar range endpoints differ from its sessions"
        )
    if calendar_start > START_SESSION or calendar_end < END_SESSION:
        raise IdentityExactGroupHistoryManifestError(
            "calendar does not cover the exact S7 source interval"
        )

    projected = tuple(
        session for session in sessions if START_SESSION <= session <= END_SESSION
    )
    if (
        len(projected) != SESSION_COUNT
        or not projected
        or projected[0] != START_SESSION
        or projected[-1] != END_SESSION
    ):
        raise IdentityExactGroupHistoryManifestError(
            "calendar projection differs from the exact S7 source interval"
        )
    return projected


def _read_exact_json(
    root: Path,
    pin: S7ExactGroupHistoryManifestInputPin,
    counts: dict[str, int],
    plan: S7ExactGroupHistoryManifestPlan,
    started: float,
) -> tuple[bytes, dict[str, Any]]:
    _check_live_caps(plan, counts, started)
    try:
        path = safe_relative_path(root, pin.path)
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except (ArtifactError, OSError) as exc:
        raise IdentityExactGroupHistoryManifestError(
            f"source JSON missing or unsafe: {pin.path}"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise IdentityExactGroupHistoryManifestError(f"source JSON is not regular: {pin.path}")
        if (
            info.st_size > plan.resource_caps.json_manifest_bytes_hard_cap
            or counts["json_bytes"] + info.st_size > plan.resource_caps.json_manifest_bytes_hard_cap
        ):
            raise IdentityExactGroupHistoryManifestError(
                f"source JSON size cap exceeded before read: {pin.path}"
            )
        _check_live_caps(plan, counts, started)
        chunks = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
            counts["json_bytes"] += len(chunk)
            _check_live_caps(plan, counts, started)
        content = b"".join(chunks)
        final_info = os.fstat(descriptor)
        if (
            len(content) != info.st_size
            or final_info.st_dev != info.st_dev
            or final_info.st_ino != info.st_ino
            or final_info.st_size != info.st_size
            or final_info.st_mtime_ns != info.st_mtime_ns
        ):
            raise IdentityExactGroupHistoryManifestError(
                f"source JSON changed while read: {pin.path}"
            )
    finally:
        os.close(descriptor)
    counts["json_reads"] += 1
    _check_live_caps(plan, counts, started)
    if hashlib.sha256(content).hexdigest() != pin.sha256:
        raise IdentityExactGroupHistoryManifestError(f"source JSON SHA differs: {pin.path}")
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityExactGroupHistoryManifestError(f"source JSON invalid: {pin.path}") from exc
    if not isinstance(document, dict) or canonical_bytes(document) != content:
        raise IdentityExactGroupHistoryManifestError(f"source JSON is not canonical: {pin.path}")
    _check_live_caps(plan, counts, started)
    return content, document


def _checked_source_lstat(
    root: Path,
    relative: str,
    *,
    expected_row_count: int,
    plan: S7ExactGroupHistoryManifestPlan,
    counts: dict[str, int],
    started: float,
) -> os.stat_result:
    _check_live_caps(plan, counts, started)
    info = _safe_lstat(root, relative)
    counts["lstats"] += 1
    counts["source_lstat_bytes"] += info.st_size
    counts["source_rows"] += expected_row_count
    _check_live_caps(plan, counts, started)
    return info


def _safe_lstat(root: Path, relative: str) -> os.stat_result:
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityExactGroupHistoryManifestError(f"Parquet path is unsafe: {relative}") from exc
    try:
        return os.lstat(path)
    except OSError as exc:
        raise IdentityExactGroupHistoryManifestError(f"Parquet lstat failed: {relative}") from exc


def _check_caps(
    plan: S7ExactGroupHistoryManifestPlan,
    counts: Mapping[str, int],
    started: float,
) -> None:
    _check_live_caps(plan, counts, started)
    if (
        counts["json_reads"] != EXPECTED_MANIFEST_INPUT_COUNT
        or counts["lstats"] != SOURCE_ARTIFACT_COUNT
        or counts["source_lstat_bytes"] != SOURCE_BYTES
        or counts["source_rows"] != SOURCE_ROW_COUNT
    ):
        raise IdentityExactGroupHistoryManifestError("manifest preflight count differs")


def _check_live_caps(
    plan: S7ExactGroupHistoryManifestPlan,
    counts: Mapping[str, int],
    started: float,
) -> None:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak if peak > 10_000_000 else peak * 1024)
    if (
        counts.get("json_reads", 0) > EXPECTED_MANIFEST_INPUT_COUNT
        or counts.get("lstats", 0) > plan.resource_caps.lstat_hard_cap
        or counts.get("parquet_bytes", 0) != 0
        or counts.get("json_bytes", 0) > plan.resource_caps.json_manifest_bytes_hard_cap
        or counts.get("source_lstat_bytes", 0) > plan.resource_caps.source_bytes_hard_cap
        or counts.get("source_rows", 0) > plan.resource_caps.source_rows_hard_cap
        or peak_bytes > plan.resource_caps.rss_bytes_hard_cap
        or time.monotonic() - started > plan.resource_caps.wall_clock_seconds_hard_cap
    ):
        raise IdentityExactGroupHistoryManifestError("manifest preflight resource cap exceeded")


def _verify_manifest_controls(
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
    root: Path,
) -> None:
    if root != Path(plan.execution_data_root):
        raise IdentityExactGroupHistoryManifestError("execution data root binding differs")
    scope, preparation_plan, preparation_request, authorization = (
        _load_preparation_cross_binding_inputs(root, plan)
    )
    verify_exact_group_history_cross_bindings(
        scope=scope,
        preparation_plan=preparation_plan,
        preparation_request=preparation_request,
        preparation_authorization=authorization,
        manifest_plan=plan,
        manifest_request=request,
        manifest_approval=approval,
    )


def _verify_repository(repository: Path, plan: S7ExactGroupHistoryManifestPlan) -> None:
    try:
        if (
            Path(_git(repository, "rev-parse", "--show-toplevel")).resolve() != repository
            or _git(repository, "rev-parse", "HEAD") != plan.git_commit
            or _git(repository, "rev-parse", "HEAD^{tree}") != plan.git_tree
            or _git(repository, "status", "--porcelain", "--untracked-files=all")
        ):
            raise IdentityExactGroupHistoryManifestError("Git checkout differs")
        for pin in (*plan.runtime_files, *plan.verification_files):
            path = repository / pin.path
            if (
                not path.is_file()
                or path.is_symlink()
                or path.stat().st_size != pin.bytes
                or sha256_file(path) != pin.sha256
                or _git(repository, "rev-parse", f"HEAD:{pin.path}") != pin.git_blob
            ):
                raise IdentityExactGroupHistoryManifestError(f"Git file pin differs: {pin.path}")
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityExactGroupHistoryManifestError("Git verification failed") from exc


def _verify_intent(
    intent: S7ExactGroupHistoryManifestRunIntent,
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
) -> None:
    expected = (
        plan.plan_id,
        plan.sha256,
        request.request_event_id,
        request.sha256,
        approval.approval_id,
        approval.sha256,
        approval.approval_literal_sha256,
        plan.input_binding_digest,
        plan.execution_data_root,
        plan.future_manifest_reader_actor,
        plan.future_execution_plan_actor,
        plan.future_execution_request_actor,
    )
    actual = (
        intent.manifest_plan_id,
        intent.manifest_plan_sha256,
        intent.manifest_request_event_id,
        intent.manifest_request_event_sha256,
        intent.manifest_approval_id,
        intent.manifest_approval_sha256,
        intent.approval_literal_sha256,
        intent.input_binding_digest,
        intent.execution_data_root,
        intent.source_binding_created_by,
        intent.execution_plan_created_by,
        intent.execution_request_created_by,
    )
    if (
        actual != expected
        or intent.source_binding_created_at_utc <= approval.approved_at_utc
        or intent.source_binding_created_at_utc > datetime.now(UTC)
    ):
        raise IdentityExactGroupHistoryManifestError("run intent differs")


def _verify_binding_controls(
    binding: S7ExactGroupHistorySourceBinding,
    intent: S7ExactGroupHistoryManifestRunIntent,
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
) -> None:
    if (
        binding.created_by != intent.source_binding_created_by
        or binding.created_at_utc != intent.source_binding_created_at_utc
        or binding.manifest_plan_id != plan.plan_id
        or binding.manifest_plan_sha256 != plan.sha256
        or binding.manifest_request_event_id != request.request_event_id
        or binding.manifest_request_event_sha256 != request.sha256
        or binding.manifest_approval_id != approval.approval_id
        or binding.manifest_approval_sha256 != approval.sha256
        or binding.manifest_literal_sha256 != approval.approval_literal_sha256
        or binding.run_intent_id != intent.intent_id
        or binding.run_intent_sha256 != intent.sha256
        or [item.to_dict() for item in binding.manifest_documents]
        != [
            {
                **item.to_dict(),
                "bytes": next(
                    ref.bytes for ref in binding.manifest_documents if ref.kind == item.kind
                ),
            }
            for item in plan.manifest_inputs
        ]
    ):
        raise IdentityExactGroupHistoryManifestError("source binding controls differ")


def _verify_completion_chain(
    root: Path,
    completion: S7ExactGroupHistoryManifestCompletion,
    plan: S7ExactGroupHistoryManifestPlan,
    request: S7ExactGroupHistoryManifestRequest,
    approval: S7ExactGroupHistoryManifestApproval,
) -> None:
    if (
        completion.manifest_plan_id != plan.plan_id
        or completion.manifest_plan_sha256 != plan.sha256
        or completion.manifest_approval_id != approval.approval_id
        or completion.manifest_approval_sha256 != approval.sha256
    ):
        raise IdentityExactGroupHistoryManifestError("completion controls differ")
    intent = _verify_receipt(
        root, completion.run_intent, S7ExactGroupHistoryManifestRunIntent.from_dict
    )
    binding = _verify_receipt(
        root, completion.source_binding, S7ExactGroupHistorySourceBinding.from_dict
    )
    execution_plan = _verify_receipt(
        root, completion.execution_plan, S7ExactGroupHistoryExecutionPlan.from_dict
    )
    execution_request = _verify_receipt(
        root, completion.execution_request, S7ExactGroupHistoryExecutionRequest.from_dict
    )
    _verify_intent(intent, plan, request, approval)
    _verify_binding_controls(binding, intent, plan, request, approval)
    expected_plan, expected_request = build_execution_controls(
        binding,
        plan,
        created_at_utc=intent.source_binding_created_at_utc + timedelta(microseconds=1),
        plan_actor=intent.execution_plan_created_by,
        request_actor=intent.execution_request_created_by,
    )
    if (
        execution_plan.content != expected_plan.content
        or execution_request.content != expected_request.content
        or completion.completed_at_utc
        != expected_request.created_at_utc + timedelta(microseconds=1)
    ):
        raise IdentityExactGroupHistoryManifestError("completion outputs differ")


def _verify_receipt(
    root: Path, receipt: StoredExactGroupHistoryManifestDocument, parser: Any
) -> Any:
    value = _load_existing(root, receipt.path, parser)
    if isinstance(value, S7ExactGroupHistoryManifestRunIntent):
        logical_id = value.intent_id
    elif isinstance(value, S7ExactGroupHistorySourceBinding):
        logical_id = value.source_binding_id
    elif isinstance(value, S7ExactGroupHistoryExecutionPlan):
        logical_id = value.plan_id
    elif isinstance(value, S7ExactGroupHistoryExecutionRequest):
        logical_id = value.request_event_id
    else:
        logical_id = None
    if (
        value is None
        or len(value.content) != receipt.bytes
        or value.sha256 != receipt.sha256
        or logical_id != receipt.logical_id
        or value.relative_path != receipt.path
    ):
        raise IdentityExactGroupHistoryManifestError("completion receipt differs")
    return value


def _load_existing(root: Path, relative: str, parser: Any) -> Any | None:
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityExactGroupHistoryManifestError(str(exc)) from exc
    if not path.exists():
        return None
    if not path.is_file() or path.is_symlink():
        raise IdentityExactGroupHistoryManifestError(f"existing control is unsafe: {relative}")
    content = path.read_bytes()
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityExactGroupHistoryManifestError(
            f"existing control JSON invalid: {relative}"
        ) from exc
    value = parser(document)
    if value.content != content:
        raise IdentityExactGroupHistoryManifestError(f"existing control differs: {relative}")
    return value


def _receipt(
    logical_id: str, relative: str, content: bytes
) -> StoredExactGroupHistoryManifestDocument:
    return StoredExactGroupHistoryManifestDocument(
        logical_id=logical_id,
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _acquire_lock(root: Path, plan_id: str, approval_id: str) -> int:
    relative = (
        "manifests/silver/identity/exact-group-history-manifest-preflight-locks/"
        f"plan_id={plan_id}/approval_id={approval_id}.lock"
    )
    try:
        path = safe_relative_path(root, relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except (ArtifactError, OSError, BlockingIOError) as exc:
        raise IdentityExactGroupHistoryManifestError("manifest preflight already running") from exc


def preparation_authorization_path(authorization_id: str) -> str:
    _digest(authorization_id, "authorization ID")
    return (
        "manifests/silver/identity/exact-group-history-preparation-authorizations/"
        f"authorization_id={authorization_id}/manifest.json"
    )


def manifest_plan_path(plan_id: str) -> str:
    _digest(plan_id, "manifest plan ID")
    return (
        "manifests/silver/identity/exact-group-history-manifest-preflight-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


def manifest_request_path(request_id: str) -> str:
    _digest(request_id, "manifest request ID")
    return (
        "manifests/silver/identity/exact-group-history-manifest-preflight-requests/"
        f"request_event_id={request_id}.json"
    )


def manifest_approval_path(approval_id: str) -> str:
    _digest(approval_id, "manifest approval ID")
    return (
        "manifests/silver/identity/exact-group-history-manifest-preflight-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )


def manifest_run_intent_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "manifest plan ID")
    _digest(approval_id, "manifest approval ID")
    return (
        "manifests/silver/identity/exact-group-history-manifest-preflight-run-intents/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def source_binding_path(run_intent_id: str) -> str:
    _digest(run_intent_id, "run intent ID")
    return (
        "manifests/silver/identity/exact-group-history-source-bindings/"
        f"run_intent_id={run_intent_id}/manifest.json"
    )


def execution_plan_path(plan_id: str) -> str:
    _digest(plan_id, "execution plan ID")
    return (
        "manifests/silver/identity/exact-group-history-execution-plans/"
        f"plan_id={plan_id}/manifest.json"
    )


def execution_request_path(request_id: str) -> str:
    _digest(request_id, "execution request ID")
    return (
        "manifests/silver/identity/exact-group-history-execution-requests/"
        f"request_event_id={request_id}.json"
    )


def manifest_completion_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "manifest plan ID")
    _digest(approval_id, "manifest approval ID")
    return (
        "manifests/silver/identity/exact-group-history-manifest-preflight-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def _git(root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args], cwd=root, check=True, capture_output=True, text=True
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityExactGroupHistoryManifestError(
            f"Git command failed: {' '.join(args)}"
        ) from exc
    return result.stdout.strip()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityExactGroupHistoryManifestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result
