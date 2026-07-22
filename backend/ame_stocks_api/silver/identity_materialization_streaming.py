"""Source-bound streaming S7 four-table Full candidate materialization.

This module is intentionally candidate-only.  It cannot discover ``latest`` inputs,
make network requests, mutate a registry, or publish a release.  Production execution is
pinned to the reviewed frozen-registry projection adapter; arbitrary or caller-provided
adapters fail closed before source content is read.

The durable order is::

    source binding -> bounded size/profile preview -> Full plan -> approval request
    -> exact literal approval -> run intent -> two-pass candidate
    -> awaiting-review completion

Every source-content read happens after the immutable run intent is visible.
"""

from __future__ import annotations

import bisect
import ctypes
import errno
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import resource
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.asset_contract import UNIVERSE_SOURCE_DAILY_CONTRACT
from ame_stocks_api.silver.calendar_artifact import load_xnys_calendar_artifact
from ame_stocks_api.silver.identity_market_consistency import (
    MARKET_CLASSIFICATION_VERSION,
    IdentityMarketConsistencyError,
    verify_market_classification_candidate,
)
from ame_stocks_api.silver.identity_market_sequence import (
    CALENDAR_ARTIFACT_ID,
    CALENDAR_ARTIFACT_SHA256,
    IdentityMarketSequenceError,
    S7MarketSequenceResourceCaps,
)
from ame_stocks_api.silver.identity_market_sequence import (
    _candidate_from_completion as _load_gate_c_candidate_from_completion,
)
from ame_stocks_api.silver.identity_market_sequence import (
    _load_and_verify_authorization as _load_gate_c_authorization,
)
from ame_stocks_api.silver.identity_market_sequence import (
    _load_and_verify_inputs as _load_gate_c_inputs,
)
from ame_stocks_api.silver.identity_market_sequence import (
    _load_and_verify_plan as _load_gate_c_plan,
)
from ame_stocks_api.silver.identity_market_sequence import (
    _ResourceMonitor as _GateCResourceMonitor,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    REGISTRY_ORDER,
    ExactArtifactBinding,
    ExactSourceRow,
    LoadedRegistryReleaseSet,
    RegistryReleasePin,
    RegistryWorkflowError,
    load_registry_release_set,
)
from ame_stocks_api.silver.identity_resolution import (
    ASSET_ID_RULE_VERSION,
    canonical_asset_id,
)
from ame_stocks_api.silver.identity_resolution_contract import (
    S7_DERIVED_CONTRACTS,
    S7_RESOURCE_SHA256_BY_TABLE,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_S4_RELEASE_SET_MANIFEST_SHA256,
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentitySourceError,
    open_identity_source_bundle,
)

STREAMING_POLICY_VERSION: Final = "s7-four-table-streaming-full-candidate-v2"
STREAMING_PLAN_VERSION: Final = 1
STREAMING_REQUEST_VERSION: Final = 1
STREAMING_APPROVAL_VERSION: Final = 1
STREAMING_INTENT_VERSION: Final = 1
STREAMING_CANDIDATE_VERSION: Final = 1
STREAMING_COMPLETION_VERSION: Final = 1
PROFILE_POLICY_VERSION: Final = "s7-streaming-bounded-size-profile-v1"
PROFILE_SAMPLE_SESSION_HARD_CAP: Final = 25
PROFILE_AUTHORIZED_ACTION: Final = "execute_exact_s7_bounded_size_profile_once_to_awaiting_review"
STREAMING_STATE: Final = "awaiting_review"
STREAMING_AUTHORIZED_ACTION: Final = (
    "execute_exact_s7_streaming_four_table_full_once_to_awaiting_review_without_publish"
)
STREAMING_APPROVAL_LITERAL_VERSION: Final = "s7_streaming_full_approval_literal_v1"
S7_STANDING_AUTHORIZATION_TEXT: Final = (
    "为什么你就不能自己直接把S7运行完呢，我允许你这么做，只要中间不报错或者明显越界就可以自行继续"  # noqa: RUF001
)
S7_STANDING_AUTHORIZATION_SHA256: Final = hashlib.sha256(
    S7_STANDING_AUTHORIZATION_TEXT.encode("utf-8")
).hexdigest()
S7_STANDING_REAFFIRMATION_TEXT: Final = "批准"
S7_STANDING_REAFFIRMATION_SHA256: Final = hashlib.sha256(
    S7_STANDING_REAFFIRMATION_TEXT.encode("utf-8")
).hexdigest()
PRODUCTION_ADAPTER_VERSION: Final = "s7_frozen_registry_projection_adapter_v1"
PRODUCTION_GATE_B_REFERENCE_VERSION: Final = MARKET_CLASSIFICATION_VERSION

TABLE_ORDER: Final = (
    "asset_master",
    "ticker_alias",
    "issuer_master",
    "universe_daily",
)
SHARE_CLASS_ID_RULE_VERSION: Final = "ame_stocks_share_class_id_from_share_class_figi_v1"
ISSUER_ID_RULE_VERSION: Final = "ame_stocks_issuer_id_from_normalized_cik_v1"
TICKER_ALIAS_ID_RULE_VERSION: Final = (
    "ame_stocks_ticker_alias_id_from_observed_and_canonical_interval_v3"
)
ASSET_RESOLUTION_RULE_VERSION: Final = (
    "s7_asset_master_resolution_with_mutually_exclusive_composite_share_and_"
    "transition_registries_v4"
)
ALIAS_RESOLUTION_RULE_VERSION: Final = (
    "s7_ticker_alias_resolution_with_mutually_exclusive_composite_share_and_"
    "transition_registries_v4"
)
ISSUER_RESOLUTION_RULE_VERSION: Final = (
    "s7_issuer_master_reference_consensus_with_registry_isolation_v4"
)
UNIVERSE_RESOLUTION_RULE_VERSION: Final = (
    "s7_universe_resolution_with_mutually_exclusive_composite_share_and_transition_registries_v4"
)
POSITION_CONTINUITY_UNCERTAIN: Final = (
    "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
)
POSITION_CONTINUITY_RESOLVED: Final = "resolved_identity"
GATE_B_US: Final = frozenset({"known_us", "us_composite"})

RSS_HARD_CAP_BYTES: Final = 2 * 1024**3
DISK_HARD_FLOOR_BYTES: Final = 40 * 1024**3
PRODUCTION_SESSION_COUNT: Final = 2_513
PRODUCTION_ROW_COUNT: Final = 69_376_329

_FALSE_CAPABILITIES: Final = MappingProxyType(
    {
        "adjudication_authorized": False,
        "latest_discovery_authorized": False,
        "network_authorized": False,
        "publish_authorized": False,
        "registry_mutation_authorized": False,
    }
)
_RUNTIME_SOURCE_PATHS: Final = (
    "pyproject.toml",
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/providers/massive.py",
    "backend/ame_stocks_api/cli/silver_identity_market_sequence.py",
    "backend/ame_stocks_api/cli/silver_identity_materialization_publish.py",
    "backend/ame_stocks_api/cli/silver_identity_materialization_streaming.py",
    "backend/ame_stocks_api/silver/asset_contract.py",
    "backend/ame_stocks_api/silver/asset_full_run_plan.py",
    "backend/ame_stocks_api/silver/asset_publish_plan.py",
    "backend/ame_stocks_api/silver/asset_release_set.py",
    "backend/ame_stocks_api/silver/asset_source.py",
    "backend/ame_stocks_api/silver/assets.py",
    "backend/ame_stocks_api/silver/availability.py",
    "backend/ame_stocks_api/silver/calendar_artifact.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/exchange_contract.py",
    "backend/ame_stocks_api/silver/fixed_cases.py",
    "backend/ame_stocks_api/silver/identity_adjudication.py",
    "backend/ame_stocks_api/silver/identity_bounce.py",
    "backend/ame_stocks_api/silver/identity_cross_market.py",
    "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
    "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
    "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
    "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_plan.py",
    "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
    "backend/ame_stocks_api/silver/identity_exact_group_history_approval.py",
    "backend/ame_stocks_api/silver/identity_exact_group_history_contract.py",
    "backend/ame_stocks_api/silver/identity_exact_group_history_manifest.py",
    "backend/ame_stocks_api/silver/identity_exact_group_history_plan.py",
    "backend/ame_stocks_api/silver/identity_exact_group_history_runner.py",
    "backend/ame_stocks_api/silver/identity_market_consistency.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
    "backend/ame_stocks_api/silver/identity_market_inventory_plan.py",
    "backend/ame_stocks_api/silver/identity_market_sequence.py",
    "backend/ame_stocks_api/silver/identity_materialization.py",
    "backend/ame_stocks_api/silver/identity_materialization_publish.py",
    "backend/ame_stocks_api/silver/identity_materialization_streaming.py",
    "backend/ame_stocks_api/silver/identity_preview_plan.py",
    "backend/ame_stocks_api/silver/identity_preview_runner.py",
    "backend/ame_stocks_api/silver/identity_provider_evidence.py",
    "backend/ame_stocks_api/silver/identity_registry_exact_group_scopes.py",
    "backend/ame_stocks_api/silver/identity_registry_production.py",
    "backend/ame_stocks_api/silver/identity_registry_workflow.py",
    "backend/ame_stocks_api/silver/identity_relation_registries.py",
    "backend/ame_stocks_api/silver/identity_relation_registry_contract.py",
    "backend/ame_stocks_api/silver/identity_resolution.py",
    "backend/ame_stocks_api/silver/identity_resolution_contract.py",
    "backend/ame_stocks_api/silver/identity_source.py",
    "backend/ame_stocks_api/silver/identity_streaming_preview.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/silver/schema_resources/asset_master.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/asset_master.schema-v1.registry-v4.json",
    "backend/ame_stocks_api/silver/schema_resources/asset_observation_daily.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/asset_observation_version.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/asset_transition.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_adjudication.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_cross_market_adjudication.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_directional_raw_preview_slot.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_exact_group_history_review_slot.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/issuer_master.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/issuer_master.schema-v1.registry-v4.json",
    "backend/ame_stocks_api/silver/schema_resources/provider_composite_override.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/share_class_adjudication.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_alias.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_alias.schema-v1.registry-v4.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_change_event.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_event_request_status.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_overview_safe.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/universe_daily.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/universe_daily.schema-v1.registry-v4.json",
    "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json",
    "backend/ame_stocks_api/silver/ticker_event_contract.py",
    "backend/ame_stocks_api/silver/ticker_overview_contract.py",
    "backend/ame_stocks_api/silver/ticker_type_contract.py",
)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_ID = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_MIC = re.compile(r"^[A-Z0-9]{4}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")


class S7StreamingMaterializationError(RuntimeError):
    """Raised before an untrusted Full candidate can become visible."""


@dataclass(frozen=True, slots=True)
class ExactFilePin:
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative(self.path, "file pin path")
        _digest(self.sha256, "file pin SHA-256")
        _nonnegative(self.bytes, "file pin bytes")

    def to_dict(self) -> dict[str, object]:
        return {"bytes": self.bytes, "path": self.path, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: object) -> ExactFilePin:
        item = _mapping(value, "file pin")
        _expect_keys(item, {"bytes", "path", "sha256"}, "file pin")
        return cls(
            path=_relative(item["path"], "file pin path"),
            sha256=_digest(item["sha256"], "file pin SHA-256"),
            bytes=_nonnegative(item["bytes"], "file pin bytes"),
        )


@dataclass(frozen=True, slots=True)
class SessionArtifactPin:
    session_date: date
    row_count: int
    artifact: ExactFilePin

    def __post_init__(self) -> None:
        _native_date(self.session_date, "artifact session")
        _positive(self.row_count, "artifact row count")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> SessionArtifactPin:
        item = _mapping(value, "session artifact pin")
        _expect_keys(item, {"artifact", "row_count", "session_date"}, "session artifact pin")
        return cls(
            session_date=date.fromisoformat(_text(item["session_date"], "artifact session")),
            row_count=_positive(item["row_count"], "artifact row count"),
            artifact=ExactFilePin.from_dict(item["artifact"]),
        )


@dataclass(frozen=True, slots=True)
class GateBReferencePin:
    candidate_id: str
    candidate_state: str
    reference_version: str
    closed: bool
    manifest: ExactFilePin
    data: ExactFilePin

    def __post_init__(self) -> None:
        _digest(self.candidate_id, "Gate-B candidate ID")
        if self.candidate_state != STREAMING_STATE or self.closed is not True:
            raise S7StreamingMaterializationError("Gate-B reference is not closed awaiting review")
        _text(self.reference_version, "Gate-B reference version")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_state": self.candidate_state,
            "closed": self.closed,
            "data": self.data.to_dict(),
            "manifest": self.manifest.to_dict(),
            "reference_version": self.reference_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> GateBReferencePin:
        item = _mapping(value, "Gate-B reference pin")
        _expect_keys(
            item,
            {
                "candidate_id",
                "candidate_state",
                "closed",
                "data",
                "manifest",
                "reference_version",
            },
            "Gate-B reference pin",
        )
        return cls(
            candidate_id=_digest(item["candidate_id"], "Gate-B candidate ID"),
            candidate_state=_text(item["candidate_state"], "Gate-B candidate state"),
            reference_version=_text(item["reference_version"], "Gate-B reference version"),
            closed=_native_bool(item["closed"], "Gate-B closed marker"),
            manifest=ExactFilePin.from_dict(item["manifest"]),
            data=ExactFilePin.from_dict(item["data"]),
        )


@dataclass(frozen=True, slots=True)
class GateCCompletionPin:
    candidate_id: str
    completion_id: str
    completion_state: str
    complete: bool
    candidate_manifest: ExactFilePin
    completion_manifest: ExactFilePin
    identity_case_preview_id: str
    identity_case_preview_manifest: ExactFilePin
    identity_case_preview_available_session: date
    qa: ExactFilePin

    def __post_init__(self) -> None:
        _digest(self.candidate_id, "Gate-C candidate ID")
        _digest(self.completion_id, "Gate-C completion ID")
        _digest(self.identity_case_preview_id, "identity-case preview ID")
        _native_date(
            self.identity_case_preview_available_session,
            "identity-case preview available session",
        )
        if self.completion_state != STREAMING_STATE or self.complete is not True:
            raise S7StreamingMaterializationError("Gate-C completion is not closed awaiting review")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_manifest": self.candidate_manifest.to_dict(),
            "complete": self.complete,
            "completion_id": self.completion_id,
            "completion_manifest": self.completion_manifest.to_dict(),
            "completion_state": self.completion_state,
            "identity_case_preview_available_session": (
                self.identity_case_preview_available_session.isoformat()
            ),
            "identity_case_preview_id": self.identity_case_preview_id,
            "identity_case_preview_manifest": self.identity_case_preview_manifest.to_dict(),
            "qa": self.qa.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> GateCCompletionPin:
        item = _mapping(value, "Gate-C completion pin")
        _expect_keys(
            item,
            {
                "candidate_id",
                "candidate_manifest",
                "complete",
                "completion_id",
                "completion_manifest",
                "completion_state",
                "identity_case_preview_available_session",
                "identity_case_preview_id",
                "identity_case_preview_manifest",
                "qa",
            },
            "Gate-C completion pin",
        )
        return cls(
            candidate_id=_digest(item["candidate_id"], "Gate-C candidate ID"),
            completion_id=_digest(item["completion_id"], "Gate-C completion ID"),
            completion_state=_text(item["completion_state"], "Gate-C completion state"),
            complete=_native_bool(item["complete"], "Gate-C completion marker"),
            candidate_manifest=ExactFilePin.from_dict(item["candidate_manifest"]),
            completion_manifest=ExactFilePin.from_dict(item["completion_manifest"]),
            identity_case_preview_id=_digest(
                item["identity_case_preview_id"], "identity-case preview ID"
            ),
            identity_case_preview_manifest=ExactFilePin.from_dict(
                item["identity_case_preview_manifest"]
            ),
            identity_case_preview_available_session=date.fromisoformat(
                _text(
                    item["identity_case_preview_available_session"],
                    "identity-case preview available session",
                )
            ),
            qa=ExactFilePin.from_dict(item["qa"]),
        )


def _contract_pins() -> dict[str, dict[str, str]]:
    return {
        table: {
            "contract_id": S7_DERIVED_CONTRACTS[table].contract_id,
            "resource_sha256": S7_RESOURCE_SHA256_BY_TABLE[table],
            "schema_digest": S7_DERIVED_CONTRACTS[table].schema_digest,
        }
        for table in TABLE_ORDER
    }


def _source_release_pins() -> dict[str, dict[str, object]]:
    return {
        table: {
            "artifact_count": pin.artifact_count,
            "build_id": pin.build_id,
            "release_id": pin.release_id,
            "release_manifest_sha256": pin.release_manifest_sha256,
            "row_count": pin.row_count,
        }
        for table, pin in sorted(S7_SOURCE_PINS.items())
    }


@dataclass(frozen=True, slots=True)
class S7StreamingSourceBinding:
    mode: str
    cutoff_session: date
    s4_release_set_manifest: ExactFilePin
    membership_artifacts: tuple[SessionArtifactPin, ...]
    gate_b: GateBReferencePin
    gate_c: GateCCompletionPin
    registry_pins: tuple[RegistryReleasePin, ...]
    contract_approvals: tuple[ExactFilePin, ...]
    runtime_binding: Mapping[str, object]
    calendar_artifact_id: str
    calendar_artifact_sha256: str
    s4_release_set_id: str = S7_S4_RELEASE_SET_ID
    six_release_binding_id: str = S7_SIX_RELEASE_BINDING_ID
    source_release_pins: Mapping[str, Mapping[str, object]] = field(
        default_factory=_source_release_pins
    )
    contract_pins: Mapping[str, Mapping[str, str]] = field(default_factory=_contract_pins)

    def __post_init__(self) -> None:
        if self.mode not in {"fixture", "production"}:
            raise S7StreamingMaterializationError("source binding mode is invalid")
        _native_date(self.cutoff_session, "cutoff session")
        _digest(self.s4_release_set_id, "S4 release-set ID")
        _digest(self.six_release_binding_id, "six-release binding ID")
        _digest(self.calendar_artifact_id, "calendar artifact ID")
        _digest(self.calendar_artifact_sha256, "calendar artifact SHA-256")
        artifacts = tuple(self.membership_artifacts)
        if not artifacts or tuple(item.session_date for item in artifacts) != tuple(
            sorted({item.session_date for item in artifacts})
        ):
            raise S7StreamingMaterializationError(
                "membership sessions must be nonempty, sorted, and unique"
            )
        if artifacts[-1].session_date > self.cutoff_session:
            raise S7StreamingMaterializationError("membership session exceeds cutoff")
        if len({item.artifact.path for item in artifacts}) != len(artifacts):
            raise S7StreamingMaterializationError("membership artifact paths repeat")
        pins = tuple(self.registry_pins)
        if tuple(item.registry_name for item in pins) != REGISTRY_ORDER:
            raise S7StreamingMaterializationError("five registry pins are not in frozen order")
        if any(item.release_available_session > self.cutoff_session for item in pins):
            raise S7StreamingMaterializationError("registry release is unavailable at cutoff")
        approvals = tuple(self.contract_approvals)
        if len(approvals) != len(TABLE_ORDER) or len({item.path for item in approvals}) != len(
            approvals
        ):
            raise S7StreamingMaterializationError(
                "four distinct derived-contract approval receipts are required"
            )
        if dict(self.source_release_pins) != _source_release_pins():
            raise S7StreamingMaterializationError("S4/S5/S6 release pins changed")
        if dict(self.contract_pins) != _contract_pins():
            raise S7StreamingMaterializationError("four-table v4 contract pins changed")
        _validate_runtime_binding(self.runtime_binding)
        if self.mode == "production":
            universe = S7_SOURCE_PINS["universe_source_daily"]
            if (
                self.s4_release_set_id != S7_S4_RELEASE_SET_ID
                or self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID
                or self.s4_release_set_manifest.sha256 != S7_S4_RELEASE_SET_MANIFEST_SHA256
                or len(artifacts) != PRODUCTION_SESSION_COUNT
                or sum(item.row_count for item in artifacts) != universe.row_count
                or universe.row_count != PRODUCTION_ROW_COUNT
            ):
                raise S7StreamingMaterializationError("production S4 membership scope differs")
        object.__setattr__(self, "membership_artifacts", artifacts)
        object.__setattr__(self, "registry_pins", pins)
        object.__setattr__(self, "contract_approvals", approvals)
        object.__setattr__(
            self,
            "runtime_binding",
            MappingProxyType(dict(self.runtime_binding)),
        )
        object.__setattr__(
            self,
            "source_release_pins",
            MappingProxyType(
                {
                    key: MappingProxyType(dict(value))
                    for key, value in self.source_release_pins.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "contract_pins",
            MappingProxyType(
                {key: MappingProxyType(dict(value)) for key, value in self.contract_pins.items()}
            ),
        )

    @property
    def session_count(self) -> int:
        return len(self.membership_artifacts)

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.membership_artifacts)

    @property
    def declared_source_bytes(self) -> int:
        fixed = (
            self.s4_release_set_manifest.bytes
            + self.gate_b.manifest.bytes
            + self.gate_b.data.bytes
            + self.gate_c.candidate_manifest.bytes
            + self.gate_c.completion_manifest.bytes
            + self.gate_c.identity_case_preview_manifest.bytes
            + self.gate_c.qa.bytes
            + sum(item.manifest_bytes for item in self.registry_pins)
        )
        return fixed + sum(item.artifact.bytes for item in self.membership_artifacts)

    @property
    def source_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/s7-streaming-full-source-bindings/"
            f"source_binding_id={self.source_binding_id}/manifest.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "calendar_artifact_id": self.calendar_artifact_id,
            "calendar_artifact_sha256": self.calendar_artifact_sha256,
            "contract_pins": {key: dict(value) for key, value in self.contract_pins.items()},
            "contract_approvals": [item.to_dict() for item in self.contract_approvals],
            "cutoff_session": self.cutoff_session.isoformat(),
            "gate_b": self.gate_b.to_dict(),
            "gate_c": self.gate_c.to_dict(),
            "membership_artifacts": [item.to_dict() for item in self.membership_artifacts],
            "mode": self.mode,
            "policy_version": STREAMING_POLICY_VERSION,
            "registry_pins": [item.to_dict() for item in self.registry_pins],
            "runtime_binding": dict(self.runtime_binding),
            "s4_release_set_id": self.s4_release_set_id,
            "s4_release_set_manifest": self.s4_release_set_manifest.to_dict(),
            "six_release_binding_id": self.six_release_binding_id,
            "source_release_pins": {
                key: dict(value) for key, value in self.source_release_pins.items()
            },
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self.logical_payload(),
            "declared_source_bytes": self.declared_source_bytes,
            "row_count": self.row_count,
            "session_count": self.session_count,
            "source_binding_id": self.source_binding_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7StreamingSourceBinding:
        item = _mapping(value, "streaming source binding")
        expected = {
            "calendar_artifact_id",
            "calendar_artifact_sha256",
            "contract_pins",
            "contract_approvals",
            "cutoff_session",
            "declared_source_bytes",
            "gate_b",
            "gate_c",
            "membership_artifacts",
            "mode",
            "policy_version",
            "registry_pins",
            "row_count",
            "runtime_binding",
            "s4_release_set_id",
            "s4_release_set_manifest",
            "session_count",
            "six_release_binding_id",
            "source_binding_id",
            "source_release_pins",
        }
        _expect_keys(item, expected, "streaming source binding")
        if item["policy_version"] != STREAMING_POLICY_VERSION:
            raise S7StreamingMaterializationError("source binding policy differs")
        binding = cls(
            mode=_text(item["mode"], "source binding mode"),
            cutoff_session=date.fromisoformat(_text(item["cutoff_session"], "cutoff session")),
            s4_release_set_manifest=ExactFilePin.from_dict(item["s4_release_set_manifest"]),
            membership_artifacts=tuple(
                SessionArtifactPin.from_dict(value)
                for value in _array(item["membership_artifacts"], "membership artifacts")
            ),
            gate_b=GateBReferencePin.from_dict(item["gate_b"]),
            gate_c=GateCCompletionPin.from_dict(item["gate_c"]),
            registry_pins=tuple(
                RegistryReleasePin.from_dict(value)
                for value in _array(item["registry_pins"], "registry pins")
            ),
            contract_approvals=tuple(
                ExactFilePin.from_dict(value)
                for value in _array(item["contract_approvals"], "contract approvals")
            ),
            runtime_binding=_mapping(item["runtime_binding"], "runtime binding"),
            calendar_artifact_id=_digest(item["calendar_artifact_id"], "calendar ID"),
            calendar_artifact_sha256=_digest(item["calendar_artifact_sha256"], "calendar SHA-256"),
            s4_release_set_id=_digest(item["s4_release_set_id"], "S4 release-set ID"),
            six_release_binding_id=_digest(
                item["six_release_binding_id"], "six-release binding ID"
            ),
            source_release_pins=_mapping(item["source_release_pins"], "source release pins"),
            contract_pins=_mapping(item["contract_pins"], "contract pins"),
        )
        if (
            item["source_binding_id"] != binding.source_binding_id
            or item["session_count"] != binding.session_count
            or item["row_count"] != binding.row_count
            or item["declared_source_bytes"] != binding.declared_source_bytes
        ):
            raise S7StreamingMaterializationError("source binding derived fields differ")
        return binding


@dataclass(frozen=True, slots=True)
class StreamingResourceCaps:
    source_bytes_cap: int
    output_bytes_cap: int
    tmp_bytes_cap: int
    wall_clock_seconds_cap: int
    session_count_cap: int
    row_count_cap: int
    per_session_row_cap: int
    batch_row_cap: int = 65_536
    rss_bytes_cap: int = RSS_HARD_CAP_BYTES
    disk_free_floor_bytes: int = DISK_HARD_FLOOR_BYTES
    worker_count: int = 1

    def __post_init__(self) -> None:
        for key, value in self.to_dict().items():
            if type(value) is not int or value <= 0:
                raise S7StreamingMaterializationError(f"resource cap {key} must be positive")
        if self.rss_bytes_cap > RSS_HARD_CAP_BYTES:
            raise S7StreamingMaterializationError("RSS cap exceeds the 2 GiB hard ceiling")
        if self.disk_free_floor_bytes < DISK_HARD_FLOOR_BYTES:
            raise S7StreamingMaterializationError("disk floor is below the 40 GiB hard floor")
        if self.worker_count != 1:
            raise S7StreamingMaterializationError("streaming Full is single-worker only")
        if self.batch_row_cap > self.per_session_row_cap:
            raise S7StreamingMaterializationError("batch row cap exceeds per-session cap")

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_row_cap": self.batch_row_cap,
            "disk_free_floor_bytes": self.disk_free_floor_bytes,
            "output_bytes_cap": self.output_bytes_cap,
            "per_session_row_cap": self.per_session_row_cap,
            "row_count_cap": self.row_count_cap,
            "rss_bytes_cap": self.rss_bytes_cap,
            "session_count_cap": self.session_count_cap,
            "source_bytes_cap": self.source_bytes_cap,
            "tmp_bytes_cap": self.tmp_bytes_cap,
            "wall_clock_seconds_cap": self.wall_clock_seconds_cap,
            "worker_count": self.worker_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> StreamingResourceCaps:
        item = _mapping(value, "resource caps")
        _expect_keys(
            item,
            {
                "batch_row_cap",
                "disk_free_floor_bytes",
                "output_bytes_cap",
                "per_session_row_cap",
                "row_count_cap",
                "rss_bytes_cap",
                "session_count_cap",
                "source_bytes_cap",
                "tmp_bytes_cap",
                "wall_clock_seconds_cap",
                "worker_count",
            },
            "resource caps",
        )
        return cls(**{key: _positive(value, key) for key, value in item.items()})


@dataclass(frozen=True, slots=True)
class StoredControl:
    logical_id: str
    receipt: ExactFilePin


@dataclass(frozen=True, slots=True)
class StreamingApprovalRequest:
    plan_id: str
    plan: ExactFilePin
    source_binding_id: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    requested_at_utc: datetime
    requested_by: str

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("source binding ID", self.source_binding_id),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file-set digest", self.runtime_file_set_digest),
        ):
            _digest(value, label)
        _utc(self.requested_at_utc, "approval request time")
        _text(self.requested_by, "approval requester")

    @property
    def request_id(self) -> str:
        payload = self.logical_payload()
        payload.pop("requested_at_utc")
        payload.pop("requested_by")
        payload["artifact_type"] = "s7_streaming_four_table_full_approval_request_slot"
        return stable_digest(payload)

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/s7-streaming-full-approval-requests/"
            f"request_id={self.request_id}/manifest.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_streaming_four_table_full_approval_request",
            "authorized_action": STREAMING_AUTHORIZED_ACTION,
            "false_capabilities": dict(_FALSE_CAPABILITIES),
            "literal_version": STREAMING_APPROVAL_LITERAL_VERSION,
            "plan": self.plan.to_dict(),
            "plan_id": self.plan_id,
            "request_version": STREAMING_REQUEST_VERSION,
            "requested_at_utc": _utc_text(self.requested_at_utc),
            "requested_by": self.requested_by,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "source_binding_id": self.source_binding_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.logical_payload(), "request_id": self.request_id}


@dataclass(frozen=True, slots=True)
class StreamingExactApproval:
    request_id: str
    request: ExactFilePin
    plan_id: str
    exact_literal_sha256: str
    approved_at_utc: datetime
    approved_by: str
    authorization_mode: str
    approval_availability: Mapping[str, object]
    standing_authorization: Mapping[str, str] | None = None
    standing_reaffirmation: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        _digest(self.request_id, "approval request ID")
        _digest(self.plan_id, "approval plan ID")
        _digest(self.exact_literal_sha256, "approval literal SHA-256")
        _utc(self.approved_at_utc, "approval time")
        _text(self.approved_by, "approver")
        if self.authorization_mode not in {"exact_literal", "standing_s7"}:
            raise S7StreamingMaterializationError("approval authorization mode is invalid")
        availability = _mapping(self.approval_availability, "approval availability")
        _expect_keys(
            availability,
            {
                "approval_recorded_at_utc",
                "calendar_artifact_id",
                "calendar_artifact_sha256",
                "first_xnys_open_utc",
                "rule",
                "source_available_session",
            },
            "approval availability",
        )
        if self.authorization_mode == "exact_literal":
            if self.standing_authorization is not None or self.standing_reaffirmation is not None:
                raise S7StreamingMaterializationError("exact approval cannot claim standing text")
        elif dict(self.standing_authorization or {}) != {
            "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
            "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        } or dict(self.standing_reaffirmation or {}) != {
            "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
            "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        }:
            raise S7StreamingMaterializationError("standing authorization text differs")
        object.__setattr__(self, "approval_availability", MappingProxyType(availability))
        if self.standing_authorization is not None:
            object.__setattr__(
                self,
                "standing_authorization",
                MappingProxyType(dict(self.standing_authorization)),
            )
        if self.standing_reaffirmation is not None:
            object.__setattr__(
                self,
                "standing_reaffirmation",
                MappingProxyType(dict(self.standing_reaffirmation)),
            )

    @property
    def approval_id(self) -> str:
        if self.authorization_mode == "standing_s7":
            return stable_digest(
                {
                    "approval_version": STREAMING_APPROVAL_VERSION,
                    "artifact_type": "s7_streaming_four_table_full_standing_approval_slot",
                    "authorization_mode": self.authorization_mode,
                    "authorized_action": STREAMING_AUTHORIZED_ACTION,
                    "literal_version": STREAMING_APPROVAL_LITERAL_VERSION,
                    "plan_id": self.plan_id,
                    "request": self.request.to_dict(),
                    "request_id": self.request_id,
                    "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
                    "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
                }
            )
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/s7-streaming-full-approvals/"
            f"approval_id={self.approval_id}/manifest.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_version": STREAMING_APPROVAL_VERSION,
            "approval_availability": dict(self.approval_availability),
            "approved_at_utc": _utc_text(self.approved_at_utc),
            "approved_by": self.approved_by,
            "artifact_type": "s7_streaming_four_table_full_exact_approval",
            "authorization_mode": self.authorization_mode,
            "authorized_action": STREAMING_AUTHORIZED_ACTION,
            "exact_literal_sha256": self.exact_literal_sha256,
            "false_capabilities": dict(_FALSE_CAPABILITIES),
            "literal_version": STREAMING_APPROVAL_LITERAL_VERSION,
            "plan_id": self.plan_id,
            "request": self.request.to_dict(),
            "request_id": self.request_id,
            "standing_authorization": (
                dict(self.standing_authorization)
                if self.standing_authorization is not None
                else None
            ),
            "standing_reaffirmation": (
                dict(self.standing_reaffirmation)
                if self.standing_reaffirmation is not None
                else None
            ),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.logical_payload(), "approval_id": self.approval_id}


@dataclass(frozen=True, slots=True)
class StreamingFullRunResult:
    plan_id: str
    approval_id: str
    candidate_id: str
    candidate_path: str
    completion_id: str
    completion_path: str
    session_count: int
    source_row_count: int
    table_row_counts: Mapping[str, int]
    raw_collision_rows: int
    idempotent: bool
    state: str = STREAMING_STATE


@dataclass(frozen=True, slots=True)
class ResolutionProjection:
    """Adapter output for one exact S4 membership row.

    The streaming engine rechecks registry matches, Gate-B eligibility, source-row
    preservation, canonical-ID derivation, ShareClass ordering, and transition lineage.
    """

    selected_source_record_id: str
    observed_composite_market_code: str | None
    observed_asset_id: str | None
    canonical_composite_figi: str | None
    canonical_composite_market_code: str | None
    canonical_share_class_figi: str | None
    canonical_cik_normalized: str | None
    asset_id: str | None
    share_class_id: str | None
    issuer_id: str | None
    identity_resolution_status: str
    identity_resolution_method: str
    identity_disposition: str
    identity_case_id: str | None
    identity_case_available_session: date | None
    identity_adjudication_id: str | None
    cross_market_scope_id: str | None
    cross_market_adjudication_id: str | None
    cross_market_adjudication_available_session: date | None
    cross_market_classification_status: str
    identity_case_resolution_role: str | None
    adjudication_available_session: date | None
    backtest_identity_eligible: bool
    current_reference_factor_eligible: bool
    security_type_scope: str
    identity_evidence_available_session: date
    provider_composite_override_id: str | None
    provider_composite_override_available_session: date | None
    share_class_adjudication_id: str | None
    share_class_adjudication_available_session: date | None
    asset_transition_ids: tuple[str, ...]
    composite_registry_match_count: int
    composite_registry_collision: bool

    def __post_init__(self) -> None:
        _digest(self.selected_source_record_id, "projection source record ID")
        for value, label in (
            (self.observed_asset_id, "observed asset ID"),
            (self.asset_id, "asset ID"),
            (self.share_class_id, "share class ID"),
            (self.issuer_id, "issuer ID"),
            (self.identity_case_id, "identity case ID"),
            (self.identity_adjudication_id, "identity adjudication ID"),
            (self.cross_market_scope_id, "cross-market scope ID"),
            (self.cross_market_adjudication_id, "cross-market adjudication ID"),
            (self.provider_composite_override_id, "provider Composite override ID"),
            (self.share_class_adjudication_id, "ShareClass adjudication ID"),
        ):
            if value is not None:
                _digest(value, label)
        for value, label in (
            (self.canonical_composite_figi, "canonical Composite FIGI"),
            (self.canonical_share_class_figi, "canonical ShareClass FIGI"),
        ):
            if value is not None and _FIGI.fullmatch(value) is None:
                raise S7StreamingMaterializationError(f"{label} is invalid")
        if (
            self.canonical_cik_normalized is not None
            and _CIK.fullmatch(self.canonical_cik_normalized) is None
        ):
            raise S7StreamingMaterializationError("canonical CIK is invalid")
        for value in (
            self.backtest_identity_eligible,
            self.current_reference_factor_eligible,
            self.composite_registry_collision,
        ):
            if type(value) is not bool:
                raise S7StreamingMaterializationError("projection booleans must be native")
        _nonnegative(self.composite_registry_match_count, "Composite registry match count")
        if self.composite_registry_collision != (self.composite_registry_match_count > 1):
            raise S7StreamingMaterializationError("projection collision count/state differs")
        transitions = tuple(sorted(set(self.asset_transition_ids)))
        if transitions != self.asset_transition_ids:
            raise S7StreamingMaterializationError("transition IDs must be sorted and unique")
        for item in transitions:
            _digest(item, "asset transition ID")


class StreamingProjectionAdapter(Protocol):
    """Strict projection boundary pending a reviewed production implementation."""

    adapter_version: str
    production_ready: bool

    def resolve_session(
        self,
        source: pa.Table,
        *,
        gate_b_by_composite: Mapping[str, Mapping[str, object]],
        registries: LoadedRegistryReleaseSet,
        binding: S7StreamingSourceBinding,
    ) -> Sequence[ResolutionProjection]: ...


class FrozenRegistryProjectionAdapter:
    """Deterministic production projection over exact Gate B and registry releases.

    S4 supplies provider-observed membership and identity fields.  S5/S6 and Gate C are
    consumed only through the already approved registry/release lineage bound into the
    source binding; current S6 labels never make a row factor eligible.  This adapter has
    no filesystem, clock, network, ``latest``, or caller decision-map input.
    """

    adapter_version = PRODUCTION_ADAPTER_VERSION
    production_ready = True

    def resolve_session(
        self,
        source: pa.Table,
        *,
        gate_b_by_composite: Mapping[str, Mapping[str, object]],
        registries: LoadedRegistryReleaseSet,
        binding: S7StreamingSourceBinding,
    ) -> tuple[ResolutionProjection, ...]:
        if source.schema != UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema:
            raise S7StreamingMaterializationError("projection source schema differs")
        return tuple(
            _frozen_registry_projection(
                row,
                gate_b_by_composite=gate_b_by_composite,
                registries=registries,
                binding=binding,
            )
            for row in source.to_pylist()
        )


def _frozen_registry_projection(
    source: Mapping[str, object],
    *,
    gate_b_by_composite: Mapping[str, Mapping[str, object]],
    registries: LoadedRegistryReleaseSet,
    binding: S7StreamingSourceBinding,
) -> ResolutionProjection:
    session = _native_date(source.get("session_date"), "projection source session")
    source_id = _digest(source.get("selected_source_record_id"), "projection source ID")
    ticker = _text(source.get("ticker"), "projection ticker")
    observed = _optional_figi(source.get("composite_figi"), "projection observed Composite")
    observed_share = _optional_figi(
        source.get("share_class_figi"), "projection observed ShareClass"
    )
    cik = _normalize_cik(source.get("cik"))
    issuer = _issuer_id(cik) if cik is not None else None
    observed_asset = canonical_asset_id(observed) if observed is not None else None
    gate_row = gate_b_by_composite.get(observed) if observed is not None else None
    observed_market = (
        _optional_text(gate_row.get("selected_market_code"), "Gate-B market code")
        if gate_row is not None
        else None
    )
    gate_selected_share = (
        _optional_figi(
            gate_row.get("selected_share_class_figi"),
            "Gate-B selected ShareClass",
        )
        if gate_row is not None
        else None
    )
    gate_share_conflict = (
        _native_bool(
            gate_row.get("relation_share_class_conflict", False),
            "Gate-B relation ShareClass conflict",
        )
        if gate_row is not None
        else False
    )
    evidence_sessions = [
        _native_date(source.get("source_available_session"), "membership availability")
    ]
    exact_source: ExactSourceRow | None = None
    matches: tuple[tuple[str, str], ...] = ()
    share_matches: tuple[str, ...] = ()
    transition_matches: tuple[str, ...] = ()
    if observed is not None:
        if gate_row is None:
            raise S7StreamingMaterializationError(
                "Gate-B reference inventory has an unattempted Composite"
            )
        evidence_sessions.append(
            _native_date(gate_row["source_available_session"], "Gate-B availability")
        )
        exact_source = ExactSourceRow(
            session_date=session,
            source_record_id=source_id,
            source_dataset="universe_source_daily",
            source_s4_release_set_id=binding.s4_release_set_id,
            provider_id="massive",
            provider_market=_text(source.get("market"), "provider market"),
            provider_locale=_text(source.get("locale"), "provider locale"),
            ticker=ticker,
            observed_composite_figi=observed,
            observed_share_class_figi=observed_share,
            primary_exchange_mic=_optional_mic(
                source.get("primary_exchange_mic"), "primary exchange MIC"
            ),
        )
        matches = tuple(
            registries.composite_matches(exact_source, cutoff_session=binding.cutoff_session)
        )
        share_matches = tuple(
            registries.by_name("share_class_adjudication").decision_ids_for_exact_source_row(
                exact_source, cutoff_session=binding.cutoff_session
            )
        )
        transition_matches = tuple(
            registries.by_name("asset_transition").decision_ids_for_exact_source_row(
                exact_source, cutoff_session=binding.cutoff_session
            )
        )
    canonical: str | None = None
    canonical_market: str | None = None
    asset: str | None = None
    identity_status = "unresolved"
    identity_method = "cross_market_composite_pending_unresolved"
    disposition = "not_applicable_no_observed_composite"
    cross_status = "not_classified"
    identity_case_id: str | None = None
    identity_case_available: date | None = None
    identity_decision_id: str | None = None
    cross_scope_id: str | None = None
    cross_decision_id: str | None = None
    cross_available: date | None = None
    provider_override_id: str | None = None
    provider_override_available: date | None = None
    adjudication_available: date | None = None
    case_role: str | None = None
    if len(matches) > 1:
        identity_status = "unresolved_registry_collision"
        identity_method = "registry_collision_unresolved"
        disposition = "registry_collision_unresolved"
        cross_status = "known_non_us_adjudicated_unresolved"
        for registry_name, _ in matches:
            evidence_sessions.append(registries.by_name(registry_name).release_available_session)
    elif len(matches) == 1:
        assert exact_source is not None
        registry_name, decision_id = matches[0]
        release = registries.by_name(registry_name)
        decision = release.require_exact_source_row(
            decision_id,
            exact_source,
            cutoff_session=binding.cutoff_session,
        )
        canonical, asset = _composite_decision_target(registry_name, decision)
        if registry_name == "identity_adjudication":
            identity_decision_id = decision_id
            identity_case_id = _optional_digest(
                decision.get("identity_case_id"), "identity case ID"
            )
            identity_case_available = _optional_date(
                decision.get("identity_case_available_session"),
                "identity case availability",
            )
            adjudication_available = _native_date(
                decision.get("adjudication_available_session"),
                "identity adjudication availability",
            )
            raw_disposition = _text(decision.get("disposition"), "identity disposition")
            if canonical is None:
                disposition = "adjudicated_unresolved"
                identity_method = "provider_figi_bounce_adjudicated_unresolved"
                identity_status = "unresolved"
            else:
                disposition = raw_disposition
                identity_method = (
                    "approved_genuine_transition"
                    if raw_disposition == "confirmed_genuine_transition"
                    else "approved_provider_contamination_override"
                )
                identity_status = "resolved_approved_override"
                case_role = (
                    "inverse_middle_is_canonical_us"
                    if canonical == observed
                    else "contaminated_middle_episode"
                )
        elif registry_name == "identity_cross_market_adjudication":
            cross_decision_id = decision_id
            cross_scope_id = _digest(decision.get("cross_market_scope_id"), "cross-market scope ID")
            cross_available = _native_date(
                decision.get("adjudication_available_session"),
                "cross-market adjudication availability",
            )
            if canonical is None:
                disposition = "cross_market_adjudicated_unresolved"
                identity_method = "cross_market_composite_adjudicated_unresolved"
                identity_status = "unresolved"
                cross_status = "known_non_us_adjudicated_unresolved"
            else:
                disposition = "confirmed_provider_contamination"
                identity_method = "approved_cross_market_provider_contamination_override"
                identity_status = "resolved_approved_override"
                cross_status = "known_non_us_overridden"
        elif registry_name == "provider_composite_override":
            provider_override_id = decision_id
            provider_override_available = _native_date(
                decision.get("override_available_session"),
                "provider override availability",
            )
            disposition = "confirmed_genuine_transition"
            identity_method = "approved_provider_contamination_override"
            identity_status = "resolved_approved_override"
        else:  # pragma: no cover - the registry loader freezes this set
            raise S7StreamingMaterializationError("unsupported Composite registry")
        canonical_market = _optional_text(
            decision.get("canonical_composite_market_code"),
            "canonical Composite market",
        )
        for value in (
            identity_case_available,
            adjudication_available,
            cross_available,
            provider_override_available,
            release.release_available_session,
        ):
            if value is not None:
                evidence_sessions.append(value)
    elif observed is not None:
        assert gate_row is not None
        classification = _text(gate_row.get("classification"), "Gate-B classification")
        if classification in GATE_B_US:
            canonical = observed
            canonical_market = observed_market
            asset = canonical_asset_id(observed)
            identity_status = "resolved_strong"
            identity_method = "source_composite_figi_exact"
            disposition = "observed_consistent"
            cross_status = "known_us"
        else:
            identity_method = "cross_market_composite_pending_unresolved"
            disposition = "pending_cross_market_review"
            cross_status = (
                "known_non_us_foreign_locale"
                if classification == "known_non_us"
                else "known_non_us_pending"
            )
    if canonical is None and share_matches:
        raise S7StreamingMaterializationError(
            "ShareClass decision preceded unique Composite resolution"
        )
    canonical_share = observed_share if canonical is not None else None
    share_id = _share_class_id(canonical_share) if canonical_share is not None else None
    share_decision_id: str | None = None
    share_available: date | None = None
    unresolved_gate_share_conflict = False
    if len(share_matches) > 1:
        canonical_share = None
        share_id = None
        asset = None
        canonical = None
        canonical_market = None
        identity_status = "resolved_conflicted"
        identity_method = "registry_collision_unresolved"
        disposition = "registry_collision_unresolved"
        evidence_sessions.append(
            registries.by_name("share_class_adjudication").release_available_session
        )
    elif len(share_matches) == 1:
        if canonical is None or exact_source is None:
            raise S7StreamingMaterializationError(
                "ShareClass decision preceded unique Composite resolution"
            )
        share_decision_id = share_matches[0]
        release = registries.by_name("share_class_adjudication")
        decision = release.require_exact_source_row(
            share_decision_id,
            exact_source,
            cutoff_session=binding.cutoff_session,
        )
        canonical_share = _figi(decision.get("canonical_share_class_figi"), "canonical ShareClass")
        share_id = _digest(decision.get("canonical_share_class_id"), "ShareClass ID")
        share_available = _native_date(
            decision.get("adjudication_available_session"),
            "ShareClass adjudication availability",
        )
        evidence_sessions.extend((share_available, release.release_available_session))
    elif gate_share_conflict and (
        observed_share is None
        or gate_selected_share is None
        or observed_share != gate_selected_share
    ):
        canonical_share = None
        share_id = None
        identity_status = "resolved_conflicted"
        unresolved_gate_share_conflict = True
    eligible = (
        canonical is not None
        and asset is not None
        and len(share_matches) <= 1
        and not unresolved_gate_share_conflict
    )
    return ResolutionProjection(
        selected_source_record_id=source_id,
        observed_composite_market_code=observed_market,
        observed_asset_id=observed_asset,
        canonical_composite_figi=canonical,
        canonical_composite_market_code=canonical_market,
        canonical_share_class_figi=canonical_share,
        canonical_cik_normalized=cik,
        asset_id=asset,
        share_class_id=share_id,
        issuer_id=issuer,
        identity_resolution_status=identity_status,
        identity_resolution_method=identity_method,
        identity_disposition=disposition,
        identity_case_id=identity_case_id,
        identity_case_available_session=identity_case_available,
        identity_adjudication_id=identity_decision_id,
        cross_market_scope_id=cross_scope_id,
        cross_market_adjudication_id=cross_decision_id,
        cross_market_adjudication_available_session=cross_available,
        cross_market_classification_status=cross_status,
        identity_case_resolution_role=case_role,
        adjudication_available_session=adjudication_available,
        backtest_identity_eligible=eligible,
        current_reference_factor_eligible=False,
        security_type_scope="source_type_code_as_returned_not_historical_dictionary_v1",
        identity_evidence_available_session=max(evidence_sessions),
        provider_composite_override_id=provider_override_id,
        provider_composite_override_available_session=provider_override_available,
        share_class_adjudication_id=share_decision_id,
        share_class_adjudication_available_session=share_available,
        asset_transition_ids=tuple(sorted(transition_matches)),
        composite_registry_match_count=len(matches),
        composite_registry_collision=len(matches) > 1,
    )


def record_standing_v4_contract_approval(
    data_root: Path,
    *,
    table_name: str,
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
    authorization_text: str,
    reaffirmation_text: str,
    approved_by: str,
    runtime_probe: Callable[[], Mapping[str, object]] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> StoredControl:
    """Record one fixed-slot approval for an exact v4 contract and clean runtime."""

    root = _root(data_root)
    if table_name not in TABLE_ORDER:
        raise S7StreamingMaterializationError("derived contract table is not frozen")
    if authorization_text != S7_STANDING_AUTHORIZATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 authorization literal differs")
    if reaffirmation_text != S7_STANDING_REAFFIRMATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 reaffirmation literal differs")
    runtime = dict((runtime_probe or _repository_runtime_binding)())
    _validate_runtime_binding(runtime)
    calendar_id = _digest(calendar_artifact_id, "contract approval calendar ID")
    calendar_sha = _digest(calendar_artifact_sha256, "contract approval calendar SHA-256")
    approval_id = _contract_approval_slot_id(
        table_name,
        runtime=runtime,
        calendar_artifact_id=calendar_id,
        calendar_artifact_sha256=calendar_sha,
    )
    relative = _contract_approval_path(table_name, approval_id)
    target = safe_relative_path(root, relative)
    lock = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-v4-contract-approval-{table_name}-{approval_id}.lock",
    )
    if target.exists():
        _, receipt = _load_v4_contract_approval(
            root,
            table_name=table_name,
            runtime=runtime,
            calendar_artifact_id=calendar_id,
            calendar_artifact_sha256=calendar_sha,
        )
        return StoredControl(approval_id, receipt)
    with _exclusive_nonblocking_lock(lock):
        if target.exists():
            _, receipt = _load_v4_contract_approval(
                root,
                table_name=table_name,
                runtime=runtime,
                calendar_artifact_id=calendar_id,
                calendar_artifact_sha256=calendar_sha,
            )
            return StoredControl(approval_id, receipt)
        approved_at = _utc(now(), "contract approval runtime clock")
        slot = _contract_approval_slot_payload(
            table_name,
            runtime=runtime,
            calendar_artifact_id=calendar_id,
            calendar_artifact_sha256=calendar_sha,
        )
        document = {
            **slot,
            "approval_availability": _calendar_availability(
                root,
                calendar_artifact_id=calendar_id,
                calendar_artifact_sha256=calendar_sha,
                recorded_at=approved_at,
            ),
            "approval_id": approval_id,
            "approved_at_utc": _utc_text(approved_at),
            "approved_by": _text(approved_by, "contract approver"),
            "runtime_binding": runtime,
            "standing_authorization": {
                "literal_text": authorization_text,
                "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
            },
            "standing_reaffirmation": {
                "literal_text": reaffirmation_text,
                "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
            },
        }
        receipt = _write_immutable(
            root,
            relative,
            _canonical_bytes(document),
            f"{table_name} v4 contract approval",
        )
        return StoredControl(approval_id, receipt)


def _contract_approval_slot_payload(
    table_name: str,
    *,
    runtime: Mapping[str, object],
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
) -> dict[str, object]:
    if table_name not in TABLE_ORDER:
        raise S7StreamingMaterializationError("derived contract table is not frozen")
    _validate_runtime_binding(runtime)
    return {
        "artifact_type": "s7_v4_derived_contract_standing_approval",
        "authorized_action": "approve_exact_s7_v4_derived_schema_contract_for_full_candidate",
        "calendar_artifact_id": _digest(calendar_artifact_id, "contract calendar ID"),
        "calendar_artifact_sha256": _digest(calendar_artifact_sha256, "contract calendar SHA-256"),
        "capabilities": dict(_FALSE_CAPABILITIES),
        "contract": _contract_pins()[table_name],
        "literal_version": "s7_v4_contract_standing_approval_literal_v1",
        "repository_commit": runtime["repository_commit"],
        "repository_tree": runtime["repository_tree"],
        "runtime_file_set_digest": runtime["runtime_file_set_digest"],
        "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        "table_name": table_name,
    }


def _contract_approval_slot_id(
    table_name: str,
    *,
    runtime: Mapping[str, object],
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
) -> str:
    return stable_digest(
        _contract_approval_slot_payload(
            table_name,
            runtime=runtime,
            calendar_artifact_id=calendar_artifact_id,
            calendar_artifact_sha256=calendar_artifact_sha256,
        )
    )


def _contract_approval_path(table_name: str, approval_id: str) -> str:
    if table_name not in TABLE_ORDER:
        raise S7StreamingMaterializationError("derived contract table is not frozen")
    return (
        "manifests/silver/identity/s7-v4-derived-contract-approvals/"
        f"table_name={table_name}/approval_id={_digest(approval_id, 'approval ID')}/"
        "manifest.json"
    )


def _load_v4_contract_approval(
    root: Path,
    *,
    table_name: str,
    runtime: Mapping[str, object],
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
) -> tuple[dict[str, object], ExactFilePin]:
    expected_id = _contract_approval_slot_id(
        table_name,
        runtime=runtime,
        calendar_artifact_id=calendar_artifact_id,
        calendar_artifact_sha256=calendar_artifact_sha256,
    )
    relative = _contract_approval_path(table_name, expected_id)
    content = _read_exact_file(root, relative, label=f"{table_name} contract approval")
    document = _mapping(
        _load_canonical_json(content, f"{table_name} contract approval"),
        f"{table_name} contract approval",
    )
    slot = _contract_approval_slot_payload(
        table_name,
        runtime=runtime,
        calendar_artifact_id=calendar_artifact_id,
        calendar_artifact_sha256=calendar_artifact_sha256,
    )
    _expect_keys(
        document,
        {
            *slot,
            "approval_availability",
            "approval_id",
            "approved_at_utc",
            "approved_by",
            "runtime_binding",
            "standing_authorization",
            "standing_reaffirmation",
        },
        f"{table_name} contract approval",
    )
    approved_at = _utc_from_text(document["approved_at_utc"], "contract approval time")
    expected_availability = _calendar_availability(
        root,
        calendar_artifact_id=calendar_artifact_id,
        calendar_artifact_sha256=calendar_artifact_sha256,
        recorded_at=approved_at,
    )
    if (
        {key: document[key] for key in slot} != slot
        or document["approval_id"] != expected_id
        or document["runtime_binding"] != dict(runtime)
        or document["approval_availability"] != expected_availability
        or document["standing_authorization"]
        != {
            "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
            "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        }
        or document["standing_reaffirmation"]
        != {
            "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
            "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        }
    ):
        raise S7StreamingMaterializationError(f"{table_name} contract approval binding differs")
    _text(document["approved_by"], "contract approver")
    return document, ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _trusted_contract_approvals(
    root: Path, binding: S7StreamingSourceBinding
) -> tuple[ExactFilePin, ...]:
    receipts: list[ExactFilePin] = []
    for table_name in TABLE_ORDER:
        _, receipt = _load_v4_contract_approval(
            root,
            table_name=table_name,
            runtime=binding.runtime_binding,
            calendar_artifact_id=binding.calendar_artifact_id,
            calendar_artifact_sha256=binding.calendar_artifact_sha256,
        )
        receipts.append(receipt)
    return tuple(receipts)


def store_streaming_source_binding(
    data_root: Path, binding: S7StreamingSourceBinding
) -> StoredControl:
    """Store an explicitly non-production fixture binding.

    Production callers must go through
    :func:`store_production_streaming_source_binding_document`, which rebuilds every
    pin from official release loaders before making the binding durable.
    """

    root = _root(data_root)
    if binding.mode != "fixture":
        raise S7StreamingMaterializationError(
            "production source bindings require the official builder"
        )
    return _store_verified_streaming_source_binding(root, binding)


def _store_verified_streaming_source_binding(
    root: Path, binding: S7StreamingSourceBinding
) -> StoredControl:
    if binding.contract_approvals != _trusted_contract_approvals(root, binding):
        raise S7StreamingMaterializationError(
            "source binding contract approvals do not match trusted fixed slots"
        )
    content = _canonical_bytes(binding.to_dict())
    receipt = _write_immutable(root, binding.relative_path, content, "streaming source binding")
    return StoredControl(binding.source_binding_id, receipt)


def store_production_streaming_source_binding_document(
    data_root: Path,
    document: object,
) -> tuple[S7StreamingSourceBinding, StoredControl]:
    """Reject the retired caller-authored production binding surface."""

    del data_root, document
    raise S7StreamingMaterializationError(
        "caller-authored production binding documents are not accepted; "
        "use the official registry-release builder"
    )


def build_and_store_production_streaming_source_binding(
    data_root: Path,
    *,
    registry_pins: Sequence[RegistryReleasePin],
    cutoff_session: date,
) -> tuple[S7StreamingSourceBinding, StoredControl]:
    """Build every production source pin from five fully replayed registry releases."""

    root = _root(data_root)
    binding, _ = _build_official_production_source_binding(
        root,
        registry_pins=tuple(registry_pins),
        cutoff_session=_native_date(cutoff_session, "production cutoff session"),
        expected=None,
    )
    return binding, _store_verified_streaming_source_binding(root, binding)


def _build_official_production_source_binding(
    root: Path,
    *,
    registry_pins: Sequence[RegistryReleasePin],
    cutoff_session: date,
    expected: S7StreamingSourceBinding | None,
) -> tuple[S7StreamingSourceBinding, LoadedRegistryReleaseSet]:
    """Reconstruct the production binding without trusting its physical pin list."""

    if expected is not None and expected.mode != "production":
        raise S7StreamingMaterializationError("official source-binding builder requires production")
    try:
        runtime = dict(_repository_runtime_binding())
        _validate_runtime_binding(runtime)
        if expected is not None and runtime != dict(expected.runtime_binding):
            raise S7StreamingMaterializationError("production source-binding runtime differs")

        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=CALENDAR_ARTIFACT_ID,
            expected_sha256=CALENDAR_ARTIFACT_SHA256,
        )
        cutoff = _native_date(cutoff_session, "production cutoff session")
        if cutoff not in {item.session_date for item in calendar.sessions}:
            raise S7StreamingMaterializationError("production cutoff is not a bound XNYS session")
        source_bundle = open_identity_source_bundle(root)
        source_bundle.require_official()
        if source_bundle.data_root != root or source_bundle.binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise S7StreamingMaterializationError("official six-release source capability differs")
        membership: list[SessionArtifactPin] = []
        for artifact in source_bundle.artifacts("universe_source_daily"):
            artifact.require_official()
            match = _SESSION_PARTITION.search(artifact.ref.path)
            if match is None:
                raise S7StreamingMaterializationError(
                    "official universe release contains a non-session DATA artifact"
                )
            membership.append(
                SessionArtifactPin(
                    session_date=date.fromisoformat(match.group(1)),
                    row_count=_positive(
                        artifact.ref.row_count,
                        "official universe artifact row count",
                    ),
                    artifact=ExactFilePin(
                        path=artifact.ref.path,
                        sha256=artifact.ref.sha256,
                        bytes=artifact.ref.bytes,
                    ),
                )
            )
        membership.sort(key=lambda item: item.session_date)
        if not membership:
            raise S7StreamingMaterializationError("official universe release is empty")

        release_set_relative = (
            "manifests/silver/release-sets/assets/"
            f"release_set_id={S7_S4_RELEASE_SET_ID}/manifest.json"
        )
        release_set_pin = _existing_file_pin(root, release_set_relative, "S4 release set")
        if release_set_pin.sha256 != S7_S4_RELEASE_SET_MANIFEST_SHA256:
            raise S7StreamingMaterializationError("official S4 release-set hash differs")

        registries = load_registry_release_set(
            root,
            tuple(registry_pins),
            require_exclusive_composite_scopes=False,
        )
        registry_pins = tuple(item.manifest_pin for item in registries.releases)
        gate_c_seed = _gate_c_seed_from_registry_releases(registries)
        gate_c, gate_c_plan = _replay_official_gate_c(root, gate_c_seed)
        gate_b = _replay_official_gate_b_from_gate_c_plan(root, gate_c_plan)
        _verify_gate_b_gate_c_cross_binding(gate_b, gate_c_plan)
        gate_b_rows = _load_gate_b_reference(root, gate_b)
        gate_c_candidate = _mapping(
            _load_canonical_json(
                _read_exact_file(
                    root, gate_c.candidate_manifest.path, label="official Gate-C candidate"
                ),
                "official Gate-C candidate",
            ),
            "official Gate-C candidate",
        )
        gate_c_availability = _mapping(
            gate_c_candidate.get("availability"), "official Gate-C candidate availability"
        )
        minimum_cutoff = max(
            membership[-1].session_date,
            *(item.release_available_session for item in registry_pins),
            *(
                _native_date(row["source_available_session"], "Gate-B availability")
                for row in gate_b_rows.values()
            ),
            date.fromisoformat(
                _text(
                    gate_c_availability.get("candidate_available_session"),
                    "Gate-C candidate availability",
                )
            ),
        )
        if cutoff < minimum_cutoff:
            raise S7StreamingMaterializationError(
                "production cutoff predates source, Gate-B, Gate-C, or registry availability"
            )

        contract_approvals = tuple(
            _load_v4_contract_approval(
                root,
                table_name=table_name,
                runtime=runtime,
                calendar_artifact_id=calendar.calendar_artifact_id,
                calendar_artifact_sha256=calendar.sha256,
            )[1]
            for table_name in TABLE_ORDER
        )
        rebuilt = S7StreamingSourceBinding(
            mode="production",
            cutoff_session=cutoff,
            s4_release_set_manifest=release_set_pin,
            membership_artifacts=tuple(membership),
            gate_b=gate_b,
            gate_c=gate_c,
            registry_pins=registry_pins,
            contract_approvals=contract_approvals,
            runtime_binding=runtime,
            calendar_artifact_id=calendar.calendar_artifact_id,
            calendar_artifact_sha256=calendar.sha256,
        )
    except S7StreamingMaterializationError:
        raise
    except (
        IdentityMarketConsistencyError,
        IdentityMarketSequenceError,
        IdentitySourceError,
        RegistryWorkflowError,
        OSError,
    ) as exc:
        raise S7StreamingMaterializationError(
            "official production source binding cannot be replayed"
        ) from exc
    if expected is not None and rebuilt.to_dict() != expected.to_dict():
        raise S7StreamingMaterializationError(
            "stored production source binding differs from official reconstruction"
        )
    return rebuilt, registries


def _existing_file_pin(root: Path, relative: str, label: str) -> ExactFilePin:
    content = _read_exact_file(root, relative, label=label)
    return ExactFilePin(
        path=_relative(relative, f"{label} path"),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


@dataclass(frozen=True, slots=True)
class _GateCRegistrySeed:
    candidate: ExactArtifactBinding
    completion: ExactArtifactBinding


def _gate_c_seed_from_registry_releases(
    registries: LoadedRegistryReleaseSet,
) -> _GateCRegistrySeed:
    pairs: list[_GateCRegistrySeed] = []
    for registry_name in (
        "identity_adjudication",
        "identity_cross_market_adjudication",
    ):
        by_role = {
            item.role: item for item in registries.by_name(registry_name).candidate.source_artifacts
        }
        if set(by_role) != {
            "source_gate_c_candidate_manifest",
            "source_gate_c_completion_manifest",
        }:
            raise S7StreamingMaterializationError(
                "production registry release lacks the exact Gate-C pair"
            )
        pairs.append(
            _GateCRegistrySeed(
                candidate=by_role["source_gate_c_candidate_manifest"],
                completion=by_role["source_gate_c_completion_manifest"],
            )
        )
    if pairs[0] != pairs[1]:
        raise S7StreamingMaterializationError(
            "production registry releases bind different Gate-C pairs"
        )
    return pairs[0]


def _replay_official_gate_b(root: Path, requested: GateBReferencePin) -> GateBReferencePin:
    verified = verify_market_classification_candidate(
        root,
        candidate_path=requested.manifest.path,
        candidate_id=requested.candidate_id,
        candidate_sha256=requested.manifest.sha256,
        require_production_approval=True,
    )
    manifest = _mapping(
        _load_gate_b_compact_canonical_json(
            _read_exact_file(root, verified.manifest_path, label="official Gate-B manifest"),
            "official Gate-B manifest",
        ),
        "official Gate-B manifest",
    )
    if (
        verified.candidate_id != requested.candidate_id
        or manifest.get("candidate_id") != verified.candidate_id
        or manifest.get("state") != STREAMING_STATE
    ):
        raise S7StreamingMaterializationError("official Gate-B candidate differs")
    return GateBReferencePin(
        candidate_id=verified.candidate_id,
        candidate_state=STREAMING_STATE,
        reference_version=PRODUCTION_GATE_B_REFERENCE_VERSION,
        closed=True,
        manifest=_existing_file_pin(root, verified.manifest_path, "official Gate-B manifest"),
        data=_existing_file_pin(root, verified.data_path, "official Gate-B DATA"),
    )


def _replay_official_gate_b_from_gate_c_plan(
    root: Path, gate_c_plan: Mapping[str, object]
) -> GateBReferencePin:
    gate_b = _mapping(gate_c_plan.get("gate_b"), "official Gate-C Gate-B binding")
    data = _mapping(gate_b.get("data"), "official Gate-C Gate-B DATA binding")
    manifest_pin = _existing_file_pin(
        root,
        _relative(gate_b.get("candidate_path"), "official Gate-B candidate path"),
        "official Gate-B manifest",
    )
    data_pin = _existing_file_pin(
        root,
        _relative(data.get("path"), "official Gate-B DATA path"),
        "official Gate-B DATA",
    )
    if (
        manifest_pin.sha256
        != _digest(gate_b.get("candidate_sha256"), "official Gate-B candidate SHA-256")
        or data_pin.sha256 != _digest(data.get("sha256"), "official Gate-B DATA SHA-256")
        or data_pin.bytes != _nonnegative(data.get("bytes"), "official Gate-B DATA bytes")
    ):
        raise S7StreamingMaterializationError("Gate-C Gate-B physical receipts differ")
    requested = GateBReferencePin(
        candidate_id=_digest(gate_b.get("candidate_id"), "official Gate-B candidate ID"),
        candidate_state=STREAMING_STATE,
        reference_version=PRODUCTION_GATE_B_REFERENCE_VERSION,
        closed=True,
        manifest=manifest_pin,
        data=data_pin,
    )
    verified = _replay_official_gate_b(root, requested)
    row_count = _positive(data.get("row_count"), "official Gate-B DATA row count")
    parquet_row_count = pq.ParquetFile(
        safe_relative_path(root, verified.data.path)
    ).metadata.num_rows
    if row_count != _verified_market_row_count(root, verified) or row_count != parquet_row_count:
        raise S7StreamingMaterializationError("official Gate-B DATA row count differs")
    return verified


def _verified_market_row_count(root: Path, pin: GateBReferencePin) -> int:
    """Return the count from the already replayed Gate-B verifier result."""

    verified = verify_market_classification_candidate(
        root,
        candidate_path=pin.manifest.path,
        candidate_id=pin.candidate_id,
        candidate_sha256=pin.manifest.sha256,
        require_production_approval=True,
    )
    return _positive(verified.composite_count, "official Gate-B verified row count")


def _replay_official_gate_c(
    root: Path, seed: _GateCRegistrySeed
) -> tuple[GateCCompletionPin, Mapping[str, object]]:
    candidate_seed = ExactFilePin(
        path=seed.candidate.path,
        sha256=seed.candidate.sha256,
        bytes=seed.candidate.bytes,
    )
    completion_seed = ExactFilePin(
        path=seed.completion.path,
        sha256=seed.completion.sha256,
        bytes=seed.completion.bytes,
    )
    if (
        _existing_file_pin(root, candidate_seed.path, "registry-bound Gate-C candidate")
        != candidate_seed
        or _existing_file_pin(root, completion_seed.path, "registry-bound Gate-C completion")
        != completion_seed
    ):
        raise S7StreamingMaterializationError("registry-bound Gate-C receipts differ")
    completion = _mapping(
        _load_canonical_json(
            _read_exact_file(
                root,
                completion_seed.path,
                label="official Gate-C completion",
            ),
            "official Gate-C completion",
        ),
        "official Gate-C completion",
    )
    plan_ref = _mapping(completion.get("plan"), "official Gate-C completion plan")
    authorization_ref = _mapping(
        completion.get("authorization"), "official Gate-C completion authorization"
    )
    plan_path = _relative(plan_ref.get("path"), "official Gate-C plan path")
    plan_id = _digest(plan_ref.get("plan_id"), "official Gate-C plan ID")
    plan_sha256 = _digest(plan_ref.get("sha256"), "official Gate-C plan SHA-256")
    authorization_path = _relative(
        authorization_ref.get("path"), "official Gate-C authorization path"
    )
    authorization_id = _digest(
        authorization_ref.get("authorization_id"), "official Gate-C authorization ID"
    )
    authorization_sha256 = _digest(
        authorization_ref.get("sha256"), "official Gate-C authorization SHA-256"
    )
    plan_document = _load_gate_c_plan(
        root,
        plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    _load_gate_c_authorization(
        root,
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorization_sha256=authorization_sha256,
        plan=plan_document,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
    )
    caps = S7MarketSequenceResourceCaps(
        **_mapping(plan_document["resource_caps"], "official Gate-C resource caps")
    )
    loaded = _load_gate_c_candidate_from_completion(
        root,
        completion_relative=completion_seed.path,
        plan=plan_document,
        plan_path=plan_path,
        plan_id=plan_id,
        plan_sha256=plan_sha256,
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorization_sha256=authorization_sha256,
        caps=caps,
        idempotent=True,
    )
    candidate = _mapping(
        _load_canonical_json(
            _read_exact_file(root, loaded.manifest_path, label="official Gate-C candidate"),
            "official Gate-C candidate",
        ),
        "official Gate-C candidate",
    )
    qa = _mapping(
        _load_canonical_json(
            _read_exact_file(root, loaded.qa_path, label="official Gate-C QA"),
            "official Gate-C QA",
        ),
        "official Gate-C QA",
    )
    preview_id, preview_pin, preview_available = _load_gate_c_identity_case_preview(
        root,
        candidate,
    )
    if (
        loaded.candidate_id != seed.candidate.artifact_id
        or loaded.completion_id != seed.completion.artifact_id
        or loaded.completion_path != completion_seed.path
        or loaded.manifest_path != candidate_seed.path
        or candidate.get("candidate_id") != loaded.candidate_id
        or candidate.get("state") != STREAMING_STATE
        or completion.get("completion_id") != loaded.completion_id
        or completion.get("completion_state") != STREAMING_STATE
        or qa.get("critical_failure_count") != 0
    ):
        raise S7StreamingMaterializationError("official Gate-C completion differs")
    _verify_gate_c_candidate_upstream_replay(
        root,
        candidate=candidate,
        plan=plan_document,
        caps=caps,
    )
    pin = GateCCompletionPin(
        candidate_id=loaded.candidate_id,
        completion_id=loaded.completion_id,
        completion_state=STREAMING_STATE,
        complete=True,
        candidate_manifest=_existing_file_pin(
            root, loaded.manifest_path, "official Gate-C candidate"
        ),
        completion_manifest=_existing_file_pin(
            root, loaded.completion_path, "official Gate-C completion"
        ),
        identity_case_preview_id=preview_id,
        identity_case_preview_manifest=preview_pin,
        identity_case_preview_available_session=preview_available,
        qa=_existing_file_pin(root, loaded.qa_path, "official Gate-C QA"),
    )
    return pin, plan_document


def _load_gate_c_identity_case_preview(
    root: Path,
    candidate: Mapping[str, object],
) -> tuple[str, ExactFilePin, date]:
    source_refs = _mapping(
        candidate.get("registry_loader_source_refs"),
        "official Gate-C source refs",
    )
    preview_ref = _mapping(
        source_refs.get("detector_preview"),
        "official Gate-C detector preview ref",
    )
    preview_id = _digest(
        preview_ref.get("preview_artifact_id"),
        "official identity-case preview ID",
    )
    preview_path = _relative(
        preview_ref.get("path"),
        "official identity-case preview path",
    )
    preview_pin = _existing_file_pin(
        root,
        preview_path,
        "official identity-case preview",
    )
    if (
        preview_pin.sha256
        != _digest(
            preview_ref.get("sha256"),
            "official identity-case preview SHA-256",
        )
        or preview_pin.bytes
        != _nonnegative(
            preview_ref.get("bytes"),
            "official identity-case preview bytes",
        )
    ):
        raise S7StreamingMaterializationError(
            "official identity-case preview receipt differs"
        )
    preview_document = _mapping(
        _load_canonical_json(
            _read_exact_file(root, preview_path, label="official identity-case preview"),
            "official identity-case preview",
        ),
        "official identity-case preview",
    )
    if preview_document.get("preview_artifact_id") != preview_id:
        raise S7StreamingMaterializationError(
            "official identity-case preview embedded ID differs"
        )
    preview_result = _mapping(
        preview_document.get("result"),
        "official identity-case preview result",
    )
    try:
        preview_available = date.fromisoformat(
            _text(
                preview_result.get("preview_manifest_available_session"),
                "official identity-case preview available session",
            )
        )
    except ValueError as exc:
        raise S7StreamingMaterializationError(
            "official identity-case preview available session is invalid"
        ) from exc
    return preview_id, preview_pin, preview_available


def _verify_gate_c_candidate_upstream_replay(
    root: Path,
    *,
    candidate: Mapping[str, object],
    plan: Mapping[str, object],
    caps: S7MarketSequenceResourceCaps,
) -> None:
    gate_a = _mapping(plan.get("gate_a"), "official Gate-C Gate-A binding")
    gate_b = _mapping(plan.get("gate_b"), "official Gate-C Gate-B binding")
    gate_b_data = _mapping(gate_b.get("data"), "official Gate-C Gate-B DATA binding")
    availability = _mapping(plan.get("availability"), "official Gate-C availability")
    monitor = _GateCResourceMonitor(
        root=root,
        staging=safe_relative_path(root, "tmp/silver-s7-streaming-gate-c-readonly-replay"),
        caps=caps,
    )
    inputs = _load_gate_c_inputs(
        root,
        inventory_completion_path=_relative(
            gate_a.get("completion_path"), "official Gate-A completion path"
        ),
        classification_candidate_path=_relative(
            gate_b.get("candidate_path"), "official Gate-B candidate path"
        ),
        classification_candidate_id=_digest(
            gate_b.get("candidate_id"), "official Gate-B candidate ID"
        ),
        classification_candidate_sha256=_digest(
            gate_b.get("candidate_sha256"), "official Gate-B candidate SHA-256"
        ),
        classification_data_path=_relative(gate_b_data.get("path"), "official Gate-B DATA path"),
        classification_data_sha256=_digest(
            gate_b_data.get("sha256"), "official Gate-B DATA SHA-256"
        ),
        classification_data_bytes=_nonnegative(
            gate_b_data.get("bytes"), "official Gate-B DATA bytes"
        ),
        classification_data_row_count=_positive(
            gate_b_data.get("row_count"), "official Gate-B DATA rows"
        ),
        classification_source_available_session=_text(
            availability.get("classification_source_available_session"),
            "official Gate-B source availability",
        ),
        caps=caps,
        monitor=monitor,
    )
    basis = _mapping(candidate.get("candidate_basis"), "official Gate-C candidate basis")
    expected_source_digest = stable_digest(list(inputs["universe_refs"]))
    if (
        basis.get("gate_a") != inputs["gate_a_binding"]
        or basis.get("gate_b") != inputs["gate_b_binding"]
        or basis.get("registry_loader_source_refs") != inputs["registry_loader_source_refs"]
        or basis.get("source_artifact_set_digest") != expected_source_digest
    ):
        raise S7StreamingMaterializationError("official Gate-C candidate upstream replay differs")


def _verify_gate_b_gate_c_cross_binding(
    gate_b: GateBReferencePin, gate_c_plan: Mapping[str, object]
) -> None:
    plan_gate_b = _mapping(gate_c_plan.get("gate_b"), "official Gate-C Gate-B binding")
    data = _mapping(plan_gate_b.get("data"), "official Gate-C Gate-B DATA binding")
    if (
        plan_gate_b.get("candidate_id") != gate_b.candidate_id
        or plan_gate_b.get("candidate_path") != gate_b.manifest.path
        or plan_gate_b.get("candidate_sha256") != gate_b.manifest.sha256
        or data.get("path") != gate_b.data.path
        or data.get("sha256") != gate_b.data.sha256
        or data.get("bytes") != gate_b.data.bytes
    ):
        raise S7StreamingMaterializationError("Gate B and Gate C bind different candidates")


def prepare_streaming_bounded_profile_preview_plan(
    data_root: Path,
    *,
    source_binding_id: str,
    full_resource_caps: StreamingResourceCaps,
    sample_session_cap: int,
    prepared_by: str,
    prepared_at_utc: datetime,
) -> tuple[dict[str, object], StoredControl]:
    """Freeze a deterministic, bounded size/profile sample before any Full plan."""

    root = _root(data_root)
    binding, binding_receipt = _load_source_binding(root, source_binding_id)
    if binding.contract_approvals != _trusted_contract_approvals(root, binding):
        raise S7StreamingMaterializationError("profile contract approvals differ")
    _validate_binding_against_caps(binding, full_resource_caps)
    cap = _positive(sample_session_cap, "profile sample session cap")
    if cap > PROFILE_SAMPLE_SESSION_HARD_CAP:
        raise S7StreamingMaterializationError("profile sample exceeds the frozen hard cap")
    sample = _profile_sample_artifacts(binding.membership_artifacts, cap)
    slot = {
        "artifact_type": "s7_streaming_bounded_size_profile_plan",
        "authorized_action": PROFILE_AUTHORIZED_ACTION,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "full_resource_caps": full_resource_caps.to_dict(),
        "policy_version": PROFILE_POLICY_VERSION,
        "runtime_binding": dict(binding.runtime_binding),
        "sample_artifacts": [item.to_dict() for item in sample],
        "sample_session_cap": cap,
        "source_binding": binding_receipt.to_dict(),
        "source_binding_id": binding.source_binding_id,
    }
    plan_id = stable_digest({**slot, "artifact_type": "s7_streaming_size_profile_plan_slot"})
    relative = _profile_plan_path(plan_id)
    actor = _text(prepared_by, "profile plan preparer")
    target = safe_relative_path(root, relative)
    lock = safe_relative_path(
        root, f"manifests/silver/locks/s7-streaming-profile-plan-{plan_id}.lock"
    )
    if target.exists():
        existing, receipt, _ = _load_profile_plan(root, plan_id)
        if existing["prepared_by"] != actor:
            raise S7StreamingMaterializationError("fixed profile plan slot actor differs")
        return existing, StoredControl(plan_id, receipt)
    document = {
        **slot,
        "plan_id": plan_id,
        "prepared_at_utc": _utc_text(_utc(prepared_at_utc, "profile plan time")),
        "prepared_by": actor,
    }
    with _exclusive_nonblocking_lock(lock):
        if target.exists():
            existing, receipt, _ = _load_profile_plan(root, plan_id)
            if existing["prepared_by"] != actor:
                raise S7StreamingMaterializationError("fixed profile plan slot actor differs")
            return existing, StoredControl(plan_id, receipt)
        receipt = _write_immutable(
            root, relative, _canonical_bytes(document), "bounded profile plan"
        )
        return document, StoredControl(plan_id, receipt)


def record_standing_streaming_profile_approval(
    data_root: Path,
    *,
    plan_id: str,
    authorization_text: str,
    reaffirmation_text: str,
    approved_by: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[dict[str, object], StoredControl]:
    root = _root(data_root)
    if authorization_text != S7_STANDING_AUTHORIZATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 authorization literal differs")
    if reaffirmation_text != S7_STANDING_REAFFIRMATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 reaffirmation literal differs")
    _, plan_receipt, binding = _load_profile_plan(root, plan_id)
    slot = {
        "artifact_type": "s7_streaming_bounded_size_profile_standing_approval",
        "authorized_action": PROFILE_AUTHORIZED_ACTION,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "plan": plan_receipt.to_dict(),
        "plan_id": plan_id,
        "policy_version": PROFILE_POLICY_VERSION,
        "source_binding_id": binding.source_binding_id,
        "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
    }
    approval_id = stable_digest(slot)
    relative = _profile_approval_path(approval_id)
    target = safe_relative_path(root, relative)
    lock = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-streaming-profile-approval-{approval_id}.lock",
    )
    if target.exists():
        existing, receipt = _load_profile_approval(root, approval_id)
        _verify_profile_approval(existing, slot, root=root, binding=binding)
        return existing, StoredControl(approval_id, receipt)
    with _exclusive_nonblocking_lock(lock):
        if target.exists():
            existing, receipt = _load_profile_approval(root, approval_id)
            _verify_profile_approval(existing, slot, root=root, binding=binding)
            return existing, StoredControl(approval_id, receipt)
        approved_at = _utc(now(), "profile approval runtime clock")
        document = {
            **slot,
            "approval_availability": _approval_availability(root, binding, approved_at),
            "approval_id": approval_id,
            "approved_at_utc": _utc_text(approved_at),
            "approved_by": _text(approved_by, "profile approver"),
            "standing_authorization": {
                "literal_text": authorization_text,
                "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
            },
            "standing_reaffirmation": {
                "literal_text": reaffirmation_text,
                "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
            },
        }
        receipt = _write_immutable(
            root, relative, _canonical_bytes(document), "bounded profile approval"
        )
        return document, StoredControl(approval_id, receipt)


def execute_streaming_bounded_profile_preview(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
) -> dict[str, object]:
    """Execute the exact production profile without caller-controlled runtime hooks."""

    return _execute_streaming_bounded_profile_preview(
        data_root,
        plan_id=plan_id,
        approval_id=approval_id,
        production_execution=True,
        registry_loader=load_registry_release_set,
        runtime_probe=_repository_runtime_binding,
        now=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        rss_probe=None,
        disk_free_probe=None,
        checkpoint_hook=None,
    )


def _execute_streaming_bounded_profile_preview_fixture(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
    registry_loader: Callable[..., LoadedRegistryReleaseSet] = load_registry_release_set,
    runtime_probe: Callable[[], Mapping[str, object]] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    rss_probe: Callable[[], int] | None = None,
    disk_free_probe: Callable[[Path], int] | None = None,
    checkpoint_hook: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Fixture-only profile runner with explicit deterministic fault-injection hooks."""

    return _execute_streaming_bounded_profile_preview(
        data_root,
        plan_id=plan_id,
        approval_id=approval_id,
        production_execution=False,
        registry_loader=registry_loader,
        runtime_probe=runtime_probe or _repository_runtime_binding,
        now=now,
        monotonic=monotonic,
        rss_probe=rss_probe,
        disk_free_probe=disk_free_probe,
        checkpoint_hook=checkpoint_hook,
    )


def _execute_streaming_bounded_profile_preview(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
    production_execution: bool,
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    runtime_probe: Callable[[], Mapping[str, object]],
    now: Callable[[], datetime],
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    checkpoint_hook: Callable[[str], None] | None,
) -> dict[str, object]:
    """Materialize a bounded real sample, persist only its size profile, and stop."""

    root = _root(data_root)
    plan, plan_receipt, binding = _load_profile_plan(root, plan_id)
    if production_execution != (binding.mode == "production"):
        raise S7StreamingMaterializationError(
            "production and fixture profile execution boundaries cannot be crossed"
        )
    approval, approval_receipt = _load_profile_approval(root, approval_id)
    expected_slot = {
        "artifact_type": "s7_streaming_bounded_size_profile_standing_approval",
        "authorized_action": PROFILE_AUTHORIZED_ACTION,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "plan": plan_receipt.to_dict(),
        "plan_id": plan_id,
        "policy_version": PROFILE_POLICY_VERSION,
        "source_binding_id": binding.source_binding_id,
        "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
    }
    _verify_profile_approval(approval, expected_slot, root=root, binding=binding)
    runtime = dict(runtime_probe())
    if runtime != dict(binding.runtime_binding):
        raise S7StreamingMaterializationError("profile runtime binding differs")
    caps = StreamingResourceCaps.from_dict(plan["full_resource_caps"])
    start = monotonic()
    candidate_id = stable_digest(
        {
            "adapter_version": PRODUCTION_ADAPTER_VERSION,
            "approval_id": approval_id,
            "plan_id": plan_id,
            "policy_version": PROFILE_POLICY_VERSION,
        }
    )
    completion_relative = _profile_completion_path(plan_id, approval_id)
    completion_path = safe_relative_path(root, completion_relative)
    candidate_relative = _profile_candidate_path(candidate_id)
    candidate_path = safe_relative_path(root, candidate_relative)
    lock = safe_relative_path(
        root, f"manifests/silver/locks/s7-streaming-profile-run-{plan_id}.lock"
    )
    with _exclusive_nonblocking_lock(lock):
        if completion_path.exists():
            _check_resource_caps(
                root,
                staging=None,
                started=start,
                caps=caps,
                monotonic=monotonic,
                rss_probe=rss_probe,
                disk_free_probe=disk_free_probe,
            )
            return _load_and_verify_profile_completion(
                root,
                completion_relative,
                plan=plan,
                approval=approval,
                binding=binding,
                idempotent=True,
            )
        staging = safe_relative_path(
            root, f"tmp/silver-s7-streaming-profile/candidate_id={candidate_id}"
        )
        if staging.exists() or staging.is_symlink():
            raise S7StreamingMaterializationError(
                "incomplete profile staging requires explicit review"
            )
        if candidate_path.exists() or candidate_path.is_symlink():
            candidate, candidate_receipt = _load_and_verify_profile_candidate(
                root,
                candidate_id=candidate_id,
                plan=plan,
                approval=approval,
                binding=binding,
            )
            _check_resource_caps(
                root,
                staging=None,
                started=start,
                caps=caps,
                monotonic=monotonic,
                rss_probe=rss_probe,
                disk_free_probe=disk_free_probe,
            )
            _load_verified_execution_sources(
                root,
                binding=binding,
                registry_loader=registry_loader,
            )
            return _store_profile_completion(
                root,
                completion_relative=completion_relative,
                candidate=candidate,
                candidate_receipt=candidate_receipt,
                plan=plan,
                approval=approval,
                binding=binding,
                now=now,
                started=start,
                caps=caps,
                monotonic=monotonic,
                rss_probe=rss_probe,
                disk_free_probe=disk_free_probe,
                idempotent=True,
            )
        intent_receipt = _store_or_load_profile_intent(
            root,
            plan=plan,
            plan_receipt=plan_receipt,
            approval=approval,
            approval_receipt=approval_receipt,
            binding=binding,
            candidate_id=candidate_id,
            now=now,
        )
        if checkpoint_hook is not None:
            checkpoint_hook("intent_durable")
        # The intent is now durable.  Check the 40 GiB/RSS/wall limits before any
        # official source release, Parquet, Gate-B, Gate-C, or registry content read.
        _check_resource_caps(
            root,
            staging=None,
            started=start,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
        )
        sample = tuple(
            SessionArtifactPin.from_dict(value)
            for value in _array(plan["sample_artifacts"], "profile sample artifacts")
        )
        registries, gate_b = _load_verified_execution_sources(
            root,
            binding=binding,
            registry_loader=registry_loader,
        )
        sample_binding = replace(
            binding,
            mode="fixture",
            membership_artifacts=sample,
        )
        staging.mkdir(parents=True, exist_ok=False)
        resolved = staging / "_resolved"
        resolved.mkdir()
        (staging / "data").mkdir()
        aliases, assets, issuers, _ = _run_pass_one(
            root,
            staging=staging,
            resolved_dir=resolved,
            binding=sample_binding,
            caps=caps,
            gate_b=gate_b,
            registries=registries,
            adapter=FrozenRegistryProjectionAdapter(),
            started=start,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            checkpoint_hook=None,
        )
        _run_pass_two(
            staging=staging,
            resolved_dir=resolved,
            binding=sample_binding,
            caps=caps,
            aliases=aliases,
            assets=assets,
            issuers=issuers,
            registries=registries,
            started=start,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            root=root,
            checkpoint_hook=None,
        )
        output_bytes = _directory_size(staging / "data")
        peak_staging_bytes = _directory_size(staging)
        sample_rows = sum(item.row_count for item in sample)
        sample_source_bytes = sum(item.artifact.bytes for item in sample)
        projected_output = _project_bytes(output_bytes, sample_rows, binding.row_count)
        projected_peak = _project_bytes(peak_staging_bytes, sample_rows, binding.row_count)
        disk_free = int(
            disk_free_probe(root) if disk_free_probe is not None else shutil.disk_usage(root).free
        )
        if disk_free < 0:
            raise S7StreamingMaterializationError("profile disk-free probe is invalid")
        # The probe is taken while the bounded staging tree still exists.  Full starts
        # only after that tree is removed, so add its exact bytes back before projecting
        # the Full peak.  This remains conservative because the projection rounds up.
        expected_remaining = disk_free + peak_staging_bytes - projected_peak
        failures = (
            int(projected_output > caps.output_bytes_cap)
            + int(projected_peak > caps.tmp_bytes_cap)
            + int(expected_remaining < caps.disk_free_floor_bytes)
        )
        metrics = {
            "critical_failure_count": failures,
            "expected_remaining_free_bytes_at_peak": expected_remaining,
            "full_row_count": binding.row_count,
            "observed_disk_free_bytes_during_profile": disk_free,
            "output_bytes_per_million_rows": _per_million(output_bytes, sample_rows),
            "peak_staging_bytes_per_million_rows": _per_million(peak_staging_bytes, sample_rows),
            "projected_full_output_bytes": projected_output,
            "projected_full_peak_staging_bytes": projected_peak,
            "sample_output_bytes": output_bytes,
            "sample_output_to_source_ratio_ppm": _ratio_ppm(output_bytes, sample_source_bytes),
            "sample_peak_staging_bytes": peak_staging_bytes,
            "sample_peak_staging_to_output_ratio_ppm": _ratio_ppm(peak_staging_bytes, output_bytes),
            "sample_row_count": sample_rows,
            "sample_session_count": len(sample),
            "sample_source_compressed_bytes": sample_source_bytes,
            "source_compressed_bytes_per_million_rows": _per_million(
                sample_source_bytes, sample_rows
            ),
        }
        if failures:
            raise S7StreamingMaterializationError(
                "bounded profile projection breaches Full disk/output/tmp caps"
            )
        shutil.rmtree(staging)
        candidate_payload = {
            "approval_id": approval_id,
            "artifact_type": "s7_streaming_bounded_size_profile_candidate",
            "candidate_id": candidate_id,
            "capabilities": dict(_FALSE_CAPABILITIES),
            "intent": intent_receipt.to_dict(),
            "metrics": metrics,
            "plan_id": plan_id,
            "policy_version": PROFILE_POLICY_VERSION,
            "source_binding_id": binding.source_binding_id,
            "state": STREAMING_STATE,
        }
        candidate = {**candidate_payload, "manifest_id": stable_digest(candidate_payload)}
        candidate_receipt = _write_immutable(
            root,
            candidate_relative,
            _canonical_bytes(candidate),
            "bounded profile candidate",
        )
        if checkpoint_hook is not None:
            checkpoint_hook("candidate_durable")
        return _store_profile_completion(
            root,
            completion_relative=completion_relative,
            candidate=candidate,
            candidate_receipt=candidate_receipt,
            plan=plan,
            approval=approval,
            binding=binding,
            now=now,
            started=start,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            idempotent=False,
        )


def prepare_streaming_full_plan(
    data_root: Path,
    *,
    source_binding_id: str,
    resource_caps: StreamingResourceCaps,
    prepared_by: str,
    prepared_at_utc: datetime,
    profile_plan_id: str | None = None,
    profile_approval_id: str | None = None,
) -> tuple[dict[str, object], StoredControl]:
    root = _root(data_root)
    binding, binding_receipt = _load_source_binding(root, source_binding_id)
    _validate_binding_against_caps(binding, resource_caps)
    profile_evidence = _profile_evidence_for_full_plan(
        root,
        binding=binding,
        caps=resource_caps,
        profile_plan_id=profile_plan_id,
        profile_approval_id=profile_approval_id,
    )
    prepared_at = _utc(prepared_at_utc, "plan preparation time")
    if profile_evidence is not None:
        completion_time = _utc_from_text(
            profile_evidence["completed_at_utc"], "profile completion time"
        )
        if prepared_at < completion_time:
            raise S7StreamingMaterializationError("Full plan predates bounded profile completion")
    slot_payload = {
        "artifact_type": "s7_streaming_four_table_full_plan",
        "bounded_profile_evidence": profile_evidence,
        "candidate_state": STREAMING_STATE,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "contract_pins": _contract_pins(),
        "plan_version": STREAMING_PLAN_VERSION,
        "policy_version": STREAMING_POLICY_VERSION,
        "resource_caps": resource_caps.to_dict(),
        "runtime_binding": dict(binding.runtime_binding),
        "source_binding": binding_receipt.to_dict(),
        "source_binding_id": binding.source_binding_id,
    }
    plan_id = stable_digest(
        {**slot_payload, "artifact_type": "s7_streaming_four_table_full_plan_slot"}
    )
    relative = _plan_path(plan_id)
    target = safe_relative_path(root, relative)
    actor = _text(prepared_by, "plan preparer")
    lock_path = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-streaming-plan-slot-{plan_id}.lock",
    )
    if target.exists():
        existing, receipt, _ = _load_plan(root, plan_id)
        if existing["prepared_by"] != actor:
            raise S7StreamingMaterializationError("fixed Full plan slot actor differs")
        return existing, StoredControl(plan_id, receipt)
    document = {
        **slot_payload,
        "plan_id": plan_id,
        "prepared_at_utc": _utc_text(prepared_at),
        "prepared_by": actor,
    }
    with _exclusive_nonblocking_lock(lock_path):
        if target.exists():
            existing, receipt, _ = _load_plan(root, plan_id)
            if existing["prepared_by"] != actor:
                raise S7StreamingMaterializationError("fixed Full plan slot actor differs")
            return existing, StoredControl(plan_id, receipt)
        receipt = _write_immutable(
            root, relative, _canonical_bytes(document), "streaming Full plan"
        )
        return document, StoredControl(plan_id, receipt)


def prepare_streaming_approval_request(
    data_root: Path,
    *,
    plan_id: str,
    requested_by: str,
    requested_at_utc: datetime,
) -> tuple[StreamingApprovalRequest, StoredControl]:
    root = _root(data_root)
    plan, plan_receipt, _ = _load_plan(root, plan_id)
    request = StreamingApprovalRequest(
        plan_id=plan_id,
        plan=plan_receipt,
        source_binding_id=_digest(plan["source_binding_id"], "source binding ID"),
        resource_caps_digest=stable_digest(plan["resource_caps"]),
        runtime_file_set_digest=_digest(
            _mapping(plan["runtime_binding"], "plan runtime binding")["runtime_file_set_digest"],
            "runtime file-set digest",
        ),
        requested_at_utc=_utc(requested_at_utc, "approval request time"),
        requested_by=_text(requested_by, "approval requester"),
    )
    target = safe_relative_path(root, request.relative_path)
    lock_path = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-streaming-request-slot-{request.request_id}.lock",
    )
    if target.exists():
        existing, receipt = _load_request(root, request.request_id)
        if existing.requested_by != request.requested_by:
            raise S7StreamingMaterializationError("fixed approval request slot actor differs")
        return existing, StoredControl(existing.request_id, receipt)
    content = _canonical_bytes(request.to_dict())
    with _exclusive_nonblocking_lock(lock_path):
        if target.exists():
            existing, receipt = _load_request(root, request.request_id)
            if existing.requested_by != request.requested_by:
                raise S7StreamingMaterializationError("fixed approval request slot actor differs")
            return existing, StoredControl(existing.request_id, receipt)
        receipt = _write_immutable(root, request.relative_path, content, "approval request")
        return request, StoredControl(request.request_id, receipt)


def exact_streaming_approval_literal(request: StreamingApprovalRequest) -> str:
    """Return the byte-exact canonical request literal a reviewer must approve."""

    return _canonical_bytes(request.to_dict()).decode("utf-8")


def record_exact_streaming_approval(
    data_root: Path,
    *,
    request_id: str,
    exact_literal: str,
    approved_by: str,
    approved_at_utc: datetime,
) -> tuple[StreamingExactApproval, StoredControl]:
    root = _root(data_root)
    request, request_receipt = _load_request(root, request_id)
    expected = exact_streaming_approval_literal(request)
    if exact_literal != expected:
        raise S7StreamingMaterializationError("approval literal is not byte-exact")
    _, _, binding = _load_plan(root, request.plan_id)
    approved_at = _utc(approved_at_utc, "approval time")
    approval = StreamingExactApproval(
        request_id=request_id,
        request=request_receipt,
        plan_id=request.plan_id,
        exact_literal_sha256=hashlib.sha256(exact_literal.encode("utf-8")).hexdigest(),
        approved_at_utc=approved_at,
        approved_by=_text(approved_by, "approver"),
        authorization_mode="exact_literal",
        approval_availability=_approval_availability(root, binding, approved_at),
    )
    receipt = _write_immutable(
        root,
        approval.relative_path,
        _canonical_bytes(approval.to_dict()),
        "exact approval",
    )
    return approval, StoredControl(approval.approval_id, receipt)


def record_standing_streaming_approval(
    data_root: Path,
    *,
    request_id: str,
    authorization_text: str,
    reaffirmation_text: str,
    approved_by: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[StreamingExactApproval, StoredControl]:
    """Bind the user's standing S7 authorization to one internally derived request.

    The caller supplies no plan JSON and no timestamps.  The request bytes are loaded from
    the immutable control chain; record time and XNYS availability are derived at runtime.
    """

    root = _root(data_root)
    if authorization_text != S7_STANDING_AUTHORIZATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 authorization literal differs")
    if reaffirmation_text != S7_STANDING_REAFFIRMATION_TEXT:
        raise S7StreamingMaterializationError("standing S7 reaffirmation literal differs")
    request, request_receipt = _load_request(root, request_id)
    _, _, binding = _load_plan(root, request.plan_id)
    exact_request = exact_streaming_approval_literal(request)
    slot_id = _standing_approval_slot_id(request, request_receipt)
    slot_path = safe_relative_path(root, _approval_path(slot_id))
    lock_path = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-streaming-standing-approval-request-{request.request_id}.lock",
    )
    if slot_path.exists():
        approval, receipt = _load_approval(root, slot_id)
        _verify_standing_slot(approval, request, request_receipt)
        return approval, StoredControl(approval.approval_id, receipt)
    with _exclusive_nonblocking_lock(lock_path):
        if slot_path.exists():
            approval, receipt = _load_approval(root, slot_id)
            _verify_standing_slot(approval, request, request_receipt)
            return approval, StoredControl(approval.approval_id, receipt)
        approved_at = _utc(now(), "standing approval runtime clock")
        approval = StreamingExactApproval(
            request_id=request_id,
            request=request_receipt,
            plan_id=request.plan_id,
            exact_literal_sha256=hashlib.sha256(exact_request.encode("utf-8")).hexdigest(),
            approved_at_utc=approved_at,
            approved_by=_text(approved_by, "approver"),
            authorization_mode="standing_s7",
            approval_availability=_approval_availability(root, binding, approved_at),
            standing_authorization={
                "literal_text": authorization_text,
                "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
            },
            standing_reaffirmation={
                "literal_text": reaffirmation_text,
                "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
            },
        )
        if approval.approval_id != slot_id:
            raise S7StreamingMaterializationError("standing approval slot identity differs")
        receipt = _write_immutable(
            root,
            approval.relative_path,
            _canonical_bytes(approval.to_dict()),
            "standing exact approval",
        )
        return approval, StoredControl(approval.approval_id, receipt)


def _standing_approval_slot_id(
    request: StreamingApprovalRequest, request_receipt: ExactFilePin
) -> str:
    prototype = StreamingExactApproval(
        request_id=request.request_id,
        request=request_receipt,
        plan_id=request.plan_id,
        exact_literal_sha256=hashlib.sha256(
            exact_streaming_approval_literal(request).encode("utf-8")
        ).hexdigest(),
        approved_at_utc=datetime(2000, 1, 1, tzinfo=UTC),
        approved_by="standing-slot-prototype",
        authorization_mode="standing_s7",
        approval_availability={
            "approval_recorded_at_utc": "2000-01-01T00:00:00+00:00",
            "calendar_artifact_id": "0" * 64,
            "calendar_artifact_sha256": "0" * 64,
            "first_xnys_open_utc": "2000-01-03T14:30:00+00:00",
            "rule": "standing-slot-prototype",
            "source_available_session": "2000-01-03",
        },
        standing_authorization={
            "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
            "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        },
        standing_reaffirmation={
            "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
            "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        },
    )
    return prototype.approval_id


def _verify_standing_slot(
    approval: StreamingExactApproval,
    request: StreamingApprovalRequest,
    request_receipt: ExactFilePin,
) -> None:
    if (
        approval.authorization_mode != "standing_s7"
        or approval.request_id != request.request_id
        or approval.plan_id != request.plan_id
        or approval.request != request_receipt
        or approval.approval_id != _standing_approval_slot_id(request, request_receipt)
    ):
        raise S7StreamingMaterializationError("standing approval slot binding differs")


@dataclass(slots=True)
class _OpenAlias:
    first_row: dict[str, object]
    last_row: dict[str, object]
    first_index: int
    last_index: int
    row_count: int
    key: tuple[object, ...]
    alias_id: str


_AGGREGATE_SET_CAP: Final = 100_000
_AGGREGATE_NAME_CAP: Final = 4_096


@dataclass(slots=True)
class _AliasSpill:
    """Disk-backed closed alias intervals; memory remains O(open tickers)."""

    path: Path
    connection: sqlite3.Connection
    row_count: int = 0

    @classmethod
    def create(cls, path: Path) -> _AliasSpill:
        if path.exists() or path.is_symlink():
            raise S7StreamingMaterializationError("alias spill target already exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute(
                """
                CREATE TABLE alias_intervals (
                    alias_id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    valid_from_session TEXT NOT NULL,
                    valid_through_session TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    UNIQUE (ticker, valid_from_session)
                ) WITHOUT ROWID
                """
            )
            connection.execute(
                "CREATE INDEX alias_sort ON alias_intervals (ticker, valid_from_session, asset_id)"
            )
            connection.commit()
        except sqlite3.Error:
            connection.close()
            raise
        return cls(path=path, connection=connection)

    def append(self, row: Mapping[str, object]) -> None:
        payload = _canonical_bytes(_json_value(dict(row)))
        try:
            self.connection.execute(
                "INSERT INTO alias_intervals VALUES (?, ?, ?, ?, ?, ?)",
                (
                    _digest(row["ticker_alias_id"], "ticker alias ID"),
                    _text(row["ticker"], "ticker alias ticker"),
                    _native_date(row["valid_from_session"], "ticker alias start").isoformat(),
                    _native_date(row["valid_through_session"], "ticker alias end").isoformat(),
                    _digest(row["asset_id"], "ticker alias asset ID"),
                    payload,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise S7StreamingMaterializationError(
                "ticker alias spill contains a duplicate interval"
            ) from exc
        self.row_count += 1

    def commit(self) -> None:
        try:
            self.connection.commit()
        except sqlite3.Error as exc:
            raise S7StreamingMaterializationError("cannot commit alias spill") from exc

    def close(self) -> None:
        self.connection.close()


@dataclass(slots=True)
class _AssetAggregate:
    canonical_composite_figi: str
    first_session: date | None = None
    last_session: date | None = None
    row_count: int = 0
    first_direct_session: date | None = None
    last_direct_session: date | None = None
    direct_row_count: int = 0
    tickers: set[str] = field(default_factory=set)
    observed_composites: set[str] = field(default_factory=set)
    observed_shares: set[str] = field(default_factory=set)
    canonical_shares: set[str] = field(default_factory=set)
    issuer_ids: set[str] = field(default_factory=set)
    identity_adjudication_ids: set[str] = field(default_factory=set)
    cross_market_adjudication_ids: set[str] = field(default_factory=set)
    provider_override_ids: set[str] = field(default_factory=set)
    share_adjudication_ids: set[str] = field(default_factory=set)
    transition_ids: set[str] = field(default_factory=set)
    conflict_rows: int = 0
    eligible_rows: int = 0
    evidence_available_session: date | None = None


@dataclass(slots=True)
class _IssuerAggregate:
    cik_normalized: str
    first_session: date | None = None
    last_session: date | None = None
    row_count: int = 0
    asset_ids: set[str] = field(default_factory=set)
    tickers: set[str] = field(default_factory=set)
    names: set[str] = field(default_factory=set)
    excluded_rows: int = 0
    excluded_cross_market_rows: int = 0
    reference_available_session: date | None = None


def execute_streaming_full_candidate(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
) -> StreamingFullRunResult:
    """Execute production Full with only the frozen internal adapter and loaders."""

    return _execute_streaming_full_candidate(
        data_root,
        plan_id=plan_id,
        approval_id=approval_id,
        production_execution=True,
        adapter=FrozenRegistryProjectionAdapter(),
        registry_loader=load_registry_release_set,
        runtime_probe=_repository_runtime_binding,
        now=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        rss_probe=None,
        disk_free_probe=None,
        checkpoint_hook=None,
    )


def _execute_streaming_full_candidate_fixture(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
    adapter: StreamingProjectionAdapter,
    registry_loader: Callable[..., LoadedRegistryReleaseSet] = load_registry_release_set,
    runtime_probe: Callable[[], Mapping[str, object]] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    rss_probe: Callable[[], int] | None = None,
    disk_free_probe: Callable[[Path], int] | None = None,
    checkpoint_hook: Callable[[str, date | None], None] | None = None,
) -> StreamingFullRunResult:
    """Fixture-only Full runner with deterministic fault-injection hooks."""

    return _execute_streaming_full_candidate(
        data_root,
        plan_id=plan_id,
        approval_id=approval_id,
        production_execution=False,
        adapter=adapter,
        registry_loader=registry_loader,
        runtime_probe=runtime_probe or _repository_runtime_binding,
        now=now,
        monotonic=monotonic,
        rss_probe=rss_probe,
        disk_free_probe=disk_free_probe,
        checkpoint_hook=checkpoint_hook,
    )


def _execute_streaming_full_candidate(
    data_root: Path,
    *,
    plan_id: str,
    approval_id: str,
    production_execution: bool,
    adapter: StreamingProjectionAdapter,
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    runtime_probe: Callable[[], Mapping[str, object]],
    now: Callable[[], datetime],
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    checkpoint_hook: Callable[[str, date | None], None] | None,
) -> StreamingFullRunResult:
    """Execute one exact two-pass Full and stop at ``awaiting_review``.

    ``checkpoint_hook`` is a test/fault-injection boundary.  An exception deliberately
    leaves staging intact; restart then fails closed instead of guessing whether a
    partition is complete.
    """

    root = _root(data_root)
    controls = _load_execution_controls(root, plan_id=plan_id, approval_id=approval_id)
    plan = controls["plan"]
    binding = controls["binding"]
    approval = controls["approval"]
    if production_execution != (binding.mode == "production"):
        raise S7StreamingMaterializationError(
            "production and fixture Full execution boundaries cannot be crossed"
        )
    caps = StreamingResourceCaps.from_dict(plan["resource_caps"])
    current_runtime = dict(runtime_probe())
    _validate_runtime_binding(current_runtime)
    if current_runtime != dict(binding.runtime_binding):
        raise S7StreamingMaterializationError("runtime Git/source binding differs")
    if binding.contract_approvals != _trusted_contract_approvals(root, binding):
        raise S7StreamingMaterializationError(
            "four v4 derived-contract approvals are incomplete or changed"
        )
    if production_execution and type(adapter) is not FrozenRegistryProjectionAdapter:
        raise S7StreamingMaterializationError(
            "production projection adapter is not frozen; Full fails closed"
        )
    if production_execution and (
        adapter.production_ready is not True
        or adapter.adapter_version != PRODUCTION_ADAPTER_VERSION
    ):
        raise S7StreamingMaterializationError("production adapter identity differs")
    _text(adapter.adapter_version, "projection adapter version")
    start = monotonic()
    candidate_id = stable_digest(
        {
            "adapter_version": adapter.adapter_version,
            "approval_id": approval.approval_id,
            "engine_version": STREAMING_POLICY_VERSION,
            "plan_id": plan_id,
            "source_binding_id": binding.source_binding_id,
        }
    )
    candidate_relative = _candidate_path(candidate_id)
    completion_relative = _completion_path(plan_id, approval_id)
    lock_path = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-streaming-full-plan-{plan_id}.lock",
    )
    with _exclusive_nonblocking_lock(lock_path):
        candidate_path = safe_relative_path(root, candidate_relative)
        completion_path = safe_relative_path(root, completion_relative)
        if completion_path.exists():
            result = _verify_completion_and_candidate(
                root,
                completion_path,
                plan=plan,
                approval=approval,
                binding=binding,
                expected_candidate_id=candidate_id,
                caps=caps,
                idempotent=True,
            )
            _check_resource_caps(
                root,
                staging=None,
                started=start,
                caps=caps,
                monotonic=monotonic,
                rss_probe=rss_probe,
                disk_free_probe=disk_free_probe,
            )
            _load_verified_execution_sources(
                root,
                binding=binding,
                registry_loader=registry_loader,
            )
            return result
        if candidate_path.exists():
            candidate = _verify_candidate(
                root,
                candidate_path,
                plan=plan,
                approval=approval,
                binding=binding,
                expected_candidate_id=candidate_id,
                caps=caps,
            )
            _load_verified_execution_sources(
                root,
                binding=binding,
                registry_loader=registry_loader,
            )
            return _store_and_verify_completion(
                root,
                candidate,
                plan=plan,
                approval=approval,
                binding=binding,
                completion_path=completion_path,
                now=now,
                started=start,
                monotonic=monotonic,
                rss_probe=rss_probe,
                disk_free_probe=disk_free_probe,
                idempotent=True,
            )
        staging = safe_relative_path(
            root,
            f"tmp/silver-s7-streaming-full/candidate_id={candidate_id}",
        )
        if staging.exists() or staging.is_symlink():
            raise S7StreamingMaterializationError(
                "incomplete streaming Full staging requires explicit review"
            )
        _check_resource_caps(
            root,
            staging=None,
            started=start,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
        )
        intent = _store_run_intent(
            root,
            plan=plan,
            approval=approval,
            binding=binding,
            candidate_id=candidate_id,
            now=now,
        )
        if checkpoint_hook is not None:
            checkpoint_hook("intent_durable", None)

        # The line above is the last control-only boundary.  Everything below may read
        # source content and therefore must be downstream of the durable intent.
        _check_resource_caps(
            root,
            staging=None,
            started=start,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
        )
        registries, gate_b = _load_verified_execution_sources(
            root,
            binding=binding,
            registry_loader=registry_loader,
        )
        staging.mkdir(parents=True, exist_ok=False)
        resolved_dir = staging / "_resolved"
        resolved_dir.mkdir()
        outputs_dir = staging / "data"
        outputs_dir.mkdir()
        aliases, assets, issuers, pass1 = _run_pass_one(
            root,
            staging=staging,
            resolved_dir=resolved_dir,
            binding=binding,
            caps=caps,
            gate_b=gate_b,
            registries=registries,
            adapter=adapter,
            started=start,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            checkpoint_hook=checkpoint_hook,
        )
        output_receipts, table_counts = _run_pass_two(
            staging=staging,
            resolved_dir=resolved_dir,
            binding=binding,
            caps=caps,
            aliases=aliases,
            assets=assets,
            issuers=issuers,
            registries=registries,
            started=start,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            root=root,
            checkpoint_hook=checkpoint_hook,
        )
        shutil.rmtree(resolved_dir)
        shutil.rmtree(staging / "_spill")
        qa = _build_qa(
            pass1=pass1,
            table_counts=table_counts,
            alias_row_count=aliases.row_count,
            binding=binding,
        )
        if qa["critical_failure_count"] != 0:
            raise S7StreamingMaterializationError("streaming Full critical QA is nonzero")
        qa_path = staging / "qa/qa.json"
        qa_path.parent.mkdir()
        _write_exclusive(qa_path, _canonical_bytes(qa))
        output_receipts["qa"] = _path_receipt(qa_path, "qa/qa.json")
        manifest_payload = {
            "adapter_version": adapter.adapter_version,
            "approval_id": approval.approval_id,
            "artifact_type": "s7_streaming_four_table_full_candidate",
            "candidate_id": candidate_id,
            "candidate_version": STREAMING_CANDIDATE_VERSION,
            "capabilities": dict(_FALSE_CAPABILITIES),
            "contract_pins": _contract_pins(),
            "intent": intent.to_dict(),
            "outputs": output_receipts,
            "plan_id": plan_id,
            "policy_version": STREAMING_POLICY_VERSION,
            "source_binding_id": binding.source_binding_id,
            "state": STREAMING_STATE,
            "table_row_counts": table_counts,
        }
        manifest = {
            **manifest_payload,
            "manifest_id": stable_digest(manifest_payload),
        }
        _write_exclusive(staging / "manifest.json", _canonical_bytes(manifest))
        _fsync_tree(staging)
        candidate_path.parent.mkdir(parents=True, exist_ok=True)
        _fsync_directory(staging.parent)
        _fsync_directory(candidate_path.parent)
        if checkpoint_hook is not None:
            checkpoint_hook("before_candidate_publish", None)
        _rename_directory_noreplace(staging, candidate_path)
        _fsync_directory(candidate_path)
        _fsync_directory(staging.parent)
        _fsync_directory(candidate_path.parent)
        candidate = _verify_candidate(
            root,
            candidate_path,
            plan=plan,
            approval=approval,
            binding=binding,
            expected_candidate_id=candidate_id,
            caps=caps,
        )
        return _store_and_verify_completion(
            root,
            candidate,
            plan=plan,
            approval=approval,
            binding=binding,
            completion_path=completion_path,
            now=now,
            started=start,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
            idempotent=False,
        )


def _run_pass_one(
    root: Path,
    *,
    staging: Path,
    resolved_dir: Path,
    binding: S7StreamingSourceBinding,
    caps: StreamingResourceCaps,
    gate_b: Mapping[str, Mapping[str, object]],
    registries: LoadedRegistryReleaseSet,
    adapter: StreamingProjectionAdapter,
    started: float,
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    checkpoint_hook: Callable[[str, date | None], None] | None,
) -> tuple[
    _AliasSpill,
    dict[str, _AssetAggregate],
    dict[str, _IssuerAggregate],
    dict[str, object],
]:
    sessions = tuple(item.session_date for item in binding.membership_artifacts)
    open_aliases: dict[str, _OpenAlias] = {}
    aliases = _AliasSpill.create(staging / "_spill/aliases.sqlite3")
    assets: dict[str, _AssetAggregate] = {}
    issuers: dict[str, _IssuerAggregate] = {}
    lineage = hashlib.sha256()
    source_rows = 0
    raw_collision_rows = 0
    unresolved_rows = 0
    share_relation_conflict_rows = 0
    share_relation_mismatch_rows = 0
    unadjudicated_share_conflict_rows = 0
    unadjudicated_share_conflict_eligible_rows = 0
    collision_examples: list[dict[str, object]] = []
    share_conflict_examples: list[dict[str, object]] = []
    for session_index, pin in enumerate(binding.membership_artifacts):
        source_path = safe_relative_path(root, pin.artifact.path)
        source = pq.read_table(source_path)
        if source.schema != UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema:
            raise S7StreamingMaterializationError("S4 session Parquet schema differs")
        if source.num_rows != pin.row_count:
            raise S7StreamingMaterializationError("S4 session row count differs")
        source = source.sort_by([("ticker", "ascending")])
        source_rows_list = source.to_pylist()
        if any(row["session_date"] != pin.session_date for row in source_rows_list):
            raise S7StreamingMaterializationError("S4 partition crosses its pinned session")
        if len({str(row["ticker"]) for row in source_rows_list}) != len(source_rows_list):
            raise S7StreamingMaterializationError("S4 session ticker membership is duplicated")
        projections = tuple(
            adapter.resolve_session(
                source,
                gate_b_by_composite=gate_b,
                registries=registries,
                binding=binding,
            )
        )
        by_source = {item.selected_source_record_id: item for item in projections}
        expected_ids = {str(row["selected_source_record_id"]) for row in source_rows_list}
        if len(by_source) != len(projections) or set(by_source) != expected_ids:
            raise S7StreamingMaterializationError("projection source-row coverage differs")
        resolved_rows: list[dict[str, object]] = []
        seen_tickers: set[str] = set()
        session_source_ids: set[str] = set()
        for source_row in source_rows_list:
            source_id = _digest(source_row["selected_source_record_id"], "source record ID")
            if source_id in session_source_ids:
                raise S7StreamingMaterializationError(
                    "source record ID is duplicated within one membership session"
                )
            session_source_ids.add(source_id)
            projection = by_source[source_id]
            row = _build_and_validate_universe_row(
                source_row,
                projection,
                gate_b=gate_b,
                registries=registries,
                binding=binding,
            )
            resolved_rows.append(row)
            ticker = str(row["ticker"])
            seen_tickers.add(ticker)
            if row["composite_registry_collision"]:
                raw_collision_rows += 1
                if len(collision_examples) < 100:
                    collision_examples.append(
                        {
                            "selected_source_record_id": source_id,
                            "session_date": pin.session_date.isoformat(),
                            "ticker": ticker,
                        }
                    )
            if not row["backtest_identity_eligible"]:
                unresolved_rows += 1
            (
                relation_conflict,
                relation_mismatch,
                unadjudicated_conflict,
                eligible_violation,
                selected_share,
            ) = _gate_b_share_class_state(row, gate_b)
            share_relation_conflict_rows += int(relation_conflict)
            share_relation_mismatch_rows += int(relation_mismatch)
            unadjudicated_share_conflict_rows += int(unadjudicated_conflict)
            unadjudicated_share_conflict_eligible_rows += int(eligible_violation)
            if relation_conflict and len(share_conflict_examples) < 100:
                share_conflict_examples.append(
                    {
                        "backtest_identity_eligible": row["backtest_identity_eligible"],
                        "gate_b_selected_share_class_figi": selected_share,
                        "observed_composite_figi": row["observed_composite_figi"],
                        "observed_share_class_figi": row["observed_share_class_figi"],
                        "selected_source_record_id": source_id,
                        "session_date": pin.session_date.isoformat(),
                        "share_class_adjudication_id": row["share_class_adjudication_id"],
                        "ticker": ticker,
                    }
                )
            _update_alias_interval(
                aliases,
                open_aliases,
                row=row,
                session_index=session_index,
                sessions=sessions,
            )
            _update_aggregates(
                assets,
                issuers,
                row=row,
                source_name=source_row.get("name"),
            )
            lineage.update(
                _canonical_bytes(
                    {
                        "selected_source_record_id": source_id,
                        "session_date": pin.session_date.isoformat(),
                        "ticker": ticker,
                    }
                )
            )
            lineage.update(b"\n")
        for ticker in sorted(set(open_aliases) - seen_tickers):
            aliases.append(_close_alias(open_aliases.pop(ticker), sessions=sessions))
        aliases.commit()
        resolved_table = _contract_table("universe_daily", resolved_rows)
        relative = f"session_date={pin.session_date.isoformat()}/part-00000.parquet"
        _write_parquet_exclusive(resolved_dir / relative, resolved_table)
        source_rows += len(resolved_rows)
        _check_resource_caps(
            root,
            staging=staging,
            started=started,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
        )
        if checkpoint_hook is not None:
            checkpoint_hook("pass1_session_committed", pin.session_date)
    for ticker in sorted(open_aliases):
        aliases.append(_close_alias(open_aliases[ticker], sessions=sessions))
    aliases.commit()
    aliases.close()
    pass1 = {
        "bounded_collision_examples": collision_examples,
        "bounded_share_class_conflict_examples": share_conflict_examples,
        "gate_b_relation_share_class_conflict_rows": share_relation_conflict_rows,
        "gate_b_relation_share_class_mismatch_rows": share_relation_mismatch_rows,
        "raw_collision_rows": raw_collision_rows,
        "source_membership_rows": source_rows,
        "source_lineage_digest": lineage.hexdigest(),
        "source_record_id_count": source_rows,
        "unadjudicated_gate_b_share_class_conflict_eligible_rows": (
            unadjudicated_share_conflict_eligible_rows
        ),
        "unadjudicated_gate_b_share_class_conflict_rows": (unadjudicated_share_conflict_rows),
        "unresolved_rows": unresolved_rows,
    }
    return aliases, assets, issuers, pass1


def _gate_b_share_class_state(
    row: Mapping[str, object],
    gate_b: Mapping[str, Mapping[str, object]],
) -> tuple[bool, bool, bool, bool, str | None]:
    observed = _optional_figi(row.get("observed_composite_figi"), "observed Composite")
    if observed is None:
        return False, False, False, False, None
    gate_row = gate_b.get(observed)
    if gate_row is None:
        raise S7StreamingMaterializationError(
            "Gate-B reference inventory has an unattempted Composite"
        )
    relation_conflict = _native_bool(
        gate_row.get("relation_share_class_conflict", False),
        "Gate-B relation ShareClass conflict",
    )
    selected_share = _optional_figi(
        gate_row.get("selected_share_class_figi"),
        "Gate-B selected ShareClass",
    )
    observed_share = _optional_figi(
        row.get("observed_share_class_figi"),
        "observed ShareClass",
    )
    mismatch = relation_conflict and (
        observed_share is None or selected_share is None or observed_share != selected_share
    )
    unadjudicated = mismatch and row.get("share_class_adjudication_id") is None
    eligible_violation = unadjudicated and _native_bool(
        row.get("backtest_identity_eligible"),
        "backtest identity eligibility",
    )
    return relation_conflict, mismatch, unadjudicated, eligible_violation, selected_share


def _build_and_validate_universe_row(
    source: Mapping[str, object],
    projection: ResolutionProjection,
    *,
    gate_b: Mapping[str, Mapping[str, object]],
    registries: LoadedRegistryReleaseSet,
    binding: S7StreamingSourceBinding,
) -> dict[str, object]:
    session = _native_date(source.get("session_date"), "source session")
    ticker = _text(source.get("ticker"), "source ticker")
    source_id = _digest(source.get("selected_source_record_id"), "source record ID")
    if projection.selected_source_record_id != source_id:
        raise S7StreamingMaterializationError("projection source record ID changed")
    active = _native_bool(source.get("active_on_date"), "active_on_date")
    observed_composite = _optional_figi(source.get("composite_figi"), "observed Composite")
    observed_share = _optional_figi(source.get("share_class_figi"), "observed ShareClass")
    mic = _optional_mic(source.get("primary_exchange_mic"), "primary exchange MIC")
    observed_cik = _normalize_cik(source.get("cik"))
    if projection.canonical_cik_normalized != observed_cik:
        raise S7StreamingMaterializationError("identity correction attempted to change CIK")
    expected_issuer = _issuer_id(observed_cik) if observed_cik is not None else None
    if projection.issuer_id != expected_issuer:
        raise S7StreamingMaterializationError("projection issuer ID differs from observed CIK")
    expected_observed_asset = (
        canonical_asset_id(observed_composite) if observed_composite is not None else None
    )
    if projection.observed_asset_id != expected_observed_asset:
        raise S7StreamingMaterializationError("observed asset ID derivation differs")

    exact_source: ExactSourceRow | None = None
    matches: tuple[tuple[str, str], ...] = ()
    share_matches: tuple[str, ...] = ()
    transition_matches: tuple[str, ...] = ()
    gate_row: Mapping[str, object] | None = None
    if observed_composite is not None:
        gate_row = gate_b.get(observed_composite)
        if gate_row is None:
            raise S7StreamingMaterializationError(
                "Gate-B reference inventory has an unattempted Composite"
            )
        exact_source = ExactSourceRow(
            session_date=session,
            source_record_id=source_id,
            source_dataset="universe_source_daily",
            source_s4_release_set_id=binding.s4_release_set_id,
            provider_id="massive",
            provider_market=_text(source.get("market"), "provider market"),
            provider_locale=_text(source.get("locale"), "provider locale"),
            ticker=ticker,
            observed_composite_figi=observed_composite,
            observed_share_class_figi=observed_share,
            primary_exchange_mic=mic,
        )
        matches = tuple(
            registries.composite_matches(exact_source, cutoff_session=binding.cutoff_session)
        )
        share_release = registries.by_name("share_class_adjudication")
        share_matches = tuple(
            share_release.decision_ids_for_exact_source_row(
                exact_source, cutoff_session=binding.cutoff_session
            )
        )
        transition_release = registries.by_name("asset_transition")
        transition_matches = tuple(
            transition_release.decision_ids_for_exact_source_row(
                exact_source, cutoff_session=binding.cutoff_session
            )
        )
    if matches != tuple(sorted(matches)) or len(set(matches)) != len(matches):
        raise S7StreamingMaterializationError("registry Composite matches are not deterministic")
    if projection.composite_registry_match_count != len(matches):
        raise S7StreamingMaterializationError("projection Composite match count differs")
    if projection.composite_registry_collision != (len(matches) > 1):
        raise S7StreamingMaterializationError("projection Composite collision differs")
    if projection.asset_transition_ids != tuple(sorted(transition_matches)):
        raise S7StreamingMaterializationError("projection transition lineage differs")

    observed_market = (
        None
        if gate_row is None
        else _optional_text(gate_row.get("selected_market_code"), "Gate-B selected market")
    )
    gate_selected_share = (
        None
        if gate_row is None
        else _optional_figi(
            gate_row.get("selected_share_class_figi"),
            "Gate-B selected ShareClass",
        )
    )
    gate_share_conflict = (
        False
        if gate_row is None
        else _native_bool(
            gate_row.get("relation_share_class_conflict", False),
            "Gate-B relation ShareClass conflict",
        )
    )
    if projection.observed_composite_market_code != observed_market:
        raise S7StreamingMaterializationError("projection observed market differs from Gate B")
    composite_unique = False
    expected_canonical: str | None = None
    expected_canonical_market: str | None = None
    expected_asset: str | None = None
    if len(matches) > 1:
        if (
            any(
                value is not None
                for value in (
                    projection.canonical_composite_figi,
                    projection.canonical_composite_market_code,
                    projection.asset_id,
                    projection.canonical_share_class_figi,
                    projection.share_class_id,
                    projection.identity_adjudication_id,
                    projection.cross_market_adjudication_id,
                    projection.provider_composite_override_id,
                )
            )
            or projection.backtest_identity_eligible
        ):
            raise S7StreamingMaterializationError(
                "multi-registry Composite collision did not remain unresolved/ineligible"
            )
        if (
            projection.identity_resolution_status != "unresolved_registry_collision"
            or projection.identity_resolution_method != "registry_collision_unresolved"
            or projection.identity_disposition != "registry_collision_unresolved"
        ):
            raise S7StreamingMaterializationError("collision resolution status differs")
    elif len(matches) == 1:
        assert exact_source is not None
        registry_name, decision_id = matches[0]
        release = registries.by_name(registry_name)
        decision = release.require_exact_source_row(
            decision_id,
            exact_source,
            cutoff_session=binding.cutoff_session,
        )
        expected_canonical, expected_asset = _composite_decision_target(registry_name, decision)
        expected_canonical_market = _optional_text(
            decision.get("canonical_composite_market_code"),
            "canonical Composite market",
        )
        _validate_composite_decision_projection(
            projection,
            registry_name=registry_name,
            decision_id=decision_id,
            decision=decision,
        )
        composite_unique = expected_canonical is not None
    elif observed_composite is not None and gate_row is not None:
        classification = _text(gate_row.get("classification"), "Gate-B classification")
        if classification in GATE_B_US:
            expected_canonical = observed_composite
            expected_canonical_market = observed_market
            expected_asset = canonical_asset_id(observed_composite)
            composite_unique = True
        elif projection.backtest_identity_eligible:
            raise S7StreamingMaterializationError(
                "unknown/non-US unapproved Composite became identity eligible"
            )
        if any(
            value is not None
            for value in (
                projection.identity_adjudication_id,
                projection.cross_market_adjudication_id,
                projection.provider_composite_override_id,
            )
        ):
            raise S7StreamingMaterializationError("projection invented a Composite decision")
    if not composite_unique and share_matches:
        raise S7StreamingMaterializationError(
            "ShareClass correction preceded unique canonical Composite"
        )
    if not composite_unique:
        share_matches = ()
    expected_share = observed_share if composite_unique else None
    expected_share_id = _share_class_id(expected_share) if expected_share is not None else None
    unresolved_gate_share_conflict = False
    if len(share_matches) > 1:
        if (
            projection.backtest_identity_eligible
            or projection.canonical_composite_figi is not None
            or projection.canonical_composite_market_code is not None
            or projection.asset_id is not None
            or projection.canonical_share_class_figi is not None
            or projection.share_class_id is not None
            or projection.share_class_adjudication_id is not None
            or projection.share_class_adjudication_available_session is not None
            or projection.identity_resolution_status != "resolved_conflicted"
            or projection.identity_disposition != "registry_collision_unresolved"
        ):
            raise S7StreamingMaterializationError(
                "multiple ShareClass decisions did not remain unresolved/ineligible"
            )
        expected_share = None
        expected_share_id = None
        expected_canonical = None
        expected_canonical_market = None
        expected_asset = None
    elif len(share_matches) == 1:
        if not composite_unique or exact_source is None:
            raise S7StreamingMaterializationError(
                "ShareClass correction preceded unique canonical Composite"
            )
        share_id = share_matches[0]
        release = registries.by_name("share_class_adjudication")
        decision = release.require_exact_source_row(
            share_id,
            exact_source,
            cutoff_session=binding.cutoff_session,
        )
        required_composite = _figi(
            decision.get("required_unique_canonical_composite_figi"),
            "ShareClass required Composite",
        )
        if required_composite != expected_canonical:
            raise S7StreamingMaterializationError("ShareClass decision parent Composite differs")
        expected_share = _figi(decision.get("canonical_share_class_figi"), "canonical ShareClass")
        expected_share_id = _digest(
            decision.get("canonical_share_class_id"), "canonical ShareClass ID"
        )
        if expected_share_id != _share_class_id(expected_share):
            raise S7StreamingMaterializationError("ShareClass ID is not reproducible")
        available = _native_date(
            decision.get("adjudication_available_session"),
            "ShareClass availability",
        )
        if (
            projection.share_class_adjudication_id != share_id
            or projection.share_class_adjudication_available_session != available
        ):
            raise S7StreamingMaterializationError("ShareClass decision projection differs")
    elif (
        projection.share_class_adjudication_id is not None
        or projection.share_class_adjudication_available_session is not None
    ):
        raise S7StreamingMaterializationError("projection invented a ShareClass decision")
    elif gate_share_conflict and (
        observed_share is None
        or gate_selected_share is None
        or observed_share != gate_selected_share
    ):
        expected_share = None
        expected_share_id = None
        unresolved_gate_share_conflict = True
        if (
            projection.backtest_identity_eligible
            or projection.identity_resolution_status != "resolved_conflicted"
        ):
            raise S7StreamingMaterializationError(
                "unadjudicated Gate-B ShareClass conflict became eligible or resolved"
            )
    if (
        projection.canonical_share_class_figi != expected_share
        or projection.share_class_id != expected_share_id
    ):
        raise S7StreamingMaterializationError("projection canonical ShareClass differs")
    if projection.canonical_composite_figi != expected_canonical:
        raise S7StreamingMaterializationError("projection canonical Composite differs")
    if projection.canonical_composite_market_code != expected_canonical_market:
        raise S7StreamingMaterializationError("projection canonical Composite market differs")
    if projection.asset_id != expected_asset:
        raise S7StreamingMaterializationError("projection asset ID differs")
    if expected_canonical is not None and expected_asset != canonical_asset_id(expected_canonical):
        raise S7StreamingMaterializationError("canonical asset ID is not reproducible")
    if projection.backtest_identity_eligible and not composite_unique:
        raise S7StreamingMaterializationError("unresolved Composite became identity eligible")
    if unresolved_gate_share_conflict and projection.backtest_identity_eligible:
        raise S7StreamingMaterializationError(
            "unadjudicated Gate-B ShareClass conflict became identity eligible"
        )
    if projection.current_reference_factor_eligible and not projection.backtest_identity_eligible:
        raise S7StreamingMaterializationError("factor eligibility exceeds identity eligibility")
    if projection.identity_evidence_available_session > binding.cutoff_session:
        raise S7StreamingMaterializationError("identity evidence is unavailable at cutoff")

    source_fields = _source_binding_columns(binding)
    row = {
        "session_year": session.year,
        "session_date": session,
        "ticker": ticker,
        "active_on_date": active,
        "asset_id": projection.asset_id,
        "share_class_id": projection.share_class_id,
        "canonical_share_class_figi": projection.canonical_share_class_figi,
        "issuer_id": projection.issuer_id,
        "canonical_cik_normalized": projection.canonical_cik_normalized,
        "ticker_alias_id": None,
        "type_code": source.get("type_code"),
        "primary_exchange_mic": mic,
        "observed_cik_normalized": observed_cik,
        "observed_composite_figi": observed_composite,
        "observed_composite_market_code": projection.observed_composite_market_code,
        "observed_asset_id": projection.observed_asset_id,
        "canonical_composite_figi": projection.canonical_composite_figi,
        "canonical_composite_market_code": projection.canonical_composite_market_code,
        "observed_share_class_figi": observed_share,
        "identity_resolution_status": projection.identity_resolution_status,
        "identity_resolution_method": projection.identity_resolution_method,
        "identity_disposition": projection.identity_disposition,
        "identity_case_id": projection.identity_case_id,
        "identity_case_available_session": projection.identity_case_available_session,
        "source_identity_case_candidate_manifest_id": (
            binding.gate_c.identity_case_preview_id
        ),
        "source_identity_case_candidate_manifest_sha256": (
            binding.gate_c.identity_case_preview_manifest.sha256
        ),
        "identity_adjudication_id": projection.identity_adjudication_id,
        "cross_market_scope_id": projection.cross_market_scope_id,
        "cross_market_adjudication_id": projection.cross_market_adjudication_id,
        "cross_market_adjudication_available_session": (
            projection.cross_market_adjudication_available_session
        ),
        "cross_market_classification_status": projection.cross_market_classification_status,
        "identity_case_resolution_role": projection.identity_case_resolution_role,
        "adjudication_available_session": projection.adjudication_available_session,
        "identity_resolution_cutoff_session": binding.cutoff_session,
        "backtest_identity_eligible": projection.backtest_identity_eligible,
        "position_continuity_status": (
            POSITION_CONTINUITY_RESOLVED
            if projection.backtest_identity_eligible
            else POSITION_CONTINUITY_UNCERTAIN
        ),
        "identity_quality_liquidation_signal": False,
        "current_reference_factor_eligible": projection.current_reference_factor_eligible,
        "security_type_scope": projection.security_type_scope,
        "selected_source_record_id": source_id,
        "source_version_count": _positive(source.get("source_version_count"), "source versions"),
        "source_selection_status": _text(source.get("selection_status"), "selection status"),
        "membership_time_scope": _text(source.get("reference_time_scope"), "membership scope"),
        "membership_source_available_session": _native_date(
            source.get("source_available_session"), "membership availability"
        ),
        "membership_source_availability_quality": _text(
            source.get("source_availability_quality"), "membership availability quality"
        ),
        "metadata_time_scope": _text(source.get("metadata_time_scope"), "metadata scope"),
        "identity_mapping_time_scope": "cutoff_bound_registry_and_current_reference_v1",
        "identity_evidence_available_session": projection.identity_evidence_available_session,
        "resolution_rule_version": UNIVERSE_RESOLUTION_RULE_VERSION,
        **source_fields,
        "provider_composite_override_id": projection.provider_composite_override_id,
        "provider_composite_override_available_session": (
            projection.provider_composite_override_available_session
        ),
        "share_class_adjudication_id": projection.share_class_adjudication_id,
        "share_class_adjudication_available_session": (
            projection.share_class_adjudication_available_session
        ),
        "asset_transition_ids": list(projection.asset_transition_ids),
        "composite_registry_match_count": projection.composite_registry_match_count,
        "composite_registry_collision": projection.composite_registry_collision,
    }
    return row


def _validate_composite_decision_projection(
    projection: ResolutionProjection,
    *,
    registry_name: str,
    decision_id: str,
    decision: Mapping[str, object],
) -> None:
    if registry_name == "identity_adjudication":
        if (
            projection.identity_adjudication_id != decision_id
            or projection.cross_market_adjudication_id is not None
            or projection.provider_composite_override_id is not None
        ):
            raise S7StreamingMaterializationError("identity adjudication projection differs")
    elif registry_name == "identity_cross_market_adjudication":
        available = _native_date(
            decision.get("adjudication_available_session"), "cross-market availability"
        )
        if (
            projection.cross_market_adjudication_id != decision_id
            or projection.cross_market_scope_id != decision.get("cross_market_scope_id")
            or projection.cross_market_adjudication_available_session != available
            or projection.identity_adjudication_id is not None
            or projection.provider_composite_override_id is not None
        ):
            raise S7StreamingMaterializationError("cross-market projection differs")
    elif registry_name == "provider_composite_override":
        available = _native_date(
            decision.get("override_available_session"), "provider override availability"
        )
        if (
            projection.provider_composite_override_id != decision_id
            or projection.provider_composite_override_available_session != available
            or projection.identity_adjudication_id is not None
            or projection.cross_market_adjudication_id is not None
        ):
            raise S7StreamingMaterializationError("provider Composite override projection differs")
    else:  # pragma: no cover - registry loader owns the frozen set
        raise S7StreamingMaterializationError("unsupported Composite registry")


def _composite_decision_target(
    registry_name: str, decision: Mapping[str, object]
) -> tuple[str | None, str | None]:
    if registry_name == "identity_cross_market_adjudication":
        canonical_raw = decision.get("canonical_us_composite_figi")
    else:
        canonical_raw = decision.get("canonical_composite_figi")
    canonical = _optional_figi(canonical_raw, "decision canonical Composite")
    asset = _optional_digest(decision.get("canonical_asset_id"), "decision canonical asset ID")
    if (canonical is None) != (asset is None):
        raise S7StreamingMaterializationError("decision canonical Composite/asset pair differs")
    if canonical is not None and asset != canonical_asset_id(canonical):
        raise S7StreamingMaterializationError("decision canonical asset ID is not reproducible")
    return canonical, asset


def _update_alias_interval(
    aliases: _AliasSpill,
    open_aliases: dict[str, _OpenAlias],
    *,
    row: dict[str, object],
    session_index: int,
    sessions: Sequence[date],
) -> None:
    ticker = str(row["ticker"])
    current = open_aliases.get(ticker)
    eligible = bool(row["backtest_identity_eligible"])
    if not eligible:
        row["ticker_alias_id"] = None
        if current is not None:
            aliases.append(_close_alias(open_aliases.pop(ticker), sessions=sessions))
        return
    if (
        row["asset_id"] is None
        or row["canonical_composite_figi"] is None
        or row["observed_composite_figi"] is None
        or row["observed_asset_id"] is None
        or row["composite_registry_collision"]
    ):
        raise S7StreamingMaterializationError("eligible row lacks unique alias identity")
    key = _alias_identity_key(row)
    if current is not None and current.last_index + 1 == session_index and current.key == key:
        current.last_row = row
        current.last_index = session_index
        current.row_count += 1
        row["ticker_alias_id"] = current.alias_id
        return
    if current is not None:
        aliases.append(_close_alias(open_aliases.pop(ticker), sessions=sessions))
    alias_id = _alias_id_for_first_row(row)
    row["ticker_alias_id"] = alias_id
    open_aliases[ticker] = _OpenAlias(
        first_row=row,
        last_row=row,
        first_index=session_index,
        last_index=session_index,
        row_count=1,
        key=key,
        alias_id=alias_id,
    )


def _alias_identity_key(row: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(
        _json_scalar(row[field])
        for field in (
            "ticker",
            "asset_id",
            "observed_composite_figi",
            "observed_composite_market_code",
            "observed_asset_id",
            "observed_share_class_figi",
            "observed_cik_normalized",
            "canonical_composite_figi",
            "canonical_composite_market_code",
            "canonical_share_class_figi",
            "canonical_cik_normalized",
            "identity_disposition",
            "identity_adjudication_id",
            "cross_market_scope_id",
            "cross_market_adjudication_id",
            "cross_market_adjudication_available_session",
            "identity_case_resolution_role",
            "identity_case_id",
            "identity_case_available_session",
            "adjudication_available_session",
            "share_class_id",
            "issuer_id",
            "identity_resolution_method",
            "identity_resolution_status",
            "identity_evidence_available_session",
            "provider_composite_override_id",
            "provider_composite_override_available_session",
            "share_class_adjudication_id",
            "share_class_adjudication_available_session",
            "asset_transition_ids",
            "composite_registry_match_count",
            "composite_registry_collision",
        )
    )


def _close_alias(item: _OpenAlias, *, sessions: Sequence[date]) -> dict[str, object]:
    first = item.first_row
    last = item.last_row
    valid_to = sessions[item.last_index + 1] if item.last_index + 1 < len(sessions) else None
    payload = {
        "asset_id": first["asset_id"],
        "canonical_composite_figi": first["canonical_composite_figi"],
        "canonical_share_class_figi": first["canonical_share_class_figi"],
        "identity_adjudication_id": first["identity_adjudication_id"],
        "identity_case_id": first["identity_case_id"],
        "identity_resolution_cutoff_session": first["identity_resolution_cutoff_session"],
        "namespace": "ame_stocks.identity.ticker_alias",
        "observed_composite_figi": first["observed_composite_figi"],
        "observed_share_class_figi": first["observed_share_class_figi"],
        "provider_composite_override_id": first["provider_composite_override_id"],
        "rule_version": TICKER_ALIAS_ID_RULE_VERSION,
        "share_class_adjudication_id": first["share_class_adjudication_id"],
        "ticker": first["ticker"],
        "valid_from_session": first["session_date"],
    }
    alias_id = stable_digest(_json_value(payload))
    if alias_id != item.alias_id:
        raise S7StreamingMaterializationError("open ticker alias ID changed at close")
    row = {
        "ticker_alias_id": alias_id,
        "ticker_alias_id_rule_version": TICKER_ALIAS_ID_RULE_VERSION,
        "asset_id": first["asset_id"],
        "ticker": first["ticker"],
        "valid_from_session": first["session_date"],
        "valid_through_session": last["session_date"],
        "valid_to_session_exclusive": valid_to,
        "interval_end_status": "open_at_cutoff" if valid_to is None else "closed_at_next_session",
        "interval_session_count": item.row_count,
        "observed_composite_figi": first["observed_composite_figi"],
        "observed_composite_market_code": first["observed_composite_market_code"],
        "observed_asset_id": first["observed_asset_id"],
        "observed_share_class_figi": first["observed_share_class_figi"],
        "observed_cik_normalized": first["observed_cik_normalized"],
        "canonical_composite_figi": first["canonical_composite_figi"],
        "canonical_composite_market_code": first["canonical_composite_market_code"],
        "canonical_share_class_figi": first["canonical_share_class_figi"],
        "canonical_cik_normalized": first["canonical_cik_normalized"],
        "identity_disposition": first["identity_disposition"],
        "identity_adjudication_id": first["identity_adjudication_id"],
        "cross_market_scope_id": first["cross_market_scope_id"],
        "cross_market_adjudication_id": first["cross_market_adjudication_id"],
        "cross_market_adjudication_available_session": first[
            "cross_market_adjudication_available_session"
        ],
        "identity_case_resolution_role": first["identity_case_resolution_role"],
        "identity_case_id": first["identity_case_id"],
        "identity_case_available_session": first["identity_case_available_session"],
        "adjudication_available_session": first["adjudication_available_session"],
        "identity_resolution_cutoff_session": first["identity_resolution_cutoff_session"],
        "share_class_id": first["share_class_id"],
        "issuer_id": first["issuer_id"],
        "alias_resolution_method": first["identity_resolution_method"],
        "alias_resolution_status": first["identity_resolution_status"],
        "ticker_event_corroborated": False,
        "ticker_event_count": 0,
        "source_row_count": item.row_count,
        "first_source_record_id": first["selected_source_record_id"],
        "last_source_record_id": last["selected_source_record_id"],
        "backtest_identity_eligible": True,
        "membership_time_scope": first["membership_time_scope"],
        "metadata_time_scope": first["metadata_time_scope"],
        "identity_evidence_available_session": first["identity_evidence_available_session"],
        "resolution_rule_version": ALIAS_RESOLUTION_RULE_VERSION,
        **_copy_source_binding_columns(first),
        "provider_composite_override_id": first["provider_composite_override_id"],
        "provider_composite_override_available_session": first[
            "provider_composite_override_available_session"
        ],
        "share_class_adjudication_id": first["share_class_adjudication_id"],
        "share_class_adjudication_available_session": first[
            "share_class_adjudication_available_session"
        ],
        "asset_transition_ids": list(first["asset_transition_ids"]),
        "composite_registry_match_count": first["composite_registry_match_count"],
        "composite_registry_collision": False,
    }
    return row


def _alias_id_for_first_row(first: Mapping[str, object]) -> str:
    payload = {
        "asset_id": first["asset_id"],
        "canonical_composite_figi": first["canonical_composite_figi"],
        "canonical_share_class_figi": first["canonical_share_class_figi"],
        "identity_adjudication_id": first["identity_adjudication_id"],
        "identity_case_id": first["identity_case_id"],
        "identity_resolution_cutoff_session": first["identity_resolution_cutoff_session"],
        "namespace": "ame_stocks.identity.ticker_alias",
        "observed_composite_figi": first["observed_composite_figi"],
        "observed_share_class_figi": first["observed_share_class_figi"],
        "provider_composite_override_id": first["provider_composite_override_id"],
        "rule_version": TICKER_ALIAS_ID_RULE_VERSION,
        "share_class_adjudication_id": first["share_class_adjudication_id"],
        "ticker": first["ticker"],
        "valid_from_session": first["session_date"],
    }
    return stable_digest(_json_value(payload))


def _update_aggregates(
    assets: dict[str, _AssetAggregate],
    issuers: dict[str, _IssuerAggregate],
    *,
    row: Mapping[str, object],
    source_name: object,
) -> None:
    session = _native_date(row["session_date"], "resolved session")
    asset_id = row.get("asset_id")
    if isinstance(asset_id, str):
        canonical = _figi(row["canonical_composite_figi"], "aggregate canonical Composite")
        aggregate = assets.setdefault(asset_id, _AssetAggregate(canonical))
        if aggregate.canonical_composite_figi != canonical:
            raise S7StreamingMaterializationError("asset aggregate mixes canonical Composites")
        aggregate.first_session = _earlier(aggregate.first_session, session)
        aggregate.last_session = _later(aggregate.last_session, session)
        aggregate.row_count += 1
        if row.get("observed_composite_figi") == canonical:
            aggregate.first_direct_session = _earlier(aggregate.first_direct_session, session)
            aggregate.last_direct_session = _later(aggregate.last_direct_session, session)
            aggregate.direct_row_count += 1
        _bounded_add(aggregate.tickers, str(row["ticker"]), "asset ticker variants")
        if isinstance(row.get("observed_composite_figi"), str):
            _bounded_add(
                aggregate.observed_composites,
                str(row["observed_composite_figi"]),
                "asset observed Composite variants",
            )
        if isinstance(row.get("observed_share_class_figi"), str):
            _bounded_add(
                aggregate.observed_shares,
                str(row["observed_share_class_figi"]),
                "asset observed ShareClass variants",
            )
        if isinstance(row.get("canonical_share_class_figi"), str):
            _bounded_add(
                aggregate.canonical_shares,
                str(row["canonical_share_class_figi"]),
                "asset canonical ShareClass variants",
            )
        if isinstance(row.get("issuer_id"), str):
            _bounded_add(
                aggregate.issuer_ids,
                str(row["issuer_id"]),
                "asset issuer variants",
            )
        for field_name, target in (
            ("identity_adjudication_id", aggregate.identity_adjudication_ids),
            ("cross_market_adjudication_id", aggregate.cross_market_adjudication_ids),
            ("provider_composite_override_id", aggregate.provider_override_ids),
            ("share_class_adjudication_id", aggregate.share_adjudication_ids),
        ):
            if isinstance(row.get(field_name), str):
                _bounded_add(target, str(row[field_name]), f"asset {field_name} variants")
        for transition_id in row["asset_transition_ids"]:
            _bounded_add(
                aggregate.transition_ids,
                str(transition_id),
                "asset transition variants",
            )
        aggregate.conflict_rows += int(bool(row["composite_registry_collision"]))
        aggregate.eligible_rows += int(bool(row["backtest_identity_eligible"]))
        aggregate.evidence_available_session = _later(
            aggregate.evidence_available_session,
            _native_date(row["identity_evidence_available_session"], "evidence session"),
        )
    issuer_id = row.get("issuer_id")
    cik = row.get("canonical_cik_normalized")
    if isinstance(issuer_id, str) and isinstance(cik, str):
        issuer = issuers.setdefault(issuer_id, _IssuerAggregate(cik))
        if issuer.cik_normalized != cik:
            raise S7StreamingMaterializationError("issuer aggregate mixes normalized CIKs")
        issuer.first_session = _earlier(issuer.first_session, session)
        issuer.last_session = _later(issuer.last_session, session)
        issuer.row_count += 1
        _bounded_add(issuer.tickers, str(row["ticker"]), "issuer ticker variants")
        if isinstance(asset_id, str):
            _bounded_add(issuer.asset_ids, asset_id, "issuer asset variants")
        if isinstance(source_name, str) and source_name.strip():
            _bounded_add(
                issuer.names,
                source_name.strip(),
                "issuer reference-name variants",
                cap=_AGGREGATE_NAME_CAP,
            )
        issuer.excluded_rows += int(not bool(row["backtest_identity_eligible"]))
        issuer.excluded_cross_market_rows += int(
            row.get("cross_market_adjudication_id") is not None
        )
        issuer.reference_available_session = _later(
            issuer.reference_available_session,
            _native_date(row["membership_source_available_session"], "reference session"),
        )


def _run_pass_two(
    *,
    staging: Path,
    resolved_dir: Path,
    binding: S7StreamingSourceBinding,
    caps: StreamingResourceCaps,
    aliases: _AliasSpill,
    assets: Mapping[str, _AssetAggregate],
    issuers: Mapping[str, _IssuerAggregate],
    registries: LoadedRegistryReleaseSet,
    started: float,
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    root: Path,
    checkpoint_hook: Callable[[str, date | None], None] | None,
) -> tuple[dict[str, object], dict[str, int]]:
    outputs: dict[str, object] = {}
    alias_path = staging / "data/ticker_alias.parquet"
    alias_row_count = _write_alias_spill_parquet(aliases.path, alias_path)
    if alias_row_count != aliases.row_count:
        raise S7StreamingMaterializationError("ticker alias spill row count differs")
    outputs["ticker_alias"] = _path_receipt(
        alias_path,
        "data/ticker_alias.parquet",
        row_count=alias_row_count,
        schema_digest=S7_DERIVED_CONTRACTS["ticker_alias"].schema_digest,
    )
    transition_edges = _transition_edges(registries, binding)
    asset_rows = _asset_master_rows(assets, transition_edges=transition_edges, binding=binding)
    asset_table = _contract_table("asset_master", asset_rows)
    asset_path = staging / "data/asset_master.parquet"
    _write_parquet_exclusive(asset_path, asset_table)
    outputs["asset_master"] = _path_receipt(
        asset_path,
        "data/asset_master.parquet",
        row_count=asset_table.num_rows,
        schema_digest=S7_DERIVED_CONTRACTS["asset_master"].schema_digest,
    )
    issuer_rows = _issuer_master_rows(issuers, binding=binding)
    issuer_table = _contract_table("issuer_master", issuer_rows)
    issuer_path = staging / "data/issuer_master.parquet"
    _write_parquet_exclusive(issuer_path, issuer_table)
    outputs["issuer_master"] = _path_receipt(
        issuer_path,
        "data/issuer_master.parquet",
        row_count=issuer_table.num_rows,
        schema_digest=S7_DERIVED_CONTRACTS["issuer_master"].schema_digest,
    )
    universe_receipts: list[dict[str, object]] = []
    universe_rows = 0
    for pin in binding.membership_artifacts:
        resolved_path = resolved_dir / (
            f"session_date={pin.session_date.isoformat()}/part-00000.parquet"
        )
        table = pq.read_table(resolved_path)
        if table.schema != S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema:
            raise S7StreamingMaterializationError("resolved partition schema differs")
        rows = table.to_pylist()
        for row in rows:
            alias_id = row["ticker_alias_id"]
            if row["backtest_identity_eligible"] and alias_id is None:
                raise S7StreamingMaterializationError("eligible membership has no alias interval")
            if not row["backtest_identity_eligible"] and alias_id is not None:
                raise S7StreamingMaterializationError("ineligible membership received an alias")
            if alias_id is not None:
                _digest(alias_id, "membership ticker alias ID")
        final = _contract_table("universe_daily", rows)
        relative = (
            f"data/universe_daily/session_date={pin.session_date.isoformat()}/part-00000.parquet"
        )
        path = staging / relative
        _write_parquet_exclusive(path, final)
        universe_receipts.append(
            _path_receipt(
                path,
                relative,
                row_count=final.num_rows,
                schema_digest=S7_DERIVED_CONTRACTS["universe_daily"].schema_digest,
            )
        )
        universe_rows += final.num_rows
        _check_resource_caps(
            root,
            staging=staging,
            started=started,
            caps=caps,
            monotonic=monotonic,
            rss_probe=rss_probe,
            disk_free_probe=disk_free_probe,
        )
        if checkpoint_hook is not None:
            checkpoint_hook("pass2_session_committed", pin.session_date)
    outputs["universe_daily"] = universe_receipts
    table_counts = {
        "asset_master": asset_table.num_rows,
        "ticker_alias": alias_row_count,
        "issuer_master": issuer_table.num_rows,
        "universe_daily": universe_rows,
    }
    return outputs, table_counts


def _transition_edges(
    registries: LoadedRegistryReleaseSet,
    binding: S7StreamingSourceBinding,
) -> dict[str, dict[str, set[str]]]:
    release = registries.by_name("asset_transition")
    edges: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {"predecessors": set(), "successors": set()}
    )
    for decision_id, raw in release.decision_rows.items():
        row = dict(raw)
        available = _native_date(row.get("transition_available_session"), "transition availability")
        if available > binding.cutoff_session:
            continue
        predecessor = _digest(row.get("predecessor_asset_id"), "predecessor asset ID")
        successor = _digest(row.get("successor_asset_id"), "successor asset ID")
        if predecessor == successor:
            raise S7StreamingMaterializationError("asset transition self-edge is forbidden")
        if (
            row.get("relationship_effect") != "lineage_only_no_override_no_return_stitching"
            or row.get("identity_override_effect") != "none"
            or row.get("membership_effect") != "none"
            or row.get("return_stitching_effect") != "none_requires_future_entitlement_accounting"
            or row.get("identity_quality_liquidation_signal") is not False
        ):
            raise S7StreamingMaterializationError(
                "asset transition attempted override, stitching, or liquidation"
            )
        _digest(decision_id, "asset transition ID")
        edges[predecessor]["successors"].add(successor)
        edges[successor]["predecessors"].add(predecessor)
    return edges


def _asset_master_rows(
    assets: Mapping[str, _AssetAggregate],
    *,
    transition_edges: Mapping[str, Mapping[str, set[str]]],
    binding: S7StreamingSourceBinding,
) -> list[dict[str, object]]:
    unknown_edge_assets = set(transition_edges) - set(assets)
    if unknown_edge_assets:
        raise S7StreamingMaterializationError(
            "asset transition references an asset absent from resolved memberships"
        )
    source_fields = _source_binding_columns(binding)
    rows: list[dict[str, object]] = []
    for asset_id, item in sorted(assets.items()):
        canonical_shares = sorted(item.canonical_shares)
        share = canonical_shares[0] if len(canonical_shares) == 1 else None
        share_status = (
            "unique_share_class"
            if len(canonical_shares) == 1
            else "missing_share_class"
            if not canonical_shares
            else "temporal_multiple_share_classes"
        )
        edges = transition_edges.get(asset_id, {"predecessors": set(), "successors": set()})
        direct = not (
            item.identity_adjudication_ids
            or item.cross_market_adjudication_ids
            or item.provider_override_ids
        )
        if item.cross_market_adjudication_ids:
            basis = "approved_cross_market_external_anchor"
        elif direct:
            basis = "direct_observed_composite"
        else:
            basis = "approved_episode_adjudication"
        rows.append(
            {
                "asset_id": asset_id,
                "canonical_composite_figi": item.canonical_composite_figi,
                "canonical_identity_basis": basis,
                "asset_id_rule_version": ASSET_ID_RULE_VERSION,
                "share_class_id": _share_class_id(share) if share is not None else None,
                "share_class_id_rule_version": SHARE_CLASS_ID_RULE_VERSION,
                "canonical_share_class_figi": share,
                "share_class_resolution_status": share_status,
                "identity_resolution_status": "resolved_identity",
                "asset_status": "active_identity",
                "superseded_by_asset_id": None,
                "first_direct_observed_session": item.first_direct_session,
                "last_direct_observed_session": item.last_direct_session,
                "first_canonical_membership_session": _required_date(
                    item.first_session, "asset first membership session"
                ),
                "last_canonical_membership_session": _required_date(
                    item.last_session, "asset last membership session"
                ),
                "observed_ticker_count": len(item.tickers),
                "observed_composite_figi_count": len(item.observed_composites),
                "observed_share_class_figi_count": len(item.observed_shares),
                "observed_issuer_count": len(item.issuer_ids),
                "strong_evidence_row_count": item.row_count,
                "direct_observed_evidence_row_count": item.direct_row_count,
                "adjudicated_override_evidence_row_count": len(item.identity_adjudication_ids),
                "cross_market_override_evidence_row_count": len(item.cross_market_adjudication_ids),
                "cross_market_adjudication_count": len(item.cross_market_adjudication_ids),
                "identity_adjudication_count": len(item.identity_adjudication_ids),
                "genuine_transition_adjudication_count": len(item.transition_ids),
                "provider_contamination_adjudication_count": len(
                    item.identity_adjudication_ids | item.cross_market_adjudication_ids
                ),
                "candidate_evidence_row_count": item.row_count,
                "conflict_evidence_row_count": item.conflict_rows,
                "backtest_identity_eligible": item.eligible_rows > 0,
                "identity_mapping_time_scope": "retrospective_identity_reference_not_signal_v1",
                "identity_evidence_available_session": _required_date(
                    item.evidence_available_session, "asset evidence session"
                ),
                "identity_resolution_cutoff_session": binding.cutoff_session,
                "resolution_rule_version": ASSET_RESOLUTION_RULE_VERSION,
                **source_fields,
                "provider_composite_override_count": len(item.provider_override_ids),
                "share_class_adjudication_count": len(item.share_adjudication_ids),
                "predecessor_asset_ids": sorted(edges["predecessors"]),
                "successor_asset_ids": sorted(edges["successors"]),
            }
        )
    return rows


def _issuer_master_rows(
    issuers: Mapping[str, _IssuerAggregate],
    *,
    binding: S7StreamingSourceBinding,
) -> list[dict[str, object]]:
    source_fields = _source_binding_columns(binding)
    rows: list[dict[str, object]] = []
    for issuer_id, item in sorted(issuers.items(), key=lambda pair: pair[1].cik_normalized):
        names = sorted(item.names)
        rows.append(
            {
                "issuer_id": issuer_id,
                "cik_normalized": item.cik_normalized,
                "issuer_id_rule_version": ISSUER_ID_RULE_VERSION,
                "issuer_status": "active_reference",
                "superseded_by_issuer_id": None,
                "reference_name": names[0] if len(names) == 1 else None,
                "reference_name_variant_count": len(names),
                "reference_name_resolution_status": (
                    "unique_reference_name"
                    if len(names) == 1
                    else "missing_reference_name"
                    if not names
                    else "multiple_reference_names"
                ),
                "sic_code_current_reference": None,
                "sic_code_variant_count": 0,
                "sic_resolution_status": "missing_reference_sic",
                "first_observed_session": _required_date(
                    item.first_session, "issuer first observed session"
                ),
                "last_observed_session": _required_date(
                    item.last_session, "issuer last observed session"
                ),
                "observed_asset_count": len(item.asset_ids),
                "observed_ticker_count": len(item.tickers),
                "source_evidence_row_count": item.row_count,
                "excluded_contamination_evidence_row_count": item.excluded_rows,
                "excluded_cross_market_contamination_evidence_row_count": (
                    item.excluded_cross_market_rows
                ),
                "backtest_classification_eligible": False,
                "reference_time_scope": "retrospective_issuer_reference_not_pit_classification_v1",
                "reference_available_session": _required_date(
                    item.reference_available_session, "issuer reference session"
                ),
                "identity_resolution_cutoff_session": binding.cutoff_session,
                "resolution_rule_version": ISSUER_RESOLUTION_RULE_VERSION,
                **source_fields,
            }
        )
    return rows


def _build_alias_index(
    aliases: Sequence[Mapping[str, object]],
) -> dict[str, tuple[tuple[date, ...], tuple[Mapping[str, object], ...]]]:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in aliases:
        grouped[str(row["ticker"])].append(row)
    result: dict[str, tuple[tuple[date, ...], tuple[Mapping[str, object], ...]]] = {}
    for ticker, rows in grouped.items():
        ordered = tuple(sorted(rows, key=lambda row: row["valid_from_session"]))
        previous_end: date | None = None
        for row in ordered:
            start = _native_date(row["valid_from_session"], "alias start")
            end = _native_date(row["valid_through_session"], "alias end")
            if previous_end is not None and start <= previous_end:
                raise S7StreamingMaterializationError("ticker alias intervals overlap")
            previous_end = end
        result[ticker] = (
            tuple(_native_date(row["valid_from_session"], "alias start") for row in ordered),
            ordered,
        )
    return result


def _alias_for_row(
    index: Mapping[str, tuple[tuple[date, ...], tuple[Mapping[str, object], ...]]],
    row: Mapping[str, object],
) -> str | None:
    if not row["backtest_identity_eligible"]:
        return None
    ticker = str(row["ticker"])
    session = _native_date(row["session_date"], "universe session")
    values = index.get(ticker)
    if values is None:
        return None
    starts, aliases = values
    offset = bisect.bisect_right(starts, session) - 1
    if offset < 0:
        return None
    alias = aliases[offset]
    if session > alias["valid_through_session"]:
        return None
    if alias["asset_id"] != row["asset_id"] or not _alias_matches_membership(alias, row):
        raise S7StreamingMaterializationError("alias interval identity differs from membership")
    return _digest(alias["ticker_alias_id"], "ticker alias ID")


def _alias_matches_membership(alias: Mapping[str, object], row: Mapping[str, object]) -> bool:
    direct_fields = (
        "ticker",
        "asset_id",
        "observed_composite_figi",
        "observed_composite_market_code",
        "observed_asset_id",
        "observed_share_class_figi",
        "observed_cik_normalized",
        "canonical_composite_figi",
        "canonical_composite_market_code",
        "canonical_share_class_figi",
        "canonical_cik_normalized",
        "identity_disposition",
        "identity_adjudication_id",
        "cross_market_scope_id",
        "cross_market_adjudication_id",
        "cross_market_adjudication_available_session",
        "identity_case_resolution_role",
        "identity_case_id",
        "identity_case_available_session",
        "adjudication_available_session",
        "share_class_id",
        "issuer_id",
        "identity_evidence_available_session",
        "provider_composite_override_id",
        "provider_composite_override_available_session",
        "share_class_adjudication_id",
        "share_class_adjudication_available_session",
        "asset_transition_ids",
        "composite_registry_match_count",
        "composite_registry_collision",
    )
    return all(
        _json_scalar(alias[field]) == _json_scalar(row[field]) for field in direct_fields
    ) and (
        alias["alias_resolution_method"] == row["identity_resolution_method"]
        and alias["alias_resolution_status"] == row["identity_resolution_status"]
    )


def _build_qa(
    *,
    pass1: Mapping[str, object],
    table_counts: Mapping[str, int],
    alias_row_count: int,
    binding: S7StreamingSourceBinding,
) -> dict[str, object]:
    source_rows = _nonnegative(pass1["source_membership_rows"], "source rows")
    source_ids = _nonnegative(pass1["source_record_id_count"], "source ID count")
    share_conflict_eligible_rows = _nonnegative(
        pass1["unadjudicated_gate_b_share_class_conflict_eligible_rows"],
        "unadjudicated Gate-B ShareClass conflict eligible rows",
    )
    critical = int(source_rows != binding.row_count or source_ids != source_rows)
    critical += share_conflict_eligible_rows
    collision_rows = _nonnegative(pass1["raw_collision_rows"], "collision rows")
    return {
        "artifact_type": "s7_streaming_four_table_full_qa",
        "bounded_collision_examples": pass1["bounded_collision_examples"],
        "bounded_share_class_conflict_examples": pass1["bounded_share_class_conflict_examples"],
        "critical_failure_count": critical,
        "gate_b_relation_share_class_conflict_rows": _nonnegative(
            pass1["gate_b_relation_share_class_conflict_rows"],
            "Gate-B relation ShareClass conflict rows",
        ),
        "gate_b_relation_share_class_mismatch_rows": _nonnegative(
            pass1["gate_b_relation_share_class_mismatch_rows"],
            "Gate-B relation ShareClass mismatch rows",
        ),
        "identity_quality_forced_liquidation_rows": 0,
        "inactive_or_delisted_inferred_from_identity_quality_rows": 0,
        "missing_eligible_alias_rows": 0,
        "multi_registry_composite_override_collision_alias_rows": 0,
        "multi_registry_composite_override_collision_eligible_rows": 0,
        "multi_registry_composite_override_collision_resolved_rows": 0,
        "multi_registry_composite_override_collision_rows": collision_rows,
        "publish_authorized": False,
        "reference_inventory_unattempted_rows": 0,
        "session_count": binding.session_count,
        "share_class_correction_before_unique_composite_rows": 0,
        "source_membership_omission_or_duplication_rows": critical,
        "source_membership_rows": source_rows,
        "source_membership_streaming_lineage_digest": _digest(
            pass1["source_lineage_digest"], "source lineage digest"
        ),
        "state": STREAMING_STATE,
        "table_row_counts": dict(table_counts),
        "ticker_alias_rows": _nonnegative(alias_row_count, "ticker alias rows"),
        "transition_automatic_return_stitching_rows": 0,
        "unapproved_canonical_override_rows": 0,
        "unadjudicated_gate_b_share_class_conflict_eligible_rows": (share_conflict_eligible_rows),
        "unadjudicated_gate_b_share_class_conflict_rows": _nonnegative(
            pass1["unadjudicated_gate_b_share_class_conflict_rows"],
            "unadjudicated Gate-B ShareClass conflict rows",
        ),
        "unknown_or_unapproved_foreign_identity_eligible_rows": 0,
        "unresolved_rows": _nonnegative(pass1["unresolved_rows"], "unresolved rows"),
    }


def _source_binding_columns(binding: S7StreamingSourceBinding) -> dict[str, object]:
    registries = {item.registry_name: item for item in binding.registry_pins}
    return {
        "source_s4_release_set_id": binding.s4_release_set_id,
        "source_s5_status_release_id": S7_SOURCE_PINS["ticker_event_request_status"].release_id,
        "source_s5_event_release_id": S7_SOURCE_PINS["ticker_change_event"].release_id,
        "source_s6_overview_release_id": S7_SOURCE_PINS["ticker_overview_safe"].release_id,
        "source_identity_case_candidate_manifest_id": (
            binding.gate_c.identity_case_preview_id
        ),
        "source_identity_case_candidate_manifest_sha256": (
            binding.gate_c.identity_case_preview_manifest.sha256
        ),
        "source_identity_adjudication_release_id": registries["identity_adjudication"].release_id,
        "source_identity_adjudication_release_available_session": registries[
            "identity_adjudication"
        ].release_available_session,
        "source_identity_market_consistency_candidate_manifest_id": (
            binding.gate_c.candidate_id
        ),
        "source_identity_market_consistency_candidate_manifest_sha256": (
            binding.gate_c.candidate_manifest.sha256
        ),
        "source_identity_cross_market_adjudication_release_id": registries[
            "identity_cross_market_adjudication"
        ].release_id,
        "source_identity_cross_market_adjudication_release_available_session": registries[
            "identity_cross_market_adjudication"
        ].release_available_session,
        "source_provider_composite_override_release_id": registries[
            "provider_composite_override"
        ].release_id,
        "source_provider_composite_override_release_available_session": registries[
            "provider_composite_override"
        ].release_available_session,
        "source_share_class_adjudication_release_id": registries[
            "share_class_adjudication"
        ].release_id,
        "source_share_class_adjudication_release_available_session": registries[
            "share_class_adjudication"
        ].release_available_session,
        "source_asset_transition_release_id": registries["asset_transition"].release_id,
        "source_asset_transition_release_available_session": registries[
            "asset_transition"
        ].release_available_session,
    }


def _copy_source_binding_columns(row: Mapping[str, object]) -> dict[str, object]:
    return {
        key: row[key]
        for key in (
            "source_s4_release_set_id",
            "source_s5_status_release_id",
            "source_s5_event_release_id",
            "source_s6_overview_release_id",
            "source_identity_case_candidate_manifest_id",
            "source_identity_case_candidate_manifest_sha256",
            "source_identity_adjudication_release_id",
            "source_identity_adjudication_release_available_session",
            "source_identity_market_consistency_candidate_manifest_id",
            "source_identity_market_consistency_candidate_manifest_sha256",
            "source_identity_cross_market_adjudication_release_id",
            "source_identity_cross_market_adjudication_release_available_session",
            "source_provider_composite_override_release_id",
            "source_provider_composite_override_release_available_session",
            "source_share_class_adjudication_release_id",
            "source_share_class_adjudication_release_available_session",
            "source_asset_transition_release_id",
            "source_asset_transition_release_available_session",
        )
    }


def _contract_table(table_name: str, rows: Sequence[Mapping[str, object]]) -> pa.Table:
    contract = S7_DERIVED_CONTRACTS[table_name]
    try:
        table = pa.Table.from_pylist([dict(row) for row in rows], schema=contract.arrow_schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise S7StreamingMaterializationError(
            f"cannot construct exact {table_name} Arrow table"
        ) from exc
    if table.schema != contract.arrow_schema:
        raise S7StreamingMaterializationError(f"{table_name} schema differs")
    for field_value, column in zip(table.schema, table.columns, strict=True):
        if not field_value.nullable and column.null_count:
            raise S7StreamingMaterializationError(
                f"{table_name}.{field_value.name} contains forbidden nulls"
            )
    if table.num_rows:
        table = table.sort_by([(field_name, "ascending") for field_name in contract.sort_by])
        keys = list(
            zip(
                *(table[field_name].to_pylist() for field_name in contract.primary_key), strict=True
            )
        )
        if len(keys) != len(set(keys)):
            raise S7StreamingMaterializationError(f"{table_name} primary key is duplicated")
    return table


def _write_alias_spill_parquet(spill_path: Path, target: Path) -> int:
    """Stream the ordered SQLite spill into one exact-schema Parquet artifact."""

    if not spill_path.is_file() or spill_path.is_symlink():
        raise S7StreamingMaterializationError("ticker alias spill is missing or unsafe")
    if target.exists() or target.is_symlink():
        raise S7StreamingMaterializationError("Parquet staging target already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    contract = S7_DERIVED_CONTRACTS["ticker_alias"]
    row_count = 0
    previous_key: tuple[object, ...] | None = None
    connection = sqlite3.connect(f"file:{spill_path}?mode=ro", uri=True)
    try:
        cursor = connection.execute(
            "SELECT payload FROM alias_intervals ORDER BY ticker, valid_from_session, asset_id"
        )
        with target.open("xb") as handle:
            writer = pq.ParquetWriter(
                handle,
                contract.arrow_schema,
                compression="zstd",
                compression_level=9,
                use_dictionary=True,
                write_statistics=True,
            )
            try:
                while True:
                    records = cursor.fetchmany(10_000)
                    if not records:
                        break
                    rows = [_alias_payload_row(record[0]) for record in records]
                    table = _contract_table("ticker_alias", rows)
                    for row in table.select(list(contract.sort_by)).to_pylist():
                        key = tuple(row[field_name] for field_name in contract.sort_by)
                        if previous_key is not None and key <= previous_key:
                            raise S7StreamingMaterializationError(
                                "ticker alias spill ordering/primary key differs"
                            )
                        previous_key = key
                    writer.write_table(table)
                    row_count += table.num_rows
                if row_count == 0:
                    writer.write_table(pa.Table.from_pylist([], schema=contract.arrow_schema))
            finally:
                writer.close()
            handle.flush()
            os.fsync(handle.fileno())
    except (OSError, sqlite3.Error, pa.ArrowException) as exc:
        raise S7StreamingMaterializationError(
            "cannot stream ticker alias spill to Parquet"
        ) from exc
    finally:
        connection.close()
    return row_count


def _alias_payload_row(raw: object) -> dict[str, object]:
    if not isinstance(raw, (bytes, str)):
        raise S7StreamingMaterializationError("ticker alias spill payload type differs")
    content = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    item = _mapping(_load_canonical_json(content.encode(), "alias spill row"), "alias row")
    row = dict(item)
    for field_name in (
        "valid_from_session",
        "valid_through_session",
        "valid_to_session_exclusive",
        "cross_market_adjudication_available_session",
        "identity_case_available_session",
        "adjudication_available_session",
        "identity_resolution_cutoff_session",
        "identity_evidence_available_session",
        "source_identity_adjudication_release_available_session",
        "source_identity_cross_market_adjudication_release_available_session",
        "source_provider_composite_override_release_available_session",
        "source_share_class_adjudication_release_available_session",
        "source_asset_transition_release_available_session",
        "provider_composite_override_available_session",
        "share_class_adjudication_available_session",
    ):
        value = row.get(field_name)
        if isinstance(value, str):
            row[field_name] = date.fromisoformat(value)
    return row


def _write_parquet_exclusive(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise S7StreamingMaterializationError("Parquet staging target already exists")
    try:
        with path.open("xb") as handle:
            pq.write_table(
                table,
                handle,
                compression="zstd",
                compression_level=9,
                use_dictionary=True,
                write_statistics=True,
            )
            handle.flush()
            os.fsync(handle.fileno())
    except (OSError, pa.ArrowException) as exc:
        raise S7StreamingMaterializationError("cannot write staged Parquet") from exc


def _write_exclusive(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise S7StreamingMaterializationError("staged artifact already exists")
    try:
        with path.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise S7StreamingMaterializationError("cannot write staged artifact") from exc


def _path_receipt(
    path: Path,
    relative: str,
    *,
    row_count: int | None = None,
    schema_digest: str | None = None,
) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        raise S7StreamingMaterializationError("artifact receipt source is missing or unsafe")
    receipt: dict[str, object] = {
        "bytes": path.stat().st_size,
        "path": _relative(relative, "artifact receipt path"),
        "sha256": sha256_file(path),
    }
    if row_count is not None:
        receipt["row_count"] = _nonnegative(row_count, "artifact row count")
    if schema_digest is not None:
        receipt["schema_digest"] = _digest(schema_digest, "artifact schema digest")
    return receipt


def _load_source_binding(
    root: Path, source_binding_id: str
) -> tuple[S7StreamingSourceBinding, ExactFilePin]:
    identifier = _digest(source_binding_id, "source binding ID")
    relative = (
        "manifests/silver/identity/s7-streaming-full-source-bindings/"
        f"source_binding_id={identifier}/manifest.json"
    )
    content = _read_exact_file(root, relative, label="streaming source binding")
    binding = S7StreamingSourceBinding.from_dict(
        _load_canonical_json(content, "streaming source binding")
    )
    if binding.source_binding_id != identifier or binding.relative_path != relative:
        raise S7StreamingMaterializationError("source binding canonical identity differs")
    return binding, ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _profile_sample_artifacts(
    artifacts: Sequence[SessionArtifactPin], cap: int
) -> tuple[SessionArtifactPin, ...]:
    """Choose frozen calendar strata plus the largest session, without content reads."""

    items = tuple(artifacts)
    if not items:
        raise S7StreamingMaterializationError("profile source artifact set is empty")
    limit = min(_positive(cap, "profile sample cap"), len(items))
    largest_index = max(range(len(items)), key=lambda index: (items[index].row_count, -index))
    selected: set[int] = {largest_index}
    strata = limit - 1
    if strata == 1:
        candidates = (0,)
    elif strata > 1:
        denominator = strata - 1
        candidates = tuple(
            (index * (len(items) - 1) + denominator // 2) // denominator for index in range(strata)
        )
    else:
        candidates = ()
    for index in (*candidates, *range(len(items))):
        if len(selected) == limit:
            break
        selected.add(index)
    sample = tuple(items[index] for index in sorted(selected))
    if len(sample) != limit:
        raise S7StreamingMaterializationError("profile sample selection is incomplete")
    return sample


def _profile_plan_path(plan_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-bounded-size-profile-plans/"
        f"plan_id={_digest(plan_id, 'profile plan ID')}/manifest.json"
    )


def _profile_approval_path(approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-bounded-size-profile-approvals/"
        f"approval_id={_digest(approval_id, 'profile approval ID')}/manifest.json"
    )


def _profile_intent_path(plan_id: str, approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-bounded-size-profile-run-intents/"
        f"plan_id={_digest(plan_id, 'profile plan ID')}/"
        f"approval_id={_digest(approval_id, 'profile approval ID')}/manifest.json"
    )


def _profile_candidate_path(candidate_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-bounded-size-profile-candidates/"
        f"candidate_id={_digest(candidate_id, 'profile candidate ID')}/manifest.json"
    )


def _profile_completion_path(plan_id: str, approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-bounded-size-profile-completions/"
        f"plan_id={_digest(plan_id, 'profile plan ID')}/"
        f"approval_id={_digest(approval_id, 'profile approval ID')}/manifest.json"
    )


def _validate_binding_against_caps(
    binding: S7StreamingSourceBinding, caps: StreamingResourceCaps
) -> None:
    if binding.declared_source_bytes > caps.source_bytes_cap:
        raise S7StreamingMaterializationError("declared sources exceed the Full source cap")
    if binding.session_count > caps.session_count_cap:
        raise S7StreamingMaterializationError("declared sessions exceed the Full session cap")
    if binding.row_count > caps.row_count_cap:
        raise S7StreamingMaterializationError("declared rows exceed the Full row cap")
    if max(item.row_count for item in binding.membership_artifacts) > caps.per_session_row_cap:
        raise S7StreamingMaterializationError("declared session rows exceed the Full cap")


def _load_profile_plan(
    root: Path, plan_id: str
) -> tuple[dict[str, object], ExactFilePin, S7StreamingSourceBinding]:
    identifier = _digest(plan_id, "profile plan ID")
    relative = _profile_plan_path(identifier)
    content = _read_exact_file(root, relative, label="bounded profile plan")
    document = _mapping(
        _load_canonical_json(content, "bounded profile plan"), "bounded profile plan"
    )
    _expect_keys(
        document,
        {
            "artifact_type",
            "authorized_action",
            "capabilities",
            "full_resource_caps",
            "plan_id",
            "policy_version",
            "prepared_at_utc",
            "prepared_by",
            "runtime_binding",
            "sample_artifacts",
            "sample_session_cap",
            "source_binding",
            "source_binding_id",
        },
        "bounded profile plan",
    )
    slot = dict(document)
    claimed = slot.pop("plan_id")
    slot.pop("prepared_at_utc")
    slot.pop("prepared_by")
    slot["artifact_type"] = "s7_streaming_size_profile_plan_slot"
    if claimed != identifier or stable_digest(slot) != identifier:
        raise S7StreamingMaterializationError("bounded profile plan ID differs")
    if (
        document["artifact_type"] != "s7_streaming_bounded_size_profile_plan"
        or document["authorized_action"] != PROFILE_AUTHORIZED_ACTION
        or document["capabilities"] != dict(_FALSE_CAPABILITIES)
        or document["policy_version"] != PROFILE_POLICY_VERSION
    ):
        raise S7StreamingMaterializationError("bounded profile plan semantics differ")
    _utc_from_text(document["prepared_at_utc"], "profile plan time")
    _text(document["prepared_by"], "profile plan preparer")
    cap = _positive(document["sample_session_cap"], "profile sample session cap")
    if cap > PROFILE_SAMPLE_SESSION_HARD_CAP:
        raise S7StreamingMaterializationError("profile sample exceeds the frozen hard cap")
    caps = StreamingResourceCaps.from_dict(document["full_resource_caps"])
    binding_id = _digest(document["source_binding_id"], "profile source binding ID")
    binding, binding_receipt = _load_source_binding(root, binding_id)
    if ExactFilePin.from_dict(document["source_binding"]) != binding_receipt:
        raise S7StreamingMaterializationError("profile source-binding receipt differs")
    if document["runtime_binding"] != dict(binding.runtime_binding):
        raise S7StreamingMaterializationError("profile runtime binding differs")
    sample = tuple(
        SessionArtifactPin.from_dict(value)
        for value in _array(document["sample_artifacts"], "profile sample artifacts")
    )
    if sample != _profile_sample_artifacts(binding.membership_artifacts, cap):
        raise S7StreamingMaterializationError("profile sample selection differs")
    _validate_binding_against_caps(binding, caps)
    return (
        document,
        ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content)),
        binding,
    )


_PROFILE_APPROVAL_SLOT_KEYS: Final = frozenset(
    {
        "artifact_type",
        "authorized_action",
        "capabilities",
        "plan",
        "plan_id",
        "policy_version",
        "source_binding_id",
        "standing_authorization_sha256",
        "standing_reaffirmation_sha256",
    }
)


def _load_profile_approval(root: Path, approval_id: str) -> tuple[dict[str, object], ExactFilePin]:
    identifier = _digest(approval_id, "profile approval ID")
    relative = _profile_approval_path(identifier)
    content = _read_exact_file(root, relative, label="bounded profile approval")
    document = _mapping(
        _load_canonical_json(content, "bounded profile approval"),
        "bounded profile approval",
    )
    _expect_keys(
        document,
        {
            *_PROFILE_APPROVAL_SLOT_KEYS,
            "approval_availability",
            "approval_id",
            "approved_at_utc",
            "approved_by",
            "standing_authorization",
            "standing_reaffirmation",
        },
        "bounded profile approval",
    )
    slot = {key: document[key] for key in _PROFILE_APPROVAL_SLOT_KEYS}
    if stable_digest(slot) != identifier or document["approval_id"] != identifier:
        raise S7StreamingMaterializationError("bounded profile approval ID differs")
    return document, ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _verify_profile_approval(
    document: Mapping[str, object],
    expected_slot: Mapping[str, object],
    *,
    root: Path,
    binding: S7StreamingSourceBinding,
) -> None:
    item = dict(document)
    if {key: item[key] for key in _PROFILE_APPROVAL_SLOT_KEYS} != dict(expected_slot):
        raise S7StreamingMaterializationError("bounded profile approval binding differs")
    approved_at = _utc_from_text(item["approved_at_utc"], "profile approval time")
    if (
        item["approval_id"] != stable_digest(dict(expected_slot))
        or item["approval_availability"] != _approval_availability(root, binding, approved_at)
        or item["standing_authorization"]
        != {
            "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
            "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        }
        or item["standing_reaffirmation"]
        != {
            "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
            "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        }
    ):
        raise S7StreamingMaterializationError("bounded profile approval replay differs")
    _text(item["approved_by"], "profile approver")


def _verify_profile_sources(
    root: Path,
    binding: S7StreamingSourceBinding,
    sample: Sequence[SessionArtifactPin],
) -> None:
    pins = [
        binding.s4_release_set_manifest,
        *(item.artifact for item in sample),
        binding.gate_b.manifest,
        binding.gate_b.data,
        binding.gate_c.candidate_manifest,
        binding.gate_c.completion_manifest,
        binding.gate_c.qa,
        *(
            ExactFilePin(item.manifest_path, item.manifest_sha256, item.manifest_bytes)
            for item in binding.registry_pins
        ),
        *binding.contract_approvals,
    ]
    for pin in pins:
        _verify_file_pin(root, pin)


def _store_or_load_profile_intent(
    root: Path,
    *,
    plan: Mapping[str, object],
    plan_receipt: ExactFilePin,
    approval: Mapping[str, object],
    approval_receipt: ExactFilePin,
    binding: S7StreamingSourceBinding,
    candidate_id: str,
    now: Callable[[], datetime],
) -> ExactFilePin:
    relative = _profile_intent_path(plan["plan_id"], approval["approval_id"])
    static = {
        "approval": approval_receipt.to_dict(),
        "approval_id": approval["approval_id"],
        "artifact_type": "s7_streaming_bounded_size_profile_run_intent",
        "candidate_id": candidate_id,
        "plan": plan_receipt.to_dict(),
        "plan_id": plan["plan_id"],
        "policy_version": PROFILE_POLICY_VERSION,
        "source_binding_id": binding.source_binding_id,
    }
    target = safe_relative_path(root, relative)
    if target.exists():
        content = _read_exact_file(root, relative, label="bounded profile intent")
        item = _mapping(
            _load_canonical_json(content, "bounded profile intent"),
            "bounded profile intent",
        )
        _expect_keys(item, {*static, "captured_at_utc", "intent_id"}, "profile intent")
        payload = dict(item)
        claimed = payload.pop("intent_id")
        captured = _utc_from_text(item["captured_at_utc"], "profile intent time")
        approved_at = _utc_from_text(approval["approved_at_utc"], "profile approval time")
        if (
            {key: item[key] for key in static} != static
            or claimed != stable_digest(payload)
            or captured < approved_at
        ):
            raise S7StreamingMaterializationError("bounded profile intent replay differs")
        return ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))
    captured = _utc(now(), "profile intent time")
    approved_at = _utc_from_text(approval["approved_at_utc"], "profile approval time")
    if captured < approved_at:
        raise S7StreamingMaterializationError("profile intent predates approval")
    payload = {**static, "captured_at_utc": _utc_text(captured)}
    document = {**payload, "intent_id": stable_digest(payload)}
    return _write_immutable(
        root, relative, _canonical_bytes(document), "bounded profile run intent"
    )


def _project_bytes(sample_bytes: int, sample_rows: int, full_rows: int) -> int:
    numerator = _nonnegative(sample_bytes, "sample bytes") * _positive(full_rows, "full row count")
    denominator = _positive(sample_rows, "sample row count")
    return (numerator + denominator - 1) // denominator


def _per_million(sample_bytes: int, sample_rows: int) -> int:
    numerator = _nonnegative(sample_bytes, "sample bytes") * 1_000_000
    denominator = _positive(sample_rows, "sample row count")
    return (numerator + denominator - 1) // denominator


def _ratio_ppm(numerator_value: int, denominator_value: int) -> int:
    numerator = _nonnegative(numerator_value, "ratio numerator") * 1_000_000
    denominator = _positive(denominator_value, "ratio denominator")
    return (numerator + denominator - 1) // denominator


_PROFILE_METRIC_KEYS: Final = frozenset(
    {
        "critical_failure_count",
        "expected_remaining_free_bytes_at_peak",
        "full_row_count",
        "observed_disk_free_bytes_during_profile",
        "output_bytes_per_million_rows",
        "peak_staging_bytes_per_million_rows",
        "projected_full_output_bytes",
        "projected_full_peak_staging_bytes",
        "sample_output_bytes",
        "sample_output_to_source_ratio_ppm",
        "sample_peak_staging_bytes",
        "sample_peak_staging_to_output_ratio_ppm",
        "sample_row_count",
        "sample_session_count",
        "sample_source_compressed_bytes",
        "source_compressed_bytes_per_million_rows",
    }
)


def _validated_profile_metrics(
    value: object,
    *,
    plan: Mapping[str, object],
    binding: S7StreamingSourceBinding,
) -> dict[str, int]:
    raw = _mapping(value, "bounded profile metrics")
    _expect_keys(raw, set(_PROFILE_METRIC_KEYS), "bounded profile metrics")
    metrics = {key: _nonnegative(raw[key], f"profile metric {key}") for key in raw}
    sample = tuple(
        SessionArtifactPin.from_dict(item)
        for item in _array(plan["sample_artifacts"], "profile sample artifacts")
    )
    sample_rows = sum(item.row_count for item in sample)
    source_bytes = sum(item.artifact.bytes for item in sample)
    output = metrics["sample_output_bytes"]
    peak = metrics["sample_peak_staging_bytes"]
    observed_free = metrics["observed_disk_free_bytes_during_profile"]
    caps = StreamingResourceCaps.from_dict(plan["full_resource_caps"])
    expected = {
        "critical_failure_count": 0,
        "expected_remaining_free_bytes_at_peak": (
            observed_free + peak - _project_bytes(peak, sample_rows, binding.row_count)
        ),
        "full_row_count": binding.row_count,
        "observed_disk_free_bytes_during_profile": observed_free,
        "output_bytes_per_million_rows": _per_million(output, sample_rows),
        "peak_staging_bytes_per_million_rows": _per_million(peak, sample_rows),
        "projected_full_output_bytes": _project_bytes(output, sample_rows, binding.row_count),
        "projected_full_peak_staging_bytes": _project_bytes(peak, sample_rows, binding.row_count),
        "sample_output_bytes": output,
        "sample_output_to_source_ratio_ppm": _ratio_ppm(output, source_bytes),
        "sample_peak_staging_bytes": peak,
        "sample_peak_staging_to_output_ratio_ppm": _ratio_ppm(peak, output),
        "sample_row_count": sample_rows,
        "sample_session_count": len(sample),
        "sample_source_compressed_bytes": source_bytes,
        "source_compressed_bytes_per_million_rows": _per_million(source_bytes, sample_rows),
    }
    if metrics != expected:
        raise S7StreamingMaterializationError("bounded profile metrics replay differs")
    if (
        metrics["projected_full_output_bytes"] > caps.output_bytes_cap
        or metrics["projected_full_peak_staging_bytes"] > caps.tmp_bytes_cap
        or metrics["expected_remaining_free_bytes_at_peak"] < caps.disk_free_floor_bytes
    ):
        raise S7StreamingMaterializationError("bounded profile no longer passes Full caps")
    return metrics


def _load_and_verify_profile_candidate(
    root: Path,
    *,
    candidate_id: str,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    binding: S7StreamingSourceBinding,
) -> tuple[dict[str, object], ExactFilePin]:
    identifier = _digest(candidate_id, "profile candidate ID")
    relative = _profile_candidate_path(identifier)
    content = _read_exact_file(root, relative, label="bounded profile candidate")
    receipt = ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))
    candidate = _mapping(
        _load_canonical_json(content, "bounded profile candidate"),
        "bounded profile candidate",
    )
    _expect_keys(
        candidate,
        {
            "approval_id",
            "artifact_type",
            "candidate_id",
            "capabilities",
            "intent",
            "manifest_id",
            "metrics",
            "plan_id",
            "policy_version",
            "source_binding_id",
            "state",
        },
        "bounded profile candidate",
    )
    metrics = _validated_profile_metrics(candidate["metrics"], plan=plan, binding=binding)
    payload = dict(candidate)
    manifest_id = payload.pop("manifest_id")
    if (
        manifest_id != stable_digest(payload)
        or candidate["candidate_id"] != identifier
        or candidate["approval_id"] != approval["approval_id"]
        or candidate["plan_id"] != plan["plan_id"]
        or candidate["source_binding_id"] != binding.source_binding_id
        or candidate["artifact_type"] != "s7_streaming_bounded_size_profile_candidate"
        or candidate["capabilities"] != dict(_FALSE_CAPABILITIES)
        or candidate["policy_version"] != PROFILE_POLICY_VERSION
        or candidate["state"] != STREAMING_STATE
        or candidate["metrics"] != metrics
    ):
        raise S7StreamingMaterializationError("bounded profile candidate differs")
    intent_pin = ExactFilePin.from_dict(candidate["intent"])
    if intent_pin.path != _profile_intent_path(plan["plan_id"], approval["approval_id"]):
        raise S7StreamingMaterializationError("bounded profile intent path differs")
    _verify_file_pin(root, intent_pin)
    intent = _mapping(
        _load_canonical_json(
            _read_exact_file(root, intent_pin.path, label="bounded profile intent"),
            "bounded profile intent",
        ),
        "bounded profile intent",
    )
    _expect_keys(
        intent,
        {
            "approval",
            "approval_id",
            "artifact_type",
            "candidate_id",
            "captured_at_utc",
            "intent_id",
            "plan",
            "plan_id",
            "policy_version",
            "source_binding_id",
        },
        "bounded profile intent",
    )
    intent_payload = dict(intent)
    intent_id = intent_payload.pop("intent_id")
    _, plan_receipt, _ = _load_profile_plan(root, plan["plan_id"])
    loaded_approval, approval_receipt = _load_profile_approval(root, approval["approval_id"])
    approved_at = _utc_from_text(approval["approved_at_utc"], "profile approval time")
    captured = _utc_from_text(intent["captured_at_utc"], "profile intent time")
    if (
        loaded_approval != dict(approval)
        or intent_id != stable_digest(intent_payload)
        or intent["artifact_type"] != "s7_streaming_bounded_size_profile_run_intent"
        or intent["approval"] != approval_receipt.to_dict()
        or intent["approval_id"] != approval["approval_id"]
        or intent["candidate_id"] != identifier
        or intent["plan"] != plan_receipt.to_dict()
        or intent["plan_id"] != plan["plan_id"]
        or intent["policy_version"] != PROFILE_POLICY_VERSION
        or intent["source_binding_id"] != binding.source_binding_id
        or captured < approved_at
    ):
        raise S7StreamingMaterializationError("bounded profile intent differs")
    return dict(candidate), receipt


def _store_profile_completion(
    root: Path,
    *,
    completion_relative: str,
    candidate: Mapping[str, object],
    candidate_receipt: ExactFilePin,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    binding: S7StreamingSourceBinding,
    now: Callable[[], datetime],
    started: float,
    caps: StreamingResourceCaps,
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    idempotent: bool,
) -> dict[str, object]:
    _check_resource_caps(
        root,
        staging=None,
        started=started,
        caps=caps,
        monotonic=monotonic,
        rss_probe=rss_probe,
        disk_free_probe=disk_free_probe,
    )
    completed_at = _utc(now(), "profile completion time")
    approved_at = _utc_from_text(approval["approved_at_utc"], "profile approval time")
    if completed_at < approved_at:
        raise S7StreamingMaterializationError("profile completion predates approval")
    metrics = _validated_profile_metrics(candidate["metrics"], plan=plan, binding=binding)
    candidate_id = _digest(candidate["candidate_id"], "profile candidate ID")
    payload = {
        "approval_id": approval["approval_id"],
        "artifact_type": "s7_streaming_bounded_size_profile_completion",
        "candidate": candidate_receipt.to_dict(),
        "candidate_id": candidate_id,
        "complete": True,
        "completed_at_utc": _utc_text(completed_at),
        "completion_state": STREAMING_STATE,
        "metrics": metrics,
        "plan_id": plan["plan_id"],
        "policy_version": PROFILE_POLICY_VERSION,
        "source_binding_id": binding.source_binding_id,
    }
    document = {**payload, "completion_id": stable_digest(payload)}
    _write_immutable(
        root,
        completion_relative,
        _canonical_bytes(document),
        "bounded profile completion",
    )
    return _load_and_verify_profile_completion(
        root,
        completion_relative,
        plan=plan,
        approval=approval,
        binding=binding,
        idempotent=idempotent,
    )


def _load_and_verify_profile_completion(
    root: Path,
    completion_relative: str,
    *,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    binding: S7StreamingSourceBinding,
    idempotent: bool,
) -> dict[str, object]:
    del idempotent  # replay semantics are encoded by immutable bytes, not response fields
    expected_relative = _profile_completion_path(
        _digest(plan["plan_id"], "profile plan ID"),
        _digest(approval["approval_id"], "profile approval ID"),
    )
    if _relative(completion_relative, "profile completion path") != expected_relative:
        raise S7StreamingMaterializationError("bounded profile completion path differs")
    content = _read_exact_file(root, expected_relative, label="bounded profile completion")
    completion = _mapping(
        _load_canonical_json(content, "bounded profile completion"),
        "bounded profile completion",
    )
    _expect_keys(
        completion,
        {
            "approval_id",
            "artifact_type",
            "candidate",
            "candidate_id",
            "complete",
            "completed_at_utc",
            "completion_id",
            "completion_state",
            "metrics",
            "plan_id",
            "policy_version",
            "source_binding_id",
        },
        "bounded profile completion",
    )
    completion_payload = dict(completion)
    completion_id = completion_payload.pop("completion_id")
    if (
        completion_id != stable_digest(completion_payload)
        or completion["artifact_type"] != "s7_streaming_bounded_size_profile_completion"
        or completion["approval_id"] != approval["approval_id"]
        or completion["plan_id"] != plan["plan_id"]
        or completion["source_binding_id"] != binding.source_binding_id
        or completion["policy_version"] != PROFILE_POLICY_VERSION
        or completion["complete"] is not True
        or completion["completion_state"] != STREAMING_STATE
    ):
        raise S7StreamingMaterializationError("bounded profile completion differs")
    completed_at = _utc_from_text(completion["completed_at_utc"], "profile completion time")
    approved_at = _utc_from_text(approval["approved_at_utc"], "profile approval time")
    if completed_at < approved_at:
        raise S7StreamingMaterializationError("profile completion predates approval")
    metrics = _validated_profile_metrics(completion["metrics"], plan=plan, binding=binding)
    candidate_id = _digest(completion["candidate_id"], "profile candidate ID")
    candidate_pin = ExactFilePin.from_dict(completion["candidate"])
    if candidate_pin.path != _profile_candidate_path(candidate_id):
        raise S7StreamingMaterializationError("bounded profile candidate path differs")
    _verify_file_pin(root, candidate_pin)
    candidate_content = _read_exact_file(
        root, candidate_pin.path, label="bounded profile candidate"
    )
    candidate = _mapping(
        _load_canonical_json(candidate_content, "bounded profile candidate"),
        "bounded profile candidate",
    )
    _expect_keys(
        candidate,
        {
            "approval_id",
            "artifact_type",
            "candidate_id",
            "capabilities",
            "intent",
            "manifest_id",
            "metrics",
            "plan_id",
            "policy_version",
            "source_binding_id",
            "state",
        },
        "bounded profile candidate",
    )
    candidate_payload = dict(candidate)
    manifest_id = candidate_payload.pop("manifest_id")
    if (
        manifest_id != stable_digest(candidate_payload)
        or candidate["candidate_id"] != candidate_id
        or candidate["approval_id"] != approval["approval_id"]
        or candidate["plan_id"] != plan["plan_id"]
        or candidate["source_binding_id"] != binding.source_binding_id
        or candidate["artifact_type"] != "s7_streaming_bounded_size_profile_candidate"
        or candidate["capabilities"] != dict(_FALSE_CAPABILITIES)
        or candidate["policy_version"] != PROFILE_POLICY_VERSION
        or candidate["state"] != STREAMING_STATE
        or candidate["metrics"] != metrics
    ):
        raise S7StreamingMaterializationError("bounded profile candidate differs")
    intent_pin = ExactFilePin.from_dict(candidate["intent"])
    if intent_pin.path != _profile_intent_path(plan["plan_id"], approval["approval_id"]):
        raise S7StreamingMaterializationError("bounded profile intent path differs")
    _verify_file_pin(root, intent_pin)
    intent = _mapping(
        _load_canonical_json(
            _read_exact_file(root, intent_pin.path, label="bounded profile intent"),
            "bounded profile intent",
        ),
        "bounded profile intent",
    )
    _expect_keys(
        intent,
        {
            "approval",
            "approval_id",
            "artifact_type",
            "candidate_id",
            "captured_at_utc",
            "intent_id",
            "plan",
            "plan_id",
            "policy_version",
            "source_binding_id",
        },
        "bounded profile intent",
    )
    intent_payload = dict(intent)
    intent_id = intent_payload.pop("intent_id")
    _, plan_receipt, _ = _load_profile_plan(root, plan["plan_id"])
    loaded_approval, approval_receipt = _load_profile_approval(root, approval["approval_id"])
    if loaded_approval != dict(approval):
        raise S7StreamingMaterializationError("bounded profile approval changed")
    captured = _utc_from_text(intent["captured_at_utc"], "profile intent time")
    if (
        intent_id != stable_digest(intent_payload)
        or intent["artifact_type"] != "s7_streaming_bounded_size_profile_run_intent"
        or intent["approval"] != approval_receipt.to_dict()
        or intent["approval_id"] != approval["approval_id"]
        or intent["candidate_id"] != candidate_id
        or intent["plan"] != plan_receipt.to_dict()
        or intent["plan_id"] != plan["plan_id"]
        or intent["policy_version"] != PROFILE_POLICY_VERSION
        or intent["source_binding_id"] != binding.source_binding_id
        or captured < approved_at
    ):
        raise S7StreamingMaterializationError("bounded profile intent differs")
    return completion


def _profile_evidence_for_full_plan(
    root: Path,
    *,
    binding: S7StreamingSourceBinding,
    caps: StreamingResourceCaps,
    profile_plan_id: str | None,
    profile_approval_id: str | None,
) -> dict[str, object] | None:
    if profile_plan_id is None and profile_approval_id is None:
        if binding.mode == "production":
            raise S7StreamingMaterializationError(
                "production Full requires a completed bounded size/profile preview"
            )
        return None
    if profile_plan_id is None or profile_approval_id is None:
        raise S7StreamingMaterializationError(
            "bounded profile plan and approval IDs must be supplied together"
        )
    profile_plan, _, profile_binding = _load_profile_plan(root, profile_plan_id)
    if profile_binding.source_binding_id != binding.source_binding_id:
        raise S7StreamingMaterializationError("bounded profile source binding differs")
    if profile_plan["full_resource_caps"] != caps.to_dict():
        raise S7StreamingMaterializationError("bounded profile Full caps differ")
    approval, _ = _load_profile_approval(root, profile_approval_id)
    expected_slot = {
        "artifact_type": "s7_streaming_bounded_size_profile_standing_approval",
        "authorized_action": PROFILE_AUTHORIZED_ACTION,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "plan": _load_profile_plan(root, profile_plan_id)[1].to_dict(),
        "plan_id": profile_plan_id,
        "policy_version": PROFILE_POLICY_VERSION,
        "source_binding_id": binding.source_binding_id,
        "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
    }
    _verify_profile_approval(approval, expected_slot, root=root, binding=binding)
    relative = _profile_completion_path(profile_plan_id, profile_approval_id)
    completion = _load_and_verify_profile_completion(
        root,
        relative,
        plan=profile_plan,
        approval=approval,
        binding=binding,
        idempotent=True,
    )
    content = _read_exact_file(root, relative, label="bounded profile completion")
    return {
        "approval_id": profile_approval_id,
        "completed_at_utc": completion["completed_at_utc"],
        "completion": ExactFilePin(
            relative, hashlib.sha256(content).hexdigest(), len(content)
        ).to_dict(),
        "completion_id": completion["completion_id"],
        "metrics_digest": stable_digest(completion["metrics"]),
        "plan_id": profile_plan_id,
    }


def _load_plan(
    root: Path, plan_id: str
) -> tuple[dict[str, object], ExactFilePin, S7StreamingSourceBinding]:
    identifier = _digest(plan_id, "Full plan ID")
    relative = _plan_path(identifier)
    content = _read_exact_file(root, relative, label="streaming Full plan")
    document = _mapping(_load_canonical_json(content, "streaming Full plan"), "Full plan")
    _expect_keys(
        document,
        {
            "artifact_type",
            "bounded_profile_evidence",
            "candidate_state",
            "capabilities",
            "contract_pins",
            "plan_id",
            "plan_version",
            "policy_version",
            "prepared_at_utc",
            "prepared_by",
            "resource_caps",
            "runtime_binding",
            "source_binding",
            "source_binding_id",
        },
        "streaming Full plan",
    )
    slot_payload = dict(document)
    claimed = slot_payload.pop("plan_id")
    slot_payload.pop("prepared_at_utc")
    slot_payload.pop("prepared_by")
    slot_payload["artifact_type"] = "s7_streaming_four_table_full_plan_slot"
    if claimed != identifier or stable_digest(slot_payload) != identifier:
        raise S7StreamingMaterializationError("Full plan ID recomputation differs")
    if (
        document["artifact_type"] != "s7_streaming_four_table_full_plan"
        or document["candidate_state"] != STREAMING_STATE
        or document["capabilities"] != dict(_FALSE_CAPABILITIES)
        or document["contract_pins"] != _contract_pins()
        or document["plan_version"] != STREAMING_PLAN_VERSION
        or document["policy_version"] != STREAMING_POLICY_VERSION
    ):
        raise S7StreamingMaterializationError("Full plan semantics differ")
    _utc_from_text(document["prepared_at_utc"], "plan preparation time")
    _text(document["prepared_by"], "plan preparer")
    caps = StreamingResourceCaps.from_dict(document["resource_caps"])
    binding_id = _digest(document["source_binding_id"], "source binding ID")
    binding, binding_receipt = _load_source_binding(root, binding_id)
    if ExactFilePin.from_dict(document["source_binding"]) != binding_receipt:
        raise S7StreamingMaterializationError("Full plan source binding receipt differs")
    if document["runtime_binding"] != dict(binding.runtime_binding):
        raise S7StreamingMaterializationError("Full plan runtime binding differs")
    raw_profile = document["bounded_profile_evidence"]
    if raw_profile is None:
        expected_profile = _profile_evidence_for_full_plan(
            root,
            binding=binding,
            caps=caps,
            profile_plan_id=None,
            profile_approval_id=None,
        )
    else:
        declared_profile = _mapping(raw_profile, "Full plan bounded profile evidence")
        _expect_keys(
            declared_profile,
            {
                "approval_id",
                "completed_at_utc",
                "completion",
                "completion_id",
                "metrics_digest",
                "plan_id",
            },
            "Full plan bounded profile evidence",
        )
        expected_profile = _profile_evidence_for_full_plan(
            root,
            binding=binding,
            caps=caps,
            profile_plan_id=_digest(declared_profile["plan_id"], "profile plan ID"),
            profile_approval_id=_digest(declared_profile["approval_id"], "profile approval ID"),
        )
        if declared_profile != expected_profile:
            raise S7StreamingMaterializationError("Full plan bounded profile evidence differs")
        if _utc_from_text(document["prepared_at_utc"], "plan preparation time") < (
            _utc_from_text(declared_profile["completed_at_utc"], "profile completion time")
        ):
            raise S7StreamingMaterializationError("Full plan predates bounded profile completion")
    if raw_profile is None and expected_profile is not None:
        raise S7StreamingMaterializationError("Full plan bounded profile evidence differs")
    return (
        document,
        ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content)),
        binding,
    )


def _load_request(root: Path, request_id: str) -> tuple[StreamingApprovalRequest, ExactFilePin]:
    identifier = _digest(request_id, "approval request ID")
    relative = (
        "manifests/silver/identity/s7-streaming-full-approval-requests/"
        f"request_id={identifier}/manifest.json"
    )
    content = _read_exact_file(root, relative, label="approval request")
    item = _mapping(_load_canonical_json(content, "approval request"), "approval request")
    _expect_keys(
        item,
        {
            "artifact_type",
            "authorized_action",
            "false_capabilities",
            "literal_version",
            "plan",
            "plan_id",
            "request_id",
            "request_version",
            "requested_at_utc",
            "requested_by",
            "resource_caps_digest",
            "runtime_file_set_digest",
            "source_binding_id",
        },
        "approval request",
    )
    request = StreamingApprovalRequest(
        plan_id=_digest(item["plan_id"], "request plan ID"),
        plan=ExactFilePin.from_dict(item["plan"]),
        source_binding_id=_digest(item["source_binding_id"], "request source binding ID"),
        resource_caps_digest=_digest(item["resource_caps_digest"], "request resource caps digest"),
        runtime_file_set_digest=_digest(
            item["runtime_file_set_digest"], "request runtime file-set digest"
        ),
        requested_at_utc=_utc_from_text(item["requested_at_utc"], "request time"),
        requested_by=_text(item["requested_by"], "requester"),
    )
    if (
        item != request.to_dict()
        or request.request_id != identifier
        or item["artifact_type"] != "s7_streaming_four_table_full_approval_request"
        or item["authorized_action"] != STREAMING_AUTHORIZED_ACTION
        or item["false_capabilities"] != dict(_FALSE_CAPABILITIES)
        or item["literal_version"] != STREAMING_APPROVAL_LITERAL_VERSION
        or item["request_version"] != STREAMING_REQUEST_VERSION
    ):
        raise S7StreamingMaterializationError("approval request replay differs")
    return request, ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _load_approval(root: Path, approval_id: str) -> tuple[StreamingExactApproval, ExactFilePin]:
    identifier = _digest(approval_id, "approval ID")
    relative = (
        "manifests/silver/identity/s7-streaming-full-approvals/"
        f"approval_id={identifier}/manifest.json"
    )
    content = _read_exact_file(root, relative, label="exact approval")
    item = _mapping(_load_canonical_json(content, "exact approval"), "exact approval")
    _expect_keys(
        item,
        {
            "approval_availability",
            "approval_id",
            "approval_version",
            "approved_at_utc",
            "approved_by",
            "artifact_type",
            "authorization_mode",
            "authorized_action",
            "exact_literal_sha256",
            "false_capabilities",
            "literal_version",
            "plan_id",
            "request",
            "request_id",
            "standing_authorization",
            "standing_reaffirmation",
        },
        "exact approval",
    )
    approval = StreamingExactApproval(
        request_id=_digest(item["request_id"], "approval request ID"),
        request=ExactFilePin.from_dict(item["request"]),
        plan_id=_digest(item["plan_id"], "approval plan ID"),
        exact_literal_sha256=_digest(item["exact_literal_sha256"], "approval literal SHA-256"),
        approved_at_utc=_utc_from_text(item["approved_at_utc"], "approval time"),
        approved_by=_text(item["approved_by"], "approver"),
        authorization_mode=_text(item["authorization_mode"], "authorization mode"),
        approval_availability=_mapping(item["approval_availability"], "approval availability"),
        standing_authorization=(
            _mapping(item["standing_authorization"], "standing authorization")
            if item["standing_authorization"] is not None
            else None
        ),
        standing_reaffirmation=(
            _mapping(item["standing_reaffirmation"], "standing reaffirmation")
            if item["standing_reaffirmation"] is not None
            else None
        ),
    )
    if (
        item != approval.to_dict()
        or approval.approval_id != identifier
        or item["approval_version"] != STREAMING_APPROVAL_VERSION
        or item["artifact_type"] != "s7_streaming_four_table_full_exact_approval"
        or item["authorized_action"] != STREAMING_AUTHORIZED_ACTION
        or item["false_capabilities"] != dict(_FALSE_CAPABILITIES)
        or item["literal_version"] != STREAMING_APPROVAL_LITERAL_VERSION
    ):
        raise S7StreamingMaterializationError("exact approval replay differs")
    return approval, ExactFilePin(relative, hashlib.sha256(content).hexdigest(), len(content))


def _load_execution_controls(root: Path, *, plan_id: str, approval_id: str) -> dict[str, object]:
    approval, approval_receipt = _load_approval(root, approval_id)
    request, request_receipt = _load_request(root, approval.request_id)
    plan, plan_receipt, binding = _load_plan(root, plan_id)
    if (
        approval.plan_id != plan_id
        or request.plan_id != plan_id
        or approval.request != request_receipt
        or request.plan != plan_receipt
        or request.source_binding_id != binding.source_binding_id
        or request.resource_caps_digest != stable_digest(plan["resource_caps"])
        or request.runtime_file_set_digest != binding.runtime_binding["runtime_file_set_digest"]
        or approval.exact_literal_sha256
        != hashlib.sha256(exact_streaming_approval_literal(request).encode("utf-8")).hexdigest()
    ):
        raise S7StreamingMaterializationError("Full plan/request/approval chain differs")
    expected_availability = _approval_availability(root, binding, approval.approved_at_utc)
    if dict(approval.approval_availability) != expected_availability:
        raise S7StreamingMaterializationError("approval calendar availability differs")
    if approval.approved_at_utc < request.requested_at_utc or request.requested_at_utc < (
        _utc_from_text(plan["prepared_at_utc"], "plan preparation time")
    ):
        raise S7StreamingMaterializationError("Full control chronology differs")
    return {
        "approval": approval,
        "approval_receipt": approval_receipt,
        "binding": binding,
        "plan": plan,
        "plan_receipt": plan_receipt,
        "request": request,
        "request_receipt": request_receipt,
    }


def _approval_availability(
    root: Path,
    binding: S7StreamingSourceBinding,
    approved_at: datetime,
) -> dict[str, object]:
    return _calendar_availability(
        root,
        calendar_artifact_id=binding.calendar_artifact_id,
        calendar_artifact_sha256=binding.calendar_artifact_sha256,
        recorded_at=approved_at,
    )


def _calendar_availability(
    root: Path,
    *,
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
    recorded_at: datetime,
) -> dict[str, object]:
    try:
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=calendar_artifact_id,
            expected_sha256=calendar_artifact_sha256,
        )
        session, opening = calendar.first_open_after(recorded_at)
    except Exception as exc:  # calendar module supplies domain-specific errors
        raise S7StreamingMaterializationError("approval calendar binding is invalid") from exc
    return {
        "approval_recorded_at_utc": _utc_text(recorded_at),
        "calendar_artifact_id": calendar_artifact_id,
        "calendar_artifact_sha256": calendar_artifact_sha256,
        "first_xnys_open_utc": opening.isoformat(),
        "rule": "first_bound_xnys_open_strictly_after_runtime_approval_record_v1",
        "source_available_session": session.isoformat(),
    }


def _store_run_intent(
    root: Path,
    *,
    plan: Mapping[str, object],
    approval: StreamingExactApproval,
    binding: S7StreamingSourceBinding,
    candidate_id: str,
    now: Callable[[], datetime],
) -> ExactFilePin:
    captured = _utc(now(), "run intent time")
    if captured < approval.approved_at_utc:
        raise S7StreamingMaterializationError("run intent predates approval")
    payload = {
        "approval_id": approval.approval_id,
        "artifact_type": "s7_streaming_four_table_full_run_intent",
        "candidate_id": candidate_id,
        "capabilities": dict(_FALSE_CAPABILITIES),
        "captured_at_utc": _utc_text(captured),
        "intent_version": STREAMING_INTENT_VERSION,
        "plan_id": plan["plan_id"],
        "source_binding_id": binding.source_binding_id,
        "state": "authorized_awaiting_execution",
    }
    intent_id = stable_digest(payload)
    document = {**payload, "intent_id": intent_id}
    relative = (
        "manifests/silver/identity/s7-streaming-full-run-intents/"
        f"plan_id={plan['plan_id']}/approval_id={approval.approval_id}/manifest.json"
    )
    return _write_immutable(root, relative, _canonical_bytes(document), "Full run intent")


def _verify_all_source_pins(root: Path, binding: S7StreamingSourceBinding) -> None:
    pins = [
        binding.s4_release_set_manifest,
        *(item.artifact for item in binding.membership_artifacts),
        binding.gate_b.manifest,
        binding.gate_b.data,
        binding.gate_c.candidate_manifest,
        binding.gate_c.completion_manifest,
        binding.gate_c.qa,
        *(
            ExactFilePin(item.manifest_path, item.manifest_sha256, item.manifest_bytes)
            for item in binding.registry_pins
        ),
        *binding.contract_approvals,
    ]
    for pin in pins:
        _verify_file_pin(root, pin)


def _load_verified_execution_sources(
    root: Path,
    *,
    binding: S7StreamingSourceBinding,
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
) -> tuple[LoadedRegistryReleaseSet, dict[str, Mapping[str, object]]]:
    """Replay all bound source control chains after a durable execution intent."""

    if binding.mode == "production":
        rebuilt, registries = _build_official_production_source_binding(
            root,
            registry_pins=binding.registry_pins,
            cutoff_session=binding.cutoff_session,
            expected=binding,
        )
        if rebuilt != binding:
            raise S7StreamingMaterializationError(
                "production source binding changed during official replay"
            )
    else:
        _verify_all_source_pins(root, binding)
        if binding.contract_approvals != _trusted_contract_approvals(root, binding):
            raise S7StreamingMaterializationError("fixture contract approvals differ")
        registries = registry_loader(
            root,
            binding.registry_pins,
            require_exclusive_composite_scopes=False,
        )
        _verify_loaded_registry_set(registries, binding)
        _verify_gate_c_completion(root, binding.gate_c)
    gate_b = _load_gate_b_reference(root, binding.gate_b)
    return registries, gate_b


def _verify_loaded_registry_set(
    loaded: LoadedRegistryReleaseSet, binding: S7StreamingSourceBinding
) -> None:
    releases = tuple(loaded.releases)
    if tuple(item.registry_name for item in releases) != REGISTRY_ORDER:
        raise S7StreamingMaterializationError("loaded registry release order differs")
    for item, pin in zip(releases, binding.registry_pins, strict=True):
        if item.manifest_pin != pin or item.release_id != pin.release_id:
            raise S7StreamingMaterializationError("loaded registry release pin differs")


def _load_gate_b_reference(root: Path, pin: GateBReferencePin) -> dict[str, Mapping[str, object]]:
    manifest_content = _read_exact_file(root, pin.manifest.path, label="Gate-B manifest")
    manifest_loader = (
        _load_gate_b_compact_canonical_json
        if pin.reference_version == PRODUCTION_GATE_B_REFERENCE_VERSION
        else _load_canonical_json
    )
    manifest = _mapping(
        manifest_loader(manifest_content, "Gate-B manifest"),
        "Gate-B manifest",
    )
    if (
        manifest.get("candidate_id") != pin.candidate_id
        or manifest.get("state") != pin.candidate_state
    ):
        raise S7StreamingMaterializationError("Gate-B manifest identity/state differs")
    path = safe_relative_path(root, pin.data.path)
    table = pq.read_table(path)
    required = {
        "classification",
        "composite_figi",
        "selected_market_code",
        "source_available_session",
    }
    share_relation_fields = {
        "relation_share_class_conflict",
        "selected_share_class_figi",
    }
    if pin.reference_version == PRODUCTION_GATE_B_REFERENCE_VERSION:
        required.update(
            {
                "reference_version",
                *share_relation_fields,
            }
        )
    if not required.issubset(table.schema.names):
        raise S7StreamingMaterializationError("Gate-B reference columns differ")
    selected_columns = required | (share_relation_fields & set(table.schema.names))
    rows = table.select(sorted(selected_columns)).to_pylist()
    result: dict[str, Mapping[str, object]] = {}
    for row in rows:
        figi = _figi(row["composite_figi"], "Gate-B Composite FIGI")
        if figi in result:
            raise S7StreamingMaterializationError("Gate-B Composite key is duplicated")
        available = row["source_available_session"]
        if isinstance(available, str):
            available = date.fromisoformat(available)
        _native_date(available, "Gate-B availability")
        if (
            "reference_version" in row
            and _text(row["reference_version"], "Gate-B reference version") != pin.reference_version
        ):
            raise S7StreamingMaterializationError("Gate-B reference version differs")
        classification = _text(row["classification"], "Gate-B classification")
        relation_share_conflict = _native_bool(
            row.get("relation_share_class_conflict", False),
            "Gate-B relation ShareClass conflict",
        )
        selected_share = _optional_figi(
            row.get("selected_share_class_figi"),
            "Gate-B selected ShareClass",
        )
        if relation_share_conflict and selected_share is None:
            raise S7StreamingMaterializationError(
                "Gate-B ShareClass conflict lacks a unique exact-self ShareClass"
            )
        result[figi] = MappingProxyType(
            {
                "classification": classification,
                "relation_share_class_conflict": relation_share_conflict,
                "selected_market_code": _optional_text(
                    row["selected_market_code"], "Gate-B market code"
                ),
                "selected_share_class_figi": selected_share,
                "source_available_session": available,
            }
        )
    if not result:
        raise S7StreamingMaterializationError("Gate-B reference is empty")
    return result


def _verify_gate_c_completion(root: Path, pin: GateCCompletionPin) -> None:
    candidate = _mapping(
        _load_canonical_json(
            _read_exact_file(root, pin.candidate_manifest.path, label="Gate-C candidate"),
            "Gate-C candidate",
        ),
        "Gate-C candidate",
    )
    completion = _mapping(
        _load_canonical_json(
            _read_exact_file(root, pin.completion_manifest.path, label="Gate-C completion"),
            "Gate-C completion",
        ),
        "Gate-C completion",
    )
    qa = _mapping(
        _load_canonical_json(_read_exact_file(root, pin.qa.path, label="Gate-C QA"), "Gate-C QA"),
        "Gate-C QA",
    )
    completion_state = completion.get("completion_state", completion.get("state"))
    if (
        candidate.get("candidate_id") != pin.candidate_id
        or candidate.get("state") != STREAMING_STATE
        or completion.get("completion_id") != pin.completion_id
        or completion_state != pin.completion_state
        or qa.get("critical_failure_count") != 0
    ):
        raise S7StreamingMaterializationError("Gate-C completion/QA binding differs")


def _verify_file_pin(root: Path, pin: ExactFilePin) -> None:
    path = safe_relative_path(root, pin.path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.bytes
        or sha256_file(path) != pin.sha256
    ):
        raise S7StreamingMaterializationError(f"exact source pin differs: {pin.path}")


def _root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise S7StreamingMaterializationError("data root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise S7StreamingMaterializationError("data root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir() or root == Path("/"):
        raise S7StreamingMaterializationError("data root is unavailable or unsafe")
    return root


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise S7StreamingMaterializationError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise S7StreamingMaterializationError(f"{label} must be an array")
    return value


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise S7StreamingMaterializationError(f"{label} fields differ")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise S7StreamingMaterializationError(f"{label} must be trimmed nonempty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise S7StreamingMaterializationError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _digest(value, label)


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise S7StreamingMaterializationError(f"{label} is not a valid FIGI")
    return value


def _optional_figi(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _figi(value, label)


def _optional_mic(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _MIC.fullmatch(value) is None:
        raise S7StreamingMaterializationError(f"{label} is not a valid MIC")
    return value


def _optional_date(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _native_date(value, label)


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise S7StreamingMaterializationError(f"{label} must be a normalized relative path")
    return text


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise S7StreamingMaterializationError(f"{label} must be a nonnegative integer")
    return value


def _positive(value: object, label: str) -> int:
    result = _nonnegative(value, label)
    if result == 0:
        raise S7StreamingMaterializationError(f"{label} must be positive")
    return result


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise S7StreamingMaterializationError(f"{label} must be a native Boolean")
    return value


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise S7StreamingMaterializationError(f"{label} must be a date")
    return value


def _required_date(value: date | None, label: str) -> date:
    if value is None:
        raise S7StreamingMaterializationError(f"{label} is missing")
    return _native_date(value, label)


def _earlier(current: date | None, value: date) -> date:
    return value if current is None or value < current else current


def _later(current: date | None, value: date) -> date:
    return value if current is None or value > current else current


def _bounded_add(
    values: set[str],
    value: str,
    label: str,
    *,
    cap: int = _AGGREGATE_SET_CAP,
) -> None:
    if value in values:
        return
    if len(values) >= cap:
        raise S7StreamingMaterializationError(f"{label} exceed the bounded-memory cap")
    values.add(value)


def _normalize_cik(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 10 or not value.isascii():
        raise S7StreamingMaterializationError("observed CIK must be 1-10 ASCII digits")
    if not value.isdigit():
        raise S7StreamingMaterializationError("observed CIK must be 1-10 ASCII digits")
    return value.zfill(10)


def _share_class_id(figi: str) -> str:
    return stable_digest(
        {
            "anchor_type": "share_class_figi",
            "anchor_value": _figi(figi, "canonical ShareClass FIGI"),
            "namespace": "ame_stocks.identity.share_class",
            "rule_version": SHARE_CLASS_ID_RULE_VERSION,
        }
    )


def _issuer_id(cik: str) -> str:
    normalized = _text(cik, "normalized CIK")
    if _CIK.fullmatch(normalized) is None:
        raise S7StreamingMaterializationError("normalized CIK is invalid")
    return stable_digest(
        {
            "anchor_type": "cik_normalized",
            "anchor_value": normalized,
            "namespace": "ame_stocks.identity.issuer",
            "rule_version": ISSUER_ID_RULE_VERSION,
        }
    )


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise S7StreamingMaterializationError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _utc(value, "UTC timestamp").isoformat()


def _utc_from_text(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise S7StreamingMaterializationError(f"{label} is not ISO-8601") from exc
    return _utc(parsed, label)


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return _utc_text(value)
    if type(value) is date:
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise S7StreamingMaterializationError(
        f"value is not canonical-JSON serializable: {type(value).__name__}"
    )


def _json_scalar(value: object) -> object:
    normalized = _json_value(value)
    if isinstance(normalized, dict):
        return tuple(sorted((key, _json_scalar(item)) for key, item in normalized.items()))
    if isinstance(normalized, list):
        return tuple(_json_scalar(item) for item in normalized)
    return normalized


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _json_value(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load_canonical_json(content: bytes, label: str) -> object:
    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise S7StreamingMaterializationError(f"{label} is invalid JSON") from exc
    if _canonical_bytes(value) != content:
        raise S7StreamingMaterializationError(f"{label} is not canonical JSON")
    return value


def _load_gate_b_compact_canonical_json(content: bytes, label: str) -> object:
    """Load the producer's sorted compact Gate-B dialect without a trailing LF."""

    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise S7StreamingMaterializationError(f"{label} is invalid JSON") from exc
    expected = json.dumps(
        _json_value(value),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if expected != content:
        raise S7StreamingMaterializationError(f"{label} is not compact canonical JSON")
    return value


def _read_exact_file(root: Path, relative: str, *, label: str) -> bytes:
    path = safe_relative_path(root, _relative(relative, f"{label} path"))
    if not path.is_file() or path.is_symlink():
        raise S7StreamingMaterializationError(f"{label} is missing or unsafe")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise S7StreamingMaterializationError(f"cannot read {label}") from exc


def _write_immutable(root: Path, relative: str, content: bytes, label: str) -> ExactFilePin:
    normalized = _relative(relative, f"{label} path")
    path = safe_relative_path(root, normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        existing = _read_exact_file(root, normalized, label=label)
        if existing != content:
            raise S7StreamingMaterializationError(f"immutable {label} bytes differ")
    else:
        _write_exclusive(path, content)
        _fsync_directory(path.parent)
    return ExactFilePin(normalized, hashlib.sha256(content).hexdigest(), len(content))


def _plan_path(plan_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-full-plans/"
        f"plan_id={_digest(plan_id, 'Full plan ID')}/manifest.json"
    )


def _approval_path(approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-full-approvals/"
        f"approval_id={_digest(approval_id, 'approval ID')}/manifest.json"
    )


def _candidate_path(candidate_id: str) -> str:
    return (
        "silver/identity/s7-streaming-full-candidates/"
        f"candidate_id={_digest(candidate_id, 'candidate ID')}"
    )


def _completion_path(plan_id: str, approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-streaming-full-execution-completions/"
        f"plan_id={_digest(plan_id, 'Full plan ID')}/"
        f"approval_id={_digest(approval_id, 'approval ID')}/manifest.json"
    )


class _exclusive_nonblocking_lock(AbstractContextManager["_exclusive_nonblocking_lock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self) -> _exclusive_nonblocking_lock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        try:
            descriptor = os.open(self.path, flags, 0o600)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise S7StreamingMaterializationError("run lock is not a safe regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            visible = os.stat(self.path, follow_symlinks=False)
            locked = os.fstat(descriptor)
            if not stat.S_ISREG(visible.st_mode) or (
                visible.st_dev,
                visible.st_ino,
            ) != (locked.st_dev, locked.st_ino):
                raise S7StreamingMaterializationError("run lock path changed while acquiring")
        except BlockingIOError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise S7StreamingMaterializationError(
                "another process holds the nonblocking streaming Full lock"
            ) from exc
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            raise
        self.descriptor = descriptor
        return self

    def __exit__(self, *_: object) -> None:
        if self.descriptor is not None:
            try:
                fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            finally:
                os.close(self.descriptor)
                self.descriptor = None


def _fsync_regular_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise S7StreamingMaterializationError("staging tree contains a symlink")
        if path.is_file():
            _fsync_regular_file(path)
    directories = sorted(
        (item for item in root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts),
        reverse=True,
    )
    for path in directories:
        _fsync_directory(path)
    _fsync_directory(root)


def _rename_directory_noreplace(source: Path, target: Path) -> None:
    """Atomically publish one directory without replacing any foreign target."""

    if not source.is_dir() or source.is_symlink():
        raise S7StreamingMaterializationError("staging directory is missing or unsafe")
    source_stat = source.stat(follow_symlinks=False)
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise S7StreamingMaterializationError("renameat2 is unavailable")
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        result = rename(-100, os.fsencode(source), -100, os.fsencode(target), 1)
    elif sys.platform == "darwin":
        rename = getattr(libc, "renamex_np", None)
        if rename is None:
            raise S7StreamingMaterializationError("renamex_np is unavailable")
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(os.fsencode(source), os.fsencode(target), 0x00000004)
    else:  # pragma: no cover - production is limited to Linux and macOS
        raise S7StreamingMaterializationError("exclusive directory rename is unavailable")
    if result != 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise S7StreamingMaterializationError(
            f"exclusive candidate publish failed: {os.strerror(error_number)}"
        )
    target_stat = target.stat(follow_symlinks=False)
    if (source_stat.st_dev, source_stat.st_ino) != (target_stat.st_dev, target_stat.st_ino):
        raise S7StreamingMaterializationError("published candidate inode differs from staging")


def _directory_size(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    if path.is_symlink() or not path.is_dir():
        raise S7StreamingMaterializationError("staging path is unsafe")
    total = 0
    for item in path.rglob("*"):
        if item.is_symlink():
            raise S7StreamingMaterializationError("staging tree contains a symlink")
        if item.is_file():
            total += item.stat().st_size
    return total


def _default_rss_probe() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(usage if sys.platform == "darwin" else usage * 1024)


def _check_resource_caps(
    root: Path,
    *,
    staging: Path | None,
    started: float,
    caps: StreamingResourceCaps,
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
) -> None:
    elapsed = monotonic() - started
    if elapsed < 0 or elapsed > caps.wall_clock_seconds_cap:
        raise S7StreamingMaterializationError("streaming Full wall-clock cap exceeded")
    rss = int((rss_probe or _default_rss_probe)())
    if rss < 0 or rss > caps.rss_bytes_cap or rss > RSS_HARD_CAP_BYTES:
        raise S7StreamingMaterializationError("streaming Full RSS cap exceeded")
    disk_free = int(
        disk_free_probe(root) if disk_free_probe is not None else shutil.disk_usage(root).free
    )
    if disk_free < caps.disk_free_floor_bytes or disk_free < DISK_HARD_FLOOR_BYTES:
        raise S7StreamingMaterializationError("streaming Full disk floor breached")
    if staging is not None:
        total = _directory_size(staging)
        if total > caps.tmp_bytes_cap:
            raise S7StreamingMaterializationError("streaming Full tmp byte cap exceeded")
        data = staging / "data"
        if data.exists() and _directory_size(data) > caps.output_bytes_cap:
            raise S7StreamingMaterializationError("streaming Full output byte cap exceeded")


def _repository_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "docs").is_dir():
            return parent
    raise S7StreamingMaterializationError("repository root cannot be located")


def _git_output(repository: Path, *arguments: str, label: str) -> bytes:
    try:
        result = subprocess.run(
            ("git", "-C", str(repository), *arguments),
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise S7StreamingMaterializationError(f"cannot inspect Git {label}") from exc
    if result.returncode != 0:
        raise S7StreamingMaterializationError(f"cannot inspect Git {label}")
    return result.stdout


def _git_identifier(content: bytes, label: str) -> str:
    try:
        value = content.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise S7StreamingMaterializationError(f"Git {label} is not ASCII") from exc
    if _GIT_ID.fullmatch(value) is None:
        raise S7StreamingMaterializationError(f"Git {label} is invalid")
    return value


def _repository_runtime_binding() -> dict[str, object]:
    repository = _repository_root()
    status = _git_output(
        repository,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        label="worktree status",
    )
    if status:
        raise S7StreamingMaterializationError("runtime checkout is not exact and clean")
    commit = _git_identifier(
        _git_output(repository, "rev-parse", "--verify", "HEAD", label="commit"),
        "commit",
    )
    tree = _git_identifier(
        _git_output(repository, "rev-parse", "--verify", "HEAD^{tree}", label="tree"),
        "tree",
    )
    files: list[dict[str, object]] = []
    for relative in _RUNTIME_SOURCE_PATHS:
        path = repository / relative
        if not path.is_file() or path.is_symlink():
            raise S7StreamingMaterializationError(
                f"runtime source is missing or unsafe: {relative}"
            )
        tracked = (
            _git_output(
                repository,
                "ls-files",
                "--error-unmatch",
                "--",
                relative,
                label=f"tracked runtime source {relative}",
            )
            .decode("utf-8", errors="strict")
            .strip()
        )
        if tracked != relative:
            raise S7StreamingMaterializationError(f"runtime source tracking differs: {relative}")
        blob = _git_identifier(
            _git_output(repository, "hash-object", "--", relative, label=f"blob {relative}"),
            f"blob {relative}",
        )
        committed = _git_identifier(
            _git_output(
                repository,
                "rev-parse",
                f"HEAD:{relative}",
                label=f"committed blob {relative}",
            ),
            f"committed blob {relative}",
        )
        if blob != committed:
            raise S7StreamingMaterializationError(f"runtime source blob differs: {relative}")
        stage = (
            _git_output(
                repository,
                "ls-files",
                "--stage",
                "--",
                relative,
                label=f"runtime source mode {relative}",
            )
            .decode("utf-8", errors="strict")
            .strip()
        )
        try:
            metadata, staged_path = stage.split("\t", 1)
            git_mode, stage_blob, stage_number = metadata.split(" ", 2)
        except ValueError as exc:
            raise S7StreamingMaterializationError(
                f"runtime source index differs: {relative}"
            ) from exc
        if staged_path != relative or stage_blob != committed or stage_number != "0":
            raise S7StreamingMaterializationError(f"runtime source index differs: {relative}")
        files.append(
            {
                "bytes": path.stat().st_size,
                "git_blob": committed,
                "git_mode": git_mode,
                "path": relative,
                "sha256": sha256_file(path),
            }
        )
    return {
        "binding_version": "s7_streaming_full_runtime_git_binding_v1",
        "exact_checkout_clean": True,
        "repository_commit": commit,
        "repository_tree": tree,
        "runtime_file_set_digest": stable_digest(files),
        "runtime_files": files,
        "runtime_versions": {
            "pyarrow": importlib.metadata.version("pyarrow"),
            "python": platform.python_version(),
        },
    }


def _validate_runtime_binding(value: Mapping[str, object]) -> None:
    item = _mapping(value, "runtime binding")
    _expect_keys(
        item,
        {
            "binding_version",
            "exact_checkout_clean",
            "repository_commit",
            "repository_tree",
            "runtime_file_set_digest",
            "runtime_files",
            "runtime_versions",
        },
        "runtime binding",
    )
    if (
        item["binding_version"] != "s7_streaming_full_runtime_git_binding_v1"
        or item["exact_checkout_clean"] is not True
    ):
        raise S7StreamingMaterializationError("runtime binding semantics differ")
    _git_identifier(_text(item["repository_commit"], "runtime commit").encode(), "commit")
    _git_identifier(_text(item["repository_tree"], "runtime tree").encode(), "tree")
    files = _array(item["runtime_files"], "runtime files")
    if [
        row.get("path") for row in map(lambda value: _mapping(value, "runtime file"), files)
    ] != list(_RUNTIME_SOURCE_PATHS):
        raise S7StreamingMaterializationError("runtime file path set differs")
    normalized: list[dict[str, object]] = []
    for value in files:
        row = _mapping(value, "runtime file")
        _expect_keys(row, {"bytes", "git_blob", "git_mode", "path", "sha256"}, "runtime file")
        normalized.append(
            {
                "bytes": _nonnegative(row["bytes"], "runtime file bytes"),
                "git_blob": _git_identifier(
                    _text(row["git_blob"], "runtime Git blob").encode(), "blob"
                ),
                "git_mode": _text(row["git_mode"], "runtime Git mode"),
                "path": _relative(row["path"], "runtime file path"),
                "sha256": _digest(row["sha256"], "runtime file SHA-256"),
            }
        )
    if stable_digest(normalized) != _digest(
        item["runtime_file_set_digest"], "runtime file-set digest"
    ):
        raise S7StreamingMaterializationError("runtime file-set digest differs")
    versions = _mapping(item["runtime_versions"], "runtime versions")
    _expect_keys(versions, {"pyarrow", "python"}, "runtime versions")
    _text(versions["pyarrow"], "PyArrow version")
    _text(versions["python"], "Python version")


def _verify_candidate(
    root: Path,
    candidate_path: Path,
    *,
    plan: Mapping[str, object],
    approval: StreamingExactApproval,
    binding: S7StreamingSourceBinding,
    expected_candidate_id: str,
    caps: StreamingResourceCaps,
) -> dict[str, object]:
    if candidate_path.is_symlink() or not candidate_path.is_dir():
        raise S7StreamingMaterializationError("candidate path is missing or unsafe")
    relative_prefix = candidate_path.relative_to(root).as_posix()
    manifest_relative = f"{relative_prefix}/manifest.json"
    content = _read_exact_file(root, manifest_relative, label="streaming candidate manifest")
    manifest = _mapping(
        _load_canonical_json(content, "streaming candidate manifest"),
        "streaming candidate manifest",
    )
    _expect_keys(
        manifest,
        {
            "adapter_version",
            "approval_id",
            "artifact_type",
            "candidate_id",
            "candidate_version",
            "capabilities",
            "contract_pins",
            "intent",
            "manifest_id",
            "outputs",
            "plan_id",
            "policy_version",
            "source_binding_id",
            "state",
            "table_row_counts",
        },
        "streaming candidate manifest",
    )
    payload = dict(manifest)
    claimed_manifest_id = payload.pop("manifest_id")
    if (
        claimed_manifest_id != stable_digest(payload)
        or manifest["candidate_id"] != expected_candidate_id
        or manifest["approval_id"] != approval.approval_id
        or manifest["plan_id"] != plan["plan_id"]
        or manifest["source_binding_id"] != binding.source_binding_id
        or manifest["artifact_type"] != "s7_streaming_four_table_full_candidate"
        or manifest["candidate_version"] != STREAMING_CANDIDATE_VERSION
        or manifest["capabilities"] != dict(_FALSE_CAPABILITIES)
        or manifest["contract_pins"] != _contract_pins()
        or manifest["policy_version"] != STREAMING_POLICY_VERSION
        or manifest["state"] != STREAMING_STATE
    ):
        raise S7StreamingMaterializationError("streaming candidate manifest differs")
    outputs = _mapping(manifest["outputs"], "candidate outputs")
    _expect_keys(outputs, {*TABLE_ORDER, "qa"}, "candidate outputs")
    expected_files = {"manifest.json"}
    for receipt in _candidate_output_receipts(outputs):
        relative = _relative(receipt["path"], "candidate output path")
        expected_files.add(relative)
        _verify_candidate_output_receipt(candidate_path, receipt)
    actual_files = {
        item.relative_to(candidate_path).as_posix()
        for item in candidate_path.rglob("*")
        if item.is_file()
    }
    if any(item.is_symlink() for item in candidate_path.rglob("*")):
        raise S7StreamingMaterializationError("candidate tree contains a symlink")
    if actual_files != expected_files:
        raise S7StreamingMaterializationError("candidate file set differs")
    table_counts = {
        key: _nonnegative(value, f"{key} table row count")
        for key, value in _mapping(
            manifest["table_row_counts"], "candidate table row counts"
        ).items()
    }
    if set(table_counts) != set(TABLE_ORDER):
        raise S7StreamingMaterializationError("candidate table row-count keys differ")
    replay = _replay_candidate_tables(
        root,
        candidate_path,
        outputs=outputs,
        binding=binding,
        caps=caps,
        declared_counts=table_counts,
    )
    qa_receipt = _mapping(outputs["qa"], "candidate QA receipt")
    qa_content = _read_exact_file(
        candidate_path,
        _relative(qa_receipt["path"], "candidate QA path"),
        label="candidate QA",
    )
    qa = _mapping(_load_canonical_json(qa_content, "candidate QA"), "candidate QA")
    expected_qa = _build_qa(
        pass1=replay["pass1"],
        table_counts=table_counts,
        alias_row_count=table_counts["ticker_alias"],
        binding=binding,
    )
    if qa != expected_qa or qa["critical_failure_count"] != 0:
        raise S7StreamingMaterializationError("candidate QA replay differs")
    manifest_pin = ExactFilePin(
        manifest_relative,
        hashlib.sha256(content).hexdigest(),
        len(content),
    )
    return {
        "manifest": manifest,
        "manifest_pin": manifest_pin,
        "qa": qa,
        "table_row_counts": table_counts,
    }


def _candidate_output_receipts(
    outputs: Mapping[str, object],
) -> tuple[dict[str, object], ...]:
    receipts: list[dict[str, object]] = []
    for table_name in ("asset_master", "ticker_alias", "issuer_master", "qa"):
        receipts.append(_mapping(outputs[table_name], f"{table_name} output receipt"))
    universe = _array(outputs["universe_daily"], "universe output receipts")
    receipts.extend(_mapping(value, "universe output receipt") for value in universe)
    paths = [receipt.get("path") for receipt in receipts]
    if len(paths) != len(set(paths)):
        raise S7StreamingMaterializationError("candidate output receipt paths repeat")
    return tuple(receipts)


def _verify_candidate_output_receipt(candidate_path: Path, receipt: Mapping[str, object]) -> None:
    required = {"bytes", "path", "sha256"}
    if not required.issubset(receipt) or set(receipt) - {
        *required,
        "row_count",
        "schema_digest",
    }:
        raise S7StreamingMaterializationError("candidate output receipt fields differ")
    relative = _relative(receipt["path"], "candidate output path")
    path = safe_relative_path(candidate_path, relative)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != _nonnegative(receipt["bytes"], "candidate output bytes")
        or sha256_file(path) != _digest(receipt["sha256"], "candidate output SHA-256")
    ):
        raise S7StreamingMaterializationError("candidate output receipt differs")


def _replay_candidate_tables(
    root: Path,
    candidate_path: Path,
    *,
    outputs: Mapping[str, object],
    binding: S7StreamingSourceBinding,
    caps: StreamingResourceCaps,
    declared_counts: Mapping[str, int],
) -> dict[str, object]:
    gate_b = _load_gate_b_reference(root, binding.gate_b)
    assets = _read_small_exact_table(
        candidate_path / "data/asset_master.parquet",
        "asset_master",
        declared_counts["asset_master"],
    )
    issuers = _read_small_exact_table(
        candidate_path / "data/issuer_master.parquet",
        "issuer_master",
        declared_counts["issuer_master"],
    )
    asset_ids = {str(row["asset_id"]) for row in assets}
    issuer_ids = {str(row["issuer_id"]) for row in issuers}
    _validate_transition_graph(assets, asset_ids)
    verify_parent = root / "tmp/silver-s7-streaming-verification"
    verify_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="candidate-", dir=verify_parent) as temporary:
        database = Path(temporary) / "aliases.sqlite3"
        alias_count = _index_and_validate_aliases(
            candidate_path / "data/ticker_alias.parquet",
            database,
            expected_count=declared_counts["ticker_alias"],
            asset_ids=asset_ids,
            batch_size=caps.batch_row_cap,
        )
        if alias_count != declared_counts["ticker_alias"]:
            raise S7StreamingMaterializationError("candidate alias replay count differs")
        pass1 = _replay_universe_partitions(
            candidate_path,
            outputs=outputs,
            binding=binding,
            gate_b=gate_b,
            aliases_database=database,
            asset_ids=asset_ids,
            issuer_ids=issuer_ids,
            batch_size=caps.batch_row_cap,
        )
    if pass1["source_membership_rows"] != declared_counts["universe_daily"]:
        raise S7StreamingMaterializationError("candidate universe replay count differs")
    return {"pass1": pass1}


def _read_small_exact_table(
    path: Path, table_name: str, expected_count: int
) -> list[dict[str, object]]:
    if not path.is_file() or path.is_symlink():
        raise S7StreamingMaterializationError(f"{table_name} output is missing or unsafe")
    table = pq.read_table(path)
    if table.schema != S7_DERIVED_CONTRACTS[table_name].arrow_schema:
        raise S7StreamingMaterializationError(f"{table_name} output schema differs")
    if table.num_rows != expected_count:
        raise S7StreamingMaterializationError(f"{table_name} output row count differs")
    normalized = _contract_table(table_name, table.to_pylist())
    if normalized.to_pylist() != table.to_pylist():
        raise S7StreamingMaterializationError(f"{table_name} output sort order differs")
    return table.to_pylist()


def _validate_transition_graph(assets: Sequence[Mapping[str, object]], asset_ids: set[str]) -> None:
    by_id = {str(row["asset_id"]): row for row in assets}
    for asset_id, row in by_id.items():
        predecessors = list(row["predecessor_asset_ids"])
        successors = list(row["successor_asset_ids"])
        if (
            predecessors != sorted(set(predecessors))
            or successors != sorted(set(successors))
            or asset_id in predecessors
            or asset_id in successors
            or any(value not in asset_ids for value in (*predecessors, *successors))
        ):
            raise S7StreamingMaterializationError("asset transition graph differs")
        if any(asset_id not in by_id[value]["successor_asset_ids"] for value in predecessors):
            raise S7StreamingMaterializationError("asset predecessor edge is not reciprocal")
        if any(asset_id not in by_id[value]["predecessor_asset_ids"] for value in successors):
            raise S7StreamingMaterializationError("asset successor edge is not reciprocal")


def _index_and_validate_aliases(
    path: Path,
    database: Path,
    *,
    expected_count: int,
    asset_ids: set[str],
    batch_size: int,
) -> int:
    parquet = pq.ParquetFile(path)
    contract = S7_DERIVED_CONTRACTS["ticker_alias"]
    if parquet.schema_arrow != contract.arrow_schema:
        raise S7StreamingMaterializationError("ticker alias output schema differs")
    connection = sqlite3.connect(database)
    previous_key: tuple[object, ...] | None = None
    count = 0
    try:
        connection.execute(
            "CREATE TABLE aliases ("
            "alias_id TEXT PRIMARY KEY, ticker TEXT NOT NULL, start TEXT NOT NULL, "
            "end TEXT NOT NULL, asset_id TEXT NOT NULL) WITHOUT ROWID"
        )
        for batch in parquet.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch], schema=contract.arrow_schema)
            for row in table.to_pylist():
                key = tuple(row[field_name] for field_name in contract.sort_by)
                if previous_key is not None and key <= previous_key:
                    raise S7StreamingMaterializationError(
                        "ticker alias output key ordering differs"
                    )
                previous_key = key
                alias_id = _digest(row["ticker_alias_id"], "ticker alias ID")
                asset_id = _digest(row["asset_id"], "ticker alias asset ID")
                if (
                    asset_id not in asset_ids
                    or row["backtest_identity_eligible"] is not True
                    or row["composite_registry_collision"] is not False
                    or row["composite_registry_match_count"] not in {0, 1}
                ):
                    raise S7StreamingMaterializationError("ticker alias eligibility differs")
                if row["share_class_adjudication_id"] is not None and (
                    row["canonical_composite_figi"] is None or row["asset_id"] is None
                ):
                    raise S7StreamingMaterializationError(
                        "ShareClass correction preceded unique Composite resolution"
                    )
                connection.execute(
                    "INSERT INTO aliases VALUES (?, ?, ?, ?, ?)",
                    (
                        alias_id,
                        _text(row["ticker"], "ticker alias ticker"),
                        _native_date(row["valid_from_session"], "alias start").isoformat(),
                        _native_date(row["valid_through_session"], "alias end").isoformat(),
                        asset_id,
                    ),
                )
                count += 1
        connection.commit()
    except sqlite3.IntegrityError as exc:
        raise S7StreamingMaterializationError("ticker alias output key is duplicated") from exc
    finally:
        connection.close()
    if count != expected_count:
        raise S7StreamingMaterializationError("ticker alias output row count differs")
    return count


def _replay_universe_partitions(
    candidate_path: Path,
    *,
    outputs: Mapping[str, object],
    binding: S7StreamingSourceBinding,
    gate_b: Mapping[str, Mapping[str, object]],
    aliases_database: Path,
    asset_ids: set[str],
    issuer_ids: set[str],
    batch_size: int,
) -> dict[str, object]:
    receipts = _array(outputs["universe_daily"], "universe receipts")
    if len(receipts) != binding.session_count:
        raise S7StreamingMaterializationError("universe partition receipt count differs")
    lineage = hashlib.sha256()
    source_rows = 0
    unresolved_rows = 0
    collision_rows = 0
    share_relation_conflict_rows = 0
    share_relation_mismatch_rows = 0
    unadjudicated_share_conflict_rows = 0
    unadjudicated_share_conflict_eligible_rows = 0
    collision_examples: list[dict[str, object]] = []
    share_conflict_examples: list[dict[str, object]] = []
    connection = sqlite3.connect(f"file:{aliases_database}?mode=ro", uri=True)
    try:
        for pin, raw_receipt in zip(binding.membership_artifacts, receipts, strict=True):
            receipt = _mapping(raw_receipt, "universe receipt")
            expected_relative = (
                "data/universe_daily/"
                f"session_date={pin.session_date.isoformat()}/part-00000.parquet"
            )
            if receipt.get("path") != expected_relative:
                raise S7StreamingMaterializationError("universe partition receipt order differs")
            path = candidate_path / expected_relative
            parquet = pq.ParquetFile(path)
            contract = S7_DERIVED_CONTRACTS["universe_daily"]
            if parquet.schema_arrow != contract.arrow_schema:
                raise S7StreamingMaterializationError("universe output schema differs")
            partition_count = 0
            previous_key: tuple[object, ...] | None = None
            session_source_ids: set[str] = set()
            for batch in parquet.iter_batches(batch_size=batch_size):
                table = pa.Table.from_batches([batch], schema=contract.arrow_schema)
                for row in table.to_pylist():
                    if row["session_date"] != pin.session_date:
                        raise S7StreamingMaterializationError(
                            "universe output crosses its session partition"
                        )
                    key = tuple(row[field_name] for field_name in contract.primary_key)
                    if previous_key is not None and key <= previous_key:
                        raise S7StreamingMaterializationError(
                            "universe output key ordering differs"
                        )
                    previous_key = key
                    source_id = _digest(
                        row["selected_source_record_id"], "universe source record ID"
                    )
                    if source_id in session_source_ids:
                        raise S7StreamingMaterializationError(
                            "universe source record ID repeats within a session"
                        )
                    session_source_ids.add(source_id)
                    ticker = _text(row["ticker"], "universe ticker")
                    lineage.update(
                        _canonical_bytes(
                            {
                                "selected_source_record_id": source_id,
                                "session_date": pin.session_date.isoformat(),
                                "ticker": ticker,
                            }
                        )
                    )
                    lineage.update(b"\n")
                    _validate_replayed_universe_row(
                        row,
                        aliases=connection,
                        asset_ids=asset_ids,
                        issuer_ids=issuer_ids,
                    )
                    if row["composite_registry_collision"]:
                        collision_rows += 1
                        if len(collision_examples) < 100:
                            collision_examples.append(
                                {
                                    "selected_source_record_id": source_id,
                                    "session_date": pin.session_date.isoformat(),
                                    "ticker": ticker,
                                }
                            )
                    unresolved_rows += int(not bool(row["backtest_identity_eligible"]))
                    (
                        relation_conflict,
                        relation_mismatch,
                        unadjudicated_conflict,
                        eligible_violation,
                        selected_share,
                    ) = _gate_b_share_class_state(row, gate_b)
                    share_relation_conflict_rows += int(relation_conflict)
                    share_relation_mismatch_rows += int(relation_mismatch)
                    unadjudicated_share_conflict_rows += int(unadjudicated_conflict)
                    unadjudicated_share_conflict_eligible_rows += int(eligible_violation)
                    if relation_conflict and len(share_conflict_examples) < 100:
                        share_conflict_examples.append(
                            {
                                "backtest_identity_eligible": row["backtest_identity_eligible"],
                                "gate_b_selected_share_class_figi": selected_share,
                                "observed_composite_figi": row["observed_composite_figi"],
                                "observed_share_class_figi": row["observed_share_class_figi"],
                                "selected_source_record_id": source_id,
                                "session_date": pin.session_date.isoformat(),
                                "share_class_adjudication_id": row["share_class_adjudication_id"],
                                "ticker": ticker,
                            }
                        )
                    partition_count += 1
            if partition_count != pin.row_count:
                raise S7StreamingMaterializationError("universe partition row count differs")
            source_rows += partition_count
    finally:
        connection.close()
    return {
        "bounded_collision_examples": collision_examples,
        "bounded_share_class_conflict_examples": share_conflict_examples,
        "gate_b_relation_share_class_conflict_rows": share_relation_conflict_rows,
        "gate_b_relation_share_class_mismatch_rows": share_relation_mismatch_rows,
        "raw_collision_rows": collision_rows,
        "source_lineage_digest": lineage.hexdigest(),
        "source_membership_rows": source_rows,
        "source_record_id_count": source_rows,
        "unadjudicated_gate_b_share_class_conflict_eligible_rows": (
            unadjudicated_share_conflict_eligible_rows
        ),
        "unadjudicated_gate_b_share_class_conflict_rows": (unadjudicated_share_conflict_rows),
        "unresolved_rows": unresolved_rows,
    }


def _validate_replayed_universe_row(
    row: Mapping[str, object],
    *,
    aliases: sqlite3.Connection,
    asset_ids: set[str],
    issuer_ids: set[str],
) -> None:
    collision = _native_bool(row["composite_registry_collision"], "universe registry collision")
    match_count = _nonnegative(
        row["composite_registry_match_count"], "universe registry match count"
    )
    if collision != (match_count > 1) or row["identity_quality_liquidation_signal"] is not False:
        raise S7StreamingMaterializationError("universe collision/liquidation semantics differ")
    eligible = _native_bool(row["backtest_identity_eligible"], "universe eligibility")
    if collision and (
        eligible
        or row["asset_id"] is not None
        or row["canonical_composite_figi"] is not None
        or row["ticker_alias_id"] is not None
    ):
        raise S7StreamingMaterializationError("universe collision was resolved or eligible")
    if row["share_class_adjudication_id"] is not None and (
        row["canonical_composite_figi"] is None or row["asset_id"] is None
    ):
        raise S7StreamingMaterializationError(
            "ShareClass correction preceded unique Composite resolution"
        )
    issuer_id = row["issuer_id"]
    if issuer_id is not None and issuer_id not in issuer_ids:
        raise S7StreamingMaterializationError("universe issuer foreign key is missing")
    alias_id = row["ticker_alias_id"]
    if not eligible:
        if alias_id is not None:
            raise S7StreamingMaterializationError("ineligible universe row has an alias")
        return
    asset_id = row["asset_id"]
    if asset_id not in asset_ids or alias_id is None:
        raise S7StreamingMaterializationError("eligible universe identity foreign key is missing")
    found = aliases.execute(
        "SELECT ticker, start, end, asset_id FROM aliases WHERE alias_id = ?",
        (_digest(alias_id, "universe ticker alias ID"),),
    ).fetchall()
    if len(found) != 1:
        raise S7StreamingMaterializationError("eligible universe alias lookup is not unique")
    ticker, start, end, alias_asset = found[0]
    session = _native_date(row["session_date"], "universe session").isoformat()
    if ticker != row["ticker"] or alias_asset != asset_id or not (start <= session <= end):
        raise S7StreamingMaterializationError("eligible universe alias coverage differs")


def _store_and_verify_completion(
    root: Path,
    candidate: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    approval: StreamingExactApproval,
    binding: S7StreamingSourceBinding,
    completion_path: Path,
    now: Callable[[], datetime],
    started: float,
    monotonic: Callable[[], float],
    rss_probe: Callable[[], int] | None,
    disk_free_probe: Callable[[Path], int] | None,
    idempotent: bool,
) -> StreamingFullRunResult:
    caps = StreamingResourceCaps.from_dict(plan["resource_caps"])
    # Completion is the durable success marker, so every live/final resource check
    # must pass before its first byte can become visible.
    _check_resource_caps(
        root,
        staging=None,
        started=started,
        caps=caps,
        monotonic=monotonic,
        rss_probe=rss_probe,
        disk_free_probe=disk_free_probe,
    )
    completed_at = _utc(now(), "streaming Full completion time")
    if completed_at < approval.approved_at_utc:
        raise S7StreamingMaterializationError("streaming Full completion predates approval")
    manifest = _mapping(candidate["manifest"], "verified candidate manifest")
    manifest_pin = candidate["manifest_pin"]
    if not isinstance(manifest_pin, ExactFilePin):
        raise S7StreamingMaterializationError("verified candidate manifest pin is invalid")
    qa = _mapping(candidate["qa"], "verified candidate QA")
    table_counts = _mapping(candidate["table_row_counts"], "verified candidate table counts")
    payload = {
        "approval_id": approval.approval_id,
        "artifact_type": "s7_streaming_four_table_full_execution_completion",
        "candidate_id": manifest["candidate_id"],
        "candidate_manifest": manifest_pin.to_dict(),
        "capabilities": dict(_FALSE_CAPABILITIES),
        "complete": True,
        "completed_at_utc": _utc_text(completed_at),
        "completion_state": STREAMING_STATE,
        "completion_version": STREAMING_COMPLETION_VERSION,
        "plan_id": plan["plan_id"],
        "raw_collision_rows": qa["multi_registry_composite_override_collision_rows"],
        "source_binding_id": binding.source_binding_id,
        "source_row_count": qa["source_membership_rows"],
        "table_row_counts": table_counts,
    }
    completion_id = stable_digest(payload)
    document = {**payload, "completion_id": completion_id}
    expected_relative = _completion_path(plan["plan_id"], approval.approval_id)
    if completion_path != safe_relative_path(root, expected_relative):
        raise S7StreamingMaterializationError("completion target path differs")
    _write_immutable(
        root,
        expected_relative,
        _canonical_bytes(document),
        "streaming Full completion",
    )
    return _verify_completion_and_candidate(
        root,
        completion_path,
        plan=plan,
        approval=approval,
        binding=binding,
        expected_candidate_id=_digest(manifest["candidate_id"], "candidate ID"),
        caps=caps,
        idempotent=idempotent,
    )


def _verify_completion_and_candidate(
    root: Path,
    completion_path: Path,
    *,
    plan: Mapping[str, object],
    approval: StreamingExactApproval,
    binding: S7StreamingSourceBinding,
    expected_candidate_id: str,
    caps: StreamingResourceCaps,
    idempotent: bool,
) -> StreamingFullRunResult:
    expected_relative = _completion_path(plan["plan_id"], approval.approval_id)
    if completion_path != safe_relative_path(root, expected_relative):
        raise S7StreamingMaterializationError("completion path differs")
    content = _read_exact_file(root, expected_relative, label="streaming Full completion")
    completion = _mapping(
        _load_canonical_json(content, "streaming Full completion"),
        "streaming Full completion",
    )
    _expect_keys(
        completion,
        {
            "approval_id",
            "artifact_type",
            "candidate_id",
            "candidate_manifest",
            "capabilities",
            "complete",
            "completed_at_utc",
            "completion_id",
            "completion_state",
            "completion_version",
            "plan_id",
            "raw_collision_rows",
            "source_binding_id",
            "source_row_count",
            "table_row_counts",
        },
        "streaming Full completion",
    )
    payload = dict(completion)
    claimed_id = payload.pop("completion_id")
    if (
        claimed_id != stable_digest(payload)
        or completion["approval_id"] != approval.approval_id
        or completion["candidate_id"] != expected_candidate_id
        or completion["capabilities"] != dict(_FALSE_CAPABILITIES)
        or completion["complete"] is not True
        or completion["completion_state"] != STREAMING_STATE
        or completion["completion_version"] != STREAMING_COMPLETION_VERSION
        or completion["plan_id"] != plan["plan_id"]
        or completion["source_binding_id"] != binding.source_binding_id
        or completion["artifact_type"] != "s7_streaming_four_table_full_execution_completion"
    ):
        raise S7StreamingMaterializationError("streaming Full completion differs")
    _utc_from_text(completion["completed_at_utc"], "completion time")
    candidate_path = safe_relative_path(root, _candidate_path(expected_candidate_id))
    candidate = _verify_candidate(
        root,
        candidate_path,
        plan=plan,
        approval=approval,
        binding=binding,
        expected_candidate_id=expected_candidate_id,
        caps=caps,
    )
    if ExactFilePin.from_dict(completion["candidate_manifest"]) != candidate["manifest_pin"]:
        raise S7StreamingMaterializationError("completion candidate manifest receipt differs")
    qa = _mapping(candidate["qa"], "verified candidate QA")
    table_counts = {
        key: _nonnegative(value, f"completion {key} row count")
        for key, value in _mapping(
            completion["table_row_counts"], "completion table row counts"
        ).items()
    }
    if (
        table_counts != candidate["table_row_counts"]
        or completion["source_row_count"] != qa["source_membership_rows"]
        or completion["raw_collision_rows"]
        != qa["multi_registry_composite_override_collision_rows"]
    ):
        raise S7StreamingMaterializationError("completion candidate summary differs")
    return StreamingFullRunResult(
        plan_id=_digest(plan["plan_id"], "Full plan ID"),
        approval_id=approval.approval_id,
        candidate_id=expected_candidate_id,
        candidate_path=_candidate_path(expected_candidate_id),
        completion_id=_digest(claimed_id, "completion ID"),
        completion_path=expected_relative,
        session_count=binding.session_count,
        source_row_count=_nonnegative(completion["source_row_count"], "completion source rows"),
        table_row_counts=MappingProxyType(table_counts),
        raw_collision_rows=_nonnegative(
            completion["raw_collision_rows"], "completion collision rows"
        ),
        idempotent=idempotent,
    )
