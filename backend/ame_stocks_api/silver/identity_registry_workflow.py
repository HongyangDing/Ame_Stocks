"""Immutable S7 registry decision and release controls.

This module is deliberately independent from the S7 scanners and materializers.  It
never discovers ``latest`` inputs and it never edits provider observations.  A caller
must provide exact source-row snapshots, exact candidate/evidence artifact receipts and
an exact contract pin.  The workflow then provides the following fail-closed chain::

    candidate -> decision plan -> literal approval request -> approval receipt -> release

The release is a content-addressed snapshot of one of the five S7 identity registries.
Every decision is represented twice: once in the contract-shaped Parquet row set and once
in a canonical JSON replay document containing the complete row plus its exact source-row
scope.  The loader byte-checks and reconciles both representations before exposing an
index to a streaming materializer.

Creating candidates/plans/requests does not authorize a release.  ``publish_release``
requires a receipt created from the byte-exact request literal.  This is a control
mechanism, not an authorization shortcut: a request must still be shown to and approved
by the reviewer outside this module.
"""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArrowType, TableContract
from ame_stocks_api.silver.identity_relation_registries import (
    canonical_share_class_id,
)
from ame_stocks_api.silver.identity_relation_registry_contract import (
    RELATION_REGISTRY_CONTRACTS,
    RELATION_REGISTRY_RESOURCE_SHA256,
)
from ame_stocks_api.silver.identity_resolution import canonical_asset_id
from ame_stocks_api.silver.identity_resolution_contract import (
    S7_ADJUDICATION_CONTRACTS,
    S7_RESOURCE_SHA256_BY_TABLE,
)

WORKFLOW_VERSION: Final = 1
WORKFLOW_POLICY: Final = "s7-registry-decision-release-control-v1"
APPROVAL_LITERAL_VERSION: Final = "s7_registry_release_approval_literal_v1"
APPROVAL_ACTION: Final = "approve_exact_s7_registry_candidate_and_release_once"
STANDING_AUTHORIZATION_VERSION: Final = "s7_registry_standing_authorization_v1"
STANDING_AUTHORIZATION_ACTION: Final = (
    "release_one_exact_reviewed_s7_registry_candidate_under_standing_authority"
)
STANDING_AUTHORIZATION_LITERAL: Final = (
    "为什么你就不能自己直接把S7运行完呢\uff0c"
    "我允许你这么做\uff0c"
    "只要中间不报错或者明显越界就可以自行继续"
)
STANDING_REAFFIRMATION_LITERAL: Final = "批准"
_PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION_V1: Final = (
    "s7_registry_production_prerequisite_authorization_v1"
)
PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION: Final = (
    "s7_registry_production_prerequisite_authorization_v2"
)
_SUPPORTED_PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSIONS: Final = frozenset(
    {
        _PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION_V1,
        PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION,
    }
)
PRODUCTION_PREREQUISITE_AUTHORIZATION_TYPE: Final = (
    "s7_registry_production_prerequisite_authorization"
)
PRODUCTION_INGRESS_ATTESTATION_VERSION: Final = "s7_registry_production_ingress_attestation_v1"
PRODUCTION_INGRESS_ATTESTATION_TYPE: Final = "s7_registry_production_ingress_attestation"
CANONICAL_PRODUCTION_DATA_ROOT: Final = Path("/mnt/HC_Volume_106309665/american_stocks")
AWAITING_REVIEW: Final = "awaiting_review"
APPROVED: Final = "approved"
PUBLISHED: Final = "published"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_ID = re.compile(r"^[0-9a-f]{40}([0-9a-f]{24})?$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_MIC = re.compile(r"^[A-Z0-9]{4}$")
_MARKET_CODE = re.compile(r"^[A-Z0-9]{2,8}$")
RUNTIME_BINDING_PATHS: Final = (
    "pyproject.toml",
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/cli/silver_identity_registry_production.py",
    "backend/ame_stocks_api/cli/silver_identity_registry_workflow.py",
    "backend/ame_stocks_api/silver/calendar_artifact.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/identity_adjudication.py",
    "backend/ame_stocks_api/silver/identity_cross_market.py",
    "backend/ame_stocks_api/silver/identity_registry_exact_group_scopes.py",
    "backend/ame_stocks_api/silver/identity_registry_production.py",
    "backend/ame_stocks_api/silver/identity_registry_workflow.py",
    "backend/ame_stocks_api/silver/identity_relation_registries.py",
    "backend/ame_stocks_api/silver/identity_relation_registry_contract.py",
    "backend/ame_stocks_api/silver/identity_resolution.py",
    "backend/ame_stocks_api/silver/identity_resolution_contract.py",
    "backend/ame_stocks_api/silver/schema_resources/asset_transition.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_adjudication.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/identity_cross_market_adjudication.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/provider_composite_override.schema-v1.json",
    "backend/ame_stocks_api/silver/schema_resources/share_class_adjudication.schema-v1.json",
)


class RegistryWorkflowError(RuntimeError):
    """Raised before an ambiguous or unbound registry artifact can be consumed."""


class RegistryName(StrEnum):
    IDENTITY_ADJUDICATION = "identity_adjudication"
    IDENTITY_CROSS_MARKET_ADJUDICATION = "identity_cross_market_adjudication"
    PROVIDER_COMPOSITE_OVERRIDE = "provider_composite_override"
    SHARE_CLASS_ADJUDICATION = "share_class_adjudication"
    ASSET_TRANSITION = "asset_transition"


def is_canonical_production_data_root(data_root: Path) -> bool:
    """Return whether ``data_root`` is the one authorized production namespace."""

    return data_root.expanduser().resolve() == CANONICAL_PRODUCTION_DATA_ROOT.resolve()


def require_fixture_registry_root(data_root: Path) -> None:
    """Fail closed before a low-level fixture command can touch production state."""

    if is_canonical_production_data_root(data_root):
        raise RegistryWorkflowError(
            "fixture/internal registry APIs cannot operate on the canonical production root"
        )


REGISTRY_ORDER: Final = tuple(item.value for item in RegistryName)
COMPOSITE_CORRECTION_REGISTRIES: Final = frozenset(
    {
        RegistryName.IDENTITY_ADJUDICATION.value,
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
    }
)
REQUIRED_CANDIDATE_AUTHORIZATION_ROLES: Final = frozenset(
    {
        "external_evidence_approval",
        "schema_contract_approval",
        "source_candidate_approval",
    }
)

# The standing instruction can replace a per-release user-supplied JSON literal only
# for one already-built and exactly reviewed registry candidate.  Everything outside
# that single immutable release remains false and therefore fail-closed.
STANDING_AUTHORIZATION_CAPABILITIES: Final[Mapping[str, bool]] = MappingProxyType(
    {
        "candidate_generation": False,
        "candidate_mutation": False,
        "decision_plan_generation": False,
        "evidence_mutation": False,
        "full_run": False,
        "identity_market_consistency_scan": False,
        "materialization": False,
        "multi_registry_release": False,
        "network_access": False,
        "publish_plan": False,
        "registry_release_set": False,
        "remote_source_code_mutation": False,
        "single_exact_registry_release": True,
        "source_scan": False,
        "source_scope_mutation": False,
    }
)
STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES: Final[Mapping[str, bool]] = MappingProxyType(
    {
        "candidate_generation": False,
        "candidate_mutation": False,
        "decision_plan_generation": False,
        "evidence_mutation": False,
        "full_run": False,
        "identity_market_consistency_scan": False,
        "materialization": False,
        "network_access": False,
        "publish_plan": False,
        "registry_release": False,
        "remote_source_code_mutation": False,
        "single_candidate_prerequisite_authorization": True,
        "source_scan": False,
        "source_scope_mutation": False,
    }
)

_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {**S7_ADJUDICATION_CONTRACTS, **RELATION_REGISTRY_CONTRACTS}
)
_RESOURCE_SHA: Final[Mapping[str, str]] = MappingProxyType(
    {
        **{
            name: S7_RESOURCE_SHA256_BY_TABLE[name]
            for name in (
                RegistryName.IDENTITY_ADJUDICATION.value,
                RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
            )
        },
        **dict(RELATION_REGISTRY_RESOURCE_SHA256),
    }
)

_DECISION_ID_COLUMN: Final[Mapping[str, str]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: "identity_adjudication_id",
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: ("cross_market_adjudication_id"),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: ("provider_composite_override_id"),
        RegistryName.SHARE_CLASS_ADJUDICATION.value: "share_class_adjudication_id",
        RegistryName.ASSET_TRANSITION.value: "asset_transition_id",
    }
)
_SERIES_ID_COLUMN: Final[Mapping[str, str]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: "adjudication_series_id",
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: "cross_market_series_id",
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: ("provider_composite_override_series_id"),
        RegistryName.SHARE_CLASS_ADJUDICATION.value: ("share_class_adjudication_series_id"),
        RegistryName.ASSET_TRANSITION.value: "asset_transition_series_id",
    }
)
_PREDECESSOR_COLUMN: Final[Mapping[str, str]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: "supersedes_identity_adjudication_id",
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: (
            "supersedes_cross_market_adjudication_id"
        ),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: (
            "supersedes_provider_composite_override_id"
        ),
        RegistryName.SHARE_CLASS_ADJUDICATION.value: ("supersedes_share_class_adjudication_id"),
        RegistryName.ASSET_TRANSITION.value: "supersedes_asset_transition_id",
    }
)
_AVAILABLE_SESSION_COLUMN: Final[Mapping[str, str]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: "adjudication_available_session",
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: ("adjudication_available_session"),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: "override_available_session",
        RegistryName.SHARE_CLASS_ADJUDICATION.value: "adjudication_available_session",
        RegistryName.ASSET_TRANSITION.value: "transition_available_session",
    }
)

_SCOPE_LAYOUT: Final[Mapping[str, tuple[str, str, str]]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: (
            "episode_source_record_count",
            "episode_source_record_set_digest",
            "",
        ),
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: (
            "scoped_source_record_count",
            "scoped_source_record_set_digest",
            "scoped_source_record_ids_json",
        ),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: (
            "scoped_source_record_count",
            "scoped_source_record_set_digest",
            "scoped_source_record_ids_json",
        ),
        RegistryName.SHARE_CLASS_ADJUDICATION.value: (
            "scoped_source_record_count",
            "scoped_source_record_set_digest",
            "scoped_source_record_ids_json",
        ),
        RegistryName.ASSET_TRANSITION.value: (
            "boundary_source_record_count",
            "boundary_source_record_set_digest",
            "boundary_source_record_ids_json",
        ),
    }
)

# These fields are populated only after a reviewer approves the exact request.  Every
# other contract field is frozen in the candidate and must replay byte-for-byte (after
# JSON normalization) in the release row.
_POST_APPROVAL_COLUMNS: Final[Mapping[str, frozenset[str]]] = MappingProxyType(
    {
        RegistryName.IDENTITY_ADJUDICATION.value: frozenset(
            {
                "approval_id",
                "approval_receipt_path",
                "approval_receipt_sha256",
                "approval_status",
                "approved_by",
                "approved_at_utc",
                "approval_available_session",
                "adjudication_available_at_utc",
                "adjudication_available_session",
                "source_decision_plan_id",
                "source_decision_plan_path",
                "source_decision_plan_sha256",
            }
        ),
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: frozenset(
            {
                "approval_status",
                "approval_request_event_id",
                "approval_request_event_sha256",
                "approval_receipt_id",
                "approval_receipt_path",
                "approval_receipt_sha256",
                "approved_by",
                "approved_at_utc",
                "approval_available_session",
                "adjudication_available_session",
            }
        ),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: frozenset(
            {
                "source_decision_plan_id",
                "source_decision_plan_path",
                "source_decision_plan_sha256",
                "approval_request_event_id",
                "approval_request_event_sha256",
                "approval_receipt_id",
                "approval_receipt_sha256",
                "approved_by",
                "approved_at_utc",
                "approval_available_session",
                "override_available_session",
            }
        ),
        RegistryName.SHARE_CLASS_ADJUDICATION.value: frozenset(
            {
                "source_decision_plan_id",
                "source_decision_plan_path",
                "source_decision_plan_sha256",
                "approval_request_event_id",
                "approval_request_event_sha256",
                "approval_receipt_id",
                "approval_receipt_sha256",
                "approved_by",
                "approved_at_utc",
                "approval_available_session",
                "adjudication_available_session",
            }
        ),
        RegistryName.ASSET_TRANSITION.value: frozenset(
            {
                "source_decision_plan_id",
                "source_decision_plan_path",
                "source_decision_plan_sha256",
                "approval_request_event_id",
                "approval_request_event_sha256",
                "approval_receipt_id",
                "approval_receipt_sha256",
                "approved_by",
                "approved_at_utc",
                "approval_available_session",
                "transition_available_session",
            }
        ),
    }
)


@dataclass(frozen=True, slots=True)
class RegistryContractPin:
    registry_name: str
    contract_id: str
    schema_digest: str
    resource_sha256: str

    def __post_init__(self) -> None:
        _registry(self.registry_name)
        _digest(self.contract_id, "contract ID")
        _digest(self.schema_digest, "schema digest")
        _digest(self.resource_sha256, "contract resource SHA-256")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "registry_name": self.registry_name,
            "resource_sha256": self.resource_sha256,
            "schema_digest": self.schema_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryContractPin:
        item = _mapping(value, "contract pin")
        _expect_keys(
            item,
            {"contract_id", "registry_name", "resource_sha256", "schema_digest"},
            "contract pin",
        )
        return cls(
            registry_name=_text(item["registry_name"], "registry name"),
            contract_id=_digest(item["contract_id"], "contract ID"),
            schema_digest=_digest(item["schema_digest"], "schema digest"),
            resource_sha256=_digest(item["resource_sha256"], "resource SHA-256"),
        )


def current_registry_contract_pin(registry_name: str) -> RegistryContractPin:
    """Return a byte pin, not an assertion that the schema has been approved."""

    name = _registry(registry_name)
    contract = _CONTRACTS[name]
    return RegistryContractPin(
        registry_name=name,
        contract_id=contract.contract_id,
        schema_digest=contract.schema_digest,
        resource_sha256=_RESOURCE_SHA[name],
    )


def create_registry_decision_candidate(
    *,
    registry_name: str,
    case_key: str,
    proposed_contract_row: Mapping[str, object],
    source_scope: ExactSourceScope,
) -> RegistryDecisionCandidate:
    """Freeze a model-produced row while excluding only post-approval control fields."""

    name = _registry(registry_name)
    contract_columns = tuple(column.name for column in _CONTRACTS[name].columns)
    row = dict(proposed_contract_row)
    if set(row) != set(contract_columns):
        raise RegistryWorkflowError("proposed decision row fields differ from contract")
    row = {column: row[column] for column in contract_columns}
    claims = {
        column: value for column, value in row.items() if column not in _POST_APPROVAL_COLUMNS[name]
    }
    id_column = _DECISION_ID_COLUMN[name]
    predecessor_column = _PREDECESSOR_COLUMN[name]
    return RegistryDecisionCandidate(
        registry_name=name,
        case_key=case_key,
        decision_id=_digest(row[id_column], "proposed decision ID"),
        decision_version=_positive(row["decision_version"], "decision version"),
        supersedes_decision_id=_optional_digest(row[predecessor_column], "superseded decision ID"),
        frozen_row_claims=claims,
        source_scope=source_scope,
    )


def build_registry_authorization_document(
    *,
    authorization_type: str,
    registry_name: str,
    target_refs: Sequence[tuple[str, str]],
    approved_by: str,
    approved_at_utc: datetime,
    approval_available_session: date,
) -> dict[str, object]:
    """Build the canonical logical approval document later bound as exact bytes."""

    if authorization_type not in REQUIRED_CANDIDATE_AUTHORIZATION_ROLES:
        raise RegistryWorkflowError("unsupported candidate authorization type")
    name = _registry(registry_name)
    targets = tuple(sorted(target_refs))
    if not targets or len(set(targets)) != len(targets):
        raise RegistryWorkflowError("authorization targets must be nonempty and unique")
    for target_id, target_sha in targets:
        _digest(target_id, "authorization target ID")
        _digest(target_sha, "authorization target SHA-256")
    _text(approved_by, "authorization actor")
    _utc(approved_at_utc, "authorization time")
    _date(approval_available_session, "authorization availability")
    logical = {
        "approval_available_session": approval_available_session.isoformat(),
        "approved_at_utc": _utc_text(approved_at_utc),
        "approved_by": approved_by,
        "authorization_type": authorization_type,
        "decision": APPROVED,
        "policy_version": WORKFLOW_POLICY,
        "registry_name": name,
        "target_refs": [
            {"artifact_id": target_id, "sha256": target_sha} for target_id, target_sha in targets
        ],
    }
    return {"authorization_id": stable_digest(logical), **logical}


@dataclass(frozen=True, slots=True)
class ExactArtifactBinding:
    role: str
    artifact_id: str
    path: str
    sha256: str
    bytes: int
    available_session: date
    embedded_id_field: str | None = None

    def __post_init__(self) -> None:
        _text(self.role, "artifact role")
        _digest(self.artifact_id, "artifact ID")
        _relative(self.path, "artifact path")
        _digest(self.sha256, "artifact SHA-256")
        _positive(self.bytes, "artifact bytes")
        _date(self.available_session, "artifact availability")
        if self.embedded_id_field is not None:
            _text(self.embedded_id_field, "embedded ID field")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "available_session": self.available_session.isoformat(),
            "bytes": self.bytes,
            "embedded_id_field": self.embedded_id_field,
            "path": self.path,
            "role": self.role,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> ExactArtifactBinding:
        item = _mapping(value, "artifact binding")
        _expect_keys(
            item,
            {
                "artifact_id",
                "available_session",
                "bytes",
                "embedded_id_field",
                "path",
                "role",
                "sha256",
            },
            "artifact binding",
        )
        return cls(
            role=_text(item["role"], "artifact role"),
            artifact_id=_digest(item["artifact_id"], "artifact ID"),
            path=_relative(item["path"], "artifact path"),
            sha256=_digest(item["sha256"], "artifact SHA-256"),
            bytes=_positive(item["bytes"], "artifact bytes"),
            available_session=date.fromisoformat(
                _text(item["available_session"], "artifact availability")
            ),
            embedded_id_field=_optional_text(item["embedded_id_field"], "embedded ID field"),
        )


@dataclass(frozen=True, slots=True, order=True)
class ExactSourceRow:
    """An immutable provider-observed row admitted to exactly one decision scope."""

    session_date: date
    source_record_id: str
    source_dataset: str
    source_s4_release_set_id: str
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    observed_composite_figi: str
    observed_share_class_figi: str | None
    primary_exchange_mic: str | None

    def __post_init__(self) -> None:
        _date(self.session_date, "source session")
        _digest(self.source_record_id, "source record ID")
        _text(self.source_dataset, "source dataset")
        _digest(self.source_s4_release_set_id, "source S4 release-set ID")
        if (
            self.provider_id != "massive"
            or self.provider_market != "stocks"
            or self.provider_locale != "us"
        ):
            raise RegistryWorkflowError("S7 registry source scope must be massive/stocks/us")
        _text(self.ticker, "source ticker")
        _figi(self.observed_composite_figi, "observed Composite FIGI")
        if self.observed_share_class_figi is not None:
            _figi(self.observed_share_class_figi, "observed Share Class FIGI")
        if self.primary_exchange_mic is not None and not _MIC.fullmatch(self.primary_exchange_mic):
            raise RegistryWorkflowError("source primary exchange MIC is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "primary_exchange_mic": self.primary_exchange_mic,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "session_date": self.session_date.isoformat(),
            "source_dataset": self.source_dataset,
            "source_record_id": self.source_record_id,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "ticker": self.ticker,
        }

    @classmethod
    def from_dict(cls, value: object) -> ExactSourceRow:
        item = _mapping(value, "exact source row")
        expected = {
            "observed_composite_figi",
            "observed_share_class_figi",
            "primary_exchange_mic",
            "provider_id",
            "provider_locale",
            "provider_market",
            "session_date",
            "source_dataset",
            "source_record_id",
            "source_s4_release_set_id",
            "ticker",
        }
        _expect_keys(item, expected, "exact source row")
        return cls(
            session_date=date.fromisoformat(_text(item["session_date"], "source session")),
            source_record_id=_digest(item["source_record_id"], "source record ID"),
            source_dataset=_text(item["source_dataset"], "source dataset"),
            source_s4_release_set_id=_digest(
                item["source_s4_release_set_id"], "source S4 release-set ID"
            ),
            provider_id=_text(item["provider_id"], "provider ID"),
            provider_market=_text(item["provider_market"], "provider market"),
            provider_locale=_text(item["provider_locale"], "provider locale"),
            ticker=_text(item["ticker"], "ticker"),
            observed_composite_figi=_figi(
                item["observed_composite_figi"], "observed Composite FIGI"
            ),
            observed_share_class_figi=_optional_figi(
                item["observed_share_class_figi"], "observed Share Class FIGI"
            ),
            primary_exchange_mic=_optional_text(
                item["primary_exchange_mic"], "primary exchange MIC"
            ),
        )


@dataclass(frozen=True, slots=True)
class ExactSourceScope:
    rows: tuple[ExactSourceRow, ...]

    def __post_init__(self) -> None:
        rows = tuple(sorted(self.rows))
        if not rows:
            raise RegistryWorkflowError("decision source scope cannot be empty")
        if rows != self.rows:
            raise RegistryWorkflowError("decision source rows must be sorted")
        ids = tuple(row.source_record_id for row in rows)
        if len(set(ids)) != len(ids):
            raise RegistryWorkflowError("decision source record IDs are repeated")
        if len({row.source_s4_release_set_id for row in rows}) != 1:
            raise RegistryWorkflowError("decision source scope mixes S4 release sets")

    @property
    def source_record_ids(self) -> tuple[str, ...]:
        return tuple(sorted(row.source_record_id for row in self.rows))

    @property
    def source_record_set_digest(self) -> str:
        return stable_digest(list(self.source_record_ids))

    @property
    def scope_digest(self) -> str:
        return stable_digest([row.to_dict() for row in self.rows])

    def to_dict(self) -> dict[str, object]:
        return {
            "row_count": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
            "scope_digest": self.scope_digest,
            "source_record_set_digest": self.source_record_set_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> ExactSourceScope:
        item = _mapping(value, "exact source scope")
        _expect_keys(
            item,
            {"row_count", "rows", "scope_digest", "source_record_set_digest"},
            "exact source scope",
        )
        scope = cls(rows=tuple(ExactSourceRow.from_dict(row) for row in _array(item["rows"])))
        if item["row_count"] != len(scope.rows):
            raise RegistryWorkflowError("source scope row count changed")
        if item["scope_digest"] != scope.scope_digest:
            raise RegistryWorkflowError("source scope digest changed")
        if item["source_record_set_digest"] != scope.source_record_set_digest:
            raise RegistryWorkflowError("source record-set digest changed")
        return scope


@dataclass(frozen=True, slots=True)
class RegistryDecisionCandidate:
    """One pre-approval decision with every non-approval row field frozen."""

    registry_name: str
    case_key: str
    decision_id: str
    decision_version: int
    supersedes_decision_id: str | None
    frozen_row_claims: Mapping[str, object]
    source_scope: ExactSourceScope

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        _text(self.case_key, "candidate case key")
        _digest(self.decision_id, "candidate decision ID")
        _positive(self.decision_version, "decision version")
        if (self.decision_version == 1) != (self.supersedes_decision_id is None):
            raise RegistryWorkflowError("candidate decision predecessor/version matrix changed")
        if self.supersedes_decision_id is not None:
            _digest(self.supersedes_decision_id, "superseded decision ID")
        claims = dict(self.frozen_row_claims)
        contract_columns = tuple(column.name for column in _CONTRACTS[name].columns)
        expected = set(contract_columns).difference(_POST_APPROVAL_COLUMNS[name])
        if set(claims) != expected:
            missing = sorted(expected.difference(claims))
            extra = sorted(set(claims).difference(expected))
            raise RegistryWorkflowError(
                f"candidate frozen row claims changed; missing={missing}, extra={extra}"
            )
        id_column = _DECISION_ID_COLUMN[name]
        predecessor_column = _PREDECESSOR_COLUMN[name]
        if (
            claims[id_column] != self.decision_id
            or claims["decision_version"] != self.decision_version
            or claims[predecessor_column] != self.supersedes_decision_id
        ):
            raise RegistryWorkflowError("candidate decision IDs differ from frozen row claims")
        normalized = _json_value(claims)
        assert isinstance(normalized, dict)
        object.__setattr__(self, "frozen_row_claims", MappingProxyType(normalized))
        _validate_scope_projection(name, normalized, self.source_scope)
        _validate_registry_responsibility(name, normalized, self.source_scope)
        _validate_relation_derived_ids(name, normalized, self.source_scope, self.decision_id)

    @property
    def intent_digest(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "case_key": self.case_key,
            "decision_id": self.decision_id,
            "decision_version": self.decision_version,
            "frozen_row_claims": dict(self.frozen_row_claims),
            "registry_name": self.registry_name,
            "source_scope": self.source_scope.to_dict(),
            "supersedes_decision_id": self.supersedes_decision_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"intent_digest": self.intent_digest, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> RegistryDecisionCandidate:
        item = _mapping(value, "registry decision candidate")
        _expect_keys(
            item,
            {
                "case_key",
                "decision_id",
                "decision_version",
                "frozen_row_claims",
                "intent_digest",
                "registry_name",
                "source_scope",
                "supersedes_decision_id",
            },
            "registry decision candidate",
        )
        candidate = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            case_key=_text(item["case_key"], "case key"),
            decision_id=_digest(item["decision_id"], "decision ID"),
            decision_version=_positive(item["decision_version"], "decision version"),
            supersedes_decision_id=_optional_digest(
                item["supersedes_decision_id"], "superseded decision ID"
            ),
            frozen_row_claims=_mapping(item["frozen_row_claims"], "frozen row claims"),
            source_scope=ExactSourceScope.from_dict(item["source_scope"]),
        )
        if item["intent_digest"] != candidate.intent_digest:
            raise RegistryWorkflowError("candidate intent digest changed")
        return candidate


@dataclass(frozen=True, slots=True)
class RegistryCandidateManifest:
    registry_name: str
    contract_pin: RegistryContractPin
    source_artifacts: tuple[ExactArtifactBinding, ...]
    evidence_artifacts: tuple[ExactArtifactBinding, ...]
    authorization_artifacts: tuple[ExactArtifactBinding, ...]
    availability_calendar_id: str
    availability_calendar_sha256: str
    created_at_utc: datetime
    candidate_available_session: date
    decisions: tuple[RegistryDecisionCandidate, ...]
    production_ingress_artifact: ExactArtifactBinding | None = None
    state: str = AWAITING_REVIEW

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        if self.contract_pin.registry_name != name:
            raise RegistryWorkflowError("candidate contract registry differs")
        _require_current_contract_pin(self.contract_pin)
        sources = _sorted_unique_artifacts(self.source_artifacts, "source")
        evidence = _sorted_unique_artifacts(self.evidence_artifacts, "evidence")
        authorizations = _sorted_unique_artifacts(self.authorization_artifacts, "authorization")
        object.__setattr__(self, "source_artifacts", sources)
        object.__setattr__(self, "evidence_artifacts", evidence)
        object.__setattr__(self, "authorization_artifacts", authorizations)
        if self.production_ingress_artifact is not None:
            if (
                self.production_ingress_artifact.role != "production_ingress_attestation"
                or self.production_ingress_artifact.embedded_id_field != "attestation_id"
            ):
                raise RegistryWorkflowError("production ingress artifact binding changed")
            if self.production_ingress_artifact in (
                *sources,
                *evidence,
                *authorizations,
            ):
                raise RegistryWorkflowError("production ingress artifact is duplicated")
        if {item.role for item in authorizations} != REQUIRED_CANDIDATE_AUTHORIZATION_ROLES or len(
            authorizations
        ) != len(REQUIRED_CANDIDATE_AUTHORIZATION_ROLES):
            raise RegistryWorkflowError(
                "candidate lacks exact schema/source/evidence approval artifacts"
            )
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        _utc(self.created_at_utc, "candidate creation time")
        _date(self.candidate_available_session, "candidate availability")
        upstream_sessions = [
            item.available_session
            for item in (
                *self.source_artifacts,
                *self.evidence_artifacts,
                *self.authorization_artifacts,
                *(
                    ()
                    if self.production_ingress_artifact is None
                    else (self.production_ingress_artifact,)
                ),
            )
        ]
        if upstream_sessions and self.candidate_available_session < max(upstream_sessions):
            raise RegistryWorkflowError("candidate availability precedes an upstream artifact")
        decisions = tuple(sorted(self.decisions, key=lambda item: item.decision_id))
        if decisions != self.decisions:
            raise RegistryWorkflowError("candidate decisions must be sorted")
        if len({item.decision_id for item in decisions}) != len(decisions):
            raise RegistryWorkflowError("candidate decision IDs repeat")
        if len({item.case_key for item in decisions}) != len(decisions):
            raise RegistryWorkflowError("candidate case keys repeat")
        if any(item.registry_name != name for item in decisions):
            raise RegistryWorkflowError("candidate mixes registries")
        _validate_candidate_chains(name, decisions)
        _validate_candidate_artifact_claims(self)
        if self.state != AWAITING_REVIEW:
            raise RegistryWorkflowError("new registry candidate must await review")

    @property
    def source_scope_set_digest(self) -> str:
        return stable_digest(
            [
                {
                    "decision_id": item.decision_id,
                    "scope_digest": item.source_scope.scope_digest,
                }
                for item in self.decisions
            ]
        )

    @property
    def candidate_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def candidate_scope_slot_id(self) -> str:
        """Stable lane excluding retry-time and reviewer receipt metadata."""

        return stable_digest(
            {
                "availability_calendar_id": self.availability_calendar_id,
                "availability_calendar_sha256": self.availability_calendar_sha256,
                "contract_pin": self.contract_pin.to_dict(),
                "decisions": [item.to_dict() for item in self.decisions],
                "evidence_artifacts": [item.to_dict() for item in self.evidence_artifacts],
                "policy_version": WORKFLOW_POLICY,
                "production_ingress_artifact": (
                    None
                    if self.production_ingress_artifact is None
                    else self.production_ingress_artifact.to_dict()
                ),
                "registry_name": self.registry_name,
                "source_artifacts": [item.to_dict() for item in self.source_artifacts],
                "source_scope_set_digest": self.source_scope_set_digest,
            }
        )

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/candidate-slots/"
            f"candidate_scope_slot_id={self.candidate_scope_slot_id}/"
            "manifest.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "authorization_artifacts": [item.to_dict() for item in self.authorization_artifacts],
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate_available_session": self.candidate_available_session.isoformat(),
            "candidate_scope_slot_id": self.candidate_scope_slot_id,
            "contract_pin": self.contract_pin.to_dict(),
            "created_at_utc": _utc_text(self.created_at_utc),
            "decisions": [item.to_dict() for item in self.decisions],
            "evidence_artifacts": [item.to_dict() for item in self.evidence_artifacts],
            "policy_version": WORKFLOW_POLICY,
            "production_ingress_artifact": (
                None
                if self.production_ingress_artifact is None
                else self.production_ingress_artifact.to_dict()
            ),
            "registry_name": self.registry_name,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "source_scope_set_digest": self.source_scope_set_digest,
            "state": self.state,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_manifest_version": WORKFLOW_VERSION,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryCandidateManifest:
        item = _mapping(value, "registry candidate manifest")
        expected = {
            "authorization_artifacts",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate_available_session",
            "candidate_id",
            "candidate_manifest_version",
            "candidate_scope_slot_id",
            "contract_pin",
            "created_at_utc",
            "decisions",
            "evidence_artifacts",
            "policy_version",
            "production_ingress_artifact",
            "registry_name",
            "source_artifacts",
            "source_scope_set_digest",
            "state",
        }
        _expect_keys(item, expected, "registry candidate manifest")
        if item["candidate_manifest_version"] != WORKFLOW_VERSION:
            raise RegistryWorkflowError("unsupported registry candidate version")
        if item["policy_version"] != WORKFLOW_POLICY:
            raise RegistryWorkflowError("registry candidate policy changed")
        candidate = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            contract_pin=RegistryContractPin.from_dict(item["contract_pin"]),
            source_artifacts=tuple(
                ExactArtifactBinding.from_dict(row) for row in _array(item["source_artifacts"])
            ),
            evidence_artifacts=tuple(
                ExactArtifactBinding.from_dict(row) for row in _array(item["evidence_artifacts"])
            ),
            authorization_artifacts=tuple(
                ExactArtifactBinding.from_dict(row)
                for row in _array(item["authorization_artifacts"])
            ),
            availability_calendar_id=_digest(
                item["availability_calendar_id"], "availability calendar ID"
            ),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "availability calendar SHA-256"
            ),
            created_at_utc=_parse_utc(_text(item["created_at_utc"], "creation time")),
            candidate_available_session=date.fromisoformat(
                _text(item["candidate_available_session"], "candidate availability")
            ),
            decisions=tuple(
                RegistryDecisionCandidate.from_dict(row) for row in _array(item["decisions"])
            ),
            production_ingress_artifact=(
                None
                if item["production_ingress_artifact"] is None
                else ExactArtifactBinding.from_dict(item["production_ingress_artifact"])
            ),
            state=_text(item["state"], "candidate state"),
        )
        if item["source_scope_set_digest"] != candidate.source_scope_set_digest:
            raise RegistryWorkflowError("candidate source-scope-set digest changed")
        if item["candidate_scope_slot_id"] != candidate.candidate_scope_slot_id:
            raise RegistryWorkflowError("candidate scope slot ID recomputation failed")
        if item["candidate_id"] != candidate.candidate_id:
            raise RegistryWorkflowError("candidate ID recomputation failed")
        return candidate


@dataclass(frozen=True, slots=True)
class StoredControlDocument:
    object_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _digest(self.object_id, "control object ID")
        _relative(self.path, "control path")
        _digest(self.sha256, "control SHA-256")
        _positive(self.bytes, "control bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "object_id": self.object_id,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> StoredControlDocument:
        item = _mapping(value, "stored control document")
        _expect_keys(item, {"bytes", "object_id", "path", "sha256"}, "control ref")
        return cls(
            object_id=_digest(item["object_id"], "control object ID"),
            path=_relative(item["path"], "control path"),
            sha256=_digest(item["sha256"], "control SHA-256"),
            bytes=_positive(item["bytes"], "control bytes"),
        )


@dataclass(frozen=True, slots=True, order=True)
class RuntimeFilePin:
    path: str
    git_mode: str
    git_blob_id: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative(self.path, "runtime source path")
        if self.git_mode not in {"100644", "100755"}:
            raise RegistryWorkflowError("runtime source Git mode is unsupported")
        _git_object(self.git_blob_id, "runtime source Git blob ID")
        _digest(self.sha256, "runtime source SHA-256")
        _positive(self.bytes, "runtime source bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "git_blob_id": self.git_blob_id,
            "git_mode": self.git_mode,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> RuntimeFilePin:
        item = _mapping(value, "runtime file pin")
        _expect_keys(
            item,
            {"bytes", "git_blob_id", "git_mode", "path", "sha256"},
            "runtime file pin",
        )
        return cls(
            path=_relative(item["path"], "runtime source path"),
            git_mode=_text(item["git_mode"], "runtime source Git mode"),
            git_blob_id=_git_object(item["git_blob_id"], "runtime source Git blob ID"),
            sha256=_digest(item["sha256"], "runtime source SHA-256"),
            bytes=_positive(item["bytes"], "runtime source bytes"),
        )


@dataclass(frozen=True, slots=True)
class RegistryRuntimeBinding:
    git_commit: str
    git_tree: str
    files: tuple[RuntimeFilePin, ...]
    python_implementation: str
    python_version: str
    pyarrow_version: str
    repository_clean: bool = True

    def __post_init__(self) -> None:
        _git_object(self.git_commit, "runtime Git commit")
        _git_object(self.git_tree, "runtime Git tree")
        files = tuple(sorted(self.files, key=lambda item: item.path))
        if files != self.files or len({item.path for item in files}) != len(files):
            raise RegistryWorkflowError("runtime file pins must be sorted and unique")
        if tuple(item.path for item in files) != tuple(sorted(RUNTIME_BINDING_PATHS)):
            raise RegistryWorkflowError("runtime file pin coverage changed")
        _text(self.python_implementation, "Python implementation")
        _text(self.python_version, "Python version")
        _text(self.pyarrow_version, "pyarrow version")
        if self.repository_clean is not True:
            raise RegistryWorkflowError("runtime repository must be clean")

    @property
    def runtime_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "files": [item.to_dict() for item in self.files],
            "git_commit": self.git_commit,
            "git_tree": self.git_tree,
            "pyarrow_version": self.pyarrow_version,
            "python_implementation": self.python_implementation,
            "python_version": self.python_version,
            "repository_clean": self.repository_clean,
            "runtime_binding_version": WORKFLOW_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"runtime_binding_id": self.runtime_binding_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> RegistryRuntimeBinding:
        item = _mapping(value, "registry runtime binding")
        _expect_keys(
            item,
            {
                "files",
                "git_commit",
                "git_tree",
                "pyarrow_version",
                "python_implementation",
                "python_version",
                "repository_clean",
                "runtime_binding_id",
                "runtime_binding_version",
            },
            "registry runtime binding",
        )
        if item["runtime_binding_version"] != WORKFLOW_VERSION:
            raise RegistryWorkflowError("runtime binding version changed")
        binding = cls(
            git_commit=_git_object(item["git_commit"], "runtime Git commit"),
            git_tree=_git_object(item["git_tree"], "runtime Git tree"),
            files=tuple(RuntimeFilePin.from_dict(row) for row in _array(item["files"])),
            python_implementation=_text(item["python_implementation"], "Python implementation"),
            python_version=_text(item["python_version"], "Python version"),
            pyarrow_version=_text(item["pyarrow_version"], "pyarrow version"),
            repository_clean=item["repository_clean"] is True,
        )
        if item["runtime_binding_id"] != binding.runtime_binding_id:
            raise RegistryWorkflowError("runtime binding ID recomputation failed")
        return binding


def record_production_prerequisite_authorization(
    data_root: Path,
    *,
    authorization_type: str,
    registry_name: str,
    target_refs: Sequence[tuple[str, str]],
    availability_calendar_id: str,
    availability_calendar_sha256: str,
    standing_authorization_literal: bytes,
    reaffirmation_literal: bytes,
    approved_by: str,
) -> ExactArtifactBinding:
    """Record one production-only, exact-target prerequisite authorization.

    Unlike the legacy fixture receipt, this artifact is rooted in the canonical
    production namespace, binds the current clean runtime and exact calendar, and
    has its own artifact type/version.  Production candidate ingress rejects every
    older prerequisite receipt shape.
    """

    root = data_root.expanduser().resolve()
    if not is_canonical_production_data_root(root):
        raise RegistryWorkflowError(
            "production prerequisite authorization requires the canonical production root"
        )
    if authorization_type not in REQUIRED_CANDIDATE_AUTHORIZATION_ROLES:
        raise RegistryWorkflowError("unsupported candidate authorization type")
    name = _registry(registry_name)
    targets = tuple(sorted(target_refs))
    if not targets or len(set(targets)) != len(targets):
        raise RegistryWorkflowError("production authorization targets must be nonempty and unique")
    for artifact_id, sha256 in targets:
        _digest(artifact_id, "production authorization target ID")
        _digest(sha256, "production authorization target SHA-256")
    if standing_authorization_literal != STANDING_AUTHORIZATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing authorization literal bytes changed")
    if reaffirmation_literal != STANDING_REAFFIRMATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing reaffirmation literal bytes changed")
    runtime_binding = capture_registry_runtime_binding()
    calendar = _load_calendar(root, availability_calendar_id, availability_calendar_sha256)
    slot_id = _production_prerequisite_authorization_slot_id(
        name,
        authorization_type,
        targets,
        availability_calendar_id,
        availability_calendar_sha256,
        runtime_binding_id=runtime_binding.runtime_binding_id,
    )
    relative_path = _production_prerequisite_authorization_path(name, authorization_type, slot_id)
    existing = _load_existing_production_prerequisite_authorization(
        root,
        relative_path,
        expected_registry_name=name,
        expected_authorization_type=authorization_type,
        expected_targets=targets,
        expected_calendar_id=availability_calendar_id,
        expected_calendar_sha256=availability_calendar_sha256,
        expected_runtime_binding=runtime_binding,
    )
    if existing is not None:
        document, binding = existing
        if document["approved_by"] != approved_by:
            raise RegistryWorkflowError(
                "production prerequisite authorization slot is already bound differently"
            )
        return binding
    approved_at = _runtime_utc_now()
    try:
        approval_available, _ = calendar.first_open_after(approved_at)
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    _require_current_runtime_binding(runtime_binding)
    logical: dict[str, object] = {
        "approval_available_session": approval_available.isoformat(),
        "approved_at_utc": _utc_text(approved_at),
        "approved_by": _text(approved_by, "production authorization actor"),
        "artifact_type": PRODUCTION_PREREQUISITE_AUTHORIZATION_TYPE,
        "artifact_version": PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION,
        "authorization_slot_id": slot_id,
        "authorization_type": authorization_type,
        "availability_calendar_id": availability_calendar_id,
        "availability_calendar_sha256": availability_calendar_sha256,
        "capabilities": dict(STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES),
        "decision": APPROVED,
        "policy_version": WORKFLOW_POLICY,
        "production_data_root": root.as_posix(),
        "reaffirmation_literal": STANDING_REAFFIRMATION_LITERAL,
        "reaffirmation_literal_sha256": _utf8_sha256(STANDING_REAFFIRMATION_LITERAL),
        "registry_name": name,
        "runtime_binding": runtime_binding.to_dict(),
        "standing_authorization_action": STANDING_AUTHORIZATION_ACTION,
        "standing_authorization_literal": STANDING_AUTHORIZATION_LITERAL,
        "standing_authorization_literal_sha256": _utf8_sha256(STANDING_AUTHORIZATION_LITERAL),
        "target_refs": [
            {"artifact_id": artifact_id, "sha256": sha256} for artifact_id, sha256 in targets
        ],
    }
    document = {"authorization_id": stable_digest(logical), **logical}
    try:
        stored = _store_control(
            root,
            str(document["authorization_id"]),
            relative_path,
            document,
        )
    except ArtifactError:
        raced = _load_existing_production_prerequisite_authorization(
            root,
            relative_path,
            expected_registry_name=name,
            expected_authorization_type=authorization_type,
            expected_targets=targets,
            expected_calendar_id=availability_calendar_id,
            expected_calendar_sha256=availability_calendar_sha256,
            expected_runtime_binding=runtime_binding,
        )
        if raced is None:
            raise
        raced_document, raced_binding = raced
        if raced_document["approved_by"] != approved_by:
            raise RegistryWorkflowError(
                "concurrent production prerequisite authorization bound the slot differently"
            ) from None
        return raced_binding
    written_binding = ExactArtifactBinding(
        role=authorization_type,
        artifact_id=stored.object_id,
        path=stored.path,
        sha256=stored.sha256,
        bytes=stored.bytes,
        available_session=approval_available,
        embedded_id_field="authorization_id",
    )
    replayed = _load_existing_production_prerequisite_authorization(
        root,
        relative_path,
        expected_registry_name=name,
        expected_authorization_type=authorization_type,
        expected_targets=targets,
        expected_calendar_id=availability_calendar_id,
        expected_calendar_sha256=availability_calendar_sha256,
        expected_runtime_binding=runtime_binding,
    )
    if replayed is None or replayed[1] != written_binding:
        raise RegistryWorkflowError(
            "production prerequisite authorization failed post-write replay"
        )
    return replayed[1]


def record_standing_candidate_authorization(
    data_root: Path,
    *,
    authorization_type: str,
    registry_name: str,
    target_refs: Sequence[tuple[str, str]],
    availability_calendar_id: str,
    availability_calendar_sha256: str,
    standing_authorization_literal: bytes,
    reaffirmation_literal: bytes,
    approved_by: str,
) -> ExactArtifactBinding:
    """Record one prerequisite approval in a target-keyed immutable slot."""

    root = data_root.expanduser().resolve()
    require_fixture_registry_root(root)
    if authorization_type not in REQUIRED_CANDIDATE_AUTHORIZATION_ROLES:
        raise RegistryWorkflowError("unsupported candidate authorization type")
    name = _registry(registry_name)
    targets = tuple(sorted(target_refs))
    if not targets or len(set(targets)) != len(targets):
        raise RegistryWorkflowError("standing authorization targets must be nonempty and unique")
    for artifact_id, sha256 in targets:
        _digest(artifact_id, "standing authorization target ID")
        _digest(sha256, "standing authorization target SHA-256")
    if standing_authorization_literal != STANDING_AUTHORIZATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing authorization literal bytes changed")
    if reaffirmation_literal != STANDING_REAFFIRMATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing reaffirmation literal bytes changed")
    _digest(availability_calendar_id, "authorization calendar ID")
    _digest(availability_calendar_sha256, "authorization calendar SHA-256")
    calendar = _load_calendar(root, availability_calendar_id, availability_calendar_sha256)
    slot_id = _standing_candidate_authorization_slot_id(name, authorization_type, targets)
    relative_path = _standing_candidate_authorization_path(name, authorization_type, slot_id)
    existing = _load_existing_standing_candidate_authorization(
        root,
        relative_path,
        expected_registry_name=name,
        expected_authorization_type=authorization_type,
        expected_targets=targets,
        expected_calendar_id=availability_calendar_id,
        expected_calendar_sha256=availability_calendar_sha256,
    )
    if existing is not None:
        document, binding = existing
        if document["approved_by"] != approved_by:
            raise RegistryWorkflowError(
                "standing candidate authorization slot is already bound differently"
            )
        return binding
    approved_at = _runtime_utc_now()
    try:
        approval_available, _ = calendar.first_open_after(approved_at)
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    runtime_binding = capture_registry_runtime_binding()
    logical: dict[str, object] = {
        "approval_available_session": approval_available.isoformat(),
        "approved_at_utc": _utc_text(approved_at),
        "approved_by": _text(approved_by, "candidate authorization actor"),
        "authorization_mode": STANDING_AUTHORIZATION_VERSION,
        "authorization_slot_id": slot_id,
        "authorization_type": authorization_type,
        "availability_calendar_id": availability_calendar_id,
        "availability_calendar_sha256": availability_calendar_sha256,
        "capabilities": dict(STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES),
        "decision": APPROVED,
        "policy_version": WORKFLOW_POLICY,
        "reaffirmation_literal": STANDING_REAFFIRMATION_LITERAL,
        "reaffirmation_literal_sha256": _utf8_sha256(STANDING_REAFFIRMATION_LITERAL),
        "registry_name": name,
        "runtime_binding": runtime_binding.to_dict(),
        "standing_authorization_action": STANDING_AUTHORIZATION_ACTION,
        "standing_authorization_literal": STANDING_AUTHORIZATION_LITERAL,
        "standing_authorization_literal_sha256": _utf8_sha256(STANDING_AUTHORIZATION_LITERAL),
        "target_refs": [
            {"artifact_id": artifact_id, "sha256": sha256} for artifact_id, sha256 in targets
        ],
    }
    document = {"authorization_id": stable_digest(logical), **logical}
    try:
        stored = _store_control(
            root,
            str(document["authorization_id"]),
            relative_path,
            document,
        )
    except ArtifactError:
        raced = _load_existing_standing_candidate_authorization(
            root,
            relative_path,
            expected_registry_name=name,
            expected_authorization_type=authorization_type,
            expected_targets=targets,
            expected_calendar_id=availability_calendar_id,
            expected_calendar_sha256=availability_calendar_sha256,
        )
        if raced is None:
            raise
        raced_document, raced_binding = raced
        if raced_document["approved_by"] != approved_by:
            raise RegistryWorkflowError(
                "concurrent candidate authorization bound the slot differently"
            ) from None
        return raced_binding
    return ExactArtifactBinding(
        role=authorization_type,
        artifact_id=stored.object_id,
        path=stored.path,
        sha256=stored.sha256,
        bytes=stored.bytes,
        available_session=approval_available,
        embedded_id_field="authorization_id",
    )


@dataclass(frozen=True, slots=True)
class RegistryDecisionPlan:
    registry_name: str
    contract_pin: RegistryContractPin
    candidate: StoredControlDocument
    decision_ids: tuple[str, ...]
    source_scope_set_digest: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    state: str = AWAITING_REVIEW

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        if self.contract_pin.registry_name != name:
            raise RegistryWorkflowError("decision plan contract registry differs")
        _require_current_contract_pin(self.contract_pin)
        decision_ids = tuple(sorted(self.decision_ids))
        if decision_ids != self.decision_ids or len(set(decision_ids)) != len(decision_ids):
            raise RegistryWorkflowError("decision plan IDs must be sorted and unique")
        for decision_id in decision_ids:
            _digest(decision_id, "planned decision ID")
        _digest(self.source_scope_set_digest, "source scope set digest")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        if self.state != AWAITING_REVIEW:
            raise RegistryWorkflowError("decision plan must await review")

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/plans/plan_id={self.plan_id}/plan.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "candidate": self.candidate.to_dict(),
            "contract_pin": self.contract_pin.to_dict(),
            "decision_ids": list(self.decision_ids),
            "policy_version": WORKFLOW_POLICY,
            "registry_name": self.registry_name,
            "release_authorized": False,
            "source_scope_set_digest": self.source_scope_set_digest,
            "state": self.state,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_plan_version": WORKFLOW_VERSION,
            "plan_id": self.plan_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryDecisionPlan:
        item = _mapping(value, "registry decision plan")
        expected = {
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate",
            "contract_pin",
            "decision_ids",
            "decision_plan_version",
            "plan_id",
            "policy_version",
            "registry_name",
            "release_authorized",
            "source_scope_set_digest",
            "state",
        }
        _expect_keys(item, expected, "registry decision plan")
        if (
            item["decision_plan_version"] != WORKFLOW_VERSION
            or item["policy_version"] != WORKFLOW_POLICY
            or item["release_authorized"] is not False
        ):
            raise RegistryWorkflowError("decision plan control flags changed")
        plan = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            contract_pin=RegistryContractPin.from_dict(item["contract_pin"]),
            candidate=StoredControlDocument.from_dict(item["candidate"]),
            decision_ids=tuple(_digest(row, "decision ID") for row in _array(item["decision_ids"])),
            source_scope_set_digest=_digest(
                item["source_scope_set_digest"], "source scope set digest"
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
            state=_text(item["state"], "plan state"),
        )
        if item["plan_id"] != plan.plan_id:
            raise RegistryWorkflowError("decision plan ID recomputation failed")
        return plan


@dataclass(frozen=True, slots=True)
class RegistryApprovalRequest:
    registry_name: str
    contract_pin: RegistryContractPin
    plan: StoredControlDocument
    candidate: StoredControlDocument
    decision_ids: tuple[str, ...]
    source_scope_set_digest: str
    availability_calendar_id: str
    availability_calendar_sha256: str

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        if self.contract_pin.registry_name != name:
            raise RegistryWorkflowError("approval request contract registry differs")
        _require_current_contract_pin(self.contract_pin)
        ids = tuple(sorted(self.decision_ids))
        if ids != self.decision_ids or len(set(ids)) != len(ids):
            raise RegistryWorkflowError("approval request decision IDs changed")
        for item in ids:
            _digest(item, "approval request decision ID")
        _digest(self.source_scope_set_digest, "source scope set digest")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")

    @property
    def request_event_id(self) -> str:
        return stable_digest(self.literal_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/approval-requests/"
            f"request_event_id={self.request_event_id}/request.json"
        )

    def literal_payload(self) -> dict[str, object]:
        """The exact object a reviewer must approve; no surrounding prose is admitted."""

        return {
            "authorized_action": APPROVAL_ACTION,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate_id": self.candidate.object_id,
            "candidate_manifest_bytes": self.candidate.bytes,
            "candidate_manifest_path": self.candidate.path,
            "candidate_manifest_sha256": self.candidate.sha256,
            "contract_id": self.contract_pin.contract_id,
            "contract_resource_sha256": self.contract_pin.resource_sha256,
            "contract_schema_digest": self.contract_pin.schema_digest,
            "decision_ids": list(self.decision_ids),
            "literal_version": APPROVAL_LITERAL_VERSION,
            "plan_bytes": self.plan.bytes,
            "plan_id": self.plan.object_id,
            "plan_path": self.plan.path,
            "plan_sha256": self.plan.sha256,
            "registry_name": self.registry_name,
            "source_scope_set_digest": self.source_scope_set_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "approval_request_version": WORKFLOW_VERSION,
            "request_event_id": self.request_event_id,
            "request_event_sha256": hashlib.sha256(self.literal_bytes()).hexdigest(),
            **self.literal_payload(),
        }

    def literal_bytes(self) -> bytes:
        return _canonical_bytes(self.literal_payload())

    @classmethod
    def from_dict(cls, value: object) -> RegistryApprovalRequest:
        item = _mapping(value, "registry approval request")
        literal_keys = {
            "authorized_action",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate_id",
            "candidate_manifest_bytes",
            "candidate_manifest_path",
            "candidate_manifest_sha256",
            "contract_id",
            "contract_resource_sha256",
            "contract_schema_digest",
            "decision_ids",
            "literal_version",
            "plan_bytes",
            "plan_id",
            "plan_path",
            "plan_sha256",
            "registry_name",
            "source_scope_set_digest",
        }
        _expect_keys(
            item,
            literal_keys | {"approval_request_version", "request_event_id", "request_event_sha256"},
            "registry approval request",
        )
        if (
            item["approval_request_version"] != WORKFLOW_VERSION
            or item["authorized_action"] != APPROVAL_ACTION
            or item["literal_version"] != APPROVAL_LITERAL_VERSION
        ):
            raise RegistryWorkflowError("approval request action/version changed")
        request = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            contract_pin=RegistryContractPin(
                registry_name=_text(item["registry_name"], "registry name"),
                contract_id=_digest(item["contract_id"], "contract ID"),
                schema_digest=_digest(item["contract_schema_digest"], "schema digest"),
                resource_sha256=_digest(item["contract_resource_sha256"], "resource SHA-256"),
            ),
            plan=StoredControlDocument(
                object_id=_digest(item["plan_id"], "plan ID"),
                path=_relative(item["plan_path"], "plan path"),
                sha256=_digest(item["plan_sha256"], "plan SHA-256"),
                bytes=_positive(item["plan_bytes"], "plan bytes"),
            ),
            candidate=StoredControlDocument(
                object_id=_digest(item["candidate_id"], "candidate ID"),
                path=_relative(item["candidate_manifest_path"], "candidate path"),
                sha256=_digest(item["candidate_manifest_sha256"], "candidate SHA-256"),
                bytes=_positive(item["candidate_manifest_bytes"], "candidate bytes"),
            ),
            decision_ids=tuple(_digest(row, "decision ID") for row in _array(item["decision_ids"])),
            source_scope_set_digest=_digest(
                item["source_scope_set_digest"], "source scope set digest"
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
        )
        if item["request_event_id"] != request.request_event_id:
            raise RegistryWorkflowError("approval request event ID changed")
        if item["request_event_sha256"] != hashlib.sha256(request.literal_bytes()).hexdigest():
            raise RegistryWorkflowError("approval request literal SHA-256 changed")
        return request


@dataclass(frozen=True, slots=True)
class RegistryApprovalReceipt:
    registry_name: str
    request: StoredControlDocument
    request_event_id: str
    plan_id: str
    candidate_id: str
    decision_ids: tuple[str, ...]
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    runtime_binding: RegistryRuntimeBinding
    decision: str = APPROVED

    def __post_init__(self) -> None:
        _registry(self.registry_name)
        _digest(self.request_event_id, "request event ID")
        _digest(self.plan_id, "plan ID")
        _digest(self.candidate_id, "candidate ID")
        ids = tuple(sorted(self.decision_ids))
        if ids != self.decision_ids or len(set(ids)) != len(ids):
            raise RegistryWorkflowError("approval receipt decision IDs changed")
        for item in ids:
            _digest(item, "approved decision ID")
        _text(self.approved_by, "approval actor")
        _utc(self.approved_at_utc, "approval time")
        _date(self.approval_available_session, "approval availability")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        if self.decision != APPROVED:
            raise RegistryWorkflowError("only an approved receipt can authorize a release")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/approval-receipts/"
            f"receipt_id={self.receipt_id}/receipt.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_available_session": self.approval_available_session.isoformat(),
            "approved_at_utc": _utc_text(self.approved_at_utc),
            "approved_by": self.approved_by,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate_id": self.candidate_id,
            "decision": self.decision,
            "decision_ids": list(self.decision_ids),
            "plan_id": self.plan_id,
            "policy_version": WORKFLOW_POLICY,
            "registry_name": self.registry_name,
            "request": self.request.to_dict(),
            "request_event_id": self.request_event_id,
            "runtime_binding": self.runtime_binding.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "approval_receipt_version": WORKFLOW_VERSION,
            "receipt_id": self.receipt_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryApprovalReceipt:
        item = _mapping(value, "registry approval receipt")
        expected = {
            "approval_available_session",
            "approval_receipt_version",
            "approved_at_utc",
            "approved_by",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate_id",
            "decision",
            "decision_ids",
            "plan_id",
            "policy_version",
            "receipt_id",
            "registry_name",
            "request",
            "request_event_id",
            "runtime_binding",
        }
        _expect_keys(item, expected, "registry approval receipt")
        if (
            item["approval_receipt_version"] != WORKFLOW_VERSION
            or item["policy_version"] != WORKFLOW_POLICY
        ):
            raise RegistryWorkflowError("approval receipt version/policy changed")
        receipt = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            request=StoredControlDocument.from_dict(item["request"]),
            request_event_id=_digest(item["request_event_id"], "request event ID"),
            plan_id=_digest(item["plan_id"], "plan ID"),
            candidate_id=_digest(item["candidate_id"], "candidate ID"),
            decision_ids=tuple(_digest(row, "decision ID") for row in _array(item["decision_ids"])),
            approved_by=_text(item["approved_by"], "approval actor"),
            approved_at_utc=_parse_utc(_text(item["approved_at_utc"], "approval time")),
            approval_available_session=date.fromisoformat(
                _text(item["approval_available_session"], "approval availability")
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
            runtime_binding=RegistryRuntimeBinding.from_dict(item["runtime_binding"]),
            decision=_text(item["decision"], "approval decision"),
        )
        if item["receipt_id"] != receipt.receipt_id:
            raise RegistryWorkflowError("approval receipt ID recomputation failed")
        return receipt


@dataclass(frozen=True, slots=True)
class RegistryStandingApprovalReceipt:
    """One-release receipt derived from the user's exact standing S7 authority.

    This receipt deliberately carries more replay material than an exact-literal
    receipt.  It preserves the two user utterances byte-for-byte, the internally
    derived request literal, every source/evidence pin, the contract pin and the
    machine review that admitted this one candidate.  Its capability map makes all
    neighboring S7 actions false.
    """

    registry_name: str
    request: StoredControlDocument
    plan: StoredControlDocument
    candidate: StoredControlDocument
    request_event_id: str
    decision_ids: tuple[str, ...]
    source_scope_set_digest: str
    contract_pin: RegistryContractPin
    source_artifacts: tuple[ExactArtifactBinding, ...]
    evidence_artifacts: tuple[ExactArtifactBinding, ...]
    candidate_authorization_artifacts: tuple[ExactArtifactBinding, ...]
    exact_request_literal: Mapping[str, object]
    exact_request_literal_sha256: str
    standing_authorization_literal: str
    standing_authorization_literal_sha256: str
    reaffirmation_literal: str
    reaffirmation_literal_sha256: str
    qa_review: Mapping[str, object]
    capabilities: Mapping[str, bool]
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    runtime_binding: RegistryRuntimeBinding
    decision: str = APPROVED
    authorization_mode: str = STANDING_AUTHORIZATION_VERSION

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        if self.contract_pin.registry_name != name:
            raise RegistryWorkflowError("standing receipt contract registry differs")
        _require_current_contract_pin(self.contract_pin)
        _digest(self.request_event_id, "request event ID")
        ids = tuple(sorted(self.decision_ids))
        if ids != self.decision_ids or len(set(ids)) != len(ids):
            raise RegistryWorkflowError("standing receipt decision IDs changed")
        for item in ids:
            _digest(item, "standing-approved decision ID")
        _digest(self.source_scope_set_digest, "standing source-scope-set digest")
        sources = _sorted_unique_artifacts(self.source_artifacts, "standing source")
        evidence = _sorted_unique_artifacts(self.evidence_artifacts, "standing evidence")
        authorizations = _sorted_unique_artifacts(
            self.candidate_authorization_artifacts,
            "standing candidate authorization",
        )
        object.__setattr__(self, "source_artifacts", sources)
        object.__setattr__(self, "evidence_artifacts", evidence)
        object.__setattr__(self, "candidate_authorization_artifacts", authorizations)

        request_literal = _json_value(dict(self.exact_request_literal))
        if not isinstance(request_literal, dict):  # pragma: no cover - defensive
            raise RegistryWorkflowError("standing exact request literal must be an object")
        request_literal_sha = hashlib.sha256(_canonical_bytes(request_literal)).hexdigest()
        if self.exact_request_literal_sha256 != request_literal_sha:
            raise RegistryWorkflowError("standing exact request literal SHA-256 changed")
        object.__setattr__(self, "exact_request_literal", MappingProxyType(request_literal))

        if (
            self.standing_authorization_literal != STANDING_AUTHORIZATION_LITERAL
            or self.reaffirmation_literal != STANDING_REAFFIRMATION_LITERAL
            or self.standing_authorization_literal_sha256
            != _utf8_sha256(self.standing_authorization_literal)
            or self.reaffirmation_literal_sha256 != _utf8_sha256(self.reaffirmation_literal)
        ):
            raise RegistryWorkflowError("standing authorization utterance binding changed")

        review = _json_value(dict(self.qa_review))
        if not isinstance(review, dict):  # pragma: no cover - defensive
            raise RegistryWorkflowError("standing QA review must be an object")
        _validate_standing_qa_review(review)
        object.__setattr__(self, "qa_review", MappingProxyType(review))
        capabilities = dict(self.capabilities)
        if capabilities != dict(STANDING_AUTHORIZATION_CAPABILITIES):
            raise RegistryWorkflowError("standing authorization capabilities broadened")
        object.__setattr__(self, "capabilities", MappingProxyType(capabilities))
        _text(self.approved_by, "standing approval actor")
        _utc(self.approved_at_utc, "standing approval time")
        _date(self.approval_available_session, "standing approval availability")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        if self.decision != APPROVED:
            raise RegistryWorkflowError("standing receipt must be approved")
        if self.authorization_mode != STANDING_AUTHORIZATION_VERSION:
            raise RegistryWorkflowError("standing authorization mode changed")

    @property
    def plan_id(self) -> str:
        return self.plan.object_id

    @property
    def candidate_id(self) -> str:
        return self.candidate.object_id

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def authorization_slot_id(self) -> str:
        return _authorization_lane_id(
            self.registry_name,
            self.candidate,
            self.plan,
            self.request,
        )

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/standing-approval-slots/"
            f"authorization_slot_id={self.authorization_slot_id}/receipt.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_available_session": self.approval_available_session.isoformat(),
            "approved_at_utc": _utc_text(self.approved_at_utc),
            "approved_by": self.approved_by,
            "authorization_slot_id": self.authorization_slot_id,
            "authorization_mode": self.authorization_mode,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate": self.candidate.to_dict(),
            "candidate_authorization_artifacts": [
                item.to_dict() for item in self.candidate_authorization_artifacts
            ],
            "capabilities": dict(self.capabilities),
            "contract_pin": self.contract_pin.to_dict(),
            "decision": self.decision,
            "decision_ids": list(self.decision_ids),
            "evidence_artifacts": [item.to_dict() for item in self.evidence_artifacts],
            "exact_request_literal": dict(self.exact_request_literal),
            "exact_request_literal_sha256": self.exact_request_literal_sha256,
            "plan": self.plan.to_dict(),
            "policy_version": WORKFLOW_POLICY,
            "qa_review": dict(self.qa_review),
            "reaffirmation_literal": self.reaffirmation_literal,
            "reaffirmation_literal_sha256": self.reaffirmation_literal_sha256,
            "registry_name": self.registry_name,
            "request": self.request.to_dict(),
            "request_event_id": self.request_event_id,
            "runtime_binding": self.runtime_binding.to_dict(),
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "source_scope_set_digest": self.source_scope_set_digest,
            "standing_authorization_action": STANDING_AUTHORIZATION_ACTION,
            "standing_authorization_literal": self.standing_authorization_literal,
            "standing_authorization_literal_sha256": (self.standing_authorization_literal_sha256),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            "standing_approval_receipt_version": WORKFLOW_VERSION,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryStandingApprovalReceipt:
        item = _mapping(value, "registry standing approval receipt")
        expected = {
            "approval_available_session",
            "approved_at_utc",
            "approved_by",
            "authorization_slot_id",
            "authorization_mode",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate",
            "candidate_authorization_artifacts",
            "capabilities",
            "contract_pin",
            "decision",
            "decision_ids",
            "evidence_artifacts",
            "exact_request_literal",
            "exact_request_literal_sha256",
            "plan",
            "policy_version",
            "qa_review",
            "reaffirmation_literal",
            "reaffirmation_literal_sha256",
            "receipt_id",
            "registry_name",
            "request",
            "request_event_id",
            "runtime_binding",
            "source_artifacts",
            "source_scope_set_digest",
            "standing_approval_receipt_version",
            "standing_authorization_action",
            "standing_authorization_literal",
            "standing_authorization_literal_sha256",
        }
        _expect_keys(item, expected, "registry standing approval receipt")
        if (
            item["standing_approval_receipt_version"] != WORKFLOW_VERSION
            or item["policy_version"] != WORKFLOW_POLICY
            or item["standing_authorization_action"] != STANDING_AUTHORIZATION_ACTION
        ):
            raise RegistryWorkflowError("standing receipt action/version changed")
        receipt = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            request=StoredControlDocument.from_dict(item["request"]),
            plan=StoredControlDocument.from_dict(item["plan"]),
            candidate=StoredControlDocument.from_dict(item["candidate"]),
            request_event_id=_digest(item["request_event_id"], "request event ID"),
            decision_ids=tuple(_digest(row, "decision ID") for row in _array(item["decision_ids"])),
            source_scope_set_digest=_digest(
                item["source_scope_set_digest"], "source-scope-set digest"
            ),
            contract_pin=RegistryContractPin.from_dict(item["contract_pin"]),
            source_artifacts=tuple(
                ExactArtifactBinding.from_dict(row) for row in _array(item["source_artifacts"])
            ),
            evidence_artifacts=tuple(
                ExactArtifactBinding.from_dict(row) for row in _array(item["evidence_artifacts"])
            ),
            candidate_authorization_artifacts=tuple(
                ExactArtifactBinding.from_dict(row)
                for row in _array(item["candidate_authorization_artifacts"])
            ),
            exact_request_literal=_mapping(item["exact_request_literal"], "exact request literal"),
            exact_request_literal_sha256=_digest(
                item["exact_request_literal_sha256"], "exact request literal SHA-256"
            ),
            standing_authorization_literal=_text(
                item["standing_authorization_literal"], "standing authorization literal"
            ),
            standing_authorization_literal_sha256=_digest(
                item["standing_authorization_literal_sha256"],
                "standing authorization literal SHA-256",
            ),
            reaffirmation_literal=_text(
                item["reaffirmation_literal"], "standing reaffirmation literal"
            ),
            reaffirmation_literal_sha256=_digest(
                item["reaffirmation_literal_sha256"],
                "standing reaffirmation literal SHA-256",
            ),
            qa_review=_mapping(item["qa_review"], "standing QA review"),
            capabilities={
                key: value
                for key, value in _mapping(item["capabilities"], "standing capabilities").items()
                if isinstance(value, bool)
            },
            approved_by=_text(item["approved_by"], "approval actor"),
            approved_at_utc=_parse_utc(_text(item["approved_at_utc"], "approval time")),
            approval_available_session=date.fromisoformat(
                _text(item["approval_available_session"], "approval availability")
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
            runtime_binding=RegistryRuntimeBinding.from_dict(item["runtime_binding"]),
            decision=_text(item["decision"], "approval decision"),
            authorization_mode=_text(item["authorization_mode"], "authorization mode"),
        )
        if len(receipt.capabilities) != len(
            _mapping(item["capabilities"], "standing capabilities")
        ):
            raise RegistryWorkflowError("standing capability values must be booleans")
        if item["authorization_slot_id"] != receipt.authorization_slot_id:
            raise RegistryWorkflowError("standing authorization slot ID changed")
        if item["receipt_id"] != receipt.receipt_id:
            raise RegistryWorkflowError("standing receipt ID recomputation failed")
        return receipt


RegistryReleaseAuthorizationReceipt = RegistryApprovalReceipt | RegistryStandingApprovalReceipt


@dataclass(frozen=True, slots=True)
class RegistryPublishIntent:
    """Durable at-most-once intent written before any release member."""

    registry_name: str
    release_lane_id: str
    candidate: StoredControlDocument
    plan: StoredControlDocument
    request: StoredControlDocument
    approval_receipt: StoredControlDocument
    decision_ids: tuple[str, ...]
    source_scope_set_digest: str
    decision_row_set_digest: str
    runtime_binding_id: str
    published_at_utc: datetime
    release_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    state: str = "release_intent_recorded"

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        expected_lane = _authorization_lane_id(
            name,
            self.candidate,
            self.plan,
            self.request,
        )
        if self.release_lane_id != expected_lane:
            raise RegistryWorkflowError("publish intent release lane changed")
        ids = tuple(sorted(self.decision_ids))
        if ids != self.decision_ids or len(set(ids)) != len(ids):
            raise RegistryWorkflowError("publish intent decision IDs changed")
        for item in ids:
            _digest(item, "publish intent decision ID")
        _digest(self.source_scope_set_digest, "publish intent source-scope-set digest")
        _digest(self.decision_row_set_digest, "publish intent row-set digest")
        _digest(self.runtime_binding_id, "publish intent runtime binding ID")
        _utc(self.published_at_utc, "publish intent time")
        _date(self.release_available_session, "publish intent availability")
        _digest(self.availability_calendar_id, "publish intent calendar ID")
        _digest(self.availability_calendar_sha256, "publish intent calendar SHA-256")
        if self.state != "release_intent_recorded":
            raise RegistryWorkflowError("publish intent state changed")

    @property
    def intent_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-workflow/"
            f"registry={self.registry_name}/publish-intents/"
            f"release_lane_id={self.release_lane_id}/intent.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_receipt": self.approval_receipt.to_dict(),
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate": self.candidate.to_dict(),
            "decision_ids": list(self.decision_ids),
            "decision_row_set_digest": self.decision_row_set_digest,
            "plan": self.plan.to_dict(),
            "policy_version": WORKFLOW_POLICY,
            "published_at_utc": _utc_text(self.published_at_utc),
            "registry_name": self.registry_name,
            "release_available_session": self.release_available_session.isoformat(),
            "release_lane_id": self.release_lane_id,
            "request": self.request.to_dict(),
            "runtime_binding_id": self.runtime_binding_id,
            "source_scope_set_digest": self.source_scope_set_digest,
            "state": self.state,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "publish_intent_version": WORKFLOW_VERSION,
            "intent_id": self.intent_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryPublishIntent:
        item = _mapping(value, "registry publish intent")
        expected = {
            "approval_receipt",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate",
            "decision_ids",
            "decision_row_set_digest",
            "intent_id",
            "plan",
            "policy_version",
            "publish_intent_version",
            "published_at_utc",
            "registry_name",
            "release_available_session",
            "release_lane_id",
            "request",
            "runtime_binding_id",
            "source_scope_set_digest",
            "state",
        }
        _expect_keys(item, expected, "registry publish intent")
        if (
            item["publish_intent_version"] != WORKFLOW_VERSION
            or item["policy_version"] != WORKFLOW_POLICY
        ):
            raise RegistryWorkflowError("publish intent version/policy changed")
        intent = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            release_lane_id=_digest(item["release_lane_id"], "release lane ID"),
            candidate=StoredControlDocument.from_dict(item["candidate"]),
            plan=StoredControlDocument.from_dict(item["plan"]),
            request=StoredControlDocument.from_dict(item["request"]),
            approval_receipt=StoredControlDocument.from_dict(item["approval_receipt"]),
            decision_ids=tuple(
                _digest(value, "publish intent decision ID")
                for value in _array(item["decision_ids"])
            ),
            source_scope_set_digest=_digest(
                item["source_scope_set_digest"], "source-scope-set digest"
            ),
            decision_row_set_digest=_digest(
                item["decision_row_set_digest"], "decision row-set digest"
            ),
            runtime_binding_id=_digest(item["runtime_binding_id"], "runtime binding ID"),
            published_at_utc=_parse_utc(_text(item["published_at_utc"], "publication time")),
            release_available_session=date.fromisoformat(
                _text(item["release_available_session"], "release availability")
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
            state=_text(item["state"], "publish intent state"),
        )
        if item["intent_id"] != intent.intent_id:
            raise RegistryWorkflowError("publish intent ID recomputation failed")
        return intent


@dataclass(frozen=True, slots=True)
class ReleasedDecisionArtifactRef:
    decision_id: str
    path: str
    sha256: str
    bytes: int
    available_session: date
    source_row_count: int
    source_record_set_digest: str
    source_scope_digest: str
    row_digest: str

    def __post_init__(self) -> None:
        _digest(self.decision_id, "released decision ID")
        _relative(self.path, "released decision path")
        _digest(self.sha256, "released decision SHA-256")
        _positive(self.bytes, "released decision bytes")
        _date(self.available_session, "released decision availability")
        _positive(self.source_row_count, "released decision source row count")
        _digest(self.source_record_set_digest, "source record-set digest")
        _digest(self.source_scope_digest, "source scope digest")
        _digest(self.row_digest, "decision row digest")

    def to_dict(self) -> dict[str, object]:
        return {
            "available_session": self.available_session.isoformat(),
            "bytes": self.bytes,
            "decision_id": self.decision_id,
            "path": self.path,
            "row_digest": self.row_digest,
            "sha256": self.sha256,
            "source_record_set_digest": self.source_record_set_digest,
            "source_row_count": self.source_row_count,
            "source_scope_digest": self.source_scope_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> ReleasedDecisionArtifactRef:
        item = _mapping(value, "released decision ref")
        expected = {
            "available_session",
            "bytes",
            "decision_id",
            "path",
            "row_digest",
            "sha256",
            "source_record_set_digest",
            "source_row_count",
            "source_scope_digest",
        }
        _expect_keys(item, expected, "released decision ref")
        return cls(
            decision_id=_digest(item["decision_id"], "decision ID"),
            path=_relative(item["path"], "decision path"),
            sha256=_digest(item["sha256"], "decision SHA-256"),
            bytes=_positive(item["bytes"], "decision bytes"),
            available_session=date.fromisoformat(
                _text(item["available_session"], "decision availability")
            ),
            source_row_count=_positive(item["source_row_count"], "source row count"),
            source_record_set_digest=_digest(
                item["source_record_set_digest"], "source record-set digest"
            ),
            source_scope_digest=_digest(item["source_scope_digest"], "source scope digest"),
            row_digest=_digest(item["row_digest"], "row digest"),
        )


@dataclass(frozen=True, slots=True)
class RegistryReleaseManifest:
    registry_name: str
    contract_pin: RegistryContractPin
    candidate: StoredControlDocument
    plan: StoredControlDocument
    request: StoredControlDocument
    approval_receipt: StoredControlDocument
    publish_intent: StoredControlDocument
    rows_path: str
    rows_sha256: str
    rows_bytes: int
    row_count: int
    decisions: tuple[ReleasedDecisionArtifactRef, ...]
    source_scope_set_digest: str
    published_at_utc: datetime
    release_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    production_ingress_artifact: ExactArtifactBinding | None = None
    state: str = PUBLISHED

    def __post_init__(self) -> None:
        name = _registry(self.registry_name)
        if self.contract_pin.registry_name != name:
            raise RegistryWorkflowError("release contract registry differs")
        _require_current_contract_pin(self.contract_pin)
        _relative(self.rows_path, "release rows path")
        _digest(self.rows_sha256, "release rows SHA-256")
        _positive(self.rows_bytes, "release rows bytes")
        _nonnegative(self.row_count, "release row count")
        decisions = tuple(sorted(self.decisions, key=lambda item: item.decision_id))
        if decisions != self.decisions or len({x.decision_id for x in decisions}) != len(decisions):
            raise RegistryWorkflowError("release decision refs must be sorted and unique")
        if self.row_count != len(decisions):
            raise RegistryWorkflowError("release row/decision counts differ")
        _digest(self.source_scope_set_digest, "release source scope set digest")
        _utc(self.published_at_utc, "release publication time")
        _date(self.release_available_session, "release availability")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        if decisions and self.release_available_session < max(
            item.available_session for item in decisions
        ):
            raise RegistryWorkflowError("release availability precedes a decision")
        if self.state != PUBLISHED:
            raise RegistryWorkflowError("registry release state must be published")

    @property
    def release_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/registry-releases/"
            f"registry={self.registry_name}/release_id={self.release_id}/manifest.json"
        )

    @property
    def release_directory(self) -> str:
        return self.relative_path.rsplit("/", 1)[0]

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_receipt": self.approval_receipt.to_dict(),
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "candidate": self.candidate.to_dict(),
            "contract_pin": self.contract_pin.to_dict(),
            "decisions": [item.to_dict() for item in self.decisions],
            "plan": self.plan.to_dict(),
            "policy_version": WORKFLOW_POLICY,
            "production_ingress_artifact": (
                None
                if self.production_ingress_artifact is None
                else self.production_ingress_artifact.to_dict()
            ),
            "publish_intent": self.publish_intent.to_dict(),
            "published_at_utc": _utc_text(self.published_at_utc),
            "registry_name": self.registry_name,
            "release_available_session": self.release_available_session.isoformat(),
            "request": self.request.to_dict(),
            "row_count": self.row_count,
            "rows_bytes": self.rows_bytes,
            "rows_path": self.rows_path,
            "rows_sha256": self.rows_sha256,
            "source_scope_set_digest": self.source_scope_set_digest,
            "state": self.state,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "registry_release_version": WORKFLOW_VERSION,
            "release_id": self.release_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryReleaseManifest:
        item = _mapping(value, "registry release manifest")
        expected = {
            "approval_receipt",
            "availability_calendar_id",
            "availability_calendar_sha256",
            "candidate",
            "contract_pin",
            "decisions",
            "plan",
            "policy_version",
            "production_ingress_artifact",
            "publish_intent",
            "published_at_utc",
            "registry_name",
            "registry_release_version",
            "release_available_session",
            "release_id",
            "request",
            "row_count",
            "rows_bytes",
            "rows_path",
            "rows_sha256",
            "source_scope_set_digest",
            "state",
        }
        _expect_keys(item, expected, "registry release manifest")
        if (
            item["registry_release_version"] != WORKFLOW_VERSION
            or item["policy_version"] != WORKFLOW_POLICY
        ):
            raise RegistryWorkflowError("registry release version/policy changed")
        manifest = cls(
            registry_name=_text(item["registry_name"], "registry name"),
            contract_pin=RegistryContractPin.from_dict(item["contract_pin"]),
            candidate=StoredControlDocument.from_dict(item["candidate"]),
            plan=StoredControlDocument.from_dict(item["plan"]),
            request=StoredControlDocument.from_dict(item["request"]),
            approval_receipt=StoredControlDocument.from_dict(item["approval_receipt"]),
            publish_intent=StoredControlDocument.from_dict(item["publish_intent"]),
            rows_path=_relative(item["rows_path"], "rows path"),
            rows_sha256=_digest(item["rows_sha256"], "rows SHA-256"),
            rows_bytes=_positive(item["rows_bytes"], "rows bytes"),
            row_count=_nonnegative(item["row_count"], "row count"),
            decisions=tuple(
                ReleasedDecisionArtifactRef.from_dict(row) for row in _array(item["decisions"])
            ),
            source_scope_set_digest=_digest(
                item["source_scope_set_digest"], "source scope set digest"
            ),
            published_at_utc=_parse_utc(_text(item["published_at_utc"], "publication time")),
            release_available_session=date.fromisoformat(
                _text(item["release_available_session"], "release availability")
            ),
            availability_calendar_id=_digest(item["availability_calendar_id"], "calendar ID"),
            availability_calendar_sha256=_digest(
                item["availability_calendar_sha256"], "calendar SHA-256"
            ),
            production_ingress_artifact=(
                None
                if item["production_ingress_artifact"] is None
                else ExactArtifactBinding.from_dict(item["production_ingress_artifact"])
            ),
            state=_text(item["state"], "release state"),
        )
        if item["release_id"] != manifest.release_id:
            raise RegistryWorkflowError("registry release ID recomputation failed")
        return manifest


@dataclass(frozen=True, slots=True)
class RegistryReleasePin:
    registry_name: str
    release_id: str
    manifest_path: str
    manifest_sha256: str
    manifest_bytes: int
    release_available_session: date

    def __post_init__(self) -> None:
        _registry(self.registry_name)
        _digest(self.release_id, "release ID")
        _relative(self.manifest_path, "release manifest path")
        _digest(self.manifest_sha256, "release manifest SHA-256")
        _positive(self.manifest_bytes, "release manifest bytes")
        _date(self.release_available_session, "release availability")

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_bytes": self.manifest_bytes,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "registry_name": self.registry_name,
            "release_available_session": self.release_available_session.isoformat(),
            "release_id": self.release_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> RegistryReleasePin:
        item = _mapping(value, "registry release pin")
        _expect_keys(
            item,
            {
                "manifest_bytes",
                "manifest_path",
                "manifest_sha256",
                "registry_name",
                "release_available_session",
                "release_id",
            },
            "registry release pin",
        )
        return cls(
            registry_name=_text(item["registry_name"], "registry name"),
            release_id=_digest(item["release_id"], "release ID"),
            manifest_path=_relative(item["manifest_path"], "manifest path"),
            manifest_sha256=_digest(item["manifest_sha256"], "manifest SHA-256"),
            manifest_bytes=_positive(item["manifest_bytes"], "manifest bytes"),
            release_available_session=date.fromisoformat(
                _text(item["release_available_session"], "release availability")
            ),
        )


@dataclass(frozen=True, slots=True)
class LoadedRegistryRelease:
    """A fully replayed release safe to index in the streaming materializer."""

    manifest: RegistryReleaseManifest
    manifest_pin: RegistryReleasePin
    candidate: RegistryCandidateManifest
    plan: RegistryDecisionPlan
    request: RegistryApprovalRequest
    approval_receipt: RegistryReleaseAuthorizationReceipt
    decision_rows: Mapping[str, Mapping[str, object]]
    source_scopes: Mapping[str, ExactSourceScope]

    def __post_init__(self) -> None:
        ids = tuple(item.decision_id for item in self.manifest.decisions)
        if set(self.decision_rows) != set(ids) or set(self.source_scopes) != set(ids):
            raise RegistryWorkflowError("loaded release decision coverage changed")
        object.__setattr__(
            self,
            "decision_rows",
            MappingProxyType(
                {item: MappingProxyType(dict(row)) for item, row in self.decision_rows.items()}
            ),
        )
        object.__setattr__(self, "source_scopes", MappingProxyType(dict(self.source_scopes)))

    @property
    def registry_name(self) -> str:
        return self.manifest.registry_name

    @property
    def release_id(self) -> str:
        return self.manifest.release_id

    @property
    def release_available_session(self) -> date:
        return self.manifest.release_available_session

    def require_decision(
        self,
        decision_id: str,
        *,
        cutoff_session: date,
    ) -> Mapping[str, object]:
        _digest(decision_id, "decision ID")
        _date(cutoff_session, "decision cutoff")
        row = self.decision_rows.get(decision_id)
        if row is None:
            raise RegistryWorkflowError(
                f"{self.registry_name} decision is absent from exact release {self.release_id}"
            )
        available = row[_AVAILABLE_SESSION_COLUMN[self.registry_name]]
        if not isinstance(available, date) or isinstance(available, datetime):
            raise RegistryWorkflowError("loaded decision availability type changed")
        if available > cutoff_session or self.release_available_session > cutoff_session:
            raise RegistryWorkflowError("decision or release is unavailable at the cutoff")
        if decision_id not in self.effective_decision_ids(cutoff_session=cutoff_session):
            raise RegistryWorkflowError("decision is not the effective terminal revision at cutoff")
        return row

    def effective_decision_ids(self, *, cutoff_session: date) -> tuple[str, ...]:
        _date(cutoff_session, "decision cutoff")
        if self.release_available_session > cutoff_session:
            raise RegistryWorkflowError("registry release is unavailable at the cutoff")
        series_column = _SERIES_ID_COLUMN[self.registry_name]
        available_column = _AVAILABLE_SESSION_COLUMN[self.registry_name]
        grouped: dict[str, list[tuple[int, str, date]]] = defaultdict(list)
        for decision_id, row in self.decision_rows.items():
            available = row[available_column]
            if not isinstance(available, date) or isinstance(available, datetime):
                raise RegistryWorkflowError("loaded decision availability type changed")
            grouped[str(row[series_column])].append(
                (int(row["decision_version"]), decision_id, available)
            )
        selected: list[str] = []
        for revisions in grouped.values():
            admitted = [item for item in revisions if item[2] <= cutoff_session]
            if admitted:
                selected.append(max(admitted, key=lambda item: item[0])[1])
        return tuple(sorted(selected))

    def require_exact_source_row(
        self,
        decision_id: str,
        source_row: ExactSourceRow,
        *,
        cutoff_session: date,
    ) -> Mapping[str, object]:
        row = self.require_decision(decision_id, cutoff_session=cutoff_session)
        scope = self.source_scopes[decision_id]
        observed = next(
            (item for item in scope.rows if item.source_record_id == source_row.source_record_id),
            None,
        )
        if observed is None or observed != source_row:
            raise RegistryWorkflowError(
                "derived decision reference does not match its exact released source-row scope"
            )
        return row

    def decision_ids_for_exact_source_row(
        self,
        source_row: ExactSourceRow,
        *,
        cutoff_session: date,
    ) -> tuple[str, ...]:
        result = []
        for decision_id in self.effective_decision_ids(cutoff_session=cutoff_session):
            scope = self.source_scopes[decision_id]
            if source_row in scope.rows:
                result.append(decision_id)
        return tuple(sorted(result))


@dataclass(frozen=True, slots=True)
class LoadedRegistryReleaseSet:
    releases: tuple[LoadedRegistryRelease, ...]

    def __post_init__(self) -> None:
        releases = tuple(self.releases)
        if tuple(item.registry_name for item in releases) != REGISTRY_ORDER:
            raise RegistryWorkflowError("registry release set must contain five exact releases")
        if len({item.release_id for item in releases}) != len(releases):
            raise RegistryWorkflowError("registry release IDs repeat")
        self._validate_relation_bindings()

    def by_name(self, registry_name: str) -> LoadedRegistryRelease:
        name = _registry(registry_name)
        return self.releases[REGISTRY_ORDER.index(name)]

    def composite_matches(
        self,
        source_row: ExactSourceRow,
        *,
        cutoff_session: date,
    ) -> tuple[tuple[str, str], ...]:
        matches: list[tuple[str, str]] = []
        for name in REGISTRY_ORDER:
            if name not in COMPOSITE_CORRECTION_REGISTRIES:
                continue
            release = self.by_name(name)
            for decision_id in release.decision_ids_for_exact_source_row(
                source_row, cutoff_session=cutoff_session
            ):
                matches.append((name, decision_id))
        return tuple(sorted(matches))

    def require_unique_composite_match(
        self,
        source_row: ExactSourceRow,
        *,
        cutoff_session: date,
    ) -> tuple[str, str] | None:
        matches = self.composite_matches(source_row, cutoff_session=cutoff_session)
        if len(matches) > 1:
            raise RegistryWorkflowError(
                "source row matches multiple Composite correction registries; "
                "no priority or majority fallback is permitted"
            )
        return matches[0] if matches else None

    def require_decision_scope(
        self,
        *,
        registry_name: str,
        release_id: str,
        decision_id: str,
        source_row: ExactSourceRow,
        cutoff_session: date,
    ) -> Mapping[str, object]:
        """Materializer adapter for exact release/decision/source-row replay."""

        release = self.by_name(registry_name)
        if release.release_id != release_id:
            raise RegistryWorkflowError("derived row registry release ID differs")
        return release.require_exact_source_row(
            decision_id,
            source_row,
            cutoff_session=cutoff_session,
        )

    def validate_all_composite_scopes_are_exclusive(self) -> None:
        index: dict[ExactSourceRow, list[tuple[str, str]]] = defaultdict(list)
        for name in COMPOSITE_CORRECTION_REGISTRIES:
            release = self.by_name(name)
            for decision_id, scope in release.source_scopes.items():
                for source_row in scope.rows:
                    index[source_row].append((name, decision_id))
        collisions = [
            (source_row.source_record_id, tuple(sorted(matches)))
            for source_row, matches in index.items()
            if len(matches) > 1
        ]
        if collisions:
            raise RegistryWorkflowError(
                f"Composite registry release set has exact source-row collisions: {collisions[:10]}"
            )

    def _validate_relation_bindings(self) -> None:
        transitions = self.by_name(RegistryName.ASSET_TRANSITION.value)
        overrides = self.by_name(RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value)
        for row in overrides.decision_rows.values():
            transition_id = row["asset_transition_id"]
            try:
                transition = transitions.require_decision(
                    str(transition_id),
                    cutoff_session=row["override_available_session"],
                )
            except RegistryWorkflowError as exc:
                raise RegistryWorkflowError(
                    "provider Composite override does not bind an available terminal "
                    "asset_transition in the exact release set"
                ) from exc
            if (
                transition["observed_ticker"] != row["observed_ticker"]
                or transition["successor_composite_figi"] != row["canonical_composite_figi"]
                or transition["asset_transition_series_id"] != row["asset_transition_series_id"]
                or transition["transition_available_session"]
                != row["asset_transition_available_session"]
            ):
                raise RegistryWorkflowError("provider override/asset transition replay differs")
            if row["asset_transition_available_session"] > row["override_available_session"]:
                raise RegistryWorkflowError("provider override predates its transition")


def store_candidate(
    data_root: Path,
    candidate: RegistryCandidateManifest,
) -> StoredControlDocument:
    root = data_root.expanduser().resolve()
    if is_canonical_production_data_root(root):
        if candidate.production_ingress_artifact is None:
            raise RegistryWorkflowError(
                "canonical production candidate lacks immutable ingress provenance"
            )
    elif candidate.production_ingress_artifact is not None:
        raise RegistryWorkflowError(
            "production ingress provenance cannot be stored outside production"
        )
    existing = _load_existing_candidate_slot(root, candidate.relative_path)
    if existing is not None:
        stored_candidate, document = existing
        if stored_candidate.candidate_scope_slot_id != candidate.candidate_scope_slot_id:
            raise RegistryWorkflowError("candidate scope slot is already bound differently")
        return document
    _verify_candidate_inputs(root, candidate)
    content = _canonical_bytes(candidate.to_dict())
    try:
        stored = write_bytes_immutable(
            root,
            safe_relative_path(root, candidate.relative_path),
            content,
            temporary_directory=root / "tmp" / "s7-registry-control-writes",
        )
    except ArtifactError:
        raced = _load_existing_candidate_slot(root, candidate.relative_path)
        if raced is None:
            raise
        stored_candidate, document = raced
        if stored_candidate.candidate_scope_slot_id != candidate.candidate_scope_slot_id:
            raise RegistryWorkflowError(
                "concurrent candidate preparation bound the slot differently"
            ) from None
        return document
    return StoredControlDocument(
        object_id=candidate.candidate_id,
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def load_candidate_control(
    data_root: Path,
    ref: StoredControlDocument,
) -> RegistryCandidateManifest:
    return _load_candidate_document(data_root.expanduser().resolve(), ref)


def create_decision_plan(
    candidate: RegistryCandidateManifest,
    candidate_document: StoredControlDocument,
) -> RegistryDecisionPlan:
    if (
        candidate_document.object_id != candidate.candidate_id
        or candidate_document.path != candidate.relative_path
    ):
        raise RegistryWorkflowError("candidate document does not bind the candidate")
    return RegistryDecisionPlan(
        registry_name=candidate.registry_name,
        contract_pin=candidate.contract_pin,
        candidate=candidate_document,
        decision_ids=tuple(item.decision_id for item in candidate.decisions),
        source_scope_set_digest=candidate.source_scope_set_digest,
        availability_calendar_id=candidate.availability_calendar_id,
        availability_calendar_sha256=candidate.availability_calendar_sha256,
    )


def store_decision_plan(
    data_root: Path,
    plan: RegistryDecisionPlan,
) -> StoredControlDocument:
    root = data_root.expanduser().resolve()
    candidate = _load_candidate_document(root, plan.candidate)
    _validate_plan_candidate(plan, candidate)
    return _store_control(root, plan.plan_id, plan.relative_path, plan.to_dict())


def load_decision_plan_control(
    data_root: Path,
    ref: StoredControlDocument,
) -> RegistryDecisionPlan:
    return _load_plan_document(data_root.expanduser().resolve(), ref)


def create_approval_request(
    plan: RegistryDecisionPlan,
    plan_document: StoredControlDocument,
) -> RegistryApprovalRequest:
    if plan_document.object_id != plan.plan_id or plan_document.path != plan.relative_path:
        raise RegistryWorkflowError("plan document does not bind the decision plan")
    return RegistryApprovalRequest(
        registry_name=plan.registry_name,
        contract_pin=plan.contract_pin,
        plan=plan_document,
        candidate=plan.candidate,
        decision_ids=plan.decision_ids,
        source_scope_set_digest=plan.source_scope_set_digest,
        availability_calendar_id=plan.availability_calendar_id,
        availability_calendar_sha256=plan.availability_calendar_sha256,
    )


def store_approval_request(
    data_root: Path,
    request: RegistryApprovalRequest,
) -> StoredControlDocument:
    root = data_root.expanduser().resolve()
    plan = _load_plan_document(root, request.plan)
    candidate = _load_candidate_document(root, request.candidate)
    _validate_request_chain(request, plan, candidate)
    return _store_control(
        root,
        request.request_event_id,
        request.relative_path,
        request.to_dict(),
    )


def load_approval_request_control(
    data_root: Path,
    ref: StoredControlDocument,
) -> RegistryApprovalRequest:
    return _load_request_document(data_root.expanduser().resolve(), ref)


def record_exact_approval(
    data_root: Path,
    *,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    literal: Mapping[str, object],
    approved_by: str,
    approved_at_utc: datetime,
    approval_available_session: date,
) -> tuple[RegistryApprovalReceipt, StoredControlDocument]:
    """Record approval only when the supplied object equals the exact request literal."""

    root = data_root.expanduser().resolve()
    require_fixture_registry_root(root)
    stored_request = _load_request_document(root, request_document)
    if stored_request != request:
        raise RegistryWorkflowError("approval request object differs from stored bytes")
    if _canonical_bytes(dict(literal)) != request.literal_bytes():
        raise RegistryWorkflowError("approval literal differs from the exact request")
    calendar = _load_calendar(
        root,
        request.availability_calendar_id,
        request.availability_calendar_sha256,
    )
    try:
        calendar.require_first_open_session(
            approved_at_utc,
            approval_available_session,
            label="registry approval availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    runtime_binding = capture_registry_runtime_binding()
    receipt = RegistryApprovalReceipt(
        registry_name=request.registry_name,
        request=request_document,
        request_event_id=request.request_event_id,
        plan_id=request.plan.object_id,
        candidate_id=request.candidate.object_id,
        decision_ids=request.decision_ids,
        approved_by=approved_by,
        approved_at_utc=approved_at_utc,
        approval_available_session=approval_available_session,
        availability_calendar_id=request.availability_calendar_id,
        availability_calendar_sha256=request.availability_calendar_sha256,
        runtime_binding=runtime_binding,
    )
    document = _store_control(
        root,
        receipt.receipt_id,
        receipt.relative_path,
        receipt.to_dict(),
    )
    return receipt, document


def record_standing_approval(
    data_root: Path,
    *,
    request_document: StoredControlDocument,
    standing_authorization_literal: bytes,
    reaffirmation_literal: bytes,
    approved_by: str,
) -> tuple[RegistryStandingApprovalReceipt, StoredControlDocument]:
    """Record one exact registry approval under the frozen standing instruction.

    The caller cannot provide a timestamp, availability date, candidate, plan or
    request payload.  All controls are replayed from ``request_document`` and the time
    is sampled from the runtime clock.  The two utterance byte strings must exactly
    match the frozen user text (including whitespace).
    """

    root = data_root.expanduser().resolve()
    if standing_authorization_literal != STANDING_AUTHORIZATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing authorization literal bytes changed")
    if reaffirmation_literal != STANDING_REAFFIRMATION_LITERAL.encode("utf-8"):
        raise RegistryWorkflowError("standing reaffirmation literal bytes changed")
    request = _load_request_document(root, request_document)
    plan = _load_plan_document(root, request.plan)
    candidate = _load_candidate_document(root, request.candidate)
    _validate_request_chain(request, plan, candidate)
    slot_path = _standing_approval_slot_path(
        request.registry_name,
        request.candidate,
        request.plan,
        request_document,
    )
    existing = _load_existing_standing_receipt(root, slot_path)
    if existing is not None:
        receipt, document = existing
        if (
            receipt.request != request_document
            or receipt.approved_by != approved_by
            or receipt.standing_authorization_literal.encode("utf-8")
            != standing_authorization_literal
            or receipt.reaffirmation_literal.encode("utf-8") != reaffirmation_literal
        ):
            raise RegistryWorkflowError("standing approval slot is already bound differently")
        return receipt, document
    review = _build_standing_qa_review(candidate, plan, request)
    runtime_binding = capture_registry_runtime_binding()
    approved_at = _runtime_utc_now()
    calendar = _load_calendar(
        root,
        request.availability_calendar_id,
        request.availability_calendar_sha256,
    )
    try:
        approval_available, _ = calendar.first_open_after(approved_at)
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    receipt = RegistryStandingApprovalReceipt(
        registry_name=request.registry_name,
        request=request_document,
        plan=request.plan,
        candidate=request.candidate,
        request_event_id=request.request_event_id,
        decision_ids=request.decision_ids,
        source_scope_set_digest=request.source_scope_set_digest,
        contract_pin=request.contract_pin,
        source_artifacts=candidate.source_artifacts,
        evidence_artifacts=candidate.evidence_artifacts,
        candidate_authorization_artifacts=candidate.authorization_artifacts,
        exact_request_literal=request.literal_payload(),
        exact_request_literal_sha256=hashlib.sha256(request.literal_bytes()).hexdigest(),
        standing_authorization_literal=standing_authorization_literal.decode("utf-8"),
        standing_authorization_literal_sha256=hashlib.sha256(
            standing_authorization_literal
        ).hexdigest(),
        reaffirmation_literal=reaffirmation_literal.decode("utf-8"),
        reaffirmation_literal_sha256=hashlib.sha256(reaffirmation_literal).hexdigest(),
        qa_review=review,
        capabilities=STANDING_AUTHORIZATION_CAPABILITIES,
        approved_by=approved_by,
        approved_at_utc=approved_at,
        approval_available_session=approval_available,
        availability_calendar_id=request.availability_calendar_id,
        availability_calendar_sha256=request.availability_calendar_sha256,
        runtime_binding=runtime_binding,
    )
    try:
        document = _store_control(
            root,
            receipt.receipt_id,
            receipt.relative_path,
            receipt.to_dict(),
        )
    except ArtifactError:
        raced = _load_existing_standing_receipt(root, slot_path)
        if raced is None:
            raise
        raced_receipt, raced_document = raced
        if (
            raced_receipt.request != request_document
            or raced_receipt.approved_by != approved_by
            or raced_receipt.standing_authorization_literal.encode("utf-8")
            != standing_authorization_literal
            or raced_receipt.reaffirmation_literal.encode("utf-8") != reaffirmation_literal
        ):
            raise RegistryWorkflowError(
                "concurrent standing approval bound the slot differently"
            ) from None
        return raced_receipt, raced_document
    return receipt, document


def build_approved_registry_rows(
    data_root: Path,
    *,
    request_document: StoredControlDocument,
    approval_receipt_document: StoredControlDocument,
) -> tuple[dict[str, object], ...]:
    """Build complete contract rows from one immutable approval chain.

    Only post-approval control fields are synthesized.  Every research/factual field
    comes from the candidate and is revalidated before the rows are returned.
    """

    root = data_root.expanduser().resolve()
    request = _load_request_document(root, request_document)
    plan = _load_plan_document(root, request.plan)
    candidate = _load_candidate_document(root, request.candidate)
    receipt = _load_receipt_document(root, approval_receipt_document)
    _validate_release_control_chain(
        candidate,
        plan,
        request.plan,
        request,
        request_document,
        receipt,
        approval_receipt_document,
    )
    calendar = _load_calendar(
        root,
        request.availability_calendar_id,
        request.availability_calendar_sha256,
    )
    rows: list[dict[str, object]] = []
    contract = _CONTRACTS[request.registry_name]
    column_types = {column.name: column.arrow_type for column in contract.columns}
    for intent in candidate.decisions:
        row = {
            name: _restore_contract_value(column_types[name], value)
            for name, value in intent.frozen_row_claims.items()
        }
        row.update(
            {
                "approval_available_session": receipt.approval_available_session,
                "approved_at_utc": receipt.approved_at_utc,
                "approved_by": receipt.approved_by,
            }
        )
        if request.registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
            row.update(
                {
                    "approval_id": receipt.receipt_id,
                    "approval_receipt_path": approval_receipt_document.path,
                    "approval_receipt_sha256": approval_receipt_document.sha256,
                    "approval_status": APPROVED,
                    "source_decision_plan_id": request.plan.object_id,
                    "source_decision_plan_path": request.plan.path,
                    "source_decision_plan_sha256": request.plan.sha256,
                }
            )
        elif request.registry_name == RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value:
            row.update(
                {
                    "approval_request_event_id": request.request_event_id,
                    "approval_request_event_sha256": hashlib.sha256(
                        request.literal_bytes()
                    ).hexdigest(),
                    "approval_receipt_id": receipt.receipt_id,
                    "approval_receipt_path": approval_receipt_document.path,
                    "approval_receipt_sha256": approval_receipt_document.sha256,
                    "approval_status": APPROVED,
                }
            )
        else:
            row.update(
                {
                    "approval_request_event_id": request.request_event_id,
                    "approval_request_event_sha256": hashlib.sha256(
                        request.literal_bytes()
                    ).hexdigest(),
                    "approval_receipt_id": receipt.receipt_id,
                    "approval_receipt_sha256": approval_receipt_document.sha256,
                    "source_decision_plan_id": request.plan.object_id,
                    "source_decision_plan_path": request.plan.path,
                    "source_decision_plan_sha256": request.plan.sha256,
                }
            )
        available = _expected_decision_available_session(
            request.registry_name,
            row,
            candidate,
            receipt,
        )
        row[_AVAILABLE_SESSION_COLUMN[request.registry_name]] = available
        if request.registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
            try:
                row["adjudication_available_at_utc"] = calendar.market_open(available)
            except XNYSCalendarArtifactError as exc:
                raise RegistryWorkflowError(str(exc)) from exc
        if set(row) != {column.name for column in contract.columns}:
            raise RegistryWorkflowError("approved row builder did not fill the exact contract")
        _validate_final_row(
            request.registry_name,
            row,
            intent,
            candidate,
            request.plan,
            request,
            request_document,
            receipt,
            approval_receipt_document,
        )
        rows.append(row)
    return tuple(rows)


def publish_release_under_standing_authority(
    data_root: Path,
    *,
    request_document: StoredControlDocument,
    standing_authorization_literal: bytes,
    reaffirmation_literal: bytes,
    approved_by: str,
) -> tuple[RegistryStandingApprovalReceipt, StoredControlDocument, RegistryReleasePin]:
    """Record and publish one exact reviewed registry using runtime timestamps only."""

    root = data_root.expanduser().resolve()
    receipt, receipt_document = record_standing_approval(
        root,
        request_document=request_document,
        standing_authorization_literal=standing_authorization_literal,
        reaffirmation_literal=reaffirmation_literal,
        approved_by=approved_by,
    )
    request = _load_request_document(root, request_document)
    plan = _load_plan_document(root, request.plan)
    rows = build_approved_registry_rows(
        root,
        request_document=request_document,
        approval_receipt_document=receipt_document,
    )
    published_at = _runtime_utc_now()
    calendar = _load_calendar(
        root,
        request.availability_calendar_id,
        request.availability_calendar_sha256,
    )
    try:
        publication_available, _ = calendar.first_open_after(published_at)
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    release_available = max(
        (
            publication_available,
            *(row[_AVAILABLE_SESSION_COLUMN[request.registry_name]] for row in rows),
        )
    )
    pin = _publish_release(
        root,
        plan=plan,
        plan_document=request.plan,
        request=request,
        request_document=request_document,
        approval_receipt=receipt,
        approval_receipt_document=receipt_document,
        decision_rows=rows,
        published_at_utc=published_at,
        release_available_session=release_available,
    )
    return receipt, receipt_document, pin


def load_approval_receipt_control(
    data_root: Path,
    ref: StoredControlDocument,
) -> RegistryReleaseAuthorizationReceipt:
    return _load_receipt_document(data_root.expanduser().resolve(), ref)


def publish_release(
    data_root: Path,
    *,
    plan: RegistryDecisionPlan,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    approval_receipt: RegistryReleaseAuthorizationReceipt,
    approval_receipt_document: StoredControlDocument,
    decision_rows: Sequence[Mapping[str, object]],
    published_at_utc: datetime,
    release_available_session: date,
) -> RegistryReleasePin:
    """Fixture/internal publisher; canonical production uses the standing path."""

    root = data_root.expanduser().resolve()
    require_fixture_registry_root(root)
    return _publish_release(
        root,
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        approval_receipt=approval_receipt,
        approval_receipt_document=approval_receipt_document,
        decision_rows=decision_rows,
        published_at_utc=published_at_utc,
        release_available_session=release_available_session,
    )


def _publish_release(
    data_root: Path,
    *,
    plan: RegistryDecisionPlan,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    approval_receipt: RegistryReleaseAuthorizationReceipt,
    approval_receipt_document: StoredControlDocument,
    decision_rows: Sequence[Mapping[str, object]],
    published_at_utc: datetime,
    release_available_session: date,
) -> RegistryReleasePin:
    """Publish exactly the approved row set; the manifest is written last."""

    root = data_root.expanduser().resolve()
    candidate = _load_candidate_document(root, plan.candidate)
    loaded_plan = _load_plan_document(root, plan_document)
    loaded_request = _load_request_document(root, request_document)
    loaded_receipt = _load_receipt_document(root, approval_receipt_document)
    if loaded_plan != plan or loaded_request != request or loaded_receipt != approval_receipt:
        raise RegistryWorkflowError("release controls differ from their immutable bytes")
    _validate_release_control_chain(
        candidate,
        plan,
        plan_document,
        request,
        request_document,
        approval_receipt,
        approval_receipt_document,
    )
    _require_current_runtime_binding(approval_receipt.runtime_binding)
    calendar = _load_calendar(
        root,
        plan.availability_calendar_id,
        plan.availability_calendar_sha256,
    )
    normalized_rows, table = _build_registry_table(
        plan.registry_name,
        decision_rows,
        candidate,
        plan_document,
        request,
        request_document,
        approval_receipt,
        approval_receipt_document,
    )
    _validate_decision_row_calendar(plan.registry_name, normalized_rows, calendar)
    decision_available = tuple(
        row[_AVAILABLE_SESSION_COLUMN[plan.registry_name]] for row in normalized_rows
    )
    first_publish_session, _ = calendar.first_open_after(published_at_utc)
    expected_release_session = max((first_publish_session, *decision_available))
    if release_available_session != expected_release_session:
        raise RegistryWorkflowError(
            "release availability must be the max of publication and decision availability"
        )

    row_set_digest = stable_digest(_json_value(normalized_rows))
    release_lane_id = _authorization_lane_id(
        plan.registry_name,
        plan.candidate,
        plan_document,
        request_document,
    )
    proposed_publish_intent = RegistryPublishIntent(
        registry_name=plan.registry_name,
        release_lane_id=release_lane_id,
        candidate=plan.candidate,
        plan=plan_document,
        request=request_document,
        approval_receipt=approval_receipt_document,
        decision_ids=plan.decision_ids,
        source_scope_set_digest=plan.source_scope_set_digest,
        decision_row_set_digest=row_set_digest,
        runtime_binding_id=approval_receipt.runtime_binding.runtime_binding_id,
        published_at_utc=published_at_utc,
        release_available_session=release_available_session,
        availability_calendar_id=plan.availability_calendar_id,
        availability_calendar_sha256=plan.availability_calendar_sha256,
    )
    publish_intent, publish_intent_document = _store_or_replay_publish_intent(
        root,
        proposed_publish_intent,
    )
    _validate_publish_intent_chain(
        publish_intent,
        candidate,
        plan,
        plan_document,
        request,
        request_document,
        approval_receipt,
        approval_receipt_document,
        row_set_digest,
        calendar,
        decision_available,
    )
    # A source or interpreter change after intent creation still fails before any
    # Parquet or release member is written.
    _require_current_runtime_binding(approval_receipt.runtime_binding)

    parquet_content = _parquet_bytes(table)
    rows_sha = hashlib.sha256(parquet_content).hexdigest()
    intent_by_id = {item.decision_id: item for item in candidate.decisions}
    decision_documents: dict[str, bytes] = {}
    refs: list[ReleasedDecisionArtifactRef] = []
    for row in normalized_rows:
        decision_id = str(row[_DECISION_ID_COLUMN[plan.registry_name]])
        intent = intent_by_id[decision_id]
        available = row[_AVAILABLE_SESSION_COLUMN[plan.registry_name]]
        assert isinstance(available, date) and not isinstance(available, datetime)
        normalized_row = _json_value(row)
        assert isinstance(normalized_row, dict)
        row_digest = stable_digest(normalized_row)
        document = {
            "approval_receipt_id": approval_receipt.receipt_id,
            "candidate_id": candidate.candidate_id,
            "decision_available_session": available.isoformat(),
            "decision_id": decision_id,
            "decision_replay_version": WORKFLOW_VERSION,
            "plan_id": plan.plan_id,
            "policy_version": WORKFLOW_POLICY,
            "registry_name": plan.registry_name,
            "row": normalized_row,
            "row_digest": row_digest,
            "source_scope": intent.source_scope.to_dict(),
        }
        content = _canonical_bytes(document)
        relative = f"decisions/decision_id={decision_id}.json"
        decision_documents[relative] = content
        refs.append(
            ReleasedDecisionArtifactRef(
                decision_id=decision_id,
                path=relative,
                sha256=hashlib.sha256(content).hexdigest(),
                bytes=len(content),
                available_session=available,
                source_row_count=len(intent.source_scope.rows),
                source_record_set_digest=intent.source_scope.source_record_set_digest,
                source_scope_digest=intent.source_scope.scope_digest,
                row_digest=row_digest,
            )
        )
    refs_tuple = tuple(sorted(refs, key=lambda item: item.decision_id))
    manifest = RegistryReleaseManifest(
        registry_name=plan.registry_name,
        contract_pin=plan.contract_pin,
        candidate=plan.candidate,
        plan=plan_document,
        request=request_document,
        approval_receipt=approval_receipt_document,
        publish_intent=publish_intent_document,
        rows_path="data/decisions.parquet",
        rows_sha256=rows_sha,
        rows_bytes=len(parquet_content),
        row_count=len(normalized_rows),
        decisions=refs_tuple,
        source_scope_set_digest=candidate.source_scope_set_digest,
        published_at_utc=publish_intent.published_at_utc,
        release_available_session=publish_intent.release_available_session,
        availability_calendar_id=plan.availability_calendar_id,
        availability_calendar_sha256=plan.availability_calendar_sha256,
        production_ingress_artifact=candidate.production_ingress_artifact,
    )
    release_dir = manifest.release_directory
    _write_release_member(root, release_dir, manifest.rows_path, parquet_content)
    for relative, content in sorted(decision_documents.items()):
        _write_release_member(root, release_dir, relative, content)
    manifest_content = _canonical_bytes(manifest.to_dict())
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, manifest.relative_path),
        manifest_content,
        temporary_directory=root / "tmp" / "s7-registry-release-writes",
    )
    pin = RegistryReleasePin(
        registry_name=manifest.registry_name,
        release_id=manifest.release_id,
        manifest_path=str(stored["path"]),
        manifest_sha256=str(stored["sha256"]),
        manifest_bytes=int(stored["bytes"]),
        release_available_session=manifest.release_available_session,
    )
    # Never report success without replaying every control and decision artifact.
    load_registry_release(root, pin)
    return pin


def load_registry_release(
    data_root: Path,
    pin: RegistryReleasePin,
) -> LoadedRegistryRelease:
    """Load one exact manifest and replay its complete control and decision row set."""

    root = data_root.expanduser().resolve()
    manifest_bytes = _read_exact(
        root,
        pin.manifest_path,
        expected_sha256=pin.manifest_sha256,
        expected_bytes=pin.manifest_bytes,
    )
    manifest = RegistryReleaseManifest.from_dict(
        _load_json(manifest_bytes, "registry release manifest")
    )
    if (
        manifest.registry_name != pin.registry_name
        or manifest.release_id != pin.release_id
        or manifest.relative_path != pin.manifest_path
        or manifest.release_available_session != pin.release_available_session
    ):
        raise RegistryWorkflowError("release pin differs from exact manifest")
    candidate = _load_candidate_document(root, manifest.candidate)
    plan = _load_plan_document(root, manifest.plan)
    request = _load_request_document(root, manifest.request)
    receipt = _load_receipt_document(root, manifest.approval_receipt)
    publish_intent = _load_publish_intent_document(root, manifest.publish_intent)
    _validate_release_control_chain(
        candidate,
        plan,
        manifest.plan,
        request,
        manifest.request,
        receipt,
        manifest.approval_receipt,
    )
    if (
        candidate.source_scope_set_digest != manifest.source_scope_set_digest
        or manifest.contract_pin != candidate.contract_pin
        or manifest.production_ingress_artifact != candidate.production_ingress_artifact
    ):
        raise RegistryWorkflowError(
            "release source scope, contract or production provenance differs from candidate"
        )
    if is_canonical_production_data_root(root) and manifest.production_ingress_artifact is None:
        raise RegistryWorkflowError("canonical production release lacks ingress provenance")
    calendar = _load_calendar(
        root,
        manifest.availability_calendar_id,
        manifest.availability_calendar_sha256,
    )
    first_publish_session, _ = calendar.first_open_after(manifest.published_at_utc)
    expected_release_session = max(
        (first_publish_session, *(item.available_session for item in manifest.decisions))
    )
    if manifest.release_available_session != expected_release_session:
        raise RegistryWorkflowError("release availability recomputation failed")

    release_dir = manifest.release_directory
    rows_content = _read_release_member(
        root,
        release_dir,
        manifest.rows_path,
        expected_sha256=manifest.rows_sha256,
        expected_bytes=manifest.rows_bytes,
    )
    table = _read_registry_table(manifest.registry_name, rows_content)
    rows = table.to_pylist()
    id_column = _DECISION_ID_COLUMN[manifest.registry_name]
    rows_by_id = {str(row[id_column]): row for row in rows}
    if len(rows_by_id) != len(rows) or set(rows_by_id) != {
        item.decision_id for item in manifest.decisions
    }:
        raise RegistryWorkflowError("release Parquet decision IDs differ from manifest")
    _validate_decision_row_calendar(manifest.registry_name, rows, calendar)
    decision_available = tuple(
        row[_AVAILABLE_SESSION_COLUMN[manifest.registry_name]] for row in rows
    )
    _validate_publish_intent_chain(
        publish_intent,
        candidate,
        plan,
        manifest.plan,
        request,
        manifest.request,
        receipt,
        manifest.approval_receipt,
        stable_digest(_json_value(rows)),
        calendar,
        decision_available,
    )
    if (
        manifest.publish_intent.object_id != publish_intent.intent_id
        or manifest.published_at_utc != publish_intent.published_at_utc
        or manifest.release_available_session != publish_intent.release_available_session
    ):
        raise RegistryWorkflowError("release manifest differs from durable publish intent")

    intent_by_id = {item.decision_id: item for item in candidate.decisions}
    scopes: dict[str, ExactSourceScope] = {}
    for ref in manifest.decisions:
        content = _read_release_member(
            root,
            release_dir,
            ref.path,
            expected_sha256=ref.sha256,
            expected_bytes=ref.bytes,
        )
        document = _mapping(_load_json(content, "decision replay"), "decision replay")
        expected_keys = {
            "approval_receipt_id",
            "candidate_id",
            "decision_available_session",
            "decision_id",
            "decision_replay_version",
            "plan_id",
            "policy_version",
            "registry_name",
            "row",
            "row_digest",
            "source_scope",
        }
        _expect_keys(document, expected_keys, "decision replay")
        if (
            document["decision_replay_version"] != WORKFLOW_VERSION
            or document["policy_version"] != WORKFLOW_POLICY
            or document["registry_name"] != manifest.registry_name
            or document["decision_id"] != ref.decision_id
            or document["candidate_id"] != candidate.candidate_id
            or document["plan_id"] != plan.plan_id
            or document["approval_receipt_id"] != receipt.receipt_id
        ):
            raise RegistryWorkflowError("decision replay control binding changed")
        scope = ExactSourceScope.from_dict(document["source_scope"])
        replay_row = _mapping(document["row"], "decision replay row")
        actual_row = _json_value(rows_by_id[ref.decision_id])
        if replay_row != actual_row:
            raise RegistryWorkflowError("decision JSON replay differs from Parquet row")
        if document["row_digest"] != stable_digest(replay_row):
            raise RegistryWorkflowError("decision replay row digest changed")
        if (
            ref.row_digest != document["row_digest"]
            or ref.source_row_count != len(scope.rows)
            or ref.source_record_set_digest != scope.source_record_set_digest
            or ref.source_scope_digest != scope.scope_digest
            or document["decision_available_session"] != ref.available_session.isoformat()
        ):
            raise RegistryWorkflowError("decision replay reference differs")
        intent = intent_by_id.get(ref.decision_id)
        if intent is None or intent.source_scope != scope:
            raise RegistryWorkflowError("decision scope differs from approved candidate")
        _validate_final_row(
            manifest.registry_name,
            rows_by_id[ref.decision_id],
            intent,
            candidate,
            manifest.plan,
            request,
            manifest.request,
            receipt,
            manifest.approval_receipt,
        )
        scopes[ref.decision_id] = scope
    _validate_final_chains(manifest.registry_name, rows)
    _verify_release_file_set(root, manifest)
    return LoadedRegistryRelease(
        manifest=manifest,
        manifest_pin=pin,
        candidate=candidate,
        plan=plan,
        request=request,
        approval_receipt=receipt,
        decision_rows=rows_by_id,
        source_scopes=scopes,
    )


def load_registry_release_set(
    data_root: Path,
    pins: Sequence[RegistryReleasePin],
    *,
    require_exclusive_composite_scopes: bool = True,
) -> LoadedRegistryReleaseSet:
    if tuple(item.registry_name for item in pins) != REGISTRY_ORDER:
        raise RegistryWorkflowError("release pins must use the frozen five-registry order")
    loaded = LoadedRegistryReleaseSet(tuple(load_registry_release(data_root, pin) for pin in pins))
    if require_exclusive_composite_scopes:
        loaded.validate_all_composite_scopes_are_exclusive()
    return loaded


@dataclass(frozen=True, slots=True)
class FixedDecisionScopeSpec:
    case_key: str
    registry_name: str
    ticker: str
    valid_from_session: date
    valid_through_session: date
    observed_composite_figi: str | None
    canonical_composite_figi: str | None
    observed_share_class_figi: str | None
    canonical_share_class_figi: str | None
    observed_market_code: str | None = None
    expected_source_row_count: int | None = None


FIXED_DECISION_SCOPE_SPECS: Final[Mapping[str, FixedDecisionScopeSpec]] = MappingProxyType(
    {
        "asset_transition:SOR": FixedDecisionScopeSpec(
            case_key="asset_transition:SOR",
            registry_name=RegistryName.ASSET_TRANSITION.value,
            ticker="SOR",
            valid_from_session=date(2024, 12, 31),
            valid_through_session=date(2025, 1, 2),
            observed_composite_figi="BBG000KMY6N2",
            canonical_composite_figi="BBG01RK6N4M5",
            observed_share_class_figi=None,
            canonical_share_class_figi=None,
            expected_source_row_count=2,
        ),
        "provider_composite_override:SOR": FixedDecisionScopeSpec(
            case_key="provider_composite_override:SOR",
            registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
            ticker="SOR",
            valid_from_session=date(2025, 1, 2),
            valid_through_session=date(2026, 7, 9),
            observed_composite_figi="BBG000KMY6N2",
            canonical_composite_figi="BBG01RK6N4M5",
            observed_share_class_figi=None,
            canonical_share_class_figi=None,
            observed_market_code="US",
            expected_source_row_count=379,
        ),
        "share_class_adjudication:XZO": FixedDecisionScopeSpec(
            case_key="share_class_adjudication:XZO",
            registry_name=RegistryName.SHARE_CLASS_ADJUDICATION.value,
            ticker="XZO",
            valid_from_session=date(2025, 11, 4),
            valid_through_session=date(2025, 11, 5),
            observed_composite_figi="BBG01XL8FHT0",
            canonical_composite_figi="BBG01XL8FHT0",
            observed_share_class_figi="BBG01XL8FJS7",
            canonical_share_class_figi="BBG01227MF17",
            expected_source_row_count=2,
        ),
        "share_class_adjudication:ANABV": FixedDecisionScopeSpec(
            case_key="share_class_adjudication:ANABV",
            registry_name=RegistryName.SHARE_CLASS_ADJUDICATION.value,
            ticker="ANABV",
            valid_from_session=date(2026, 4, 6),
            valid_through_session=date(2026, 4, 6),
            observed_composite_figi="BBG021DMXXT2",
            canonical_composite_figi="BBG021DMXXT2",
            observed_share_class_figi="BBG0026ZDHT8",
            canonical_share_class_figi="BBG021GNPBR6",
            expected_source_row_count=1,
        ),
        **{
            f"identity_cross_market_adjudication:{ticker}": FixedDecisionScopeSpec(
                case_key=f"identity_cross_market_adjudication:{ticker}",
                registry_name=RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
                ticker=ticker,
                valid_from_session=start,
                valid_through_session=end,
                observed_composite_figi=foreign,
                canonical_composite_figi=canonical,
                observed_share_class_figi=share_class,
                canonical_share_class_figi=share_class,
                observed_market_code=market,
                expected_source_row_count=count,
            )
            for ticker, canonical, foreign, share_class, market, start, end, count in (
                (
                    "AZPN",
                    "BBG000DFMXT3",
                    "BBG000KRLLH9",
                    "BBG001S87NT0",
                    "GR",
                    date(2022, 2, 9),
                    date(2022, 3, 2),
                    15,
                ),
                (
                    "CR",
                    "BBG000BG7423",
                    "BBG00CTGPFW0",
                    "BBG001S5Q3X4",
                    "EO",
                    date(2022, 2, 9),
                    date(2022, 3, 2),
                    15,
                ),
                (
                    "FLOW",
                    "BBG007FL7ZD2",
                    "BBG00K03RX51",
                    "BBG007FL7ZF0",
                    "EO",
                    date(2022, 2, 9),
                    date(2022, 3, 2),
                    15,
                ),
                (
                    "SBGI",
                    "BBG000F2XXP2",
                    "BBG000C3K505",
                    "BBG001S7W602",
                    "GR",
                    date(2022, 2, 8),
                    date(2022, 2, 8),
                    1,
                ),
                (
                    "SIRI",
                    "BBG000BT0093",
                    "BBG000BGPKZ1",
                    "BBG001S70ZY6",
                    "GR",
                    date(2022, 2, 8),
                    date(2022, 2, 8),
                    1,
                ),
                (
                    "TA",
                    "BBG000F71CC6",
                    "BBG000CVD896",
                    "BBG001SHR063",
                    "GR",
                    date(2022, 2, 8),
                    date(2022, 2, 8),
                    1,
                ),
                (
                    "TBLT",
                    "BBG00LDFP150",
                    "BBG00YGNW2D3",
                    "BBG00LDFP1X9",
                    "EO",
                    date(2022, 2, 9),
                    date(2022, 3, 2),
                    15,
                ),
                (
                    "TNXP",
                    "BBG000LG8XM5",
                    "BBG00R4FG9L2",
                    "BBG001T49NZ9",
                    "EP",
                    date(2022, 2, 9),
                    date(2022, 3, 2),
                    15,
                ),
                (
                    "WW",
                    "BBG000DY6735",
                    "BBG000D08924",
                    "BBG001SFWZR1",
                    "GR",
                    date(2022, 2, 8),
                    date(2022, 2, 8),
                    1,
                ),
            )
        },
    }
)


def validate_fixed_decision_candidate(candidate: RegistryDecisionCandidate) -> None:
    """Bind the reviewed 3+9 cases without inventing missing source-record IDs."""

    spec = FIXED_DECISION_SCOPE_SPECS.get(candidate.case_key)
    if spec is None:
        raise RegistryWorkflowError("candidate is not one of the frozen S7 reviewed cases")
    if candidate.registry_name != spec.registry_name:
        raise RegistryWorkflowError("fixed case registry responsibility changed")
    scope = candidate.source_scope
    if spec.expected_source_row_count is not None and len(scope.rows) != (
        spec.expected_source_row_count
    ):
        raise RegistryWorkflowError("fixed case exact source-row count changed")
    if (
        min(row.session_date for row in scope.rows) != spec.valid_from_session
        or max(row.session_date for row in scope.rows) != spec.valid_through_session
        or any(row.ticker != spec.ticker for row in scope.rows)
    ):
        raise RegistryWorkflowError("fixed case ticker/session scope changed")
    claims = candidate.frozen_row_claims
    if claims.get("observed_ticker") != spec.ticker:
        raise RegistryWorkflowError("fixed case row ticker changed")
    observed_column = {
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: ("observed_foreign_composite_figi"),
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: "observed_composite_figi",
        RegistryName.SHARE_CLASS_ADJUDICATION.value: "observed_composite_figi",
        RegistryName.ASSET_TRANSITION.value: "predecessor_composite_figi",
    }[candidate.registry_name]
    canonical_column = {
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value: "canonical_us_composite_figi",
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: "canonical_composite_figi",
        RegistryName.SHARE_CLASS_ADJUDICATION.value: ("required_unique_canonical_composite_figi"),
        RegistryName.ASSET_TRANSITION.value: "successor_composite_figi",
    }[candidate.registry_name]
    if (
        claims.get(observed_column) != spec.observed_composite_figi
        or claims.get(canonical_column) != spec.canonical_composite_figi
    ):
        raise RegistryWorkflowError("fixed case Composite mapping changed")
    if candidate.registry_name == RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value:
        if (
            claims.get("share_class_figi") != spec.observed_share_class_figi
            or claims.get("observed_composite_market_code") != spec.observed_market_code
            or claims.get("canonical_composite_market_code") != "US"
        ):
            raise RegistryWorkflowError("fixed cross-market hierarchy changed")
        if any(
            row.observed_composite_figi != spec.observed_composite_figi
            or row.observed_share_class_figi != spec.observed_share_class_figi
            for row in scope.rows
        ):
            raise RegistryWorkflowError("fixed cross-market source row changed")
        expected_case_count = 3 if spec.expected_source_row_count == 15 else 1
        case_roles = _canonical_json_text(
            claims["related_identity_case_roles_json"], "fixed related case roles"
        )
        assert isinstance(case_roles, dict)
        role_set = set(case_roles.values())
        if claims["related_identity_case_count"] != expected_case_count:
            raise RegistryWorkflowError("fixed cross-market related case count changed")
        if expected_case_count == 3 and not {
            "contaminated_middle_episode",
            "inverse_middle_is_canonical_us",
        }.issubset(role_set):
            raise RegistryWorkflowError("fixed inverse case lineage is incomplete")
        if expected_case_count == 1 and role_set != {"contaminated_middle_episode"}:
            raise RegistryWorkflowError("fixed single-day case lineage changed")
    elif candidate.registry_name == RegistryName.SHARE_CLASS_ADJUDICATION.value:
        if (
            claims.get("observed_share_class_figi") != spec.observed_share_class_figi
            or claims.get("canonical_share_class_figi") != spec.canonical_share_class_figi
        ):
            raise RegistryWorkflowError("fixed Share Class mapping changed")
        if spec.ticker == "ANABV" and claims.get("observed_composite_figi") == "BBG0026ZDHR0":
            raise RegistryWorkflowError("ANABV cannot be merged into ANAB")
    elif candidate.registry_name == RegistryName.ASSET_TRANSITION.value:
        by_date = {row.session_date: row for row in scope.rows}
        if (
            by_date[date(2024, 12, 31)].observed_composite_figi != "BBG000KMY6N2"
            or by_date[date(2025, 1, 2)].observed_composite_figi != "BBG000KMY6N2"
            or claims.get("legal_effective_date") != "2025-01-01"
        ):
            raise RegistryWorkflowError("SOR transition boundary changed")
    else:
        if any(row.observed_composite_figi != spec.observed_composite_figi for row in scope.rows):
            raise RegistryWorkflowError("SOR stale-provider source scope changed")


def _validate_scope_projection(
    registry_name: str,
    claims: Mapping[str, object],
    scope: ExactSourceScope,
) -> None:
    count_column, digest_column, ids_column = _SCOPE_LAYOUT[registry_name]
    if claims[count_column] != len(scope.rows):
        raise RegistryWorkflowError("candidate source-row count differs from exact scope")
    if claims[digest_column] != scope.source_record_set_digest:
        raise RegistryWorkflowError("candidate source-record digest differs from exact scope")
    if ids_column:
        expected_ids_json = json.dumps(
            list(scope.source_record_ids),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if claims[ids_column] != expected_ids_json:
            raise RegistryWorkflowError("candidate source-record JSON differs from exact scope")
    s4_release = scope.rows[0].source_s4_release_set_id
    if claims.get("source_s4_release_set_id") != s4_release:
        raise RegistryWorkflowError("candidate S4 release differs from exact source scope")


def _validate_registry_responsibility(
    registry_name: str,
    claims: Mapping[str, object],
    scope: ExactSourceScope,
) -> None:
    ticker = claims.get("observed_ticker")
    if not isinstance(ticker, str) or any(row.ticker != ticker for row in scope.rows):
        raise RegistryWorkflowError("decision ticker differs from exact source rows")
    if claims.get("outcome_or_backtest_evidence_used") is not False:
        raise RegistryWorkflowError("returns/factors/backtests cannot be registry evidence")
    if claims.get("identity_quality_liquidation_signal") is not False:
        raise RegistryWorkflowError("identity quality cannot trigger liquidation")

    if registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
        if any(
            row.observed_composite_figi != claims["observed_composite_figi"] for row in scope.rows
        ):
            raise RegistryWorkflowError("bounce adjudication scope Composite differs")
        _claim_interval(
            claims,
            scope,
            "episode_valid_from_session",
            "episode_valid_through_session",
        )
        return
    if registry_name == RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value:
        if (
            claims.get("provider_id") != "massive"
            or claims.get("provider_market") != "stocks"
            or claims.get("provider_locale") != "us"
            or claims.get("observed_composite_market_code") == "US"
            or claims.get("canonical_composite_market_code") not in {"US", None}
            or claims.get("membership_effect") != "none"
            or claims.get("active_status_effect") != "none"
        ):
            raise RegistryWorkflowError("cross-market registry responsibility changed")
        if any(
            row.observed_composite_figi != claims["observed_foreign_composite_figi"]
            or row.observed_share_class_figi != claims["share_class_figi"]
            for row in scope.rows
        ):
            raise RegistryWorkflowError("cross-market exact hierarchy scope differs")
        case_ids = _canonical_json_text(
            claims["related_identity_case_ids_json"], "related identity case IDs"
        )
        case_roles = _canonical_json_text(
            claims["related_identity_case_roles_json"], "related identity case roles"
        )
        if (
            not isinstance(case_ids, list)
            or not all(isinstance(item, str) and _DIGEST.fullmatch(item) for item in case_ids)
            or case_ids != sorted(set(case_ids))
            or not isinstance(case_roles, dict)
            or set(case_roles) != set(case_ids)
            or any(
                role
                not in {
                    "contaminated_middle_episode",
                    "inverse_middle_is_canonical_us",
                }
                for role in case_roles.values()
            )
            or claims["related_identity_case_count"] != len(case_ids)
        ):
            raise RegistryWorkflowError("cross-market linked identity-case lineage changed")
        _claim_interval(claims, scope, "valid_from_session", "valid_through_session")
        return
    if registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value:
        if (
            claims.get("provider_id") != "massive"
            or claims.get("provider_market") != "stocks"
            or claims.get("provider_locale") != "us"
            or claims.get("observed_composite_market_code") != "US"
            or claims.get("canonical_composite_market_code") not in {"US", None}
            or claims.get("membership_effect") != "none"
            or claims.get("active_status_effect") != "none"
        ):
            raise RegistryWorkflowError(
                "provider override must remain same-market and identity-only"
            )
        _digest(claims.get("asset_transition_id"), "bound asset transition ID")
        if any(
            row.observed_composite_figi != claims["observed_composite_figi"] for row in scope.rows
        ):
            raise RegistryWorkflowError("provider override exact Composite scope differs")
        _claim_interval(claims, scope, "valid_from_session", "valid_through_session")
        return
    if registry_name == RegistryName.SHARE_CLASS_ADJUDICATION.value:
        if (
            claims.get("composite_identity_effect") != "none"
            or claims.get("asset_id_effect") != "none"
            or claims.get("issuer_identity_effect") != "none"
            or claims.get("membership_effect") != "none"
            or claims.get("tradability_effect") != "none"
        ):
            raise RegistryWorkflowError("Share Class adjudication exceeded hierarchy scope")
        if any(
            row.observed_composite_figi != claims["observed_composite_figi"]
            or row.observed_share_class_figi != claims["observed_share_class_figi"]
            for row in scope.rows
        ):
            raise RegistryWorkflowError("Share Class exact source scope differs")
        _claim_interval(claims, scope, "valid_from_session", "valid_through_session")
        return
    if (
        claims.get("relationship_effect") != "lineage_only_no_override_no_return_stitching"
        or claims.get("identity_override_effect") != "none"
        or claims.get("membership_effect") != "none"
        or claims.get("tradability_effect") != "none"
        or claims.get("return_stitching_effect") != "none_requires_future_entitlement_accounting"
    ):
        raise RegistryWorkflowError("asset transition exceeded lineage-only responsibility")
    boundary_dates = {row.session_date for row in scope.rows}
    expected = {
        date.fromisoformat(str(claims["predecessor_last_session"])),
        date.fromisoformat(str(claims["successor_first_session"])),
    }
    if boundary_dates != expected:
        raise RegistryWorkflowError("asset transition boundary source scope changed")


def _validate_relation_derived_ids(
    registry_name: str,
    claims: Mapping[str, object],
    scope: ExactSourceScope,
    decision_id: str,
) -> None:
    """Recompute every row-local derived ID for the three relation registries.

    Content-addressing the enclosing candidate is not enough: without this replay,
    caller-authored subject/series/asset/decision IDs can be internally consistent
    bytes while carrying false relation semantics.
    """

    if registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value:
        source_digest = stable_digest(list(scope.source_record_ids))
        subject_id = stable_digest(
            {
                "asset_transition_series_id": claims["asset_transition_series_id"],
                "namespace": "ame_stocks.identity.provider_composite_override_subject",
                "observed_composite_figi": claims["observed_composite_figi"],
                "observed_ticker": claims["observed_ticker"],
                "provider_id": claims["provider_id"],
                "provider_locale": claims["provider_locale"],
                "provider_market": claims["provider_market"],
                "rule_version": "s7_provider_composite_override_subject_id_v1",
            }
        )
        series_id = stable_digest(
            {
                "namespace": "ame_stocks.identity.provider_composite_override_series",
                "provider_composite_override_subject_id": subject_id,
                "rule_version": "s7_provider_composite_override_series_id_v1",
            }
        )
        canonical_figi = claims["canonical_composite_figi"]
        canonical_id = None if canonical_figi is None else canonical_asset_id(str(canonical_figi))
        expected_id = stable_digest(
            {
                "asset_transition_id": claims["asset_transition_id"],
                "canonical_composite_figi": canonical_figi,
                "decision_version": claims["decision_version"],
                "disposition": claims["disposition"],
                "namespace": "ame_stocks.identity.provider_composite_override",
                "provider_composite_override_series_id": series_id,
                "reason_code": claims["reason_code"],
                "reason_detail": claims["reason_detail"],
                "rule_version": "s7_provider_composite_override_id_v1",
                "source_exact_group_candidate_manifest_id": claims[
                    "source_exact_group_candidate_manifest_id"
                ],
                "source_record_set_digest": source_digest,
                "source_s4_release_set_id": claims["source_s4_release_set_id"],
                "supersedes_provider_composite_override_id": claims[
                    "supersedes_provider_composite_override_id"
                ],
                "valid_from_session": claims["valid_from_session"],
                "valid_through_session": claims["valid_through_session"],
            }
        )
        expected = {
            "canonical_asset_id": canonical_id,
            "provider_composite_override_id": expected_id,
            "provider_composite_override_series_id": series_id,
            "provider_composite_override_subject_id": subject_id,
            "scoped_source_record_set_digest": source_digest,
        }
    elif registry_name == RegistryName.SHARE_CLASS_ADJUDICATION.value:
        source_digest = stable_digest(list(scope.source_record_ids))
        subject_id = stable_digest(
            {
                "namespace": "ame_stocks.identity.share_class_adjudication_subject",
                "observed_composite_figi": claims["observed_composite_figi"],
                "observed_share_class_figi": claims["observed_share_class_figi"],
                "observed_ticker": claims["observed_ticker"],
                "provider_id": claims["provider_id"],
                "provider_locale": claims["provider_locale"],
                "provider_market": claims["provider_market"],
                "rule_version": "s7_share_class_adjudication_subject_id_v1",
            }
        )
        series_id = stable_digest(
            {
                "namespace": "ame_stocks.identity.share_class_adjudication_series",
                "rule_version": "s7_share_class_adjudication_series_id_v1",
                "share_class_adjudication_subject_id": subject_id,
            }
        )
        canonical_figi = claims["canonical_share_class_figi"]
        canonical_id = (
            None if canonical_figi is None else canonical_share_class_id(str(canonical_figi))
        )
        expected_id = stable_digest(
            {
                "canonical_share_class_figi": canonical_figi,
                "decision_version": claims["decision_version"],
                "disposition": claims["disposition"],
                "namespace": "ame_stocks.identity.share_class_adjudication",
                "reason_code": claims["reason_code"],
                "reason_detail": claims["reason_detail"],
                "required_unique_canonical_composite_figi": claims[
                    "required_unique_canonical_composite_figi"
                ],
                "rule_version": "s7_share_class_adjudication_id_v1",
                "share_class_adjudication_series_id": series_id,
                "source_exact_group_candidate_manifest_id": claims[
                    "source_exact_group_candidate_manifest_id"
                ],
                "source_record_set_digest": source_digest,
                "source_s4_release_set_id": claims["source_s4_release_set_id"],
                "supersedes_share_class_adjudication_id": claims[
                    "supersedes_share_class_adjudication_id"
                ],
                "valid_from_session": claims["valid_from_session"],
                "valid_through_session": claims["valid_through_session"],
            }
        )
        expected = {
            "canonical_share_class_id": canonical_id,
            "scoped_source_record_set_digest": source_digest,
            "share_class_adjudication_id": expected_id,
            "share_class_adjudication_series_id": series_id,
            "share_class_adjudication_subject_id": subject_id,
        }
    elif registry_name == RegistryName.ASSET_TRANSITION.value:
        source_digest = stable_digest(list(scope.source_record_ids))
        subject_id = stable_digest(
            {
                "legal_effective_date": claims["legal_effective_date"],
                "namespace": "ame_stocks.identity.asset_transition_subject",
                "observed_ticker": claims["observed_ticker"],
                "provider_id": claims["provider_id"],
                "provider_locale": claims["provider_locale"],
                "provider_market": claims["provider_market"],
                "rule_version": "s7_asset_transition_subject_id_v1",
                "transition_type": claims["transition_type"],
            }
        )
        series_id = stable_digest(
            {
                "asset_transition_subject_id": subject_id,
                "namespace": "ame_stocks.identity.asset_transition_series",
                "rule_version": "s7_asset_transition_series_id_v1",
            }
        )
        successor_figi = claims["successor_composite_figi"]
        predecessor_asset_id = canonical_asset_id(str(claims["predecessor_composite_figi"]))
        successor_asset_id = (
            None if successor_figi is None else canonical_asset_id(str(successor_figi))
        )
        expected_id = stable_digest(
            {
                "asset_transition_series_id": series_id,
                "boundary_source_record_set_digest": source_digest,
                "decision_version": claims["decision_version"],
                "disposition": claims["disposition"],
                "namespace": "ame_stocks.identity.asset_transition",
                "predecessor_composite_figi": claims["predecessor_composite_figi"],
                "predecessor_last_session": claims["predecessor_last_session"],
                "reason_code": claims["reason_code"],
                "reason_detail": claims["reason_detail"],
                "rule_version": "s7_asset_transition_id_v1",
                "source_exact_group_candidate_manifest_id": claims[
                    "source_exact_group_candidate_manifest_id"
                ],
                "source_s4_release_set_id": claims["source_s4_release_set_id"],
                "successor_composite_figi": successor_figi,
                "successor_first_session": claims["successor_first_session"],
                "supersedes_asset_transition_id": claims["supersedes_asset_transition_id"],
            }
        )
        expected = {
            "asset_transition_id": expected_id,
            "asset_transition_series_id": series_id,
            "asset_transition_subject_id": subject_id,
            "boundary_source_record_set_digest": source_digest,
            "predecessor_asset_id": predecessor_asset_id,
            "successor_asset_id": successor_asset_id,
        }
    else:
        return

    for field, expected_value in expected.items():
        if claims.get(field) != expected_value:
            raise RegistryWorkflowError(f"relation derived field recomputation failed: {field}")
    if decision_id != expected_id:
        raise RegistryWorkflowError("relation decision ID recomputation failed")


def _claim_interval(
    claims: Mapping[str, object],
    scope: ExactSourceScope,
    start_column: str,
    end_column: str,
) -> None:
    start = date.fromisoformat(str(claims[start_column]))
    end = date.fromisoformat(str(claims[end_column]))
    if start > end or any(not start <= row.session_date <= end for row in scope.rows):
        raise RegistryWorkflowError("decision interval differs from exact source scope")
    if (
        min(row.session_date for row in scope.rows) != start
        or max(row.session_date for row in scope.rows) != end
    ):
        raise RegistryWorkflowError("decision interval endpoints lack exact source rows")


def _validate_candidate_chains(
    registry_name: str,
    decisions: Sequence[RegistryDecisionCandidate],
) -> None:
    claims = [item.frozen_row_claims for item in decisions]
    _validate_final_chains(registry_name, claims)


def _validate_candidate_artifact_claims(candidate: RegistryCandidateManifest) -> None:
    admitted = {
        (item.artifact_id, item.sha256)
        for item in (*candidate.source_artifacts, *candidate.evidence_artifacts)
    }
    transitive_identity_case = _production_cross_market_identity_case_binding(candidate)
    for intent in candidate.decisions:
        claims = intent.frozen_row_claims
        if (
            claims.get("availability_calendar_id") != candidate.availability_calendar_id
            or claims.get("availability_calendar_sha256") != candidate.availability_calendar_sha256
        ):
            raise RegistryWorkflowError("decision calendar differs from candidate calendar")
        if (
            "candidate_available_session" in claims
            and claims["candidate_available_session"]
            != candidate.candidate_available_session.isoformat()
        ):
            raise RegistryWorkflowError("decision candidate availability differs")
        for field, artifact_id in claims.items():
            if not field.endswith("_manifest_id") or artifact_id is None:
                continue
            sha_field = f"{field[:-2]}sha256"
            if sha_field not in claims:
                continue
            artifact_sha = claims[sha_field]
            if (artifact_id, artifact_sha) not in admitted:
                if (
                    field == "source_identity_case_candidate_manifest_id"
                    and transitive_identity_case == (artifact_id, artifact_sha)
                ):
                    continue
                raise RegistryWorkflowError(
                    f"decision manifest binding is absent from candidate artifacts: {field}"
                )


def _production_cross_market_identity_case_binding(
    candidate: RegistryCandidateManifest,
) -> tuple[str, str] | None:
    """Admit only the detector preview transitively pinned by exact Gate C.

    The frozen production source authorization deliberately binds the Gate C
    candidate/completion pair.  Gate C in turn binds the immutable detector
    preview.  Cross-market rows preserve that preview as their identity-case
    lineage, so this one claim is admitted transitively and is replayed against
    Gate C before candidate storage.  No other registry or manifest claim may
    use this exception.
    """

    if (
        candidate.registry_name != RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value
        or candidate.production_ingress_artifact is None
        or not candidate.decisions
        or {item.role for item in candidate.source_artifacts}
        != {
            "source_gate_c_candidate_manifest",
            "source_gate_c_completion_manifest",
        }
    ):
        return None
    bindings = {
        (
            item.frozen_row_claims.get("source_identity_case_candidate_manifest_id"),
            item.frozen_row_claims.get("source_identity_case_candidate_manifest_sha256"),
        )
        for item in candidate.decisions
    }
    if len(bindings) != 1:
        return None
    artifact_id, sha256 = next(iter(bindings))
    if not isinstance(artifact_id, str) or not isinstance(sha256, str):
        return None
    return artifact_id, sha256


def _validate_final_chains(
    registry_name: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    id_column = _DECISION_ID_COLUMN[registry_name]
    series_column = _SERIES_ID_COLUMN[registry_name]
    predecessor_column = _PREDECESSOR_COLUMN[registry_name]
    available_column = _AVAILABLE_SESSION_COLUMN[registry_name]
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        series_id = _digest(row[series_column], "decision series ID")
        grouped[series_id].append(row)
    for series_rows in grouped.values():
        ordered = sorted(series_rows, key=lambda item: int(item["decision_version"]))
        prior_id: str | None = None
        prior_available: date | None = None
        for expected_version, row in enumerate(ordered, start=1):
            if row["decision_version"] != expected_version:
                raise RegistryWorkflowError("decision versions are not contiguous")
            if row[predecessor_column] != prior_id:
                raise RegistryWorkflowError("decision predecessor chain changed")
            current_id = _digest(row[id_column], "decision ID")
            current_available = row.get(available_column)
            # Candidate claims omit the post-approval availability column.
            if current_available is not None:
                if not isinstance(current_available, date) or isinstance(
                    current_available, datetime
                ):
                    raise RegistryWorkflowError("decision availability type changed")
                if prior_available is not None and current_available < prior_available:
                    raise RegistryWorkflowError("decision availability moves backward")
                prior_available = current_available
            prior_id = current_id


def _build_registry_table(
    registry_name: str,
    decision_rows: Sequence[Mapping[str, object]],
    candidate: RegistryCandidateManifest,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    receipt: RegistryReleaseAuthorizationReceipt,
    receipt_document: StoredControlDocument,
) -> tuple[list[dict[str, object]], pa.Table]:
    contract = _CONTRACTS[registry_name]
    columns = tuple(column.name for column in contract.columns)
    raw: list[dict[str, object]] = []
    for value in decision_rows:
        row = dict(value)
        if set(row) != set(columns):
            raise RegistryWorkflowError("release row fields differ from exact contract")
        raw.append({column: row[column] for column in columns})
    try:
        table = pa.Table.from_pylist(raw, schema=contract.arrow_schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise RegistryWorkflowError("cannot construct exact registry Arrow table") from exc
    if table.schema != contract.arrow_schema:
        raise RegistryWorkflowError("registry Arrow schema changed")
    for field, column in zip(table.schema, table.columns, strict=True):
        if not field.nullable and column.null_count:
            raise RegistryWorkflowError(f"registry required column has nulls: {field.name}")
    if table.num_rows:
        table = table.sort_by([(name, "ascending") for name in contract.sort_by])
    rows = table.to_pylist()
    ids = [str(row[_DECISION_ID_COLUMN[registry_name]]) for row in rows]
    if len(ids) != len(set(ids)) or set(ids) != {item.decision_id for item in candidate.decisions}:
        raise RegistryWorkflowError("release row set differs from approved candidate")
    intents = {item.decision_id: item for item in candidate.decisions}
    for row in rows:
        _validate_final_row(
            registry_name,
            row,
            intents[str(row[_DECISION_ID_COLUMN[registry_name]])],
            candidate,
            plan_document,
            request,
            request_document,
            receipt,
            receipt_document,
        )
    _validate_final_chains(registry_name, rows)
    return rows, table


def _validate_final_row(
    registry_name: str,
    row: Mapping[str, object],
    intent: RegistryDecisionCandidate,
    candidate: RegistryCandidateManifest,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    receipt: RegistryReleaseAuthorizationReceipt,
    receipt_document: StoredControlDocument,
) -> None:
    normalized = _json_value(row)
    assert isinstance(normalized, dict)
    for column, expected in intent.frozen_row_claims.items():
        if normalized.get(column) != expected:
            raise RegistryWorkflowError(
                f"released decision changed frozen candidate field {column}"
            )
    _validate_scope_projection(registry_name, normalized, intent.source_scope)
    _validate_registry_responsibility(registry_name, normalized, intent.source_scope)

    common = {
        "approved_by": receipt.approved_by,
        "approved_at_utc": receipt.approved_at_utc,
        "approval_available_session": receipt.approval_available_session,
    }
    for column, expected in common.items():
        if row[column] != expected:
            raise RegistryWorkflowError(f"released decision approval field changed: {column}")
    if registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
        expected_controls = {
            "approval_id": receipt.receipt_id,
            "approval_receipt_path": receipt_document.path,
            "approval_receipt_sha256": receipt_document.sha256,
            "approval_status": APPROVED,
            "source_decision_plan_id": request.plan.object_id,
            "source_decision_plan_path": plan_document.path,
            "source_decision_plan_sha256": plan_document.sha256,
        }
    elif registry_name == RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value:
        expected_controls = {
            "approval_status": APPROVED,
            "approval_request_event_id": request.request_event_id,
            "approval_request_event_sha256": hashlib.sha256(request.literal_bytes()).hexdigest(),
            "approval_receipt_id": receipt.receipt_id,
            "approval_receipt_path": receipt_document.path,
            "approval_receipt_sha256": receipt_document.sha256,
        }
    else:
        expected_controls = {
            "source_decision_plan_id": request.plan.object_id,
            "source_decision_plan_path": plan_document.path,
            "source_decision_plan_sha256": plan_document.sha256,
            "approval_request_event_id": request.request_event_id,
            "approval_request_event_sha256": hashlib.sha256(request.literal_bytes()).hexdigest(),
            "approval_receipt_id": receipt.receipt_id,
            "approval_receipt_sha256": receipt_document.sha256,
        }
    for column, expected in expected_controls.items():
        if row[column] != expected:
            raise RegistryWorkflowError(f"released decision control field changed: {column}")
    expected_available = _expected_decision_available_session(
        registry_name,
        row,
        candidate,
        receipt,
    )
    if row[_AVAILABLE_SESSION_COLUMN[registry_name]] != expected_available:
        raise RegistryWorkflowError("released decision availability recomputation failed")


def _expected_decision_available_session(
    registry_name: str,
    row: Mapping[str, object],
    candidate: RegistryCandidateManifest,
    receipt: RegistryReleaseAuthorizationReceipt,
) -> date:
    upstream = [candidate.candidate_available_session, receipt.approval_available_session]
    if registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
        upstream.extend((row["identity_case_available_session"], row["evidence_cutoff_session"]))
    elif registry_name == RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value:
        upstream.extend(
            (row["candidate_available_session"], row["external_evidence_available_session"])
        )
    elif registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value:
        upstream.extend(
            (
                row["candidate_available_session"],
                row["asset_transition_available_session"],
                row["external_evidence_available_session"],
            )
        )
    else:
        upstream.extend(
            (row["candidate_available_session"], row["external_evidence_available_session"])
        )
    if any(not isinstance(item, date) or isinstance(item, datetime) for item in upstream):
        raise RegistryWorkflowError("decision upstream availability type changed")
    return max(upstream)


def _validate_decision_row_calendar(
    registry_name: str,
    rows: Sequence[Mapping[str, object]],
    calendar: object,
) -> None:
    for row in rows:
        available = row[_AVAILABLE_SESSION_COLUMN[registry_name]]
        try:
            market_open = calendar.market_open(available)  # type: ignore[attr-defined]
        except XNYSCalendarArtifactError as exc:
            raise RegistryWorkflowError("decision availability is absent from calendar") from exc
        if (
            registry_name == RegistryName.IDENTITY_ADJUDICATION.value
            and row["adjudication_available_at_utc"] != market_open
        ):
            raise RegistryWorkflowError(
                "identity adjudication availability timestamp differs from XNYS open"
            )


def _verify_candidate_inputs(root: Path, candidate: RegistryCandidateManifest) -> None:
    production = is_canonical_production_data_root(root)
    if production != (candidate.production_ingress_artifact is not None):
        raise RegistryWorkflowError("candidate production provenance/root binding changed")
    calendar = _load_calendar(
        root,
        candidate.availability_calendar_id,
        candidate.availability_calendar_sha256,
    )
    first_created_session, _ = calendar.first_open_after(candidate.created_at_utc)
    expected = max(
        (
            first_created_session,
            *(item.available_session for item in candidate.source_artifacts),
            *(item.available_session for item in candidate.evidence_artifacts),
            *(item.available_session for item in candidate.authorization_artifacts),
            *(
                ()
                if candidate.production_ingress_artifact is None
                else (candidate.production_ingress_artifact.available_session,)
            ),
        )
    )
    if candidate.candidate_available_session != expected:
        raise RegistryWorkflowError("candidate availability recomputation failed")
    authorization_documents: dict[str, Mapping[str, object]] = {}
    for artifact in (
        *candidate.source_artifacts,
        *candidate.evidence_artifacts,
        *candidate.authorization_artifacts,
        *(
            ()
            if candidate.production_ingress_artifact is None
            else (candidate.production_ingress_artifact,)
        ),
    ):
        content = _read_exact(
            root,
            artifact.path,
            expected_sha256=artifact.sha256,
            expected_bytes=artifact.bytes,
        )
        if artifact.embedded_id_field is not None:
            try:
                decoded = json.loads(content)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RegistryWorkflowError(f"{artifact.role} is not valid UTF-8 JSON") from exc
            document = _mapping(decoded, artifact.role)
            if document.get(artifact.embedded_id_field) != artifact.artifact_id:
                raise RegistryWorkflowError(
                    f"{artifact.role} embedded object ID differs from its binding"
                )
            if artifact in candidate.authorization_artifacts:
                if _canonical_bytes(document) != content:
                    raise RegistryWorkflowError("authorization artifact is not canonical JSON")
                authorization_documents[artifact.role] = document
    _validate_candidate_authorizations(
        root,
        candidate,
        calendar,
        authorization_documents,
    )
    _validate_exact_group_relation_scopes(root, candidate)
    _validate_gate_c_registry_scopes(root, candidate)
    if candidate.production_ingress_artifact is not None:
        from ame_stocks_api.silver.identity_registry_production import (
            IdentityRegistryProductionError,
            validate_production_candidate_rebuild,
        )

        try:
            validate_production_candidate_rebuild(root, candidate)
        except IdentityRegistryProductionError as exc:
            raise RegistryWorkflowError("production candidate ingress replay failed") from exc


def _validate_exact_group_relation_scopes(
    root: Path,
    candidate: RegistryCandidateManifest,
) -> None:
    """Replay relation-registry scopes from the exact candidate/completion pair.

    Importing lazily keeps the exact-group loader independent from this workflow,
    while making hand-written source IDs impossible on every stored/reloaded
    production relation candidate.
    """

    relation_registries = {
        RegistryName.ASSET_TRANSITION.value,
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        RegistryName.SHARE_CLASS_ADJUDICATION.value,
    }
    if candidate.registry_name not in relation_registries:
        return
    expected_roles = {
        "source_exact_group_candidate_manifest",
        "source_exact_group_completion_manifest",
    }
    by_role = {item.role: item for item in candidate.source_artifacts}
    if set(by_role) != expected_roles:
        raise RegistryWorkflowError(
            "relation candidate requires the exact-group candidate/completion pair"
        )
    try:
        from ame_stocks_api.silver.identity_registry_exact_group_scopes import (
            IdentityRegistryExactGroupScopeError,
            load_identity_registry_exact_group_scopes,
        )

        loaded = load_identity_registry_exact_group_scopes(
            root,
            candidate_pin=by_role["source_exact_group_candidate_manifest"],
            completion_pin=by_role["source_exact_group_completion_manifest"],
        )
        for decision in candidate.decisions:
            canonical_scope = ExactSourceScope.from_dict(
                loaded.require_scope(decision.case_key).to_dict()
            )
            if decision.source_scope != canonical_scope:
                raise RegistryWorkflowError(
                    "relation decision scope differs from exact-group selected-parent replay"
                )
    except IdentityRegistryExactGroupScopeError as exc:
        raise RegistryWorkflowError("exact-group relation source replay failed") from exc


def _validate_gate_c_registry_scopes(
    root: Path,
    candidate: RegistryCandidateManifest,
) -> None:
    if candidate.registry_name not in {
        RegistryName.IDENTITY_ADJUDICATION.value,
        RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
    }:
        return
    by_role = {item.role: item for item in candidate.source_artifacts}
    if set(by_role) != {
        "source_gate_c_candidate_manifest",
        "source_gate_c_completion_manifest",
    }:
        raise RegistryWorkflowError("episode/cross-market candidate requires the exact Gate C pair")
    from ame_stocks_api.silver.identity_registry_production import (
        IdentityRegistryProductionError,
        _load_gate_c_source,
    )

    completion = by_role["source_gate_c_completion_manifest"]
    try:
        loaded = _load_gate_c_source(
            root,
            StoredControlDocument(
                object_id=completion.artifact_id,
                path=completion.path,
                sha256=completion.sha256,
                bytes=completion.bytes,
            ),
        )
    except IdentityRegistryProductionError as exc:
        raise RegistryWorkflowError("Gate C registry source replay failed") from exc
    if (
        loaded.candidate != by_role["source_gate_c_candidate_manifest"]
        or loaded.completion != completion
    ):
        raise RegistryWorkflowError("Gate C candidate/completion binding changed")
    if candidate.registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
        if candidate.decisions:
            raise RegistryWorkflowError(
                "fixed Gate C package cannot create duplicate episode adjudications"
            )
        return
    for decision in candidate.decisions:
        try:
            scope = loaded.scopes[decision.case_key]
        except KeyError as exc:
            raise RegistryWorkflowError("cross-market case is absent from exact Gate C") from exc
        if decision.source_scope != scope:
            raise RegistryWorkflowError("cross-market scope differs from exact Gate C replay")
        claims = decision.frozen_row_claims
        if (
            claims.get("source_identity_case_candidate_manifest_id")
            != loaded.detector_preview.artifact_id
            or claims.get("source_identity_case_candidate_manifest_sha256")
            != loaded.detector_preview.sha256
        ):
            raise RegistryWorkflowError(
                "cross-market identity-case binding differs from exact Gate C replay"
            )
        if (
            claims.get("source_identity_market_consistency_candidate_manifest_id")
            != loaded.candidate.artifact_id
            or claims.get("source_identity_market_consistency_candidate_manifest_sha256")
            != loaded.candidate.sha256
        ):
            raise RegistryWorkflowError(
                "cross-market market-consistency binding differs from exact Gate C replay"
            )


def _validate_candidate_authorizations(
    root: Path,
    candidate: RegistryCandidateManifest,
    calendar: object,
    documents: Mapping[str, Mapping[str, object]],
) -> None:
    if set(documents) != {item.role for item in candidate.authorization_artifacts}:
        raise RegistryWorkflowError("candidate authorization artifacts were not replayed")
    expected_targets = {
        "schema_contract_approval": (
            (candidate.contract_pin.contract_id, candidate.contract_pin.resource_sha256),
        ),
        "source_candidate_approval": tuple(
            sorted((item.artifact_id, item.sha256) for item in candidate.source_artifacts)
        ),
        "external_evidence_approval": tuple(
            sorted((item.artifact_id, item.sha256) for item in candidate.evidence_artifacts)
        ),
    }
    for artifact in candidate.authorization_artifacts:
        document = documents[artifact.role]
        if is_canonical_production_data_root(root):
            _validate_production_prerequisite_authorization_document(
                document,
                artifact=artifact,
                registry_name=candidate.registry_name,
                expected_targets=expected_targets[artifact.role],
                calendar=calendar,
                root=root,
                revalidate_runtime=True,
            )
            continue
        if document.get("authorization_mode") == STANDING_AUTHORIZATION_VERSION:
            _validate_standing_candidate_authorization_document(
                document,
                artifact=artifact,
                registry_name=candidate.registry_name,
                expected_targets=expected_targets[artifact.role],
                calendar=calendar,
                revalidate_runtime=True,
            )
            continue
        expected_keys = {
            "approval_available_session",
            "approved_at_utc",
            "approved_by",
            "authorization_id",
            "authorization_type",
            "decision",
            "policy_version",
            "registry_name",
            "target_refs",
        }
        _expect_keys(document, expected_keys, "candidate authorization")
        logical = {key: value for key, value in document.items() if key != "authorization_id"}
        raw_targets = _array(document["target_refs"])
        targets: list[tuple[str, str]] = []
        for raw in raw_targets:
            item = _mapping(raw, "authorization target")
            _expect_keys(item, {"artifact_id", "sha256"}, "authorization target")
            targets.append(
                (
                    _digest(item["artifact_id"], "authorization target ID"),
                    _digest(item["sha256"], "authorization target SHA-256"),
                )
            )
        approved_at = _parse_utc(_text(document["approved_at_utc"], "authorization time"))
        available = date.fromisoformat(
            _text(document["approval_available_session"], "authorization availability")
        )
        if (
            document["authorization_id"] != artifact.artifact_id
            or document["authorization_id"] != stable_digest(logical)
            or document["authorization_type"] != artifact.role
            or document["registry_name"] != candidate.registry_name
            or document["decision"] != APPROVED
            or document["policy_version"] != WORKFLOW_POLICY
            or tuple(targets) != expected_targets[artifact.role]
            or available != artifact.available_session
        ):
            raise RegistryWorkflowError("candidate authorization binding changed")
        try:
            # ``calendar`` is the exact XNYS artifact loaded above; keeping the helper
            # structurally typed avoids leaking it into the serialized model.
            calendar.require_first_open_session(  # type: ignore[attr-defined]
                approved_at,
                available,
                label="candidate authorization availability",
            )
        except XNYSCalendarArtifactError as exc:
            raise RegistryWorkflowError(str(exc)) from exc


def _validate_plan_candidate(
    plan: RegistryDecisionPlan,
    candidate: RegistryCandidateManifest,
) -> None:
    if (
        plan.registry_name != candidate.registry_name
        or plan.contract_pin != candidate.contract_pin
        or plan.candidate.object_id != candidate.candidate_id
        or plan.candidate.path != candidate.relative_path
        or plan.decision_ids != tuple(item.decision_id for item in candidate.decisions)
        or plan.source_scope_set_digest != candidate.source_scope_set_digest
        or plan.availability_calendar_id != candidate.availability_calendar_id
        or plan.availability_calendar_sha256 != candidate.availability_calendar_sha256
    ):
        raise RegistryWorkflowError("decision plan differs from exact candidate")


def _validate_request_chain(
    request: RegistryApprovalRequest,
    plan: RegistryDecisionPlan,
    candidate: RegistryCandidateManifest,
) -> None:
    _validate_plan_candidate(plan, candidate)
    if (
        request.registry_name != plan.registry_name
        or request.contract_pin != plan.contract_pin
        or request.plan.object_id != plan.plan_id
        or request.plan.path != plan.relative_path
        or request.candidate != plan.candidate
        or request.decision_ids != plan.decision_ids
        or request.source_scope_set_digest != plan.source_scope_set_digest
        or request.availability_calendar_id != plan.availability_calendar_id
        or request.availability_calendar_sha256 != plan.availability_calendar_sha256
    ):
        raise RegistryWorkflowError("approval request differs from decision plan")


def _build_standing_qa_review(
    candidate: RegistryCandidateManifest,
    plan: RegistryDecisionPlan,
    request: RegistryApprovalRequest,
) -> dict[str, object]:
    """Run the deterministic gate required by the standing authorization path."""

    _validate_request_chain(request, plan, candidate)
    reviewed_case_keys: list[str] = []
    for decision in candidate.decisions:
        try:
            validate_fixed_decision_candidate(decision)
        except RegistryWorkflowError as exc:
            raise RegistryWorkflowError(
                "standing authorization cannot establish zero factual contradictions "
                f"for unreviewed or changed case {decision.case_key}"
            ) from exc
        reviewed_case_keys.append(decision.case_key)
    scope_reviews = [
        {
            "decision_id": decision.decision_id,
            "row_count": len(decision.source_scope.rows),
            "scope_digest": decision.source_scope.scope_digest,
            "source_record_set_digest": decision.source_scope.source_record_set_digest,
        }
        for decision in candidate.decisions
    ]
    checks = [
        {
            "check_id": check_id,
            "severity": "Critical",
            "status": "passed",
        }
        for check_id in (
            "candidate_plan_request_exact_replay",
            "contract_pin_exact_replay",
            "source_evidence_authorization_exact_replay",
            "exact_source_scope_integrity",
            "fixed_case_factual_consistency",
            "single_registry_scope",
        )
    ]
    logical: dict[str, object] = {
        "checks": checks,
        "decision_count": len(candidate.decisions),
        "decision_ids": list(request.decision_ids),
        "factual_contradiction_count": 0,
        "factual_contradictions": [],
        "open_critical_qa_count": 0,
        "plan_id": plan.plan_id,
        "policy_version": WORKFLOW_POLICY,
        "registry_name": candidate.registry_name,
        "request_event_id": request.request_event_id,
        "review_version": WORKFLOW_VERSION,
        "reviewed_case_keys": reviewed_case_keys,
        "source_scope_reviews": scope_reviews,
        "source_scope_set_digest": candidate.source_scope_set_digest,
        "status": "passed",
    }
    return {"review_id": stable_digest(logical), **logical}


def _validate_standing_qa_review(review: Mapping[str, object]) -> None:
    expected = {
        "checks",
        "decision_count",
        "decision_ids",
        "factual_contradiction_count",
        "factual_contradictions",
        "open_critical_qa_count",
        "plan_id",
        "policy_version",
        "registry_name",
        "request_event_id",
        "review_id",
        "review_version",
        "reviewed_case_keys",
        "source_scope_reviews",
        "source_scope_set_digest",
        "status",
    }
    _expect_keys(review, expected, "standing QA review")
    if (
        review["review_version"] != WORKFLOW_VERSION
        or review["policy_version"] != WORKFLOW_POLICY
        or review["status"] != "passed"
        or review["open_critical_qa_count"] != 0
        or review["factual_contradiction_count"] != 0
        or review["factual_contradictions"] != []
    ):
        raise RegistryWorkflowError("standing QA review is not clean")
    _registry(review["registry_name"])
    _digest(review["plan_id"], "standing review plan ID")
    _digest(review["request_event_id"], "standing review request event ID")
    _digest(review["source_scope_set_digest"], "standing review scope-set digest")
    decisions = [
        _digest(value, "standing review decision ID") for value in _array(review["decision_ids"])
    ]
    if decisions != sorted(decisions) or len(set(decisions)) != len(decisions):
        raise RegistryWorkflowError("standing review decision IDs changed")
    if review["decision_count"] != len(decisions):
        raise RegistryWorkflowError("standing review decision count changed")
    case_keys = [
        _text(value, "standing reviewed case key") for value in _array(review["reviewed_case_keys"])
    ]
    if len(case_keys) != len(decisions):
        raise RegistryWorkflowError("standing reviewed case coverage changed")
    scopes = _array(review["source_scope_reviews"])
    if len(scopes) != len(decisions):
        raise RegistryWorkflowError("standing source-scope review coverage changed")
    scope_ids: list[str] = []
    for value in scopes:
        scope = _mapping(value, "standing source-scope review")
        _expect_keys(
            scope,
            {"decision_id", "row_count", "scope_digest", "source_record_set_digest"},
            "standing source-scope review",
        )
        scope_ids.append(_digest(scope["decision_id"], "standing scope decision ID"))
        _positive(scope["row_count"], "standing scope row count")
        _digest(scope["scope_digest"], "standing scope digest")
        _digest(scope["source_record_set_digest"], "standing source-record-set digest")
    if scope_ids != decisions:
        raise RegistryWorkflowError("standing reviewed source scopes changed")
    expected_check_ids = (
        "candidate_plan_request_exact_replay",
        "contract_pin_exact_replay",
        "source_evidence_authorization_exact_replay",
        "exact_source_scope_integrity",
        "fixed_case_factual_consistency",
        "single_registry_scope",
    )
    checks = _array(review["checks"])
    if len(checks) != len(expected_check_ids):
        raise RegistryWorkflowError("standing QA check coverage changed")
    for value, check_id in zip(checks, expected_check_ids, strict=True):
        check = _mapping(value, "standing QA check")
        _expect_keys(check, {"check_id", "severity", "status"}, "standing QA check")
        if check != {"check_id": check_id, "severity": "Critical", "status": "passed"}:
            raise RegistryWorkflowError("standing QA check result changed")
    logical = {key: value for key, value in review.items() if key != "review_id"}
    if review["review_id"] != stable_digest(logical):
        raise RegistryWorkflowError("standing QA review ID recomputation failed")


def _validate_release_control_chain(
    candidate: RegistryCandidateManifest,
    plan: RegistryDecisionPlan,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    receipt: RegistryReleaseAuthorizationReceipt,
    receipt_document: StoredControlDocument,
) -> None:
    _validate_request_chain(request, plan, candidate)
    if (
        plan_document.object_id != plan.plan_id
        or plan_document.path != plan.relative_path
        or request_document.object_id != request.request_event_id
        or request_document.path != request.relative_path
        or receipt_document.object_id != receipt.receipt_id
        or receipt_document.path != receipt.relative_path
        or receipt.registry_name != request.registry_name
        or receipt.request != request_document
        or receipt.request_event_id != request.request_event_id
        or receipt.plan_id != plan.plan_id
        or receipt.candidate_id != candidate.candidate_id
        or receipt.decision_ids != plan.decision_ids
        or receipt.availability_calendar_id != plan.availability_calendar_id
        or receipt.availability_calendar_sha256 != plan.availability_calendar_sha256
    ):
        raise RegistryWorkflowError("release approval chain differs")
    if isinstance(receipt, RegistryStandingApprovalReceipt) and (
        receipt.plan != plan_document
        or receipt.candidate != plan.candidate
        or receipt.source_scope_set_digest != candidate.source_scope_set_digest
        or receipt.contract_pin != candidate.contract_pin
    ):
        raise RegistryWorkflowError("standing release approval scope differs")


def _store_or_replay_publish_intent(
    root: Path,
    proposed: RegistryPublishIntent,
) -> tuple[RegistryPublishIntent, StoredControlDocument]:
    existing = _load_existing_publish_intent(root, proposed.relative_path)
    if existing is not None:
        intent, document = existing
        _require_same_publish_lane(intent, proposed)
        return intent, document
    try:
        document = _store_control(
            root,
            proposed.intent_id,
            proposed.relative_path,
            proposed.to_dict(),
        )
    except ArtifactError:
        raced = _load_existing_publish_intent(root, proposed.relative_path)
        if raced is None:
            raise
        intent, document = raced
        _require_same_publish_lane(intent, proposed)
        return intent, document
    return proposed, document


def _require_same_publish_lane(
    existing: RegistryPublishIntent,
    proposed: RegistryPublishIntent,
) -> None:
    ignored = {"published_at_utc", "release_available_session"}
    existing_payload = {
        key: value for key, value in existing.logical_payload().items() if key not in ignored
    }
    proposed_payload = {
        key: value for key, value in proposed.logical_payload().items() if key not in ignored
    }
    if existing_payload != proposed_payload:
        raise RegistryWorkflowError("publish intent lane is already bound differently")


def _load_existing_publish_intent(
    root: Path,
    relative_path: str,
) -> tuple[RegistryPublishIntent, StoredControlDocument] | None:
    path = safe_relative_path(root, relative_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RegistryWorkflowError("publish intent path is not a regular file")
    content = path.read_bytes()
    intent = RegistryPublishIntent.from_dict(_load_json(content, "publish intent"))
    document = StoredControlDocument(
        object_id=intent.intent_id,
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    return intent, document


def _load_publish_intent_document(
    root: Path,
    ref: StoredControlDocument,
) -> RegistryPublishIntent:
    content = _read_control(root, ref)
    intent = RegistryPublishIntent.from_dict(_load_json(content, "publish intent"))
    if intent.intent_id != ref.object_id or intent.relative_path != ref.path:
        raise RegistryWorkflowError("publish intent ref differs from immutable bytes")
    return intent


def _validate_publish_intent_chain(
    intent: RegistryPublishIntent,
    candidate: RegistryCandidateManifest,
    plan: RegistryDecisionPlan,
    plan_document: StoredControlDocument,
    request: RegistryApprovalRequest,
    request_document: StoredControlDocument,
    receipt: RegistryReleaseAuthorizationReceipt,
    receipt_document: StoredControlDocument,
    decision_row_set_digest: str,
    calendar: object,
    decision_available: Sequence[date],
) -> None:
    _validate_release_control_chain(
        candidate,
        plan,
        plan_document,
        request,
        request_document,
        receipt,
        receipt_document,
    )
    if (
        intent.registry_name != plan.registry_name
        or intent.candidate != plan.candidate
        or intent.plan != plan_document
        or intent.request != request_document
        or intent.approval_receipt != receipt_document
        or intent.decision_ids != plan.decision_ids
        or intent.source_scope_set_digest != plan.source_scope_set_digest
        or intent.decision_row_set_digest != decision_row_set_digest
        or intent.runtime_binding_id != receipt.runtime_binding.runtime_binding_id
        or intent.availability_calendar_id != plan.availability_calendar_id
        or intent.availability_calendar_sha256 != plan.availability_calendar_sha256
    ):
        raise RegistryWorkflowError("publish intent differs from exact release lane")
    try:
        first_session, _ = calendar.first_open_after(intent.published_at_utc)  # type: ignore[attr-defined]
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    expected_session = max((first_session, *decision_available))
    if intent.release_available_session != expected_session:
        raise RegistryWorkflowError("publish intent availability recomputation failed")


def _store_control(
    root: Path,
    object_id: str,
    relative_path: str,
    document: Mapping[str, object],
) -> StoredControlDocument:
    content = _canonical_bytes(document)
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative_path),
        content,
        temporary_directory=root / "tmp" / "s7-registry-control-writes",
    )
    return StoredControlDocument(
        object_id=object_id,
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _load_existing_candidate_slot(
    root: Path,
    relative_path: str,
) -> tuple[RegistryCandidateManifest, StoredControlDocument] | None:
    path = safe_relative_path(root, relative_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RegistryWorkflowError("candidate scope slot is not a regular file")
    content = path.read_bytes()
    candidate = RegistryCandidateManifest.from_dict(_load_json(content, "candidate scope slot"))
    document = StoredControlDocument(
        object_id=candidate.candidate_id,
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    loaded = _load_candidate_document(root, document)
    return loaded, document


def _load_candidate_document(
    root: Path,
    ref: StoredControlDocument,
) -> RegistryCandidateManifest:
    content = _read_control(root, ref)
    candidate = RegistryCandidateManifest.from_dict(_load_json(content, "candidate"))
    if candidate.candidate_id != ref.object_id or candidate.relative_path != ref.path:
        raise RegistryWorkflowError("candidate ref differs from candidate bytes")
    _verify_candidate_inputs(root, candidate)
    return candidate


def _load_plan_document(root: Path, ref: StoredControlDocument) -> RegistryDecisionPlan:
    content = _read_control(root, ref)
    plan = RegistryDecisionPlan.from_dict(_load_json(content, "decision plan"))
    if plan.plan_id != ref.object_id or plan.relative_path != ref.path:
        raise RegistryWorkflowError("plan ref differs from plan bytes")
    candidate = _load_candidate_document(root, plan.candidate)
    _validate_plan_candidate(plan, candidate)
    return plan


def _load_request_document(
    root: Path,
    ref: StoredControlDocument,
) -> RegistryApprovalRequest:
    content = _read_control(root, ref)
    request = RegistryApprovalRequest.from_dict(_load_json(content, "approval request"))
    if request.request_event_id != ref.object_id or request.relative_path != ref.path:
        raise RegistryWorkflowError("request ref differs from request bytes")
    plan = _load_plan_document(root, request.plan)
    candidate = _load_candidate_document(root, request.candidate)
    _validate_request_chain(request, plan, candidate)
    return request


def _load_receipt_document(
    root: Path,
    ref: StoredControlDocument,
) -> RegistryReleaseAuthorizationReceipt:
    content = _read_control(root, ref)
    raw = _mapping(_load_json(content, "approval receipt"), "approval receipt")
    if "standing_approval_receipt_version" in raw:
        receipt: RegistryReleaseAuthorizationReceipt = RegistryStandingApprovalReceipt.from_dict(
            raw
        )
    else:
        receipt = RegistryApprovalReceipt.from_dict(raw)
    if receipt.receipt_id != ref.object_id or receipt.relative_path != ref.path:
        raise RegistryWorkflowError("receipt ref differs from receipt bytes")
    request = _load_request_document(root, receipt.request)
    plan = _load_plan_document(root, request.plan)
    candidate = _load_candidate_document(root, request.candidate)
    if (
        receipt.request_event_id != request.request_event_id
        or receipt.plan_id != request.plan.object_id
        or receipt.candidate_id != request.candidate.object_id
        or receipt.decision_ids != request.decision_ids
    ):
        raise RegistryWorkflowError("receipt differs from exact request")
    calendar = _load_calendar(
        root,
        receipt.availability_calendar_id,
        receipt.availability_calendar_sha256,
    )
    try:
        calendar.require_first_open_session(
            receipt.approved_at_utc,
            receipt.approval_available_session,
            label="registry approval availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    if isinstance(receipt, RegistryStandingApprovalReceipt) and (
        receipt.plan != request.plan
        or receipt.candidate != request.candidate
        or receipt.source_scope_set_digest != request.source_scope_set_digest
        or receipt.contract_pin != request.contract_pin
        or receipt.source_artifacts != candidate.source_artifacts
        or receipt.evidence_artifacts != candidate.evidence_artifacts
        or receipt.candidate_authorization_artifacts != candidate.authorization_artifacts
        or dict(receipt.exact_request_literal) != request.literal_payload()
        or receipt.exact_request_literal_sha256
        != hashlib.sha256(request.literal_bytes()).hexdigest()
        or dict(receipt.qa_review) != _build_standing_qa_review(candidate, plan, request)
    ):
        raise RegistryWorkflowError("standing receipt differs from exact reviewed request")
    return receipt


def _read_control(root: Path, ref: StoredControlDocument) -> bytes:
    return _read_exact(
        root,
        ref.path,
        expected_sha256=ref.sha256,
        expected_bytes=ref.bytes,
    )


def _read_exact(
    root: Path,
    relative_path: str,
    *,
    expected_sha256: str,
    expected_bytes: int,
) -> bytes:
    path = safe_relative_path(root, relative_path)
    if not path.is_file() or path.is_symlink():
        raise RegistryWorkflowError(f"exact artifact is missing or unsafe: {relative_path}")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise RegistryWorkflowError(f"cannot read exact artifact: {relative_path}") from exc
    if len(content) != expected_bytes:
        raise RegistryWorkflowError(f"exact artifact byte count changed: {relative_path}")
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise RegistryWorkflowError(f"exact artifact SHA-256 changed: {relative_path}")
    return content


def _write_release_member(root: Path, release_dir: str, member: str, content: bytes) -> None:
    _relative(member, "release member")
    write_bytes_immutable(
        root,
        safe_relative_path(root, f"{release_dir}/{member}"),
        content,
        temporary_directory=root / "tmp" / "s7-registry-release-writes",
    )


def _read_release_member(
    root: Path,
    release_dir: str,
    member: str,
    *,
    expected_sha256: str,
    expected_bytes: int,
) -> bytes:
    return _read_exact(
        root,
        f"{release_dir}/{member}",
        expected_sha256=expected_sha256,
        expected_bytes=expected_bytes,
    )


def _verify_release_file_set(root: Path, manifest: RegistryReleaseManifest) -> None:
    directory = safe_relative_path(root, manifest.release_directory)
    if not directory.is_dir() or directory.is_symlink():
        raise RegistryWorkflowError("registry release directory is missing or unsafe")
    actual = {
        path.relative_to(directory).as_posix() for path in directory.rglob("*") if path.is_file()
    }
    expected = {
        "manifest.json",
        manifest.rows_path,
        *(item.path for item in manifest.decisions),
    }
    if actual != expected:
        raise RegistryWorkflowError("registry release file set changed")


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    return sink.getvalue().to_pybytes()


def _read_registry_table(registry_name: str, content: bytes) -> pa.Table:
    try:
        table = pq.read_table(pa.BufferReader(content))
    except (pa.ArrowException, OSError) as exc:
        raise RegistryWorkflowError("cannot read registry Parquet") from exc
    contract = _CONTRACTS[registry_name]
    if table.schema != contract.arrow_schema:
        raise RegistryWorkflowError("registry Parquet schema differs from exact contract")
    for field, column in zip(table.schema, table.columns, strict=True):
        if not field.nullable and column.null_count:
            raise RegistryWorkflowError(f"registry Parquet required nulls: {field.name}")
    if table.num_rows:
        sorted_table = table.sort_by([(name, "ascending") for name in contract.sort_by])
        if sorted_table.to_pylist() != table.to_pylist():
            raise RegistryWorkflowError("registry Parquet sort order changed")
    ids = table[_DECISION_ID_COLUMN[registry_name]].to_pylist()
    if len(ids) != len(set(ids)):
        raise RegistryWorkflowError("registry Parquet primary key repeats")
    return table


def _load_calendar(root: Path, calendar_id: str, calendar_sha256: str):
    try:
        return load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=calendar_id,
            expected_sha256=calendar_sha256,
        )
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError("availability calendar trust chain failed") from exc


def _require_current_contract_pin(pin: RegistryContractPin) -> None:
    expected = current_registry_contract_pin(pin.registry_name)
    if pin != expected:
        raise RegistryWorkflowError(
            "registry contract pin differs from the code-pinned schema; this does not imply "
            "that the code-pinned schema is approved"
        )


def _sorted_unique_artifacts(
    artifacts: Sequence[ExactArtifactBinding],
    label: str,
) -> tuple[ExactArtifactBinding, ...]:
    ordered = tuple(sorted(artifacts, key=lambda item: (item.role, item.artifact_id)))
    if ordered != tuple(artifacts):
        raise RegistryWorkflowError(f"{label} artifact bindings must be sorted")
    keys = {(item.role, item.artifact_id) for item in ordered}
    if len(keys) != len(ordered):
        raise RegistryWorkflowError(f"{label} artifact bindings repeat")
    return ordered


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        _json_value(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_json_text(value: object, label: str) -> object:
    if not isinstance(value, str):
        raise RegistryWorkflowError(f"{label} must be canonical JSON text")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RegistryWorkflowError(f"{label} is invalid JSON") from exc
    expected = json.dumps(
        parsed,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    if expected != value:
        raise RegistryWorkflowError(f"{label} is not canonical JSON")
    return parsed


def _load_json(content: bytes, label: str) -> object:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegistryWorkflowError(f"{label} is not valid UTF-8 JSON") from exc
    if _canonical_bytes(value) != content:
        raise RegistryWorkflowError(f"{label} JSON bytes are not canonical")
    return value


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise RegistryWorkflowError(f"unsupported canonical JSON value: {type(value).__name__}")


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RegistryWorkflowError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise RegistryWorkflowError(f"{label} keys must be strings")
    return dict(value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise RegistryWorkflowError("expected an array")
    return value


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise RegistryWorkflowError(f"{label} fields changed")


def _registry(value: object) -> str:
    if not isinstance(value, str) or value not in REGISTRY_ORDER:
        raise RegistryWorkflowError("unsupported S7 registry name")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2_000:
        raise RegistryWorkflowError(f"{label} is invalid")
    return value


def _optional_text(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise RegistryWorkflowError(f"{label} is not a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    return None if value is None else _digest(value, label)


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or not _FIGI.fullmatch(value):
        raise RegistryWorkflowError(f"{label} is invalid")
    return value


def _optional_figi(value: object, label: str) -> str | None:
    return None if value is None else _figi(value, label)


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise RegistryWorkflowError(f"{label} must be a normalized relative path")
    if "latest" in path.parts:
        raise RegistryWorkflowError(f"{label} cannot discover latest")
    return text


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise RegistryWorkflowError(f"{label} must be a positive native integer")
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise RegistryWorkflowError(f"{label} must be a nonnegative native integer")
    return value


def _date(value: object, label: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise RegistryWorkflowError(f"{label} must be a date")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise RegistryWorkflowError(f"{label} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset() != normalized.utcoffset():
        raise RegistryWorkflowError(f"{label} must use exact UTC")
    return normalized


def _utc_text(value: datetime) -> str:
    return _utc(value, "UTC timestamp").isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    if not value.endswith("Z"):
        raise RegistryWorkflowError("UTC timestamp must use Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise RegistryWorkflowError("UTC timestamp is invalid") from exc
    return _utc(parsed, "UTC timestamp")


def _restore_contract_value(arrow_type: ArrowType, value: object) -> object:
    """Restore JSON-normalized candidate claims to their exact Arrow input types."""

    if value is None:
        return None
    if arrow_type is ArrowType.DATE32:
        return date.fromisoformat(_text(value, "candidate date claim"))
    if arrow_type is ArrowType.TIMESTAMP_NS_UTC:
        return _parse_utc(_text(value, "candidate timestamp claim"))
    if arrow_type is ArrowType.LIST_STRING:
        return [_text(item, "candidate list claim") for item in _array(value)]
    return value


def capture_registry_runtime_binding(repo_root: Path | None = None) -> RegistryRuntimeBinding:
    """Capture the clean executable checkout used to authorize a registry release."""

    root = (repo_root if repo_root is not None else _runtime_repo_root()).expanduser().resolve()
    status = _run_git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise RegistryWorkflowError("registry runtime Git checkout is not clean")
    commit = _git_object(_run_git(root, "rev-parse", "HEAD"), "runtime Git commit")
    tree = _git_object(_run_git(root, "rev-parse", "HEAD^{tree}"), "runtime Git tree")
    pins: list[RuntimeFilePin] = []
    for relative in sorted(RUNTIME_BINDING_PATHS):
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise RegistryWorkflowError(f"runtime source file is missing or a symlink: {relative}")
        stage = _run_git(root, "ls-files", "--stage", "--", relative)
        lines = stage.splitlines()
        if len(lines) != 1 or "\t" not in lines[0]:
            raise RegistryWorkflowError(f"runtime source is not uniquely tracked: {relative}")
        metadata, tracked_path = lines[0].split("\t", 1)
        parts = metadata.split()
        if len(parts) != 3 or parts[2] != "0" or tracked_path != relative:
            raise RegistryWorkflowError(f"runtime source Git stage changed: {relative}")
        mode, blob_id, _ = parts
        content = path.read_bytes()
        actual_blob = _run_git(root, "hash-object", "--", relative)
        if actual_blob != blob_id:
            raise RegistryWorkflowError(f"runtime source differs from Git blob: {relative}")
        pins.append(
            RuntimeFilePin(
                path=relative,
                git_mode=mode,
                git_blob_id=blob_id,
                sha256=hashlib.sha256(content).hexdigest(),
                bytes=len(content),
            )
        )
    return RegistryRuntimeBinding(
        git_commit=commit,
        git_tree=tree,
        files=tuple(pins),
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        pyarrow_version=pa.__version__,
    )


def _require_current_runtime_binding(expected: RegistryRuntimeBinding) -> None:
    current = capture_registry_runtime_binding()
    if current != expected:
        raise RegistryWorkflowError("registry runtime binding drifted before release write")


def require_current_registry_runtime_binding(expected: RegistryRuntimeBinding) -> None:
    """Replay a captured runtime binding for production-only ingress controls."""

    _require_current_runtime_binding(expected)


def _runtime_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _authorization_lane_id(
    registry_name: str,
    candidate: StoredControlDocument,
    plan: StoredControlDocument,
    request: StoredControlDocument,
) -> str:
    return stable_digest(
        {
            "candidate": candidate.to_dict(),
            "plan": plan.to_dict(),
            "registry_name": _registry(registry_name),
            "request": request.to_dict(),
            "standing_authorization_version": STANDING_AUTHORIZATION_VERSION,
        }
    )


def _standing_candidate_authorization_slot_id(
    registry_name: str,
    authorization_type: str,
    target_refs: Sequence[tuple[str, str]],
) -> str:
    return stable_digest(
        {
            "authorization_type": authorization_type,
            "registry_name": _registry(registry_name),
            "standing_authorization_version": STANDING_AUTHORIZATION_VERSION,
            "target_refs": [
                {"artifact_id": artifact_id, "sha256": sha256}
                for artifact_id, sha256 in target_refs
            ],
        }
    )


def _production_prerequisite_authorization_slot_id(
    registry_name: str,
    authorization_type: str,
    target_refs: Sequence[tuple[str, str]],
    availability_calendar_id: str,
    availability_calendar_sha256: str,
    *,
    artifact_version: str = PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION,
    runtime_binding_id: str | None = None,
) -> str:
    if artifact_version not in _SUPPORTED_PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSIONS:
        raise RegistryWorkflowError("production prerequisite authorization version changed")
    payload: dict[str, object] = {
        "artifact_type": PRODUCTION_PREREQUISITE_AUTHORIZATION_TYPE,
        "artifact_version": artifact_version,
        "authorization_type": authorization_type,
        "availability_calendar_id": availability_calendar_id,
        "availability_calendar_sha256": availability_calendar_sha256,
        "production_data_root": CANONICAL_PRODUCTION_DATA_ROOT.resolve().as_posix(),
        "registry_name": _registry(registry_name),
        "target_refs": [
            {"artifact_id": artifact_id, "sha256": sha256}
            for artifact_id, sha256 in target_refs
        ],
    }
    if artifact_version == PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION:
        payload["runtime_binding_id"] = _digest(
            runtime_binding_id, "production authorization runtime binding ID"
        )
    elif runtime_binding_id is not None:
        raise RegistryWorkflowError("legacy production authorization slot cannot bind runtime")
    return stable_digest(payload)


def _production_prerequisite_authorization_path(
    registry_name: str,
    authorization_type: str,
    slot_id: str,
) -> str:
    return (
        "manifests/silver/identity/registry-workflow/"
        f"registry={registry_name}/production-prerequisite-authorization-slots/"
        f"authorization_type={authorization_type}/slot_id={slot_id}/authorization.json"
    )


def _load_existing_production_prerequisite_authorization(
    root: Path,
    relative_path: str,
    *,
    expected_registry_name: str,
    expected_authorization_type: str,
    expected_targets: tuple[tuple[str, str], ...],
    expected_calendar_id: str,
    expected_calendar_sha256: str,
    expected_runtime_binding: RegistryRuntimeBinding,
) -> tuple[dict[str, object], ExactArtifactBinding] | None:
    path = safe_relative_path(root, relative_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RegistryWorkflowError("production prerequisite authorization is not a regular file")
    content = path.read_bytes()
    document = _mapping(
        _load_json(content, "production prerequisite authorization"),
        "production prerequisite authorization",
    )
    available = date.fromisoformat(
        _text(document.get("approval_available_session"), "authorization availability")
    )
    binding = ExactArtifactBinding(
        role=expected_authorization_type,
        artifact_id=_digest(document.get("authorization_id"), "authorization ID"),
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        available_session=available,
        embedded_id_field="authorization_id",
    )
    calendar = _load_calendar(root, expected_calendar_id, expected_calendar_sha256)
    _validate_production_prerequisite_authorization_document(
        document,
        artifact=binding,
        registry_name=expected_registry_name,
        expected_targets=expected_targets,
        calendar=calendar,
        root=root,
        revalidate_runtime=False,
        expected_runtime_binding=expected_runtime_binding,
    )
    return dict(document), binding


def _validate_production_prerequisite_authorization_document(
    document: Mapping[str, object],
    *,
    artifact: ExactArtifactBinding,
    registry_name: str,
    expected_targets: tuple[tuple[str, str], ...],
    calendar: object,
    root: Path,
    revalidate_runtime: bool,
    expected_runtime_binding: RegistryRuntimeBinding | None = None,
) -> None:
    expected_keys = {
        "approval_available_session",
        "approved_at_utc",
        "approved_by",
        "artifact_type",
        "artifact_version",
        "authorization_id",
        "authorization_slot_id",
        "authorization_type",
        "availability_calendar_id",
        "availability_calendar_sha256",
        "capabilities",
        "decision",
        "policy_version",
        "production_data_root",
        "reaffirmation_literal",
        "reaffirmation_literal_sha256",
        "registry_name",
        "runtime_binding",
        "standing_authorization_action",
        "standing_authorization_literal",
        "standing_authorization_literal_sha256",
        "target_refs",
    }
    _expect_keys(document, expected_keys, "production prerequisite authorization")
    targets: list[tuple[str, str]] = []
    for raw in _array(document["target_refs"]):
        item = _mapping(raw, "production authorization target")
        _expect_keys(item, {"artifact_id", "sha256"}, "production authorization target")
        targets.append(
            (
                _digest(item["artifact_id"], "authorization target ID"),
                _digest(item["sha256"], "authorization target SHA-256"),
            )
        )
    targets_tuple = tuple(targets)
    artifact_version = _text(document["artifact_version"], "authorization version")
    runtime_binding = RegistryRuntimeBinding.from_dict(document["runtime_binding"])
    slot_id = _production_prerequisite_authorization_slot_id(
        registry_name,
        artifact.role,
        targets_tuple,
        str(document["availability_calendar_id"]),
        str(document["availability_calendar_sha256"]),
        artifact_version=artifact_version,
        runtime_binding_id=(
            runtime_binding.runtime_binding_id
            if artifact_version == PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION
            else None
        ),
    )
    logical = {key: value for key, value in document.items() if key != "authorization_id"}
    approved_at = _parse_utc(_text(document["approved_at_utc"], "authorization time"))
    available = date.fromisoformat(
        _text(document["approval_available_session"], "authorization availability")
    )
    if (
        not is_canonical_production_data_root(root)
        or document["authorization_id"] != artifact.artifact_id
        or document["authorization_id"] != stable_digest(logical)
        or document["artifact_type"] != PRODUCTION_PREREQUISITE_AUTHORIZATION_TYPE
        or artifact_version not in _SUPPORTED_PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSIONS
        or document["authorization_slot_id"] != slot_id
        or document["authorization_type"] != artifact.role
        or document["availability_calendar_id"] != getattr(calendar, "calendar_artifact_id", None)
        or document["availability_calendar_sha256"] != getattr(calendar, "sha256", None)
        or document["capabilities"] != dict(STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES)
        or document["decision"] != APPROVED
        or document["policy_version"] != WORKFLOW_POLICY
        or document["production_data_root"] != root.as_posix()
        or document["reaffirmation_literal"] != STANDING_REAFFIRMATION_LITERAL
        or document["reaffirmation_literal_sha256"] != _utf8_sha256(STANDING_REAFFIRMATION_LITERAL)
        or document["registry_name"] != registry_name
        or document["standing_authorization_action"] != STANDING_AUTHORIZATION_ACTION
        or document["standing_authorization_literal"] != STANDING_AUTHORIZATION_LITERAL
        or document["standing_authorization_literal_sha256"]
        != _utf8_sha256(STANDING_AUTHORIZATION_LITERAL)
        or targets_tuple != expected_targets
        or available != artifact.available_session
        or artifact.path
        != _production_prerequisite_authorization_path(registry_name, artifact.role, slot_id)
        or (
            expected_runtime_binding is not None
            and runtime_binding != expected_runtime_binding
        )
    ):
        raise RegistryWorkflowError("production prerequisite authorization binding changed")
    try:
        calendar.require_first_open_session(  # type: ignore[attr-defined]
            approved_at,
            available,
            label="production prerequisite authorization availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    if revalidate_runtime:
        _require_current_runtime_binding(runtime_binding)


def _standing_candidate_authorization_path(
    registry_name: str,
    authorization_type: str,
    slot_id: str,
) -> str:
    return (
        "manifests/silver/identity/registry-workflow/"
        f"registry={registry_name}/candidate-authorization-slots/"
        f"authorization_type={authorization_type}/slot_id={slot_id}/authorization.json"
    )


def _load_existing_standing_candidate_authorization(
    root: Path,
    relative_path: str,
    *,
    expected_registry_name: str,
    expected_authorization_type: str,
    expected_targets: tuple[tuple[str, str], ...],
    expected_calendar_id: str,
    expected_calendar_sha256: str,
) -> tuple[dict[str, object], ExactArtifactBinding] | None:
    path = safe_relative_path(root, relative_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RegistryWorkflowError("standing candidate authorization is not a regular file")
    content = path.read_bytes()
    document = _mapping(
        _load_json(content, "standing candidate authorization"),
        "standing candidate authorization",
    )
    available = date.fromisoformat(
        _text(document.get("approval_available_session"), "authorization availability")
    )
    binding = ExactArtifactBinding(
        role=expected_authorization_type,
        artifact_id=_digest(document.get("authorization_id"), "authorization ID"),
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        available_session=available,
        embedded_id_field="authorization_id",
    )
    calendar = _load_calendar(root, expected_calendar_id, expected_calendar_sha256)
    _validate_standing_candidate_authorization_document(
        document,
        artifact=binding,
        registry_name=expected_registry_name,
        expected_targets=expected_targets,
        calendar=calendar,
        revalidate_runtime=False,
    )
    return document, binding


def _validate_standing_candidate_authorization_document(
    document: Mapping[str, object],
    *,
    artifact: ExactArtifactBinding,
    registry_name: str,
    expected_targets: tuple[tuple[str, str], ...],
    calendar: object,
    revalidate_runtime: bool,
) -> None:
    expected_keys = {
        "approval_available_session",
        "approved_at_utc",
        "approved_by",
        "authorization_id",
        "authorization_mode",
        "authorization_slot_id",
        "authorization_type",
        "availability_calendar_id",
        "availability_calendar_sha256",
        "capabilities",
        "decision",
        "policy_version",
        "reaffirmation_literal",
        "reaffirmation_literal_sha256",
        "registry_name",
        "runtime_binding",
        "standing_authorization_action",
        "standing_authorization_literal",
        "standing_authorization_literal_sha256",
        "target_refs",
    }
    _expect_keys(document, expected_keys, "standing candidate authorization")
    targets: list[tuple[str, str]] = []
    for raw in _array(document["target_refs"]):
        item = _mapping(raw, "standing authorization target")
        _expect_keys(item, {"artifact_id", "sha256"}, "standing authorization target")
        targets.append(
            (
                _digest(item["artifact_id"], "authorization target ID"),
                _digest(item["sha256"], "authorization target SHA-256"),
            )
        )
    targets_tuple = tuple(targets)
    slot_id = _standing_candidate_authorization_slot_id(
        registry_name,
        artifact.role,
        targets_tuple,
    )
    logical = {key: value for key, value in document.items() if key != "authorization_id"}
    runtime_binding = RegistryRuntimeBinding.from_dict(document["runtime_binding"])
    approved_at = _parse_utc(_text(document["approved_at_utc"], "authorization time"))
    available = date.fromisoformat(
        _text(document["approval_available_session"], "authorization availability")
    )
    if (
        document["authorization_id"] != artifact.artifact_id
        or document["authorization_id"] != stable_digest(logical)
        or document["authorization_mode"] != STANDING_AUTHORIZATION_VERSION
        or document["authorization_slot_id"] != slot_id
        or document["authorization_type"] != artifact.role
        or document["availability_calendar_id"] != getattr(calendar, "calendar_artifact_id", None)
        or document["availability_calendar_sha256"] != getattr(calendar, "sha256", None)
        or document["capabilities"] != dict(STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES)
        or document["decision"] != APPROVED
        or document["policy_version"] != WORKFLOW_POLICY
        or document["reaffirmation_literal"] != STANDING_REAFFIRMATION_LITERAL
        or document["reaffirmation_literal_sha256"] != _utf8_sha256(STANDING_REAFFIRMATION_LITERAL)
        or document["registry_name"] != registry_name
        or document["standing_authorization_action"] != STANDING_AUTHORIZATION_ACTION
        or document["standing_authorization_literal"] != STANDING_AUTHORIZATION_LITERAL
        or document["standing_authorization_literal_sha256"]
        != _utf8_sha256(STANDING_AUTHORIZATION_LITERAL)
        or targets_tuple != expected_targets
        or available != artifact.available_session
        or artifact.path
        != _standing_candidate_authorization_path(registry_name, artifact.role, slot_id)
    ):
        raise RegistryWorkflowError("standing candidate authorization binding changed")
    try:
        calendar.require_first_open_session(  # type: ignore[attr-defined]
            approved_at,
            available,
            label="standing candidate authorization availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise RegistryWorkflowError(str(exc)) from exc
    if revalidate_runtime:
        _require_current_runtime_binding(runtime_binding)


def _standing_approval_slot_path(
    registry_name: str,
    candidate: StoredControlDocument,
    plan: StoredControlDocument,
    request: StoredControlDocument,
) -> str:
    slot_id = _authorization_lane_id(registry_name, candidate, plan, request)
    return (
        "manifests/silver/identity/registry-workflow/"
        f"registry={registry_name}/standing-approval-slots/"
        f"authorization_slot_id={slot_id}/receipt.json"
    )


def _load_existing_standing_receipt(
    root: Path,
    relative_path: str,
) -> tuple[RegistryStandingApprovalReceipt, StoredControlDocument] | None:
    path = safe_relative_path(root, relative_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RegistryWorkflowError("standing approval slot is not a regular file")
    content = path.read_bytes()
    raw = _mapping(_load_json(content, "standing approval slot"), "standing approval slot")
    receipt = RegistryStandingApprovalReceipt.from_dict(raw)
    ref = StoredControlDocument(
        object_id=receipt.receipt_id,
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    loaded = _load_receipt_document(root, ref)
    if not isinstance(loaded, RegistryStandingApprovalReceipt):  # pragma: no cover - defensive
        raise RegistryWorkflowError("standing approval slot contains an exact-literal receipt")
    return loaded, ref


def _run_git(root: Path, *args: str) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *args),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RegistryWorkflowError("cannot capture exact registry Git runtime") from exc
    return completed.stdout.rstrip("\n")


def _utf8_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _git_object(value: object, label: str) -> str:
    text = _text(value, label)
    if not _GIT_OBJECT_ID.fullmatch(text):
        raise RegistryWorkflowError(f"{label} is not a Git object ID")
    return text


def _runtime_utc_now() -> datetime:
    """Single production clock seam; CLI users cannot supply or override this value."""

    return datetime.now(UTC)


__all__ = [
    "APPROVAL_ACTION",
    "APPROVAL_LITERAL_VERSION",
    "CANONICAL_PRODUCTION_DATA_ROOT",
    "COMPOSITE_CORRECTION_REGISTRIES",
    "FIXED_DECISION_SCOPE_SPECS",
    "PRODUCTION_INGRESS_ATTESTATION_TYPE",
    "PRODUCTION_INGRESS_ATTESTATION_VERSION",
    "PRODUCTION_PREREQUISITE_AUTHORIZATION_TYPE",
    "PRODUCTION_PREREQUISITE_AUTHORIZATION_VERSION",
    "REGISTRY_ORDER",
    "REQUIRED_CANDIDATE_AUTHORIZATION_ROLES",
    "RUNTIME_BINDING_PATHS",
    "STANDING_AUTHORIZATION_ACTION",
    "STANDING_AUTHORIZATION_CAPABILITIES",
    "STANDING_AUTHORIZATION_LITERAL",
    "STANDING_AUTHORIZATION_VERSION",
    "STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES",
    "STANDING_REAFFIRMATION_LITERAL",
    "ExactArtifactBinding",
    "ExactSourceRow",
    "ExactSourceScope",
    "FixedDecisionScopeSpec",
    "LoadedRegistryRelease",
    "LoadedRegistryReleaseSet",
    "RegistryApprovalReceipt",
    "RegistryApprovalRequest",
    "RegistryCandidateManifest",
    "RegistryContractPin",
    "RegistryDecisionCandidate",
    "RegistryDecisionPlan",
    "RegistryName",
    "RegistryPublishIntent",
    "RegistryReleaseManifest",
    "RegistryReleasePin",
    "RegistryRuntimeBinding",
    "RegistryStandingApprovalReceipt",
    "RegistryWorkflowError",
    "RuntimeFilePin",
    "StoredControlDocument",
    "build_approved_registry_rows",
    "build_registry_authorization_document",
    "capture_registry_runtime_binding",
    "create_approval_request",
    "create_decision_plan",
    "create_registry_decision_candidate",
    "current_registry_contract_pin",
    "is_canonical_production_data_root",
    "load_approval_receipt_control",
    "load_approval_request_control",
    "load_candidate_control",
    "load_decision_plan_control",
    "load_registry_release",
    "load_registry_release_set",
    "publish_release",
    "publish_release_under_standing_authority",
    "record_exact_approval",
    "record_production_prerequisite_authorization",
    "record_standing_approval",
    "record_standing_candidate_authorization",
    "require_current_registry_runtime_binding",
    "require_fixture_registry_root",
    "store_approval_request",
    "store_candidate",
    "store_decision_plan",
    "validate_fixed_decision_candidate",
]
