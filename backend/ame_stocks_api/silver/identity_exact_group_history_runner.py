"""Source-bound runner for the S7 exact-group full-history review.

The runner scans the frozen 5,026 S4 daily Parquet artifacts exactly once and
emits only three exact observed groups.  It preserves every provider version,
attests physical rows with the v2 provider-row schema, reconciles the selected
Universe parent, and stops at an immutable ``awaiting_review`` candidate.

No observed run is treated as an effective registry interval.  This module has
no network client, registry evaluator, adjudicator, identity override,
membership mutation, forced-liquidation signal, Full-run path, or publisher.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import resource
import shutil
import stat
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import QAStatus, TableContract
from ame_stocks_api.silver.identity_exact_group_history_approval import (
    ExactGroupHistoryExecutionApprovalStore,
    IdentityExactGroupHistoryApprovalError,
    S7ExactGroupHistoryExecutionApproval,
)
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_CAPABILITIES,
    EXACT_GROUP_HISTORY_END_SESSION,
    EXACT_GROUP_HISTORY_FIXED_GROUPS,
    EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS,
    EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
    EXACT_GROUP_HISTORY_FIXED_TICKERS,
    EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST,
    EXACT_GROUP_HISTORY_PROVIDER_ID,
    EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
    EXACT_GROUP_HISTORY_PROVIDER_MARKET,
    EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
    EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
    EXACT_GROUP_HISTORY_START_SESSION,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
    exact_group_history_review_group_id,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    UNIVERSE_PARENT_PROJECTION,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    ProviderEvidenceError,
    ProviderRowAttestation,
)
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID, S7_SOURCE_PINS

RUNNER_RULE_VERSION: Final = "s7_exact_group_history_source_bound_runner_v1"
INTENT_SCHEMA_VERSION: Final = 1
INTENT_RULE_VERSION: Final = "s7_exact_group_history_execution_intent_v1"
EVIDENCE_SCHEMA_VERSION: Final = 2
EVIDENCE_RULE_VERSION: Final = "s7_exact_group_history_group_evidence_v2"
CANDIDATE_SCHEMA_VERSION: Final = 1
CANDIDATE_RULE_VERSION: Final = "s7_exact_group_history_candidate_v1"
COMPLETION_SCHEMA_VERSION: Final = 1
COMPLETION_RULE_VERSION: Final = "s7_exact_group_history_completion_v1"
CANDIDATE_STATE: Final = "awaiting_review"

SLOTS_FILENAME: Final = "data/review-slots.parquet"
SEQUENCES_FILENAME: Final = "review/group-sequences.json"
QA_FILENAME: Final = "qa/qa.json"
EXAMPLES_FILENAME: Final = "examples/review-anomalies.json"
MANIFEST_FILENAME: Final = "manifest.json"
EVIDENCE_DIRECTORY: Final = "evidence"

ASSET_TABLE: Final = "asset_observation_daily"
UNIVERSE_TABLE: Final = "universe_source_daily"
SOURCE_TABLES: Final = (ASSET_TABLE, UNIVERSE_TABLE)
RSS_HARD_FLOOR_BYTES: Final = 2 * 1024**3
DEFAULT_BATCH_SIZE: Final = 65_536
DEFAULT_WALL_CLOCK_CAP_SECONDS: Final = 12 * 60 * 60
DEFAULT_DISK_FLOOR_BYTES: Final = 40 * 1024**3
DEFAULT_TMP_CAP_BYTES: Final = 2 * 1024**3
DEFAULT_OUTPUT_CAP_BYTES: Final = 512 * 1024**2
DEFAULT_SELECTED_ROW_CAP: Final = 1_000_000
PHYSICAL_REPLAY_BATCH_SIZE: Final = 8_192

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")


class IdentityExactGroupHistoryRunnerError(RuntimeError):
    """Raised before a trustworthy exact-group completion is committed."""


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    try:
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
    except (TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryRunnerError("artifact is not canonical JSON") from exc


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be lowercase 64-hex")
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be text")
    path = Path(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be a canonical relative path")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != value:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be canonical UTC")
    return normalized


def _native_int(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise IdentityExactGroupHistoryRunnerError(f"{label} must be a native integer >= {minimum}")
    return value


@dataclass(frozen=True, slots=True, order=True)
class ExactGroupHistorySourceArtifactRef:
    table: str
    session_date: date
    release_id: str
    release_manifest_sha256: str
    path: str
    sha256: str
    bytes: int
    row_count: int
    source_contract_id: str
    schema_digest: str

    def __post_init__(self) -> None:
        if self.table not in SOURCE_TABLES or type(self.session_date) is not date:
            raise IdentityExactGroupHistoryRunnerError("source artifact scope is invalid")
        for label, value in (
            ("release ID", self.release_id),
            ("release manifest SHA", self.release_manifest_sha256),
            ("artifact SHA", self.sha256),
            ("source contract ID", self.source_contract_id),
            ("schema digest", self.schema_digest),
        ):
            _digest(value, label)
        _relative_path(self.path, "source artifact path")
        _native_int(self.bytes, "source artifact bytes")
        _native_int(self.row_count, "source artifact rows")
        match = _SESSION_PARTITION.search(self.path)
        if match is None or date.fromisoformat(match.group(1)) != self.session_date:
            raise IdentityExactGroupHistoryRunnerError(
                "source artifact path/session binding differs"
            )

    @classmethod
    def from_plan_pin(cls, value: object) -> ExactGroupHistorySourceArtifactRef:
        try:
            return cls(
                table=value.table,
                session_date=(
                    value.session_date
                    if type(value.session_date) is date
                    else date.fromisoformat(value.session_date)
                ),
                release_id=value.release_id,
                release_manifest_sha256=value.release_manifest_sha256,
                path=value.path,
                sha256=value.sha256,
                bytes=value.bytes,
                row_count=value.row_count,
                source_contract_id=value.source_contract_id,
                schema_digest=value.schema_digest,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "execution source pin has the wrong shape"
            ) from exc

    @classmethod
    def from_dict(cls, value: object) -> ExactGroupHistorySourceArtifactRef:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("source ref must be an object")
        item = dict(value)
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
        if set(item) != expected:
            raise IdentityExactGroupHistoryRunnerError("source ref schema is not exact")
        return cls(
            table=str(item["table"]),
            session_date=date.fromisoformat(str(item["session_date"])),
            release_id=str(item["release_id"]),
            release_manifest_sha256=str(item["release_manifest_sha256"]),
            path=str(item["path"]),
            sha256=str(item["sha256"]),
            bytes=int(item["bytes"]),
            row_count=int(item["row_count"]),
            source_contract_id=str(item["source_contract_id"]),
            schema_digest=str(item["schema_digest"]),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "source_contract_id": self.source_contract_id,
            "table": self.table,
        }


def exact_group_history_execution_intent_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "Plan ID")
    _digest(approval_id, "Approval ID")
    return (
        "manifests/silver/identity/exact-group-history-execution-intents/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryExecutionIntent:
    created_at_utc: datetime
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    execution_data_root: str
    source_binding_id: str
    source_binding_sha256: str
    source_artifact_set_digest: str
    normalized_source_artifact_set_digest: str
    fixed_scope_digest: str
    contract_id: str
    contract_schema_digest: str
    qa_semantics_digest: str
    observed_run_semantics_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "created_at_utc",
            _utc(self.created_at_utc, "intent created_at_utc"),
        )
        for label, value in (
            ("Plan ID", self.plan_id),
            ("Plan SHA", self.plan_sha256),
            ("Approval ID", self.approval_id),
            ("Approval SHA", self.approval_sha256),
            ("Request ID", self.request_event_id),
            ("Request SHA", self.request_event_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA", self.source_binding_sha256),
            ("raw source digest", self.source_artifact_set_digest),
            ("normalized source digest", self.normalized_source_artifact_set_digest),
            ("scope digest", self.fixed_scope_digest),
            ("contract ID", self.contract_id),
            ("contract schema digest", self.contract_schema_digest),
            ("QA semantics digest", self.qa_semantics_digest),
            ("observed run semantics digest", self.observed_run_semantics_digest),
        ):
            _digest(value, label)
        root = Path(self.execution_data_root)
        if not root.is_absolute() or str(root) != self.execution_data_root:
            raise IdentityExactGroupHistoryRunnerError("intent execution root is not canonical")

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_exact_group_history_execution_intent",
            "capabilities": _false_capabilities(),
            "contract_id": self.contract_id,
            "contract_schema_digest": self.contract_schema_digest,
            "created_at_utc": self.created_at_utc.isoformat(),
            "execution_data_root": self.execution_data_root,
            "fixed_scope_digest": self.fixed_scope_digest,
            "intent_rule_version": INTENT_RULE_VERSION,
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "observed_run_semantics_digest": self.observed_run_semantics_digest,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "qa_semantics_digest": self.qa_semantics_digest,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "schema_version": INTENT_SCHEMA_VERSION,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_binding_id": self.source_binding_id,
            "source_binding_sha256": self.source_binding_sha256,
        }

    @property
    def intent_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "intent_id": self.intent_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return exact_group_history_execution_intent_path(self.plan_id, self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryExecutionIntent:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("execution intent must be an object")
        document = dict(value)
        intent_id = document.pop("intent_id", None)
        expected = {
            "approval_id",
            "approval_sha256",
            "artifact_type",
            "capabilities",
            "contract_id",
            "contract_schema_digest",
            "created_at_utc",
            "execution_data_root",
            "fixed_scope_digest",
            "intent_rule_version",
            "normalized_source_artifact_set_digest",
            "observed_run_semantics_digest",
            "plan_id",
            "plan_sha256",
            "qa_semantics_digest",
            "request_event_id",
            "request_event_sha256",
            "schema_version",
            "source_artifact_set_digest",
            "source_binding_id",
            "source_binding_sha256",
        }
        if set(document) != expected:
            raise IdentityExactGroupHistoryRunnerError("execution intent schema differs")
        intent = cls(
            created_at_utc=_parse_utc(document["created_at_utc"], "intent time"),
            plan_id=str(document["plan_id"]),
            plan_sha256=str(document["plan_sha256"]),
            approval_id=str(document["approval_id"]),
            approval_sha256=str(document["approval_sha256"]),
            request_event_id=str(document["request_event_id"]),
            request_event_sha256=str(document["request_event_sha256"]),
            execution_data_root=str(document["execution_data_root"]),
            source_binding_id=str(document["source_binding_id"]),
            source_binding_sha256=str(document["source_binding_sha256"]),
            source_artifact_set_digest=str(document["source_artifact_set_digest"]),
            normalized_source_artifact_set_digest=str(
                document["normalized_source_artifact_set_digest"]
            ),
            fixed_scope_digest=str(document["fixed_scope_digest"]),
            contract_id=str(document["contract_id"]),
            contract_schema_digest=str(document["contract_schema_digest"]),
            qa_semantics_digest=str(document["qa_semantics_digest"]),
            observed_run_semantics_digest=str(document["observed_run_semantics_digest"]),
        )
        if (
            document != intent.logical_payload()
            or intent_id != intent.intent_id
            or value != {**document, "intent_id": intent.intent_id}
        ):
            raise IdentityExactGroupHistoryRunnerError(
                "execution intent canonical identity differs"
            )
        return intent


def _false_capabilities() -> dict[str, bool]:
    return {
        **dict(EXACT_GROUP_HISTORY_CAPABILITIES),
        "forced_liquidation": False,
        "membership_mutation": False,
        "source_discovery": False,
    }


def _review_group_id(ticker: str, composite_figi: str) -> str:
    result = exact_group_history_review_group_id(
        ticker=ticker,
        observed_composite_figi=composite_figi,
    )
    if result != EXACT_GROUP_HISTORY_FIXED_REVIEW_GROUP_IDS[ticker]:
        raise IdentityExactGroupHistoryRunnerError("fixed review-group ID drifted")
    return result


@dataclass(frozen=True, slots=True)
class ExactGroupHistoryEvidenceManifestV2:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    execution_intent_id: str
    execution_intent_sha256: str
    review_scope_set_id: str
    ticker: str
    exact_group_observed_composite_figi: str
    source_artifact_set_digest: str
    normalized_source_artifact_set_digest: str
    asset_attestations: tuple[ProviderRowAttestation, ...]
    universe_attestations: tuple[ProviderRowAttestation, ...]
    created_at_utc: datetime

    def __post_init__(self) -> None:
        for label, value in (
            ("Plan ID", self.plan_id),
            ("Plan SHA", self.plan_sha256),
            ("Approval ID", self.approval_id),
            ("Approval SHA", self.approval_sha256),
            ("execution intent ID", self.execution_intent_id),
            ("execution intent SHA", self.execution_intent_sha256),
            ("review scope-set ID", self.review_scope_set_id),
            ("source artifact-set digest", self.source_artifact_set_digest),
            (
                "normalized source artifact-set digest",
                self.normalized_source_artifact_set_digest,
            ),
        ):
            _digest(value, label)
        if (self.ticker, self.exact_group_observed_composite_figi) not in (
            EXACT_GROUP_HISTORY_FIXED_GROUPS
        ):
            raise IdentityExactGroupHistoryRunnerError(
                "evidence manifest review group is outside fixed scope"
            )
        if not _FIGI.fullmatch(self.exact_group_observed_composite_figi):
            raise IdentityExactGroupHistoryRunnerError("evidence Composite FIGI is invalid")
        object.__setattr__(
            self, "created_at_utc", _utc(self.created_at_utc, "evidence created_at_utc")
        )
        assets = tuple(sorted(self.asset_attestations, key=lambda item: item.locator))
        universes = tuple(sorted(self.universe_attestations, key=lambda item: item.locator))
        if any(type(item) is not ProviderRowAttestation for item in (*assets, *universes)):
            raise IdentityExactGroupHistoryRunnerError(
                "evidence manifest requires concrete v2 attestations"
            )
        if any(item.dataset != ASSET_TABLE for item in assets) or any(
            item.dataset != UNIVERSE_TABLE for item in universes
        ):
            raise IdentityExactGroupHistoryRunnerError(
                "evidence manifest attestation dataset differs"
            )
        all_rows = (*assets, *universes)
        if len({item.row_attestation_id for item in all_rows}) != len(all_rows) or len(
            {item.locator for item in all_rows}
        ) != len(all_rows):
            raise IdentityExactGroupHistoryRunnerError(
                "evidence manifest repeats attestation or locator"
            )
        exact_rows = [
            item
            for item in assets
            if _attestation_matches_exact_group(
                item, self.ticker, self.exact_group_observed_composite_figi
            )
        ]
        if not exact_rows:
            raise IdentityExactGroupHistoryRunnerError(
                "evidence manifest has no exact-group Asset row"
            )
        sessions = {
            date.fromisoformat(str(item.full_row_snapshot["session_date"])) for item in exact_rows
        }
        for item in all_rows:
            row = item.full_row_snapshot
            if (
                row.get("ticker") != self.ticker
                or date.fromisoformat(str(row.get("session_date"))) not in sessions
            ):
                raise IdentityExactGroupHistoryRunnerError(
                    "evidence manifest contains out-of-group session lineage"
                )
        object.__setattr__(self, "asset_attestations", assets)
        object.__setattr__(self, "universe_attestations", universes)

    @property
    def review_group_id(self) -> str:
        return _review_group_id(self.ticker, self.exact_group_observed_composite_figi)

    @property
    def exact_asset_attestation_ids(self) -> tuple[str, ...]:
        return tuple(
            item.row_attestation_id
            for item in self.asset_attestations
            if _attestation_matches_exact_group(
                item, self.ticker, self.exact_group_observed_composite_figi
            )
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_exact_group_history_group_evidence_manifest_v2",
            "asset_attestations": [item.to_dict() for item in self.asset_attestations],
            "capabilities": _false_capabilities(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "evidence_rule_version": EVIDENCE_RULE_VERSION,
            "exact_asset_attestation_ids": list(self.exact_asset_attestation_ids),
            "exact_group_observed_composite_figi": (self.exact_group_observed_composite_figi),
            "execution_intent_id": self.execution_intent_id,
            "execution_intent_sha256": self.execution_intent_sha256,
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "provider_row_attestation_schema_version": (
                EXACT_GROUP_HISTORY_PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION
            ),
            "review_group_id": self.review_group_id,
            "review_scope_set_id": self.review_scope_set_id,
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "ticker": self.ticker,
            "universe_attestations": [item.to_dict() for item in self.universe_attestations],
        }

    @property
    def manifest_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "manifest_id": self.manifest_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def candidate_relative_path(self) -> str:
        return f"{EVIDENCE_DIRECTORY}/review_group_id={self.review_group_id}/manifest.json"

    @classmethod
    def from_dict(cls, value: object) -> ExactGroupHistoryEvidenceManifestV2:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("group evidence manifest must be an object")
        document = dict(value)
        try:
            manifest = cls(
                plan_id=str(document["plan_id"]),
                plan_sha256=str(document["plan_sha256"]),
                approval_id=str(document["approval_id"]),
                approval_sha256=str(document["approval_sha256"]),
                execution_intent_id=str(document["execution_intent_id"]),
                execution_intent_sha256=str(document["execution_intent_sha256"]),
                review_scope_set_id=str(document["review_scope_set_id"]),
                ticker=str(document["ticker"]),
                exact_group_observed_composite_figi=str(
                    document["exact_group_observed_composite_figi"]
                ),
                source_artifact_set_digest=str(document["source_artifact_set_digest"]),
                normalized_source_artifact_set_digest=str(
                    document["normalized_source_artifact_set_digest"]
                ),
                asset_attestations=tuple(
                    ProviderRowAttestation.from_dict(item)
                    for item in document["asset_attestations"]
                ),
                universe_attestations=tuple(
                    ProviderRowAttestation.from_dict(item)
                    for item in document["universe_attestations"]
                ),
                created_at_utc=_parse_utc(document["created_at_utc"], "evidence time"),
            )
        except (KeyError, TypeError, ProviderEvidenceError) as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "group evidence manifest is malformed"
            ) from exc
        if manifest.content != _canonical_bytes(document):
            raise IdentityExactGroupHistoryRunnerError(
                "group evidence manifest canonical bytes differ"
            )
        return manifest


@dataclass(frozen=True, slots=True)
class ExactGroupHistoryBuild:
    slots: tuple[Mapping[str, object], ...]
    evidence_manifests: tuple[ExactGroupHistoryEvidenceManifestV2, ...]
    sequences: Mapping[str, object]
    qa: Mapping[str, object]
    examples: Mapping[str, object]


@dataclass(slots=True)
class _ObservedSession:
    session_date: date
    all_assets: tuple[ProviderRowAttestation, ...]
    exact_assets: tuple[ProviderRowAttestation, ...]
    universes: tuple[ProviderRowAttestation, ...]


class ExactGroupHistoryEngine:
    """Collect only fixed exact groups while preserving all same-ticker versions."""

    def __init__(self, *, selected_row_cap: int = DEFAULT_SELECTED_ROW_CAP) -> None:
        _native_int(selected_row_cap, "selected row cap", minimum=1)
        self._selected_row_cap = selected_row_cap
        self._groups: dict[str, dict[date, _ObservedSession]] = {
            ticker: {} for ticker in EXACT_GROUP_HISTORY_FIXED_TICKERS
        }
        self._seen_ids: set[str] = set()
        self._seen_locators: set[tuple[str, str, str, int, int]] = set()
        self._sealed = False

    @property
    def retained_attestations(self) -> tuple[ProviderRowAttestation, ...]:
        return tuple(
            sorted(
                {
                    item.row_attestation_id: item
                    for sessions in self._groups.values()
                    for observed in sessions.values()
                    for item in (*observed.all_assets, *observed.universes)
                }.values(),
                key=lambda item: item.locator,
            )
        )

    def consume_session(
        self,
        session: date,
        *,
        asset_attestations: Sequence[ProviderRowAttestation],
        universe_attestations: Sequence[ProviderRowAttestation],
    ) -> int:
        if self._sealed:
            raise IdentityExactGroupHistoryRunnerError("engine is already sealed")
        if type(session) is not date:
            raise IdentityExactGroupHistoryRunnerError("session must be a date")
        assets_by_ticker: dict[str, list[ProviderRowAttestation]] = defaultdict(list)
        universe_by_ticker: dict[str, list[ProviderRowAttestation]] = defaultdict(list)
        selected = (*asset_attestations, *universe_attestations)
        for item in selected:
            if type(item) is not ProviderRowAttestation:
                raise IdentityExactGroupHistoryRunnerError(
                    "engine requires concrete v2 provider attestations"
                )
            if item.row_attestation_id in self._seen_ids or item.locator in self._seen_locators:
                raise IdentityExactGroupHistoryRunnerError("orphan_or_duplicate_attestation_rows")
            row = item.full_row_snapshot
            try:
                row_session = date.fromisoformat(str(row["session_date"]))
                ticker = str(row["ticker"])
            except (KeyError, TypeError, ValueError) as exc:
                raise IdentityExactGroupHistoryRunnerError(
                    "attested row scope is malformed"
                ) from exc
            if row_session != session or ticker not in EXACT_GROUP_HISTORY_FIXED_TICKERS:
                raise IdentityExactGroupHistoryRunnerError("exact_group_scope_leakage_rows")
            if item.dataset == ASSET_TABLE:
                assets_by_ticker[ticker].append(item)
            elif item.dataset == UNIVERSE_TABLE:
                universe_by_ticker[ticker].append(item)
            else:
                raise IdentityExactGroupHistoryRunnerError("exact_group_scope_leakage_rows")
            self._seen_ids.add(item.row_attestation_id)
            self._seen_locators.add(item.locator)
        retained = 0
        for ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS:
            all_assets = tuple(sorted(assets_by_ticker[ticker], key=lambda item: item.locator))
            exact = tuple(
                item
                for item in all_assets
                if _attestation_matches_exact_group(item, ticker, composite)
            )
            if not exact:
                continue
            universes = tuple(sorted(universe_by_ticker[ticker], key=lambda item: item.locator))
            self._groups[ticker][session] = _ObservedSession(
                session_date=session,
                all_assets=all_assets,
                exact_assets=exact,
                universes=universes,
            )
            retained += len(all_assets) + len(universes)
        if len(self.retained_attestations) > self._selected_row_cap:
            raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: selected rows")
        return retained

    def finish(
        self,
        *,
        plan: object,
        approval: S7ExactGroupHistoryExecutionApproval,
        intent: S7ExactGroupHistoryExecutionIntent,
        calendar: XNYSCalendarArtifact,
        created_at_utc: datetime,
        runner_verified_critical: Mapping[str, int],
    ) -> ExactGroupHistoryBuild:
        if self._sealed:
            raise IdentityExactGroupHistoryRunnerError("engine is already sealed")
        self._sealed = True
        created = _utc(created_at_utc, "candidate created_at_utc")
        scope_id = str(plan.scope_set_id)
        _digest(scope_id, "scope-set ID")
        evidence = tuple(
            ExactGroupHistoryEvidenceManifestV2(
                plan_id=str(plan.plan_id),
                plan_sha256=str(plan.sha256),
                approval_id=approval.approval_id,
                approval_sha256=approval.sha256,
                execution_intent_id=intent.intent_id,
                execution_intent_sha256=intent.sha256,
                review_scope_set_id=scope_id,
                ticker=ticker,
                exact_group_observed_composite_figi=composite,
                source_artifact_set_digest=str(plan.source_artifact_set_digest),
                normalized_source_artifact_set_digest=str(
                    plan.normalized_source_artifact_set_digest
                ),
                asset_attestations=tuple(
                    item for session in self._groups[ticker].values() for item in session.all_assets
                ),
                universe_attestations=tuple(
                    item for session in self._groups[ticker].values() for item in session.universes
                ),
                created_at_utc=created,
            )
            for ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS
        )
        evidence_by_ticker = {item.ticker: item for item in evidence}
        calendar_sessions = tuple(item.session_date for item in calendar.sessions)
        calendar_index = {session: index for index, session in enumerate(calendar_sessions)}
        critical = Counter({rule.check_id: 0 for rule in _critical_rules()})
        high = Counter({rule.check_id: 0 for rule in _high_rules()})
        if set(runner_verified_critical) - set(critical):
            raise IdentityExactGroupHistoryRunnerError(
                "runner critical QA map contains uncontracted checks"
            )
        critical.update(runner_verified_critical)
        slots: list[dict[str, object]] = []
        sequences: list[dict[str, object]] = []
        examples: dict[str, list[dict[str, object]]] = defaultdict(list)
        for ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS:
            sessions = sorted(self._groups[ticker])
            if not sessions:
                critical["exact_group_asset_omission_rows"] += 1
                continue
            runs = _segment_observed_sessions(sessions, calendar_index)
            run_metadata: dict[date, tuple[int, int, tuple[date, ...]]] = {}
            for run_ordinal, run in enumerate(runs):
                for session_ordinal, session in enumerate(run):
                    run_metadata[session] = (run_ordinal, session_ordinal, run)
            evidence_manifest = evidence_by_ticker[ticker]
            group_slots: list[dict[str, object]] = []
            for ordinal, session in enumerate(sessions):
                observed = self._groups[ticker][session]
                previous = None if ordinal == 0 else sessions[ordinal - 1]
                adjacent = (
                    None
                    if previous is None
                    else calendar_index[session] - calendar_index[previous] == 1
                )
                run_ordinal, in_run_ordinal, run = run_metadata[session]
                slot = self._build_slot(
                    plan=plan,
                    scope_id=scope_id,
                    ticker=ticker,
                    composite=composite,
                    observed=observed,
                    previous=previous,
                    previous_adjacent=adjacent,
                    run_ordinal=run_ordinal,
                    in_run_ordinal=in_run_ordinal,
                    run=run,
                    all_group_sessions=sessions,
                    run_count=len(runs),
                    evidence=evidence_manifest,
                    critical=critical,
                    high=high,
                    examples=examples,
                )
                group_slots.append(slot)
            _update_edge_warnings(group_slots, high, examples)
            if len(runs) > 1:
                high["multiple_exact_observed_run_groups"] += 1
                examples["multiple_exact_observed_run_groups"].append(
                    {
                        "exact_group_observed_composite_figi": composite,
                        "review_group_id": _review_group_id(ticker, composite),
                        "run_count": len(runs),
                        "run_spans": [
                            {
                                "end": run[-1].isoformat(),
                                "start": run[0].isoformat(),
                            }
                            for run in runs
                        ],
                        "ticker": ticker,
                    }
                )
            if sessions[0] == EXACT_GROUP_HISTORY_START_SESSION or sessions[-1] == (
                EXACT_GROUP_HISTORY_END_SESSION
            ):
                high["release_boundary_touching_groups"] += 1
                examples["release_boundary_touching_groups"].append(
                    {
                        "first_observed_session": sessions[0].isoformat(),
                        "last_observed_session": sessions[-1].isoformat(),
                        "review_group_id": _review_group_id(ticker, composite),
                        "ticker": ticker,
                    }
                )
            slots.extend(group_slots)
            sequences.append(
                {
                    "exact_effective_interval_proven": False,
                    "exact_group_observed_composite_figi": composite,
                    "observed_interval_state": EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
                    "review_group_id": _review_group_id(ticker, composite),
                    "runs": [
                        {
                            "end_observed_session": run[-1].isoformat(),
                            "exact_effective_interval_proven": False,
                            "observed_sessions": [item.isoformat() for item in run],
                            "run_id": _observed_run_id(ticker, composite, run),
                            "run_ordinal": run_ordinal,
                            "start_observed_session": run[0].isoformat(),
                        }
                        for run_ordinal, run in enumerate(runs)
                    ],
                    "ticker": ticker,
                }
            )
        slots.sort(key=lambda item: (str(item["ticker"]), item["session_date"]))
        if len({(item["review_group_id"], item["session_date"]) for item in slots}) != len(slots):
            critical["primary_key_duplicate_excess"] += 1
        if [(item["ticker"], item["session_date"]) for item in slots] != sorted(
            (item["ticker"], item["session_date"]) for item in slots
        ):
            critical["output_sort_invalid"] += 1
        if any(
            value is not False
            for item in slots
            for field, value in item.items()
            if field.endswith("_eligible")
        ):
            critical["capability_true_rows"] += 1
        qa = _qa_document(critical, high, denominator=len(slots))
        failed = [
            item["check_id"]
            for item in qa["checks"]
            if item["severity"] == "critical" and item["status"] == "failed"
        ]
        if failed:
            raise IdentityExactGroupHistoryRunnerError(
                f"critical exact-group history QA failed: {sorted(failed)}"
            )
        missing_high_examples = sorted(
            check_id for check_id, count in high.items() if count and not examples.get(check_id)
        )
        if missing_high_examples:
            raise IdentityExactGroupHistoryRunnerError(
                f"nonzero High QA lacks bounded examples: {missing_high_examples}"
            )
        sequence_document = MappingProxyType(
            {
                "artifact_type": "s7_exact_group_history_group_sequences",
                "capabilities": _false_capabilities(),
                "created_at_utc": created.isoformat(),
                "exact_effective_interval_proven": False,
                "groups": sequences,
                "observed_interval_state": EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
                "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
                "schema_version": 1,
            }
        )
        example_document = MappingProxyType(
            {
                "artifact_type": "s7_exact_group_history_bounded_examples",
                "capabilities": _false_capabilities(),
                "examples": {key: values[:20] for key, values in sorted(examples.items())},
                "reason_counts": {key: value for key, value in sorted(high.items()) if value},
                "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
                "schema_version": 1,
            }
        )
        return ExactGroupHistoryBuild(
            slots=tuple(MappingProxyType(item) for item in slots),
            evidence_manifests=evidence,
            sequences=sequence_document,
            qa=MappingProxyType(qa),
            examples=example_document,
        )

    def _build_slot(
        self,
        *,
        plan: object,
        scope_id: str,
        ticker: str,
        composite: str,
        observed: _ObservedSession,
        previous: date | None,
        previous_adjacent: bool | None,
        run_ordinal: int,
        in_run_ordinal: int,
        run: tuple[date, ...],
        all_group_sessions: list[date],
        run_count: int,
        evidence: ExactGroupHistoryEvidenceManifestV2,
        critical: Counter[str],
        high: Counter[str],
        examples: dict[str, list[dict[str, object]]],
    ) -> dict[str, object]:
        exact = observed.exact_assets
        assets = observed.all_assets
        universes = observed.universes
        if not exact:
            critical["exact_group_asset_match_count_invalid_rows"] += 1
        if len(universes) > 1:
            critical["duplicate_universe_membership_rows"] += len(universes) - 1
        universe = universes[0] if len(universes) == 1 else None
        urow = universe.full_row_snapshot if universe is not None else None
        selected_id = None if urow is None else urow.get("selected_source_record_id")
        selected = [item for item in assets if item.source_record_id == selected_id]
        if universe is not None and not selected:
            critical["selected_parent_missing_rows"] += 1
        if len(selected) > 1:
            critical["selected_parent_multiple_rows"] += len(selected) - 1
        parent = selected[0] if len(selected) == 1 else None
        projection = (
            None
            if universe is None
            else parent is not None and _projection_matches(parent.full_row_snapshot, urow)
        )
        if projection is False:
            critical["selected_parent_projection_mismatch_rows"] += 1
        parent_exact = (
            None
            if universe is None
            else parent is not None and _attestation_matches_exact_group(parent, ticker, composite)
        )
        if parent_exact is False:
            high["selected_parent_other_composite_sessions"] += 1
            examples["selected_parent_other_composite_sessions"].append(
                _bounded_example(ticker, observed)
            )
        if universe is None:
            high["exact_group_asset_only_sessions"] += 1
            examples["exact_group_asset_only_sessions"].append(_bounded_example(ticker, observed))
        nonselected_exact = [item for item in exact if item is not parent]
        high["nonselected_exact_group_asset_versions"] += len(nonselected_exact)
        if nonselected_exact:
            examples["nonselected_exact_group_asset_versions"].append(
                {
                    **_bounded_example(ticker, observed),
                    "nonselected_exact_attestation_ids": [
                        item.row_attestation_id for item in nonselected_exact
                    ],
                }
            )
        projections = {
            (
                item.full_row_snapshot.get("share_class_figi"),
                item.full_row_snapshot.get("cik"),
                item.full_row_snapshot.get("primary_exchange_mic"),
                item.full_row_snapshot.get("type_code"),
                item.full_row_snapshot.get("provider_active"),
            )
            for item in exact
        }
        variant_count = max(0, len(projections) - 1)
        if variant_count:
            high["same_session_exact_group_identity_variant_groups"] += 1
            examples["same_session_exact_group_identity_variant_groups"].append(
                _bounded_example(ticker, observed)
            )
        membership = (
            "absent_source_membership"
            if universe is None
            else "present_active"
            if urow["active_on_date"] is True
            else "present_inactive"
        )
        run_id = _observed_run_id(ticker, composite, run)
        parent_row = None if parent is None else parent.full_row_snapshot
        return {
            "review_group_id": _review_group_id(ticker, composite),
            "review_scope_set_id": scope_id,
            "provider_id": EXACT_GROUP_HISTORY_PROVIDER_ID,
            "provider_market": EXACT_GROUP_HISTORY_PROVIDER_MARKET,
            "provider_locale": EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
            "ticker": ticker,
            "exact_group_observed_composite_figi": composite,
            "s4_release_set_id": EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
            "inventory_completion_id": str(plan.inventory_completion_id),
            "directional_preview_candidate_id": str(plan.directional_preview_candidate_id),
            "directional_preview_completion_id": str(plan.directional_preview_completion_id),
            "session_date": observed.session_date,
            "previous_observed_session": previous,
            "previous_observed_session_is_adjacent_xnys": previous_adjacent,
            "exact_observed_run_id": run_id,
            "exact_observed_run_ordinal": run_ordinal,
            "observed_session_ordinal_in_run": in_run_ordinal,
            "exact_observed_run_start_session": run[0],
            "exact_observed_run_end_session": run[-1],
            "exact_observed_run_session_count": len(run),
            "group_first_observed_session": all_group_sessions[0],
            "group_last_observed_session": all_group_sessions[-1],
            "group_observed_session_count": len(all_group_sessions),
            "group_exact_observed_run_count": run_count,
            "exact_asset_observation_match_count": len(exact),
            "exact_asset_observation_attestation_ids_json": _json_array(
                [item.row_attestation_id for item in exact]
            ),
            "exact_group_observed_share_class_figis_json": _json_array_distinct(
                [item.full_row_snapshot.get("share_class_figi") for item in exact]
            ),
            "exact_group_observed_ciks_json": _json_array_distinct(
                [item.full_row_snapshot.get("cik") for item in exact]
            ),
            "exact_group_observed_primary_exchange_mics_json": _json_array_distinct(
                [item.full_row_snapshot.get("primary_exchange_mic") for item in exact]
            ),
            "exact_group_observed_type_codes_json": _json_array_distinct(
                [item.full_row_snapshot.get("type_code") for item in exact]
            ),
            "exact_group_provider_active_values_json": _json_array_distinct(
                [item.full_row_snapshot.get("provider_active") for item in exact]
            ),
            "nonselected_exact_group_asset_observation_count": len(nonselected_exact),
            "same_session_exact_group_identity_variant_count": variant_count,
            "universe_membership_count": len(universes),
            "membership_status": membership,
            "active_on_date": None if urow is None else urow["active_on_date"],
            "universe_row_attestation_id": (
                None if universe is None else universe.row_attestation_id
            ),
            "selected_source_record_id": selected_id,
            "source_version_count": None if urow is None else urow["source_version_count"],
            "version_group_id": None if urow is None else urow["version_group_id"],
            "selection_status": None if urow is None else urow["selection_status"],
            "selected_parent_asset_match_count": len(selected),
            "selected_parent_attestation_id": (
                None if parent is None else parent.row_attestation_id
            ),
            "selected_parent_projection_match": projection,
            "selected_parent_matches_exact_group": parent_exact,
            "selected_parent_observed_composite_figi": (
                None if parent_row is None else parent_row.get("composite_figi")
            ),
            "selected_parent_observed_share_class_figi": (
                None if parent_row is None else parent_row.get("share_class_figi")
            ),
            "selected_parent_observed_cik": (None if parent_row is None else parent_row.get("cik")),
            "selected_parent_observed_primary_exchange_mic": (
                None if parent_row is None else parent_row.get("primary_exchange_mic")
            ),
            "selected_parent_observed_type_code": (
                None if parent_row is None else parent_row.get("type_code")
            ),
            "selected_parent_source_available_session": (
                None if parent is None else parent.source_available_session
            ),
            "exact_group_evidence_manifest_id": evidence.manifest_id,
            "exact_group_evidence_manifest_path": evidence.candidate_relative_path,
            "exact_group_evidence_manifest_sha256": evidence.sha256,
            "observed_interval_state": EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
            "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
            "adjudication_eligible": False,
            "canonical_candidate_eligible": False,
            "transition_candidate_eligible": False,
            "exact_override_interval_eligible": False,
            "full_run_eligible": False,
            "publication_eligible": False,
        }


def _attestation_matches_exact_group(
    item: ProviderRowAttestation, ticker: str, composite: str
) -> bool:
    row = item.full_row_snapshot
    return (
        item.dataset == ASSET_TABLE
        and row.get("ticker") == ticker
        and row.get("market") == EXACT_GROUP_HISTORY_PROVIDER_MARKET
        and row.get("locale") == EXACT_GROUP_HISTORY_PROVIDER_LOCALE
        and row.get("composite_figi") == composite
    )


def _projection_matches(asset: Mapping[str, object], universe: Mapping[str, object]) -> bool:
    return all(
        asset.get(asset_field) == universe.get(universe_field)
        for asset_field, universe_field in UNIVERSE_PARENT_PROJECTION
    )


def _segment_observed_sessions(
    sessions: Sequence[date], calendar_index: Mapping[date, int]
) -> tuple[tuple[date, ...], ...]:
    if not sessions:
        return ()
    runs: list[list[date]] = [[sessions[0]]]
    try:
        for session in sessions[1:]:
            if calendar_index[session] - calendar_index[runs[-1][-1]] == 1:
                runs[-1].append(session)
            else:
                runs.append([session])
    except KeyError as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "observed_run_segmentation_invalid_rows"
        ) from exc
    return tuple(tuple(run) for run in runs)


def _observed_run_id(ticker: str, composite: str, run: Sequence[date]) -> str:
    return stable_digest(
        {
            "namespace": "ame_stocks.s7.exact_group_history.observed_run.v1",
            "observed_composite_figi": composite,
            "observed_sessions": [item.isoformat() for item in run],
            "review_group_id": _review_group_id(ticker, composite),
            "ticker": ticker,
        }
    )


def _json_array(values: Sequence[object]) -> str:
    return json.dumps(list(values), allow_nan=False, ensure_ascii=False, separators=(",", ":"))


def _json_array_distinct(values: Sequence[object]) -> str:
    unique = {json.dumps(value, allow_nan=False, sort_keys=True): value for value in values}
    ordered = [unique[key] for key in sorted(unique)]
    return _json_array(ordered)


def _bounded_example(ticker: str, observed: _ObservedSession) -> dict[str, object]:
    return {
        "asset_attestation_ids": [item.row_attestation_id for item in observed.all_assets],
        "session_date": observed.session_date.isoformat(),
        "ticker": ticker,
        "universe_attestation_ids": [item.row_attestation_id for item in observed.universes],
    }


def _update_edge_warnings(
    slots: Sequence[Mapping[str, object]],
    high: Counter[str],
    examples: dict[str, list[dict[str, object]]],
) -> None:
    fields = (
        ("exact_group_observed_share_class_figis_json", "observed_share_class_change_edges"),
        ("exact_group_observed_ciks_json", "observed_cik_change_edges"),
        (
            "exact_group_observed_primary_exchange_mics_json",
            "observed_primary_exchange_mic_change_edges",
        ),
        ("exact_group_observed_type_codes_json", "observed_type_code_change_edges"),
    )
    for left, right in pairwise(slots):
        if right["previous_observed_session_is_adjacent_xnys"] is False:
            high["exact_observed_run_gap_edges"] += 1
            examples["exact_observed_run_gap_edges"].append(_edge_example(left, right))
        for field, check_id in fields:
            if left[field] != right[field]:
                high[check_id] += 1
                examples[check_id].append({**_edge_example(left, right), "changed_field": field})
        left_active_state = (
            left["exact_group_provider_active_values_json"],
            left["membership_status"],
            left["active_on_date"],
        )
        right_active_state = (
            right["exact_group_provider_active_values_json"],
            right["membership_status"],
            right["active_on_date"],
        )
        # Membership absence is an observed source state, not an inference of
        # inactivity; a transition to/from absence remains a reviewable edge.
        if left_active_state != right_active_state:
            high["observed_active_status_change_edges"] += 1
            examples["observed_active_status_change_edges"].append(
                {
                    **_edge_example(left, right),
                    "left_active_state": list(left_active_state),
                    "right_active_state": list(right_active_state),
                }
            )


def _edge_example(left: Mapping[str, object], right: Mapping[str, object]) -> dict[str, object]:
    return {
        "left_session": left["session_date"].isoformat(),
        "review_group_id": left["review_group_id"],
        "right_session": right["session_date"].isoformat(),
        "ticker": left["ticker"],
    }


def _critical_rules() -> tuple[Any, ...]:
    return tuple(
        rule
        for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules
        if rule.severity.value == "critical"
    )


def _high_rules() -> tuple[Any, ...]:
    return tuple(
        rule
        for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules
        if rule.severity.value == "high"
    )


def _qa_document(
    critical: Counter[str], high: Counter[str], *, denominator: int
) -> dict[str, object]:
    expected_critical = {rule.check_id for rule in _critical_rules()}
    expected_high = {rule.check_id for rule in _high_rules()}
    if set(critical) != expected_critical or set(high) != expected_high:
        raise IdentityExactGroupHistoryRunnerError(
            "QA implementation does not match frozen contract"
        )
    checks = []
    for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules:
        numerator = (
            critical[rule.check_id] if rule.severity.value == "critical" else high[rule.check_id]
        )
        status = (
            QAStatus.FAILED.value
            if rule.severity.value == "critical" and numerator
            else QAStatus.WARNING.value
            if rule.severity.value == "high" and numerator
            else QAStatus.PASSED.value
        )
        checks.append(
            {
                "check_id": rule.check_id,
                "denominator": denominator,
                "numerator": numerator,
                "severity": rule.severity.value,
                "status": status,
            }
        )
    return {
        "artifact_type": "s7_exact_group_history_qa",
        "capabilities": _false_capabilities(),
        "checks": checks,
        "critical_failure_count": sum(item["status"] == "failed" for item in checks),
        "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
        "schema_version": 1,
        "warning_count": sum(item["status"] == "warning" for item in checks),
    }


@dataclass(frozen=True, slots=True, order=True)
class ExactGroupHistoryOutputRef:
    role: str
    path: str
    sha256: str
    bytes: int
    media_type: str
    row_count: int | None = None

    def __post_init__(self) -> None:
        if not self.role or not self.media_type:
            raise IdentityExactGroupHistoryRunnerError("output ref text is empty")
        _relative_path(self.path, "output path")
        _digest(self.sha256, "output SHA")
        _native_int(self.bytes, "output bytes")
        if self.row_count is not None:
            _native_int(self.row_count, "output rows")

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "bytes": self.bytes,
            "media_type": self.media_type,
            "path": self.path,
            "role": self.role,
            "sha256": self.sha256,
        }
        if self.row_count is not None:
            result["row_count"] = self.row_count
        return result

    @classmethod
    def from_dict(cls, value: object) -> ExactGroupHistoryOutputRef:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("output ref must be an object")
        item = dict(value)
        return cls(
            role=str(item["role"]),
            path=str(item["path"]),
            sha256=str(item["sha256"]),
            bytes=int(item["bytes"]),
            media_type=str(item["media_type"]),
            row_count=(None if "row_count" not in item else int(item["row_count"])),
        )


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryCandidate:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    execution_intent_id: str
    execution_intent_path: str
    execution_intent_sha256: str
    source_binding_id: str
    source_binding_sha256: str
    source_artifact_set_digest: str
    normalized_source_artifact_set_digest: str
    review_scope_set_id: str
    artifacts: tuple[ExactGroupHistoryOutputRef, ...]
    evidence_manifest_ids: tuple[str, ...]
    created_at_utc: datetime
    candidate_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("Plan ID", self.plan_id),
            ("Plan SHA", self.plan_sha256),
            ("Approval ID", self.approval_id),
            ("Approval SHA", self.approval_sha256),
            ("Request ID", self.request_event_id),
            ("Request SHA", self.request_event_sha256),
            ("intent ID", self.execution_intent_id),
            ("intent SHA", self.execution_intent_sha256),
            ("source binding ID", self.source_binding_id),
            ("source binding SHA", self.source_binding_sha256),
            ("source digest", self.source_artifact_set_digest),
            ("normalized source digest", self.normalized_source_artifact_set_digest),
            ("scope-set ID", self.review_scope_set_id),
        ):
            _digest(value, label)
        _relative_path(self.execution_intent_path, "execution intent path")
        object.__setattr__(
            self, "created_at_utc", _utc(self.created_at_utc, "candidate created_at_utc")
        )
        artifacts = tuple(sorted(self.artifacts))
        if len({item.role for item in artifacts}) != len(artifacts) or len(
            {item.path for item in artifacts}
        ) != len(artifacts):
            raise IdentityExactGroupHistoryRunnerError("candidate output refs duplicate")
        expected_roles = {
            "review_slots",
            "group_sequences",
            "qa",
            "bounded_examples",
            *(f"group_evidence:{ticker}" for ticker in EXACT_GROUP_HISTORY_FIXED_TICKERS),
        }
        if {item.role for item in artifacts} != expected_roles:
            raise IdentityExactGroupHistoryRunnerError("candidate output role set differs")
        evidence = tuple(sorted(self.evidence_manifest_ids))
        if len(evidence) != 3 or len(set(evidence)) != 3:
            raise IdentityExactGroupHistoryRunnerError(
                "candidate requires exactly three evidence manifests"
            )
        if self.candidate_state != CANDIDATE_STATE:
            raise IdentityExactGroupHistoryRunnerError("candidate state differs")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "evidence_manifest_ids", evidence)

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_exact_group_history_candidate",
            "artifacts": [item.to_dict() for item in self.artifacts],
            "candidate_rule_version": CANDIDATE_RULE_VERSION,
            "candidate_state": self.candidate_state,
            "capabilities": _false_capabilities(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "evidence_manifest_ids": list(self.evidence_manifest_ids),
            "execution_intent_id": self.execution_intent_id,
            "execution_intent_path": self.execution_intent_path,
            "execution_intent_sha256": self.execution_intent_sha256,
            "normalized_source_artifact_set_digest": (self.normalized_source_artifact_set_digest),
            "observed_interval_state": EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "registry_evaluation_state": EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "review_scope_set_id": self.review_scope_set_id,
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_binding_id": self.source_binding_id,
            "source_binding_sha256": self.source_binding_sha256,
        }

    @property
    def candidate_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "candidate_id": self.candidate_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_directory(self) -> str:
        return (
            "manifests/silver/identity/exact-group-history-candidates/"
            f"candidate_id={self.candidate_id}"
        )

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryCandidate:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("candidate must be an object")
        document = dict(value)
        try:
            candidate = cls(
                plan_id=str(document["plan_id"]),
                plan_sha256=str(document["plan_sha256"]),
                approval_id=str(document["approval_id"]),
                approval_sha256=str(document["approval_sha256"]),
                request_event_id=str(document["request_event_id"]),
                request_event_sha256=str(document["request_event_sha256"]),
                execution_intent_id=str(document["execution_intent_id"]),
                execution_intent_path=str(document["execution_intent_path"]),
                execution_intent_sha256=str(document["execution_intent_sha256"]),
                source_binding_id=str(document["source_binding_id"]),
                source_binding_sha256=str(document["source_binding_sha256"]),
                source_artifact_set_digest=str(document["source_artifact_set_digest"]),
                normalized_source_artifact_set_digest=str(
                    document["normalized_source_artifact_set_digest"]
                ),
                review_scope_set_id=str(document["review_scope_set_id"]),
                artifacts=tuple(
                    ExactGroupHistoryOutputRef.from_dict(item) for item in document["artifacts"]
                ),
                evidence_manifest_ids=tuple(document["evidence_manifest_ids"]),
                created_at_utc=_parse_utc(document["created_at_utc"], "candidate time"),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise IdentityExactGroupHistoryRunnerError("candidate is malformed") from exc
        if candidate.content != _canonical_bytes(document):
            raise IdentityExactGroupHistoryRunnerError("candidate canonical bytes differ")
        return candidate


def exact_group_history_completion_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "Plan ID")
    _digest(approval_id, "Approval ID")
    return (
        "manifests/silver/identity/exact-group-history-execution-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


@dataclass(frozen=True, slots=True)
class S7ExactGroupHistoryCompletion:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    execution_intent_id: str
    execution_intent_path: str
    execution_intent_sha256: str
    candidate_id: str
    candidate_path: str
    candidate_sha256: str
    output_artifacts: tuple[ExactGroupHistoryOutputRef, ...]
    completed_at_utc: datetime
    source_artifact_count: int
    source_row_count: int
    source_bytes: int
    output_slot_row_count: int
    peak_rss_bytes: int
    wall_clock_seconds: float
    output_bytes: int
    completion_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("Plan ID", self.plan_id),
            ("Plan SHA", self.plan_sha256),
            ("Approval ID", self.approval_id),
            ("Approval SHA", self.approval_sha256),
            ("Request ID", self.request_event_id),
            ("Request SHA", self.request_event_sha256),
            ("intent ID", self.execution_intent_id),
            ("intent SHA", self.execution_intent_sha256),
            ("candidate ID", self.candidate_id),
            ("candidate SHA", self.candidate_sha256),
        ):
            _digest(value, label)
        _relative_path(self.execution_intent_path, "intent path")
        _relative_path(self.candidate_path, "candidate path")
        object.__setattr__(self, "completed_at_utc", _utc(self.completed_at_utc, "completion time"))
        for label, value in (
            ("source artifact count", self.source_artifact_count),
            ("source row count", self.source_row_count),
            ("source bytes", self.source_bytes),
            ("slot rows", self.output_slot_row_count),
            ("peak RSS", self.peak_rss_bytes),
            ("output bytes", self.output_bytes),
        ):
            _native_int(value, label)
        if (
            type(self.wall_clock_seconds) is not float
            or not math.isfinite(self.wall_clock_seconds)
            or self.wall_clock_seconds < 0
            or self.completion_state != CANDIDATE_STATE
        ):
            raise IdentityExactGroupHistoryRunnerError("completion state/resources differ")

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_exact_group_history_execution_completion",
            "candidate": {
                "candidate_id": self.candidate_id,
                "path": self.candidate_path,
                "sha256": self.candidate_sha256,
            },
            "capabilities": _false_capabilities(),
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "completion_rule_version": COMPLETION_RULE_VERSION,
            "completion_state": self.completion_state,
            "counts": {
                "output_slot_row_count": self.output_slot_row_count,
                "source_artifact_count": self.source_artifact_count,
                "source_bytes": self.source_bytes,
                "source_row_count": self.source_row_count,
            },
            "execution_intent_id": self.execution_intent_id,
            "execution_intent_path": self.execution_intent_path,
            "execution_intent_sha256": self.execution_intent_sha256,
            "output_artifacts": [item.to_dict() for item in self.output_artifacts],
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "resource_measurements": {
                "output_bytes": self.output_bytes,
                "peak_rss_bytes": self.peak_rss_bytes,
                "wall_clock_seconds": self.wall_clock_seconds,
            },
            "schema_version": COMPLETION_SCHEMA_VERSION,
        }

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "completion_id": self.completion_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> S7ExactGroupHistoryCompletion:
        if not isinstance(value, Mapping):
            raise IdentityExactGroupHistoryRunnerError("completion must be an object")
        document = dict(value)
        candidate = document.get("candidate")
        counts = document.get("counts")
        resources = document.get("resource_measurements")
        if not all(isinstance(item, Mapping) for item in (candidate, counts, resources)):
            raise IdentityExactGroupHistoryRunnerError("completion sections are malformed")
        try:
            completion = cls(
                plan_id=str(document["plan_id"]),
                plan_sha256=str(document["plan_sha256"]),
                approval_id=str(document["approval_id"]),
                approval_sha256=str(document["approval_sha256"]),
                request_event_id=str(document["request_event_id"]),
                request_event_sha256=str(document["request_event_sha256"]),
                execution_intent_id=str(document["execution_intent_id"]),
                execution_intent_path=str(document["execution_intent_path"]),
                execution_intent_sha256=str(document["execution_intent_sha256"]),
                candidate_id=str(candidate["candidate_id"]),
                candidate_path=str(candidate["path"]),
                candidate_sha256=str(candidate["sha256"]),
                output_artifacts=tuple(
                    ExactGroupHistoryOutputRef.from_dict(item)
                    for item in document["output_artifacts"]
                ),
                completed_at_utc=_parse_utc(document["completed_at_utc"], "completion time"),
                source_artifact_count=int(counts["source_artifact_count"]),
                source_row_count=int(counts["source_row_count"]),
                source_bytes=int(counts["source_bytes"]),
                output_slot_row_count=int(counts["output_slot_row_count"]),
                peak_rss_bytes=int(resources["peak_rss_bytes"]),
                wall_clock_seconds=float(resources["wall_clock_seconds"]),
                output_bytes=int(resources["output_bytes"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise IdentityExactGroupHistoryRunnerError("completion is malformed") from exc
        if completion.content != _canonical_bytes(document):
            raise IdentityExactGroupHistoryRunnerError("completion canonical bytes differ")
        return completion


@dataclass(frozen=True, slots=True)
class _LoadedControls:
    plan: object
    request: object
    approval: S7ExactGroupHistoryExecutionApproval


class _ResourceMonitor:
    def __init__(self, *, root: Path, staging: Path, started: float, plan: object) -> None:
        self.root = root
        self.staging = staging
        self.started = started
        self.rss_cap = max(
            RSS_HARD_FLOOR_BYTES,
            _plan_cap(plan, "rss_bytes_hard_cap", RSS_HARD_FLOOR_BYTES),
        )
        self.wall_cap = _plan_cap(
            plan, "wall_clock_seconds_hard_cap", DEFAULT_WALL_CLOCK_CAP_SECONDS
        )
        self.disk_floor = _plan_cap(plan, "disk_free_bytes_hard_floor", DEFAULT_DISK_FLOOR_BYTES)
        self.tmp_cap = _plan_cap(plan, "tmp_bytes_hard_cap", DEFAULT_TMP_CAP_BYTES)
        self.output_cap = _plan_cap(plan, "output_bytes_hard_cap", DEFAULT_OUTPUT_CAP_BYTES)
        self.peak_rss_bytes = 0

    def check(self) -> None:
        if time.monotonic() - self.started > self.wall_cap:
            raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: wall clock")
        rss = _peak_rss_bytes()
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
        if rss > self.rss_cap:
            raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: RSS")
        if shutil.disk_usage(self.root).free < self.disk_floor:
            raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: disk floor")
        if _tree_bytes(self.staging) > self.tmp_cap:
            raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: temporary bytes")


def run_exact_s7_exact_group_history_review(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> S7ExactGroupHistoryCompletion:
    """Execute the exact approved review once; no scope parameters are accepted."""

    started = time.monotonic()
    root = _validated_root(data_root)
    for label, value in (
        ("Plan ID", plan_id),
        ("Plan SHA", expected_plan_sha256),
        ("Approval ID", approval_id),
        ("Approval SHA", expected_approval_sha256),
    ):
        _digest(value, label)
    controls = _load_controls(
        root,
        plan_id=plan_id,
        expected_plan_sha256=expected_plan_sha256,
        approval_id=approval_id,
        expected_approval_sha256=expected_approval_sha256,
    )
    refs = _verify_controls_without_source_read(root, controls)
    paths = _execution_paths(plan_id, approval_id)
    completion_path = safe_relative_path(root, paths["completion"])
    if completion_path.is_file() and not completion_path.is_symlink():
        return _read_completed_without_source(root, completion_path, controls)
    if completion_path.exists() or completion_path.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("completion slot is unsafe")
    lock_path = safe_relative_path(root, paths["lock"])
    with _exclusive_lock(lock_path):
        if completion_path.is_file() and not completion_path.is_symlink():
            return _read_completed_without_source(root, completion_path, controls)
        intent_path = safe_relative_path(root, paths["intent"])
        staging = safe_relative_path(root, paths["staging"])
        if intent_path.exists() or intent_path.is_symlink():
            raise IdentityExactGroupHistoryRunnerError(
                "incomplete execution intent already exists; fail closed without source read"
            )
        if staging.exists() or staging.is_symlink():
            raise IdentityExactGroupHistoryRunnerError(
                "stale staging exists before execution intent"
            )
        intent = _store_execution_intent(root, controls)
        # No Parquet path may be opened, read, or hashed above this line.
        staging.mkdir(parents=True, exist_ok=False)
        monitor = _ResourceMonitor(root=root, staging=staging, started=started, plan=controls.plan)
        monitor.check()
        return _execute_after_intent(
            root,
            controls,
            refs=refs,
            intent=intent,
            staging=staging,
            completion_path=completion_path,
            monitor=monitor,
            started=started,
        )


def _load_controls(
    root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> _LoadedControls:
    try:
        from ame_stocks_api.silver.identity_exact_group_history_manifest import (
            ExactGroupHistoryManifestStore,
        )

        manifest_store = ExactGroupHistoryManifestStore(root)
        plan, _ = manifest_store.load_execution_plan(plan_id, expected_sha256=expected_plan_sha256)
        approval, _ = ExactGroupHistoryExecutionApprovalStore(root).load_approval(
            approval_id, expected_sha256=expected_approval_sha256
        )
        request, _ = manifest_store.load_execution_request(
            approval.request_event_id,
            expected_sha256=approval.request_event_sha256,
        )
    except (
        ImportError,
        IdentityExactGroupHistoryApprovalError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "exact execution controls cannot be loaded"
        ) from exc
    return _LoadedControls(plan=plan, request=request, approval=approval)


def _verify_controls_without_source_read(
    root: Path, controls: _LoadedControls
) -> tuple[ExactGroupHistorySourceArtifactRef, ...]:
    plan, request, approval = controls.plan, controls.request, controls.approval
    caps = getattr(plan, "execution_resource_caps", None)
    if (
        getattr(plan, "execution_data_root", None) != str(root)
        or approval.execution_data_root != str(root)
        or approval.plan_id != getattr(plan, "plan_id", None)
        or approval.plan_sha256 != getattr(plan, "sha256", None)
        or approval.request_event_id != getattr(request, "request_event_id", None)
        or approval.request_event_sha256 != getattr(request, "sha256", None)
        or getattr(request, "plan_id", None) != getattr(plan, "plan_id", None)
        or getattr(request, "plan_sha256", None) != getattr(plan, "sha256", None)
        or approval.approval_literal != getattr(request, "canonical_approval_literal", None)
    ):
        raise IdentityExactGroupHistoryRunnerError("execution controls cross bindings")
    required_true = (
        "exact_group_history_execution_authorized",
        "source_read_authorized",
        "parquet_read_authorized",
        "once_to_awaiting_review",
    )
    required_false = (
        "source_discovery_authorized",
        "caller_scope_override_authorized",
        "share_class_filter_authorized",
        "network_access_authorized",
        "external_evidence_capture_authorized",
        "registry_evaluation_authorized",
        "adjudication_authorized",
        "override_generation_authorized",
        "table_materialization_authorized",
        "full_run_authorized",
        "publication_authorized",
        "membership_mutation_authorized",
        "forced_liquidation_authorized",
    )
    if any(getattr(approval, field) is not True for field in required_true) or any(
        getattr(approval, field) is not False for field in required_false
    ):
        raise IdentityExactGroupHistoryRunnerError("approval capabilities drifted")
    refs = tuple(
        sorted(
            ExactGroupHistorySourceArtifactRef.from_plan_pin(item) for item in plan.source_artifacts
        )
    )
    if (
        caps is None
        or len(refs) != caps.physical_source_artifact_count
        or sum(item.row_count for item in refs) != caps.physical_source_row_count
        or sum(item.bytes for item in refs) != caps.physical_source_bytes
        or len({(item.table, item.session_date) for item in refs}) != len(refs)
        or stable_digest([item.to_dict() for item in refs])
        != getattr(plan, "normalized_source_artifact_set_digest", None)
    ):
        raise IdentityExactGroupHistoryRunnerError("exact S4 normalized source binding differs")
    by_table = {
        table: tuple(item for item in refs if item.table == table) for table in SOURCE_TABLES
    }
    sessions = tuple(item.session_date for item in by_table[ASSET_TABLE])
    if (
        len(sessions) != caps.xnys_session_count
        or sessions[0] != EXACT_GROUP_HISTORY_START_SESSION
        or sessions[-1] != EXACT_GROUP_HISTORY_END_SESSION
        or tuple(item.session_date for item in by_table[UNIVERSE_TABLE]) != sessions
        or caps.review_group_count != len(EXACT_GROUP_HISTORY_FIXED_GROUPS)
    ):
        raise IdentityExactGroupHistoryRunnerError("source session spine differs")
    if (
        getattr(plan, "contract_id", None) != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID
        or getattr(plan, "contract_schema_digest", None)
        != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
        or caps.rss_bytes_hard_cap != RSS_HARD_FLOOR_BYTES
    ):
        raise IdentityExactGroupHistoryRunnerError("schema_exact/input binding invalid")
    try:
        from ame_stocks_api.silver.identity_exact_group_history_manifest import (
            ExactGroupHistoryManifestStore,
        )

        store = ExactGroupHistoryManifestStore(root)
        manifest_plan = store.load_manifest_plan(plan.manifest_plan_id, plan.manifest_plan_sha256)
        match = re.search(r"run_intent_id=([0-9a-f]{64})/manifest\.json$", plan.source_binding_path)
        if match is None:
            raise IdentityExactGroupHistoryRunnerError("source binding path differs")
        source_binding = store.load_source_binding(match.group(1), plan.source_binding_sha256)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "source/manifest control binding cannot be loaded"
        ) from exc
    if (
        source_binding.source_binding_id != plan.source_binding_id
        or source_binding.relative_path != plan.source_binding_path
        or source_binding.sha256 != plan.source_binding_sha256
        or source_binding.raw_source_artifact_set_digest != plan.raw_source_artifact_set_digest
        or source_binding.inventory_projection_set_digest != plan.inventory_projection_set_digest
        or source_binding.normalized_source_artifact_set_digest
        != plan.normalized_source_artifact_set_digest
        or source_binding.execution_source_pins != plan.source_artifacts
        or source_binding.manifest_plan_id != plan.manifest_plan_id
        or source_binding.manifest_plan_sha256 != plan.manifest_plan_sha256
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "raw/normalized/source-binding projection differs"
        )
    _verify_git_and_runtime_pins(plan, manifest_plan)
    return refs


def _store_execution_intent(
    root: Path, controls: _LoadedControls
) -> S7ExactGroupHistoryExecutionIntent:
    plan, approval = controls.plan, controls.approval
    intent = S7ExactGroupHistoryExecutionIntent(
        created_at_utc=datetime.now(UTC),
        plan_id=str(plan.plan_id),
        plan_sha256=str(plan.sha256),
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_data_root=str(root),
        source_binding_id=str(plan.source_binding_id),
        source_binding_sha256=str(plan.source_binding_sha256),
        source_artifact_set_digest=str(plan.source_artifact_set_digest),
        normalized_source_artifact_set_digest=str(plan.normalized_source_artifact_set_digest),
        fixed_scope_digest=EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
        contract_id=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
        qa_semantics_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST,
        observed_run_semantics_digest=(EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST),
    )
    try:
        destination = safe_relative_path(root, intent.relative_path)
        receipt = write_bytes_immutable(root, destination, intent.content)
    except ArtifactError as exc:
        raise IdentityExactGroupHistoryRunnerError("execution intent cannot be persisted") from exc
    if (
        receipt["path"] != intent.relative_path
        or receipt["sha256"] != intent.sha256
        or receipt["bytes"] != len(intent.content)
        or destination.read_bytes() != intent.content
    ):
        raise IdentityExactGroupHistoryRunnerError("execution intent immutable readback differs")
    return intent


@dataclass(frozen=True, slots=True)
class _ScanResult:
    engine: ExactGroupHistoryEngine
    exact_attestation_ids: frozenset[str]
    scanned_artifacts: int
    scanned_rows: int
    scanned_bytes: int


def _execute_after_intent(
    root: Path,
    controls: _LoadedControls,
    *,
    refs: tuple[ExactGroupHistorySourceArtifactRef, ...],
    intent: S7ExactGroupHistoryExecutionIntent,
    staging: Path,
    completion_path: Path,
    monitor: _ResourceMonitor,
    started: float,
) -> S7ExactGroupHistoryCompletion:
    """Run only after the immutable intent exists on durable storage."""

    from ame_stocks_api.silver.identity_exact_group_history_plan import (
        CALENDAR_ARTIFACT_ID,
        CALENDAR_ARTIFACT_SHA256,
    )

    try:
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=CALENDAR_ARTIFACT_ID,
            expected_sha256=CALENDAR_ARTIFACT_SHA256,
        )
    except XNYSCalendarArtifactError as exc:
        raise IdentityExactGroupHistoryRunnerError("bound XNYS calendar cannot be loaded") from exc
    sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if EXACT_GROUP_HISTORY_START_SESSION <= item.session_date <= EXACT_GROUP_HISTORY_END_SESSION
    )
    if (
        len(sessions) != controls.plan.execution_resource_caps.xnys_session_count
        or sessions[0] != EXACT_GROUP_HISTORY_START_SESSION
        or sessions[-1] != EXACT_GROUP_HISTORY_END_SESSION
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "observed_run_segmentation_invalid_rows: calendar spine"
        )
    monitor.check()
    scanned = _scan_exact_sources(root, refs, calendar=calendar, controls=controls, monitor=monitor)
    monitor.check()
    retained = scanned.engine.retained_attestations
    _replay_provider_rows(root, refs, retained, calendar=calendar, monitor=monitor)
    retained_exact = frozenset(
        item.row_attestation_id
        for item in retained
        if any(
            _attestation_matches_exact_group(item, ticker, composite)
            for ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS
        )
    )
    if retained_exact != scanned.exact_attestation_ids:
        raise IdentityExactGroupHistoryRunnerError(
            "exact_group_asset_omission_rows: retained exact locators differ"
        )
    created = datetime.now(UTC)
    build = scanned.engine.finish(
        plan=controls.plan,
        approval=controls.approval,
        intent=intent,
        calendar=calendar,
        created_at_utc=created,
        runner_verified_critical={},
    )
    monitor.check()
    return _stage_commit_and_complete(
        root,
        controls,
        build,
        refs=refs,
        intent=intent,
        staging=staging,
        completion_path=completion_path,
        monitor=monitor,
        started=started,
        scanned=scanned,
        created_at_utc=created,
        calendar=calendar,
    )


def _scan_exact_sources(
    root: Path,
    refs: Sequence[ExactGroupHistorySourceArtifactRef],
    *,
    calendar: XNYSCalendarArtifact,
    controls: _LoadedControls,
    monitor: _ResourceMonitor,
) -> _ScanResult:
    """Perform one full physical scan of only the 5,026 frozen Plan artifacts."""

    caps = controls.plan.execution_resource_caps
    engine = ExactGroupHistoryEngine(selected_row_cap=caps.selected_row_hard_cap)
    by_session: dict[date, dict[str, list[ProviderRowAttestation]]] = defaultdict(
        lambda: {ASSET_TABLE: [], UNIVERSE_TABLE: []}
    )
    exact_ids: set[str] = set()
    selected_rows = 0
    rows = 0
    bytes_scanned = 0
    artifacts = 0
    for ref in refs:
        contract = ASSET_CONTRACTS[ref.table]
        pin = S7_SOURCE_PINS[ref.table]
        if (
            ref.release_id != pin.release_id
            or ref.release_manifest_sha256 != pin.release_manifest_sha256
            or ref.source_contract_id != contract.contract_id
            or ref.schema_digest != contract.schema_digest
        ):
            raise IdentityExactGroupHistoryRunnerError(
                "s4_source_binding_invalid: release or contract"
            )
        path = safe_relative_path(root, ref.path)
        digest = _sha256_regular_nofollow(path, expected_size=ref.bytes)
        if digest != ref.sha256:
            raise IdentityExactGroupHistoryRunnerError(
                "source_artifact_integrity_invalid: SHA differs"
            )
        try:
            parquet = pq.ParquetFile(path)
        except (OSError, pa.ArrowException) as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "source_artifact_integrity_invalid: Parquet open"
            ) from exc
        if (
            not parquet.schema_arrow.equals(contract.arrow_schema)
            or parquet.metadata.num_rows != ref.row_count
        ):
            raise IdentityExactGroupHistoryRunnerError("schema_exact/source_scan_count_mismatch")
        artifact_rows = 0
        for row_group in range(parquet.num_row_groups):
            row_index_in_group = 0
            for batch in parquet.iter_batches(
                row_groups=[row_group], batch_size=caps.batch_row_count
            ):
                artifact_rows += batch.num_rows
                ticker_index = batch.schema.get_field_index("ticker")
                if ticker_index < 0:
                    raise IdentityExactGroupHistoryRunnerError("schema_exact")
                tickers = batch.column(ticker_index).to_pylist()
                for offset, ticker in enumerate(tickers):
                    if ticker not in EXACT_GROUP_HISTORY_FIXED_TICKERS:
                        continue
                    row = batch.slice(offset, 1).to_pylist()[0]
                    attestation = _attest_source_row(
                        ref,
                        row_group=row_group,
                        row_index_in_group=row_index_in_group + offset,
                        row=row,
                        calendar=calendar,
                    )
                    selected_rows += 1
                    if selected_rows > caps.selected_row_hard_cap:
                        raise IdentityExactGroupHistoryRunnerError(
                            "resource_cap_exceeded: selected rows"
                        )
                    by_session[ref.session_date][ref.table].append(attestation)
                    if ref.table == ASSET_TABLE and any(
                        _attestation_matches_exact_group(attestation, fixed_ticker, composite)
                        for fixed_ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS
                    ):
                        exact_ids.add(attestation.row_attestation_id)
                row_index_in_group += batch.num_rows
        if artifact_rows != ref.row_count:
            raise IdentityExactGroupHistoryRunnerError("source_scan_count_mismatch")
        rows += artifact_rows
        bytes_scanned += ref.bytes
        artifacts += 1
        monitor.check()
    if (
        artifacts != caps.physical_source_artifact_count
        or rows != caps.physical_source_row_count
        or bytes_scanned != caps.physical_source_bytes
    ):
        raise IdentityExactGroupHistoryRunnerError("source_scan_count_mismatch")
    for session in sorted(by_session):
        tables = by_session[session]
        engine.consume_session(
            session,
            asset_attestations=tables[ASSET_TABLE],
            universe_attestations=tables[UNIVERSE_TABLE],
        )
    return _ScanResult(
        engine=engine,
        exact_attestation_ids=frozenset(exact_ids),
        scanned_artifacts=artifacts,
        scanned_rows=rows,
        scanned_bytes=bytes_scanned,
    )


def _attest_source_row(
    ref: ExactGroupHistorySourceArtifactRef,
    *,
    row_group: int,
    row_index_in_group: int,
    row: Mapping[str, object],
    calendar: XNYSCalendarArtifact,
) -> ProviderRowAttestation:
    contract = ASSET_CONTRACTS[ref.table]
    normalized = _normalize_source_row(row, contract)
    if normalized.get("session_date") != ref.session_date.isoformat():
        raise IdentityExactGroupHistoryRunnerError(
            "exact_group_scope_leakage_rows: session partition differs"
        )
    if ref.table == ASSET_TABLE:
        record_field = "source_record_id"
        basis_field = "source_capture_at_utc"
        availability_rule = "first_xnys_open_after_source_capture_v1"
    else:
        record_field = "selected_source_record_id"
        basis_field = "universe_capture_completed_at_utc"
        availability_rule = "first_xnys_open_after_complete_active_inactive_pair_v1"
    basis_raw = row.get(basis_field)
    if not isinstance(basis_raw, datetime):
        raise IdentityExactGroupHistoryRunnerError(
            "provider_row_attestation_schema_invalid_rows: availability basis"
        )
    basis = _utc(basis_raw, basis_field)
    try:
        available_session, available_at = calendar.first_open_after(basis)
    except XNYSCalendarArtifactError as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "provider_row_attestation_schema_invalid_rows: availability calendar"
        ) from exc
    if (
        "source_available_session" in normalized
        and normalized["source_available_session"] != available_session.isoformat()
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "row_attestation_replay_invalid_rows: available session"
        )
    if (
        "source_available_at_utc" in normalized
        and normalized["source_available_at_utc"] != available_at.isoformat()
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "row_attestation_replay_invalid_rows: available timestamp"
        )
    record_id = normalized.get(record_field)
    request_id = normalized.get("source_request_id")
    _digest(record_id, record_field)
    _digest(request_id, "source_request_id")
    full_row_digest = stable_digest(
        {
            "arrow_schema_digest": contract.schema_digest,
            "namespace": "ame_stocks.identity.provider_full_row",
            "row": normalized,
            "rule_version": "s7_provider_full_row_digest_v1",
        }
    )
    try:
        return ProviderRowAttestation(
            six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
            dataset=ref.table,
            release_id=ref.release_id,
            release_manifest_path=(f"manifests/silver/releases/release_id={ref.release_id}.json"),
            release_manifest_sha256=ref.release_manifest_sha256,
            contract_id=contract.contract_id,
            arrow_schema_digest=contract.schema_digest,
            silver_artifact_path=ref.path,
            silver_artifact_sha256=ref.sha256,
            parquet_row_group=row_group,
            row_index_in_row_group=row_index_in_group,
            primary_key={field: normalized[field] for field in contract.primary_key},
            source_record_id_field=record_field,
            source_record_id=str(record_id),
            source_request_id=str(request_id),
            full_row_digest=full_row_digest,
            full_row_snapshot=normalized,
            availability_basis_field=basis_field,
            availability_basis_at_utc=basis,
            source_available_session=available_session,
            source_available_at_utc=available_at,
            source_availability_rule=availability_rule,
            availability_calendar_id=calendar.calendar_artifact_id,
            availability_calendar_sha256=calendar.sha256,
        )
    except ProviderEvidenceError as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "provider_row_attestation_schema_invalid_rows"
        ) from exc


def _normalize_source_row(row: Mapping[str, object], contract: TableContract) -> dict[str, object]:
    if not isinstance(row, Mapping) or set(row) != set(contract.arrow_schema.names):
        raise IdentityExactGroupHistoryRunnerError("schema_exact")
    normalized: dict[str, object] = {}
    for field in contract.arrow_schema:
        value = row[field.name]
        if value is None:
            if not field.nullable:
                raise IdentityExactGroupHistoryRunnerError(
                    "schema_exact: non-nullable field is null"
                )
            normalized[field.name] = None
        elif pa.types.is_string(field.type):
            if not isinstance(value, str):
                raise IdentityExactGroupHistoryRunnerError("schema_exact: string")
            normalized[field.name] = value
        elif pa.types.is_boolean(field.type):
            if type(value) is not bool:
                raise IdentityExactGroupHistoryRunnerError("schema_exact: boolean")
            normalized[field.name] = value
        elif pa.types.is_int64(field.type):
            if type(value) is not int:
                raise IdentityExactGroupHistoryRunnerError("schema_exact: int64")
            normalized[field.name] = value
        elif pa.types.is_float64(field.type):
            if type(value) not in {int, float} or not math.isfinite(float(value)):
                raise IdentityExactGroupHistoryRunnerError("schema_exact: float64")
            normalized[field.name] = float(value)
        elif pa.types.is_date32(field.type):
            if type(value) is not date:
                raise IdentityExactGroupHistoryRunnerError("schema_exact: date32")
            normalized[field.name] = value.isoformat()
        elif pa.types.is_timestamp(field.type):
            normalized[field.name] = _utc(value, field.name).isoformat()
        else:
            raise IdentityExactGroupHistoryRunnerError("schema_exact: scalar type")
    return normalized


def _replay_provider_rows(
    root: Path,
    refs: Sequence[ExactGroupHistorySourceArtifactRef],
    attestations: Sequence[ProviderRowAttestation],
    *,
    calendar: XNYSCalendarArtifact,
    monitor: _ResourceMonitor,
) -> None:
    """Replay only retained physical locators; this is not a second full scan."""

    refs_by_path = {item.path: item for item in refs}
    grouped: dict[tuple[str, int], list[ProviderRowAttestation]] = defaultdict(list)
    for item in attestations:
        grouped[(item.silver_artifact_path, item.parquet_row_group)].append(item)
    replayed: set[str] = set()
    for (relative, row_group), items in sorted(grouped.items()):
        ref = refs_by_path.get(relative)
        if ref is None:
            raise IdentityExactGroupHistoryRunnerError("orphan_or_duplicate_attestation_rows")
        path = safe_relative_path(root, relative)
        parquet = pq.ParquetFile(path)
        wanted = {item.row_index_in_row_group for item in items}
        physical: dict[int, Mapping[str, object]] = {}
        base = 0
        for batch in parquet.iter_batches(
            row_groups=[row_group], batch_size=PHYSICAL_REPLAY_BATCH_SIZE
        ):
            for index in sorted(wanted):
                if base <= index < base + batch.num_rows:
                    physical[index] = batch.slice(index - base, 1).to_pylist()[0]
            base += batch.num_rows
            if len(physical) == len(wanted):
                break
        for item in items:
            try:
                row = physical[item.row_index_in_row_group]
            except KeyError as exc:
                raise IdentityExactGroupHistoryRunnerError(
                    "row_attestation_replay_invalid_rows"
                ) from exc
            rebuilt = _attest_source_row(
                ref,
                row_group=row_group,
                row_index_in_group=item.row_index_in_row_group,
                row=row,
                calendar=calendar,
            )
            if rebuilt != item or rebuilt.row_attestation_id != item.row_attestation_id:
                raise IdentityExactGroupHistoryRunnerError("row_attestation_replay_invalid_rows")
            if rebuilt.row_attestation_id in replayed:
                raise IdentityExactGroupHistoryRunnerError("orphan_or_duplicate_attestation_rows")
            replayed.add(rebuilt.row_attestation_id)
        monitor.check()
    if replayed != {item.row_attestation_id for item in attestations}:
        raise IdentityExactGroupHistoryRunnerError("orphan_or_duplicate_attestation_rows")


def _stage_commit_and_complete(
    root: Path,
    controls: _LoadedControls,
    build: ExactGroupHistoryBuild,
    *,
    refs: tuple[ExactGroupHistorySourceArtifactRef, ...],
    intent: S7ExactGroupHistoryExecutionIntent,
    staging: Path,
    completion_path: Path,
    monitor: _ResourceMonitor,
    started: float,
    scanned: _ScanResult,
    created_at_utc: datetime,
    calendar: XNYSCalendarArtifact,
) -> S7ExactGroupHistoryCompletion:
    candidate_stage = staging / "candidate"
    candidate_stage.mkdir(parents=True, exist_ok=False)
    slots_path = candidate_stage / SLOTS_FILENAME
    slots_path.parent.mkdir(parents=True, exist_ok=False)
    table = pa.Table.from_pylist(
        [dict(item) for item in build.slots],
        schema=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema,
    )
    _write_parquet_exclusive(slots_path, table)
    payloads = {
        SEQUENCES_FILENAME: _canonical_bytes(build.sequences),
        QA_FILENAME: _canonical_bytes(build.qa),
        EXAMPLES_FILENAME: _canonical_bytes(build.examples),
    }
    for relative, content in payloads.items():
        target = candidate_stage / relative
        target.parent.mkdir(parents=True, exist_ok=False)
        _write_exclusive(target, content)
    for evidence in build.evidence_manifests:
        target = candidate_stage / evidence.candidate_relative_path
        target.parent.mkdir(parents=True, exist_ok=False)
        _write_exclusive(target, evidence.content)
    artifacts = _output_refs(candidate_stage, build)
    plan = controls.plan
    approval = controls.approval
    candidate = S7ExactGroupHistoryCandidate(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_intent_id=intent.intent_id,
        execution_intent_path=intent.relative_path,
        execution_intent_sha256=intent.sha256,
        source_binding_id=plan.source_binding_id,
        source_binding_sha256=plan.source_binding_sha256,
        source_artifact_set_digest=plan.source_artifact_set_digest,
        normalized_source_artifact_set_digest=(plan.normalized_source_artifact_set_digest),
        review_scope_set_id=plan.scope_set_id,
        artifacts=artifacts,
        evidence_manifest_ids=tuple(item.manifest_id for item in build.evidence_manifests),
        created_at_utc=created_at_utc,
    )
    _write_exclusive(candidate_stage / MANIFEST_FILENAME, candidate.content)
    _fsync_tree_bottom_up(candidate_stage)
    _validate_candidate_directory(
        candidate_stage,
        candidate,
        expected_build=build,
        calendar=calendar,
        root=root,
        refs=refs,
    )
    monitor.check()
    output_bytes = _tree_bytes(candidate_stage)
    if output_bytes > monitor.output_cap:
        raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: output bytes")
    # Detect every source mutation immediately before candidate publication.
    _verify_all_source_hashes(root, refs, monitor=monitor)
    monitor.check()
    final_candidate = safe_relative_path(root, candidate.relative_directory)
    final_candidate.parent.mkdir(parents=True, exist_ok=True)
    completion_path.parent.mkdir(parents=True, exist_ok=True)
    if final_candidate.exists() or final_candidate.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("exclusive candidate target already exists")
    if completion_path.exists() or completion_path.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("completion target already exists")
    final_peak_rss_bytes = max(monitor.peak_rss_bytes, _peak_rss_bytes())
    final_wall_clock_seconds = float(time.monotonic() - started)
    if (
        final_peak_rss_bytes > monitor.rss_cap
        or final_wall_clock_seconds > monitor.wall_cap
        or output_bytes > monitor.output_cap
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "resource_cap_exceeded: final pre-commit measurements"
        )
    completion = S7ExactGroupHistoryCompletion(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        execution_intent_id=intent.intent_id,
        execution_intent_path=intent.relative_path,
        execution_intent_sha256=intent.sha256,
        candidate_id=candidate.candidate_id,
        candidate_path=f"{candidate.relative_directory}/{MANIFEST_FILENAME}",
        candidate_sha256=candidate.sha256,
        output_artifacts=artifacts,
        completed_at_utc=datetime.now(UTC),
        source_artifact_count=scanned.scanned_artifacts,
        source_row_count=scanned.scanned_rows,
        source_bytes=scanned.scanned_bytes,
        output_slot_row_count=len(build.slots),
        peak_rss_bytes=final_peak_rss_bytes,
        wall_clock_seconds=final_wall_clock_seconds,
        output_bytes=output_bytes,
    )
    completion_stage = staging / "completion.json"
    _write_exclusive(completion_stage, completion.content)
    # A cap violation after staging is recoverable; it must never publish a candidate.
    monitor.check()
    if (
        max(monitor.peak_rss_bytes, _peak_rss_bytes()) > monitor.rss_cap
        or time.monotonic() - started > monitor.wall_cap
        or output_bytes > monitor.output_cap
    ):
        raise IdentityExactGroupHistoryRunnerError("resource_cap_exceeded: final commit boundary")
    _rename_directory_noreplace(candidate_stage, final_candidate)
    _publish_file_noreplace(completion_stage, completion_path)
    return _read_completed_without_source(root, completion_path, controls)


def _output_refs(
    candidate: Path, build: ExactGroupHistoryBuild
) -> tuple[ExactGroupHistoryOutputRef, ...]:
    roles: dict[str, tuple[str, str, int | None]] = {
        "review_slots": (SLOTS_FILENAME, "application/vnd.apache.parquet", len(build.slots)),
        "group_sequences": (SEQUENCES_FILENAME, "application/json", None),
        "qa": (QA_FILENAME, "application/json", None),
        "bounded_examples": (EXAMPLES_FILENAME, "application/json", None),
    }
    for evidence in build.evidence_manifests:
        roles[f"group_evidence:{evidence.ticker}"] = (
            evidence.candidate_relative_path,
            "application/json",
            None,
        )
    result = []
    for role, (relative, media_type, row_count) in roles.items():
        path = candidate / relative
        result.append(
            ExactGroupHistoryOutputRef(
                role=role,
                path=relative,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
                media_type=media_type,
                row_count=row_count,
            )
        )
    return tuple(sorted(result))


def _validate_candidate_directory(
    directory: Path,
    candidate: S7ExactGroupHistoryCandidate,
    *,
    expected_build: ExactGroupHistoryBuild,
    calendar: XNYSCalendarArtifact,
    root: Path,
    refs: Sequence[ExactGroupHistorySourceArtifactRef],
) -> None:
    del calendar, root, refs  # physical replay already completed before staging
    actual = _relative_file_set(directory)
    expected = {MANIFEST_FILENAME, *(item.path for item in candidate.artifacts)}
    if actual != expected:
        raise IdentityExactGroupHistoryRunnerError("output_artifact_readback_invalid: file set")
    manifest_path = directory / MANIFEST_FILENAME
    if manifest_path.read_bytes() != candidate.content:
        raise IdentityExactGroupHistoryRunnerError(
            "output_artifact_readback_invalid: candidate manifest"
        )
    for ref in candidate.artifacts:
        path = directory / ref.path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != ref.bytes
            or sha256_file(path) != ref.sha256
        ):
            raise IdentityExactGroupHistoryRunnerError(
                "output_artifact_readback_invalid: artifact hash"
            )
    readback = pq.read_table(directory / SLOTS_FILENAME)
    if not readback.schema.equals(
        IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema
    ) or readback.to_pylist() != [dict(item) for item in expected_build.slots]:
        raise IdentityExactGroupHistoryRunnerError("output_artifact_readback_invalid: Parquet")
    json_expected = {
        SEQUENCES_FILENAME: expected_build.sequences,
        QA_FILENAME: expected_build.qa,
        EXAMPLES_FILENAME: expected_build.examples,
    }
    for relative, expected_value in json_expected.items():
        if (directory / relative).read_bytes() != _canonical_bytes(expected_value):
            raise IdentityExactGroupHistoryRunnerError("output_artifact_readback_invalid: JSON")
    for evidence in expected_build.evidence_manifests:
        content = (directory / evidence.candidate_relative_path).read_bytes()
        try:
            parsed = ExactGroupHistoryEvidenceManifestV2.from_dict(json.loads(content))
        except (json.JSONDecodeError, ProviderEvidenceError) as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "output_artifact_readback_invalid: evidence"
            ) from exc
        if parsed != evidence or content != evidence.content:
            raise IdentityExactGroupHistoryRunnerError(
                "output_artifact_readback_invalid: evidence differs"
            )


def _verify_all_source_hashes(
    root: Path,
    refs: Sequence[ExactGroupHistorySourceArtifactRef],
    *,
    monitor: _ResourceMonitor,
) -> None:
    for ref in refs:
        path = safe_relative_path(root, ref.path)
        if _sha256_regular_nofollow(path, expected_size=ref.bytes) != ref.sha256:
            raise IdentityExactGroupHistoryRunnerError("observed_source_row_mutation_rows")
        monitor.check()


def _read_completed_without_source(
    root: Path,
    completion_path: Path,
    controls: _LoadedControls,
) -> S7ExactGroupHistoryCompletion:
    """Validate a completed retry without stat/open/hash of any S4 source."""

    if not completion_path.is_file() or completion_path.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("completion path is unsafe")
    content = completion_path.read_bytes()
    try:
        completion = S7ExactGroupHistoryCompletion.from_dict(json.loads(content))
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryRunnerError("completion is malformed") from exc
    plan, approval = controls.plan, controls.approval
    if (
        completion.content != content
        or completion.plan_id != plan.plan_id
        or completion.plan_sha256 != plan.sha256
        or completion.approval_id != approval.approval_id
        or completion.approval_sha256 != approval.sha256
        or completion.request_event_id != approval.request_event_id
        or completion.request_event_sha256 != approval.request_event_sha256
        or completion.source_artifact_count
        != plan.execution_resource_caps.physical_source_artifact_count
        or completion.source_row_count != plan.execution_resource_caps.physical_source_row_count
        or completion.source_bytes != plan.execution_resource_caps.physical_source_bytes
        or completion.peak_rss_bytes > plan.execution_resource_caps.rss_bytes_hard_cap
        or completion.wall_clock_seconds > plan.execution_resource_caps.wall_clock_seconds_hard_cap
        or completion.output_bytes > plan.execution_resource_caps.output_bytes_hard_cap
        or completion.output_artifacts is None
    ):
        raise IdentityExactGroupHistoryRunnerError("completion controls differ")
    intent_path = safe_relative_path(root, completion.execution_intent_path)
    if not intent_path.is_file() or intent_path.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("execution intent is missing")
    intent_content = intent_path.read_bytes()
    try:
        intent = S7ExactGroupHistoryExecutionIntent.from_dict(json.loads(intent_content))
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryRunnerError("execution intent is malformed") from exc
    if (
        intent.content != intent_content
        or intent.intent_id != completion.execution_intent_id
        or intent.sha256 != completion.execution_intent_sha256
        or intent.plan_id != plan.plan_id
        or intent.plan_sha256 != plan.sha256
        or intent.approval_id != approval.approval_id
        or intent.approval_sha256 != approval.sha256
        or intent.request_event_id != approval.request_event_id
        or intent.request_event_sha256 != approval.request_event_sha256
        or intent.execution_data_root != str(root)
        or intent.source_binding_id != plan.source_binding_id
        or intent.source_binding_sha256 != plan.source_binding_sha256
        or intent.source_artifact_set_digest != plan.source_artifact_set_digest
        or intent.normalized_source_artifact_set_digest
        != plan.normalized_source_artifact_set_digest
        or intent.fixed_scope_digest != EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST
        or intent.contract_id != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID
        or intent.contract_schema_digest != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST
        or intent.qa_semantics_digest
        != IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST
        or intent.observed_run_semantics_digest != EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST
    ):
        raise IdentityExactGroupHistoryRunnerError("execution intent differs")
    candidate_path = safe_relative_path(root, completion.candidate_path)
    if (
        not candidate_path.is_file()
        or candidate_path.is_symlink()
        or sha256_file(candidate_path) != completion.candidate_sha256
    ):
        raise IdentityExactGroupHistoryRunnerError("candidate manifest changed")
    candidate_content = candidate_path.read_bytes()
    try:
        candidate = S7ExactGroupHistoryCandidate.from_dict(json.loads(candidate_content))
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise IdentityExactGroupHistoryRunnerError("candidate is malformed") from exc
    if (
        candidate.content != candidate_content
        or candidate.candidate_id != completion.candidate_id
        or candidate.sha256 != completion.candidate_sha256
        or candidate.artifacts != completion.output_artifacts
        or candidate.plan_id != plan.plan_id
        or candidate.plan_sha256 != plan.sha256
        or candidate.approval_id != approval.approval_id
        or candidate.approval_sha256 != approval.sha256
        or candidate.request_event_id != approval.request_event_id
        or candidate.request_event_sha256 != approval.request_event_sha256
        or candidate.execution_intent_id != intent.intent_id
        or candidate.execution_intent_path != intent.relative_path
        or candidate.execution_intent_sha256 != intent.sha256
        or candidate.source_binding_id != plan.source_binding_id
        or candidate.source_binding_sha256 != plan.source_binding_sha256
        or candidate.source_artifact_set_digest != plan.source_artifact_set_digest
        or candidate.normalized_source_artifact_set_digest
        != plan.normalized_source_artifact_set_digest
        or candidate.review_scope_set_id != plan.scope_set_id
        or candidate.created_at_utc < intent.created_at_utc
        or completion.completed_at_utc < candidate.created_at_utc
        or completion.candidate_path != f"{candidate.relative_directory}/{MANIFEST_FILENAME}"
    ):
        raise IdentityExactGroupHistoryRunnerError("candidate bindings differ")
    directory = candidate_path.parent
    actual = _relative_file_set(directory)
    expected = {MANIFEST_FILENAME, *(item.path for item in candidate.artifacts)}
    if actual != expected:
        raise IdentityExactGroupHistoryRunnerError("candidate file set changed")
    if _tree_bytes(directory) != completion.output_bytes:
        raise IdentityExactGroupHistoryRunnerError("candidate output byte count changed")
    for ref in candidate.artifacts:
        path = directory / ref.path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != ref.bytes
            or sha256_file(path) != ref.sha256
        ):
            raise IdentityExactGroupHistoryRunnerError("candidate output changed")
    slot_ref = next(item for item in candidate.artifacts if item.role == "review_slots")
    if slot_ref.row_count != completion.output_slot_row_count:
        raise IdentityExactGroupHistoryRunnerError("completion slot count differs")
    _validate_completed_candidate_semantics(
        root,
        directory,
        candidate,
        completion,
        plan=plan,
        approval=approval,
        intent=intent,
    )
    return completion


def _validate_completed_candidate_semantics(
    root: Path,
    directory: Path,
    candidate: S7ExactGroupHistoryCandidate,
    completion: S7ExactGroupHistoryCompletion,
    *,
    plan: object,
    approval: S7ExactGroupHistoryExecutionApproval,
    intent: S7ExactGroupHistoryExecutionIntent,
) -> None:
    """Re-parse stable outputs without touching any frozen S4 source artifact."""

    try:
        slots = pq.read_table(directory / SLOTS_FILENAME)
    except (OSError, pa.ArrowException) as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "completed candidate Parquet cannot be read"
        ) from exc
    if (
        not slots.schema.equals(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema)
        or slots.num_rows != completion.output_slot_row_count
    ):
        raise IdentityExactGroupHistoryRunnerError("completed candidate Parquet differs")
    slot_rows = slots.to_pylist()
    expected_groups = set(EXACT_GROUP_HISTORY_FIXED_GROUPS)
    if any(
        (row["ticker"], row["exact_group_observed_composite_figi"]) not in expected_groups
        or row["review_scope_set_id"] != candidate.review_scope_set_id
        or row["registry_evaluation_state"] != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
        or any(
            row[field] is not False
            for field in (
                "adjudication_eligible",
                "canonical_candidate_eligible",
                "transition_candidate_eligible",
                "exact_override_interval_eligible",
                "full_run_eligible",
                "publication_eligible",
            )
        )
        for row in slot_rows
    ):
        raise IdentityExactGroupHistoryRunnerError("completed candidate capability/scope differs")
    parsed_json: dict[str, object] = {}
    for relative in (SEQUENCES_FILENAME, QA_FILENAME, EXAMPLES_FILENAME):
        content = (directory / relative).read_bytes()
        try:
            value = json.loads(content)
        except json.JSONDecodeError as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "completed candidate JSON is malformed"
            ) from exc
        if content != _canonical_bytes(value):
            raise IdentityExactGroupHistoryRunnerError("completed candidate JSON is not canonical")
        parsed_json[relative] = value
    sequences = parsed_json[SEQUENCES_FILENAME]
    qa = parsed_json[QA_FILENAME]
    examples = parsed_json[EXAMPLES_FILENAME]
    _validate_completed_qa_and_examples(qa, examples, slot_count=len(slot_rows))
    sequence_groups = _validate_completed_sequences(sequences, slot_rows)
    evidence_by_ticker: dict[str, ExactGroupHistoryEvidenceManifestV2] = {}
    for ticker in EXACT_GROUP_HISTORY_FIXED_TICKERS:
        ref = next(item for item in candidate.artifacts if item.role == f"group_evidence:{ticker}")
        content = (directory / ref.path).read_bytes()
        try:
            evidence = ExactGroupHistoryEvidenceManifestV2.from_dict(json.loads(content))
        except (json.JSONDecodeError, ProviderEvidenceError) as exc:
            raise IdentityExactGroupHistoryRunnerError(
                "completed candidate evidence is malformed"
            ) from exc
        if (
            content != evidence.content
            or evidence.ticker != ticker
            or evidence.plan_id != plan.plan_id
            or evidence.plan_sha256 != plan.sha256
            or evidence.approval_id != approval.approval_id
            or evidence.approval_sha256 != approval.sha256
            or evidence.execution_intent_id != intent.intent_id
            or evidence.execution_intent_sha256 != intent.sha256
            or evidence.review_scope_set_id != candidate.review_scope_set_id
            or evidence.source_artifact_set_digest != plan.source_artifact_set_digest
            or evidence.normalized_source_artifact_set_digest
            != plan.normalized_source_artifact_set_digest
            or evidence.created_at_utc != candidate.created_at_utc
        ):
            raise IdentityExactGroupHistoryRunnerError("completed candidate evidence differs")
        evidence_by_ticker[ticker] = evidence
    if {item.manifest_id for item in evidence_by_ticker.values()} != set(
        candidate.evidence_manifest_ids
    ):
        raise IdentityExactGroupHistoryRunnerError("completed evidence ID set differs")
    _validate_completed_slot_evidence(slot_rows, evidence_by_ticker, sequence_groups)
    _rebuild_completed_candidate_from_evidence(
        root,
        slot_rows,
        parsed_json,
        tuple(evidence_by_ticker.values()),
        plan=plan,
        approval=approval,
        intent=intent,
        created_at_utc=candidate.created_at_utc,
    )


def _validate_completed_qa_and_examples(qa: object, examples: object, *, slot_count: int) -> None:
    if not isinstance(qa, Mapping) or not isinstance(examples, Mapping):
        raise IdentityExactGroupHistoryRunnerError("completed QA/examples are malformed")
    checks = qa.get("checks")
    if (
        set(qa)
        != {
            "artifact_type",
            "capabilities",
            "checks",
            "critical_failure_count",
            "registry_evaluation_state",
            "schema_version",
            "warning_count",
        }
        or qa.get("artifact_type") != "s7_exact_group_history_qa"
        or qa.get("capabilities") != _false_capabilities()
        or qa.get("registry_evaluation_state") != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
        or qa.get("schema_version") != 1
        or not isinstance(checks, list)
    ):
        raise IdentityExactGroupHistoryRunnerError("completed QA controls differ")
    expected_rules = {
        rule.check_id: rule for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules
    }
    if len(checks) != len(expected_rules):
        raise IdentityExactGroupHistoryRunnerError("completed QA check set differs")
    observed: dict[str, int] = {}
    warning_count = 0
    for item in checks:
        if not isinstance(item, Mapping) or set(item) != {
            "check_id",
            "denominator",
            "numerator",
            "severity",
            "status",
        }:
            raise IdentityExactGroupHistoryRunnerError("completed QA check is malformed")
        check_id = item.get("check_id")
        rule = expected_rules.get(check_id)
        numerator = item.get("numerator")
        if (
            rule is None
            or type(numerator) is not int
            or numerator < 0
            or item.get("denominator") != slot_count
            or item.get("severity") != rule.severity.value
        ):
            raise IdentityExactGroupHistoryRunnerError("completed QA check differs")
        expected_status = (
            QAStatus.FAILED.value
            if rule.severity.value == "critical" and numerator
            else QAStatus.WARNING.value
            if rule.severity.value == "high" and numerator
            else QAStatus.PASSED.value
        )
        if item.get("status") != expected_status or check_id in observed:
            raise IdentityExactGroupHistoryRunnerError("completed QA status differs")
        if rule.severity.value == "critical" and numerator:
            raise IdentityExactGroupHistoryRunnerError("completed candidate critical QA differs")
        if expected_status == QAStatus.WARNING.value:
            warning_count += 1
        observed[str(check_id)] = numerator
    if (
        set(observed) != set(expected_rules)
        or qa.get("critical_failure_count") != 0
        or qa.get("warning_count") != warning_count
    ):
        raise IdentityExactGroupHistoryRunnerError("completed QA summary differs")
    high_counts = {
        check_id: observed[check_id]
        for check_id, rule in expected_rules.items()
        if rule.severity.value == "high" and observed[check_id]
    }
    example_values = examples.get("examples")
    if (
        set(examples)
        != {
            "artifact_type",
            "capabilities",
            "examples",
            "reason_counts",
            "registry_evaluation_state",
            "schema_version",
        }
        or examples.get("artifact_type") != "s7_exact_group_history_bounded_examples"
        or examples.get("capabilities") != _false_capabilities()
        or examples.get("registry_evaluation_state")
        != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
        or examples.get("schema_version") != 1
        or examples.get("reason_counts") != high_counts
        or not isinstance(example_values, Mapping)
        or set(example_values) != set(high_counts)
        or any(
            not isinstance(values, list) or not values or len(values) > 20
            for values in example_values.values()
        )
    ):
        raise IdentityExactGroupHistoryRunnerError("completed bounded examples differ")


def _validate_completed_sequences(
    sequences: object, slots: Sequence[Mapping[str, object]]
) -> dict[str, Mapping[str, object]]:
    if (
        not isinstance(sequences, Mapping)
        or set(sequences)
        != {
            "artifact_type",
            "capabilities",
            "created_at_utc",
            "exact_effective_interval_proven",
            "groups",
            "observed_interval_state",
            "registry_evaluation_state",
            "schema_version",
        }
        or sequences.get("artifact_type") != "s7_exact_group_history_group_sequences"
        or sequences.get("capabilities") != _false_capabilities()
        or sequences.get("exact_effective_interval_proven") is not False
        or sequences.get("observed_interval_state") != EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE
        or sequences.get("registry_evaluation_state")
        != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
        or sequences.get("schema_version") != 1
        or not isinstance(sequences.get("groups"), list)
    ):
        raise IdentityExactGroupHistoryRunnerError("completed sequence controls differ")
    groups: dict[str, Mapping[str, object]] = {}
    expected = dict(EXACT_GROUP_HISTORY_FIXED_GROUPS)
    for group in sequences["groups"]:
        if not isinstance(group, Mapping):
            raise IdentityExactGroupHistoryRunnerError("completed sequence group malformed")
        ticker = group.get("ticker")
        composite = group.get("exact_group_observed_composite_figi")
        group_id = group.get("review_group_id")
        if (
            not isinstance(ticker, str)
            or expected.get(ticker) != composite
            or group_id != _review_group_id(ticker, str(composite))
            or not isinstance(group_id, str)
            or group_id in groups
            or group.get("observed_interval_state") != EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE
            or group.get("exact_effective_interval_proven") is not False
            or not isinstance(group.get("runs"), list)
        ):
            raise IdentityExactGroupHistoryRunnerError("completed sequence group differs")
        groups[group_id] = group
    if len(groups) != len(EXACT_GROUP_HISTORY_FIXED_GROUPS):
        raise IdentityExactGroupHistoryRunnerError("completed sequence group set differs")
    if {str(item["review_group_id"]) for item in slots} != set(groups):
        raise IdentityExactGroupHistoryRunnerError("completed slot/sequence groups differ")
    return groups


def _validate_completed_slot_evidence(
    slots: Sequence[Mapping[str, object]],
    evidence_by_ticker: Mapping[str, ExactGroupHistoryEvidenceManifestV2],
    sequence_groups: Mapping[str, Mapping[str, object]],
) -> None:
    slots_by_group: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for slot in slots:
        slots_by_group[str(slot["review_group_id"])].append(slot)
    for group_id, group_slots in slots_by_group.items():
        group = sequence_groups[group_id]
        ticker = str(group["ticker"])
        evidence = evidence_by_ticker[ticker]
        evidence_sessions: dict[date, list[str]] = defaultdict(list)
        for item in evidence.asset_attestations:
            if _attestation_matches_exact_group(
                item, ticker, evidence.exact_group_observed_composite_figi
            ):
                session = date.fromisoformat(str(item.full_row_snapshot["session_date"]))
                evidence_sessions[session].append(item.row_attestation_id)
        runs = group["runs"]
        if not isinstance(runs, list):
            raise IdentityExactGroupHistoryRunnerError("completed sequence runs malformed")
        flattened: list[tuple[int, int, str, str, str, int]] = []
        observed_dates: list[str] = []
        for run_ordinal, run in enumerate(runs):
            if not isinstance(run, Mapping) or run.get("run_ordinal") != run_ordinal:
                raise IdentityExactGroupHistoryRunnerError("completed sequence run malformed")
            observed = run.get("observed_sessions")
            if not isinstance(observed, list) or not observed:
                raise IdentityExactGroupHistoryRunnerError("completed sequence run is empty")
            run_dates = tuple(date.fromisoformat(str(value)) for value in observed)
            run_id = _observed_run_id(
                ticker, evidence.exact_group_observed_composite_figi, run_dates
            )
            if (
                run.get("run_id") != run_id
                or run.get("start_observed_session") != observed[0]
                or run.get("end_observed_session") != observed[-1]
                or run.get("exact_effective_interval_proven") is not False
            ):
                raise IdentityExactGroupHistoryRunnerError("completed sequence run differs")
            observed_dates.extend(str(value) for value in observed)
            flattened.extend(
                (run_ordinal, ordinal, run_id, str(observed[0]), str(observed[-1]), len(observed))
                for ordinal, _ in enumerate(observed)
            )
        ordered_slots = sorted(group_slots, key=lambda item: item["session_date"])
        if [item["session_date"].isoformat() for item in ordered_slots] != observed_dates:
            raise IdentityExactGroupHistoryRunnerError("completed slot/run sessions differ")
        for index, (slot, run_meta) in enumerate(zip(ordered_slots, flattened, strict=True)):
            run_ordinal, in_run, run_id, run_start, run_end, run_count = run_meta
            session = slot["session_date"]
            exact_ids = sorted(evidence_sessions.get(session, []))
            try:
                slot_ids = sorted(
                    json.loads(str(slot["exact_asset_observation_attestation_ids_json"]))
                )
            except (json.JSONDecodeError, TypeError) as exc:
                raise IdentityExactGroupHistoryRunnerError(
                    "completed slot attestation IDs malformed"
                ) from exc
            previous = None if index == 0 else ordered_slots[index - 1]["session_date"]
            expected_adjacent = None if previous is None else run_ordinal == flattened[index - 1][0]
            if (
                slot["exact_group_evidence_manifest_id"] != evidence.manifest_id
                or slot["exact_group_evidence_manifest_path"] != evidence.candidate_relative_path
                or slot["exact_group_evidence_manifest_sha256"] != evidence.sha256
                or slot_ids != exact_ids
                or slot["exact_asset_observation_match_count"] != len(exact_ids)
                or slot["previous_observed_session"] != previous
                or slot["previous_observed_session_is_adjacent_xnys"] is not expected_adjacent
                or slot["exact_observed_run_id"] != run_id
                or slot["exact_observed_run_ordinal"] != run_ordinal
                or slot["observed_session_ordinal_in_run"] != in_run
                or slot["exact_observed_run_start_session"].isoformat() != run_start
                or slot["exact_observed_run_end_session"].isoformat() != run_end
                or slot["exact_observed_run_session_count"] != run_count
                or slot["group_first_observed_session"] != ordered_slots[0]["session_date"]
                or slot["group_last_observed_session"] != ordered_slots[-1]["session_date"]
                or slot["group_observed_session_count"] != len(ordered_slots)
                or slot["group_exact_observed_run_count"] != len(runs)
            ):
                raise IdentityExactGroupHistoryRunnerError(
                    "completed slot/evidence/run binding differs"
                )


def _rebuild_completed_candidate_from_evidence(
    root: Path,
    slot_rows: Sequence[Mapping[str, object]],
    parsed_json: Mapping[str, object],
    evidence_manifests: Sequence[ExactGroupHistoryEvidenceManifestV2],
    *,
    plan: object,
    approval: S7ExactGroupHistoryExecutionApproval,
    intent: S7ExactGroupHistoryExecutionIntent,
    created_at_utc: datetime,
) -> None:
    """Reproduce every derived output from immutable v2 row evidence and calendar."""

    try:
        from ame_stocks_api.silver.identity_exact_group_history_plan import (
            CALENDAR_ARTIFACT_ID,
            CALENDAR_ARTIFACT_SHA256,
        )

        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=CALENDAR_ARTIFACT_ID,
            expected_sha256=CALENDAR_ARTIFACT_SHA256,
        )
    except (ImportError, XNYSCalendarArtifactError) as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "completed candidate calendar cannot be loaded"
        ) from exc
    grouped: dict[date, dict[str, list[ProviderRowAttestation]]] = defaultdict(
        lambda: {ASSET_TABLE: [], UNIVERSE_TABLE: []}
    )
    for evidence in evidence_manifests:
        for item in evidence.asset_attestations:
            session = date.fromisoformat(str(item.full_row_snapshot["session_date"]))
            grouped[session][ASSET_TABLE].append(item)
        for item in evidence.universe_attestations:
            session = date.fromisoformat(str(item.full_row_snapshot["session_date"]))
            grouped[session][UNIVERSE_TABLE].append(item)
    engine = ExactGroupHistoryEngine(
        selected_row_cap=plan.execution_resource_caps.selected_row_hard_cap
    )
    for session in sorted(grouped):
        engine.consume_session(
            session,
            asset_attestations=grouped[session][ASSET_TABLE],
            universe_attestations=grouped[session][UNIVERSE_TABLE],
        )
    rebuilt = engine.finish(
        plan=plan,
        approval=approval,
        intent=intent,
        calendar=calendar,
        created_at_utc=created_at_utc,
        runner_verified_critical={},
    )
    if (
        [dict(item) for item in rebuilt.slots] != [dict(item) for item in slot_rows]
        or rebuilt.sequences != parsed_json[SEQUENCES_FILENAME]
        or rebuilt.qa != parsed_json[QA_FILENAME]
        or rebuilt.examples != parsed_json[EXAMPLES_FILENAME]
        or tuple(sorted(rebuilt.evidence_manifests, key=lambda item: item.ticker))
        != tuple(sorted(evidence_manifests, key=lambda item: item.ticker))
    ):
        raise IdentityExactGroupHistoryRunnerError(
            "completed candidate does not reproduce from immutable evidence"
        )


def _execution_paths(plan_id: str, approval_id: str) -> dict[str, str]:
    return {
        "completion": exact_group_history_completion_path(plan_id, approval_id),
        "intent": exact_group_history_execution_intent_path(plan_id, approval_id),
        "lock": (
            "manifests/silver/identity/exact-group-history-execution-locks/"
            f"plan_id={plan_id}/approval_id={approval_id}.lock"
        ),
        "staging": (
            f"tmp/silver/identity/exact-group-history/plan_id={plan_id}/approval_id={approval_id}"
        ),
    }


def _validated_root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise IdentityExactGroupHistoryRunnerError("data_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("data_root is unsafe")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityExactGroupHistoryRunnerError("data_root must exist")
    return root


class _exclusive_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            opened = os.fstat(self.fd)
            visible = os.lstat(self.path)
            if not stat.S_ISREG(visible.st_mode) or (
                opened.st_dev,
                opened.st_ino,
            ) != (visible.st_dev, visible.st_ino):
                raise IdentityExactGroupHistoryRunnerError("lock path changed")
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise

    def __exit__(self, *_: object) -> None:
        assert self.fd is not None
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None


def _write_exclusive(path: Path, content: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb", closefd=True) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _write_parquet_exclusive(path: Path, table: pa.Table) -> None:
    """Write one Parquet artifact without following or replacing a foreign target."""

    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise IdentityExactGroupHistoryRunnerError("Parquet no-clobber creation failed") from exc
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            pq.write_table(table, handle, compression="zstd", use_dictionary=True)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        # A partial exclusive file is retained as fail-closed audit evidence.
        raise
    _fsync_regular_file(path)


def _publish_file_noreplace(source: Path, target: Path) -> None:
    try:
        os.link(source, target, follow_symlinks=False)
    except (FileExistsError, OSError) as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "completion no-clobber publication failed"
        ) from exc
    if (
        not target.is_file()
        or target.is_symlink()
        or target.stat(follow_symlinks=False).st_ino != source.stat(follow_symlinks=False).st_ino
        or target.read_bytes() != source.read_bytes()
    ):
        raise IdentityExactGroupHistoryRunnerError("completion publication postcondition failed")
    _fsync_directory(target.parent)


def _rename_directory_noreplace(source: Path, target: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("candidate staging is unsafe")
    before = source.stat(follow_symlinks=False)
    try:
        _exclusive_rename_primitive(source, target)
    except OSError as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "candidate no-clobber publication failed"
        ) from exc
    if source.exists() or not target.is_dir() or target.is_symlink():
        raise IdentityExactGroupHistoryRunnerError("candidate publication postcondition failed")
    after = target.stat(follow_symlinks=False)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise IdentityExactGroupHistoryRunnerError("candidate publication identity changed")
    _fsync_directory(source.parent)
    _fsync_directory(target.parent)


def _exclusive_rename_primitive(source: Path, target: Path) -> None:
    import ctypes
    import errno

    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise OSError(errno.ENOSYS, "renameat2 unavailable")
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
            raise OSError(errno.ENOSYS, "renamex_np unavailable")
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(os.fsencode(source), os.fsencode(target), 0x00000004)
    else:
        raise OSError(errno.ENOSYS, "exclusive rename unavailable")
    if result != 0:
        number = ctypes.get_errno() or errno.EIO
        raise OSError(number, os.strerror(number), str(target))


def _fsync_regular_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise IdentityExactGroupHistoryRunnerError("fsync target is not regular")
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree_bottom_up(root: Path) -> None:
    directories: list[Path] = []
    for current, children, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories.append(current_path)
        for name in children:
            child = current_path / name
            if child.is_symlink() or not child.is_dir():
                raise IdentityExactGroupHistoryRunnerError("candidate tree is unsafe")
        for name in files:
            item = current_path / name
            if item.is_symlink() or not item.is_file():
                raise IdentityExactGroupHistoryRunnerError("candidate tree is unsafe")
            _fsync_regular_file(item)
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        _fsync_directory(directory)


def _relative_file_set(root: Path) -> set[str]:
    result: set[str] = set()
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        if any((current_path / name).is_symlink() for name in directories):
            raise IdentityExactGroupHistoryRunnerError("candidate contains a symlink")
        for name in files:
            path = current_path / name
            if path.is_symlink() or not path.is_file():
                raise IdentityExactGroupHistoryRunnerError("candidate file is unsafe")
            result.add(path.relative_to(root).as_posix())
    return result


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum((path / relative).stat().st_size for relative in _relative_file_set(path))


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value) if sys.platform == "darwin" else int(value) * 1024


def _plan_cap(plan: object, field: str, _fallback: int) -> int:
    caps = getattr(plan, "execution_resource_caps", None)
    value = getattr(caps, field, None)
    if type(value) is not int or value <= 0:
        raise IdentityExactGroupHistoryRunnerError(f"resource cap is absent or invalid: {field}")
    if field == "rss_bytes_hard_cap" and value < RSS_HARD_FLOOR_BYTES:
        raise IdentityExactGroupHistoryRunnerError("RSS cap is below the 2 GiB floor")
    return value


def _sha256_regular_nofollow(path: Path, *, expected_size: int) -> str:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise IdentityExactGroupHistoryRunnerError(
            "source artifact cannot open without following links"
        ) from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != expected_size:
            raise IdentityExactGroupHistoryRunnerError(
                "source artifact descriptor metadata differs"
            )
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(fd)
        visible = os.lstat(path)
        identity = (before.st_dev, before.st_ino, before.st_size)
        if (
            (after.st_dev, after.st_ino, after.st_size) != identity
            or (visible.st_dev, visible.st_ino, visible.st_size) != identity
            or not stat.S_ISREG(visible.st_mode)
        ):
            raise IdentityExactGroupHistoryRunnerError("source artifact changed while hashing")
        return digest.hexdigest()
    finally:
        os.close(fd)


def _verify_git_and_runtime_pins(plan: object, manifest_plan: object) -> None:
    repository = Path(__file__).resolve().parents[3]
    if (
        Path(_git(repository, "rev-parse", "--show-toplevel")).resolve() != repository
        or _git(repository, "rev-parse", "HEAD") != plan.git_commit
        or _git(repository, "rev-parse", "HEAD^{tree}") != plan.git_tree
        or _git(repository, "status", "--porcelain", "--untracked-files=all")
    ):
        raise IdentityExactGroupHistoryRunnerError("Git checkout differs from Plan")
    if (
        manifest_plan.git_commit != plan.git_commit
        or manifest_plan.git_tree != plan.git_tree
        or manifest_plan.runtime_file_set_digest != plan.runtime_file_set_digest
        or manifest_plan.verification_file_set_digest != plan.verification_file_set_digest
        or stable_digest([item.to_dict() for item in manifest_plan.runtime_files])
        != plan.runtime_file_set_digest
        or stable_digest([item.to_dict() for item in manifest_plan.verification_files])
        != plan.verification_file_set_digest
    ):
        raise IdentityExactGroupHistoryRunnerError("Git file-set projection differs")
    pins = (*manifest_plan.runtime_files, *manifest_plan.verification_files)
    if not pins or len({item.path for item in pins}) != len(pins):
        raise IdentityExactGroupHistoryRunnerError("Git file pins are incomplete")
    for pin in pins:
        path = repository / pin.path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != pin.bytes
            or sha256_file(path) != pin.sha256
            or _git(repository, "rev-parse", f"HEAD:{pin.path}") != pin.git_blob
        ):
            raise IdentityExactGroupHistoryRunnerError(f"Git file pin differs: {pin.path}")


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise IdentityExactGroupHistoryRunnerError("Git verification failed")
    return result.stdout.strip()


__all__ = [
    "ExactGroupHistoryEngine",
    "ExactGroupHistoryEvidenceManifestV2",
    "ExactGroupHistoryOutputRef",
    "ExactGroupHistorySourceArtifactRef",
    "IdentityExactGroupHistoryRunnerError",
    "S7ExactGroupHistoryCandidate",
    "S7ExactGroupHistoryCompletion",
    "S7ExactGroupHistoryExecutionIntent",
    "exact_group_history_completion_path",
    "exact_group_history_execution_intent_path",
    "run_exact_s7_exact_group_history_review",
]
