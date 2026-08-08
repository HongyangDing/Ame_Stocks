"""Independent, non-publishing S7.5 I7 Full-reconciliation runtime.

I7 is a consumer of two already completed authorities.  The oracle side is an
independent legacy streaming Full whose exact control chain, complete candidate
tree, every Parquet file, and QA are replayed by the official streaming
verifier.  The incremental side is the resolved research snapshot selected by
the frozen exact Gate-C reader in ``incremental_i6_pointer_runtime``; any API,
type, locator, or lineage mismatch at that seam fails closed before I7 output.

The runtime never launches either producer, discovers ``latest``, mutates a
pointer, publishes a release, or declares S7.5 complete.  A successful run
freezes only trigger, per-partition comparison, QA, alert, and completion
artifacts in ``awaiting_review`` state.
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
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date
from enum import StrEnum
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Final, Protocol, Self

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest
from ame_stocks_api.silver import identity_materialization_streaming as legacy_full
from ame_stocks_api.silver.identity_registry_workflow import load_registry_release_set
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin, ViewKind
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
)
from ame_stocks_api.silver.incremental_i3_delta_io import (
    load_production_delta_input_binding,
)
from ame_stocks_api.silver.incremental_i3_production import (
    verify_i3_production_deep_attestation,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionOutputStorage,
    I3ProductionRunKind,
    LoadedI3ProductionStaging,
    load_i3_production_completion_exact,
    load_i3_production_parent_shallow_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
)
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    I7_TABLE_ORDER,
    FullReconciliationPartitionEvidence,
    FullReconciliationReceipt,
    FullReconciliationSpec,
    FullReconciliationTableEvidence,
    FullReconciliationTableScope,
    ReconciliationCadence,
    ResourceGatePolicy,
    validate_full_reconciliation,
)
from ame_stocks_api.silver.incremental_i5_shadow_runtime import (
    I5ShadowRuntimeError,
    _canonical_v1_rows,
    _legacy_alias_reverse,
    _resolved_incremental_small_rows,
)

I7_RUNTIME_RULE_VERSION: Final = "s7_5_i7_independent_full_reconciliation_runtime_v1"
I7_RUN_SPEC_RULE_VERSION: Final = "s7_5_i7_exact_run_spec_v1"
I7_TRIGGER_RULE_VERSION: Final = "s7_5_i7_reconciliation_trigger_v1"
I7_ALERT_RULE_VERSION: Final = "s7_5_i7_reconciliation_alert_v1"
I7_COMPLETION_RULE_VERSION: Final = "s7_5_i7_awaiting_review_completion_v1"
I7_RESOURCE_OBSERVATION_RULE_VERSION: Final = "s7_5_i7_process_io_phase_resource_observation_v3"
I7_CANONICAL_PROJECTION_RULE_VERSION: Final = "s7_5_i7_all_rows_canonical_research_projection_v1"
I7_CHECKPOINT_REBASE_RULE_VERSION: Final = (
    "s7_5_i7_checkpoint_base_compaction_logical_equivalence_v1"
)
I7_TRIGGER_POLICY_RULE_VERSION: Final = "s7_5_i7_monthly_quarterly_trigger_policy_v1"
I7_STATE: Final = "awaiting_review"
I7_PRODUCTION_AUTHORITY: Final = "gate_c_top_and_official_legacy_full_exact"
I7_FIXTURE_AUTHORITY: Final = "fixture_non_authoritative"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_AUTHORITY_PARTS = frozenset(
    {"latest", "tmp", ".tmp", "fixture", "fixtures", "test", "tests"}
)
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_PRODUCTION_RUN_SPEC_LOCATOR = re.compile(
    r"^manifests/silver/incremental/i7/full-reconciliation-run-specs/"
    r"run_spec_id=([0-9a-f]{64})/manifest\.json$"
)
_PRODUCTION_COMPLETION_LOCATOR = re.compile(
    r"^manifests/silver/incremental/i7/completions/"
    r"run_spec_id=([0-9a-f]{64})/manifest\.json$"
)
_LEGACY_FULL_COMPLETION_LOCATOR = re.compile(
    r"^manifests/silver/identity/s7-streaming-full-execution-completions/"
    r"plan_id=([0-9a-f]{64})/approval_id=([0-9a-f]{64})/manifest\.json$"
)


class I7ReconciliationRuntimeError(RuntimeError):
    """Raised before incomplete or unequal evidence can become a receipt."""


class I7IncrementalTopSeamError(I7ReconciliationRuntimeError):
    """P0: no frozen production I6 Gate-C top snapshot reader exists yet."""


class I7LegacyFullSeamError(I7ReconciliationRuntimeError):
    """P0: the independent Full cannot be replayed by the official verifier."""


class ReconciliationTriggerKind(StrEnum):
    SCHEDULED = "scheduled"
    INCIDENT = "incident"


class CrossProducerProjectionKind(StrEnum):
    """Closed native input domains; both emit the same canonical v1 rows."""

    INCREMENTAL_NATIVE_V2 = "incremental_native_v2_resolved"
    LEGACY_NATIVE_V1 = "legacy_streaming_native_v1"
    FIXTURE_CANONICAL_V1 = "fixture_canonical_v1_non_authoritative"


@dataclass(frozen=True, slots=True)
class CrossProducerProjectionContract:
    """Named, table-specific bridge between native-v2 and legacy-v1.

    Native schema IDs remain lineage.  ``canonical_schema_digest`` is the only
    comparison domain; a native digest can never be silently substituted for
    the canonical projection digest.
    """

    table_name: str
    incremental_native_schema_digest: str
    legacy_native_schema_digest: str
    canonical_schema_digest: str
    incremental_projection_rule: str
    legacy_projection_rule: str

    def __post_init__(self) -> None:
        if self.table_name not in I7_TABLE_ORDER:
            raise I7ReconciliationRuntimeError("cross-producer projection table is invalid")
        for value, label in (
            (self.incremental_native_schema_digest, "incremental native schema"),
            (self.legacy_native_schema_digest, "legacy native schema"),
            (self.canonical_schema_digest, "canonical projection schema"),
        ):
            _digest(value, label)
        _text(self.incremental_projection_rule, "incremental projection rule")
        _text(self.legacy_projection_rule, "legacy projection rule")
        if self.legacy_native_schema_digest != self.canonical_schema_digest:
            raise I7ReconciliationRuntimeError("legacy native schema is not canonical v1")
        if self.incremental_native_schema_digest == self.canonical_schema_digest:
            raise I7ReconciliationRuntimeError(
                "native-v2 and canonical-v1 schema identities were conflated"
            )

    @property
    def contract_digest(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {
            "canonical_schema_digest": self.canonical_schema_digest,
            "incremental_native_schema_digest": self.incremental_native_schema_digest,
            "incremental_projection_rule": self.incremental_projection_rule,
            "legacy_native_schema_digest": self.legacy_native_schema_digest,
            "legacy_projection_rule": self.legacy_projection_rule,
            "rule_version": "s7_5_i7_named_cross_producer_projection_contract_v1",
            "table_name": self.table_name,
        }


def cross_producer_projection_contract(table_name: str) -> CrossProducerProjectionContract:
    if table_name not in I7_TABLE_ORDER:
        raise I7ReconciliationRuntimeError("cross-producer projection table is invalid")
    return CrossProducerProjectionContract(
        table_name=table_name,
        incremental_native_schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
        legacy_native_schema_digest=S7_DERIVED_CONTRACTS[table_name].schema_digest,
        canonical_schema_digest=S7_DERIVED_CONTRACTS[table_name].schema_digest,
        incremental_projection_rule=(
            "resolve_exact_checkpoint_terminal_versions_then_project_v2_to_s7_v1"
            if table_name != "universe_daily"
            else "project_exact_native_v2_session_partition_to_s7_v1"
        ),
        legacy_projection_rule="validate_exact_s7_v1_rows_identity_projection",
    )


def cross_producer_identity_policy_digest(
    registry_release_ids: Mapping[str, str],
) -> str:
    """Project producer-native policy envelopes to release-ID semantics only."""

    if not isinstance(registry_release_ids, Mapping) or not registry_release_ids:
        raise I7ReconciliationRuntimeError("cross-producer policy registry set is empty")
    normalized = {
        _text(name, "identity registry name"): _digest(release_id, "registry release ID")
        for name, release_id in registry_release_ids.items()
    }
    if len(normalized) != len(registry_release_ids):
        raise I7ReconciliationRuntimeError("cross-producer policy registry names repeat")
    return stable_digest(
        {
            "registry_release_ids": dict(sorted(normalized.items())),
            "rule_version": "s7_5_i7_cross_producer_identity_policy_projection_v1",
        }
    )


def cross_producer_transform_semantics_digest(
    incremental_transform_semantics_digest: str,
    legacy_native_transform_lineage_id: str,
) -> str:
    """Keep both native transform authorities in the comparison contract."""

    return stable_digest(
        {
            "incremental_native_transform_semantics_digest": _digest(
                incremental_transform_semantics_digest,
                "incremental native transform semantics",
            ),
            "legacy_native_transform_lineage_id": _digest(
                legacy_native_transform_lineage_id,
                "legacy native transform lineage",
            ),
            "projection_rule_version": I7_CANONICAL_PROJECTION_RULE_VERSION,
            "rule_version": "s7_5_i7_cross_producer_transform_semantics_bridge_v1",
        }
    )


def project_cross_producer_rows(
    table_name: str,
    projection_kind: CrossProducerProjectionKind,
    rows: Sequence[Mapping[str, object]],
    *,
    alias_reverse: Mapping[str, tuple[str, str]] | None = None,
) -> tuple[dict[str, object], ...]:
    """Apply the named native-to-canonical bridge and validate its row domain."""

    contract = cross_producer_projection_contract(table_name)
    if not isinstance(projection_kind, CrossProducerProjectionKind):
        raise I7ReconciliationRuntimeError("cross-producer projection kind is invalid")
    if projection_kind is CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2:
        if alias_reverse is None:
            raise I7ReconciliationRuntimeError("native-v2 projection lacks alias authority")
        try:
            projected = _canonical_v1_rows(table_name, rows, alias_reverse)
        except I5ShadowRuntimeError as exc:
            raise I7ReconciliationRuntimeError("I7 native-v2 projection failed") from exc
    else:
        if alias_reverse is not None:
            raise I7ReconciliationRuntimeError("legacy-v1 projection received native aliases")
        projected = tuple(_canonical_row(table_name, row) for row in rows)
    canonical = tuple(_canonical_row(table_name, row) for row in projected)
    primary_key = tuple(S7_DERIVED_CONTRACTS[table_name].primary_key)
    ordered = tuple(
        sorted(
            canonical,
            key=lambda row: tuple(_sort_value(row[field]) for field in primary_key),
        )
    )
    keys = [tuple(_json_value(row[field]) for field in primary_key) for row in ordered]
    if len(keys) != len(set(keys)):
        raise I7ReconciliationRuntimeError("I7 canonical projection has duplicate primary keys")
    if contract.canonical_schema_digest != S7_DERIVED_CONTRACTS[table_name].schema_digest:
        raise I7ReconciliationRuntimeError("I7 canonical projection schema changed")
    return ordered


@dataclass(frozen=True, slots=True)
class ReconciliationPartition:
    """One exact canonical projection partition returned by a trusted loader."""

    table_name: str
    partition_key: str
    artifact: ArtifactPin
    row_count: int
    schema_digest: str
    physical_digest: str
    projection_kind: CrossProducerProjectionKind
    projection_contract_digest: str
    native_release_id: str
    lineage_artifacts: tuple[ArtifactPin, ...]

    def __post_init__(self) -> None:
        if self.table_name not in I7_TABLE_ORDER:
            raise I7ReconciliationRuntimeError("snapshot partition table is invalid")
        _partition_key(self.table_name, self.partition_key)
        _artifact(self.artifact, "snapshot partition")
        if type(self.row_count) is not int or self.row_count <= 0:
            raise I7ReconciliationRuntimeError("snapshot partition row count must be positive")
        _digest(self.schema_digest, "snapshot partition schema digest")
        _digest(self.physical_digest, "snapshot partition physical digest")
        if not isinstance(self.projection_kind, CrossProducerProjectionKind):
            raise I7ReconciliationRuntimeError("snapshot projection kind is invalid")
        _digest(self.projection_contract_digest, "snapshot projection contract digest")
        _digest(self.native_release_id, "snapshot native release ID")
        if not isinstance(self.lineage_artifacts, tuple) or not self.lineage_artifacts:
            raise I7ReconciliationRuntimeError("snapshot native lineage is empty")
        for pin in self.lineage_artifacts:
            _artifact(pin, "snapshot native lineage artifact")
        if len({pin.path for pin in self.lineage_artifacts}) != len(self.lineage_artifacts):
            raise I7ReconciliationRuntimeError("snapshot native lineage repeats an artifact")
        contract = cross_producer_projection_contract(self.table_name)
        if self.projection_contract_digest != contract.contract_digest:
            raise I7ReconciliationRuntimeError("snapshot projection contract differs")
        expected_native = (
            contract.incremental_native_schema_digest
            if self.projection_kind is CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2
            else contract.legacy_native_schema_digest
        )
        if self.schema_digest != expected_native:
            raise I7ReconciliationRuntimeError("snapshot native schema/projection kind differ")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "partition_key": self.partition_key,
            "physical_digest": self.physical_digest,
            "projection_contract_digest": self.projection_contract_digest,
            "projection_kind": self.projection_kind.value,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "table_name": self.table_name,
            "native_release_id": self.native_release_id,
            "lineage_artifacts": [item.to_dict() for item in self.lineage_artifacts],
        }

    @classmethod
    def from_dict(cls, value: object) -> ReconciliationPartition:
        item = _mapping(value, "snapshot partition")
        _keys(
            item,
            {
                "artifact",
                "partition_key",
                "physical_digest",
                "projection_contract_digest",
                "projection_kind",
                "row_count",
                "schema_digest",
                "table_name",
                "native_release_id",
                "lineage_artifacts",
            },
            "snapshot partition",
        )
        return cls(
            table_name=_text(item["table_name"], "snapshot table"),
            partition_key=_text(item["partition_key"], "snapshot partition key"),
            artifact=_artifact_from_dict(item["artifact"], "snapshot partition artifact"),
            row_count=_positive_int(item["row_count"], "snapshot partition row count"),
            schema_digest=_digest(item["schema_digest"], "snapshot schema digest"),
            physical_digest=_digest(item["physical_digest"], "snapshot physical digest"),
            projection_kind=CrossProducerProjectionKind(
                _text(item["projection_kind"], "snapshot projection kind")
            ),
            projection_contract_digest=_digest(
                item["projection_contract_digest"], "snapshot projection contract"
            ),
            native_release_id=_digest(item["native_release_id"], "snapshot native release"),
            lineage_artifacts=tuple(
                _artifact_from_dict(value, "snapshot lineage artifact")
                for value in _sequence(item["lineage_artifacts"], "snapshot lineage")
            ),
        )


@dataclass(frozen=True, slots=True)
class VerifiedIncrementalTopSnapshot:
    """Output of the future frozen I6 exact Gate-C top reader.

    ``resolved_partitions`` is the top snapshot.  ``checkpoint_before_partitions``
    is the exact logical state before checkpoint/base compaction.  I7 reads and
    compares both; a proof document or digest claim cannot replace that replay.
    """

    authority: str
    release_id: str
    native_v2_release_id: str
    checkpoint_id: str
    checkpoint_base_native_v2_release_id: str
    checkpoint_base_checkpoint_id: str
    resolved_state_digest: str
    resolved_content_digest: str
    physical_index_digest: str
    row_semantic_attestation_digest: str
    cutoff_session: date
    producer_available_session: date
    producer_replay_declared_bytes: int
    bronze_source_binding_digest: str
    s4_source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    native_identity_policy_lineage_id: str
    identity_policy_bundle_id: str
    calendar_digest: str
    top_pointer_artifact: ArtifactPin
    gate_c_approval_artifact: ArtifactPin
    producer_verification_artifact: ArtifactPin
    release_completion_artifact: ArtifactPin
    checkpoint_base_compaction_completion_artifact: ArtifactPin
    checkpoint_base_compaction_proof_artifact: ArtifactPin
    resolved_partitions: tuple[ReconciliationPartition, ...]
    checkpoint_before_partitions: tuple[ReconciliationPartition, ...]

    def __post_init__(self) -> None:
        if self.authority not in {I7_PRODUCTION_AUTHORITY, I7_FIXTURE_AUTHORITY}:
            raise I7ReconciliationRuntimeError("incremental snapshot authority is invalid")
        for value, label in (
            (self.release_id, "incremental release ID"),
            (self.native_v2_release_id, "incremental native-v2 release ID"),
            (self.checkpoint_id, "incremental checkpoint ID"),
            (
                self.checkpoint_base_native_v2_release_id,
                "checkpoint-base native-v2 release ID",
            ),
            (self.checkpoint_base_checkpoint_id, "checkpoint-base checkpoint ID"),
            (self.resolved_state_digest, "incremental resolved-state digest"),
            (self.resolved_content_digest, "incremental resolved-content digest"),
            (self.physical_index_digest, "incremental physical-index digest"),
            (
                self.row_semantic_attestation_digest,
                "incremental row-semantic attestation",
            ),
            (self.bronze_source_binding_digest, "incremental Bronze binding"),
            (self.s4_source_binding_digest, "incremental S4 binding"),
            (self.schema_bundle_digest, "incremental schema bundle"),
            (self.transform_semantics_digest, "incremental transform semantics"),
            (
                self.native_identity_policy_lineage_id,
                "incremental native identity-policy lineage",
            ),
            (self.identity_policy_bundle_id, "incremental identity policy"),
            (self.calendar_digest, "incremental calendar"),
        ):
            _digest(value, label)
        if not isinstance(self.cutoff_session, date):
            raise I7ReconciliationRuntimeError("incremental cutoff is invalid")
        if (
            not isinstance(self.producer_available_session, date)
            or self.producer_available_session < self.cutoff_session
        ):
            raise I7ReconciliationRuntimeError("incremental producer availability is invalid")
        _positive_int(
            self.producer_replay_declared_bytes,
            "incremental producer replay declared bytes",
        )
        for value, label in (
            (self.top_pointer_artifact, "incremental top pointer"),
            (self.gate_c_approval_artifact, "incremental Gate C approval"),
            (self.producer_verification_artifact, "incremental producer verification"),
            (self.release_completion_artifact, "incremental release completion"),
            (
                self.checkpoint_base_compaction_completion_artifact,
                "checkpoint/base compaction completion",
            ),
            (
                self.checkpoint_base_compaction_proof_artifact,
                "checkpoint/base compaction proof",
            ),
        ):
            _artifact(value, label)
        _partition_set(self.resolved_partitions, "resolved incremental snapshot")
        _partition_set(self.checkpoint_before_partitions, "checkpoint-before snapshot")
        if _partition_scope(self.resolved_partitions) != _partition_scope(
            self.checkpoint_before_partitions
        ):
            raise I7ReconciliationRuntimeError(
                "checkpoint-before scope differs from resolved snapshot"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "authority": self.authority,
            "bronze_source_binding_digest": self.bronze_source_binding_digest,
            "calendar_digest": self.calendar_digest,
            "checkpoint_base_compaction_proof_artifact": (
                self.checkpoint_base_compaction_proof_artifact.to_dict()
            ),
            "checkpoint_base_compaction_completion_artifact": (
                self.checkpoint_base_compaction_completion_artifact.to_dict()
            ),
            "checkpoint_before_partitions": [
                item.to_dict() for item in self.checkpoint_before_partitions
            ],
            "cutoff_session": self.cutoff_session.isoformat(),
            "producer_available_session": self.producer_available_session.isoformat(),
            "producer_replay_declared_bytes": self.producer_replay_declared_bytes,
            "gate_c_approval_artifact": self.gate_c_approval_artifact.to_dict(),
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "native_identity_policy_lineage_id": self.native_identity_policy_lineage_id,
            "native_v2_release_id": self.native_v2_release_id,
            "checkpoint_id": self.checkpoint_id,
            "checkpoint_base_native_v2_release_id": (self.checkpoint_base_native_v2_release_id),
            "checkpoint_base_checkpoint_id": self.checkpoint_base_checkpoint_id,
            "resolved_state_digest": self.resolved_state_digest,
            "resolved_content_digest": self.resolved_content_digest,
            "physical_index_digest": self.physical_index_digest,
            "row_semantic_attestation_digest": self.row_semantic_attestation_digest,
            "producer_verification_artifact": self.producer_verification_artifact.to_dict(),
            "release_completion_artifact": self.release_completion_artifact.to_dict(),
            "release_id": self.release_id,
            "resolved_partitions": [item.to_dict() for item in self.resolved_partitions],
            "s4_source_binding_digest": self.s4_source_binding_digest,
            "schema_bundle_digest": self.schema_bundle_digest,
            "top_pointer_artifact": self.top_pointer_artifact.to_dict(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> VerifiedIncrementalTopSnapshot:
        item = _mapping(value, "incremental snapshot")
        _keys(
            item,
            {
                "authority",
                "bronze_source_binding_digest",
                "calendar_digest",
                "checkpoint_base_compaction_proof_artifact",
                "checkpoint_base_compaction_completion_artifact",
                "checkpoint_before_partitions",
                "cutoff_session",
                "producer_available_session",
                "producer_replay_declared_bytes",
                "gate_c_approval_artifact",
                "identity_policy_bundle_id",
                "native_identity_policy_lineage_id",
                "native_v2_release_id",
                "checkpoint_id",
                "checkpoint_base_native_v2_release_id",
                "checkpoint_base_checkpoint_id",
                "resolved_state_digest",
                "resolved_content_digest",
                "physical_index_digest",
                "row_semantic_attestation_digest",
                "producer_verification_artifact",
                "release_completion_artifact",
                "release_id",
                "resolved_partitions",
                "s4_source_binding_digest",
                "schema_bundle_digest",
                "top_pointer_artifact",
                "transform_semantics_digest",
            },
            "incremental snapshot",
        )
        return cls(
            authority=_text(item["authority"], "incremental authority"),
            release_id=_digest(item["release_id"], "incremental release ID"),
            native_v2_release_id=_digest(
                item["native_v2_release_id"], "incremental native-v2 release ID"
            ),
            checkpoint_id=_digest(item["checkpoint_id"], "incremental checkpoint ID"),
            checkpoint_base_native_v2_release_id=_digest(
                item["checkpoint_base_native_v2_release_id"],
                "checkpoint-base native-v2 release ID",
            ),
            checkpoint_base_checkpoint_id=_digest(
                item["checkpoint_base_checkpoint_id"],
                "checkpoint-base checkpoint ID",
            ),
            resolved_state_digest=_digest(
                item["resolved_state_digest"], "incremental resolved-state digest"
            ),
            resolved_content_digest=_digest(
                item["resolved_content_digest"], "incremental resolved-content digest"
            ),
            physical_index_digest=_digest(
                item["physical_index_digest"], "incremental physical-index digest"
            ),
            row_semantic_attestation_digest=_digest(
                item["row_semantic_attestation_digest"],
                "incremental row-semantic attestation",
            ),
            cutoff_session=date.fromisoformat(_text(item["cutoff_session"], "cutoff")),
            producer_available_session=date.fromisoformat(
                _text(item["producer_available_session"], "producer availability")
            ),
            producer_replay_declared_bytes=_positive_int(
                item["producer_replay_declared_bytes"],
                "incremental producer replay declared bytes",
            ),
            bronze_source_binding_digest=_digest(
                item["bronze_source_binding_digest"], "Bronze binding"
            ),
            s4_source_binding_digest=_digest(item["s4_source_binding_digest"], "S4 binding"),
            schema_bundle_digest=_digest(item["schema_bundle_digest"], "schema bundle"),
            transform_semantics_digest=_digest(
                item["transform_semantics_digest"], "transform semantics"
            ),
            identity_policy_bundle_id=_digest(item["identity_policy_bundle_id"], "identity policy"),
            native_identity_policy_lineage_id=_digest(
                item["native_identity_policy_lineage_id"],
                "incremental native identity-policy lineage",
            ),
            calendar_digest=_digest(item["calendar_digest"], "calendar"),
            top_pointer_artifact=_artifact_from_dict(item["top_pointer_artifact"], "top pointer"),
            gate_c_approval_artifact=_artifact_from_dict(
                item["gate_c_approval_artifact"], "Gate C approval"
            ),
            producer_verification_artifact=_artifact_from_dict(
                item["producer_verification_artifact"], "producer verification"
            ),
            release_completion_artifact=_artifact_from_dict(
                item["release_completion_artifact"], "release completion"
            ),
            checkpoint_base_compaction_proof_artifact=_artifact_from_dict(
                item["checkpoint_base_compaction_proof_artifact"], "compaction proof"
            ),
            checkpoint_base_compaction_completion_artifact=_artifact_from_dict(
                item["checkpoint_base_compaction_completion_artifact"],
                "compaction completion",
            ),
            resolved_partitions=tuple(
                ReconciliationPartition.from_dict(value)
                for value in _sequence(item["resolved_partitions"], "resolved partitions")
            ),
            checkpoint_before_partitions=tuple(
                ReconciliationPartition.from_dict(value)
                for value in _sequence(
                    item["checkpoint_before_partitions"], "checkpoint-before partitions"
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class VerifiedLegacyFullSnapshot:
    """Result of the official all-tree legacy streaming verifier."""

    authority: str
    release_id: str
    cutoff_session: date
    producer_available_session: date
    producer_replay_declared_bytes: int
    bronze_source_binding_digest: str
    s4_source_binding_digest: str
    schema_bundle_digest: str
    native_transform_lineage_id: str
    native_identity_policy_lineage_id: str
    identity_policy_bundle_id: str
    calendar_digest: str
    completion_artifact: ArtifactPin
    producer_verification_artifact: ArtifactPin
    partitions: tuple[ReconciliationPartition, ...]

    def __post_init__(self) -> None:
        if self.authority not in {I7_PRODUCTION_AUTHORITY, I7_FIXTURE_AUTHORITY}:
            raise I7ReconciliationRuntimeError("legacy Full authority is invalid")
        for value, label in (
            (self.release_id, "legacy Full release ID"),
            (self.bronze_source_binding_digest, "legacy Full Bronze binding"),
            (self.s4_source_binding_digest, "legacy Full S4 binding"),
            (self.schema_bundle_digest, "legacy Full schema bundle"),
            (self.native_transform_lineage_id, "legacy Full native transform lineage"),
            (
                self.native_identity_policy_lineage_id,
                "legacy Full native identity-policy lineage",
            ),
            (self.identity_policy_bundle_id, "legacy Full identity policy"),
            (self.calendar_digest, "legacy Full calendar"),
        ):
            _digest(value, label)
        if not isinstance(self.cutoff_session, date):
            raise I7ReconciliationRuntimeError("legacy Full cutoff is invalid")
        if (
            not isinstance(self.producer_available_session, date)
            or self.producer_available_session < self.cutoff_session
        ):
            raise I7ReconciliationRuntimeError("legacy Full producer availability is invalid")
        _positive_int(
            self.producer_replay_declared_bytes,
            "legacy Full producer replay declared bytes",
        )
        _artifact(self.completion_artifact, "legacy Full completion")
        _artifact(self.producer_verification_artifact, "legacy Full verification")
        _partition_set(self.partitions, "legacy Full snapshot")

    def to_dict(self) -> dict[str, object]:
        return {
            "authority": self.authority,
            "bronze_source_binding_digest": self.bronze_source_binding_digest,
            "calendar_digest": self.calendar_digest,
            "completion_artifact": self.completion_artifact.to_dict(),
            "cutoff_session": self.cutoff_session.isoformat(),
            "producer_available_session": self.producer_available_session.isoformat(),
            "producer_replay_declared_bytes": self.producer_replay_declared_bytes,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "native_identity_policy_lineage_id": self.native_identity_policy_lineage_id,
            "native_transform_lineage_id": self.native_transform_lineage_id,
            "partitions": [item.to_dict() for item in self.partitions],
            "producer_verification_artifact": self.producer_verification_artifact.to_dict(),
            "release_id": self.release_id,
            "s4_source_binding_digest": self.s4_source_binding_digest,
            "schema_bundle_digest": self.schema_bundle_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> VerifiedLegacyFullSnapshot:
        item = _mapping(value, "legacy Full snapshot")
        _keys(
            item,
            {
                "authority",
                "bronze_source_binding_digest",
                "calendar_digest",
                "completion_artifact",
                "cutoff_session",
                "producer_available_session",
                "producer_replay_declared_bytes",
                "identity_policy_bundle_id",
                "native_identity_policy_lineage_id",
                "native_transform_lineage_id",
                "partitions",
                "producer_verification_artifact",
                "release_id",
                "s4_source_binding_digest",
                "schema_bundle_digest",
            },
            "legacy Full snapshot",
        )
        return cls(
            authority=_text(item["authority"], "legacy authority"),
            release_id=_digest(item["release_id"], "legacy release ID"),
            cutoff_session=date.fromisoformat(_text(item["cutoff_session"], "legacy cutoff")),
            producer_available_session=date.fromisoformat(
                _text(item["producer_available_session"], "legacy producer availability")
            ),
            producer_replay_declared_bytes=_positive_int(
                item["producer_replay_declared_bytes"],
                "legacy producer replay declared bytes",
            ),
            bronze_source_binding_digest=_digest(
                item["bronze_source_binding_digest"], "legacy Bronze binding"
            ),
            s4_source_binding_digest=_digest(item["s4_source_binding_digest"], "legacy S4 binding"),
            schema_bundle_digest=_digest(item["schema_bundle_digest"], "legacy schema bundle"),
            native_transform_lineage_id=_digest(
                item["native_transform_lineage_id"], "legacy native transform lineage"
            ),
            identity_policy_bundle_id=_digest(
                item["identity_policy_bundle_id"], "legacy identity policy"
            ),
            native_identity_policy_lineage_id=_digest(
                item["native_identity_policy_lineage_id"],
                "legacy native identity-policy lineage",
            ),
            calendar_digest=_digest(item["calendar_digest"], "legacy calendar"),
            completion_artifact=_artifact_from_dict(
                item["completion_artifact"], "legacy completion"
            ),
            producer_verification_artifact=_artifact_from_dict(
                item["producer_verification_artifact"], "legacy verification"
            ),
            partitions=tuple(
                ReconciliationPartition.from_dict(value)
                for value in _sequence(item["partitions"], "legacy partitions")
            ),
        )


class IncrementalTopLoader(Protocol):
    def __call__(
        self,
        data_root: Path,
        *,
        top_pointer_artifact: ArtifactPin,
        gate_c_approval_artifact: ArtifactPin,
        checkpoint_compaction_completion_artifact: ArtifactPin,
        cutoff_session: date,
    ) -> VerifiedIncrementalTopSnapshot: ...


class LegacyFullLoader(Protocol):
    def __call__(
        self,
        data_root: Path,
        *,
        completion_artifact: ArtifactPin,
        cutoff_session: date,
    ) -> VerifiedLegacyFullSnapshot: ...


@dataclass(frozen=True, slots=True)
class I7ReconciliationConfig:
    incremental_top_pointer_artifact: ArtifactPin
    gate_c_approval_artifact: ArtifactPin
    checkpoint_compaction_completion_artifact: ArtifactPin
    independent_full_completion_artifact: ArtifactPin
    cutoff_session: date
    receipt_available_session: date
    cadence: ReconciliationCadence
    trigger_kind: ReconciliationTriggerKind
    trigger_reason: str
    resource_policy: ResourceGatePolicy

    def __post_init__(self) -> None:
        for value, label in (
            (self.incremental_top_pointer_artifact, "research top pointer"),
            (self.gate_c_approval_artifact, "Gate C approval"),
            (
                self.checkpoint_compaction_completion_artifact,
                "checkpoint compaction completion",
            ),
            (self.independent_full_completion_artifact, "independent Full completion"),
        ):
            _artifact(value, label)
            _production_authority_path(value.path, label)
        if not isinstance(self.cutoff_session, date):
            raise I7ReconciliationRuntimeError("I7 cutoff is invalid")
        if (
            not isinstance(self.receipt_available_session, date)
            or self.receipt_available_session < self.cutoff_session
        ):
            raise I7ReconciliationRuntimeError("I7 receipt availability predates cutoff")
        if not isinstance(self.cadence, ReconciliationCadence):
            raise I7ReconciliationRuntimeError("I7 cadence is invalid")
        if not isinstance(self.trigger_kind, ReconciliationTriggerKind):
            raise I7ReconciliationRuntimeError("I7 trigger kind is invalid")
        _text(self.trigger_reason, "I7 trigger reason")
        if not isinstance(self.resource_policy, ResourceGatePolicy):
            raise I7ReconciliationRuntimeError("I7 resource policy is invalid")


@dataclass(frozen=True, slots=True)
class I7RuntimeResourceObservation:
    """Frozen stage observation plus declared preflight envelope."""

    declared_read_bytes: int
    estimated_write_bytes: int
    metered_read_bytes: int
    process_read_bytes: int
    process_write_bytes: int
    observed_write_bytes: int
    peak_rss_bytes: int
    minimum_free_disk_bytes: int
    wall_clock_seconds: int
    phase_minimum_free_disk_bytes: tuple[tuple[str, int], ...]
    phase_peak_rss_bytes: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.declared_read_bytes, "declared read bytes"),
            (self.estimated_write_bytes, "estimated write bytes"),
            (self.metered_read_bytes, "metered read bytes"),
            (self.process_read_bytes, "process read bytes"),
            (self.process_write_bytes, "process write bytes"),
            (self.observed_write_bytes, "observed write bytes"),
            (self.peak_rss_bytes, "peak RSS bytes"),
            (self.minimum_free_disk_bytes, "minimum free disk bytes"),
            (self.wall_clock_seconds, "wall-clock seconds"),
        ):
            _nonnegative_int(value, label)
        if (
            not isinstance(self.phase_minimum_free_disk_bytes, tuple)
            or not self.phase_minimum_free_disk_bytes
            or not isinstance(self.phase_peak_rss_bytes, tuple)
            or not self.phase_peak_rss_bytes
        ):
            raise I7ReconciliationRuntimeError("resource phase observations are empty")
        disk_names = []
        for phase, value in self.phase_minimum_free_disk_bytes:
            disk_names.append(_text(phase, "resource phase"))
            _nonnegative_int(value, "resource phase disk bytes")
        rss_names = []
        for phase, value in self.phase_peak_rss_bytes:
            rss_names.append(_text(phase, "resource phase"))
            _nonnegative_int(value, "resource phase RSS bytes")
        if (
            len(disk_names) != len(set(disk_names))
            or len(rss_names) != len(set(rss_names))
            or disk_names != rss_names
        ):
            raise I7ReconciliationRuntimeError("resource phase observation repeats")
        if self.minimum_free_disk_bytes != min(
            value for _, value in self.phase_minimum_free_disk_bytes
        ):
            raise I7ReconciliationRuntimeError("minimum disk observation does not reproduce")
        if self.peak_rss_bytes != max(value for _, value in self.phase_peak_rss_bytes):
            raise I7ReconciliationRuntimeError("peak RSS observation does not reproduce")

    def validate(self, policy: ResourceGatePolicy) -> None:
        if self.declared_read_bytes > policy.max_read_bytes:
            raise I7ReconciliationRuntimeError("stored declared-read resource gate failed")
        if self.estimated_write_bytes > policy.max_write_bytes:
            raise I7ReconciliationRuntimeError("stored estimated-write resource gate failed")
        if max(self.metered_read_bytes, self.process_read_bytes) > policy.max_read_bytes:
            raise I7ReconciliationRuntimeError("stored runtime read resource gate failed")
        if max(self.observed_write_bytes, self.process_write_bytes) > policy.max_write_bytes:
            raise I7ReconciliationRuntimeError("stored runtime write resource gate failed")
        if self.peak_rss_bytes > policy.max_peak_rss_bytes:
            raise I7ReconciliationRuntimeError("stored peak-RSS resource gate failed")
        if self.minimum_free_disk_bytes < policy.min_free_disk_bytes:
            raise I7ReconciliationRuntimeError("stored disk-floor resource gate failed")
        if self.wall_clock_seconds > policy.max_wall_clock_seconds:
            raise I7ReconciliationRuntimeError("stored wall-clock resource gate failed")

    def to_dict(self) -> dict[str, object]:
        return {
            "declared_read_bytes": self.declared_read_bytes,
            "estimated_write_bytes": self.estimated_write_bytes,
            "metered_read_bytes": self.metered_read_bytes,
            "minimum_free_disk_bytes": self.minimum_free_disk_bytes,
            "observed_write_bytes": self.observed_write_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "phase_minimum_free_disk_bytes": [
                {"free_disk_bytes": value, "phase": phase}
                for phase, value in self.phase_minimum_free_disk_bytes
            ],
            "phase_peak_rss_bytes": [
                {"peak_rss_bytes": value, "phase": phase}
                for phase, value in self.phase_peak_rss_bytes
            ],
            "process_read_bytes": self.process_read_bytes,
            "process_write_bytes": self.process_write_bytes,
            "rule_version": I7_RESOURCE_OBSERVATION_RULE_VERSION,
            "wall_clock_seconds": self.wall_clock_seconds,
        }


@dataclass(frozen=True, slots=True)
class I7ReconciliationRunSpec:
    authority: str
    incremental: VerifiedIncrementalTopSnapshot
    independent_full: VerifiedLegacyFullSnapshot
    lifecycle_spec: FullReconciliationSpec
    receipt_available_session: date
    trigger_kind: ReconciliationTriggerKind
    trigger_reason: str
    resource_policy: ResourceGatePolicy

    def __post_init__(self) -> None:
        if self.authority not in {I7_PRODUCTION_AUTHORITY, I7_FIXTURE_AUTHORITY}:
            raise I7ReconciliationRuntimeError("I7 RunSpec authority is invalid")
        if not isinstance(self.incremental, VerifiedIncrementalTopSnapshot):
            raise I7ReconciliationRuntimeError("I7 incremental snapshot is invalid")
        if not isinstance(self.independent_full, VerifiedLegacyFullSnapshot):
            raise I7ReconciliationRuntimeError("I7 Full snapshot is invalid")
        if not isinstance(self.lifecycle_spec, FullReconciliationSpec):
            raise I7ReconciliationRuntimeError("I7 lifecycle spec is invalid")
        if self.receipt_available_session < self.lifecycle_spec.reconciliation_cutoff_session:
            raise I7ReconciliationRuntimeError("I7 RunSpec availability predates cutoff")
        if self.receipt_available_session < max(
            self.incremental.producer_available_session,
            self.independent_full.producer_available_session,
        ):
            raise I7ReconciliationRuntimeError("I7 receipt availability predates a producer")
        if not isinstance(self.trigger_kind, ReconciliationTriggerKind):
            raise I7ReconciliationRuntimeError("I7 RunSpec trigger kind is invalid")
        _text(self.trigger_reason, "I7 RunSpec trigger reason")
        if not isinstance(self.resource_policy, ResourceGatePolicy):
            raise I7ReconciliationRuntimeError("I7 RunSpec resource policy is invalid")
        if self.incremental.release_id != self.lifecycle_spec.incremental_top_release_id:
            raise I7ReconciliationRuntimeError("I7 incremental release binding differs")
        if (
            self.independent_full.release_id
            != self.lifecycle_spec.independent_full_candidate_release_id
        ):
            raise I7ReconciliationRuntimeError("I7 Full release binding differs")

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "authority": self.authority,
            "incremental": self.incremental.to_dict(),
            "independent_full": self.independent_full.to_dict(),
            "lifecycle_spec": self.lifecycle_spec.to_dict(),
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "resource_policy": self.resource_policy.to_dict(),
            "rule_version": I7_RUN_SPEC_RULE_VERSION,
            "trigger_kind": self.trigger_kind.value,
            "trigger_reason": self.trigger_reason,
        }

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class I7PreparedRun:
    run_spec: I7ReconciliationRunSpec
    run_spec_artifact: ArtifactPin
    trigger_artifact: ArtifactPin
    idempotent: bool


@dataclass(frozen=True, slots=True)
class I7ReconciliationCompletion:
    run_spec_id: str
    run_spec_artifact: ArtifactPin
    trigger_artifact: ArtifactPin
    alert_artifact: ArtifactPin
    receipt: FullReconciliationReceipt
    resource_observation: I7RuntimeResourceObservation
    authority: str

    def __post_init__(self) -> None:
        _digest(self.run_spec_id, "I7 completion RunSpec ID")
        for value, label in (
            (self.run_spec_artifact, "I7 completion RunSpec"),
            (self.trigger_artifact, "I7 completion trigger"),
            (self.alert_artifact, "I7 completion alert"),
        ):
            _artifact(value, label)
        if not isinstance(self.receipt, FullReconciliationReceipt):
            raise I7ReconciliationRuntimeError("I7 completion receipt is invalid")
        if not isinstance(self.resource_observation, I7RuntimeResourceObservation):
            raise I7ReconciliationRuntimeError("I7 completion resource observation is invalid")
        if self.authority not in {I7_PRODUCTION_AUTHORITY, I7_FIXTURE_AUTHORITY}:
            raise I7ReconciliationRuntimeError("I7 completion authority is invalid")

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "alert_artifact": self.alert_artifact.to_dict(),
            "artifact_type": "s7_5_i7_full_reconciliation_completion",
            "authority": self.authority,
            "automatic_publish_authorized": False,
            "publish_authorized": False,
            "receipt": self.receipt.to_dict(),
            "resource_observation": self.resource_observation.to_dict(),
            "rule_version": I7_COMPLETION_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "s7_5_complete": False,
            "state": I7_STATE,
            "trigger_artifact": self.trigger_artifact.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"completion_id": self.completion_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class I7ReconciliationResult:
    run_spec: I7ReconciliationRunSpec
    run_spec_artifact: ArtifactPin
    completion: I7ReconciliationCompletion
    completion_artifact: ArtifactPin
    idempotent: bool


def _freeze_completion_with_full_logical_write_evidence(
    *,
    run_spec: I7ReconciliationRunSpec,
    run_spec_artifact: ArtifactPin,
    trigger_artifact: ArtifactPin,
    alert_artifact: ArtifactPin,
    receipt: FullReconciliationReceipt,
    base_observation: I7RuntimeResourceObservation,
    review_write_bytes: int,
    authority: str,
) -> tuple[I7ReconciliationCompletion, bytes]:
    """Solve the completion-size/write-evidence fixed point before staging."""

    observation = base_observation
    for _ in range(16):
        completion = I7ReconciliationCompletion(
            run_spec_id=run_spec.run_spec_id,
            run_spec_artifact=run_spec_artifact,
            trigger_artifact=trigger_artifact,
            alert_artifact=alert_artifact,
            receipt=receipt,
            resource_observation=observation,
            authority=authority,
        )
        content = completion.canonical_bytes()
        updated = replace(
            base_observation,
            observed_write_bytes=review_write_bytes + len(content),
        )
        updated.validate(run_spec.resource_policy)
        if updated == observation:
            return completion, content
        observation = updated
    raise I7ReconciliationRuntimeError(
        "I7 completion logical-write evidence fixed point did not converge"
    )


class _ReadMeter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.bytes = 0

    def read_pin(self, pin: ArtifactPin, *, label: str) -> bytes:
        _artifact(pin, label)
        path = safe_relative_path(self.root, pin.path)
        if not path.is_file() or path.is_symlink():
            raise I7ReconciliationRuntimeError(f"{label} is missing or unsafe")
        content = path.read_bytes()
        self.bytes += len(content)
        if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
            raise I7ReconciliationRuntimeError(f"{label} exact pin differs")
        return content

    def read_path(self, relative: str) -> bytes:
        path = safe_relative_path(self.root, _relative(relative, "artifact reader path"))
        if not path.is_file() or path.is_symlink():
            raise I7ReconciliationRuntimeError("exact reconciliation artifact is unavailable")
        content = path.read_bytes()
        self.bytes += len(content)
        return content

    def account_verified_reader_bytes(self, value: int) -> None:
        """Conservatively account I/O performed inside an official producer reader."""

        if type(value) is not int or value < 0:
            raise I7ReconciliationRuntimeError("verified-reader byte count is invalid")
        self.bytes += value


@dataclass(frozen=True, slots=True)
class _IncrementalProjectionContext:
    loaded: LoadedI3ProductionStaging
    alias_reverse: Mapping[str, tuple[str, str]]
    small_rows: Mapping[str, tuple[dict[str, object], ...]]


class _RuntimeResourceTracker:
    def __init__(
        self,
        root: Path,
        *,
        policy: ResourceGatePolicy,
        meter: _ReadMeter,
        started: float,
        declared_read_bytes: int,
        estimated_write_bytes: int,
        process_io_start: tuple[int, int] | None = None,
        entry_free_disk_bytes: int | None = None,
        entry_peak_rss_bytes: int | None = None,
    ) -> None:
        self.root = root
        self.policy = policy
        self.meter = meter
        self.started = started
        self.declared_read_bytes = declared_read_bytes
        self.estimated_write_bytes = estimated_write_bytes
        if process_io_start is None:
            process_io_start = _proc_io_bytes()
        if (
            not isinstance(process_io_start, tuple)
            or len(process_io_start) != 2
            or any(type(value) is not int or value < 0 for value in process_io_start)
        ):
            raise I7ReconciliationRuntimeError("resource process-I/O baseline is invalid")
        self.start_read_bytes, self.start_write_bytes = process_io_start
        self.written_bytes = 0
        self.phase_disk: dict[str, int] = {}
        self.phase_rss: dict[str, int] = {}
        if entry_free_disk_bytes is not None or entry_peak_rss_bytes is not None:
            if entry_free_disk_bytes is None or entry_peak_rss_bytes is None:
                raise I7ReconciliationRuntimeError("resource entry observation is incomplete")
            self.phase_disk["entry"] = _nonnegative_int(
                entry_free_disk_bytes, "entry free disk bytes"
            )
            self.phase_rss["entry"] = _nonnegative_int(entry_peak_rss_bytes, "entry peak RSS bytes")
        self.check("preflight")

    def check(self, phase: str, *, written_bytes: int | None = None) -> None:
        phase_name = _text(phase, "resource phase")
        if written_bytes is not None:
            _nonnegative_int(written_bytes, "resource observed write bytes")
            self.written_bytes = max(self.written_bytes, written_bytes)
        free = _disk_free(self.root)
        prior = self.phase_disk.get(phase_name)
        self.phase_disk[phase_name] = free if prior is None else min(prior, free)
        rss = _peak_rss_bytes()
        prior_rss = self.phase_rss.get(phase_name)
        self.phase_rss[phase_name] = rss if prior_rss is None else max(prior_rss, rss)
        process_read, process_write = _proc_io_bytes()
        read_delta = max(0, process_read - self.start_read_bytes)
        write_delta = max(0, process_write - self.start_write_bytes)
        if self.declared_read_bytes > self.policy.max_read_bytes:
            raise I7ReconciliationRuntimeError("I7 declared-read preflight gate failed")
        if self.estimated_write_bytes > self.policy.max_write_bytes:
            raise I7ReconciliationRuntimeError("I7 estimated-write preflight gate failed")
        reserve = self.policy.min_free_disk_bytes + self.estimated_write_bytes
        if phase_name == "preflight" and free < reserve:
            raise I7ReconciliationRuntimeError("I7 preflight disk reserve gate failed")
        _enforce_resources(
            self.policy,
            self.started,
            self.meter,
            max(self.written_bytes, write_delta),
            free,
            process_read_bytes=read_delta,
            process_write_bytes=write_delta,
        )

    def observation(self) -> I7RuntimeResourceObservation:
        self.check("before_completion", written_bytes=self.written_bytes)
        process_read, process_write = _proc_io_bytes()
        observation = I7RuntimeResourceObservation(
            declared_read_bytes=self.declared_read_bytes,
            estimated_write_bytes=self.estimated_write_bytes,
            metered_read_bytes=self.meter.bytes,
            process_read_bytes=max(0, process_read - self.start_read_bytes),
            process_write_bytes=max(0, process_write - self.start_write_bytes),
            observed_write_bytes=self.written_bytes,
            peak_rss_bytes=max(self.phase_rss.values()),
            minimum_free_disk_bytes=min(self.phase_disk.values()),
            wall_clock_seconds=max(0, math.ceil(time.monotonic() - self.started)),
            phase_minimum_free_disk_bytes=tuple(sorted(self.phase_disk.items())),
            phase_peak_rss_bytes=tuple(sorted(self.phase_rss.items())),
        )
        observation.validate(self.policy)
        return observation


class _exclusive_nonblocking_lock(AbstractContextManager["_exclusive_nonblocking_lock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self) -> Self:
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
                raise I7ReconciliationRuntimeError("I7 lock is not a safe regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise I7ReconciliationRuntimeError("another process holds the exact I7 lock") from exc
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


def prepare_i7_full_reconciliation(
    data_root: Path,
    *,
    config: I7ReconciliationConfig,
) -> I7PreparedRun:
    """Prepare production I7 after a metadata-only aggregate resource preflight."""

    _preflight_production_declarations(data_root, config)
    return _prepare_i7_full_reconciliation(
        data_root,
        config=config,
        authority=I7_PRODUCTION_AUTHORITY,
        incremental_loader=_load_production_incremental_top_snapshot,
        full_loader=load_official_legacy_full_snapshot,
    )


def stage_i7_full_reconciliation(
    data_root: Path,
    *,
    run_spec_artifact: ArtifactPin,
) -> I7ReconciliationResult:
    """Stage production comparison and stop at immutable ``awaiting_review``."""

    return _stage_i7_full_reconciliation(
        data_root,
        run_spec_artifact=run_spec_artifact,
        authority=I7_PRODUCTION_AUTHORITY,
        incremental_loader=_load_production_incremental_top_snapshot,
        full_loader=load_official_legacy_full_snapshot,
    )


def verify_i7_full_reconciliation(
    data_root: Path,
    *,
    completion_artifact: ArtifactPin,
) -> I7ReconciliationResult:
    """Deep replay production completion; never grants publication authority."""

    return _verify_i7_full_reconciliation(
        data_root,
        completion_artifact=completion_artifact,
        authority=I7_PRODUCTION_AUTHORITY,
        incremental_loader=_load_production_incremental_top_snapshot,
        full_loader=load_official_legacy_full_snapshot,
    )


def _prepare_i7_full_reconciliation_fixture(
    data_root: Path,
    *,
    config: I7ReconciliationConfig,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7PreparedRun:
    return _prepare_i7_full_reconciliation(
        data_root,
        config=config,
        authority=I7_FIXTURE_AUTHORITY,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )


def _stage_i7_full_reconciliation_fixture(
    data_root: Path,
    *,
    run_spec_artifact: ArtifactPin,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7ReconciliationResult:
    return _stage_i7_full_reconciliation(
        data_root,
        run_spec_artifact=run_spec_artifact,
        authority=I7_FIXTURE_AUTHORITY,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )


def _verify_i7_full_reconciliation_fixture(
    data_root: Path,
    *,
    completion_artifact: ArtifactPin,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7ReconciliationResult:
    return _verify_i7_full_reconciliation(
        data_root,
        completion_artifact=completion_artifact,
        authority=I7_FIXTURE_AUTHORITY,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )


def _prepare_i7_full_reconciliation(
    data_root: Path,
    *,
    config: I7ReconciliationConfig,
    authority: str,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7PreparedRun:
    if not isinstance(config, I7ReconciliationConfig):
        raise I7ReconciliationRuntimeError("I7 prepare requires a typed config")
    root = _root(data_root)
    started = time.monotonic()
    process_io_start = _proc_io_bytes()
    entry_free_disk_bytes = _disk_free(root)
    entry_peak_rss_bytes = _peak_rss_bytes()
    meter = _ReadMeter(root)
    chain_started = time.monotonic()
    incremental = incremental_loader(
        root,
        top_pointer_artifact=config.incremental_top_pointer_artifact,
        gate_c_approval_artifact=config.gate_c_approval_artifact,
        checkpoint_compaction_completion_artifact=(
            config.checkpoint_compaction_completion_artifact
        ),
        cutoff_session=config.cutoff_session,
    )
    full = full_loader(
        root,
        completion_artifact=config.independent_full_completion_artifact,
        cutoff_session=config.cutoff_session,
    )
    chain_milliseconds = math.ceil((time.monotonic() - chain_started) * 1000)
    declared_read, estimated_write = _declared_preflight(
        root,
        policy=config.resource_policy,
        incremental=incremental,
        full=full,
    )
    meter.account_verified_reader_bytes(declared_read // 2)
    tracker = _RuntimeResourceTracker(
        root,
        policy=config.resource_policy,
        meter=meter,
        started=started,
        declared_read_bytes=declared_read,
        estimated_write_bytes=estimated_write,
        process_io_start=process_io_start,
        entry_free_disk_bytes=entry_free_disk_bytes,
        entry_peak_rss_bytes=entry_peak_rss_bytes,
    )
    if chain_milliseconds > config.resource_policy.max_chain_resolution_milliseconds:
        raise I7ReconciliationRuntimeError("I7 prepare chain-resolution gate failed")
    _validate_loaded_authorities(
        config=config,
        authority=authority,
        incremental=incremental,
        full=full,
    )
    spec = _build_run_spec(config, authority=authority, incremental=incremental, full=full)
    production = authority == I7_PRODUCTION_AUTHORITY
    run_relative = _run_spec_path(spec.run_spec_id, production=production)
    trigger_relative = _trigger_path(spec.run_spec_id, production=production)
    trigger = _trigger_document(spec)
    lock = safe_relative_path(root, _lock_path(spec.run_spec_id, production=production))
    with _exclusive_nonblocking_lock(lock):
        idempotent = safe_relative_path(root, run_relative).exists()
        run_pin = _write_immutable(
            root,
            run_relative,
            spec.canonical_bytes(),
            label="I7 RunSpec",
        )
        trigger_pin = _write_immutable(
            root,
            trigger_relative,
            _canonical_json_bytes(trigger),
            label="I7 trigger",
        )
        tracker.check("prepared_controls", written_bytes=run_pin.bytes + trigger_pin.bytes)
    return I7PreparedRun(spec, run_pin, trigger_pin, idempotent)


def _stage_i7_full_reconciliation(
    data_root: Path,
    *,
    run_spec_artifact: ArtifactPin,
    authority: str,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7ReconciliationResult:
    root = _root(data_root)
    _artifact(run_spec_artifact, "I7 RunSpec")
    meter = _ReadMeter(root)
    started = time.monotonic()
    process_io_start = _proc_io_bytes()
    entry_free_disk_bytes = _disk_free(root)
    entry_peak_rss_bytes = _peak_rss_bytes()
    spec = _load_run_spec_exact(root, run_spec_artifact, authority=authority, meter=meter)
    declared_read, estimated_write = _declared_preflight(
        root,
        policy=spec.resource_policy,
        incremental=spec.incremental,
        full=spec.independent_full,
    )
    tracker = _RuntimeResourceTracker(
        root,
        policy=spec.resource_policy,
        meter=meter,
        started=started,
        declared_read_bytes=declared_read,
        estimated_write_bytes=estimated_write,
        process_io_start=process_io_start,
        entry_free_disk_bytes=entry_free_disk_bytes,
        entry_peak_rss_bytes=entry_peak_rss_bytes,
    )
    production = authority == I7_PRODUCTION_AUTHORITY
    completion_relative = _completion_path(spec.run_spec_id, production=production)
    completion_path = safe_relative_path(root, completion_relative)
    lock = safe_relative_path(root, _lock_path(spec.run_spec_id, production=production))
    with _exclusive_nonblocking_lock(lock):
        if completion_path.exists() or completion_path.is_symlink():
            return _verify_i7_full_reconciliation(
                root,
                completion_artifact=_pin_existing(root, completion_relative, "I7 completion"),
                authority=authority,
                incremental_loader=incremental_loader,
                full_loader=full_loader,
            )
        chain_started = time.monotonic()
        incremental = incremental_loader(
            root,
            top_pointer_artifact=spec.incremental.top_pointer_artifact,
            gate_c_approval_artifact=spec.incremental.gate_c_approval_artifact,
            checkpoint_compaction_completion_artifact=(
                spec.incremental.checkpoint_base_compaction_completion_artifact
            ),
            cutoff_session=spec.lifecycle_spec.reconciliation_cutoff_session,
        )
        full = full_loader(
            root,
            completion_artifact=spec.independent_full.completion_artifact,
            cutoff_session=spec.lifecycle_spec.reconciliation_cutoff_session,
        )
        chain_milliseconds = math.ceil((time.monotonic() - chain_started) * 1000)
        meter.account_verified_reader_bytes(declared_read // 2)
        if chain_milliseconds > spec.resource_policy.max_chain_resolution_milliseconds:
            raise I7ReconciliationRuntimeError("I7 stage chain-resolution gate failed")
        tracker.check("producer_replay")
        if incremental != spec.incremental or full != spec.independent_full:
            raise I7ReconciliationRuntimeError("I7 producer replay differs from frozen RunSpec")
        _read_required_authority_artifacts(root, incremental=incremental, full=full, meter=meter)
        tracker.check("authority_artifacts")
        table_evidence, details_documents, checkpoint_before, checkpoint_rebased = (
            _compare_all_partitions(root, spec=spec, meter=meter, resource_tracker=tracker)
        )
        total_differences = sum(table.unexpected_difference_count for table in table_evidence)
        details = _aggregate_details_document(
            spec,
            table_evidence=table_evidence,
            checkpoint_before=checkpoint_before,
            checkpoint_rebased=checkpoint_rebased,
        )
        details_pin = _document_pin(_details_path(spec.run_spec_id, production=production), details)
        qa = _qa_document(
            spec,
            table_evidence=table_evidence,
            checkpoint_before=checkpoint_before,
            checkpoint_rebased=checkpoint_rebased,
        )
        qa_pin = _document_pin(_qa_path(spec.run_spec_id, production=production), qa)
        alert = _alert_document(
            spec,
            unexpected_difference_count=total_differences,
            checkpoint_drift=(checkpoint_before != checkpoint_rebased),
        )
        alert_pin = _document_pin(_alert_path(spec.run_spec_id, production=production), alert)
        written = 0
        for pin, document in details_documents:
            stored = _write_immutable(
                root, pin.path, _canonical_json_bytes(document), label="partition details"
            )
            if stored != pin:
                raise I7ReconciliationRuntimeError("partition details pin changed")
            written += stored.bytes
        for pin, document, label in (
            (details_pin, details, "I7 aggregate details"),
            (qa_pin, qa, "I7 QA"),
            (alert_pin, alert, "I7 alert"),
        ):
            stored = _write_immutable(root, pin.path, _canonical_json_bytes(document), label=label)
            if stored != pin:
                raise I7ReconciliationRuntimeError(f"{label} pin changed")
            written += stored.bytes
        tracker.check("review_artifacts", written_bytes=written)
        if total_differences != 0 or checkpoint_before != checkpoint_rebased:
            raise I7ReconciliationRuntimeError(
                "I7 reconciliation found unexpected or checkpoint-rebase differences"
            )
        receipt = FullReconciliationReceipt(
            spec_id=spec.lifecycle_spec.spec_id,
            incremental_top_release_id=spec.incremental.release_id,
            independent_full_candidate_release_id=spec.independent_full.release_id,
            table_evidence=table_evidence,
            checkpoint_before_projection_digest=checkpoint_before,
            checkpoint_rebased_projection_digest=checkpoint_rebased,
            qa_artifact=qa_pin,
            details_artifact=details_pin,
            receipt_available_session=spec.receipt_available_session,
        )
        validate_full_reconciliation(
            spec.lifecycle_spec,
            receipt,
            availability_cutoff_session=spec.receipt_available_session,
            artifact_reader=meter.read_path,
        )
        trigger_pin = _pin_existing(
            root, _trigger_path(spec.run_spec_id, production=production), "I7 trigger"
        )
        resource_observation = tracker.observation()
        completion, completion_bytes = _freeze_completion_with_full_logical_write_evidence(
            run_spec=spec,
            run_spec_artifact=run_spec_artifact,
            trigger_artifact=trigger_pin,
            alert_artifact=alert_pin,
            receipt=receipt,
            base_observation=resource_observation,
            review_write_bytes=written,
            authority=authority,
        )
        _validate_stored_stage_resource_evidence(
            completion.resource_observation,
            spec=spec,
            receipt=receipt,
            alert_artifact=alert_pin,
            completion_artifact_bytes=len(completion_bytes),
        )
        tracker.check("completion_prewrite", written_bytes=written + len(completion_bytes))
        completion_pin = _stage_then_publish_immutable(
            root,
            completion_relative,
            completion_bytes,
            label="I7 completion",
            before_publish=lambda: tracker.check(
                "completion_staged",
                written_bytes=written + len(completion_bytes),
            ),
        )
        return I7ReconciliationResult(
            run_spec=spec,
            run_spec_artifact=run_spec_artifact,
            completion=completion,
            completion_artifact=completion_pin,
            idempotent=False,
        )


def _verify_i7_full_reconciliation(
    data_root: Path,
    *,
    completion_artifact: ArtifactPin,
    authority: str,
    incremental_loader: IncrementalTopLoader,
    full_loader: LegacyFullLoader,
) -> I7ReconciliationResult:
    root = _root(data_root)
    locator_id: str | None = None
    if authority == I7_PRODUCTION_AUTHORITY:
        locator_id = _validate_production_control_locator(completion_artifact, kind="completion")
    meter = _ReadMeter(root)
    started = time.monotonic()
    process_io_start = _proc_io_bytes()
    entry_free_disk_bytes = _disk_free(root)
    entry_peak_rss_bytes = _peak_rss_bytes()
    content = meter.read_pin(completion_artifact, label="I7 completion")
    completion = _completion_from_dict(_closed_json(content, "I7 completion"))
    if locator_id is not None and completion.run_spec_id != locator_id:
        raise I7ReconciliationRuntimeError("I7 completion directory ID differs")
    if completion.authority != authority:
        raise I7ReconciliationRuntimeError("I7 completion authority differs")
    production = authority == I7_PRODUCTION_AUTHORITY
    expected_path = _completion_path(completion.run_spec_id, production=production)
    if completion_artifact.path != expected_path:
        raise I7ReconciliationRuntimeError("I7 completion path is noncanonical")
    spec = _load_run_spec_exact(
        root, completion.run_spec_artifact, authority=authority, meter=meter
    )
    declared_read, estimated_write = _declared_preflight(
        root,
        policy=spec.resource_policy,
        incremental=spec.incremental,
        full=spec.independent_full,
    )
    tracker = _RuntimeResourceTracker(
        root,
        policy=spec.resource_policy,
        meter=meter,
        started=started,
        declared_read_bytes=declared_read,
        estimated_write_bytes=estimated_write,
        process_io_start=process_io_start,
        entry_free_disk_bytes=entry_free_disk_bytes,
        entry_peak_rss_bytes=entry_peak_rss_bytes,
    )
    incremental = incremental_loader(
        root,
        top_pointer_artifact=spec.incremental.top_pointer_artifact,
        gate_c_approval_artifact=spec.incremental.gate_c_approval_artifact,
        checkpoint_compaction_completion_artifact=(
            spec.incremental.checkpoint_base_compaction_completion_artifact
        ),
        cutoff_session=spec.lifecycle_spec.reconciliation_cutoff_session,
    )
    full = full_loader(
        root,
        completion_artifact=spec.independent_full.completion_artifact,
        cutoff_session=spec.lifecycle_spec.reconciliation_cutoff_session,
    )
    if incremental != spec.incremental or full != spec.independent_full:
        raise I7ReconciliationRuntimeError("I7 verification producer replay differs")
    meter.account_verified_reader_bytes(declared_read // 2)
    tracker.check("deep_producer_replay")
    _read_required_authority_artifacts(root, incremental=incremental, full=full, meter=meter)
    table_evidence, _, checkpoint_before, checkpoint_rebased = _compare_all_partitions(
        root,
        spec=spec,
        meter=meter,
        write_documents=False,
        resource_tracker=tracker,
    )
    if table_evidence != completion.receipt.table_evidence:
        raise I7ReconciliationRuntimeError("I7 stored partition evidence does not replay")
    if (
        checkpoint_before != completion.receipt.checkpoint_before_projection_digest
        or checkpoint_rebased != completion.receipt.checkpoint_rebased_projection_digest
    ):
        raise I7ReconciliationRuntimeError("I7 checkpoint evidence does not replay")
    expected_trigger = _trigger_document(spec)
    expected_alert = _alert_document(
        spec,
        unexpected_difference_count=0,
        checkpoint_drift=False,
    )
    expected_qa = _qa_document(
        spec,
        table_evidence=table_evidence,
        checkpoint_before=checkpoint_before,
        checkpoint_rebased=checkpoint_rebased,
    )
    expected_details = _aggregate_details_document(
        spec,
        table_evidence=table_evidence,
        checkpoint_before=checkpoint_before,
        checkpoint_rebased=checkpoint_rebased,
    )
    expected_documents = (
        (completion.trigger_artifact, expected_trigger, "I7 trigger"),
        (completion.alert_artifact, expected_alert, "I7 alert"),
        (completion.receipt.qa_artifact, expected_qa, "I7 QA"),
        (
            completion.receipt.details_artifact,
            expected_details,
            "I7 aggregate details",
        ),
    )
    for pin, document, label in expected_documents:
        expected = _document_pin(pin.path, document)
        if pin != expected or meter.read_pin(pin, label=label) != _canonical_json_bytes(document):
            raise I7ReconciliationRuntimeError(f"{label} does not replay")
    validate_full_reconciliation(
        spec.lifecycle_spec,
        completion.receipt,
        availability_cutoff_session=spec.receipt_available_session,
        artifact_reader=meter.read_path,
    )
    completion.resource_observation.validate(spec.resource_policy)
    if (
        completion.resource_observation.declared_read_bytes != declared_read
        or completion.resource_observation.estimated_write_bytes != estimated_write
    ):
        raise I7ReconciliationRuntimeError("I7 stored resource preflight does not replay")
    _validate_stored_stage_resource_evidence(
        completion.resource_observation,
        spec=spec,
        receipt=completion.receipt,
        alert_artifact=completion.alert_artifact,
        completion_artifact_bytes=completion_artifact.bytes,
    )
    tracker.check("deep_verification_complete")
    if completion.to_dict()["state"] != I7_STATE:
        raise I7ReconciliationRuntimeError("I7 completion state differs")
    return I7ReconciliationResult(
        run_spec=spec,
        run_spec_artifact=completion.run_spec_artifact,
        completion=completion,
        completion_artifact=completion_artifact,
        idempotent=True,
    )


def _load_production_incremental_top_snapshot(
    data_root: Path,
    *,
    top_pointer_artifact: ArtifactPin,
    gate_c_approval_artifact: ArtifactPin,
    checkpoint_compaction_completion_artifact: ArtifactPin,
    cutoff_session: date,
) -> VerifiedIncrementalTopSnapshot:
    """Replay the I6 top plus an independently materialized checkpoint BASE."""

    root = _root(data_root)
    try:
        from ame_stocks_api.silver import incremental_i6_pointer_runtime as i6
    except ImportError as exc:  # pragma: no cover - production packaging guard
        raise I7IncrementalTopSeamError(
            "P0 I6 seam: exact research-top snapshot loader is unavailable"
        ) from exc
    loader = getattr(i6, "load_research_top_snapshot_exact", None)
    snapshot_type = getattr(i6, "ResearchTopSnapshot", None)
    if not callable(loader) or not isinstance(snapshot_type, type):
        raise I7IncrementalTopSeamError("P0 I6 seam: exact research-top snapshot API is not frozen")
    try:
        snapshot = loader(root)
    except Exception as exc:
        raise I7IncrementalTopSeamError("P0 I6 seam: research top cannot be replayed") from exc
    if type(snapshot) is not snapshot_type:
        raise I7IncrementalTopSeamError("P0 I6 seam: research-top loader returned another type")
    if (
        snapshot.research_top_event_artifact != top_pointer_artifact
        or snapshot.gate_c_approval_artifact != gate_c_approval_artifact
        or snapshot.terminal_session != cutoff_session
        or snapshot.source_cutoff_session != cutoff_session
    ):
        raise I7IncrementalTopSeamError("P0 I6 seam: exact top pins or cutoff differ")
    try:
        from ame_stocks_api.silver import incremental_i7_checkpoint_compaction as compaction

        compacted = compaction.verify_i7_checkpoint_compaction(
            root,
            checkpoint_compaction_completion_artifact,
        )
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I7 checkpoint-rebase seam: independent checkpoint BASE cannot be replayed"
        ) from exc
    source = compacted.run_spec.source
    if (
        compacted.completion_artifact != checkpoint_compaction_completion_artifact
        or source.snapshot_id != snapshot.snapshot_id
        or source.release_id != snapshot.release_id
        or source.native_v2_release_id != snapshot.native_v2_release_id
        or source.checkpoint_id != snapshot.checkpoint_id
        or source.top_pointer_artifact != top_pointer_artifact
        or source.gate_c_approval_artifact != gate_c_approval_artifact
        or source.terminal_session != cutoff_session
        or compacted.source_checkpoint.checkpoint_id != snapshot.checkpoint_id
    ):
        raise I7IncrementalTopSeamError(
            "P0 I7 checkpoint-rebase seam: compaction source differs from the I6 top"
        )
    try:
        meter = _ReadMeter(root)
        completion = load_i3_production_completion_exact(
            snapshot.release_completion_artifact,
            meter.read_path,
        )
        receipt = load_i3_production_run_receipt_exact(
            completion.receipt_artifact,
            meter.read_path,
        )
        run_spec = load_i3_production_run_spec_exact(
            receipt.run_spec_artifact,
            meter.read_path,
        )
        parent = load_i3_production_parent_shallow_exact(root, run_spec)
        if parent is None or receipt.output_set is None:
            raise I7IncrementalTopSeamError("P0 I6 seam: exact DELTA parent is unavailable")
        inputs = load_production_delta_input_binding(
            data_root=root,
            run_spec=run_spec,
            parent=parent,
        )
        bronze, s4, policy = _streaming_source_projection(inputs.source_binding)
    except I7IncrementalTopSeamError:
        raise
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I6 seam: cross-producer source projection cannot be replayed"
        ) from exc
    if (
        run_spec.run_kind is not I3ProductionRunKind.DELTA
        or run_spec.terminal_session != cutoff_session
        or receipt.output_set.table_outputs != snapshot.table_outputs
        or run_spec.identity_policy_bundle.identity_policy_bundle_id
        != snapshot.identity_policy_bundle_id
    ):
        raise I7IncrementalTopSeamError("P0 I6 seam: source projection authority differs")
    declared = (
        _unique_pin_bytes(_i6_authority_pins(snapshot))
        + receipt.output_set.total_output_bytes
        + _positive_int(
            inputs.source_binding.declared_source_bytes,
            "incremental declared source bytes",
        )
        + sum(
            _pin_like_bytes(pin, "incremental contract approval")
            for pin in inputs.source_binding.contract_approvals
        )
        + _unique_pin_bytes(inputs.binding.declared_input_artifacts)
        + _positive_int(
            inputs.binding.transitive_control_replay_bytes,
            "incremental transitive control replay bytes",
        )
        + meter.bytes
        + compacted.run_spec_artifact.bytes
        + compacted.completion_artifact.bytes
        + compacted.completion.output_bytes
    )
    return VerifiedIncrementalTopSnapshot(
        authority=I7_PRODUCTION_AUTHORITY,
        release_id=snapshot.release_id,
        native_v2_release_id=snapshot.native_v2_release_id,
        checkpoint_id=snapshot.checkpoint_id,
        checkpoint_base_native_v2_release_id=compacted.compacted_manifest.release_id,
        checkpoint_base_checkpoint_id=compacted.compacted_checkpoint.checkpoint_id,
        resolved_state_digest=snapshot.resolved_state_digest,
        resolved_content_digest=snapshot.resolved_content_digest,
        physical_index_digest=snapshot.physical_index_digest,
        row_semantic_attestation_digest=snapshot.row_semantic_attestation_digest,
        cutoff_session=cutoff_session,
        producer_available_session=max(
            snapshot.producer_available_session,
            compacted.completion.completion_available_session,
        ),
        producer_replay_declared_bytes=declared,
        bronze_source_binding_digest=bronze,
        s4_source_binding_digest=s4,
        schema_bundle_digest=snapshot.schema_bundle_digest,
        transform_semantics_digest=snapshot.transform_semantics_digest,
        native_identity_policy_lineage_id=snapshot.identity_policy_bundle_id,
        identity_policy_bundle_id=policy,
        calendar_digest=snapshot.calendar_digest,
        top_pointer_artifact=snapshot.research_top_event_artifact,
        gate_c_approval_artifact=snapshot.gate_c_approval_artifact,
        producer_verification_artifact=snapshot.deep_attestation_artifact,
        release_completion_artifact=snapshot.release_completion_artifact,
        checkpoint_base_compaction_completion_artifact=compacted.completion_artifact,
        checkpoint_base_compaction_proof_artifact=compacted.proof_artifact,
        resolved_partitions=_checkpoint_base_native_partitions(compacted),
        checkpoint_before_partitions=_incremental_native_partitions(snapshot),
    )


def _i6_authority_pins(snapshot: object) -> tuple[ArtifactPin, ...]:
    return (
        snapshot.research_top_event_artifact,
        snapshot.gate_c_approval_artifact,
        snapshot.gate_b_approval_artifact,
        snapshot.shadow_stage_receipt_artifact,
        snapshot.shadow_pointer_event_artifact,
        snapshot.rollback_stage_receipt_artifact,
        snapshot.rollback_pointer_event_artifact,
        snapshot.rollback_receipt_artifact,
        snapshot.research_top_stage_receipt_artifact,
        snapshot.release_completion_artifact,
        snapshot.deep_attestation_artifact,
        snapshot.checkpoint_artifact,
    )


def _incremental_native_partitions(snapshot: object) -> tuple[ReconciliationPartition, ...]:
    partitions: list[ReconciliationPartition] = []
    for output in snapshot.table_outputs:
        table_name = output.table_name
        contract = cross_producer_projection_contract(table_name)
        if table_name == "universe_daily":
            if output.dataset_index is None:
                raise I7IncrementalTopSeamError("I6 universe output lacks a dataset index")
            for member in output.dataset_index.partitions:
                partitions.append(
                    ReconciliationPartition(
                        table_name=table_name,
                        partition_key=member.session_date.isoformat(),
                        artifact=member.artifact,
                        row_count=member.row_count,
                        schema_digest=member.schema_digest,
                        physical_digest=stable_digest(
                            {
                                "native_release_id": snapshot.native_v2_release_id,
                                "partition": member.to_dict(),
                                "table_output_id": output.table_output_id,
                            }
                        ),
                        projection_kind=CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
                        projection_contract_digest=contract.contract_digest,
                        native_release_id=snapshot.native_v2_release_id,
                        lineage_artifacts=(
                            output.manifest_output.artifact,
                            member.artifact,
                        ),
                    )
                )
        else:
            if (
                output.storage is not I3ProductionOutputStorage.ROWSET_INDEX
                or output.rowset_index is None
            ):
                raise I7IncrementalTopSeamError("I6 small table lacks a rowset index")
            lineage = (
                output.manifest_output.artifact,
                *(item.artifact for item in output.rowset_index.segments),
            )
            partitions.append(
                ReconciliationPartition(
                    table_name=table_name,
                    partition_key="__table__",
                    artifact=output.manifest_output.artifact,
                    row_count=output.rowset_index.row_count,
                    schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                    physical_digest=stable_digest(
                        {
                            "native_release_id": snapshot.native_v2_release_id,
                            "rowset_index": output.rowset_index.to_dict(),
                            "table_output_id": output.table_output_id,
                        }
                    ),
                    projection_kind=CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
                    projection_contract_digest=contract.contract_digest,
                    native_release_id=snapshot.native_v2_release_id,
                    lineage_artifacts=lineage,
                )
            )
    return tuple(sorted(partitions, key=_partition_sort_key))


def _checkpoint_base_native_partitions(
    compacted: object,
) -> tuple[ReconciliationPartition, ...]:
    """Expose independent compacted bytes, never source aliases, to I7."""

    partitions: list[ReconciliationPartition] = []
    native_release_id = compacted.compacted_manifest.release_id
    for output in compacted.output_artifacts:
        table_name = output.table_name
        contract = cross_producer_projection_contract(table_name)
        if table_name == "universe_daily":
            if output.dataset_index is None:
                raise I7IncrementalTopSeamError("checkpoint BASE universe index is absent")
            for member in output.dataset_index.partitions:
                partitions.append(
                    ReconciliationPartition(
                        table_name=table_name,
                        partition_key=member.session_date.isoformat(),
                        artifact=member.artifact,
                        row_count=member.row_count,
                        schema_digest=member.schema_digest,
                        physical_digest=stable_digest(
                            {
                                "checkpoint_base_release_id": native_release_id,
                                "dataset_index": output.manifest_output.artifact.to_dict(),
                                "partition": member.to_dict(),
                            }
                        ),
                        projection_kind=CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
                        projection_contract_digest=contract.contract_digest,
                        native_release_id=native_release_id,
                        lineage_artifacts=(output.manifest_output.artifact, member.artifact),
                    )
                )
            continue
        if (
            output.storage is not I3ProductionOutputStorage.ROWSET_INDEX
            or output.rowset_index is None
            or len(output.rowset_index.segments) != 1
        ):
            raise I7IncrementalTopSeamError(
                "checkpoint BASE small table is not one independent segment"
            )
        segment = output.rowset_index.segments[0]
        partitions.append(
            ReconciliationPartition(
                table_name=table_name,
                partition_key="__table__",
                artifact=segment.artifact,
                row_count=segment.row_count,
                schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                physical_digest=stable_digest(
                    {
                        "checkpoint_base_release_id": native_release_id,
                        "rowset_index": output.manifest_output.artifact.to_dict(),
                        "segment": segment.to_dict(),
                    }
                ),
                projection_kind=CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
                projection_contract_digest=contract.contract_digest,
                native_release_id=native_release_id,
                lineage_artifacts=(output.manifest_output.artifact, segment.artifact),
            )
        )
    return tuple(sorted(partitions, key=_partition_sort_key))


def load_official_legacy_full_snapshot(
    data_root: Path,
    *,
    completion_artifact: ArtifactPin,
    cutoff_session: date,
) -> VerifiedLegacyFullSnapshot:
    """Replay one production Full through the official all-tree verifier.

    This function deliberately invokes ``_verify_completion_and_candidate``.
    That verifier checks the exact candidate file set, receipt bytes, every
    small-table Parquet, every universe partition, and reconstructed QA.  I7
    then extracts, but never substitutes for, that verified output inventory.
    """

    root = _root(data_root)
    locator_plan_id, locator_approval_id = _validate_legacy_full_completion_locator(
        completion_artifact
    )
    content = _read_exact_pin(root, completion_artifact, "legacy Full completion")
    completion = _closed_json(content, "legacy Full completion")
    try:
        plan_id = _digest(completion["plan_id"], "legacy Full plan ID")
        approval_id = _digest(completion["approval_id"], "legacy Full approval ID")
        candidate_id = _digest(completion["candidate_id"], "legacy Full candidate ID")
        completion_id = _digest(completion["completion_id"], "legacy Full completion ID")
        completed_at = legacy_full._utc_from_text(
            completion["completed_at_utc"], "legacy Full completion time"
        )
    except (KeyError, TypeError) as exc:
        raise I7LegacyFullSeamError("legacy Full completion fields are incomplete") from exc
    if (
        plan_id != locator_plan_id
        or approval_id != locator_approval_id
        or completion_artifact.path != legacy_full._completion_path(plan_id, approval_id)
    ):
        raise I7LegacyFullSeamError("legacy Full completion path is not canonical")
    try:
        controls = legacy_full._load_execution_controls(
            root, plan_id=plan_id, approval_id=approval_id
        )
        plan = controls["plan"]
        approval = controls["approval"]
        binding = controls["binding"]
        if binding.mode != "production" or binding.cutoff_session != cutoff_session:
            raise I7LegacyFullSeamError("legacy Full production scope or cutoff differs")
        approved_at = approval.approved_at_utc
        if completed_at < approved_at:
            raise I7LegacyFullSeamError("legacy Full completion predates its exact approval")
        result = legacy_full._verify_completion_and_candidate(
            root,
            safe_relative_path(root, completion_artifact.path),
            plan=plan,
            approval=approval,
            binding=binding,
            expected_candidate_id=candidate_id,
            caps=legacy_full.StreamingResourceCaps.from_dict(plan["resource_caps"]),
            idempotent=True,
        )
        legacy_full._load_verified_execution_sources(
            root,
            binding=binding,
            registry_loader=load_registry_release_set,
        )
        availability = legacy_full._calendar_availability(
            root,
            calendar_artifact_id=binding.calendar_artifact_id,
            calendar_artifact_sha256=binding.calendar_artifact_sha256,
            recorded_at=completed_at,
        )
        completion_available_session = date.fromisoformat(
            _text(
                availability["source_available_session"],
                "legacy Full completion availability",
            )
        )
        approval_available_session = date.fromisoformat(
            _text(
                approval.approval_availability["source_available_session"],
                "legacy Full approval availability",
            )
        )
        producer_available_session = max(
            completion_available_session,
            approval_available_session,
        )
    except I7LegacyFullSeamError:
        raise
    except Exception as exc:
        raise I7LegacyFullSeamError(
            "official legacy Full all-tree/Parquet/QA verification failed"
        ) from exc
    if (
        result.candidate_id != candidate_id
        or result.completion_id != completion_id
        or result.state != legacy_full.STREAMING_STATE
    ):
        raise I7LegacyFullSeamError("official legacy Full result binding differs")
    candidate_relative = legacy_full._candidate_path(candidate_id)
    candidate_pin = _artifact_from_dict(
        completion["candidate_manifest"], "legacy candidate manifest"
    )
    if candidate_pin.path != f"{candidate_relative}/manifest.json":
        raise I7LegacyFullSeamError("legacy candidate manifest path differs")
    candidate = _closed_json(
        _read_exact_pin(root, candidate_pin, "legacy candidate manifest"),
        "legacy candidate manifest",
    )
    outputs = _mapping(candidate["outputs"], "legacy Full outputs")
    qa_receipt = _mapping(outputs.get("qa"), "legacy Full QA output")
    candidate_output_bytes = _positive_int(qa_receipt.get("bytes"), "legacy Full QA output bytes")
    partitions: list[ReconciliationPartition] = []
    for table_name in I7_TABLE_ORDER:
        raw = outputs[table_name]
        receipts = _sequence(raw, f"{table_name} outputs") if isinstance(raw, list) else (raw,)
        for value in receipts:
            receipt = _mapping(value, f"{table_name} output receipt")
            relative = _relative(receipt["path"], f"{table_name} output path")
            artifact = ArtifactPin(
                path=f"{candidate_relative}/{relative}",
                sha256=_digest(receipt["sha256"], f"{table_name} output SHA"),
                bytes=_positive_int(receipt["bytes"], f"{table_name} output bytes"),
            )
            candidate_output_bytes += artifact.bytes
            row_count = _positive_int(receipt["row_count"], f"{table_name} output rows")
            schema_digest = _digest(receipt["schema_digest"], f"{table_name} output schema digest")
            if table_name == "universe_daily":
                match = _SESSION_PARTITION.search(artifact.path)
                if match is None:
                    raise I7LegacyFullSeamError("legacy universe partition path is invalid")
                partition_key = match.group(1)
            else:
                partition_key = "__table__"
            partitions.append(
                ReconciliationPartition(
                    table_name=table_name,
                    partition_key=partition_key,
                    artifact=artifact,
                    row_count=row_count,
                    schema_digest=schema_digest,
                    physical_digest=stable_digest(
                        {
                            "artifact": artifact.to_dict(),
                            "partition_key": partition_key,
                            "row_count": row_count,
                            "schema_digest": schema_digest,
                        }
                    ),
                    projection_kind=CrossProducerProjectionKind.LEGACY_NATIVE_V1,
                    projection_contract_digest=(
                        cross_producer_projection_contract(table_name).contract_digest
                    ),
                    native_release_id=candidate_id,
                    lineage_artifacts=(artifact,),
                )
            )
    ordered = tuple(sorted(partitions, key=_partition_sort_key))
    _partition_set(ordered, "official legacy Full snapshot")
    bronze, s4, policy = _streaming_source_projection(binding)
    legacy_transform_lineage = stable_digest(
        {
            "contract_pins": plan["contract_pins"],
            "policy_version": plan["policy_version"],
            "rule_version": "s7_5_i7_legacy_streaming_transform_lineage_v1",
            "runtime_binding": dict(binding.runtime_binding),
            "source_binding_id": binding.source_binding_id,
        }
    )
    direct_control_bytes = sum(
        _pin_like_bytes(controls[key], f"legacy Full {key}")
        for key in ("plan_receipt", "request_receipt", "approval_receipt")
    )
    direct_control_bytes += _artifact_from_dict(
        plan["source_binding"], "legacy Full source binding receipt"
    ).bytes
    direct_control_bytes += sum(
        _pin_like_bytes(pin, "legacy Full contract approval") for pin in binding.contract_approvals
    )
    profile = plan.get("bounded_profile_evidence")
    if profile is not None:
        direct_control_bytes += _artifact_from_dict(
            _mapping(profile, "legacy Full profile evidence")["completion"],
            "legacy Full profile completion",
        ).bytes
    return VerifiedLegacyFullSnapshot(
        authority=I7_PRODUCTION_AUTHORITY,
        release_id=candidate_id,
        cutoff_session=cutoff_session,
        producer_available_session=producer_available_session,
        producer_replay_declared_bytes=(
            completion_artifact.bytes
            + candidate_pin.bytes
            + candidate_output_bytes
            + direct_control_bytes
            + _positive_int(binding.declared_source_bytes, "legacy declared source bytes")
        ),
        bronze_source_binding_digest=bronze,
        s4_source_binding_digest=s4,
        schema_bundle_digest=_canonical_schema_bundle_digest(),
        native_transform_lineage_id=legacy_transform_lineage,
        native_identity_policy_lineage_id=binding.six_release_binding_id,
        identity_policy_bundle_id=policy,
        calendar_digest=binding.calendar_artifact_id,
        completion_artifact=completion_artifact,
        producer_verification_artifact=candidate_pin,
        partitions=ordered,
    )


def _streaming_source_projection(binding: object) -> tuple[str, str, str]:
    source_release_pins = {key: dict(value) for key, value in binding.source_release_pins.items()}
    registry_ids = {item.registry_name: item.release_id for item in binding.registry_pins}
    return (
        stable_digest(
            {
                "source_release_pins": source_release_pins,
                "six_release_binding_id": binding.six_release_binding_id,
            }
        ),
        stable_digest(
            {
                "s4_release_set_id": binding.s4_release_set_id,
                "s4_release_set_manifest": binding.s4_release_set_manifest.to_dict(),
            }
        ),
        cross_producer_identity_policy_digest(registry_ids),
    )


def _validate_loaded_authorities(
    *,
    config: I7ReconciliationConfig,
    authority: str,
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
) -> None:
    if incremental.authority != authority or full.authority != authority:
        raise I7ReconciliationRuntimeError("I7 loader authority crossed production/fixture")
    if (
        incremental.top_pointer_artifact != config.incremental_top_pointer_artifact
        or incremental.gate_c_approval_artifact != config.gate_c_approval_artifact
        or incremental.checkpoint_base_compaction_completion_artifact
        != config.checkpoint_compaction_completion_artifact
        or full.completion_artifact != config.independent_full_completion_artifact
    ):
        raise I7ReconciliationRuntimeError("I7 loader exact input pins differ")
    if (
        incremental.cutoff_session != config.cutoff_session
        or full.cutoff_session != config.cutoff_session
    ):
        raise I7ReconciliationRuntimeError("I7 producer cutoff differs")
    if config.receipt_available_session < max(
        incremental.producer_available_session,
        full.producer_available_session,
    ):
        raise I7ReconciliationRuntimeError("I7 receipt availability predates a producer")
    if incremental.release_id == full.release_id:
        raise I7ReconciliationRuntimeError("I7 Full is not independently identified")
    for left, right, label in (
        (
            incremental.bronze_source_binding_digest,
            full.bronze_source_binding_digest,
            "Bronze source binding",
        ),
        (
            incremental.s4_source_binding_digest,
            full.s4_source_binding_digest,
            "S4 source binding",
        ),
        (
            incremental.identity_policy_bundle_id,
            full.identity_policy_bundle_id,
            "identity policy bundle",
        ),
        (incremental.calendar_digest, full.calendar_digest, "calendar"),
    ):
        if left != right:
            raise I7ReconciliationRuntimeError(f"I7 producer {label} differs")
    if authority == I7_PRODUCTION_AUTHORITY:
        if incremental.schema_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I7ReconciliationRuntimeError("I7 incremental native-v2 schema bundle differs")
        if full.schema_bundle_digest != _canonical_schema_bundle_digest():
            raise I7ReconciliationRuntimeError("I7 legacy native-v1 schema bundle differs")
        if any(
            item.projection_kind is not CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2
            for item in incremental.resolved_partitions
        ):
            raise I7ReconciliationRuntimeError("I7 incremental native projection kind differs")
        if any(
            item.projection_kind is not CrossProducerProjectionKind.LEGACY_NATIVE_V1
            for item in full.partitions
        ):
            raise I7ReconciliationRuntimeError("I7 legacy native projection kind differs")
    if any(
        item.native_release_id != incremental.checkpoint_base_native_v2_release_id
        for item in incremental.resolved_partitions
    ) or any(
        item.native_release_id != incremental.native_v2_release_id
        for item in incremental.checkpoint_before_partitions
    ):
        raise I7ReconciliationRuntimeError("I7 incremental native release lineage differs")
    if any(item.native_release_id != full.release_id for item in full.partitions):
        raise I7ReconciliationRuntimeError("I7 legacy native release lineage differs")
    if _partition_scope(incremental.resolved_partitions) != _partition_scope(full.partitions):
        raise I7ReconciliationRuntimeError("I7 producer partition scope differs")


def _build_run_spec(
    config: I7ReconciliationConfig,
    *,
    authority: str,
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
) -> I7ReconciliationRunSpec:
    scopes: list[FullReconciliationTableScope] = []
    by_table = _partitions_by_table(incremental.resolved_partitions)
    for table_name in I7_TABLE_ORDER:
        scopes.append(
            FullReconciliationTableScope(
                table_name=table_name,
                partition_keys=tuple(item.partition_key for item in by_table[table_name]),
            )
        )
    canonical_digest = stable_digest(
        {
            "cross_producer_projection_contracts": {
                table: cross_producer_projection_contract(table).to_dict()
                for table in I7_TABLE_ORDER
            },
            "rule_version": I7_CANONICAL_PROJECTION_RULE_VERSION,
            "table_order": list(I7_TABLE_ORDER),
        }
    )
    checkpoint_digest = stable_digest(
        {
            "completion_artifact": (
                incremental.checkpoint_base_compaction_completion_artifact.to_dict()
            ),
            "compacted_checkpoint_id": incremental.checkpoint_base_checkpoint_id,
            "compacted_native_v2_release_id": (incremental.checkpoint_base_native_v2_release_id),
            "proof_artifact": incremental.checkpoint_base_compaction_proof_artifact.to_dict(),
            "rule_version": I7_CHECKPOINT_REBASE_RULE_VERSION,
            "source_checkpoint_id": incremental.checkpoint_id,
            "source_native_v2_release_id": incremental.native_v2_release_id,
        }
    )
    trigger_digest = stable_digest(
        {
            "cadence": config.cadence.value,
            "cutoff_session": config.cutoff_session.isoformat(),
            "rule_version": I7_TRIGGER_POLICY_RULE_VERSION,
            "trigger_kind": config.trigger_kind.value,
            "trigger_reason": config.trigger_reason,
        }
    )
    lifecycle = FullReconciliationSpec(
        incremental_top_release_id=incremental.release_id,
        independent_full_candidate_release_id=full.release_id,
        bronze_source_binding_digest=incremental.bronze_source_binding_digest,
        s4_source_binding_digest=incremental.s4_source_binding_digest,
        schema_bundle_digest=_cross_producer_schema_bundle_digest(incremental, full),
        transform_semantics_digest=cross_producer_transform_semantics_digest(
            incremental.transform_semantics_digest,
            full.native_transform_lineage_id,
        ),
        identity_policy_bundle_id=incremental.identity_policy_bundle_id,
        calendar_digest=incremental.calendar_digest,
        view=ViewKind.LATEST_REVIEWED_RESEARCH,
        reconciliation_cutoff_session=config.cutoff_session,
        canonical_projection_semantics_digest=canonical_digest,
        checkpoint_rebase_semantics_digest=checkpoint_digest,
        trigger_policy_digest=trigger_digest,
        table_scopes=tuple(scopes),
        cadence=config.cadence,
    )
    return I7ReconciliationRunSpec(
        authority=authority,
        incremental=incremental,
        independent_full=full,
        lifecycle_spec=lifecycle,
        receipt_available_session=config.receipt_available_session,
        trigger_kind=config.trigger_kind,
        trigger_reason=config.trigger_reason,
        resource_policy=config.resource_policy,
    )


def _compare_all_partitions(
    root: Path,
    *,
    spec: I7ReconciliationRunSpec,
    meter: _ReadMeter,
    write_documents: bool = True,
    resource_tracker: _RuntimeResourceTracker | None = None,
) -> tuple[
    tuple[FullReconciliationTableEvidence, ...],
    tuple[tuple[ArtifactPin, dict[str, object]], ...],
    str,
    str,
]:
    incremental = _partition_map(spec.incremental.resolved_partitions)
    before = _partition_map(spec.incremental.checkpoint_before_partitions)
    full = _partition_map(spec.independent_full.partitions)
    if set(incremental) != set(full) or set(incremental) != set(before):
        raise I7ReconciliationRuntimeError("I7 partition scope changed after prepare")
    production = spec.authority == I7_PRODUCTION_AUTHORITY
    table_evidence: list[FullReconciliationTableEvidence] = []
    details: list[tuple[ArtifactPin, dict[str, object]]] = []
    checkpoint_before_parts: list[dict[str, str]] = []
    checkpoint_rebased_parts: list[dict[str, str]] = []
    by_table: dict[str, list[FullReconciliationPartitionEvidence]] = {
        table: [] for table in I7_TABLE_ORDER
    }
    projection_context = _load_incremental_projection_context(root, spec=spec, meter=meter)
    if resource_tracker is not None:
        resource_tracker.check("projection_context")
    for table_name, partition_key in sorted(incremental, key=_scope_sort_key):
        current_rows = _read_canonical_partition(
            root,
            incremental[(table_name, partition_key)],
            meter,
            projection_context=projection_context,
        )
        before_rows = _read_canonical_partition(
            root,
            before[(table_name, partition_key)],
            meter,
            projection_context=projection_context,
        )
        full_rows = _read_canonical_partition(
            root,
            full[(table_name, partition_key)],
            meter,
            projection_context=projection_context,
        )
        current_digest = _row_projection_digest(table_name, partition_key, current_rows)
        before_digest = _row_projection_digest(table_name, partition_key, before_rows)
        full_digest = _row_projection_digest(table_name, partition_key, full_rows)
        differences = _unexpected_difference_count(current_rows, full_rows)
        checkpoint_differences = _unexpected_difference_count(before_rows, current_rows)
        detail = {
            "artifact_type": "s7_5_i7_partition_reconciliation_details",
            "checkpoint_before_projection_digest": before_digest,
            "checkpoint_rebased_projection_digest": current_digest,
            "checkpoint_unexpected_difference_count": checkpoint_differences,
            "full_physical_digest": full[(table_name, partition_key)].physical_digest,
            "full_projection_digest": full_digest,
            "incremental_physical_digest": incremental[(table_name, partition_key)].physical_digest,
            "incremental_projection_digest": current_digest,
            "partition_key": partition_key,
            "publish_authorized": False,
            "rule_version": I7_RUNTIME_RULE_VERSION,
            "state": I7_STATE,
            "table_name": table_name,
            "unexpected_difference_count": differences,
        }
        detail_pin = _document_pin(
            _partition_details_path(
                spec.run_spec_id,
                table_name,
                partition_key,
                production=production,
            ),
            detail,
        )
        evidence = FullReconciliationPartitionEvidence(
            table_name=table_name,
            partition_key=partition_key,
            compared_row_count=len(current_rows),
            incremental_projection_digest=current_digest,
            full_projection_digest=full_digest,
            unexpected_difference_count=differences,
            details_artifact=detail_pin,
        )
        by_table[table_name].append(evidence)
        if write_documents:
            details.append((detail_pin, detail))
        checkpoint_before_parts.append(
            {"partition_key": f"{table_name}/{partition_key}", "projection_digest": before_digest}
        )
        checkpoint_rebased_parts.append(
            {"partition_key": f"{table_name}/{partition_key}", "projection_digest": current_digest}
        )
        if resource_tracker is not None:
            resource_tracker.check(f"partition_{table_name}_{stable_digest(partition_key)[:12]}")
    for table_name in I7_TABLE_ORDER:
        table_evidence.append(
            FullReconciliationTableEvidence(
                table_name=table_name,
                semantics_digest=spec.lifecycle_spec.table_semantics_digest(table_name),
                partitions=tuple(by_table[table_name]),
            )
        )
    return (
        tuple(table_evidence),
        tuple(details),
        stable_digest(checkpoint_before_parts),
        stable_digest(checkpoint_rebased_parts),
    )


def _read_canonical_partition(
    root: Path,
    partition: ReconciliationPartition,
    meter: _ReadMeter,
    *,
    projection_context: _IncrementalProjectionContext | None,
) -> tuple[dict[str, object], ...]:
    contract = S7_DERIVED_CONTRACTS[partition.table_name]
    projection = cross_producer_projection_contract(partition.table_name)
    if partition.projection_contract_digest != projection.contract_digest:
        raise I7ReconciliationRuntimeError("I7 projection contract changed")
    if partition.projection_kind is CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2:
        if projection_context is None:
            raise I7ReconciliationRuntimeError("I7 native-v2 projection context is absent")
        if partition.schema_digest != projection.incremental_native_schema_digest:
            raise I7ReconciliationRuntimeError("I7 native-v2 schema lineage differs")
        if partition.table_name != "universe_daily":
            if partition.artifact.path.endswith(".parquet"):
                content = meter.read_pin(
                    partition.artifact,
                    label="I7 checkpoint-base native-v2 Parquet",
                )
                try:
                    parquet = pq.ParquetFile(pa.BufferReader(content))
                    if (
                        parquet.schema_arrow != I3_V2_CONTRACTS[partition.table_name].arrow_schema
                        or parquet.metadata.num_rows != partition.row_count
                    ):
                        raise I7ReconciliationRuntimeError(
                            "I7 checkpoint-base Parquet schema or row count differs"
                        )
                    native_rows = tuple(dict(row) for row in parquet.read().to_pylist())
                except I7ReconciliationRuntimeError:
                    raise
                except (OSError, pa.ArrowException) as exc:
                    raise I7ReconciliationRuntimeError(
                        "I7 checkpoint-base artifact is not readable Parquet"
                    ) from exc
                if partition.table_name == "ticker_alias":
                    native_rows = tuple(row for row in native_rows if not row["alias_is_tombstone"])
                return project_cross_producer_rows(
                    partition.table_name,
                    partition.projection_kind,
                    native_rows,
                    alias_reverse=projection_context.alias_reverse,
                )
            rows = projection_context.small_rows.get(partition.table_name)
            if rows is None:
                raise I7ReconciliationRuntimeError("I7 small-table projection is absent")
            return rows
        content = meter.read_pin(partition.artifact, label="I7 native-v2 universe Parquet")
        native_schema = I3_V2_CONTRACTS[partition.table_name].arrow_schema
        try:
            parquet = pq.ParquetFile(pa.BufferReader(content))
            if (
                parquet.schema_arrow != native_schema
                or parquet.metadata.num_rows != partition.row_count
            ):
                raise I7ReconciliationRuntimeError(
                    "I7 native-v2 Parquet schema or row count differs"
                )
            native_rows = tuple(dict(row) for row in parquet.read().to_pylist())
        except I7ReconciliationRuntimeError:
            raise
        except (OSError, pa.ArrowException) as exc:
            raise I7ReconciliationRuntimeError(
                "I7 native-v2 artifact is not readable Parquet"
            ) from exc
        return project_cross_producer_rows(
            partition.table_name,
            partition.projection_kind,
            native_rows,
            alias_reverse=projection_context.alias_reverse,
        )
    if partition.schema_digest != projection.legacy_native_schema_digest:
        raise I7ReconciliationRuntimeError("I7 legacy-v1 schema lineage differs")
    content = meter.read_pin(partition.artifact, label="I7 canonical-v1 Parquet")
    try:
        parquet = pq.ParquetFile(pa.BufferReader(content))
        if (
            parquet.schema_arrow != contract.arrow_schema
            or parquet.metadata.num_rows != partition.row_count
        ):
            raise I7ReconciliationRuntimeError("I7 Parquet schema or row count differs")
        table = parquet.read()
    except I7ReconciliationRuntimeError:
        raise
    except (OSError, pa.ArrowException) as exc:
        raise I7ReconciliationRuntimeError("I7 canonical artifact is not readable Parquet") from exc
    rows = project_cross_producer_rows(
        partition.table_name,
        partition.projection_kind,
        tuple(dict(row) for row in table.to_pylist()),
    )
    if partition.table_name == "universe_daily":
        expected = date.fromisoformat(partition.partition_key)
        if any(row["session_date"] != expected.isoformat() for row in rows):
            raise I7ReconciliationRuntimeError("I7 universe partition crosses sessions")
    return tuple(rows)


def _load_incremental_projection_context(
    root: Path,
    *,
    spec: I7ReconciliationRunSpec,
    meter: _ReadMeter,
) -> _IncrementalProjectionContext | None:
    native = any(
        item.projection_kind is CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2
        for item in spec.incremental.resolved_partitions
    )
    if not native:
        return None
    try:
        loaded = verify_i3_production_deep_attestation(
            root,
            spec.incremental.release_completion_artifact,
            spec.incremental.producer_verification_artifact,
            expected_kind=I3ProductionRunKind.DELTA,
        )
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I6 seam: native-v2 projection authority cannot be replayed"
        ) from exc
    if (
        loaded.gate_a_manifest.release_id != spec.incremental.release_id
        or loaded.manifest.release_id != spec.incremental.native_v2_release_id
        or loaded.checkpoint.checkpoint_id != spec.incremental.checkpoint_id
        or loaded.checkpoint.resolved_state_digest != spec.incremental.resolved_state_digest
        or loaded.receipt.output_set is None
        or loaded.receipt.output_set.resolved_content_digest
        != spec.incremental.resolved_content_digest
    ):
        raise I7ReconciliationRuntimeError("I7 native-v2 projection authority differs")
    try:
        alias_native = _resolved_incremental_small_rows(
            root,
            loaded=loaded,
            table_name="ticker_alias",
            meter=meter,
        )
        alias_reverse = _legacy_alias_reverse(alias_native)
        small = {
            table_name: project_cross_producer_rows(
                table_name,
                CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
                (
                    alias_native
                    if table_name == "ticker_alias"
                    else _resolved_incremental_small_rows(
                        root,
                        loaded=loaded,
                        table_name=table_name,
                        meter=meter,
                    )
                ),
                alias_reverse=alias_reverse,
            )
            for table_name in ("asset_master", "ticker_alias", "issuer_master")
        }
    except I5ShadowRuntimeError as exc:
        raise I7ReconciliationRuntimeError("I7 native-v2 small-table projection failed") from exc
    return _IncrementalProjectionContext(
        loaded=loaded,
        alias_reverse=alias_reverse,
        small_rows=MappingProxyType(small),
    )


def _canonical_row(table_name: str, value: Mapping[str, object]) -> dict[str, object]:
    fields = tuple(S7_DERIVED_CONTRACTS[table_name].arrow_schema.names)
    if set(value) != set(fields):
        raise I7ReconciliationRuntimeError("I7 canonical row fields differ")
    return {field: _json_value(value[field]) for field in fields}


def _unexpected_difference_count(
    left: Sequence[Mapping[str, object]], right: Sequence[Mapping[str, object]]
) -> int:
    left_bytes = {_canonical_json_bytes(dict(row)) for row in left}
    right_bytes = {_canonical_json_bytes(dict(row)) for row in right}
    return len(left_bytes.symmetric_difference(right_bytes))


def _row_projection_digest(
    table_name: str,
    partition_key: str,
    rows: Sequence[Mapping[str, object]],
) -> str:
    return stable_digest(
        {
            "partition_key": partition_key,
            "projection_contract_digest": cross_producer_projection_contract(
                table_name
            ).contract_digest,
            "rows": list(rows),
            "rule_version": I7_CANONICAL_PROJECTION_RULE_VERSION,
            "table_name": table_name,
        }
    )


def _read_required_authority_artifacts(
    root: Path,
    *,
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
    meter: _ReadMeter,
) -> None:
    del root
    for pin, label in (
        (incremental.top_pointer_artifact, "research top pointer"),
        (incremental.gate_c_approval_artifact, "Gate C approval"),
        (incremental.producer_verification_artifact, "incremental producer verification"),
        (incremental.release_completion_artifact, "incremental release completion"),
        (
            incremental.checkpoint_base_compaction_completion_artifact,
            "checkpoint/base compaction completion",
        ),
        (
            incremental.checkpoint_base_compaction_proof_artifact,
            "checkpoint/base compaction proof",
        ),
        (full.completion_artifact, "legacy Full completion"),
        (full.producer_verification_artifact, "legacy Full producer verification"),
    ):
        meter.read_pin(pin, label=label)


def _trigger_document(spec: I7ReconciliationRunSpec) -> dict[str, object]:
    cadence = spec.lifecycle_spec.cadence
    cutoff = spec.lifecycle_spec.reconciliation_cutoff_session
    bucket = (
        cutoff.strftime("%Y-%m")
        if cadence is ReconciliationCadence.MONTHLY
        else f"{cutoff.year}-Q{((cutoff.month - 1) // 3) + 1}"
    )
    payload = {
        "artifact_type": "s7_5_i7_reconciliation_trigger",
        "automatic_publish_authorized": False,
        "cadence": cadence.value,
        "cadence_bucket": bucket,
        "cutoff_session": cutoff.isoformat(),
        "incremental_top_release_id": spec.incremental.release_id,
        "independent_full_candidate_release_id": spec.independent_full.release_id,
        "publish_authorized": False,
        "rule_version": I7_TRIGGER_RULE_VERSION,
        "run_spec_id": spec.run_spec_id,
        "state": I7_STATE,
        "trigger_kind": spec.trigger_kind.value,
        "trigger_reason": spec.trigger_reason,
        "trigger_policy_digest": spec.lifecycle_spec.trigger_policy_digest,
    }
    return {"trigger_id": stable_digest(payload), **payload}


def _alert_document(
    spec: I7ReconciliationRunSpec,
    *,
    unexpected_difference_count: int,
    checkpoint_drift: bool,
) -> dict[str, object]:
    failed = unexpected_difference_count != 0 or checkpoint_drift
    payload = {
        "artifact_type": "s7_5_i7_reconciliation_alert",
        "automatic_publish_authorized": False,
        "checkpoint_logical_drift": checkpoint_drift,
        "cutoff_session": spec.lifecycle_spec.reconciliation_cutoff_session.isoformat(),
        "outcome": "failed" if failed else "passed",
        "publish_authorized": False,
        "requires_review": True,
        "rule_version": I7_ALERT_RULE_VERSION,
        "run_spec_id": spec.run_spec_id,
        "severity": "critical" if failed else "info",
        "state": I7_STATE,
        "unexpected_difference_count": unexpected_difference_count,
    }
    return {"alert_id": stable_digest(payload), **payload}


def _aggregate_details_document(
    spec: I7ReconciliationRunSpec,
    *,
    table_evidence: tuple[FullReconciliationTableEvidence, ...],
    checkpoint_before: str,
    checkpoint_rebased: str,
) -> dict[str, object]:
    return {
        "artifact_type": "s7_5_i7_full_reconciliation_details",
        "checkpoint_base_compaction_proof_artifact": (
            spec.incremental.checkpoint_base_compaction_proof_artifact.to_dict()
        ),
        "checkpoint_base_compaction_completion_artifact": (
            spec.incremental.checkpoint_base_compaction_completion_artifact.to_dict()
        ),
        "checkpoint_base_native_v2_release_id": (
            spec.incremental.checkpoint_base_native_v2_release_id
        ),
        "checkpoint_base_checkpoint_id": spec.incremental.checkpoint_base_checkpoint_id,
        "checkpoint_before_projection_digest": checkpoint_before,
        "checkpoint_rebased_projection_digest": checkpoint_rebased,
        "full_oracle_official_verification_artifact": (
            spec.independent_full.producer_verification_artifact.to_dict()
        ),
        "cross_producer_projection_contracts": {
            table: cross_producer_projection_contract(table).to_dict() for table in I7_TABLE_ORDER
        },
        "incremental_native_v2_release_id": spec.incremental.native_v2_release_id,
        "incremental_native_schema_bundle_digest": spec.incremental.schema_bundle_digest,
        "legacy_native_v1_release_id": spec.independent_full.release_id,
        "legacy_native_schema_bundle_digest": spec.independent_full.schema_bundle_digest,
        "incremental_gate_c_approval_artifact": (
            spec.incremental.gate_c_approval_artifact.to_dict()
        ),
        "incremental_top_pointer_artifact": spec.incremental.top_pointer_artifact.to_dict(),
        "publish_authorized": False,
        "rule_version": I7_RUNTIME_RULE_VERSION,
        "run_spec_id": spec.run_spec_id,
        "state": I7_STATE,
        "table_evidence": [item.to_dict() for item in table_evidence],
    }


def _qa_document(
    spec: I7ReconciliationRunSpec,
    *,
    table_evidence: tuple[FullReconciliationTableEvidence, ...],
    checkpoint_before: str,
    checkpoint_rebased: str,
) -> dict[str, object]:
    differences = sum(item.unexpected_difference_count for item in table_evidence)
    return {
        "artifact_type": "s7_5_i7_full_reconciliation_qa",
        "checkpoint_logical_drift_rows": 0 if checkpoint_before == checkpoint_rebased else 1,
        "compared_partition_count": sum(len(item.partitions) for item in table_evidence),
        "compared_row_count": sum(item.compared_row_count for item in table_evidence),
        "critical_failure_count": (
            0 if differences == 0 and checkpoint_before == checkpoint_rebased else 1
        ),
        "missing_partition_count": 0,
        "publish_authorized": False,
        "rule_version": I7_RUNTIME_RULE_VERSION,
        "run_spec_id": spec.run_spec_id,
        "state": I7_STATE,
        "unexpected_difference_count": differences,
    }


def _load_run_spec_exact(
    root: Path,
    artifact: ArtifactPin,
    *,
    authority: str,
    meter: _ReadMeter,
) -> I7ReconciliationRunSpec:
    locator_id: str | None = None
    if authority == I7_PRODUCTION_AUTHORITY:
        locator_id = _validate_production_control_locator(artifact, kind="run_spec")
    content = meter.read_pin(artifact, label="I7 RunSpec")
    document = _closed_json(content, "I7 RunSpec")
    spec = _run_spec_from_dict(document)
    production = authority == I7_PRODUCTION_AUTHORITY
    if spec.authority != authority:
        raise I7ReconciliationRuntimeError("I7 RunSpec authority differs")
    if locator_id is not None and spec.run_spec_id != locator_id:
        raise I7ReconciliationRuntimeError("I7 RunSpec directory ID differs")
    if artifact.path != _run_spec_path(spec.run_spec_id, production=production):
        raise I7ReconciliationRuntimeError("I7 RunSpec path is noncanonical")
    if spec.canonical_bytes() != content:
        raise I7ReconciliationRuntimeError("I7 RunSpec bytes do not reproduce")
    return spec


def _run_spec_from_dict(value: Mapping[str, object]) -> I7ReconciliationRunSpec:
    item = _mapping(value, "I7 RunSpec")
    _keys(
        item,
        {
            "authority",
            "incremental",
            "independent_full",
            "lifecycle_spec",
            "receipt_available_session",
            "resource_policy",
            "rule_version",
            "run_spec_id",
            "trigger_kind",
            "trigger_reason",
        },
        "I7 RunSpec",
    )
    if item["rule_version"] != I7_RUN_SPEC_RULE_VERSION:
        raise I7ReconciliationRuntimeError("I7 RunSpec rule version differs")
    result = I7ReconciliationRunSpec(
        authority=_text(item["authority"], "I7 authority"),
        incremental=VerifiedIncrementalTopSnapshot.from_dict(item["incremental"]),
        independent_full=VerifiedLegacyFullSnapshot.from_dict(item["independent_full"]),
        lifecycle_spec=_lifecycle_spec_from_dict(item["lifecycle_spec"]),
        receipt_available_session=date.fromisoformat(
            _text(item["receipt_available_session"], "I7 availability")
        ),
        trigger_kind=ReconciliationTriggerKind(_text(item["trigger_kind"], "I7 trigger kind")),
        trigger_reason=_text(item["trigger_reason"], "I7 trigger reason"),
        resource_policy=_resource_policy_from_dict(item["resource_policy"]),
    )
    if item["run_spec_id"] != result.run_spec_id:
        raise I7ReconciliationRuntimeError("I7 RunSpec ID differs")
    return result


def _lifecycle_spec_from_dict(value: object) -> FullReconciliationSpec:
    item = _mapping(value, "I7 lifecycle spec")
    expected = {
        "bronze_source_binding_digest",
        "cadence",
        "calendar_digest",
        "canonical_projection_semantics_digest",
        "checkpoint_rebase_semantics_digest",
        "identity_policy_bundle_id",
        "incremental_top_release_id",
        "independent_full_candidate_release_id",
        "reconciliation_cutoff_session",
        "rule_version",
        "s4_source_binding_digest",
        "schema_bundle_digest",
        "spec_id",
        "table_scopes",
        "transform_semantics_digest",
        "trigger_policy_digest",
        "view",
    }
    _keys(item, expected, "I7 lifecycle spec")
    if item["rule_version"] != "s7_5_i7_full_reconciliation_spec_v1":
        raise I7ReconciliationRuntimeError("I7 lifecycle rule version differs")
    scopes = []
    for value in _sequence(item["table_scopes"], "I7 table scopes"):
        row = _mapping(value, "I7 table scope")
        _keys(row, {"partition_keys", "table_name"}, "I7 table scope")
        scopes.append(
            FullReconciliationTableScope(
                table_name=_text(row["table_name"], "I7 scope table"),
                partition_keys=tuple(
                    _text(value, "I7 partition key")
                    for value in _sequence(row["partition_keys"], "I7 partition keys")
                ),
            )
        )
    result = FullReconciliationSpec(
        incremental_top_release_id=_digest(
            item["incremental_top_release_id"], "I7 lifecycle incremental release"
        ),
        independent_full_candidate_release_id=_digest(
            item["independent_full_candidate_release_id"], "I7 lifecycle Full release"
        ),
        bronze_source_binding_digest=_digest(
            item["bronze_source_binding_digest"], "I7 lifecycle Bronze binding"
        ),
        s4_source_binding_digest=_digest(
            item["s4_source_binding_digest"], "I7 lifecycle S4 binding"
        ),
        schema_bundle_digest=_digest(item["schema_bundle_digest"], "I7 lifecycle schema"),
        transform_semantics_digest=_digest(
            item["transform_semantics_digest"], "I7 lifecycle transform"
        ),
        identity_policy_bundle_id=_digest(item["identity_policy_bundle_id"], "I7 lifecycle policy"),
        calendar_digest=_digest(item["calendar_digest"], "I7 lifecycle calendar"),
        view=ViewKind(_text(item["view"], "I7 lifecycle view")),
        reconciliation_cutoff_session=date.fromisoformat(
            _text(item["reconciliation_cutoff_session"], "I7 lifecycle cutoff")
        ),
        canonical_projection_semantics_digest=_digest(
            item["canonical_projection_semantics_digest"], "I7 canonical semantics"
        ),
        checkpoint_rebase_semantics_digest=_digest(
            item["checkpoint_rebase_semantics_digest"], "I7 checkpoint semantics"
        ),
        trigger_policy_digest=_digest(item["trigger_policy_digest"], "I7 trigger policy"),
        table_scopes=tuple(scopes),
        cadence=ReconciliationCadence(_text(item["cadence"], "I7 cadence")),
    )
    if item["spec_id"] != result.spec_id:
        raise I7ReconciliationRuntimeError("I7 lifecycle spec ID differs")
    return result


def _completion_from_dict(value: Mapping[str, object]) -> I7ReconciliationCompletion:
    item = _mapping(value, "I7 completion")
    _keys(
        item,
        {
            "alert_artifact",
            "artifact_type",
            "authority",
            "automatic_publish_authorized",
            "completion_id",
            "publish_authorized",
            "receipt",
            "resource_observation",
            "rule_version",
            "run_spec_artifact",
            "run_spec_id",
            "s7_5_complete",
            "state",
            "trigger_artifact",
        },
        "I7 completion",
    )
    if (
        item["artifact_type"] != "s7_5_i7_full_reconciliation_completion"
        or item["automatic_publish_authorized"] is not False
        or item["publish_authorized"] is not False
        or item["s7_5_complete"] is not False
        or item["rule_version"] != I7_COMPLETION_RULE_VERSION
        or item["state"] != I7_STATE
    ):
        raise I7ReconciliationRuntimeError("I7 completion semantics differ")
    result = I7ReconciliationCompletion(
        run_spec_id=_digest(item["run_spec_id"], "I7 completion RunSpec ID"),
        run_spec_artifact=_artifact_from_dict(item["run_spec_artifact"], "I7 RunSpec"),
        trigger_artifact=_artifact_from_dict(item["trigger_artifact"], "I7 trigger"),
        alert_artifact=_artifact_from_dict(item["alert_artifact"], "I7 alert"),
        receipt=_full_receipt_from_dict(item["receipt"]),
        resource_observation=_resource_observation_from_dict(item["resource_observation"]),
        authority=_text(item["authority"], "I7 completion authority"),
    )
    if item["completion_id"] != result.completion_id:
        raise I7ReconciliationRuntimeError("I7 completion ID differs")
    return result


def _resource_observation_from_dict(value: object) -> I7RuntimeResourceObservation:
    item = _mapping(value, "I7 resource observation")
    _keys(
        item,
        {
            "declared_read_bytes",
            "estimated_write_bytes",
            "metered_read_bytes",
            "minimum_free_disk_bytes",
            "observed_write_bytes",
            "peak_rss_bytes",
            "phase_minimum_free_disk_bytes",
            "phase_peak_rss_bytes",
            "process_read_bytes",
            "process_write_bytes",
            "rule_version",
            "wall_clock_seconds",
        },
        "I7 resource observation",
    )
    if item["rule_version"] != I7_RESOURCE_OBSERVATION_RULE_VERSION:
        raise I7ReconciliationRuntimeError("I7 resource observation rule differs")
    disk_phases = []
    for value in _sequence(item["phase_minimum_free_disk_bytes"], "resource phases"):
        phase = _mapping(value, "resource phase")
        _keys(phase, {"free_disk_bytes", "phase"}, "resource phase")
        disk_phases.append(
            (
                _text(phase["phase"], "resource phase name"),
                _nonnegative_int(phase["free_disk_bytes"], "resource phase disk bytes"),
            )
        )
    rss_phases = []
    for value in _sequence(item["phase_peak_rss_bytes"], "resource RSS phases"):
        phase = _mapping(value, "resource RSS phase")
        _keys(phase, {"peak_rss_bytes", "phase"}, "resource RSS phase")
        rss_phases.append(
            (
                _text(phase["phase"], "resource RSS phase name"),
                _nonnegative_int(phase["peak_rss_bytes"], "resource phase RSS bytes"),
            )
        )
    result = I7RuntimeResourceObservation(
        declared_read_bytes=_nonnegative_int(item["declared_read_bytes"], "declared read bytes"),
        estimated_write_bytes=_nonnegative_int(
            item["estimated_write_bytes"], "estimated write bytes"
        ),
        metered_read_bytes=_nonnegative_int(item["metered_read_bytes"], "metered read bytes"),
        process_read_bytes=_nonnegative_int(item["process_read_bytes"], "process read bytes"),
        process_write_bytes=_nonnegative_int(item["process_write_bytes"], "process write bytes"),
        observed_write_bytes=_nonnegative_int(item["observed_write_bytes"], "observed write bytes"),
        peak_rss_bytes=_nonnegative_int(item["peak_rss_bytes"], "peak RSS bytes"),
        minimum_free_disk_bytes=_nonnegative_int(
            item["minimum_free_disk_bytes"], "minimum free disk bytes"
        ),
        wall_clock_seconds=_nonnegative_int(item["wall_clock_seconds"], "wall-clock seconds"),
        phase_minimum_free_disk_bytes=tuple(disk_phases),
        phase_peak_rss_bytes=tuple(rss_phases),
    )
    if result.to_dict() != item:
        raise I7ReconciliationRuntimeError("I7 resource observation does not reproduce")
    return result


def _full_receipt_from_dict(value: object) -> FullReconciliationReceipt:
    item = _mapping(value, "I7 receipt")
    # Derived summary fields are replayed by the lifecycle dataclass and validator.
    evidence = []
    for table_value in _sequence(item["table_evidence"], "I7 table evidence"):
        table = _mapping(table_value, "I7 table evidence")
        partitions = []
        for part_value in _sequence(table["partitions"], "I7 partition evidence"):
            part = _mapping(part_value, "I7 partition evidence")
            partitions.append(
                FullReconciliationPartitionEvidence(
                    table_name=_text(part["table_name"], "evidence table"),
                    partition_key=_text(part["partition_key"], "evidence partition"),
                    compared_row_count=_positive_int(
                        part["compared_row_count"], "evidence row count"
                    ),
                    incremental_projection_digest=_digest(
                        part["incremental_projection_digest"], "incremental projection"
                    ),
                    full_projection_digest=_digest(
                        part["full_projection_digest"], "Full projection"
                    ),
                    unexpected_difference_count=_nonnegative_int(
                        part["unexpected_difference_count"], "unexpected differences"
                    ),
                    details_artifact=_artifact_from_dict(
                        part["details_artifact"], "partition details"
                    ),
                )
            )
        evidence.append(
            FullReconciliationTableEvidence(
                table_name=_text(table["table_name"], "evidence table"),
                semantics_digest=_digest(table["semantics_digest"], "table semantics"),
                partitions=tuple(partitions),
            )
        )
    result = FullReconciliationReceipt(
        spec_id=_digest(item["spec_id"], "receipt spec ID"),
        incremental_top_release_id=_digest(
            item["incremental_top_release_id"], "receipt incremental release"
        ),
        independent_full_candidate_release_id=_digest(
            item["independent_full_candidate_release_id"], "receipt Full release"
        ),
        table_evidence=tuple(evidence),
        checkpoint_before_projection_digest=_digest(
            item["checkpoint_before_projection_digest"], "receipt checkpoint before"
        ),
        checkpoint_rebased_projection_digest=_digest(
            item["checkpoint_rebased_projection_digest"], "receipt checkpoint rebased"
        ),
        qa_artifact=_artifact_from_dict(item["qa_artifact"], "receipt QA"),
        details_artifact=_artifact_from_dict(item["details_artifact"], "receipt details"),
        receipt_available_session=date.fromisoformat(
            _text(item["receipt_available_session"], "receipt availability")
        ),
    )
    if item != result.to_dict():
        raise I7ReconciliationRuntimeError("I7 receipt derived summaries differ")
    return result


def _resource_policy_from_dict(value: object) -> ResourceGatePolicy:
    item = _mapping(value, "I7 resource policy")
    result = ResourceGatePolicy(
        max_wall_clock_seconds=_positive_int(item["max_wall_clock_seconds"], "maximum wall clock"),
        max_peak_rss_bytes=_positive_int(item["max_peak_rss_bytes"], "maximum RSS"),
        min_free_disk_bytes=_positive_int(item["min_free_disk_bytes"], "disk floor"),
        max_read_bytes=_positive_int(item["max_read_bytes"], "maximum read bytes"),
        max_write_bytes=_positive_int(item["max_write_bytes"], "maximum write bytes"),
        max_chain_resolution_milliseconds=_positive_int(
            item["max_chain_resolution_milliseconds"], "maximum chain resolution"
        ),
    )
    if result.to_dict() != item:
        raise I7ReconciliationRuntimeError("I7 resource policy does not reproduce")
    return result


def _partition_set(partitions: tuple[ReconciliationPartition, ...], label: str) -> None:
    if not isinstance(partitions, tuple) or not partitions:
        raise I7ReconciliationRuntimeError(f"{label} is empty or untyped")
    if any(not isinstance(item, ReconciliationPartition) for item in partitions):
        raise I7ReconciliationRuntimeError(f"{label} contains an invalid record")
    keys = tuple((item.table_name, item.partition_key) for item in partitions)
    if keys != tuple(sorted(set(keys), key=_scope_sort_key)):
        raise I7ReconciliationRuntimeError(f"{label} must be closed, sorted, and unique")
    if tuple(dict.fromkeys(item.table_name for item in partitions)) != I7_TABLE_ORDER:
        raise I7ReconciliationRuntimeError(f"{label} does not cover the exact four tables")
    small = {table: 0 for table in I7_TABLE_ORDER[:-1]}
    for item in partitions:
        if item.table_name in small:
            small[item.table_name] += 1
    if any(count != 1 for count in small.values()):
        raise I7ReconciliationRuntimeError(f"{label} small-table partition count differs")


def _partition_scope(
    partitions: tuple[ReconciliationPartition, ...],
) -> tuple[tuple[str, str], ...]:
    return tuple((item.table_name, item.partition_key) for item in partitions)


def _partition_map(
    partitions: tuple[ReconciliationPartition, ...],
) -> dict[tuple[str, str], ReconciliationPartition]:
    return {(item.table_name, item.partition_key): item for item in partitions}


def _partitions_by_table(
    partitions: tuple[ReconciliationPartition, ...],
) -> Mapping[str, tuple[ReconciliationPartition, ...]]:
    return MappingProxyType(
        {
            table: tuple(item for item in partitions if item.table_name == table)
            for table in I7_TABLE_ORDER
        }
    )


def _partition_sort_key(value: ReconciliationPartition) -> tuple[int, str]:
    return (I7_TABLE_ORDER.index(value.table_name), value.partition_key)


def _scope_sort_key(value: tuple[str, str]) -> tuple[int, str]:
    return (I7_TABLE_ORDER.index(value[0]), value[1])


def _partition_key(table_name: str, value: str) -> str:
    text = _text(value, "partition key")
    if table_name == "universe_daily":
        try:
            if date.fromisoformat(text).isoformat() != text:
                raise ValueError
        except ValueError as exc:
            raise I7ReconciliationRuntimeError("universe partition key is invalid") from exc
    elif text != "__table__":
        raise I7ReconciliationRuntimeError("small-table partition key must be __table__")
    return text


def _canonical_schema_bundle_digest() -> str:
    return stable_digest(
        {
            "schemas": {
                table: S7_DERIVED_CONTRACTS[table].schema_digest for table in I7_TABLE_ORDER
            },
            "table_order": list(I7_TABLE_ORDER),
            "rule_version": "s7_5_i7_canonical_schema_bundle_v1",
        }
    )


def _cross_producer_schema_bundle_digest(
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
) -> str:
    return stable_digest(
        {
            "canonical_projection_contracts": {
                table: cross_producer_projection_contract(table).to_dict()
                for table in I7_TABLE_ORDER
            },
            "incremental_native_schema_bundle_digest": incremental.schema_bundle_digest,
            "legacy_native_schema_bundle_digest": full.schema_bundle_digest,
            "rule_version": "s7_5_i7_cross_producer_schema_lineage_bundle_v1",
        }
    )


def _run_spec_path(run_spec_id: str, *, production: bool) -> str:
    prefix = (
        "manifests/silver/incremental/i7/full-reconciliation-run-specs"
        if production
        else "manifests/fixtures/i7/full-reconciliation-run-specs"
    )
    return f"{prefix}/run_spec_id={_digest(run_spec_id, 'RunSpec ID')}/manifest.json"


def _trigger_path(run_spec_id: str, *, production: bool) -> str:
    return _sibling_path(run_spec_id, "triggers", production=production)


def _details_path(run_spec_id: str, *, production: bool) -> str:
    return _sibling_path(run_spec_id, "details", production=production)


def _qa_path(run_spec_id: str, *, production: bool) -> str:
    return _sibling_path(run_spec_id, "qa", production=production)


def _alert_path(run_spec_id: str, *, production: bool) -> str:
    return _sibling_path(run_spec_id, "alerts", production=production)


def _completion_path(run_spec_id: str, *, production: bool) -> str:
    return _sibling_path(run_spec_id, "completions", production=production)


def _sibling_path(run_spec_id: str, kind: str, *, production: bool) -> str:
    prefix = "manifests/silver/incremental/i7" if production else "manifests/fixtures/i7"
    return f"{prefix}/{kind}/run_spec_id={_digest(run_spec_id, 'RunSpec ID')}/manifest.json"


def _partition_details_path(
    run_spec_id: str,
    table_name: str,
    partition_key: str,
    *,
    production: bool,
) -> str:
    prefix = "manifests/silver/incremental/i7" if production else "manifests/fixtures/i7"
    key_digest = stable_digest({"partition_key": partition_key, "table_name": table_name})
    return (
        f"{prefix}/partition-details/run_spec_id={_digest(run_spec_id, 'RunSpec ID')}/"
        f"table_name={table_name}/partition_id={key_digest}/manifest.json"
    )


def _lock_path(run_spec_id: str, *, production: bool) -> str:
    prefix = "manifests/silver/locks" if production else "manifests/fixtures/i7/locks"
    return f"{prefix}/s7-5-i7-{_digest(run_spec_id, 'RunSpec ID')}.lock"


def _root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise I7ReconciliationRuntimeError("I7 data root must be a Path")
    root = value.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise I7ReconciliationRuntimeError("I7 data root is missing or unsafe")
    return root


def _production_authority_path(value: str, label: str) -> str:
    relative = _relative(value, f"{label} path")
    parts = {part.lower() for part in PurePosixPath(relative).parts}
    if parts & _FORBIDDEN_AUTHORITY_PARTS:
        raise I7ReconciliationRuntimeError(f"{label} path uses forbidden discovery/fixture scope")
    return relative


def _validate_production_control_locator(pin: ArtifactPin, *, kind: str) -> str:
    _artifact(pin, f"production {kind}")
    path = _production_authority_path(pin.path, f"production {kind}")
    pattern = {
        "run_spec": _PRODUCTION_RUN_SPEC_LOCATOR,
        "completion": _PRODUCTION_COMPLETION_LOCATOR,
    }.get(kind)
    if pattern is None:
        raise I7ReconciliationRuntimeError("production control locator kind is invalid")
    match = pattern.fullmatch(path)
    if match is None:
        raise I7ReconciliationRuntimeError(f"production {kind} locator is noncanonical")
    return match.group(1)


def _validate_legacy_full_completion_locator(pin: ArtifactPin) -> tuple[str, str]:
    """Validate the official Full locator before opening caller-selected bytes."""

    _artifact(pin, "legacy Full completion")
    path = _production_authority_path(pin.path, "legacy Full completion")
    match = _LEGACY_FULL_COMPLETION_LOCATOR.fullmatch(path)
    if match is None:
        raise I7LegacyFullSeamError("legacy Full completion locator is noncanonical")
    return match.group(1), match.group(2)


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    candidate = PurePosixPath(text)
    if (
        candidate.is_absolute()
        or text != candidate.as_posix()
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or "*" in text
        or "?" in text
        or "\\" in text
    ):
        raise I7ReconciliationRuntimeError(f"{label} is not a canonical relative path")
    return text


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise I7ReconciliationRuntimeError(f"{label} is not an ArtifactPin")
    _relative(value.path, f"{label} path")
    _digest(value.sha256, f"{label} SHA-256")
    if type(value.bytes) is not int or value.bytes <= 0:
        raise I7ReconciliationRuntimeError(f"{label} bytes must be positive")
    return value


def _artifact_from_dict(value: object, label: str) -> ArtifactPin:
    item = _mapping(value, label)
    _keys(item, {"bytes", "path", "sha256"}, label)
    return ArtifactPin(
        path=_relative(item["path"], f"{label} path"),
        sha256=_digest(item["sha256"], f"{label} SHA"),
        bytes=_positive_int(item["bytes"], f"{label} bytes"),
    )


def _read_exact_pin(root: Path, pin: ArtifactPin, label: str) -> bytes:
    path = safe_relative_path(root, pin.path)
    if not path.is_file() or path.is_symlink():
        raise I7ReconciliationRuntimeError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I7ReconciliationRuntimeError(f"{label} exact pin differs")
    return content


def _write_immutable(root: Path, relative: str, content: bytes, *, label: str) -> ArtifactPin:
    path_text = _relative(relative, f"{label} path")
    path = safe_relative_path(root, path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != content:
            raise I7ReconciliationRuntimeError(f"immutable {label} bytes differ")
        return _pin_bytes(path_text, content)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o640)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(descriptor)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise I7ReconciliationRuntimeError(f"cannot write immutable {label}") from exc
    return _pin_bytes(path_text, content)


def _stage_then_publish_immutable(
    root: Path,
    relative: str,
    content: bytes,
    *,
    label: str,
    before_publish: Callable[[], None],
) -> ArtifactPin:
    """Write a hidden complete inode, gate it, then no-clobber-link it visible.

    The successful ``link`` is the commit point.  No fallible resource gate is
    allowed after that point, so a failed gate can never leave a discoverable
    completion that a later idempotent replay accepts.
    """

    path_text = _relative(relative, f"{label} path")
    path = safe_relative_path(root, path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise I7ReconciliationRuntimeError(
            f"immutable {label} appeared during staged commit; replay is required"
        )
    staged = path.parent / (f".{path.name}.staged-{os.getpid()}-{time.monotonic_ns()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    committed = False
    try:
        descriptor = os.open(staged, flags, 0o600)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
            raise I7ReconciliationRuntimeError(f"staged {label} inode is unsafe")
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.close(descriptor)
        descriptor = None
        before_publish()
        os.link(staged, path, follow_symlinks=False)
        committed = True
        # The link is already the success commit point.  Durability and hidden
        # staging cleanup are best-effort finalization and must not turn a
        # visible valid completion into a reported failed run.
        try:
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            pass
    except I7ReconciliationRuntimeError:
        raise
    except OSError as exc:
        raise I7ReconciliationRuntimeError(f"cannot publish immutable {label}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if staged.exists() or staged.is_symlink():
            try:
                staged.unlink()
            except OSError as exc:
                if not committed:
                    raise I7ReconciliationRuntimeError(
                        f"cannot clean failed staged {label}"
                    ) from exc
    return _pin_bytes(path_text, content)


def _pin_existing(root: Path, relative: str, label: str) -> ArtifactPin:
    path = safe_relative_path(root, _relative(relative, f"{label} path"))
    if not path.is_file() or path.is_symlink():
        raise I7ReconciliationRuntimeError(f"{label} is missing or unsafe")
    return ArtifactPin(path=relative, sha256=_sha256_file(path), bytes=path.stat().st_size)


def _document_pin(path: str, document: Mapping[str, object]) -> ArtifactPin:
    return _pin_bytes(path, _canonical_json_bytes(document))


def _pin_bytes(path: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(
        path=_relative(path, "artifact path"),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _enforce_resources(
    policy: ResourceGatePolicy,
    started: float,
    meter: _ReadMeter,
    written_bytes: int,
    free_disk_bytes: int,
    *,
    chain_resolution_milliseconds: int = 0,
    process_read_bytes: int = 0,
    process_write_bytes: int = 0,
) -> None:
    if math.ceil(time.monotonic() - started) > policy.max_wall_clock_seconds:
        raise I7ReconciliationRuntimeError("I7 wall-clock resource gate failed")
    if _peak_rss_bytes() > policy.max_peak_rss_bytes:
        raise I7ReconciliationRuntimeError("I7 peak-RSS resource gate failed")
    if free_disk_bytes < policy.min_free_disk_bytes:
        raise I7ReconciliationRuntimeError("I7 disk-floor resource gate failed")
    if max(meter.bytes, process_read_bytes) > policy.max_read_bytes:
        raise I7ReconciliationRuntimeError("I7 read-byte resource gate failed")
    if max(written_bytes, process_write_bytes) > policy.max_write_bytes:
        raise I7ReconciliationRuntimeError("I7 write-byte resource gate failed")
    if chain_resolution_milliseconds > policy.max_chain_resolution_milliseconds:
        raise I7ReconciliationRuntimeError("I7 chain-resolution resource gate failed")


def _validate_stored_stage_resource_evidence(
    observation: I7RuntimeResourceObservation,
    *,
    spec: I7ReconciliationRunSpec,
    receipt: FullReconciliationReceipt,
    alert_artifact: ArtifactPin,
    completion_artifact_bytes: int,
) -> None:
    """Reproduce the deterministic portion of frozen stage resource evidence."""

    expected_phases = {
        "entry",
        "preflight",
        "producer_replay",
        "authority_artifacts",
        "projection_context",
        "review_artifacts",
        "before_completion",
        *(
            f"partition_{table}_{stable_digest(partition)[:12]}"
            for table, partition in sorted(
                _partition_scope(spec.incremental.resolved_partitions),
                key=_scope_sort_key,
            )
        ),
    }
    disk_phases = {phase for phase, _ in observation.phase_minimum_free_disk_bytes}
    rss_phases = {phase for phase, _ in observation.phase_peak_rss_bytes}
    if disk_phases != expected_phases or rss_phases != expected_phases:
        raise I7ReconciliationRuntimeError("I7 stored resource phase coverage differs")
    if observation.metered_read_bytes < observation.declared_read_bytes // 2:
        raise I7ReconciliationRuntimeError("I7 stored metered-read evidence is understated")
    expected_review_write = (
        alert_artifact.bytes
        + receipt.qa_artifact.bytes
        + receipt.details_artifact.bytes
        + sum(
            partition.details_artifact.bytes
            for table in receipt.table_evidence
            for partition in table.partitions
        )
    )
    expected_full_write = expected_review_write + _positive_int(
        completion_artifact_bytes,
        "I7 completion artifact bytes",
    )
    if observation.observed_write_bytes != expected_full_write:
        raise I7ReconciliationRuntimeError("I7 stored full-write evidence differs")


def _declared_snapshot_bytes(
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
) -> int:
    """Declared exact bytes for one producer pass, deduplicated by exact path."""

    pins = [
        *(pin for item in incremental.resolved_partitions for pin in item.lineage_artifacts),
        *(
            pin
            for item in incremental.checkpoint_before_partitions
            for pin in item.lineage_artifacts
        ),
        *(pin for item in full.partitions for pin in item.lineage_artifacts),
        *(
            incremental.top_pointer_artifact,
            incremental.gate_c_approval_artifact,
            incremental.producer_verification_artifact,
            incremental.release_completion_artifact,
            incremental.checkpoint_base_compaction_completion_artifact,
            incremental.checkpoint_base_compaction_proof_artifact,
            full.completion_artifact,
            full.producer_verification_artifact,
        ),
    ]
    pinned_bytes = _unique_pin_bytes(pins)
    declared_bytes = (
        incremental.producer_replay_declared_bytes + full.producer_replay_declared_bytes
    )
    if declared_bytes < pinned_bytes:
        raise I7ReconciliationRuntimeError("producer replay declaration understates exact pins")
    return declared_bytes


def _unique_pin_bytes(pins: Sequence[ArtifactPin]) -> int:
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        _artifact(pin, "declared replay artifact")
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise I7ReconciliationRuntimeError("declared snapshot path has conflicting pins")
        by_path[pin.path] = pin
    return sum(pin.bytes for pin in by_path.values())


def _pin_like_bytes(value: object, label: str) -> int:
    """Read the declared bytes of a verified ExactFilePin-like control receipt."""

    return _positive_int(getattr(value, "bytes", None), f"{label} bytes")


def _estimated_review_write_bytes(partition_count: int) -> int:
    if type(partition_count) is not int or partition_count <= 0:
        raise I7ReconciliationRuntimeError("I7 estimated partition count is invalid")
    return 4 * 1024**2 + partition_count * 16 * 1024


def _declared_preflight(
    root: Path,
    *,
    policy: ResourceGatePolicy,
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
) -> tuple[int, int]:
    # One official producer verification pass plus one independent canonical
    # comparison pass.  This intentionally overestimates shared controls.
    declared_read = 2 * _declared_snapshot_bytes(incremental, full)
    estimated_write = _estimated_review_write_bytes(len(full.partitions))
    if declared_read > policy.max_read_bytes:
        raise I7ReconciliationRuntimeError("I7 declared-read preflight gate failed")
    if estimated_write > policy.max_write_bytes:
        raise I7ReconciliationRuntimeError("I7 estimated-write preflight gate failed")
    if _disk_free(root) < policy.min_free_disk_bytes + estimated_write:
        raise I7ReconciliationRuntimeError("I7 preflight disk reserve gate failed")
    if _peak_rss_bytes() > policy.max_peak_rss_bytes:
        raise I7ReconciliationRuntimeError("I7 preflight peak-RSS gate failed")
    return declared_read, estimated_write


def _preflight_production_declarations(
    data_root: Path,
    config: I7ReconciliationConfig,
) -> None:
    """Metadata-only aggregate gate before the legacy verifier opens Parquet."""

    if not isinstance(config, I7ReconciliationConfig):
        raise I7ReconciliationRuntimeError("I7 production preflight requires a typed config")
    root = _root(data_root)
    try:
        from ame_stocks_api.silver import incremental_i6_pointer_runtime as i6

        top = i6.load_research_top_snapshot_exact(root)
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I6 seam: production declaration cannot replay research top"
        ) from exc
    if (
        top.research_top_event_artifact != config.incremental_top_pointer_artifact
        or top.gate_c_approval_artifact != config.gate_c_approval_artifact
        or top.terminal_session != config.cutoff_session
        or top.source_cutoff_session != config.cutoff_session
    ):
        raise I7IncrementalTopSeamError("P0 I6 seam: production declaration scope differs")
    try:
        from ame_stocks_api.silver import incremental_i7_checkpoint_compaction as compaction

        compaction._validate_control_locator(
            config.checkpoint_compaction_completion_artifact,
            "completion",
            authority=compaction.I7_CHECKPOINT_COMPACTION_AUTHORITY,
        )
        compaction_content = _read_exact_pin(
            root,
            config.checkpoint_compaction_completion_artifact,
            "checkpoint compaction declaration completion",
        )
        compaction_completion = compaction._completion_from_dict(
            _closed_json(compaction_content, "checkpoint compaction declaration completion")
        )
        if (
            compaction_completion.canonical_bytes() != compaction_content
            or compaction_completion.source_snapshot_id != top.snapshot_id
            or compaction_completion.completion_available_session > config.receipt_available_session
        ):
            raise I7IncrementalTopSeamError(
                "P0 I7 checkpoint-rebase declaration differs from the I6 top"
            )
        compaction_pins = [
            config.checkpoint_compaction_completion_artifact,
            compaction_completion.run_spec_artifact,
            compaction_completion.compacted_manifest_artifact,
            compaction_completion.compacted_checkpoint_artifact,
            compaction_completion.proof_artifact,
        ]
        for output in compaction_completion.output_artifacts:
            compaction_pins.append(output.manifest_output.artifact)
            if output.rowset_index is not None:
                compaction_pins.extend(item.artifact for item in output.rowset_index.segments)
            elif output.dataset_index is not None:
                compaction_pins.extend(item.artifact for item in output.dataset_index.partitions)
        compaction_bytes = _unique_pin_bytes(compaction_pins)
        if compaction_bytes > compaction_completion.input_bytes + (
            compaction_completion.output_bytes
        ):
            raise I7IncrementalTopSeamError(
                "P0 I7 checkpoint-rebase declaration understates exact artifacts"
            )
    except I7IncrementalTopSeamError:
        raise
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I7 checkpoint-rebase declaration cannot be replayed"
        ) from exc
    try:
        incremental_meter = _ReadMeter(root)
        incremental_completion = load_i3_production_completion_exact(
            top.release_completion_artifact,
            incremental_meter.read_path,
        )
        incremental_receipt = load_i3_production_run_receipt_exact(
            incremental_completion.receipt_artifact,
            incremental_meter.read_path,
        )
        incremental_run_spec = load_i3_production_run_spec_exact(
            incremental_receipt.run_spec_artifact,
            incremental_meter.read_path,
        )
        if (
            incremental_run_spec.run_kind is not I3ProductionRunKind.DELTA
            or incremental_run_spec.terminal_session != config.cutoff_session
            or incremental_receipt.output_set is None
            or incremental_receipt.output_set.table_outputs != top.table_outputs
        ):
            raise I7IncrementalTopSeamError("P0 I6 seam: production declaration release differs")
        incremental_parent = load_i3_production_parent_shallow_exact(
            root,
            incremental_run_spec,
        )
        if incremental_parent is None:
            raise I7IncrementalTopSeamError("P0 I6 seam: production declaration DELTA lacks BASE")
        incremental_inputs = load_production_delta_input_binding(
            data_root=root,
            run_spec=incremental_run_spec,
            parent=incremental_parent,
        )
    except I7IncrementalTopSeamError:
        raise
    except Exception as exc:
        raise I7IncrementalTopSeamError(
            "P0 I6 seam: production source declaration cannot replay"
        ) from exc
    i6_pins = list(_i6_authority_pins(top))
    i6_partition_count = 0
    for output in top.table_outputs:
        if output.dataset_index is not None:
            i6_partition_count += len(output.dataset_index.partitions)
        elif output.rowset_index is not None:
            i6_partition_count += 1
    locator_plan_id, locator_approval_id = _validate_legacy_full_completion_locator(
        config.independent_full_completion_artifact
    )
    content = _read_exact_pin(
        root,
        config.independent_full_completion_artifact,
        "legacy Full declaration completion",
    )
    completion = _closed_json(content, "legacy Full declaration completion")
    plan_id = _digest(completion.get("plan_id"), "legacy declaration plan ID")
    approval_id = _digest(completion.get("approval_id"), "legacy declaration approval ID")
    if (
        plan_id != locator_plan_id
        or approval_id != locator_approval_id
        or config.independent_full_completion_artifact.path
        != legacy_full._completion_path(plan_id, approval_id)
    ):
        raise I7LegacyFullSeamError("legacy Full declaration locator differs")
    try:
        controls = legacy_full._load_execution_controls(
            root, plan_id=plan_id, approval_id=approval_id
        )
        binding = controls["binding"]
        approval = controls["approval"]
        completed_at = legacy_full._utc_from_text(
            completion.get("completed_at_utc"),
            "legacy Full declaration completion time",
        )
        if binding.mode != "production" or binding.cutoff_session != config.cutoff_session:
            raise I7LegacyFullSeamError("legacy Full declaration cutoff differs")
        if completed_at < approval.approved_at_utc:
            raise I7LegacyFullSeamError("legacy Full declaration completion predates approval")
    except I7LegacyFullSeamError:
        raise
    except Exception as exc:
        raise I7LegacyFullSeamError("legacy Full declaration controls failed") from exc
    candidate_pin = _artifact_from_dict(
        completion.get("candidate_manifest"), "legacy declaration candidate"
    )
    candidate = _closed_json(
        _read_exact_pin(root, candidate_pin, "legacy declaration candidate"),
        "legacy declaration candidate",
    )
    outputs = _mapping(candidate.get("outputs"), "legacy declaration outputs")
    full_bytes = (
        config.independent_full_completion_artifact.bytes
        + candidate_pin.bytes
        + _positive_int(binding.declared_source_bytes, "legacy declared source bytes")
        + sum(
            _pin_like_bytes(controls[key], f"legacy Full {key}")
            for key in ("plan_receipt", "request_receipt", "approval_receipt")
        )
        + _artifact_from_dict(
            controls["plan"]["source_binding"],
            "legacy Full source binding receipt",
        ).bytes
        + sum(
            _pin_like_bytes(pin, "legacy Full contract approval")
            for pin in binding.contract_approvals
        )
    )
    profile = controls["plan"].get("bounded_profile_evidence")
    if profile is not None:
        full_bytes += _artifact_from_dict(
            _mapping(profile, "legacy Full profile evidence")["completion"],
            "legacy Full profile completion",
        ).bytes
    full_partition_count = 0
    for table_name in I7_TABLE_ORDER:
        raw = outputs.get(table_name)
        values = (
            _sequence(raw, "legacy declaration partitions") if isinstance(raw, list) else (raw,)
        )
        for value in values:
            receipt = _mapping(value, "legacy declaration receipt")
            full_bytes += _positive_int(receipt.get("bytes"), "legacy declared bytes")
            full_partition_count += 1
    qa_receipt = _mapping(outputs.get("qa"), "legacy declaration QA receipt")
    full_bytes += _positive_int(qa_receipt.get("bytes"), "legacy declared QA bytes")
    i6_bytes = (
        _unique_pin_bytes(i6_pins)
        + incremental_receipt.output_set.total_output_bytes
        + _positive_int(
            incremental_inputs.source_binding.declared_source_bytes,
            "incremental declared source bytes",
        )
        + sum(
            _pin_like_bytes(pin, "incremental contract approval")
            for pin in incremental_inputs.source_binding.contract_approvals
        )
        + _unique_pin_bytes(incremental_inputs.binding.declared_input_artifacts)
        + _positive_int(
            incremental_inputs.binding.transitive_control_replay_bytes,
            "incremental transitive control replay bytes",
        )
        + incremental_meter.bytes
    )
    declared_read = 2 * (i6_bytes + compaction_bytes + full_bytes)
    if i6_partition_count != full_partition_count:
        raise I7ReconciliationRuntimeError("I7 declaration partition counts differ")
    estimated_write = _estimated_review_write_bytes(full_partition_count)
    if declared_read > config.resource_policy.max_read_bytes:
        raise I7ReconciliationRuntimeError("I7 declared-read preflight gate failed")
    if estimated_write > config.resource_policy.max_write_bytes:
        raise I7ReconciliationRuntimeError("I7 estimated-write preflight gate failed")
    if _disk_free(root) < config.resource_policy.min_free_disk_bytes + estimated_write:
        raise I7ReconciliationRuntimeError("I7 preflight disk reserve gate failed")


def _disk_free(root: Path) -> int:
    try:
        return int(shutil.disk_usage(root).free)
    except OSError as exc:
        raise I7ReconciliationRuntimeError("cannot inspect I7 disk free bytes") from exc


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _proc_io_bytes() -> tuple[int, int]:
    """Return process read/write bytes; unsupported platforms fail to zero safely."""

    path = Path("/proc/self/io")
    if not path.is_file() or path.is_symlink():
        return (0, 0)
    try:
        values = {}
        for line in path.read_text(encoding="ascii").splitlines():
            key, separator, raw = line.partition(":")
            if separator:
                values[key.strip()] = int(raw.strip())
        # read_bytes/write_bytes are physical I/O; rchar/wchar keep cached I/O
        # visible.  Taking the maximum gives a conservative whole-process gate.
        return (
            max(values.get("read_bytes", 0), values.get("rchar", 0)),
            max(values.get("write_bytes", 0), values.get("wchar", 0)),
        )
    except (OSError, UnicodeError, ValueError):
        return (0, 0)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise I7ReconciliationRuntimeError(f"{label} must be an object")
    return dict(value)


def _sequence(value: object, label: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise I7ReconciliationRuntimeError(f"{label} must be an array")
    return tuple(value)


def _keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise I7ReconciliationRuntimeError(f"{label} fields differ")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise I7ReconciliationRuntimeError(f"{label} must be nonempty canonical text")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if _DIGEST.fullmatch(text) is None:
        raise I7ReconciliationRuntimeError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise I7ReconciliationRuntimeError(f"{label} must be a positive integer")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I7ReconciliationRuntimeError(f"{label} must be a nonnegative integer")
    return value


def _closed_json(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I7ReconciliationRuntimeError(f"{label} is not canonical JSON") from exc
    result = _mapping(value, label)
    if _canonical_json_bytes(result) != content:
        raise I7ReconciliationRuntimeError(f"{label} bytes are not canonical JSON")
    return result


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _json_value(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")


def _json_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, bytes):
        return value.hex()
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise I7ReconciliationRuntimeError("I7 value is not canonically serializable")


def _sort_value(value: object) -> tuple[int, str]:
    normalized = _json_value(value)
    return (0 if normalized is None else 1, json.dumps(normalized, sort_keys=True))


__all__ = [
    "CrossProducerProjectionContract",
    "CrossProducerProjectionKind",
    "I7IncrementalTopSeamError",
    "I7LegacyFullSeamError",
    "I7PreparedRun",
    "I7ReconciliationCompletion",
    "I7ReconciliationConfig",
    "I7ReconciliationResult",
    "I7ReconciliationRunSpec",
    "I7ReconciliationRuntimeError",
    "I7RuntimeResourceObservation",
    "ReconciliationPartition",
    "ReconciliationTriggerKind",
    "VerifiedIncrementalTopSnapshot",
    "VerifiedLegacyFullSnapshot",
    "cross_producer_identity_policy_digest",
    "cross_producer_projection_contract",
    "cross_producer_transform_semantics_digest",
    "load_official_legacy_full_snapshot",
    "prepare_i7_full_reconciliation",
    "project_cross_producer_rows",
    "stage_i7_full_reconciliation",
    "verify_i7_full_reconciliation",
]
