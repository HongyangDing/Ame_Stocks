"""Exact, bounded production IO for the first native-v2 I3 clean DELTA.

This module advances the authenticated 2026-07-09 compact BASE by exactly one
XNYS session, 2026-07-10.  It never discovers a source directory or resolves a
``latest`` pointer.  The source window is the closed three-session boundary
2026-07-08..2026-07-10: the first two rows come from exact native-v2 parent
partitions and the target comes from the sole authenticated I2 receipt.

The three versioned tables preserve their complete parent rowset prefix and
append one immutable Parquet segment each.  ``universe_daily`` preserves its
complete dataset-index prefix and appends one immutable target partition.
Missing Gate-B coverage and an expired provider-Composite override are explicit
fail-closed identity outcomes: membership and observed evidence remain, while
canonical identity, alias/master references and backtest eligibility are null.

The returned nominal prepared bundle carries a process-local, module-sealed
attestation.  The production executor must require the verifier in this module
before constructing Gate-A controls; structural materializers have no authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import resource
import shutil
import stat
import sys
import time
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Final, Self

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver import identity_materialization_streaming as streaming
from ame_stocks_api.silver import incremental_i3_runner as runner
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental import load_completed_s4_asset_session_run
from ame_stocks_api.silver.asset_incremental_contract import (
    S4BaseFrontier,
    S4ParentKind,
    S4SessionPartitionReceipt,
    S4SessionRunReceipt,
    S4SessionRunSpec,
)
from ame_stocks_api.silver.asset_release_set import AssetReleaseSet
from ame_stocks_api.silver.calendar_artifact import load_xnys_calendar_artifact
from ame_stocks_api.silver.contracts import ReleaseManifest, arrow_schema_digest
from ame_stocks_api.silver.identity_registry_workflow import (
    LoadedRegistryReleaseSet,
    RegistryCandidateManifest,
    RegistryReleaseManifest,
    RegistryReleasePin,
    load_registry_release,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin, RowVersionOperation
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    LEGACY_S7_V1_RELEASE_SET_ID,
    NATIVE_V2_RELEASE_FAMILY,
    S4_TERMINAL_TABLE_ORDER,
    I3CheckpointState,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    ResolvedPartitionState,
    S4TerminalPartitionPin,
    TerminalRowVersionState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_QA_CATALOG,
    I3_QA_CATALOG_DIGEST,
)
from ame_stocks_api.silver.incremental_i3_migration_io import (
    I3MigrationIOError,
    readback_i3_migration_parquet_exact,
    write_i3_migration_parquet_no_clobber,
)
from ame_stocks_api.silver.incremental_i3_production import (
    I3ProductionPreparedMaterialization,
    I3ProductionPreparedRowVersion,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionDatasetIndex,
    I3ProductionI2ReceiptPin,
    I3ProductionOutputStorage,
    I3ProductionParentAuthority,
    I3ProductionPartitionPin,
    I3ProductionResourceCaps,
    I3ProductionResourceObservation,
    I3ProductionRowsetIndex,
    I3ProductionRunKind,
    I3ProductionRunSpec,
    I3ProductionSegmentPin,
    I3ProductionTableOutput,
    LoadedI3ProductionStaging,
    load_i3_production_completion_exact,
    load_i3_production_deep_attestation_exact,
    load_i3_production_parent_shallow_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
    production_v2_contract_pins,
)
from ame_stocks_api.silver.incremental_i3_production_inputs import (
    load_frozen_s7_oracle_marker_exact,
)
from ame_stocks_api.silver.incremental_i3_production_policy import (
    load_production_identity_policy_snapshot,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_PRODUCTION_DELTA_ALIAS_AVAILABILITY_PROGRESSION_RULE_VERSION,
    I3_PRODUCTION_DELTA_APPEND_SEGMENT_RULE_VERSION,
    I3_PRODUCTION_DELTA_IDENTITY_FALLBACK_RULE_VERSION,
    I3_PRODUCTION_DELTA_INPUT_BINDING_RULE_VERSION,
    I3_PRODUCTION_DELTA_PARTITION_RECEIPT_RULE_VERSION,
    I3_PRODUCTION_DELTA_RESOLUTION_RULE_VERSION,
    I3_PRODUCTION_DELTA_RESOURCE_ENVELOPE_RULE_VERSION,
    I3_PRODUCTION_DELTA_ROW_VALIDATOR_RULE_VERSION,
    I3_PRODUCTION_DELTA_SOR_EXPIRY_RULE_VERSION,
    I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION,
    I3_PRODUCTION_DELTA_SOURCE_WINDOW_RULE_VERSION,
    I3_PRODUCTION_DELTA_STATE_TRANSITION_RULE_VERSION,
    I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP,
    I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES,
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
    production_delta_append_segment_id,
    production_delta_row_validator_digest,
)
from ame_stocks_api.silver.incremental_identity import (
    canonical_asset_id,
)

I3_PRODUCTION_DELTA_MATERIALIZATION_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_production_delta_materialization_attestation_v1"
)
I3_PRODUCTION_DELTA_SOURCE_DIGEST_RULE_VERSION: Final = (
    "s7_5_i3_production_delta_physical_source_v2"
)
I3_PRODUCTION_DELTA_CANONICAL_PROJECTION_RULE_VERSION: Final = (
    "s7_5_i3_production_delta_canonical_projection_v1"
)
I3_PRODUCTION_DELTA_ROW_CHANGE_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_production_delta_row_change_attestation_v1"
)
I3_PRODUCTION_DELTA_CONFIG_RULE_VERSION: Final = "s7_5_i3_production_delta_config_v1"
PRODUCTION_FIRST_DELTA_SESSION: Final = date(2026, 7, 10)
PRODUCTION_FIRST_DELTA_PARENT_SESSION: Final = date(2026, 7, 9)
PRODUCTION_DELTA_BOUNDARY_SESSIONS: Final = (
    date(2026, 7, 8),
    PRODUCTION_FIRST_DELTA_PARENT_SESSION,
    PRODUCTION_FIRST_DELTA_SESSION,
)

_SMALL_TABLES: Final = I3_V2_TABLE_ORDER[:-1]
_V2_UNIVERSE_ENVELOPE: Final = frozenset(
    {
        "alias_resolution_version_id",
        "alias_segment_id",
        "asset_master_version_id",
        "identity_policy_bundle_id",
        "issuer_master_version_id",
        "row_available_session",
    }
)
_VERSION_SHAPE: Final[Mapping[str, tuple[str, str, str, str]]] = {
    "asset_master": (
        "asset_id",
        "asset_master_version_id",
        "predecessor_asset_master_version_id",
        "version_available_session",
    ),
    "ticker_alias": (
        "alias_segment_id",
        "alias_resolution_version_id",
        "predecessor_alias_resolution_version_id",
        "alias_version_available_session",
    ),
    "issuer_master": (
        "issuer_id",
        "issuer_master_version_id",
        "predecessor_issuer_master_version_id",
        "version_available_session",
    ),
}
_MINIMUM_DELTA_DISK_RESERVE: Final = 2 * 1024**3
_PRODUCTION_CONTROL_ROOT: Final = "manifests/silver/identity/s7-5-native-v2-staging"
_PRODUCTION_FIRST_DELTA_I2_RECEIPT_PATH: Final = (
    "manifests/silver/incremental/s4/assets/"
    "session_year=2026/session_date=2026-07-10/run-receipt.json"
)
_SOR_OVERRIDE_TICKER: Final = "SOR"
_SOR_OVERRIDE_OBSERVED_COMPOSITE: Final = "BBG000KMY6N2"
_SOR_OVERRIDE_OBSERVED_SHARE_CLASS: Final = "BBG01RK6N5G9"
_SOR_OVERRIDE_CANONICAL_COMPOSITE: Final = "BBG01RK6N4M5"
_SOR_OVERRIDE_VALID_FROM: Final = date(2025, 1, 2)
_SOR_OVERRIDE_VALID_THROUGH: Final = PRODUCTION_FIRST_DELTA_PARENT_SESSION
_SOR_OVERRIDE_SOURCE_ROW_COUNT: Final = 379


class I3DeltaIOError(RuntimeError):
    """Raised when the exact first-delta trust boundary fails closed."""


@dataclass(frozen=True, slots=True)
class I3ProductionDeltaInputBinding:
    """Closed logical expansion of the exact parent, I2 and policy controls."""

    run_spec_id: str
    parent_release_id: str
    parent_checkpoint_id: str
    parent_deep_attestation_id: str
    parent_completion_artifact: ArtifactPin
    parent_deep_attestation_artifact: ArtifactPin
    i2_receipt_id: str
    i2_receipt_artifact: ArtifactPin
    i2_partitions: tuple[S4SessionPartitionReceipt, ...]
    parent_boundary_partitions: tuple[I3ProductionPartitionPin, ...]
    requested_sessions: tuple[date, ...]
    source_binding_id: str
    source_binding_artifact: ArtifactPin
    gate_b_manifest_artifact: ArtifactPin
    gate_b_data_artifact: ArtifactPin
    declared_input_artifacts: tuple[ArtifactPin, ...]
    parent_output_bytes: int
    parent_output_rows: int
    asset_transition_decision_count: int
    transitive_control_replay_bytes: int
    policy_snapshot_id: str
    policy_release_set_binding_digest: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "delta RunSpec ID"),
            (self.parent_release_id, "delta parent release ID"),
            (self.parent_checkpoint_id, "delta parent checkpoint ID"),
            (self.parent_deep_attestation_id, "delta parent deep-attestation ID"),
            (self.i2_receipt_id, "delta I2 receipt ID"),
            (self.source_binding_id, "delta source-binding ID"),
            (self.policy_snapshot_id, "delta policy-snapshot ID"),
            (
                self.policy_release_set_binding_digest,
                "delta policy release-set binding digest",
            ),
        ):
            _digest(value, label)
        for value, label in (
            (self.parent_completion_artifact, "delta parent completion artifact"),
            (self.parent_deep_attestation_artifact, "delta parent deep artifact"),
            (self.i2_receipt_artifact, "delta I2 receipt artifact"),
            (self.source_binding_artifact, "delta source-binding artifact"),
            (self.gate_b_manifest_artifact, "delta Gate-B manifest artifact"),
            (self.gate_b_data_artifact, "delta Gate-B data artifact"),
        ):
            if not isinstance(value, ArtifactPin):
                raise I3DeltaIOError(f"{label} is invalid")
            _explicit_path(value.path)
        if (
            type(self.i2_partitions) is not tuple
            or tuple(item.table_name for item in self.i2_partitions)
            != tuple(sorted(S4_TERMINAL_TABLE_ORDER))
            or any(
                item.session_date != PRODUCTION_FIRST_DELTA_SESSION for item in self.i2_partitions
            )
        ):
            raise I3DeltaIOError("delta I2 partition set differs from the exact three tables")
        if (
            type(self.parent_boundary_partitions) is not tuple
            or tuple(item.session_date for item in self.parent_boundary_partitions)
            != PRODUCTION_DELTA_BOUNDARY_SESSIONS[:2]
        ):
            raise I3DeltaIOError("delta parent boundary is not exactly 2026-07-08..09")
        if self.requested_sessions != PRODUCTION_DELTA_BOUNDARY_SESSIONS:
            raise I3DeltaIOError("delta source window is not exactly 2026-07-08..10")
        for value, label in (
            (self.parent_output_bytes, "delta parent output bytes"),
            (self.parent_output_rows, "delta parent output rows"),
            (
                self.asset_transition_decision_count,
                "delta asset-transition decision count",
            ),
            (
                self.transitive_control_replay_bytes,
                "delta transitive control replay bytes",
            ),
        ):
            if type(value) is not int or value < 0:
                raise I3DeltaIOError(f"{label} is invalid")
        _validate_transitive_control_replay_bytes(self.transitive_control_replay_bytes)
        if (
            type(self.declared_input_artifacts) is not tuple
            or self.declared_input_artifacts
            != tuple(sorted(self.declared_input_artifacts, key=lambda item: item.path))
            or len({item.path for item in self.declared_input_artifacts})
            != len(self.declared_input_artifacts)
        ):
            raise I3DeltaIOError("delta declared input artifacts are not sorted and unique")
        required = {
            item.path: item
            for item in (
                self.parent_completion_artifact,
                self.parent_deep_attestation_artifact,
                self.i2_receipt_artifact,
                self.source_binding_artifact,
                self.gate_b_manifest_artifact,
                self.gate_b_data_artifact,
                *(item.artifact for item in self.i2_partitions),
                *(item.artifact for item in self.parent_boundary_partitions),
            )
        }
        declared = {item.path: item for item in self.declared_input_artifacts}
        if any(declared.get(path) != pin for path, pin in required.items()):
            raise I3DeltaIOError("delta declared input set omits an exact required artifact")

    @property
    def input_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "gate_b_data_artifact": self.gate_b_data_artifact.to_dict(),
            "gate_b_manifest_artifact": self.gate_b_manifest_artifact.to_dict(),
            "declared_input_artifacts": [item.to_dict() for item in self.declared_input_artifacts],
            "i2_partitions": [item.to_dict() for item in self.i2_partitions],
            "i2_receipt_artifact": self.i2_receipt_artifact.to_dict(),
            "i2_receipt_id": self.i2_receipt_id,
            "parent_boundary_partitions": [
                item.to_dict() for item in self.parent_boundary_partitions
            ],
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "parent_output_bytes": self.parent_output_bytes,
            "parent_output_rows": self.parent_output_rows,
            "parent_completion_artifact": self.parent_completion_artifact.to_dict(),
            "parent_deep_attestation_artifact": (self.parent_deep_attestation_artifact.to_dict()),
            "parent_deep_attestation_id": self.parent_deep_attestation_id,
            "parent_release_id": self.parent_release_id,
            "policy_release_set_binding_digest": self.policy_release_set_binding_digest,
            "policy_snapshot_id": self.policy_snapshot_id,
            "requested_sessions": [item.isoformat() for item in self.requested_sessions],
            "rule_version": I3_PRODUCTION_DELTA_INPUT_BINDING_RULE_VERSION,
            "run_spec_id": self.run_spec_id,
            "source_binding_artifact": self.source_binding_artifact.to_dict(),
            "source_binding_id": self.source_binding_id,
            "asset_transition_decision_count": (self.asset_transition_decision_count),
            "transitive_control_replay_bytes": self.transitive_control_replay_bytes,
        }


@dataclass(frozen=True, slots=True)
class I3ProductionDeltaResourceEstimate:
    source_bytes: int
    estimated_peak_rss_bytes: int
    estimated_output_bytes: int
    estimated_output_rows: int
    estimated_temporary_bytes: int
    minimum_free_disk_bytes_required: int
    transitive_control_replay_bytes: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.source_bytes, "delta estimated source bytes"),
            (self.estimated_peak_rss_bytes, "delta estimated peak RSS"),
            (self.estimated_output_bytes, "delta estimated output bytes"),
            (self.estimated_output_rows, "delta estimated output rows"),
            (self.estimated_temporary_bytes, "delta estimated temporary bytes"),
            (
                self.minimum_free_disk_bytes_required,
                "delta minimum free disk bytes",
            ),
            (
                self.transitive_control_replay_bytes,
                "delta transitive control replay bytes",
            ),
        ):
            if type(value) is not int or value < 0:
                raise I3DeltaIOError(f"{label} is invalid")
        _validate_transitive_control_replay_bytes(self.transitive_control_replay_bytes)

    def to_dict(self) -> dict[str, object]:
        return {
            "estimated_output_bytes": self.estimated_output_bytes,
            "estimated_output_rows": self.estimated_output_rows,
            "estimated_peak_rss_bytes": self.estimated_peak_rss_bytes,
            "estimated_temporary_bytes": self.estimated_temporary_bytes,
            "minimum_free_disk_bytes_required": self.minimum_free_disk_bytes_required,
            "rule_version": I3_PRODUCTION_DELTA_RESOURCE_ENVELOPE_RULE_VERSION,
            "source_bytes": self.source_bytes,
            "transitive_control_replay_bytes": self.transitive_control_replay_bytes,
            "transitive_control_replay_bytes_cap": (
                I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP
            ),
            "transitive_control_replay_rss_reserve_bytes": (
                I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES
            ),
        }


@dataclass(frozen=True, slots=True)
class I3ProductionDeltaRunConfig:
    """Operator-selected exact controls from which the DELTA RunSpec is derived."""

    parent_completion_artifact: ArtifactPin
    parent_deep_attestation_artifact: ArtifactPin
    i2_receipt_artifact: ArtifactPin
    run_available_session: date
    resource_caps: I3ProductionResourceCaps
    parent_authority: I3ProductionParentAuthority = I3ProductionParentAuthority.MIGRATION_SHADOW
    parent_pointer_event_artifact: ArtifactPin | None = None

    def __post_init__(self) -> None:
        for value, label in (
            (self.parent_completion_artifact, "parent completion artifact"),
            (self.parent_deep_attestation_artifact, "parent deep-attestation artifact"),
            (self.i2_receipt_artifact, "I2 receipt artifact"),
        ):
            if not isinstance(value, ArtifactPin):
                raise I3DeltaIOError(f"delta {label} is invalid")
            _explicit_path(value.path)
        if (
            type(self.run_available_session) is not date
            or self.run_available_session < PRODUCTION_FIRST_DELTA_SESSION
        ):
            raise I3DeltaIOError("delta run availability is invalid")
        if not isinstance(self.resource_caps, I3ProductionResourceCaps):
            raise I3DeltaIOError("delta resource caps are invalid")
        if not isinstance(self.parent_authority, I3ProductionParentAuthority):
            raise I3DeltaIOError("delta parent authority is invalid")
        if self.parent_authority is I3ProductionParentAuthority.MIGRATION_SHADOW:
            if self.parent_pointer_event_artifact is not None:
                raise I3DeltaIOError("migration-shadow delta cannot carry a pointer event")
        elif not isinstance(self.parent_pointer_event_artifact, ArtifactPin):
            raise I3DeltaIOError("published-daily delta requires an exact pointer event")
        else:
            _explicit_path(self.parent_pointer_event_artifact.path)
        if self.i2_receipt_artifact.path != _PRODUCTION_FIRST_DELTA_I2_RECEIPT_PATH:
            raise I3DeltaIOError("delta I2 receipt path differs from the fixed target control")

    @property
    def config_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i3_production_delta_config",
            "i2_receipt_artifact": self.i2_receipt_artifact.to_dict(),
            "parent_authority": self.parent_authority.value,
            "parent_completion_artifact": self.parent_completion_artifact.to_dict(),
            "parent_deep_attestation_artifact": (self.parent_deep_attestation_artifact.to_dict()),
            "parent_pointer_event_artifact": (
                None
                if self.parent_pointer_event_artifact is None
                else self.parent_pointer_event_artifact.to_dict()
            ),
            "resource_caps": self.resource_caps.to_dict(),
            "rule_version": I3_PRODUCTION_DELTA_CONFIG_RULE_VERSION,
            "run_available_session": self.run_available_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"config_id": self.config_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact_type",
                "config_id",
                "i2_receipt_artifact",
                "parent_authority",
                "parent_completion_artifact",
                "parent_deep_attestation_artifact",
                "parent_pointer_event_artifact",
                "resource_caps",
                "rule_version",
                "run_available_session",
            },
            "production DELTA config",
        )
        if item["artifact_type"] != "s7_5_i3_production_delta_config":
            raise I3DeltaIOError("production DELTA config type changed")
        if item["rule_version"] != I3_PRODUCTION_DELTA_CONFIG_RULE_VERSION:
            raise I3DeltaIOError("production DELTA config rule changed")
        try:
            authority = I3ProductionParentAuthority(
                _nonempty_text(item["parent_authority"], "delta parent authority")
            )
        except ValueError as exc:
            raise I3DeltaIOError("delta parent authority is invalid") from exc
        result = cls(
            parent_completion_artifact=_artifact_pin_from_dict(
                item["parent_completion_artifact"], "parent completion artifact"
            ),
            parent_deep_attestation_artifact=_artifact_pin_from_dict(
                item["parent_deep_attestation_artifact"],
                "parent deep-attestation artifact",
            ),
            i2_receipt_artifact=_artifact_pin_from_dict(
                item["i2_receipt_artifact"], "I2 receipt artifact"
            ),
            run_available_session=_date_from_json(
                item["run_available_session"], "delta run availability"
            ),
            resource_caps=I3ProductionResourceCaps.from_dict(item["resource_caps"]),
            parent_authority=authority,
            parent_pointer_event_artifact=(
                None
                if item["parent_pointer_event_artifact"] is None
                else _artifact_pin_from_dict(
                    item["parent_pointer_event_artifact"],
                    "parent pointer-event artifact",
                )
            ),
        )
        if item["config_id"] != result.config_id:
            raise I3DeltaIOError("production DELTA config ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class PreparedI3ProductionDeltaRunSpec:
    run_spec: I3ProductionRunSpec
    run_spec_artifact: ArtifactPin
    config_artifact: ArtifactPin


@dataclass(frozen=True, slots=True, weakref_slot=True)
class DeltaMaterializationAttestation:
    run_spec_id: str
    input_binding_id: str
    source_digest: str
    transform_semantics_digest: str
    parent_release_id: str
    parent_checkpoint_id: str
    parent_deep_attestation_id: str
    table_output_set_digest: str
    row_change_index_digest: str
    native_manifest_id: str
    native_manifest_artifact: ArtifactPin
    checkpoint_id: str
    checkpoint_artifact: ArtifactPin
    canonical_projection_digest: str
    canonical_projection_difference_count: int
    resource_observation_digest: str
    terminal_session: date
    availability_session: date
    _seal: object = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "delta attestation RunSpec ID"),
            (self.input_binding_id, "delta attestation input-binding ID"),
            (self.source_digest, "delta attestation source digest"),
            (self.transform_semantics_digest, "delta attestation transform digest"),
            (self.parent_release_id, "delta attestation parent release ID"),
            (self.parent_checkpoint_id, "delta attestation parent checkpoint ID"),
            (self.parent_deep_attestation_id, "delta attestation parent deep ID"),
            (self.table_output_set_digest, "delta attestation output-set digest"),
            (self.row_change_index_digest, "delta attestation row-change digest"),
            (self.native_manifest_id, "delta attestation native-manifest ID"),
            (self.checkpoint_id, "delta attestation checkpoint ID"),
            (self.canonical_projection_digest, "delta attestation projection digest"),
            (self.resource_observation_digest, "delta attestation resource digest"),
        ):
            _digest(value, label)
        if not isinstance(self.native_manifest_artifact, ArtifactPin):
            raise I3DeltaIOError("delta attestation native-manifest artifact is invalid")
        if not isinstance(self.checkpoint_artifact, ArtifactPin):
            raise I3DeltaIOError("delta attestation checkpoint artifact is invalid")
        if self.canonical_projection_difference_count != 0:
            raise I3DeltaIOError("delta attestation cannot authorize projection differences")
        if self.terminal_session != PRODUCTION_FIRST_DELTA_SESSION:
            raise I3DeltaIOError("delta attestation targets another session")
        if self.availability_session < self.terminal_session:
            raise I3DeltaIOError("delta attestation availability precedes its terminal")

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "canonical_projection_difference_count": (self.canonical_projection_difference_count),
            "canonical_projection_digest": self.canonical_projection_digest,
            "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
            "checkpoint_id": self.checkpoint_id,
            "input_binding_id": self.input_binding_id,
            "native_manifest_artifact": self.native_manifest_artifact.to_dict(),
            "native_manifest_id": self.native_manifest_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "parent_deep_attestation_id": self.parent_deep_attestation_id,
            "parent_release_id": self.parent_release_id,
            "resource_observation_digest": self.resource_observation_digest,
            "row_change_index_digest": self.row_change_index_digest,
            "rule_version": I3_PRODUCTION_DELTA_MATERIALIZATION_ATTESTATION_RULE_VERSION,
            "run_kind": I3ProductionRunKind.DELTA.value,
            "run_spec_id": self.run_spec_id,
            "source_digest": self.source_digest,
            "table_output_set_digest": self.table_output_set_digest,
            "terminal_session": self.terminal_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    @property
    def attestation_id(self) -> str:
        return stable_digest(self.logical_payload())


@dataclass(frozen=True, slots=True)
class DeltaPreparedMaterialization(I3ProductionPreparedMaterialization):
    delta_materialization_attestation: DeltaMaterializationAttestation


@dataclass(frozen=True, slots=True)
class _LoadedDeltaInputs:
    binding: I3ProductionDeltaInputBinding
    parent: LoadedI3ProductionStaging
    i2_run: object
    source_binding: streaming.S7StreamingSourceBinding
    gate_b_by_composite: Mapping[str, Mapping[str, object]]
    registries: LoadedRegistryReleaseSet
    policy_snapshot: object
    calendar_sessions: tuple[date, ...]


@dataclass(frozen=True, slots=True)
class _DeltaInputEnvelope:
    """Lightweight exact pins expanded before any large payload is loaded."""

    parent: LoadedI3ProductionStaging
    parent_boundary_partitions: tuple[I3ProductionPartitionPin, ...]
    i2_receipt: S4SessionRunReceipt
    source_binding: streaming.S7StreamingSourceBinding
    source_binding_artifact: ArtifactPin
    gate_b_manifest_artifact: ArtifactPin
    gate_b_data_artifact: ArtifactPin
    declared_input_artifacts: tuple[ArtifactPin, ...]
    parent_output_bytes: int
    parent_output_rows: int
    asset_transition_decision_count: int
    transitive_control_replay_bytes: int
    minimum_disk_free_bytes: int


@dataclass(frozen=True, slots=True)
class _RegistryDeclaredExpansion:
    artifacts: tuple[ArtifactPin, ...]
    asset_transition_decision_count: int
    transitive_control_replay_bytes: int


@dataclass(frozen=True, slots=True)
class _OpaqueReplaySubtree:
    subtree_id: str
    replay_bytes: int
    control_artifacts: tuple[ArtifactPin, ...]


_DELTA_ATTESTATION_SEAL: Final = object()
_MINTED_DELTA_ATTESTATIONS: Final[
    weakref.WeakValueDictionary[int, DeltaMaterializationAttestation]
] = weakref.WeakValueDictionary()


class ProductionDeltaMaterializer:
    """Executor seam that accepts no caller-authored data or identity facts."""

    def __init__(self, expected_run_spec_id: str) -> None:
        self._expected_run_spec_id = _digest(expected_run_spec_id, "delta RunSpec ID")

    def prepare(
        self,
        *,
        data_root: Path,
        run_spec: I3ProductionRunSpec,
        parent: LoadedI3ProductionStaging | None,
        workspace: Path,
    ) -> DeltaPreparedMaterialization:
        if run_spec.run_spec_id != self._expected_run_spec_id:
            raise I3DeltaIOError("delta materializer received another RunSpec")
        if parent is None:
            raise I3DeltaIOError("delta materializer requires an authenticated parent")
        return prepare_production_delta(
            data_root=data_root,
            run_spec=run_spec,
            parent=parent,
            workspace=workspace,
        )


def load_production_delta_materializer(
    *, data_root: Path, run_spec: I3ProductionRunSpec
) -> ProductionDeltaMaterializer:
    """CLI loader that preflights exact pins before returning the bounded seam."""

    _require_delta_controls(run_spec, parent=None)
    root = data_root.expanduser().resolve()
    _preflight_delta_entry_resources(
        root,
        run_spec.resource_caps,
        _delta_run_spec_direct_artifacts(run_spec),
    )
    parent = load_i3_production_parent_shallow_exact(root, run_spec)
    if parent is None:
        raise I3DeltaIOError("production DELTA loader did not authenticate a parent")
    _preflight_delta_input_envelope(
        data_root=root,
        run_spec=run_spec,
        parent=parent,
    )
    return ProductionDeltaMaterializer(run_spec.run_spec_id)


def store_i3_production_delta_config(
    data_root: Path,
    config: I3ProductionDeltaRunConfig,
) -> ArtifactPin:
    """Immutably persist one canonical, explicit first-DELTA input config."""

    if not isinstance(config, I3ProductionDeltaRunConfig):
        raise I3DeltaIOError("production DELTA config is not typed")
    root = data_root.expanduser().resolve()
    relative = _delta_config_relative(config)
    pin = _write_control_immutable(root, relative, config.canonical_bytes())
    if pin != config.exact_pin(path=relative):
        raise I3DeltaIOError("stored production DELTA config bytes changed")
    return pin


def load_i3_production_delta_config_exact(
    pin: ArtifactPin,
    *,
    data_root: Path,
) -> I3ProductionDeltaRunConfig:
    """Exact-load one canonical config at its module-owned content path."""

    if not isinstance(pin, ArtifactPin):
        raise I3DeltaIOError("production DELTA config pin is invalid")
    _explicit_path(pin.path)
    root = data_root.expanduser().resolve()
    content = _read_exact_artifact(root, pin)
    config = I3ProductionDeltaRunConfig.from_dict(_strict_json(content))
    if (
        config.canonical_bytes() != content
        or pin.path != _delta_config_relative(config)
        or config.exact_pin(path=pin.path) != pin
    ):
        raise I3DeltaIOError("production DELTA config is noncanonical or misplaced")
    return config


def prepare_i3_production_delta_run_spec(
    data_root: Path,
    config_pin: ArtifactPin,
) -> PreparedI3ProductionDeltaRunSpec:
    """Build and immutably persist one RunSpec from an exact DELTA config."""

    root = data_root.expanduser().resolve()
    config = load_i3_production_delta_config_exact(config_pin, data_root=root)
    run_spec = build_production_delta_run_spec(data_root=root, config=config)
    relative = (
        f"{_PRODUCTION_CONTROL_ROOT}/run-specs/run_spec_id={run_spec.run_spec_id}/run-spec.json"
    )
    run_spec_pin = _write_control_immutable(root, relative, run_spec.canonical_bytes())
    if run_spec_pin != run_spec.exact_pin(path=relative):
        raise I3DeltaIOError("stored production DELTA RunSpec bytes changed")
    return PreparedI3ProductionDeltaRunSpec(
        run_spec=run_spec,
        run_spec_artifact=run_spec_pin,
        config_artifact=config_pin,
    )


def build_production_delta_run_spec(
    *, data_root: Path, config: I3ProductionDeltaRunConfig
) -> I3ProductionRunSpec:
    """Derive a DELTA RunSpec from exact parent controls and one exact I2 receipt.

    The caller does not supply terminal, parent IDs, policy, calendar, schema,
    migration or transform facts.  They are read from the authenticated parent
    completion/deep chain and the exact I2 receipt.  Resource caps and the
    requested availability cutoff remain explicit operator controls.
    """

    if not isinstance(config, I3ProductionDeltaRunConfig):
        raise I3DeltaIOError("delta RunSpec builder requires a typed config")
    root = data_root.expanduser().resolve()
    _preflight_delta_entry_resources(
        root,
        config.resource_caps,
        (
            config.parent_completion_artifact,
            config.parent_deep_attestation_artifact,
            config.i2_receipt_artifact,
            *(
                (config.parent_pointer_event_artifact,)
                if config.parent_pointer_event_artifact
                else ()
            ),
        ),
    )

    def reader(relative: str) -> bytes:
        return _read_exact_path(root, relative)

    completion = load_i3_production_completion_exact(
        config.parent_completion_artifact,
        reader,
    )
    if completion.exact_pin(path=config.parent_completion_artifact.path) != (
        config.parent_completion_artifact
    ):
        raise I3DeltaIOError("parent completion exact pin does not reproduce")
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader)
    if receipt.output_set is None:
        raise I3DeltaIOError("parent completion does not name a successful OutputSet")
    parent_spec = load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader)
    deep = load_i3_production_deep_attestation_exact(
        config.parent_deep_attestation_artifact,
        reader,
    )
    output_set = receipt.output_set
    if (
        completion.receipt_id != receipt.receipt_id
        or completion.run_spec_id != parent_spec.run_spec_id
        or receipt.run_spec_id != parent_spec.run_spec_id
        or completion.output_set_id != output_set.output_set_id
        or completion.release_id != output_set.gate_a_manifest_pin.release_id
        or completion.native_v2_envelope_id != output_set.release_id
        or completion.checkpoint_id != output_set.checkpoint_id
        or deep.completion_id != completion.completion_id
        or deep.completion_artifact != config.parent_completion_artifact
        or deep.output_set_id != output_set.output_set_id
        or deep.checkpoint_id != output_set.checkpoint_id
        or deep.checkpoint_artifact != output_set.checkpoint_artifact
        or deep.gate_a_manifest_pin != output_set.gate_a_manifest_pin
    ):
        raise I3DeltaIOError("parent completion, receipt and deep controls do not reconcile")
    expected_parent_control_root = (
        f"{_PRODUCTION_CONTROL_ROOT}/run_spec_id={parent_spec.run_spec_id}"
    )
    if (
        config.parent_completion_artifact.path != f"{expected_parent_control_root}/completion.json"
        or config.parent_deep_attestation_artifact.path
        != f"{expected_parent_control_root}/deep-verification-attestation.json"
    ):
        raise I3DeltaIOError("parent completion/deep paths are not canonical production controls")
    if (
        parent_spec.run_kind is not I3ProductionRunKind.BASE
        or parent_spec.terminal_session != PRODUCTION_FIRST_DELTA_PARENT_SESSION
    ):
        raise I3DeltaIOError("first production DELTA requires the exact 2026-07-09 BASE")

    native_content = _read_exact_artifact(root, output_set.release_manifest_artifact)
    native = NativeV2ReleaseManifest.from_dict(_strict_json(native_content))
    if (
        native.canonical_bytes() != native_content
        or native.release_id != output_set.release_id
        or native.terminal_session != parent_spec.terminal_session
    ):
        raise I3DeltaIOError("parent native manifest does not reproduce")
    parent_release = NativeV2ParentReleasePin.from_manifest(
        native,
        path=output_set.release_manifest_artifact.path,
    )

    i2_content = _read_exact_artifact(root, config.i2_receipt_artifact)
    i2_receipt = S4SessionRunReceipt.from_dict(_strict_json(i2_content))
    if (
        _canonical_json_bytes(i2_receipt.to_dict()) != i2_content
        or i2_receipt.session_date != PRODUCTION_FIRST_DELTA_SESSION
        or config.i2_receipt_artifact.path != _PRODUCTION_FIRST_DELTA_I2_RECEIPT_PATH
        or i2_receipt.receipt_available_session > config.run_available_session
    ):
        raise I3DeltaIOError("delta I2 receipt differs from the fixed target boundary")
    i2_pin = I3ProductionI2ReceiptPin(
        session_date=i2_receipt.session_date,
        receipt_id=i2_receipt.receipt_id,
        artifact=config.i2_receipt_artifact,
        receipt_available_session=i2_receipt.receipt_available_session,
    )
    candidate = I3ProductionRunSpec(
        run_kind=I3ProductionRunKind.DELTA,
        terminal_session=PRODUCTION_FIRST_DELTA_SESSION,
        source_cutoff_session=parent_spec.source_cutoff_session,
        run_available_session=config.run_available_session,
        native_v2_migration_id=parent_spec.native_v2_migration_id,
        transform_semantics_digest=parent_spec.transform_semantics_digest,
        i0_oracle=parent_spec.i0_oracle,
        s4_v1_source=parent_spec.s4_v1_source,
        identity_policy_bundle=parent_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=parent_spec.identity_policy_bundle_artifact,
        calendar=parent_spec.calendar,
        v2_contracts=production_v2_contract_pins(),
        i2_receipts=(i2_pin,),
        resource_caps=config.resource_caps,
        parent_release=parent_release,
        parent_checkpoint_artifact=output_set.checkpoint_artifact,
        parent_gate_a_manifest=output_set.gate_a_manifest_pin,
        parent_shadow_completion_artifact=config.parent_completion_artifact,
        parent_deep_attestation_artifact=config.parent_deep_attestation_artifact,
        parent_authority=config.parent_authority,
        parent_pointer_event_artifact=config.parent_pointer_event_artifact,
    )
    parent = load_i3_production_parent_shallow_exact(root, candidate)
    if parent is None or parent.run_spec != parent_spec or parent.manifest != native:
        raise I3DeltaIOError("module-owned shallow parent loader returned another parent")
    _preflight_delta_input_envelope(
        data_root=root,
        run_spec=candidate,
        parent=parent,
    )
    return candidate


def _delta_parent_boundary(
    parent: LoadedI3ProductionStaging,
) -> tuple[I3ProductionPartitionPin, ...]:
    parent_output_set = parent.receipt.output_set
    if parent_output_set is None:  # pragma: no cover - authenticated loader proves
        raise I3DeltaIOError("authenticated parent lost its OutputSet")
    parent_universe = parent_output_set.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    parent_index = parent_universe.dataset_index
    if parent_index is None:
        raise I3DeltaIOError("authenticated parent lost its universe dataset index")
    by_session = {item.session_date: item for item in parent_index.partitions}
    try:
        boundary = tuple(by_session[session] for session in PRODUCTION_DELTA_BOUNDARY_SESSIONS[:2])
    except KeyError as exc:
        raise I3DeltaIOError("parent does not contain the exact 2026-07-08..09 boundary") from exc
    if parent_index.partitions[-2:] != boundary:
        raise I3DeltaIOError("parent boundary is not the terminal two-session prefix")
    return boundary


def _delta_parent_control_artifacts(
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
) -> tuple[ArtifactPin, ...]:
    if (
        run_spec.parent_release is None
        or run_spec.parent_checkpoint_artifact is None
        or run_spec.parent_gate_a_manifest is None
        or run_spec.parent_shadow_completion_artifact is None
        or run_spec.parent_deep_attestation_artifact is None
    ):
        raise I3DeltaIOError("delta parent controls are incomplete")
    gate_a = ArtifactPin(
        path=run_spec.parent_gate_a_manifest.manifest_path,
        sha256=run_spec.parent_gate_a_manifest.manifest_sha256,
        bytes=run_spec.parent_gate_a_manifest.manifest_bytes,
    )
    output_set = parent.receipt.output_set
    if output_set is None:  # pragma: no cover - authenticated loader proves
        raise I3DeltaIOError("delta parent output set is absent")
    return _unique_artifact_pins(
        (
            run_spec.parent_release.manifest,
            run_spec.parent_checkpoint_artifact,
            gate_a,
            run_spec.parent_shadow_completion_artifact,
            run_spec.parent_deep_attestation_artifact,
            parent.completion.receipt_artifact,
            parent.receipt.run_spec_artifact,
            output_set.gate_a_run_spec_pin.artifact,
            output_set.gate_a_run_receipt_pin.artifact,
            *(item.manifest_output.artifact for item in output_set.table_outputs),
            *output_set.control_extension_artifacts,
            *(
                (run_spec.parent_pointer_event_artifact,)
                if run_spec.parent_pointer_event_artifact
                else ()
            ),
        )
    )


def _unique_artifact_pins(pins: Sequence[ArtifactPin]) -> tuple[ArtifactPin, ...]:
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        if not isinstance(pin, ArtifactPin):
            raise I3DeltaIOError("delta declared input contains an invalid artifact pin")
        _explicit_path(pin.path)
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise I3DeltaIOError("one delta input path carries conflicting exact pins")
        by_path[pin.path] = pin
    return tuple(by_path[path] for path in sorted(by_path))


def _delta_run_spec_direct_artifacts(
    run_spec: I3ProductionRunSpec,
) -> tuple[ArtifactPin, ...]:
    parent_release = run_spec.parent_release
    gate_a = run_spec.parent_gate_a_manifest
    direct = (
        None if parent_release is None else parent_release.manifest,
        run_spec.parent_checkpoint_artifact,
        run_spec.parent_shadow_completion_artifact,
        run_spec.parent_deep_attestation_artifact,
        run_spec.i2_receipts[0].artifact,
        (
            None
            if gate_a is None
            else ArtifactPin(
                path=gate_a.manifest_path,
                sha256=gate_a.manifest_sha256,
                bytes=gate_a.manifest_bytes,
            )
        ),
        run_spec.parent_pointer_event_artifact,
    )
    if any(item is None for item in direct[:-1]):
        raise I3DeltaIOError("delta RunSpec lacks a direct parent control artifact")
    return _unique_artifact_pins(tuple(item for item in direct if item is not None))


def _artifact_pin_from_path_sha(root: Path, path: str, sha256: str) -> ArtifactPin:
    _explicit_path(path)
    exact = safe_relative_path(root, path)
    try:
        metadata = exact.lstat()
    except OSError as exc:
        raise I3DeltaIOError(f"delta declared artifact is unavailable: {path}") from exc
    if not stat.S_ISREG(metadata.st_mode) or exact.is_symlink():
        raise I3DeltaIOError(f"delta declared artifact is not a regular file: {path}")
    return ArtifactPin(
        path=path,
        sha256=_digest(sha256, "declared artifact SHA"),
        bytes=metadata.st_size,
    )


def _expand_i2_declared_artifacts(
    root: Path,
    receipt: S4SessionRunReceipt,
    *,
    run_spec: I3ProductionRunSpec,
) -> tuple[ArtifactPin, ...]:
    """Expand the exact I2 receipt into the payloads its authenticator reads."""

    content = _read_exact_artifact(root, receipt.run_spec_artifact)
    i2_spec = S4SessionRunSpec.from_dict(_strict_json(content))
    if (
        _canonical_json_bytes(i2_spec.to_dict()) != content
        or i2_spec.run_spec_id != receipt.run_spec_id
        or i2_spec.source_binding.source_binding_id != receipt.source_binding_id
    ):
        raise I3DeltaIOError("I2 preflight RunSpec differs from its receipt")
    if i2_spec.parent_frontier.parent_kind is not S4ParentKind.BASE_RELEASE:
        raise I3DeltaIOError("first production DELTA I2 parent is not the exact BASE frontier")
    parent_content = _read_exact_artifact(root, i2_spec.parent_frontier.artifact)
    parent_frontier = S4BaseFrontier.from_dict(_strict_json(parent_content))
    if (
        _canonical_json_bytes(parent_frontier.to_dict()) != parent_content
        or parent_frontier.frontier_id != i2_spec.parent_frontier.terminal_receipt_id
        or parent_frontier.terminal_session != PRODUCTION_FIRST_DELTA_PARENT_SESSION
    ):
        raise I3DeltaIOError("I2 preflight BASE frontier differs from its exact pin")
    base_release_content = _read_exact_artifact(
        root,
        parent_frontier.base_release_set_artifact,
    )
    base_release = AssetReleaseSet.from_dict(_strict_json(base_release_content))
    if base_release.release_set_id != parent_frontier.base_release_set_id:
        raise I3DeltaIOError("I2 preflight BASE release set differs from its frontier")
    expanded: list[ArtifactPin] = [
        receipt.run_spec_artifact,
        receipt.qa_details_artifact,
        *(item.artifact for item in receipt.partition_receipts),
        i2_spec.calendar_artifact,
        i2_spec.parent_frontier.artifact,
        parent_frontier.base_release_set_artifact,
        *i2_spec.reference_binding.dependency_pins,
        _artifact_pin_from_path_sha(
            root,
            base_release.intent_path,
            base_release.intent_sha256,
        ),
        _artifact_pin_from_path_sha(
            root,
            base_release.group_approval_path,
            base_release.group_approval_sha256,
        ),
        ArtifactPin(
            path=base_release.publish_plan_path,
            sha256=base_release.publish_plan_sha256,
            bytes=base_release.publish_plan_bytes,
        ),
    ]
    expanded.extend(
        _artifact_pin_from_path_sha(
            root,
            (
                "manifests/silver/full-run-plans/"
                f"{member.table}/plan_id={member.full_run_plan_id}/manifest.json"
            ),
            member.full_run_plan_sha256,
        )
        for member in base_release.members
    )
    expanded.extend(
        _artifact_pin_from_path_sha(root, item.path, item.sha256)
        for item in i2_spec.source_binding.inventory.upstream_manifests
    )
    for release_pin in i2_spec.reference_binding.dependency_pins:
        release_content = _read_exact_artifact(root, release_pin)
        release = ReleaseManifest.from_dict(_strict_json(release_content))
        if release.release_id not in release_pin.path:
            raise I3DeltaIOError("I2 reference release path differs from its exact ID")
        expanded.extend(
            ArtifactPin(path=item.path, sha256=item.sha256, bytes=item.bytes)
            for item in release.outputs
        )
        expanded.append(
            _artifact_pin_from_path_sha(
                root,
                (
                    "manifests/silver/builds/"
                    f"{release.table}/build_id={release.build_id}/manifest.json"
                ),
                release.build_manifest_sha256,
            )
        )
        expanded.append(
            _artifact_pin_from_path_sha(
                root,
                f"manifests/silver/approvals/{release.approval_id}.json",
                release.approval_sha256,
            )
        )
        _check_live_resources(root, run_spec)
    return _unique_artifact_pins(tuple(expanded))


def _artifact_pin_from_mapping(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, Mapping):
        raise I3DeltaIOError(f"{label} is not an exact artifact mapping")
    try:
        pin = ArtifactPin(
            path=value["path"],
            sha256=value["sha256"],
            bytes=value["bytes"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise I3DeltaIOError(f"{label} exact artifact fields are invalid") from exc
    _explicit_path(pin.path)
    return pin


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise I3DeltaIOError(f"{label} must be an object")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I3DeltaIOError(f"{label} must be a nonnegative native integer")
    return value


def _validate_transitive_control_replay_bytes(value: object) -> int:
    replay_bytes = _native_nonnegative_int(
        value,
        "delta transitive control replay bytes",
    )
    if replay_bytes > I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP:
        raise I3DeltaIOError("delta transitive control replay exceeds the module-owned byte cap")
    return replay_bytes


def _canonical_control_document(
    root: Path,
    pin: ArtifactPin,
    label: str,
) -> tuple[Mapping[str, object], bytes]:
    content = _read_exact_artifact(root, pin)
    document = _strict_json(content)
    canonical = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    if canonical != content:
        raise I3DeltaIOError(f"{label} is not canonical JSON")
    return document, content


def _binding_artifact(binding: object, label: str) -> ArtifactPin:
    try:
        pin = ArtifactPin(
            path=binding.path,
            sha256=binding.sha256,
            bytes=binding.bytes,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise I3DeltaIOError(f"{label} exact binding is invalid") from exc
    _explicit_path(pin.path)
    return pin


def _exact_group_opaque_replay_subtree(
    root: Path,
    source_by_role: Mapping[str, object],
) -> _OpaqueReplaySubtree:
    """Authenticate one exact-group tree through its aggregate completion."""

    from ame_stocks_api.silver.identity_exact_group_history_runner import (
        S7ExactGroupHistoryCandidate,
        S7ExactGroupHistoryCompletion,
    )

    candidate_binding = source_by_role["source_exact_group_candidate_manifest"]
    completion_binding = source_by_role["source_exact_group_completion_manifest"]
    candidate_pin = _binding_artifact(candidate_binding, "exact-group candidate")
    completion_pin = _binding_artifact(completion_binding, "exact-group completion")
    _validate_transitive_control_replay_bytes(candidate_pin.bytes)
    candidate_document, candidate_content = _canonical_control_document(
        root,
        candidate_pin,
        "exact-group candidate",
    )
    _validate_transitive_control_replay_bytes(candidate_pin.bytes + completion_pin.bytes)
    completion_document, completion_content = _canonical_control_document(
        root,
        completion_pin,
        "exact-group completion",
    )
    try:
        candidate = S7ExactGroupHistoryCandidate.from_dict(candidate_document)
        completion = S7ExactGroupHistoryCompletion.from_dict(completion_document)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise I3DeltaIOError("exact-group aggregate controls are invalid") from exc
    expected_candidate_path = f"{candidate.relative_directory}/manifest.json"
    if (
        candidate.content != candidate_content
        or completion.content != completion_content
        or candidate_binding.artifact_id != candidate.candidate_id
        or completion_binding.artifact_id != completion.completion_id
        or candidate_pin.path != expected_candidate_path
        or candidate_pin.sha256 != candidate.sha256
        or completion_pin.sha256 != completion.sha256
        or completion.candidate_id != candidate.candidate_id
        or completion.candidate_path != candidate_pin.path
        or completion.candidate_sha256 != candidate_pin.sha256
        or completion.output_artifacts != candidate.artifacts
    ):
        raise I3DeltaIOError("exact-group candidate/completion aggregate binding differs")
    expected_tree_bytes = candidate_pin.bytes + sum(item.bytes for item in candidate.artifacts)
    if completion.output_bytes != expected_tree_bytes:
        raise I3DeltaIOError("exact-group completion tree byte aggregate differs")
    replay_bytes = _validate_transitive_control_replay_bytes(
        completion.output_bytes + completion_pin.bytes
    )
    return _OpaqueReplaySubtree(
        subtree_id=completion.completion_id,
        replay_bytes=replay_bytes,
        control_artifacts=_unique_artifact_pins((candidate_pin, completion_pin)),
    )


_GATE_C_OUTPUT_ROLES: Final = frozenset(
    {
        "bounded_examples",
        "daily_reason_counts",
        "interval_data",
        "qa",
        "reviewed_foreign_source_evidence",
    }
)
_GATE_C_REGISTRY_LOADER_SOURCE_REF_ROLES: Final = frozenset(
    {
        "detector_preview",
        "detector_preview_completion",
        "gate_a_candidate",
        "gate_a_completion",
        "gate_b_candidate",
        "gate_b_data",
        "reviewed_case_evidence",
        "reviewed_external_evidence",
    }
)


def _gate_c_opaque_replay_subtree(
    root: Path,
    source_by_role: Mapping[str, object],
) -> _OpaqueReplaySubtree:
    """Authenticate one Gate-C output set without enumerating its leaf paths."""

    candidate_binding = source_by_role["source_gate_c_candidate_manifest"]
    completion_binding = source_by_role["source_gate_c_completion_manifest"]
    candidate_pin = _binding_artifact(candidate_binding, "Gate-C candidate")
    completion_pin = _binding_artifact(completion_binding, "Gate-C completion")
    _validate_transitive_control_replay_bytes(candidate_pin.bytes)
    candidate, _ = _canonical_control_document(root, candidate_pin, "Gate-C candidate")
    _validate_transitive_control_replay_bytes(candidate_pin.bytes + completion_pin.bytes)
    completion, _ = _canonical_control_document(root, completion_pin, "Gate-C completion")

    completion_payload = dict(completion)
    completion_id = _digest(
        completion_payload.pop("completion_id", None),
        "Gate-C completion ID",
    )
    candidate_payload = dict(candidate)
    candidate_manifest_id = _digest(
        candidate_payload.pop("manifest_id", None),
        "Gate-C candidate manifest ID",
    )
    candidate_id = _digest(candidate.get("candidate_id"), "Gate-C candidate ID")
    if (
        stable_digest(completion_payload) != completion_id
        or stable_digest(candidate_payload) != candidate_manifest_id
        or completion_binding.artifact_id != completion_id
        or candidate_binding.artifact_id != candidate_id
    ):
        raise I3DeltaIOError("Gate-C aggregate control identity differs")

    candidate_ref = _require_mapping(completion.get("candidate"), "Gate-C candidate ref")
    if (
        candidate_ref.get("candidate_id") != candidate_id
        or candidate_ref.get("manifest_id") != candidate_manifest_id
        or candidate_ref.get("path") != candidate_pin.path
        or candidate_ref.get("sha256") != candidate_pin.sha256
        or candidate_ref.get("bytes") != candidate_pin.bytes
    ):
        raise I3DeltaIOError("Gate-C completion names another candidate")

    candidate_outputs = _require_mapping(candidate.get("outputs"), "Gate-C outputs")
    completion_outputs = _require_mapping(
        completion.get("outputs"),
        "Gate-C completion outputs",
    )
    if set(candidate_outputs) != _GATE_C_OUTPUT_ROLES or completion_outputs != candidate_outputs:
        raise I3DeltaIOError("Gate-C output aggregate role set differs")
    output_pins = tuple(
        _artifact_pin_from_mapping(value, f"Gate-C {role} output")
        for role, value in sorted(candidate_outputs.items())
    )
    output_bytes = sum(item.bytes for item in output_pins)
    _validate_transitive_control_replay_bytes(
        output_bytes + candidate_pin.bytes + completion_pin.bytes
    )
    candidate_resources = _require_mapping(
        candidate.get("resource_measurements"),
        "Gate-C candidate resources",
    )
    completion_resources = _require_mapping(
        completion.get("resource_measurements"),
        "Gate-C completion resources",
    )
    if (
        completion_resources != candidate_resources
        or _native_nonnegative_int(
            candidate_resources.get("output_bytes"),
            "Gate-C aggregate output bytes",
        )
        != output_bytes
    ):
        raise I3DeltaIOError("Gate-C output byte aggregate differs")

    source_refs = _require_mapping(
        candidate.get("registry_loader_source_refs"),
        "Gate-C registry-loader source refs",
    )
    if set(source_refs) != _GATE_C_REGISTRY_LOADER_SOURCE_REF_ROLES or any(
        not isinstance(value, Mapping) for value in source_refs.values()
    ):
        raise I3DeltaIOError("Gate-C registry-loader source-ref schema differs")
    detector_preview = _require_mapping(
        source_refs["detector_preview"],
        "Gate-C detector preview",
    )
    detector_preview_pin = _artifact_pin_from_mapping(
        detector_preview,
        "Gate-C detector preview",
    )
    _digest(
        detector_preview.get("preview_artifact_id"),
        "Gate-C detector preview ID",
    )
    _validate_transitive_control_replay_bytes(
        output_bytes + candidate_pin.bytes + completion_pin.bytes + detector_preview_pin.bytes
    )

    plan_ref = _require_mapping(completion.get("plan"), "Gate-C plan ref")
    authorization_ref = _require_mapping(
        completion.get("authorization"),
        "Gate-C authorization ref",
    )
    try:
        plan_pin = _artifact_pin_from_path_sha(
            root,
            str(plan_ref["path"]),
            str(plan_ref["sha256"]),
        )
        authorization_pin = _artifact_pin_from_path_sha(
            root,
            str(authorization_ref["path"]),
            str(authorization_ref["sha256"]),
        )
    except KeyError as exc:
        raise I3DeltaIOError("Gate-C aggregate control ref is incomplete") from exc
    controls = _unique_artifact_pins(
        (
            candidate_pin,
            completion_pin,
            plan_pin,
            authorization_pin,
            detector_preview_pin,
        )
    )
    replay_bytes = _validate_transitive_control_replay_bytes(
        output_bytes + sum(item.bytes for item in controls)
    )
    plan, _ = _canonical_control_document(root, plan_pin, "Gate-C plan")
    authorization, _ = _canonical_control_document(
        root,
        authorization_pin,
        "Gate-C authorization",
    )
    if plan.get("plan_id") != plan_ref.get("plan_id") or authorization.get(
        "authorization_id"
    ) != authorization_ref.get("authorization_id"):
        raise I3DeltaIOError("Gate-C plan/authorization aggregate binding differs")

    return _OpaqueReplaySubtree(
        subtree_id=completion_id,
        replay_bytes=replay_bytes,
        control_artifacts=controls,
    )


def _registry_opaque_replay_subtree(
    root: Path,
    candidate: RegistryCandidateManifest,
) -> _OpaqueReplaySubtree:
    source_by_role = {item.role: item for item in candidate.source_artifacts}
    roles = set(source_by_role)
    if roles == {
        "source_exact_group_candidate_manifest",
        "source_exact_group_completion_manifest",
    }:
        return _exact_group_opaque_replay_subtree(root, source_by_role)
    if roles == {
        "source_gate_c_candidate_manifest",
        "source_gate_c_completion_manifest",
    }:
        return _gate_c_opaque_replay_subtree(root, source_by_role)
    raise I3DeltaIOError("registry replay source roles are unknown or incomplete")


def _expand_registry_ingress_artifacts(
    root: Path,
    candidate: RegistryCandidateManifest,
) -> tuple[ArtifactPin, ...]:
    ingress = candidate.production_ingress_artifact
    if ingress is None:
        return ()
    ingress_pin = ArtifactPin(path=ingress.path, sha256=ingress.sha256, bytes=ingress.bytes)
    ingress_document = _strict_json(_read_exact_artifact(root, ingress_pin))
    import_pin = _artifact_pin_from_mapping(
        ingress_document.get("evidence_import_artifact"),
        "registry evidence import",
    )
    import_document = _strict_json(_read_exact_artifact(root, import_pin))
    manifest_pin = _artifact_pin_from_mapping(
        import_document.get("manifest"),
        "registry external evidence manifest",
    )
    raw_from_import = tuple(
        _artifact_pin_from_mapping(item, "registry imported raw evidence")
        for item in _mapping_array(
            import_document.get("raw_artifacts"),
            "registry imported raw evidence",
        )
    )
    evidence_document = _strict_json(_read_exact_artifact(root, manifest_pin))
    raw_from_manifest = tuple(
        _artifact_pin_from_mapping(item, "registry manifest raw evidence")
        for item in _mapping_array(
            evidence_document.get("artifacts"),
            "registry manifest raw evidence",
        )
    )
    imported = {item.path: item for item in raw_from_import}
    if any(imported.get(item.path) != item for item in raw_from_manifest):
        raise I3DeltaIOError("registry evidence import and manifest raw pins differ")
    return _unique_artifact_pins(
        (ingress_pin, import_pin, manifest_pin, *raw_from_import, *raw_from_manifest)
    )


def _expand_registry_declared_artifacts(
    root: Path,
    pins: Sequence[RegistryReleasePin],
    *,
    run_spec: I3ProductionRunSpec,
) -> _RegistryDeclaredExpansion:
    """Expand exact controls and authenticate opaque replay-subtree aggregates."""

    expanded: list[ArtifactPin] = []
    transition_count: int | None = None
    opaque_subtrees: dict[str, _OpaqueReplaySubtree] = {}
    for pin in pins:
        manifest_pin = ArtifactPin(
            path=pin.manifest_path,
            sha256=pin.manifest_sha256,
            bytes=pin.manifest_bytes,
        )
        manifest_content = _read_exact_artifact(root, manifest_pin)
        manifest = RegistryReleaseManifest.from_dict(_strict_json(manifest_content))
        if (
            manifest.registry_name != pin.registry_name
            or manifest.release_id != pin.release_id
            or manifest.relative_path != pin.manifest_path
            or manifest.release_available_session != pin.release_available_session
        ):
            raise I3DeltaIOError("registry preflight manifest differs from its exact pin")
        if manifest.registry_name == "asset_transition":
            transition_count = manifest.row_count
        candidate_pin = ArtifactPin(
            path=manifest.candidate.path,
            sha256=manifest.candidate.sha256,
            bytes=manifest.candidate.bytes,
        )
        candidate_content = _read_exact_artifact(root, candidate_pin)
        candidate = RegistryCandidateManifest.from_dict(_strict_json(candidate_content))
        if (
            candidate.candidate_id != manifest.candidate.object_id
            or candidate.relative_path != manifest.candidate.path
            or candidate.registry_name != manifest.registry_name
        ):
            raise I3DeltaIOError("registry preflight candidate differs from its exact pin")
        release_dir = PurePosixPath(manifest.release_directory)
        member_pins = (
            ArtifactPin(
                path=(release_dir / manifest.rows_path).as_posix(),
                sha256=manifest.rows_sha256,
                bytes=manifest.rows_bytes,
            ),
            *(
                ArtifactPin(
                    path=(release_dir / item.path).as_posix(),
                    sha256=item.sha256,
                    bytes=item.bytes,
                )
                for item in manifest.decisions
            ),
        )
        control_pins = tuple(
            ArtifactPin(path=item.path, sha256=item.sha256, bytes=item.bytes)
            for item in (
                manifest.candidate,
                manifest.plan,
                manifest.request,
                manifest.approval_receipt,
                manifest.publish_intent,
            )
        )
        candidate_inputs = tuple(
            ArtifactPin(path=item.path, sha256=item.sha256, bytes=item.bytes)
            for item in (
                *candidate.source_artifacts,
                *candidate.evidence_artifacts,
                *candidate.authorization_artifacts,
                *(
                    (candidate.production_ingress_artifact,)
                    if candidate.production_ingress_artifact
                    else ()
                ),
            )
        )
        ingress_inputs = _expand_registry_ingress_artifacts(root, candidate)
        opaque_subtree = _registry_opaque_replay_subtree(root, candidate)
        prior_subtree = opaque_subtrees.get(opaque_subtree.subtree_id)
        if prior_subtree is not None and prior_subtree != opaque_subtree:
            raise I3DeltaIOError(
                "one registry replay subtree identity carries conflicting aggregates"
            )
        opaque_subtrees[opaque_subtree.subtree_id] = opaque_subtree
        replay_bytes = sum(item.replay_bytes for item in opaque_subtrees.values())
        _validate_transitive_control_replay_bytes(replay_bytes)
        expanded.extend(
            (
                manifest_pin,
                *control_pins,
                *member_pins,
                *candidate_inputs,
                *ingress_inputs,
                *opaque_subtree.control_artifacts,
            )
        )
        _check_live_resources(root, run_spec)
    if transition_count is None:
        raise I3DeltaIOError("registry preflight lacks asset-transition authority")
    return _RegistryDeclaredExpansion(
        artifacts=_unique_artifact_pins(tuple(expanded)),
        asset_transition_decision_count=transition_count,
        transitive_control_replay_bytes=_validate_transitive_control_replay_bytes(
            sum(item.replay_bytes for item in opaque_subtrees.values())
        ),
    )


def _preflight_delta_input_envelope(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging | None,
) -> _DeltaInputEnvelope:
    """Expand exact lightweight controls and enforce caps before payload reads."""

    _require_delta_controls(run_spec, parent=parent)
    root = data_root.expanduser().resolve()
    loaded_parent = parent or load_i3_production_parent_shallow_exact(root, run_spec)
    if loaded_parent is None:
        raise I3DeltaIOError("delta preflight lacks an authenticated parent")
    _require_delta_controls(run_spec, parent=loaded_parent)
    minimum_disk = _check_live_resources(root, run_spec)

    parent_boundary = _delta_parent_boundary(loaded_parent)
    i2_expected = run_spec.i2_receipts[0]
    i2_content = _read_exact_artifact(root, i2_expected.artifact)
    i2_receipt = S4SessionRunReceipt.from_dict(_strict_json(i2_content))
    if (
        _canonical_json_bytes(i2_receipt.to_dict()) != i2_content
        or i2_receipt.receipt_id != i2_expected.receipt_id
        or i2_receipt.session_date != i2_expected.session_date
        or i2_receipt.receipt_available_session != i2_expected.receipt_available_session
        or i2_receipt.session_date != PRODUCTION_FIRST_DELTA_SESSION
    ):
        raise I3DeltaIOError("delta preflight I2 receipt differs from its exact pin")
    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    marker_content = _read_exact_artifact(root, run_spec.i0_oracle.artifact)
    marker = load_frozen_s7_oracle_marker_exact(
        run_spec.i0_oracle.artifact,
        content=marker_content,
    )
    if marker.get("release_set_id") != run_spec.i0_oracle.object_id:
        raise I3DeltaIOError("frozen I0 marker returned another release set")
    source_binding_id = _digest(marker.get("source_binding_id"), "I0 source-binding ID")
    try:
        source_binding, source_pin = streaming._load_source_binding(root, source_binding_id)
    except streaming.S7StreamingMaterializationError as exc:
        raise I3DeltaIOError("frozen S7 source binding cannot be loaded exactly") from exc
    source_artifact = ArtifactPin(
        path=source_pin.path,
        sha256=source_pin.sha256,
        bytes=source_pin.bytes,
    )
    _explicit_path(source_artifact.path)
    if (
        source_binding.s4_release_set_id != run_spec.s4_v1_source.object_id
        or source_binding.cutoff_session != run_spec.source_cutoff_session
    ):
        raise I3DeltaIOError("delta source binding differs from the inherited frozen source")
    registry_pins = tuple(
        RegistryReleasePin(
            registry_name=item.registry_kind.value,
            release_id=item.release_id,
            manifest_path=item.artifact.path,
            manifest_sha256=item.artifact.sha256,
            manifest_bytes=item.artifact.bytes,
            release_available_session=item.release_available_session,
        )
        for item in run_spec.identity_policy_bundle.registry_releases
    )
    if tuple(source_binding.registry_pins) != registry_pins:
        raise I3DeltaIOError("delta policy differs from the frozen S7 source binding")
    gate_b_manifest = _artifact_from_exact_pin(source_binding.gate_b.manifest)
    gate_b_data = _artifact_from_exact_pin(source_binding.gate_b.data)
    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    registry_expansion = _expand_registry_declared_artifacts(
        root,
        registry_pins,
        run_spec=run_spec,
    )
    i2_expansion = _expand_i2_declared_artifacts(
        root,
        i2_receipt,
        run_spec=run_spec,
    )
    declared = _unique_artifact_pins(
        (
            run_spec.i0_oracle.artifact,
            run_spec.s4_v1_source.artifact,
            run_spec.identity_policy_bundle_artifact,
            run_spec.calendar.artifact,
            *(item.artifact for item in run_spec.identity_policy_bundle.registry_releases),
            i2_expected.artifact,
            i2_receipt.run_spec_artifact,
            i2_receipt.qa_details_artifact,
            *(item.artifact for item in i2_receipt.partition_receipts),
            *(item.artifact for item in parent_boundary),
            source_artifact,
            gate_b_manifest,
            gate_b_data,
            *_delta_parent_control_artifacts(run_spec, loaded_parent),
            *i2_expansion,
            *registry_expansion.artifacts,
        )
    )
    parent_output_set = loaded_parent.receipt.output_set
    if parent_output_set is None:  # pragma: no cover - authenticated loader proves
        raise I3DeltaIOError("authenticated parent lost its OutputSet")
    estimate = _estimate_production_delta_resources_from_pins(
        run_spec,
        parent_boundary_partitions=parent_boundary,
        i2_partitions=i2_receipt.partition_receipts,
        declared_input_artifacts=declared,
        parent_output_bytes=parent_output_set.total_output_bytes,
        parent_output_rows=parent_output_set.total_rows,
        asset_transition_decision_count=(registry_expansion.asset_transition_decision_count),
        transitive_control_replay_bytes=(registry_expansion.transitive_control_replay_bytes),
    )
    _validate_resource_estimate(run_spec, estimate)
    minimum_disk = min(
        minimum_disk,
        _check_preflight_resources(root, run_spec, estimate),
    )
    return _DeltaInputEnvelope(
        parent=loaded_parent,
        parent_boundary_partitions=parent_boundary,
        i2_receipt=i2_receipt,
        source_binding=source_binding,
        source_binding_artifact=source_artifact,
        gate_b_manifest_artifact=gate_b_manifest,
        gate_b_data_artifact=gate_b_data,
        declared_input_artifacts=declared,
        parent_output_bytes=parent_output_set.total_output_bytes,
        parent_output_rows=parent_output_set.total_rows,
        asset_transition_decision_count=(registry_expansion.asset_transition_decision_count),
        transitive_control_replay_bytes=(registry_expansion.transitive_control_replay_bytes),
        minimum_disk_free_bytes=minimum_disk,
    )


def load_production_delta_input_binding(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging | None = None,
) -> _LoadedDeltaInputs:
    """Replay the exact bounded DELTA inputs without reading historical data."""

    root = data_root.expanduser().resolve()
    envelope = _preflight_delta_input_envelope(
        data_root=root,
        run_spec=run_spec,
        parent=parent,
    )
    loaded_parent = envelope.parent

    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=run_spec.calendar.calendar_artifact_id,
        expected_sha256=run_spec.calendar.artifact.sha256,
    )
    calendar_pin = ArtifactPin(
        path=calendar.relative_path,
        sha256=calendar.sha256,
        bytes=len(calendar.content),
    )
    sessions = tuple(item.session_date for item in calendar.sessions)
    if calendar_pin != run_spec.calendar.artifact:
        raise I3DeltaIOError("delta calendar exact pin differs")
    try:
        target_index = sessions.index(PRODUCTION_FIRST_DELTA_SESSION)
    except ValueError as exc:
        raise I3DeltaIOError("delta target session is absent from the exact calendar") from exc
    if target_index < 2 or sessions[target_index - 2 : target_index + 1] != (
        PRODUCTION_DELTA_BOUNDARY_SESSIONS
    ):
        raise I3DeltaIOError("delta calendar does not reproduce the fixed three-session window")

    parent_boundary = envelope.parent_boundary_partitions

    i2_expected = run_spec.i2_receipts[0]
    i2_run = load_completed_s4_asset_session_run(root, i2_expected.session_date)
    if i2_run is None:
        raise I3DeltaIOError("exact I2 session receipt is not complete")
    i2_receipt = i2_run.receipt
    if (
        i2_run.receipt_artifact != i2_expected.artifact
        or i2_receipt != envelope.i2_receipt
        or i2_receipt.receipt_id != i2_expected.receipt_id
        or i2_receipt.receipt_available_session != i2_expected.receipt_available_session
        or i2_receipt.session_date != PRODUCTION_FIRST_DELTA_SESSION
    ):
        raise I3DeltaIOError("module-owned I2 loader returned another exact receipt")
    _verify_i2_parent_frontier(run_spec, loaded_parent, i2_run)
    for partition in i2_receipt.partition_receipts:
        contract = ASSET_CONTRACTS[partition.table_name]
        if (
            partition.contract_id != contract.contract_id
            or partition.schema_digest != contract.schema_digest
        ):
            raise I3DeltaIOError("I2 partition binds another exact schema")
        _verify_parquet_metadata(root, partition, contract.arrow_schema)
        _check_live_resources(root, run_spec)

    source_binding = envelope.source_binding
    source_artifact = envelope.source_binding_artifact
    gate_b_manifest = envelope.gate_b_manifest_artifact
    gate_b_data = envelope.gate_b_data_artifact
    _verify_exact_artifact(root, gate_b_manifest)
    _check_live_resources(root, run_spec)
    _verify_exact_artifact(root, gate_b_data)
    _check_live_resources(root, run_spec)

    registry_pins = tuple(
        RegistryReleasePin(
            registry_name=item.registry_kind.value,
            release_id=item.release_id,
            manifest_path=item.artifact.path,
            manifest_sha256=item.artifact.sha256,
            manifest_bytes=item.artifact.bytes,
            release_available_session=item.release_available_session,
        )
        for item in run_spec.identity_policy_bundle.registry_releases
    )
    _validate_transitive_control_replay_bytes(envelope.transitive_control_replay_bytes)
    loaded_releases = []
    for registry_pin in registry_pins:
        _check_live_resources(root, run_spec)
        loaded_releases.append(
            load_registry_release(
                root,
                registry_pin,
                revalidate_current_runtime=False,
            )
        )
        _validate_transitive_control_replay_bytes(envelope.transitive_control_replay_bytes)
        _check_live_resources(root, run_spec)
    registries = LoadedRegistryReleaseSet(tuple(loaded_releases))
    registries.validate_all_composite_scopes_are_exclusive()
    policy_snapshot = load_production_identity_policy_snapshot(
        registries,
        run_spec.identity_policy_bundle,
    )
    if policy_snapshot.production_release_set_binding_digest is None:
        raise I3DeltaIOError("delta policy snapshot lacks production release authority")
    try:
        gate_b = streaming._load_gate_b_reference(root, source_binding.gate_b)
    except streaming.S7StreamingMaterializationError as exc:
        raise I3DeltaIOError("exact Gate-B inventory cannot be loaded") from exc
    _check_live_resources(root, run_spec)

    deep = loaded_parent.deep_attestation
    if deep is None:  # pragma: no cover - shallow loader always carries it
        raise I3DeltaIOError("authenticated parent lacks its deep attestation")
    binding = I3ProductionDeltaInputBinding(
        run_spec_id=run_spec.run_spec_id,
        parent_release_id=loaded_parent.manifest.release_id,
        parent_checkpoint_id=loaded_parent.checkpoint.checkpoint_id,
        parent_deep_attestation_id=deep.deep_attestation_id,
        parent_completion_artifact=run_spec.parent_shadow_completion_artifact,
        parent_deep_attestation_artifact=run_spec.parent_deep_attestation_artifact,
        i2_receipt_id=i2_receipt.receipt_id,
        i2_receipt_artifact=i2_expected.artifact,
        i2_partitions=i2_receipt.partition_receipts,
        parent_boundary_partitions=parent_boundary,
        requested_sessions=PRODUCTION_DELTA_BOUNDARY_SESSIONS,
        source_binding_id=source_binding.source_binding_id,
        source_binding_artifact=source_artifact,
        gate_b_manifest_artifact=gate_b_manifest,
        gate_b_data_artifact=gate_b_data,
        declared_input_artifacts=envelope.declared_input_artifacts,
        parent_output_bytes=envelope.parent_output_bytes,
        parent_output_rows=envelope.parent_output_rows,
        asset_transition_decision_count=(envelope.asset_transition_decision_count),
        transitive_control_replay_bytes=(envelope.transitive_control_replay_bytes),
        policy_snapshot_id=policy_snapshot.policy_snapshot_id,
        policy_release_set_binding_digest=(policy_snapshot.production_release_set_binding_digest),
    )
    return _LoadedDeltaInputs(
        binding=binding,
        parent=loaded_parent,
        i2_run=i2_run,
        source_binding=source_binding,
        gate_b_by_composite=gate_b,
        registries=registries,
        policy_snapshot=policy_snapshot,
        calendar_sessions=sessions,
    )


def prepare_production_delta(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    workspace: Path,
) -> DeltaPreparedMaterialization:
    """Materialize the immutable 2026-07-10 clean append from exact controls."""

    started = time.monotonic()
    root = data_root.expanduser().resolve()
    work = workspace.expanduser().resolve()
    _require_delta_controls(run_spec, parent=parent)
    _require_empty_workspace(root, work)
    loaded = load_production_delta_input_binding(
        data_root=root,
        run_spec=run_spec,
        parent=parent,
    )
    estimate = estimate_production_delta_resources(run_spec, loaded.binding)
    _validate_resource_estimate(run_spec, estimate)
    minimum_disk = _check_preflight_resources(root, run_spec, estimate)

    source_digest = _delta_source_digest(run_spec, loaded.binding)
    lookback_rows: list[dict[str, object]] = []
    for pin in loaded.binding.parent_boundary_partitions:
        table = readback_i3_migration_parquet_exact(
            data_root=root,
            artifact=pin.artifact,
            table_name="universe_daily",
            row_count=pin.row_count,
            session_date=pin.session_date,
        )
        lookback_rows.extend(dict(row) for row in table.to_pylist())
        minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    target_partition_receipt = _i2_partition(
        loaded.binding.i2_partitions,
        "universe_source_daily",
    )
    target_source = _read_i2_partition_exact(root, target_partition_receipt)
    if target_source.num_rows != target_partition_receipt.row_count:
        raise I3DeltaIOError("target I2 membership row count changed")
    target_source = target_source.sort_by([("ticker", "ascending")])
    source_rows = tuple(dict(row) for row in target_source.to_pylist())
    if any(row["session_date"] != PRODUCTION_FIRST_DELTA_SESSION for row in source_rows) or tuple(
        str(row["ticker"]) for row in source_rows
    ) != tuple(sorted({str(row["ticker"]) for row in source_rows})):
        raise I3DeltaIOError("target I2 membership is not session-pure/sorted/unique")

    observation_receipt = _i2_partition(
        loaded.binding.i2_partitions,
        "asset_observation_daily",
    )
    observation = _read_i2_partition_exact(root, observation_receipt)
    version_receipt = _i2_partition(
        loaded.binding.i2_partitions,
        "asset_observation_version",
    )
    versions = _read_i2_partition_exact(root, version_receipt)
    reference_metadata = _reference_metadata_by_selected_source(
        source_rows,
        observation,
        versions,
    )
    target_rows, fallback_counts = _resolve_target_rows(
        source_rows,
        run_spec=run_spec,
        source_binding=loaded.source_binding,
        gate_b_by_composite=loaded.gate_b_by_composite,
        registries=loaded.registries,
    )
    lookback_rows.extend(target_rows)
    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    try:
        materialized = _materialize_delta_day(
            target_rows=target_rows,
            lookback_rows=tuple(lookback_rows),
            target_session=PRODUCTION_FIRST_DELTA_SESSION,
            calendar=loaded.calendar_sessions,
            availability_session=run_spec.run_available_session,
            policy=run_spec.identity_policy_bundle,
            policy_snapshot=loaded.policy_snapshot,
            prior_open_aliases=parent.checkpoint.open_aliases,
            prior_assets=parent.checkpoint.asset_aggregates,
            prior_issuers=parent.checkpoint.issuer_aggregates,
            prior_unresolved=parent.checkpoint.unresolved_subjects,
            prior_terminal_rows=parent.checkpoint.terminal_row_versions,
            reference_metadata_by_source_id=reference_metadata,
            reference_metadata_available_session=loaded.i2_run.receipt.receipt_available_session,
        )
    except Exception as exc:
        # The imported transition is deliberately not exposed as a production
        # entrypoint.  All inputs and outputs are revalidated below under the
        # module-owned production DELTA semantics before acquiring authority.
        raise I3DeltaIOError(
            f"production delta state transition failed closed: {type(exc).__name__}: {exc}"
        ) from exc
    _validate_materialized_delta(
        materialized=materialized,
        target_rows=target_rows,
        parent=parent,
        fallback_counts=fallback_counts,
    )

    rows_by_table: dict[str, tuple[dict[str, object], ...]] = {
        "asset_master": tuple(dict(row) for row in materialized.asset_rows),
        "ticker_alias": tuple(dict(row) for row in materialized.alias_rows),
        "issuer_master": tuple(dict(row) for row in materialized.issuer_rows),
    }
    parent_outputs = {item.table_name: item for item in parent.receipt.output_set.table_outputs}
    small_outputs: dict[str, I3ProductionTableOutput] = {}
    new_segments: dict[str, I3ProductionSegmentPin] = {}
    for table_name in _SMALL_TABLES:
        parent_output = parent_outputs[table_name]
        parent_rowset = parent_output.rowset_index
        if (
            parent_output.storage is not I3ProductionOutputStorage.ROWSET_INDEX
            or parent_rowset is None
        ):
            raise I3DeltaIOError("delta parent small table is not a rowset index")
        table = pa.Table.from_pylist(
            list(rows_by_table[table_name]),
            schema=I3_V2_CONTRACTS[table_name].arrow_schema,
        )
        relative = _workspace_relative(
            root,
            work
            / table_name
            / "segments"
            / f"session_date={PRODUCTION_FIRST_DELTA_SESSION.isoformat()}"
            / "part-000.parquet",
        )
        artifact = write_i3_migration_parquet_no_clobber(
            data_root=root,
            relative_path=relative,
            table=table,
            run_spec=run_spec,
        )
        readback = readback_i3_migration_parquet_exact(
            data_root=root,
            artifact=artifact,
            table_name=table_name,
            row_count=table.num_rows,
        )
        _assert_rows_equal(table_name, readback.to_pylist(), rows_by_table[table_name])
        contract = I3_V2_CONTRACTS[table_name]
        segment = I3ProductionSegmentPin(
            table_name=table_name,
            segment_id=production_delta_append_segment_id(
                table_name=table_name,
                parent_rowset_id=parent_rowset.rowset_index_id,
                parent_segment_ids=tuple(item.segment_id for item in parent_rowset.segments),
                artifact=artifact,
                terminal_session=run_spec.terminal_session,
                availability_session=run_spec.run_available_session,
                native_v2_migration_id=run_spec.native_v2_migration_id,
            ),
            artifact=artifact,
            row_count=readback.num_rows,
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            availability_session=run_spec.run_available_session,
        )
        rowset = I3ProductionRowsetIndex(
            table_name=table_name,
            terminal_session=run_spec.terminal_session,
            segments=(*parent_rowset.segments, segment),
        )
        index_relative = _workspace_relative(root, work / table_name / "index.json")
        index_artifact = _write_bytes_no_clobber(
            root,
            index_relative,
            rowset.canonical_bytes(),
            run_spec=run_spec,
        )
        if index_artifact != rowset.exact_pin(path=index_relative):
            raise I3DeltaIOError("delta rowset-index bytes changed during write")
        small_outputs[table_name] = I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.ROWSET_INDEX,
            manifest_output=NativeV2OutputArtifact(
                table_name=table_name,
                session_date=run_spec.terminal_session,
                row_count=rowset.row_count,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                artifact=index_artifact,
            ),
            rowset_index=rowset,
        )
        new_segments[table_name] = segment
        minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    universe_table = pa.Table.from_pylist(
        [dict(row) for row in materialized.universe_rows],
        schema=I3_V2_CONTRACTS["universe_daily"].arrow_schema,
    )
    universe_relative = _workspace_relative(
        root,
        work
        / "universe_daily"
        / f"session_date={PRODUCTION_FIRST_DELTA_SESSION.isoformat()}"
        / "part-000.parquet",
    )
    universe_artifact = write_i3_migration_parquet_no_clobber(
        data_root=root,
        relative_path=universe_relative,
        table=universe_table,
        run_spec=run_spec,
    )
    universe_readback = readback_i3_migration_parquet_exact(
        data_root=root,
        artifact=universe_artifact,
        table_name="universe_daily",
        row_count=universe_table.num_rows,
        session_date=PRODUCTION_FIRST_DELTA_SESSION,
    )
    projection_difference_count = _canonical_projection_difference_count(
        target_rows,
        universe_readback.to_pylist(),
    )
    if projection_difference_count:
        raise I3DeltaIOError("delta universe differs from its canonical target projection")
    universe_contract = I3_V2_CONTRACTS["universe_daily"]
    partition = I3ProductionPartitionPin(
        session_date=run_spec.terminal_session,
        partition_receipt_id=stable_digest(
            {
                "artifact": universe_artifact.to_dict(),
                "input_binding_id": loaded.binding.input_binding_id,
                "i2_partition_receipt_id": target_partition_receipt.partition_receipt_id,
                "row_count": universe_readback.num_rows,
                "rule_version": I3_PRODUCTION_DELTA_PARTITION_RECEIPT_RULE_VERSION,
                "source_digest": source_digest,
            }
        ),
        artifact=universe_artifact,
        row_count=universe_readback.num_rows,
        contract_id=universe_contract.contract_id,
        schema_digest=universe_contract.schema_digest,
        availability_session=run_spec.run_available_session,
    )
    parent_universe_index = parent_outputs["universe_daily"].dataset_index
    if parent_universe_index is None:  # pragma: no cover - binding loader proves
        raise I3DeltaIOError("delta parent universe index disappeared")
    dataset_index = I3ProductionDatasetIndex(
        table_name="universe_daily",
        terminal_session=run_spec.terminal_session,
        partitions=(*parent_universe_index.partitions, partition),
    )
    dataset_relative = _workspace_relative(root, work / "universe_daily" / "index.json")
    dataset_artifact = _write_bytes_no_clobber(
        root,
        dataset_relative,
        dataset_index.canonical_bytes(),
        run_spec=run_spec,
    )
    if dataset_artifact != dataset_index.exact_pin(path=dataset_relative):
        raise I3DeltaIOError("delta dataset-index bytes changed during write")
    universe_output = I3ProductionTableOutput(
        storage=I3ProductionOutputStorage.DATASET_INDEX,
        manifest_output=NativeV2OutputArtifact(
            table_name="universe_daily",
            session_date=run_spec.terminal_session,
            row_count=dataset_index.row_count,
            contract_id=universe_contract.contract_id,
            schema_digest=universe_contract.schema_digest,
            artifact=dataset_artifact,
        ),
        dataset_index=dataset_index,
    )
    table_outputs = tuple(
        universe_output if table == "universe_daily" else small_outputs[table]
        for table in I3_V2_TABLE_ORDER
    )

    terminal_rows, prepared_row_versions = _physical_row_lineage(
        rows_by_table,
        new_segments,
        parent=parent,
        availability_session=run_spec.run_available_session,
    )
    s4_terminal_pins = tuple(
        S4TerminalPartitionPin(
            table_name=table_name,
            session_date=run_spec.terminal_session,
            partition_receipt_id=_i2_partition(
                loaded.binding.i2_partitions,
                table_name,
            ).partition_receipt_id,
            artifact=_i2_partition(loaded.binding.i2_partitions, table_name).artifact,
            availability_session=loaded.i2_run.receipt.receipt_available_session,
        )
        for table_name in S4_TERMINAL_TABLE_ORDER
    )
    resolved_partition_map = (
        *parent.checkpoint.resolved_partition_map,
        ResolvedPartitionState(
            session_date=partition.session_date,
            partition_receipt_id=partition.partition_receipt_id,
            artifact=partition.artifact,
            row_count=partition.row_count,
            availability_session=partition.availability_session,
        ),
    )
    resolved_state_digest = i3_resolved_state_digest(
        last_session=run_spec.terminal_session,
        source_cutoff_session=run_spec.source_cutoff_session,
        availability_cutoff_session=run_spec.run_available_session,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=run_spec.calendar.calendar_artifact_id,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=run_spec.transform_semantics_digest,
        identity_policy_bundle=run_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=run_spec.identity_policy_bundle_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partition_map,
        terminal_row_versions=terminal_rows,
    )
    native_manifest = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=run_spec.terminal_session,
        release_available_session=run_spec.run_available_session,
        native_v2_migration_id=run_spec.native_v2_migration_id,
        identity_policy_bundle_id=run_spec.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=run_spec.transform_semantics_digest,
        resolved_state_digest=resolved_state_digest,
        output_artifacts=tuple(item.manifest_output for item in table_outputs),
        parent_release_id=parent.manifest.release_id,
        source_checkpoint_id=parent.checkpoint.checkpoint_id,
        legacy_oracle_release_set_id=LEGACY_S7_V1_RELEASE_SET_ID,
    )
    manifest_relative = _workspace_relative(root, work / "native-v2-release.json")
    native_manifest_artifact = _write_bytes_no_clobber(
        root,
        manifest_relative,
        native_manifest.canonical_bytes(),
        run_spec=run_spec,
    )
    if native_manifest_artifact != native_manifest.exact_pin(path=manifest_relative):
        raise I3DeltaIOError("delta native manifest bytes changed during write")
    checkpoint = I3CheckpointState(
        parent_release=NativeV2ParentReleasePin.from_manifest(
            native_manifest,
            path=manifest_relative,
        ),
        last_session=run_spec.terminal_session,
        source_cutoff_session=run_spec.source_cutoff_session,
        availability_cutoff_session=run_spec.run_available_session,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=run_spec.calendar.calendar_artifact_id,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=run_spec.transform_semantics_digest,
        identity_policy_bundle=run_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=run_spec.identity_policy_bundle_artifact,
        open_aliases=materialized.open_aliases,
        asset_aggregates=materialized.asset_aggregates,
        issuer_aggregates=materialized.issuer_aggregates,
        unresolved_subjects=materialized.unresolved_subjects,
        resolved_partition_map=resolved_partition_map,
        terminal_row_versions=terminal_rows,
    )
    checkpoint_relative = _workspace_relative(root, work / "checkpoint.json")
    checkpoint_artifact = _write_bytes_no_clobber(
        root,
        checkpoint_relative,
        checkpoint.canonical_bytes(),
        run_spec=run_spec,
    )
    if checkpoint_artifact != checkpoint.exact_pin(path=checkpoint_relative):
        raise I3DeltaIOError("delta checkpoint bytes changed during write")

    output_bytes = _workspace_file_bytes(work)
    prepared_physical = _prepared_output_artifacts(
        table_outputs,
        native_manifest_artifact=native_manifest_artifact,
        checkpoint_artifact=checkpoint_artifact,
    )
    prepared_output_bytes = sum(item.bytes for item in prepared_physical) + 64 * 1024**2
    if (
        output_bytes > run_spec.resource_caps.output_bytes_hard_cap
        or prepared_output_bytes > run_spec.resource_caps.output_bytes_hard_cap
    ):
        raise I3DeltaIOError("delta output bytes exceed the hard cap")
    if sum(item.manifest_output.row_count for item in table_outputs) > (
        run_spec.resource_caps.output_rows_hard_cap
    ):
        raise I3DeltaIOError("delta logical output rows exceed the hard cap")
    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))
    observed_temporary_bytes = max(
        partition.artifact.bytes,
        *(item.artifact.bytes for item in new_segments.values()),
    )
    observation = I3ProductionResourceObservation(
        peak_rss_bytes=_peak_rss_bytes(),
        elapsed_seconds=max(0, math.ceil(time.monotonic() - started)),
        minimum_disk_free_bytes=minimum_disk,
        temporary_bytes=observed_temporary_bytes,
    )
    observation.validate_caps(run_spec.resource_caps)

    unsealed = I3ProductionPreparedMaterialization(
        table_outputs=table_outputs,
        native_manifest=native_manifest,
        native_manifest_artifact=native_manifest_artifact,
        checkpoint=checkpoint,
        checkpoint_artifact=checkpoint_artifact,
        source_digest=source_digest,
        resource_observation=observation,
        canonical_projection_difference_count=projection_difference_count,
        row_versions=prepared_row_versions,
    )
    attestation = _mint_delta_attestation(
        run_spec=run_spec,
        parent=parent,
        prepared=unsealed,
        binding=loaded.binding,
    )
    return DeltaPreparedMaterialization(
        table_outputs=unsealed.table_outputs,
        native_manifest=unsealed.native_manifest,
        native_manifest_artifact=unsealed.native_manifest_artifact,
        checkpoint=unsealed.checkpoint,
        checkpoint_artifact=unsealed.checkpoint_artifact,
        source_digest=unsealed.source_digest,
        resource_observation=unsealed.resource_observation,
        canonical_projection_difference_count=0,
        row_versions=unsealed.row_versions,
        delta_materialization_attestation=attestation,
    )


def _resolve_target_rows(
    source_rows: Sequence[Mapping[str, object]],
    *,
    run_spec: I3ProductionRunSpec,
    source_binding: streaming.S7StreamingSourceBinding,
    gate_b_by_composite: Mapping[str, Mapping[str, object]],
    registries: LoadedRegistryReleaseSet,
) -> tuple[tuple[dict[str, object], ...], Mapping[str, int]]:
    rows: list[dict[str, object]] = []
    missing_gate_b = 0
    expired_override = 0
    for source in source_rows:
        observed = source.get("composite_figi")
        gate_missing = isinstance(observed, str) and observed not in gate_b_by_composite
        override_expired = (
            not gate_missing
            and isinstance(observed, str)
            and _expired_provider_override_subject(
                source,
                registries=registries,
                cutoff_session=source_binding.cutoff_session,
                source_s4_release_set_id=source_binding.s4_release_set_id,
            )
        )
        if gate_missing or override_expired:
            gate_row = gate_b_by_composite.get(str(observed)) if not gate_missing else None
            projection = _pending_projection(
                source,
                run_spec=run_spec,
                gate_row=gate_row,
                reason=(
                    "gate_b_reference_unattempted"
                    if gate_missing
                    else "provider_composite_override_scope_expired"
                ),
            )
            missing_gate_b += int(gate_missing)
            expired_override += int(override_expired)
        else:
            try:
                projection = streaming._frozen_registry_projection(
                    source,
                    gate_b_by_composite=gate_b_by_composite,
                    registries=registries,
                    binding=source_binding,
                )
            except streaming.S7StreamingMaterializationError as exc:
                raise I3DeltaIOError(
                    "exact known-Gate-B row did not resolve deterministically"
                ) from exc
        row = _build_delta_resolved_row(
            source,
            projection,
            run_spec=run_spec,
            source_binding=source_binding,
            fallback_reason=(
                "gate_b_reference_unattempted"
                if gate_missing
                else "provider_composite_override_scope_expired"
                if override_expired
                else None
            ),
        )
        rows.append(row)
    ordered = tuple(sorted(rows, key=lambda item: str(item["ticker"])))
    if len({str(item["ticker"]) for item in ordered}) != len(ordered):
        raise I3DeltaIOError("resolved target membership repeats a ticker")
    return ordered, {
        "gate_b_reference_unattempted": missing_gate_b,
        "provider_composite_override_scope_expired": expired_override,
    }


def _expired_provider_override_subject(
    source: Mapping[str, object],
    *,
    registries: LoadedRegistryReleaseSet,
    cutoff_session: date,
    source_s4_release_set_id: str,
) -> bool:
    """Detect a known override subject whose exact source/date scope has ended."""

    release = registries.by_name("provider_composite_override")
    source_id = _digest(source.get("selected_source_record_id"), "target source record ID")
    target = source.get("session_date")
    if type(target) is not date:
        raise I3DeltaIOError("provider-override target session is invalid")
    effective_ids = release.effective_decision_ids(cutoff_session=cutoff_session)
    if any(
        source_id in release.source_scopes[decision_id].source_record_ids
        for decision_id in effective_ids
    ):
        return False
    if not _is_exact_expired_sor_target(source):
        return False
    candidates = tuple(
        decision_id
        for decision_id in effective_ids
        if (
            release.decision_rows[decision_id].get("provider_id") == "massive"
            and release.decision_rows[decision_id].get("provider_market") == "stocks"
            and release.decision_rows[decision_id].get("provider_locale") == "us"
            and release.decision_rows[decision_id].get("observed_ticker") == _SOR_OVERRIDE_TICKER
            and release.decision_rows[decision_id].get("observed_composite_figi")
            == _SOR_OVERRIDE_OBSERVED_COMPOSITE
            and type(release.decision_rows[decision_id].get("valid_through_session")) is date
            and release.decision_rows[decision_id]["valid_through_session"] < target
        )
    )
    if len(candidates) != 1:
        raise I3DeltaIOError("exact SOR expired-override subject is absent or ambiguous")
    decision_id = candidates[0]
    row = release.decision_rows[decision_id]
    scope = release.source_scopes[decision_id]
    expected_decision = {
        "canonical_composite_figi": _SOR_OVERRIDE_CANONICAL_COMPOSITE,
        "canonical_composite_market_code": "US",
        "observed_composite_market_code": "US",
        "source_s4_release_set_id": source_s4_release_set_id,
        "valid_from_session": _SOR_OVERRIDE_VALID_FROM,
        "valid_through_session": _SOR_OVERRIDE_VALID_THROUGH,
    }
    if (
        any(row.get(key) != value for key, value in expected_decision.items())
        or row.get("scoped_source_record_count") != _SOR_OVERRIDE_SOURCE_ROW_COUNT
    ):
        raise I3DeltaIOError("exact SOR override decision boundary changed")
    scope_rows = tuple(scope.rows)
    if (
        len(scope_rows) != _SOR_OVERRIDE_SOURCE_ROW_COUNT
        or min(item.session_date for item in scope_rows) != _SOR_OVERRIDE_VALID_FROM
        or max(item.session_date for item in scope_rows) != _SOR_OVERRIDE_VALID_THROUGH
        or any(
            item.provider_id != "massive"
            or item.provider_market != "stocks"
            or item.provider_locale != "us"
            or item.ticker != _SOR_OVERRIDE_TICKER
            or item.observed_composite_figi != _SOR_OVERRIDE_OBSERVED_COMPOSITE
            or item.source_s4_release_set_id != source_s4_release_set_id
            for item in scope_rows
        )
    ):
        raise I3DeltaIOError("exact SOR override source scope changed")
    terminal_rows = tuple(
        item for item in scope_rows if item.session_date == _SOR_OVERRIDE_VALID_THROUGH
    )
    if len(terminal_rows) != 1:
        raise I3DeltaIOError("exact SOR override lacks one terminal source row")
    terminal = terminal_rows[0]
    if terminal.observed_share_class_figi != source.get(
        "share_class_figi"
    ) or terminal.primary_exchange_mic != source.get("primary_exchange_mic"):
        raise I3DeltaIOError("SOR target does not continue the exact terminal observed row")
    return True


def _is_exact_expired_sor_target(source: Mapping[str, object]) -> bool:
    return (
        source.get("session_date") == PRODUCTION_FIRST_DELTA_SESSION
        and source.get("ticker") == _SOR_OVERRIDE_TICKER
        and source.get("market") == "stocks"
        and source.get("locale") == "us"
        and source.get("composite_figi") == _SOR_OVERRIDE_OBSERVED_COMPOSITE
        and source.get("share_class_figi") == _SOR_OVERRIDE_OBSERVED_SHARE_CLASS
        and source.get("active_on_date") is True
    )


def _pending_projection(
    source: Mapping[str, object],
    *,
    run_spec: I3ProductionRunSpec,
    gate_row: Mapping[str, object] | None,
    reason: str,
) -> streaming.ResolutionProjection:
    observed = _optional_text(source.get("composite_figi"))
    observed_market = (
        None if gate_row is None else _optional_text(gate_row.get("selected_market_code"))
    )
    source_available = source.get("source_available_session")
    if type(source_available) is not date:
        raise I3DeltaIOError("pending target source availability is invalid")
    evidence_sessions = [
        source_available,
        run_spec.identity_policy_bundle.bundle_available_session,
    ]
    if reason == "gate_b_reference_unattempted":
        if observed is None or gate_row is not None:
            raise I3DeltaIOError("Gate-B-unattempted fallback scope differs")
        method = "cross_market_composite_pending_unresolved"
        disposition = "pending_cross_market_review"
        cross_status = "not_classified"
    elif reason == "provider_composite_override_scope_expired":
        if gate_row is None or not _is_exact_expired_sor_target(source):
            raise I3DeltaIOError("expired provider override is not the exact SOR boundary")
        classification = gate_row.get("classification")
        gate_available = gate_row.get("source_available_session")
        if (
            observed is None
            or observed_market != "US"
            or classification not in streaming.GATE_B_US
            or type(gate_available) is not date
        ):
            raise I3DeltaIOError("expired provider override lacks exact US Gate-B evidence")
        evidence_sessions.append(gate_available)
        method = "provider_figi_bounce_pending_unresolved"
        disposition = "pending_unresolved"
        cross_status = "known_us"
    else:  # pragma: no cover - closed caller set
        raise I3DeltaIOError("unknown fail-closed delta identity reason")
    evidence_available = max(evidence_sessions)
    if evidence_available > run_spec.run_available_session:
        raise I3DeltaIOError("pending target evidence exceeds run availability")
    return streaming.ResolutionProjection(
        selected_source_record_id=_digest(
            source.get("selected_source_record_id"),
            "pending source record ID",
        ),
        observed_composite_market_code=observed_market,
        observed_asset_id=(canonical_asset_id(observed) if observed is not None else None),
        canonical_composite_figi=None,
        canonical_composite_market_code=None,
        canonical_share_class_figi=None,
        canonical_cik_normalized=None,
        asset_id=None,
        share_class_id=None,
        issuer_id=None,
        identity_resolution_status="unresolved",
        identity_resolution_method=method,
        identity_disposition=disposition,
        identity_case_id=None,
        identity_case_available_session=None,
        identity_adjudication_id=None,
        cross_market_scope_id=None,
        cross_market_adjudication_id=None,
        cross_market_adjudication_available_session=None,
        cross_market_classification_status=cross_status,
        identity_case_resolution_role=None,
        adjudication_available_session=None,
        backtest_identity_eligible=False,
        current_reference_factor_eligible=False,
        security_type_scope="source_type_code_as_returned_not_historical_dictionary_v1",
        identity_evidence_available_session=evidence_available,
        provider_composite_override_id=None,
        provider_composite_override_available_session=None,
        share_class_adjudication_id=None,
        share_class_adjudication_available_session=None,
        asset_transition_ids=(),
        composite_registry_match_count=0,
        composite_registry_collision=False,
    )


def _build_delta_resolved_row(
    source: Mapping[str, object],
    projection: streaming.ResolutionProjection,
    *,
    run_spec: I3ProductionRunSpec,
    source_binding: streaming.S7StreamingSourceBinding,
    fallback_reason: str | None,
) -> dict[str, object]:
    session = source.get("session_date")
    if session != PRODUCTION_FIRST_DELTA_SESSION:
        raise I3DeltaIOError("resolved target row belongs to another session")
    ticker = source.get("ticker")
    if not isinstance(ticker, str) or not ticker:
        raise I3DeltaIOError("resolved target ticker is invalid")
    source_id = _digest(source.get("selected_source_record_id"), "target source record ID")
    if projection.selected_source_record_id != source_id:
        raise I3DeltaIOError("target projection changed its source record ID")
    observed = _optional_text(source.get("composite_figi"))
    observed_share = _optional_text(source.get("share_class_figi"))
    observed_cik = streaming._normalize_cik(source.get("cik"))
    expected_observed_asset = canonical_asset_id(observed) if observed is not None else None
    if projection.observed_asset_id != expected_observed_asset:
        raise I3DeltaIOError("target projection changed observed asset lineage")
    if projection.canonical_composite_figi is not None:
        if projection.asset_id != canonical_asset_id(projection.canonical_composite_figi):
            raise I3DeltaIOError("target canonical asset does not reproduce")
    elif projection.asset_id is not None:
        raise I3DeltaIOError("target projection has an asset without canonical Composite")
    if fallback_reason is not None and any(
        value is not None
        for value in (
            projection.asset_id,
            projection.share_class_id,
            projection.issuer_id,
            projection.canonical_composite_figi,
            projection.canonical_share_class_figi,
            projection.canonical_cik_normalized,
        )
    ):
        raise I3DeltaIOError("fail-closed target retained canonical/master identity")
    if projection.backtest_identity_eligible and projection.asset_id is None:
        raise I3DeltaIOError("eligible target lacks canonical asset identity")
    if projection.identity_evidence_available_session > run_spec.run_available_session:
        raise I3DeltaIOError("target identity evidence exceeds run availability")

    source_fields = streaming._source_binding_columns(source_binding)
    alias_presence_token = (
        stable_digest(
            {
                "canonical_composite_figi": projection.canonical_composite_figi,
                "selected_source_record_id": source_id,
                "ticker": ticker,
                "rule_version": I3_PRODUCTION_DELTA_STATE_TRANSITION_RULE_VERSION,
            }
        )
        if projection.backtest_identity_eligible
        else None
    )
    row: dict[str, object] = {
        "session_year": session.year,
        "session_date": session,
        "ticker": ticker,
        "active_on_date": bool(source.get("active_on_date")),
        "asset_id": projection.asset_id,
        "share_class_id": projection.share_class_id,
        "canonical_share_class_figi": projection.canonical_share_class_figi,
        "issuer_id": projection.issuer_id,
        "canonical_cik_normalized": projection.canonical_cik_normalized,
        "ticker_alias_id": alias_presence_token,
        "type_code": source.get("type_code"),
        "primary_exchange_mic": source.get("primary_exchange_mic"),
        "observed_cik_normalized": observed_cik,
        "observed_composite_figi": observed,
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
            source_binding.gate_c.identity_case_preview_id
        ),
        "source_identity_case_candidate_manifest_sha256": (
            source_binding.gate_c.identity_case_preview_manifest.sha256
        ),
        "identity_adjudication_id": projection.identity_adjudication_id,
        "cross_market_scope_id": projection.cross_market_scope_id,
        "cross_market_adjudication_id": projection.cross_market_adjudication_id,
        "cross_market_adjudication_available_session": (
            projection.cross_market_adjudication_available_session
        ),
        "cross_market_classification_status": (projection.cross_market_classification_status),
        "identity_case_resolution_role": projection.identity_case_resolution_role,
        "adjudication_available_session": projection.adjudication_available_session,
        "identity_resolution_cutoff_session": (
            run_spec.identity_policy_bundle.decision_cutoff_session
        ),
        "backtest_identity_eligible": projection.backtest_identity_eligible,
        "position_continuity_status": (
            "resolved_identity"
            if projection.backtest_identity_eligible
            else "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
        ),
        "identity_quality_liquidation_signal": False,
        "current_reference_factor_eligible": projection.current_reference_factor_eligible,
        "security_type_scope": projection.security_type_scope,
        "selected_source_record_id": source_id,
        "source_version_count": source.get("source_version_count"),
        "source_selection_status": source.get("selection_status"),
        "membership_time_scope": source.get("reference_time_scope"),
        "membership_source_available_session": source.get("source_available_session"),
        "membership_source_availability_quality": source.get("source_availability_quality"),
        "metadata_time_scope": source.get("metadata_time_scope"),
        "identity_mapping_time_scope": "cutoff_bound_registry_and_current_reference_v1",
        "identity_evidence_available_session": projection.identity_evidence_available_session,
        "resolution_rule_version": I3_PRODUCTION_DELTA_RESOLUTION_RULE_VERSION,
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
    expected = {item.name for item in streaming.S7_DERIVED_CONTRACTS["universe_daily"].columns}
    if set(row) != expected:
        raise I3DeltaIOError(
            f"delta resolved row fields differ: missing={sorted(expected - set(row))}, "
            f"extra={sorted(set(row) - expected)}"
        )
    try:
        table = pa.Table.from_pylist(
            [row],
            schema=streaming.S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema,
        )
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise I3DeltaIOError("delta resolved row does not satisfy the exact v1 shape") from exc
    if any(
        not field.nullable and column.null_count
        for field, column in zip(table.schema, table.columns, strict=True)
    ):
        raise I3DeltaIOError("delta resolved row contains a forbidden null")
    return row


def _materialize_delta_day(
    *,
    target_rows: Sequence[Mapping[str, object]],
    lookback_rows: Sequence[Mapping[str, object]],
    target_session: date,
    calendar: tuple[date, ...],
    availability_session: date,
    policy: object,
    policy_snapshot: object,
    prior_open_aliases: Sequence[object],
    prior_assets: Sequence[object],
    prior_issuers: Sequence[object],
    prior_unresolved: Sequence[object],
    prior_terminal_rows: Sequence[TerminalRowVersionState],
    reference_metadata_by_source_id: Mapping[str, Mapping[str, object]],
    reference_metadata_available_session: date | None,
) -> object:
    """Production wrapper around the pure checkpoint state primitives.

    The fixture runner's whole-day wrapper rejects the production-legal
    ``resolved_conflicted`` shape that retains canonical Composite/asset while
    disabling alias/FKs.  This narrow wrapper keeps that shape and reuses only
    the deterministic low-level state constructors.
    """

    if availability_session < max(target_session, policy.policy_available_session):
        raise I3DeltaIOError("delta availability precedes source or policy")
    rows = tuple(_validate_delta_target_row(row, target_session, policy) for row in target_rows)
    tickers = [str(row["ticker"]) for row in rows]
    if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
        raise I3DeltaIOError("delta target rows must be sorted/unique by ticker")
    calendar_index = {session: index for index, session in enumerate(calendar)}
    prior_open_by_ticker: dict[str, object] = {}
    for item in prior_open_aliases:
        if item.segment.ticker in prior_open_by_ticker:
            raise I3DeltaIOError("parent checkpoint repeats an open ticker alias")
        prior_open_by_ticker[item.segment.ticker] = item
    terminal = {item.map_key: item for item in prior_terminal_rows}
    assets = {item.asset_id: item for item in prior_assets}
    issuers = {item.issuer_id: item for item in prior_issuers}
    prior_asset_ids = frozenset(assets)
    prior_issuer_ids = frozenset(issuers)
    unresolved = {(item.subject_kind, item.subject_key): item for item in prior_unresolved}
    if len(unresolved) != len(prior_unresolved):
        raise I3DeltaIOError("parent checkpoint repeats an unresolved subject")

    open_aliases: list[object] = []
    alias_rows: list[dict[str, object]] = []
    row_aliases: dict[str, object] = {}
    unresolved_target_rows = 0
    for row in rows:
        ticker = str(row["ticker"])
        eligible = bool(row["backtest_identity_eligible"])
        collision = bool(row["composite_registry_collision"])
        if not eligible or collision:
            unresolved_target_rows += 1
            reason = _delta_unresolved_reason(row)
            unresolved[("ticker_identity", ticker)] = runner._advance_unresolved(
                unresolved.get(("ticker_identity", ticker)),
                ticker=ticker,
                target_session=target_session,
                reason=reason,
                source_record_id=str(row["selected_source_record_id"]),
                availability_session=availability_session,
            )
            continue
        segment, resolution, operation = runner._alias_for_target(
            row,
            prior=prior_open_by_ticker.get(ticker),
            target_session=target_session,
            policy=policy,
        )
        state = runner.OpenAliasState(segment=segment, resolution=resolution)
        open_aliases.append(state)
        row_aliases[ticker] = state
        alias_row = runner._alias_physical_row(
            row,
            segment=segment,
            resolution=resolution,
            availability_session=availability_session,
            calendar_index=calendar_index,
        )
        alias_rows.append(alias_row)
        terminal[("ticker_alias", segment.alias_segment_id)] = runner._terminal_state(
            "ticker_alias",
            segment.alias_segment_id,
            resolution.alias_resolution_version_id,
            resolution.predecessor_alias_resolution_version_id,
            alias_row,
            availability_session,
            operation=operation,
        )
        unresolved.pop(("ticker_identity", ticker), None)

    touched_assets: set[str] = set()
    touched_issuers: set[str] = set()
    first_asset_rows: dict[str, Mapping[str, object]] = {}
    first_issuer_rows: dict[str, Mapping[str, object]] = {}
    for row in rows:
        asset_id = row.get("asset_id")
        if isinstance(asset_id, str):
            assets[asset_id] = runner._advance_asset(
                assets.get(asset_id),
                row,
                availability_session=availability_session,
            )
            touched_assets.add(asset_id)
            first_asset_rows.setdefault(asset_id, row)
        issuer_id = row.get("issuer_id")
        cik = row.get("canonical_cik_normalized")
        if isinstance(issuer_id, str) and isinstance(cik, str):
            metadata = reference_metadata_by_source_id.get(
                str(row["selected_source_record_id"]),
                {},
            )
            advanced = runner._advance_issuer(
                issuers.get(issuer_id),
                row,
                availability_session=availability_session,
                policy=policy,
                reference_name=metadata.get("reference_name"),
                sic_code=metadata.get("sic_code"),
                reference_metadata_available_session=(
                    reference_metadata_available_session if metadata else None
                ),
            )
            if advanced is not None:
                issuers[issuer_id] = advanced
                touched_issuers.add(issuer_id)
                first_issuer_rows.setdefault(issuer_id, row)

    runner._apply_symmetric_asset_transition_lineage(
        rows=rows,
        assets=assets,
        touched_assets=touched_assets,
        first_asset_rows=first_asset_rows,
        policy_snapshot=policy_snapshot,
        availability_session=availability_session,
    )

    asset_rows: list[dict[str, object]] = []
    for asset_id in sorted(touched_assets):
        state = assets[asset_id]
        predecessor = state.terminal_row_version_id if asset_id in prior_asset_ids else None
        row, state = runner._asset_master_row(
            state,
            predecessor=predecessor,
            source_row=first_asset_rows[asset_id],
            availability_session=availability_session,
        )
        assets[asset_id] = state
        asset_rows.append(row)
        terminal[("asset_master", asset_id)] = runner._terminal_state(
            "asset_master",
            asset_id,
            state.terminal_row_version_id,
            predecessor,
            row,
            availability_session,
            operation="new_root" if predecessor is None else "mechanical_successor",
        )

    issuer_rows: list[dict[str, object]] = []
    for issuer_id in sorted(touched_issuers):
        state = issuers[issuer_id]
        predecessor = state.terminal_row_version_id if issuer_id in prior_issuer_ids else None
        row, state = runner._issuer_master_row(
            state,
            predecessor=predecessor,
            source_row=first_issuer_rows[issuer_id],
            availability_session=availability_session,
        )
        issuers[issuer_id] = state
        issuer_rows.append(row)
        terminal[("issuer_master", issuer_id)] = runner._terminal_state(
            "issuer_master",
            issuer_id,
            state.terminal_row_version_id,
            predecessor,
            row,
            availability_session,
            operation="new_root" if predecessor is None else "mechanical_successor",
        )

    universe_rows: list[dict[str, object]] = []
    for source in rows:
        row = dict(source)
        legacy_alias = row.pop("ticker_alias_id")
        ticker = str(row["ticker"])
        alias = row_aliases.get(ticker)
        if alias is None:
            if legacy_alias is not None and not bool(row["backtest_identity_eligible"]):
                raise I3DeltaIOError("ineligible delta membership unexpectedly has alias")
            alias_segment = alias_version = None
        else:
            alias_segment = alias.segment.alias_segment_id
            alias_version = alias.resolution.alias_resolution_version_id
        asset_id = row.get("asset_id")
        issuer_id = row.get("issuer_id")
        eligible = bool(row["backtest_identity_eligible"])
        issuer_state = (
            issuers.get(str(issuer_id)) if eligible and isinstance(issuer_id, str) else None
        )
        row.update(
            {
                "alias_segment_id": alias_segment,
                "alias_resolution_version_id": alias_version,
                "asset_master_version_id": (
                    assets[str(asset_id)].terminal_row_version_id
                    if eligible and isinstance(asset_id, str)
                    else None
                ),
                "issuer_master_version_id": (
                    issuer_state.terminal_row_version_id if issuer_state is not None else None
                ),
                "identity_policy_bundle_id": policy.identity_policy_bundle_id,
                "row_available_session": availability_session,
            }
        )
        if row["identity_quality_liquidation_signal"] is not False:
            raise I3DeltaIOError("delta identity quality attempted forced liquidation")
        universe_rows.append(row)

    for table_name, values in (
        ("universe_daily", universe_rows),
        ("ticker_alias", alias_rows),
        ("asset_master", asset_rows),
        ("issuer_master", issuer_rows),
    ):
        runner._validate_v2_rows(table_name, values)
    qa = runner._build_qa(
        rows=rows,
        lookback_rows=lookback_rows,
        universe_rows=universe_rows,
        alias_rows=alias_rows,
        asset_rows=asset_rows,
        issuer_rows=issuer_rows,
        unresolved_target_rows=unresolved_target_rows,
    )
    return runner._DayMaterialization(
        universe_rows=tuple(universe_rows),
        alias_rows=tuple(alias_rows),
        asset_rows=tuple(asset_rows),
        issuer_rows=tuple(issuer_rows),
        open_aliases=tuple(sorted(open_aliases, key=lambda item: item.segment.alias_segment_id)),
        asset_aggregates=tuple(sorted(assets.values(), key=lambda item: item.asset_id)),
        issuer_aggregates=tuple(sorted(issuers.values(), key=lambda item: item.issuer_id)),
        unresolved_subjects=tuple(
            sorted(unresolved.values(), key=lambda item: item.unresolved_subject_id)
        ),
        terminal_rows=tuple(sorted(terminal.values(), key=lambda item: item.map_key)),
        qa=qa,
    )


def _validate_delta_target_row(
    row: Mapping[str, object], target_session: date, policy: object
) -> dict[str, object]:
    expected = {item.name for item in streaming.S7_DERIVED_CONTRACTS["universe_daily"].columns}
    if set(row) != expected:
        raise I3DeltaIOError("delta target row fields differ from immutable S7 v1 shape")
    result = dict(row)
    if result["session_date"] != target_session or result["session_year"] != target_session.year:
        raise I3DeltaIOError("delta target session fields differ")
    _digest(result["selected_source_record_id"], "delta target source record ID")
    if result["identity_resolution_cutoff_session"] != policy.decision_cutoff_session:
        raise I3DeltaIOError("delta identity cutoff differs from the policy bundle")
    eligible = bool(result["backtest_identity_eligible"])
    if (result.get("ticker_alias_id") is None) == eligible:
        raise I3DeltaIOError("delta alias-presence shim differs from eligibility")
    if result["identity_quality_liquidation_signal"] is not False:
        raise I3DeltaIOError("delta identity quality attempted forced liquidation")
    if eligible:
        canonical = result.get("canonical_composite_figi")
        if not isinstance(canonical, str) or result.get("asset_id") != canonical_asset_id(
            canonical
        ):
            raise I3DeltaIOError("eligible delta target canonical asset does not reproduce")
    elif result.get("identity_resolution_status") != "resolved_conflicted" and any(
        result.get(name) is not None
        for name in (
            "asset_id",
            "canonical_composite_figi",
            "canonical_composite_market_code",
            "canonical_share_class_figi",
            "share_class_id",
        )
    ):
        raise I3DeltaIOError("unresolved delta target retained canonical research identity")
    return result


def _delta_unresolved_reason(row: Mapping[str, object]) -> str:
    if (
        row.get("identity_disposition") == "pending_cross_market_review"
        and row.get("observed_composite_market_code") is None
    ):
        return "gate_b_reference_unattempted"
    if (
        row.get("identity_disposition") == "pending_unresolved"
        and row.get("identity_resolution_method") == "provider_figi_bounce_pending_unresolved"
    ):
        return "provider_composite_override_scope_expired"
    if bool(row.get("composite_registry_collision")):
        return "registry_collision"
    return runner._reason_code(str(row["identity_disposition"]))


def _validate_materialized_delta(
    *,
    materialized: object,
    target_rows: Sequence[Mapping[str, object]],
    parent: LoadedI3ProductionStaging,
    fallback_counts: Mapping[str, int],
) -> None:
    _validate_delta_qa(materialized.qa)
    if any(type(value) is not int or value < 0 for value in fallback_counts.values()):
        raise I3DeltaIOError("delta unresolved-reason counts are malformed")
    if len(materialized.universe_rows) != len(target_rows):
        raise I3DeltaIOError("delta materialization omitted or duplicated membership")
    output_by_ticker = {str(row["ticker"]): row for row in materialized.universe_rows}
    if len(output_by_ticker) != len(materialized.universe_rows):
        raise I3DeltaIOError("delta materialization repeats an output ticker")
    for source in target_rows:
        row = output_by_ticker.get(str(source["ticker"]))
        if row is None:
            raise I3DeltaIOError("delta materialization omitted a target ticker")
        for field_name in (
            "active_on_date",
            "observed_cik_normalized",
            "observed_composite_figi",
            "observed_share_class_figi",
            "selected_source_record_id",
        ):
            if row[field_name] != source[field_name]:
                raise I3DeltaIOError("delta materialization changed observed membership evidence")
        pending_reason = (
            _delta_unresolved_reason(source) if not source["backtest_identity_eligible"] else None
        )
        if pending_reason in {
            "gate_b_reference_unattempted",
            "provider_composite_override_scope_expired",
        }:
            violations: list[str] = []
            if row["backtest_identity_eligible"] is not False:
                violations.append("new_trade_eligible")
            if row["identity_quality_liquidation_signal"] is not False:
                violations.append("forced_liquidation")
            if (
                row["position_continuity_status"]
                != "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
            ):
                violations.append("position_continuity")
            # These are the only fields that can make an unresolved membership
            # participate in the tradable identity graph.  Issuer/CIK and other
            # descriptive lineage may remain present for research continuity;
            # backtest_identity_eligible is the new-entry gate.
            for name in (
                "alias_segment_id",
                "alias_resolution_version_id",
                "asset_master_version_id",
                "issuer_master_version_id",
            ):
                if row[name] is not None:
                    violations.append(name)
            if violations:
                raise I3DeltaIOError(
                    "unresolved membership violates factor-safety gates: "
                    f"ticker={source['ticker']}, fields={','.join(violations)}"
                )
    prior_terminal = {item.map_key: item for item in parent.checkpoint.terminal_row_versions}
    output_terminal = {item.map_key: item for item in materialized.terminal_rows}
    if not set(prior_terminal).issubset(output_terminal):
        raise I3DeltaIOError("delta state transition removed parent terminal row history")
    prior_aliases = {item.segment.ticker: item for item in parent.checkpoint.open_aliases}
    target_by_ticker = {
        str(row["ticker"]): row for row in target_rows if row["backtest_identity_eligible"]
    }
    if {item.segment.ticker for item in materialized.open_aliases} != set(target_by_ticker):
        raise I3DeltaIOError("delta open-alias frontier differs from eligible membership")
    policy = parent.checkpoint.identity_policy_bundle
    for state in materialized.open_aliases:
        target = target_by_ticker[state.segment.ticker]
        target_evidence = runner._native_date(
            target["identity_evidence_available_session"],
            "delta target identity evidence availability",
        )
        prior = prior_aliases.get(state.segment.ticker)
        same_segment_prior = (
            prior
            if prior is not None
            and prior.segment.alias_segment_id == state.segment.alias_segment_id
            else None
        )
        expected_evidence = (
            target_evidence
            if same_segment_prior is None
            else max(
                target_evidence,
                same_segment_prior.resolution.evidence_available_session,
            )
        )
        expected_resolution_available = max(
            policy.bundle_available_session,
            expected_evidence,
            *(
                ()
                if same_segment_prior is None
                else (same_segment_prior.resolution.resolution_available_session,)
            ),
        )
        expected_identity_cutoff = max(
            policy.policy_cutoff_session,
            expected_resolution_available,
            *(
                ()
                if same_segment_prior is None
                else (same_segment_prior.resolution.identity_cutoff_session,)
            ),
        )
        resolution = state.resolution
        if (
            resolution.evidence_available_session != expected_evidence
            or resolution.resolution_available_session != expected_resolution_available
            or resolution.evidence_cutoff_session != expected_identity_cutoff
            or resolution.identity_cutoff_session != expected_identity_cutoff
        ):
            raise I3DeltaIOError("delta alias availability progression does not reproduce")
    if any(
        row["identity_quality_liquidation_signal"] is not False
        for row in materialized.universe_rows
    ):
        raise I3DeltaIOError("delta materialization emitted a forced-liquidation signal")


def _validate_delta_qa(qa: Mapping[str, object]) -> None:
    if qa.get("qa_catalog_digest") != I3_QA_CATALOG_DIGEST:
        raise I3DeltaIOError("DELTA QA does not bind the closed I3 catalog")
    critical = 0
    for rule in I3_QA_CATALOG:
        value = qa.get(rule.check_id)
        if type(value) is not int or value < 0:
            raise I3DeltaIOError(f"DELTA QA metric is invalid: {rule.check_id}")
        if rule.severity.value == "critical":
            critical += value
    if qa.get("critical_failure_count") != critical:
        raise I3DeltaIOError("DELTA QA critical count does not reproduce")
    if critical:
        raise I3DeltaIOError(f"DELTA QA failed closed with {critical} critical rows")


def _physical_row_lineage(
    rows_by_table: Mapping[str, tuple[dict[str, object], ...]],
    new_segments: Mapping[str, I3ProductionSegmentPin],
    *,
    parent: LoadedI3ProductionStaging,
    availability_session: date,
) -> tuple[tuple[TerminalRowVersionState, ...], tuple[I3ProductionPreparedRowVersion, ...]]:
    parent_terminal = {item.map_key: item for item in parent.checkpoint.terminal_row_versions}
    terminal = dict(parent_terminal)
    prepared: list[I3ProductionPreparedRowVersion] = []
    for table_name in _SMALL_TABLES:
        segment = new_segments[table_name]
        key_field, version_field, predecessor_field, availability_field = _VERSION_SHAPE[table_name]
        seen_keys: set[str] = set()
        for row_index, row in enumerate(rows_by_table[table_name]):
            stable_key = _digest(row[key_field], f"{table_name} stable row key")
            row_version_id = _digest(row[version_field], f"{table_name} row-version ID")
            predecessor = row[predecessor_field]
            if predecessor is not None:
                predecessor = _digest(predecessor, f"{table_name} predecessor ID")
            if row[availability_field] != availability_session:
                raise I3DeltaIOError("delta row availability differs from its segment")
            if stable_key in seen_keys:
                raise I3DeltaIOError("delta segment repeats a stable row key")
            seen_keys.add(stable_key)
            prior = parent_terminal.get((table_name, stable_key))
            if prior is None:
                if predecessor is not None:
                    raise I3DeltaIOError("delta new root names a predecessor")
                operation = RowVersionOperation.NEW_ROOT
                predecessor_payload = None
            else:
                if predecessor != prior.row_version_id:
                    raise I3DeltaIOError("delta successor differs from parent terminal version")
                operation = RowVersionOperation.MECHANICAL_SUCCESSOR
                predecessor_payload = prior.row_payload_digest
            payload_digest = stable_digest(_jsonable(row))
            state = TerminalRowVersionState(
                table_name=table_name,
                stable_row_key=stable_key,
                row_version_id=row_version_id,
                predecessor_row_version_id=predecessor,
                row_payload_digest=payload_digest,
                index_artifact=segment.artifact,
                availability_session=availability_session,
            )
            terminal[state.map_key] = state
            prepared.append(
                I3ProductionPreparedRowVersion(
                    table_name=table_name,
                    stable_row_key=stable_key,
                    row_version_id=row_version_id,
                    predecessor_row_version_id=predecessor,
                    operation=operation,
                    availability_session=availability_session,
                    index_artifact=segment.artifact,
                    row_locator=f"row_index={row_index}",
                    row_payload_digest=payload_digest,
                    predecessor_payload_digest=predecessor_payload,
                    validator_semantics_digest=production_delta_row_validator_digest(
                        table_name=table_name,
                        schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                        operation=operation.value,
                    ),
                )
            )
    if not prepared:
        raise I3DeltaIOError("delta produced no versioned-table row changes")
    return (
        tuple(sorted(terminal.values(), key=lambda item: item.map_key)),
        tuple(sorted(prepared, key=lambda item: (item.table_name, item.stable_row_key))),
    )


def _canonical_projection_difference_count(
    target_rows: Sequence[Mapping[str, object]],
    universe_rows: Sequence[Mapping[str, object]],
) -> int:
    if len(target_rows) != len(universe_rows):
        return abs(len(target_rows) - len(universe_rows)) or 1
    target_by_ticker = {str(row["ticker"]): row for row in target_rows}
    output_by_ticker = {str(row["ticker"]): row for row in universe_rows}
    if len(target_by_ticker) != len(target_rows) or len(output_by_ticker) != len(universe_rows):
        return 1
    difference = 0
    for ticker, target in target_by_ticker.items():
        output = output_by_ticker.get(ticker)
        if output is None:
            difference += 1
            continue
        target_projection = {
            key: value for key, value in target.items() if key != "ticker_alias_id"
        }
        output_projection = {
            key: value for key, value in output.items() if key not in _V2_UNIVERSE_ENVELOPE
        }
        difference += int(_jsonable(target_projection) != _jsonable(output_projection))
    difference += len(set(output_by_ticker).difference(target_by_ticker))
    return difference


def _assert_rows_equal(
    table_name: str,
    observed: Sequence[Mapping[str, object]],
    expected: Sequence[Mapping[str, object]],
) -> None:
    if [_jsonable(row) for row in observed] != [_jsonable(row) for row in expected]:
        raise I3DeltaIOError(f"{table_name} readback rows changed")


def estimate_production_delta_resources(
    run_spec: I3ProductionRunSpec,
    binding: I3ProductionDeltaInputBinding,
) -> I3ProductionDeltaResourceEstimate:
    if run_spec.run_kind is not I3ProductionRunKind.DELTA:
        raise I3DeltaIOError("delta resource estimate requires a DELTA RunSpec")
    return _estimate_production_delta_resources_from_pins(
        run_spec,
        parent_boundary_partitions=binding.parent_boundary_partitions,
        i2_partitions=binding.i2_partitions,
        declared_input_artifacts=binding.declared_input_artifacts,
        parent_output_bytes=binding.parent_output_bytes,
        parent_output_rows=binding.parent_output_rows,
        asset_transition_decision_count=binding.asset_transition_decision_count,
        transitive_control_replay_bytes=binding.transitive_control_replay_bytes,
    )


def _estimate_production_delta_resources_from_pins(
    run_spec: I3ProductionRunSpec,
    *,
    parent_boundary_partitions: Sequence[I3ProductionPartitionPin],
    i2_partitions: Sequence[S4SessionPartitionReceipt],
    declared_input_artifacts: Sequence[ArtifactPin],
    parent_output_bytes: int,
    parent_output_rows: int,
    asset_transition_decision_count: int,
    transitive_control_replay_bytes: int,
) -> I3ProductionDeltaResourceEstimate:
    artifacts = _unique_artifact_pins(tuple(declared_input_artifacts))
    declared = {item.path: item for item in artifacts}
    required = (
        *(item.artifact for item in parent_boundary_partitions),
        *(item.artifact for item in i2_partitions),
    )
    if any(declared.get(item.path) != item for item in required):
        raise I3DeltaIOError("delta resource estimate omits a required exact input")
    source_bytes = (
        sum(item.bytes for item in artifacts)
        + I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP
    )
    target_partition = _i2_partition(i2_partitions, "universe_source_daily")
    target_bytes = target_partition.artifact.bytes
    largest_source = max((item.bytes for item in artifacts), default=0)
    for value, label in (
        (parent_output_bytes, "delta parent output bytes"),
        (parent_output_rows, "delta parent output rows"),
        (asset_transition_decision_count, "delta transition decision count"),
    ):
        if type(value) is not int or value < 0:
            raise I3DeltaIOError(f"{label} is invalid")
    transitive_replay_bytes = _validate_transitive_control_replay_bytes(
        transitive_control_replay_bytes
    )
    # Universe/alias/issuer/target-asset rows are bounded by membership.  Every
    # effective transition can additionally touch both endpoints, including a
    # prior-only counterpart absent from target membership.
    new_row_bound = (
        target_partition.row_count * len(I3_V2_TABLE_ORDER) + 2 * asset_transition_decision_count
    )
    estimated_output_rows = parent_output_rows + new_row_bound
    # OutputSet accounting includes the immutable parent prefix.  Reserve a
    # fixed control/index envelope in addition to the target rewrite bound.
    new_output_bytes = max(64 * 1024**2, target_bytes * 8) + 64 * 1024**2
    estimated_output = parent_output_bytes + new_output_bytes
    estimated_temporary = max(
        64 * 1024**2,
        target_bytes * 2,
        I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP,
    )
    estimated_rss = (
        max(1024**3, target_bytes * 4, largest_source * 3)
        + I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES
    )
    disk_floor = max(
        run_spec.resource_caps.disk_free_bytes_hard_floor,
        _MINIMUM_DELTA_DISK_RESERVE,
    )
    return I3ProductionDeltaResourceEstimate(
        source_bytes=source_bytes,
        estimated_peak_rss_bytes=estimated_rss,
        estimated_output_bytes=estimated_output,
        estimated_output_rows=estimated_output_rows,
        estimated_temporary_bytes=estimated_temporary,
        minimum_free_disk_bytes_required=disk_floor + new_output_bytes + estimated_temporary,
        transitive_control_replay_bytes=transitive_replay_bytes,
    )


def verify_delta_materialization_attestation(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    prepared: I3ProductionPreparedMaterialization,
) -> DeltaMaterializationAttestation:
    """Require and replay the official module-minted DELTA capability."""

    _require_module_sealed_delta_attestation(prepared)
    loaded = load_production_delta_input_binding(
        data_root=data_root,
        run_spec=run_spec,
        parent=parent,
    )
    try:
        _verify_prepared_delta_physical_semantics(
            data_root=data_root,
            run_spec=run_spec,
            parent=parent,
            prepared=prepared,
            loaded=loaded,
        )
    except I3MigrationIOError as exc:
        raise I3DeltaIOError("delta exact artifact differs during replay") from exc
    return _verify_delta_attestation_with_binding(
        run_spec=run_spec,
        parent=parent,
        prepared=prepared,
        binding=loaded.binding,
    )


def _delta_source_digest(
    run_spec: I3ProductionRunSpec,
    binding: I3ProductionDeltaInputBinding,
) -> str:
    return stable_digest(
        {
            "alias_availability_progression_rule_version": (
                I3_PRODUCTION_DELTA_ALIAS_AVAILABILITY_PROGRESSION_RULE_VERSION
            ),
            "append_segment_rule_version": I3_PRODUCTION_DELTA_APPEND_SEGMENT_RULE_VERSION,
            "input_binding_id": binding.input_binding_id,
            "native_v2_migration_id": run_spec.native_v2_migration_id,
            "rule_version": I3_PRODUCTION_DELTA_SOURCE_DIGEST_RULE_VERSION,
            "run_spec_id": run_spec.run_spec_id,
            "source_version_projection_rule_version": (
                I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION
            ),
            "transform_semantics_digest": run_spec.transform_semantics_digest,
        }
    )


def _delta_table_output_set_digest(prepared: I3ProductionPreparedMaterialization) -> str:
    if tuple(item.table_name for item in prepared.table_outputs) != I3_V2_TABLE_ORDER:
        raise I3DeltaIOError("delta attestation output order differs")
    return stable_digest([item.to_dict() for item in prepared.table_outputs])


def _delta_row_change_digest(prepared: I3ProductionPreparedMaterialization) -> str:
    return stable_digest(
        {
            "rows": [
                {
                    "availability_session": item.availability_session.isoformat(),
                    "index_artifact": item.index_artifact.to_dict(),
                    "operation": item.operation.value,
                    "predecessor_payload_digest": item.predecessor_payload_digest,
                    "predecessor_row_version_id": item.predecessor_row_version_id,
                    "row_locator": item.row_locator,
                    "row_payload_digest": item.row_payload_digest,
                    "row_version_id": item.row_version_id,
                    "stable_row_key": item.stable_row_key,
                    "table_name": item.table_name,
                    "validator_semantics_digest": item.validator_semantics_digest,
                }
                for item in prepared.row_versions
            ],
            "rule_version": I3_PRODUCTION_DELTA_ROW_CHANGE_ATTESTATION_RULE_VERSION,
        }
    )


def _delta_projection_digest(
    prepared: I3ProductionPreparedMaterialization,
    binding: I3ProductionDeltaInputBinding,
) -> str:
    universe = prepared.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    if universe.dataset_index is None:
        raise I3DeltaIOError("delta attestation lacks universe dataset index")
    target_partition = universe.dataset_index.partitions[-1]
    return stable_digest(
        {
            "canonical_projection_difference_count": (
                prepared.canonical_projection_difference_count
            ),
            "identity_fallback_rule_version": (I3_PRODUCTION_DELTA_IDENTITY_FALLBACK_RULE_VERSION),
            "input_binding_id": binding.input_binding_id,
            "output_partition": target_partition.to_dict(),
            "rule_version": I3_PRODUCTION_DELTA_CANONICAL_PROJECTION_RULE_VERSION,
            "row_validator_rule_version": I3_PRODUCTION_DELTA_ROW_VALIDATOR_RULE_VERSION,
            "sor_expiry_rule_version": I3_PRODUCTION_DELTA_SOR_EXPIRY_RULE_VERSION,
            "source_partition": _i2_partition(
                binding.i2_partitions,
                "universe_source_daily",
            ).to_dict(),
            "source_window_rule_version": I3_PRODUCTION_DELTA_SOURCE_WINDOW_RULE_VERSION,
            "state_transition_rule_version": I3_PRODUCTION_DELTA_STATE_TRANSITION_RULE_VERSION,
        }
    )


def _delta_attestation_fields(
    *,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3ProductionDeltaInputBinding,
) -> dict[str, object]:
    expected_source_digest = _delta_source_digest(run_spec, binding)
    if prepared.source_digest != expected_source_digest:
        raise I3DeltaIOError("delta prepared source differs from exact bounded inputs")
    if prepared.canonical_projection_difference_count != 0:
        raise I3DeltaIOError("delta prepared projection contains differences")
    deep = parent.deep_attestation
    if deep is None:
        raise I3DeltaIOError("delta attestation parent lacks a deep attestation")
    if (
        binding.parent_release_id != parent.manifest.release_id
        or binding.parent_checkpoint_id != parent.checkpoint.checkpoint_id
        or binding.parent_deep_attestation_id != deep.deep_attestation_id
    ):
        raise I3DeltaIOError("delta attestation input binding names another parent")
    return {
        "run_spec_id": run_spec.run_spec_id,
        "input_binding_id": binding.input_binding_id,
        "source_digest": expected_source_digest,
        "transform_semantics_digest": run_spec.transform_semantics_digest,
        "parent_release_id": parent.manifest.release_id,
        "parent_checkpoint_id": parent.checkpoint.checkpoint_id,
        "parent_deep_attestation_id": deep.deep_attestation_id,
        "table_output_set_digest": _delta_table_output_set_digest(prepared),
        "row_change_index_digest": _delta_row_change_digest(prepared),
        "native_manifest_id": prepared.native_manifest.release_id,
        "native_manifest_artifact": prepared.native_manifest_artifact,
        "checkpoint_id": prepared.checkpoint.checkpoint_id,
        "checkpoint_artifact": prepared.checkpoint_artifact,
        "canonical_projection_digest": _delta_projection_digest(prepared, binding),
        "canonical_projection_difference_count": (prepared.canonical_projection_difference_count),
        "resource_observation_digest": stable_digest(
            {
                "observation": prepared.resource_observation.to_dict(),
                "rule_version": "s7_5_i3_production_delta_resource_observation_v1",
            }
        ),
        "terminal_session": run_spec.terminal_session,
        "availability_session": run_spec.run_available_session,
    }


def _mint_delta_attestation(
    *,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3ProductionDeltaInputBinding,
) -> DeltaMaterializationAttestation:
    attestation = DeltaMaterializationAttestation(
        **_delta_attestation_fields(
            run_spec=run_spec,
            parent=parent,
            prepared=prepared,
            binding=binding,
        )
    )
    object.__setattr__(attestation, "_seal", _DELTA_ATTESTATION_SEAL)
    _MINTED_DELTA_ATTESTATIONS[id(attestation)] = attestation
    return attestation


def _verify_delta_attestation_with_binding(
    *,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3ProductionDeltaInputBinding,
) -> DeltaMaterializationAttestation:
    attestation = _require_module_sealed_delta_attestation(prepared)
    expected = DeltaMaterializationAttestation(
        **_delta_attestation_fields(
            run_spec=run_spec,
            parent=parent,
            prepared=prepared,
            binding=binding,
        )
    )
    if attestation.logical_payload() != expected.logical_payload():
        raise I3DeltaIOError(
            "delta materialization attestation differs from exact inputs, outputs, or resources"
        )
    return attestation


def _require_module_sealed_delta_attestation(
    prepared: I3ProductionPreparedMaterialization,
) -> DeltaMaterializationAttestation:
    if type(prepared) is not DeltaPreparedMaterialization:
        raise I3DeltaIOError("delta materialization lacks the official nominal capability")
    attestation = prepared.delta_materialization_attestation
    if (
        type(attestation) is not DeltaMaterializationAttestation
        or attestation._seal is not _DELTA_ATTESTATION_SEAL
        or _MINTED_DELTA_ATTESTATIONS.get(id(attestation)) is not attestation
    ):
        raise I3DeltaIOError("delta materialization attestation is not module-sealed")
    return attestation


def _verify_prepared_delta_physical_semantics(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    prepared: I3ProductionPreparedMaterialization,
    loaded: _LoadedDeltaInputs,
) -> None:
    root = data_root.expanduser().resolve()
    parent_outputs = {item.table_name: item for item in parent.receipt.output_set.table_outputs}
    outputs = {item.table_name: item for item in prepared.table_outputs}
    segment_rows: dict[str, tuple[dict[str, object], ...]] = {}
    new_segments: dict[str, I3ProductionSegmentPin] = {}
    if tuple(item.table_name for item in prepared.table_outputs) != I3_V2_TABLE_ORDER:
        raise I3DeltaIOError("delta prepared output order differs")
    for table_name in _SMALL_TABLES:
        parent_rowset = parent_outputs[table_name].rowset_index
        child_rowset = outputs[table_name].rowset_index
        if parent_rowset is None or child_rowset is None:
            raise I3DeltaIOError("delta small-table rowset index is missing")
        if child_rowset.segments[:-1] != parent_rowset.segments:
            raise I3DeltaIOError("delta changed an existing rowset segment prefix")
        segment = child_rowset.segments[-1]
        expected_segment_id = production_delta_append_segment_id(
            table_name=table_name,
            parent_rowset_id=parent_rowset.rowset_index_id,
            parent_segment_ids=tuple(item.segment_id for item in parent_rowset.segments),
            artifact=segment.artifact,
            terminal_session=run_spec.terminal_session,
            availability_session=run_spec.run_available_session,
            native_v2_migration_id=run_spec.native_v2_migration_id,
        )
        if segment.segment_id != expected_segment_id:
            raise I3DeltaIOError("delta append segment ID does not reproduce")
        readback = readback_i3_migration_parquet_exact(
            data_root=root,
            artifact=segment.artifact,
            table_name=table_name,
            row_count=segment.row_count,
        )
        segment_rows[table_name] = tuple(dict(row) for row in readback.to_pylist())
        new_segments[table_name] = segment
    parent_dataset = parent_outputs["universe_daily"].dataset_index
    child_dataset = outputs["universe_daily"].dataset_index
    if (
        parent_dataset is None
        or child_dataset is None
        or child_dataset.partitions[:-1] != parent_dataset.partitions
        or child_dataset.partitions[-1].session_date != PRODUCTION_FIRST_DELTA_SESSION
    ):
        raise I3DeltaIOError("delta universe dataset prefix/suffix differs")
    output_rows = readback_i3_migration_parquet_exact(
        data_root=root,
        artifact=child_dataset.partitions[-1].artifact,
        table_name="universe_daily",
        row_count=child_dataset.partitions[-1].row_count,
        session_date=PRODUCTION_FIRST_DELTA_SESSION,
    ).to_pylist()
    source_receipt = _i2_partition(loaded.binding.i2_partitions, "universe_source_daily")
    source_rows = (
        _read_i2_partition_exact(root, source_receipt)
        .sort_by([("ticker", "ascending")])
        .to_pylist()
    )
    observation_rows = _read_i2_partition_exact(
        root,
        _i2_partition(loaded.binding.i2_partitions, "asset_observation_daily"),
    )
    version_rows = _read_i2_partition_exact(
        root,
        _i2_partition(loaded.binding.i2_partitions, "asset_observation_version"),
    )
    reference_metadata = _reference_metadata_by_selected_source(
        source_rows,
        observation_rows,
        version_rows,
    )
    target_rows, fallback_counts = _resolve_target_rows(
        source_rows,
        run_spec=run_spec,
        source_binding=loaded.source_binding,
        gate_b_by_composite=loaded.gate_b_by_composite,
        registries=loaded.registries,
    )
    lookback_rows: list[dict[str, object]] = []
    for pin in loaded.binding.parent_boundary_partitions:
        lookback_rows.extend(
            dict(row)
            for row in readback_i3_migration_parquet_exact(
                data_root=root,
                artifact=pin.artifact,
                table_name="universe_daily",
                row_count=pin.row_count,
                session_date=pin.session_date,
            ).to_pylist()
        )
    lookback_rows.extend(dict(row) for row in target_rows)
    try:
        materialized = _materialize_delta_day(
            target_rows=target_rows,
            lookback_rows=tuple(lookback_rows),
            target_session=PRODUCTION_FIRST_DELTA_SESSION,
            calendar=loaded.calendar_sessions,
            availability_session=run_spec.run_available_session,
            policy=run_spec.identity_policy_bundle,
            policy_snapshot=loaded.policy_snapshot,
            prior_open_aliases=parent.checkpoint.open_aliases,
            prior_assets=parent.checkpoint.asset_aggregates,
            prior_issuers=parent.checkpoint.issuer_aggregates,
            prior_unresolved=parent.checkpoint.unresolved_subjects,
            prior_terminal_rows=parent.checkpoint.terminal_row_versions,
            reference_metadata_by_source_id=reference_metadata,
            reference_metadata_available_session=(loaded.i2_run.receipt.receipt_available_session),
        )
    except Exception as exc:
        raise I3DeltaIOError("delta verifier state-transition replay failed") from exc
    _validate_materialized_delta(
        materialized=materialized,
        target_rows=target_rows,
        parent=parent,
        fallback_counts=fallback_counts,
    )
    expected_rows = {
        "asset_master": tuple(dict(row) for row in materialized.asset_rows),
        "ticker_alias": tuple(dict(row) for row in materialized.alias_rows),
        "issuer_master": tuple(dict(row) for row in materialized.issuer_rows),
    }
    for table_name in _SMALL_TABLES:
        _assert_rows_equal(table_name, segment_rows[table_name], expected_rows[table_name])
    _assert_rows_equal("universe_daily", output_rows, materialized.universe_rows)
    if _canonical_projection_difference_count(target_rows, output_rows) != 0:
        raise I3DeltaIOError("delta replay canonical projection differs")
    expected_terminal, expected_row_versions = _physical_row_lineage(
        expected_rows,
        new_segments,
        parent=parent,
        availability_session=run_spec.run_available_session,
    )
    if prepared.row_versions != expected_row_versions:
        raise I3DeltaIOError("delta prepared row changes differ from state-transition replay")
    for item in expected_row_versions:
        expected = production_delta_row_validator_digest(
            table_name=item.table_name,
            schema_digest=I3_V2_CONTRACTS[item.table_name].schema_digest,
            operation=item.operation.value,
        )
        if item.validator_semantics_digest != expected:
            raise I3DeltaIOError("delta row validator semantics are not module-owned")
    target_partition = child_dataset.partitions[-1]
    expected_partitions = (
        *parent.checkpoint.resolved_partition_map,
        ResolvedPartitionState(
            session_date=target_partition.session_date,
            partition_receipt_id=target_partition.partition_receipt_id,
            artifact=target_partition.artifact,
            row_count=target_partition.row_count,
            availability_session=target_partition.availability_session,
        ),
    )
    expected_s4 = tuple(
        S4TerminalPartitionPin(
            table_name=table_name,
            session_date=run_spec.terminal_session,
            partition_receipt_id=_i2_partition(
                loaded.binding.i2_partitions,
                table_name,
            ).partition_receipt_id,
            artifact=_i2_partition(
                loaded.binding.i2_partitions,
                table_name,
            ).artifact,
            availability_session=loaded.i2_run.receipt.receipt_available_session,
        )
        for table_name in S4_TERMINAL_TABLE_ORDER
    )
    checkpoint = prepared.checkpoint
    if (
        checkpoint.last_session != run_spec.terminal_session
        or checkpoint.source_cutoff_session != run_spec.source_cutoff_session
        or checkpoint.availability_cutoff_session != run_spec.run_available_session
        or checkpoint.s4_terminal_pins != expected_s4
        or checkpoint.calendar_digest != run_spec.calendar.calendar_artifact_id
        or checkpoint.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST
        or checkpoint.transform_semantics_digest != run_spec.transform_semantics_digest
        or checkpoint.identity_policy_bundle != run_spec.identity_policy_bundle
        or checkpoint.identity_policy_bundle_artifact != run_spec.identity_policy_bundle_artifact
        or checkpoint.open_aliases != materialized.open_aliases
        or checkpoint.asset_aggregates != materialized.asset_aggregates
        or checkpoint.issuer_aggregates != materialized.issuer_aggregates
        or checkpoint.unresolved_subjects != materialized.unresolved_subjects
        or checkpoint.resolved_partition_map != expected_partitions
        or checkpoint.terminal_row_versions != expected_terminal
    ):
        raise I3DeltaIOError("delta checkpoint differs from state-transition replay")


def _require_delta_controls(
    run_spec: I3ProductionRunSpec,
    *,
    parent: LoadedI3ProductionStaging | None,
) -> None:
    if type(run_spec) is not I3ProductionRunSpec:
        raise I3DeltaIOError("delta adapter requires an exact typed RunSpec")
    if (
        run_spec.run_kind is not I3ProductionRunKind.DELTA
        or run_spec.terminal_session != PRODUCTION_FIRST_DELTA_SESSION
        or run_spec.transform_semantics_digest != I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST
        or len(run_spec.i2_receipts) != 1
        or run_spec.i2_receipts[0].session_date != PRODUCTION_FIRST_DELTA_SESSION
    ):
        raise I3DeltaIOError("adapter is bounded to the exact 2026-07-10 production DELTA")
    if parent is None:
        return
    if type(parent) is not LoadedI3ProductionStaging:
        raise I3DeltaIOError("delta parent is not an authenticated staging object")
    parent_output_set = parent.receipt.output_set
    deep = parent.deep_attestation
    if parent_output_set is None or deep is None:
        raise I3DeltaIOError("delta parent lacks completed deep staging controls")
    parent_release = NativeV2ParentReleasePin.from_manifest(
        parent.manifest,
        path=parent_output_set.release_manifest_artifact.path,
    )
    if (
        parent.run_spec.run_kind is not I3ProductionRunKind.BASE
        or parent.run_spec.terminal_session != PRODUCTION_FIRST_DELTA_PARENT_SESSION
        or parent.checkpoint.last_session != PRODUCTION_FIRST_DELTA_PARENT_SESSION
        or run_spec.parent_release != parent_release
        or run_spec.parent_checkpoint_artifact != parent_output_set.checkpoint_artifact
        or run_spec.parent_gate_a_manifest != parent_output_set.gate_a_manifest_pin
        or run_spec.parent_shadow_completion_artifact != deep.completion_artifact
        or run_spec.parent_deep_attestation_artifact is None
        or run_spec.native_v2_migration_id != parent.run_spec.native_v2_migration_id
        or run_spec.transform_semantics_digest != parent.run_spec.transform_semantics_digest
        or run_spec.identity_policy_bundle != parent.run_spec.identity_policy_bundle
        or run_spec.calendar != parent.run_spec.calendar
    ):
        raise I3DeltaIOError("delta RunSpec differs from its authenticated BASE parent")


def _verify_i2_parent_frontier(
    run_spec: I3ProductionRunSpec,
    parent: LoadedI3ProductionStaging,
    i2_run: object,
) -> None:
    prior_frontier = parent.run_spec.i2_base_frontier
    spec = getattr(i2_run, "run_spec", None)
    receipt = getattr(i2_run, "receipt", None)
    frontier = getattr(spec, "parent_frontier", None)
    source_binding = getattr(spec, "source_binding", None)
    if (
        prior_frontier is None
        or frontier is None
        or receipt is None
        or source_binding is None
        or frontier.parent_kind is not S4ParentKind.BASE_RELEASE
        or frontier.terminal_session != PRODUCTION_FIRST_DELTA_PARENT_SESSION
        or frontier.terminal_receipt_id != prior_frontier.frontier_id
        or frontier.artifact != prior_frontier.artifact
        or receipt.parent_frontier_id != frontier.parent_frontier_id
        or source_binding.session_date != PRODUCTION_FIRST_DELTA_SESSION
        or receipt.session_date != PRODUCTION_FIRST_DELTA_SESSION
        or run_spec.i2_receipts[0].receipt_id != receipt.receipt_id
    ):
        raise I3DeltaIOError("I2 receipt does not advance the authenticated BASE frontier")


def _verify_parquet_metadata(
    root: Path,
    receipt: S4SessionPartitionReceipt,
    expected_schema: pa.Schema,
) -> None:
    _verify_exact_artifact(root, receipt.artifact)
    path = safe_relative_path(root, receipt.artifact.path)
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, pa.ArrowException) as exc:
        raise I3DeltaIOError(f"I2 partition is not readable Parquet: {receipt.table_name}") from exc
    if (
        parquet.metadata.num_rows != receipt.row_count
        or arrow_schema_digest(parquet.schema_arrow) != receipt.schema_digest
        or not parquet.schema_arrow.equals(expected_schema, check_metadata=True)
    ):
        raise I3DeltaIOError(f"I2 partition rows/schema differ: {receipt.table_name}")


def _i2_partition(
    receipts: Sequence[S4SessionPartitionReceipt],
    table_name: str,
) -> S4SessionPartitionReceipt:
    matches = tuple(item for item in receipts if item.table_name == table_name)
    if len(matches) != 1:
        raise I3DeltaIOError(f"I2 receipt does not name one exact {table_name} partition")
    return matches[0]


def _read_i2_partition_exact(
    root: Path,
    receipt: S4SessionPartitionReceipt,
) -> pa.Table:
    contract = ASSET_CONTRACTS.get(receipt.table_name)
    if contract is None:
        raise I3DeltaIOError("I2 partition names an unknown table")
    _verify_parquet_metadata(root, receipt, contract.arrow_schema)
    try:
        table = pq.ParquetFile(safe_relative_path(root, receipt.artifact.path)).read()
    except (OSError, pa.ArrowException) as exc:
        raise I3DeltaIOError(f"cannot read exact I2 partition: {receipt.table_name}") from exc
    if (
        table.num_rows != receipt.row_count
        or not table.schema.equals(contract.arrow_schema, check_metadata=True)
        or any(value != receipt.session_date for value in table.column("session_date").to_pylist())
    ):
        raise I3DeltaIOError(f"I2 partition content differs: {receipt.table_name}")
    return table


def _reference_metadata_by_selected_source(
    source_rows: Sequence[Mapping[str, object]],
    observation_table: pa.Table | Sequence[Mapping[str, object]],
    version_table: pa.Table | Sequence[Mapping[str, object]],
) -> Mapping[str, Mapping[str, object]]:
    observations = (
        observation_table.to_pylist()
        if isinstance(observation_table, pa.Table)
        else list(observation_table)
    )
    versions = (
        version_table.to_pylist() if isinstance(version_table, pa.Table) else list(version_table)
    )
    observation_by_id: dict[str, Mapping[str, object]] = {}
    for raw in observations:
        row = dict(raw)
        source_id = _digest(row.get("source_record_id"), "I2 observation source-record ID")
        if source_id in observation_by_id:
            raise I3DeltaIOError("I2 observation partition repeats a source-record ID")
        observation_by_id[source_id] = row
    versions_by_ticker: dict[str, list[Mapping[str, object]]] = {}
    for raw in versions:
        row = dict(raw)
        ticker = _nonempty_text(row.get("ticker"), "I2 version ticker")
        versions_by_ticker.setdefault(ticker, []).append(row)

    result: dict[str, Mapping[str, object]] = {}
    selected_ids: set[str] = set()
    expected_version_tickers: set[str] = set()
    for raw in source_rows:
        source = dict(raw)
        ticker = _nonempty_text(source.get("ticker"), "I2 selected ticker")
        source_id = _digest(
            source.get("selected_source_record_id"),
            "I2 selected source-record ID",
        )
        if source_id in selected_ids:
            raise I3DeltaIOError("I2 universe reuses one selected source record")
        selected_ids.add(source_id)
        observation = observation_by_id.get(source_id)
        if observation is None:
            raise I3DeltaIOError("I2 selected source record is absent from observations")
        if (
            observation.get("session_date") != source.get("session_date")
            or observation.get("ticker") != ticker
            or bool(observation.get("requested_active")) != bool(source.get("active_on_date"))
        ):
            raise I3DeltaIOError("I2 selected observation differs from universe membership")
        for field_name in (
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
        ):
            if observation.get(field_name) != source.get(field_name):
                raise I3DeltaIOError("I2 selected observation projection differs")

        group = versions_by_ticker.get(ticker, [])
        expected_count = source.get("source_version_count")
        if type(expected_count) is not int or expected_count <= 0:
            raise I3DeltaIOError("I2 version group row count differs from universe membership")
        if expected_count == 1:
            if (
                group
                or source.get("version_group_id") is not None
                or source.get("selection_status") != "singleton"
            ):
                raise I3DeltaIOError("I2 singleton version projection differs")
            reference_name = _optional_clean_text(observation.get("name"))
            if reference_name is not None:
                result[source_id] = {"reference_name": reference_name, "sic_code": None}
            continue

        expected_version_tickers.add(ticker)
        if len(group) != expected_count:
            raise I3DeltaIOError("I2 version group row count differs from universe membership")
        if any(
            row.get("session_date") != source.get("session_date")
            or row.get("version_group_id") != source.get("version_group_id")
            or row.get("version_count") != expected_count
            or row.get("selected_source_record_id") != source_id
            or row.get("selection_status") != source.get("selection_status")
            for row in group
        ):
            raise I3DeltaIOError("I2 version group lineage differs from universe membership")
        if any(row.get("source_record_id") not in observation_by_id for row in group):
            raise I3DeltaIOError("I2 version source record is absent from observations")
        selected = [row for row in group if row.get("is_selected") is True]
        if (
            len(selected) != 1
            or selected[0].get("source_record_id") != source_id
            or selected[0].get("selection_rank") != 1
        ):
            raise I3DeltaIOError("I2 version group does not prove one selected source record")
        reference_name = _optional_clean_text(observation.get("name"))
        if reference_name is not None:
            result[source_id] = {"reference_name": reference_name, "sic_code": None}
    if set(versions_by_ticker) != expected_version_tickers:
        raise I3DeltaIOError("I2 version partition contains an unexpected ticker group")
    version_source_ids = {
        _digest(row.get("source_record_id"), "I2 version source-record ID") for row in versions
    }
    if set(observation_by_id) != selected_ids | version_source_ids:
        raise I3DeltaIOError("I2 observation/version projection is not exact")
    return result


def _preflight_delta_entry_resources(
    root: Path,
    caps: I3ProductionResourceCaps,
    direct_artifacts: Sequence[ArtifactPin],
) -> None:
    """Reject impossible fixed caps before any parent/control payload is read."""

    minimum_rss = 1024**3 + I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES
    if caps.rss_bytes_hard_cap < minimum_rss:
        raise I3DeltaIOError("DELTA RSS cap is below the module-owned fixed reserve")
    if caps.temporary_bytes_hard_cap < I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP:
        raise I3DeltaIOError("DELTA temporary cap is below the module-owned fixed reserve")
    artifacts = _unique_artifact_pins(tuple(direct_artifacts))
    largest = 0
    for pin in artifacts:
        path = safe_relative_path(root, pin.path)
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise I3DeltaIOError(
                f"direct DELTA control is unavailable before preflight: {pin.path}"
            ) from exc
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink() or metadata.st_size != pin.bytes:
            raise I3DeltaIOError(
                f"direct DELTA control metadata differs before preflight: {pin.path}"
            )
        largest = max(largest, pin.bytes)
    direct_rss = (
        max(1024**3, largest * 3) + I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES
    )
    if direct_rss > caps.rss_bytes_hard_cap:
        raise I3DeltaIOError("direct DELTA control estimate exceeds the RSS hard cap")


def _validate_resource_estimate(
    run_spec: I3ProductionRunSpec,
    estimate: I3ProductionDeltaResourceEstimate,
) -> None:
    caps = run_spec.resource_caps
    if estimate.estimated_peak_rss_bytes > caps.rss_bytes_hard_cap:
        raise I3DeltaIOError("estimated DELTA RSS exceeds the hard cap")
    if estimate.estimated_output_bytes > caps.output_bytes_hard_cap:
        raise I3DeltaIOError("estimated DELTA output exceeds the hard cap")
    if estimate.estimated_output_rows > caps.output_rows_hard_cap:
        raise I3DeltaIOError("estimated DELTA output rows exceed the hard cap")
    if estimate.estimated_temporary_bytes > caps.temporary_bytes_hard_cap:
        raise I3DeltaIOError("estimated DELTA temporary bytes exceed the hard cap")


def _check_preflight_resources(
    root: Path,
    run_spec: I3ProductionRunSpec,
    estimate: I3ProductionDeltaResourceEstimate,
) -> int:
    peak = _peak_rss_bytes()
    free = shutil.disk_usage(root).free
    if peak > run_spec.resource_caps.rss_bytes_hard_cap:
        raise I3DeltaIOError("DELTA RSS exceeds the hard cap")
    if free < estimate.minimum_free_disk_bytes_required:
        raise I3DeltaIOError(
            "DELTA free disk cannot preserve its hard floor through the estimated peak"
        )
    return free


def _check_live_resources(root: Path, run_spec: I3ProductionRunSpec) -> int:
    peak = _peak_rss_bytes()
    free = shutil.disk_usage(root).free
    if peak > run_spec.resource_caps.rss_bytes_hard_cap:
        raise I3DeltaIOError("DELTA RSS exceeds the hard cap")
    if free < max(
        _MINIMUM_DELTA_DISK_RESERVE,
        run_spec.resource_caps.disk_free_bytes_hard_floor,
    ):
        raise I3DeltaIOError("DELTA free disk is below its hard floor")
    return free


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _require_empty_workspace(root: Path, workspace: Path) -> None:
    try:
        workspace.relative_to(root)
    except ValueError as exc:
        raise I3DeltaIOError("DELTA workspace escapes data_root") from exc
    if not workspace.is_dir() or workspace.is_symlink():
        raise I3DeltaIOError("executor DELTA workspace must be a regular directory")
    try:
        if any(workspace.iterdir()):
            raise I3DeltaIOError("DELTA workspace is not empty; no-clobber enforced")
    except OSError as exc:
        raise I3DeltaIOError("cannot inspect DELTA workspace") from exc


def _workspace_relative(root: Path, path: Path) -> str:
    try:
        relative = path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise I3DeltaIOError("DELTA output path escapes data_root") from exc
    _explicit_path(relative)
    return relative


def _write_bytes_no_clobber(
    root: Path,
    relative: str,
    content: bytes,
    *,
    run_spec: I3ProductionRunSpec,
) -> ArtifactPin:
    _explicit_path(relative)
    path = safe_relative_path(root, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o444)
    except FileExistsError as exc:
        raise I3DeltaIOError(f"immutable DELTA output already exists: {relative}") from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        # Retain a partial immutable path as fail-closed evidence.
        raise
    _check_live_resources(root, run_spec)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _read_exact_path(root: Path, relative: str) -> bytes:
    _explicit_path(relative)
    path = safe_relative_path(root, relative)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise I3DeltaIOError(f"exact path is unavailable: {relative}") from exc
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise I3DeltaIOError(f"exact path is not a regular file: {relative}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise I3DeltaIOError(f"cannot read exact path: {relative}") from exc


def _read_exact_artifact(root: Path, pin: ArtifactPin) -> bytes:
    _verify_exact_artifact(root, pin)
    return _read_exact_path(root, pin.path)


def _verify_exact_artifact(root: Path, pin: ArtifactPin) -> None:
    if not isinstance(pin, ArtifactPin):
        raise I3DeltaIOError("exact artifact pin is invalid")
    content = _read_exact_path(root, pin.path)
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I3DeltaIOError(f"exact artifact differs from pin: {pin.path}")


def _artifact_from_exact_pin(pin: object) -> ArtifactPin:
    try:
        artifact = ArtifactPin(
            path=pin.path,
            sha256=pin.sha256,
            bytes=pin.bytes,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise I3DeltaIOError("source exact pin is invalid") from exc
    _explicit_path(artifact.path)
    return artifact


def _strict_json(content: bytes) -> Mapping[str, object]:
    def reject_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise I3DeltaIOError(f"duplicate exact-control JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise I3DeltaIOError(f"non-finite exact-control JSON value is forbidden: {value}")

    try:
        value = json.loads(
            content,
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3DeltaIOError("exact control is not valid JSON") from exc
    if not isinstance(value, dict):
        raise I3DeltaIOError("exact control JSON root is not an object")
    return value


def _closed_mapping(
    value: object,
    fields: set[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise I3DeltaIOError(f"{label} fields differ")
    return value


def _artifact_pin_from_dict(value: object, label: str) -> ArtifactPin:
    item = _closed_mapping(value, {"bytes", "path", "sha256"}, label)
    path = _nonempty_text(item["path"], f"{label} path")
    sha256 = _nonempty_text(item["sha256"], f"{label} SHA-256")
    size = item["bytes"]
    if type(size) is not int:
        raise I3DeltaIOError(f"{label} bytes must be an integer")
    try:
        return ArtifactPin(path=path, sha256=sha256, bytes=size)
    except ValueError as exc:
        raise I3DeltaIOError(f"{label} is invalid") from exc


def _date_from_json(value: object, label: str) -> date:
    text = _nonempty_text(value, label)
    try:
        result = date.fromisoformat(text)
    except ValueError as exc:
        raise I3DeltaIOError(f"{label} is not an ISO date") from exc
    if result.isoformat() != text:
        raise I3DeltaIOError(f"{label} is not canonical")
    return result


def _mapping_array(value: object, label: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise I3DeltaIOError(f"{label} must be an array of objects")
    return tuple(value)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _explicit_path(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise I3DeltaIOError("DELTA path must be nonempty text")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or (path.parts and path.parts[0] in {"tmp", "fixtures"})
        or any("latest" in part.lower() for part in path.parts)
        or any(character in value for character in "*?[]{}")
    ):
        raise I3DeltaIOError("DELTA path must be explicit, normalized, and non-latest")


def _delta_config_relative(config: I3ProductionDeltaRunConfig) -> str:
    return f"{_PRODUCTION_CONTROL_ROOT}/delta-configs/config_id={config.config_id}/config.json"


def _write_control_immutable(root: Path, relative: str, content: bytes) -> ArtifactPin:
    _explicit_path(relative)
    try:
        document = write_bytes_immutable(
            root,
            safe_relative_path(root, relative),
            content,
        )
        return ArtifactPin(
            path=str(document["path"]),
            sha256=str(document["sha256"]),
            bytes=int(document["bytes"]),
        )
    except (ArtifactError, OSError, ValueError) as exc:
        raise I3DeltaIOError(f"cannot immutably store DELTA control: {relative}") from exc


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise I3DeltaIOError(f"{label} must be lowercase SHA-256")
    return value


def _nonempty_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I3DeltaIOError(f"{label} must be nonempty text")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return _nonempty_text(value, "optional text")


def _optional_clean_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise I3DeltaIOError("reference metadata must be nonempty text")
    return value.strip()


def _workspace_file_bytes(workspace: Path) -> int:
    total = 0
    for directory, _, files in os.walk(workspace):
        for name in files:
            path = Path(directory, name)
            if path.is_symlink() or not path.is_file():
                raise I3DeltaIOError("DELTA workspace contains a non-regular file")
            total += path.stat().st_size
    return total


def _prepared_output_artifacts(
    table_outputs: Sequence[I3ProductionTableOutput],
    *,
    native_manifest_artifact: ArtifactPin,
    checkpoint_artifact: ArtifactPin,
) -> tuple[ArtifactPin, ...]:
    pins: list[ArtifactPin] = [native_manifest_artifact, checkpoint_artifact]
    for output in table_outputs:
        pins.append(output.manifest_output.artifact)
        if output.dataset_index is not None:
            pins.extend(item.artifact for item in output.dataset_index.partitions)
        if output.rowset_index is not None:
            pins.extend(item.artifact for item in output.rowset_index.segments)
    return _unique_artifact_pins(tuple(pins))


__all__ = [
    "I3_PRODUCTION_DELTA_CONFIG_RULE_VERSION",
    "PRODUCTION_DELTA_BOUNDARY_SESSIONS",
    "PRODUCTION_FIRST_DELTA_PARENT_SESSION",
    "PRODUCTION_FIRST_DELTA_SESSION",
    "DeltaMaterializationAttestation",
    "DeltaPreparedMaterialization",
    "I3DeltaIOError",
    "I3ProductionDeltaInputBinding",
    "I3ProductionDeltaResourceEstimate",
    "I3ProductionDeltaRunConfig",
    "PreparedI3ProductionDeltaRunSpec",
    "ProductionDeltaMaterializer",
    "build_production_delta_run_spec",
    "estimate_production_delta_resources",
    "load_i3_production_delta_config_exact",
    "load_production_delta_input_binding",
    "load_production_delta_materializer",
    "prepare_i3_production_delta_run_spec",
    "prepare_production_delta",
    "store_i3_production_delta_config",
    "verify_delta_materialization_attestation",
]
