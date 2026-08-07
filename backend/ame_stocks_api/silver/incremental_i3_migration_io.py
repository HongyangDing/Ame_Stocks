"""Exact-pin, streaming IO adapter for the production I3 compact base.

The adapter has one deliberately narrow job: migrate the immutable S7 v1
four-table base into a production native-v2 *staging* envelope.  Every source
Parquet file is supplied by an exact pin.  No directory scan, glob, ``latest``
pointer, network call, publication, or cutover exists in this module.

The 69M-row membership history is used in two bounded ways:

* Polars lazy/streaming group-bys reconstruct the complete distinct sets needed
  by the checkpoint without hashing source rows in Python; and
* each explicitly listed session partition is migrated, written, read back,
  schema/row-count checked, and projected to the exact supplied v1 partition
  before the next session is opened.

The three compact v1 tables are intentionally handled in memory (production is
roughly 15k/33k/15k rows).  Outputs live only in the executor-provided empty
workspace and are immutable/no-clobber.  A BASE run must terminate at the v1
terminal session and bind the exact authenticated I2 ``S4BaseFrontier``.  The
2026-07-10 clean append is a DELTA concern and is rejected here.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import resource
import shutil
import sys
import time
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Final

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental import (
    S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
)
from ame_stocks_api.silver.asset_incremental_contract import S4BaseFrontier
from ame_stocks_api.silver.asset_release_set import (
    AssetReleaseSet,
    load_exact_asset_release_set_control,
)
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    RowVersionOperation,
)
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
    UnresolvedSubjectState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_migration_core import (
    MIGRATION_RULE_VERSION,
    LegacyAssetAggregateProjection,
    LegacyIssuerAggregateProjection,
    MigratedAliasRoot,
    build_asset_aggregate_state,
    build_issuer_aggregate_state,
    materialize_asset_root,
    materialize_issuer_root,
    migrate_alias_root,
    migration_source_seed_digest,
)
from ame_stocks_api.silver.incremental_i3_production import (
    I3ProductionPreparedMaterialization,
    I3ProductionPreparedRowVersion,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionDatasetIndex,
    I3ProductionOutputStorage,
    I3ProductionPartitionPin,
    I3ProductionResourceObservation,
    I3ProductionRunKind,
    I3ProductionRunSpec,
    I3ProductionTableOutput,
    LoadedI3ProductionStaging,
)

COMPACT_BASE_SOURCE_RULE_VERSION: Final = "s7_5_i3_compact_base_source_v1"
COMPACT_BASE_INPUT_BINDING_RULE_VERSION: Final = "s7_5_i3_compact_base_exact_input_binding_v1"
COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION: Final = "s7_5_i3_compact_base_partition_receipt_v1"
COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION: Final = "s7_5_i3_compact_base_s4_terminal_receipt_v1"
COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION: Final = "s7_5_i3_compact_base_unresolved_seed_v1"
COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION: Final = "s7_5_i3_compact_base_new_root_validator_v1"
COMPACT_BASE_MATERIALIZATION_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_compact_base_materialization_attestation_v1"
)
COMPACT_BASE_MIGRATION_SEMANTICS_RULE_VERSION: Final = "s7_5_i3_compact_base_migration_semantics_v1"
COMPACT_BASE_CANONICAL_PROJECTION_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_compact_base_canonical_projection_attestation_v1"
)
COMPACT_BASE_ROW_CHANGE_INDEX_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_compact_base_row_change_index_attestation_v1"
)

_MINIMUM_PRODUCTION_DISK_FLOOR: Final = 40 * 1024**3
_ESTIMATED_BASELINE_RSS: Final = 2 * 1024**3
_ESTIMATED_OUTPUT_MULTIPLIER_NUMERATOR: Final = 3
_ESTIMATED_OUTPUT_MULTIPLIER_DENOMINATOR: Final = 2
_V1_TABLE_ORDER: Final = (
    "asset_master",
    "ticker_alias",
    "issuer_master",
    "universe_daily",
)
_S4_TABLES: Final = tuple(S4_TERMINAL_TABLE_ORDER)
_SMALL_TABLES: Final = ("asset_master", "ticker_alias", "issuer_master")
_VERSION_SHAPE: Final[Mapping[str, tuple[str, str, str]]] = {
    "asset_master": (
        "asset_id",
        "asset_master_version_id",
        "predecessor_asset_master_version_id",
    ),
    "ticker_alias": (
        "alias_segment_id",
        "alias_resolution_version_id",
        "predecessor_alias_resolution_version_id",
    ),
    "issuer_master": (
        "issuer_id",
        "issuer_master_version_id",
        "predecessor_issuer_master_version_id",
    ),
}
_V2_ENVELOPE_FIELDS: Final[Mapping[str, frozenset[str]]] = {
    "asset_master": frozenset(
        {
            "aggregate_state_digest",
            "asset_master_version_id",
            "predecessor_asset_master_version_id",
            "version_available_session",
        }
    ),
    "ticker_alias": frozenset(
        {
            "alias_is_tombstone",
            "alias_resolution_version_id",
            "alias_resolution_version_id_rule_version",
            "alias_segment_id",
            "alias_segment_id_rule_version",
            "alias_tombstone_reason_code",
            "alias_version_available_session",
            "decision_lineage_ids",
            "evidence_cutoff_session",
            "identity_policy_bundle_id",
            "predecessor_alias_resolution_version_id",
            "provider_id",
            "provider_locale",
            "provider_market",
            "resolution_available_session",
            "segment_origin_source_record_id",
            "share_class_decision_lineage_ids",
            "source_record_set_digest",
        }
    ),
    "issuer_master": frozenset(
        {
            "aggregate_state_digest",
            "issuer_master_version_id",
            "predecessor_issuer_master_version_id",
            "version_available_session",
        }
    ),
    "universe_daily": frozenset(
        {
            "alias_resolution_version_id",
            "alias_segment_id",
            "asset_master_version_id",
            "identity_policy_bundle_id",
            "issuer_master_version_id",
            "row_available_session",
        }
    ),
}
_REGISTRY_SOURCE_FIELDS: Final[Mapping[str, tuple[str, str]]] = {
    "identity_adjudication": (
        "source_identity_adjudication_release_id",
        "source_identity_adjudication_release_available_session",
    ),
    "identity_cross_market_adjudication": (
        "source_identity_cross_market_adjudication_release_id",
        "source_identity_cross_market_adjudication_release_available_session",
    ),
    "provider_composite_override": (
        "source_provider_composite_override_release_id",
        "source_provider_composite_override_release_available_session",
    ),
    "share_class_adjudication": (
        "source_share_class_adjudication_release_id",
        "source_share_class_adjudication_release_available_session",
    ),
    "asset_transition": (
        "source_asset_transition_release_id",
        "source_asset_transition_release_available_session",
    ),
}
_SESSION_PARTITION_PATH = re.compile(
    r"(?:^|/)session_date=(?P<session>[0-9]{4}-[0-9]{2}-[0-9]{2})/"
    r"part-00000[.]parquet$"
)
_S7_RELEASE_SET_FIELDS: Final = frozenset(
    {
        "approval",
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_manifest",
        "candidate_qa",
        "full_completion",
        "full_completion_id",
        "intent",
        "intent_id",
        "members",
        "plan",
        "plan_id",
        "policy_version",
        "published_at_utc",
        "release_availability",
        "release_set_id",
        "release_set_version",
        "source_binding_id",
        "state",
        "table_order",
        "visibility_rule",
    }
)
_S7_MEMBER_FIELDS: Final = frozenset(
    {
        "approval",
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_manifest",
        "candidate_qa",
        "contract",
        "full_completion",
        "full_completion_id",
        "output_receipts",
        "output_set_digest",
        "plan",
        "plan_id",
        "policy_version",
        "published_at_utc",
        "release_availability",
        "release_id",
        "release_version",
        "row_count",
        "source_binding_id",
        "state",
        "table_name",
    }
)


class I3MigrationIOError(RuntimeError):
    """Raised when an exact base input or immutable output fails closed."""


@dataclass(frozen=True, slots=True)
class I3MigrationParquetPin:
    """One explicit exact source Parquet pin; it never resolves a directory."""

    table_name: str
    artifact: ArtifactPin
    row_count: int
    contract_id: str
    schema_digest: str
    availability_session: date
    session_date: date | None = None

    def __post_init__(self) -> None:
        contracts = {
            **S7_DERIVED_CONTRACTS,
            **ASSET_CONTRACTS,
        }
        contract = contracts.get(self.table_name)
        if contract is None:
            raise I3MigrationIOError("migration source table is invalid")
        if not isinstance(self.artifact, ArtifactPin):
            raise I3MigrationIOError("migration source artifact is invalid")
        _explicit_path(self.artifact.path)
        if not self.artifact.path.endswith(".parquet"):
            raise I3MigrationIOError("migration source artifact must be Parquet")
        if type(self.row_count) is not int or self.row_count < 0:
            raise I3MigrationIOError("migration source row count is invalid")
        if self.contract_id != contract.contract_id or self.schema_digest != contract.schema_digest:
            raise I3MigrationIOError("migration source schema pin differs from contract")
        if type(self.availability_session) is not date:
            raise I3MigrationIOError("migration source availability is invalid")
        partitioned = self.table_name == "universe_daily" or self.table_name in _S4_TABLES
        if partitioned != (type(self.session_date) is date):
            raise I3MigrationIOError("migration source partition session shape is invalid")
        if self.session_date is not None and self.availability_session < self.session_date:
            raise I3MigrationIOError("migration source availability precedes its session")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "availability_session": self.availability_session.isoformat(),
            "contract_id": self.contract_id,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": (None if self.session_date is None else self.session_date.isoformat()),
            "table_name": self.table_name,
        }


@dataclass(frozen=True, slots=True)
class I3LegacyV1BasePins:
    """Exact S7 v1 release pin plus the complete four-table output pin list."""

    release_set_id: str
    release_set_artifact: ArtifactPin
    member_outputs: tuple[I3MigrationParquetPin, ...]

    def __post_init__(self) -> None:
        _digest(self.release_set_id, "legacy release-set ID")
        if self.release_set_id != LEGACY_S7_V1_RELEASE_SET_ID:
            raise I3MigrationIOError("compact base binds the wrong immutable S7 v1 oracle")
        if not isinstance(self.release_set_artifact, ArtifactPin):
            raise I3MigrationIOError("legacy release-set artifact is invalid")
        _explicit_path(self.release_set_artifact.path)
        if type(self.member_outputs) is not tuple or not self.member_outputs:
            raise I3MigrationIOError("legacy base requires explicit member output pins")
        if not all(type(item) is I3MigrationParquetPin for item in self.member_outputs):
            raise I3MigrationIOError("legacy member output pin is invalid")
        tables = {item.table_name for item in self.member_outputs}
        if tables != set(_V1_TABLE_ORDER):
            raise I3MigrationIOError("legacy member output table roles are incomplete")
        for table in _SMALL_TABLES:
            values = self.pins_for(table)
            if not values or any(item.session_date is not None for item in values):
                raise I3MigrationIOError("legacy compact table pin shape is invalid")
        universe = self.pins_for("universe_daily")
        sessions = tuple(item.session_date for item in universe)
        if sessions != tuple(sorted(set(sessions))):
            raise I3MigrationIOError(
                "legacy universe partition pins must be sorted and session-unique"
            )
        paths = [item.artifact.path for item in self.member_outputs]
        if len(paths) != len(set(paths)):
            raise I3MigrationIOError("legacy member output paths are duplicated")
        expected = tuple(sorted(self.member_outputs, key=_migration_pin_sort_key))
        if self.member_outputs != expected:
            raise I3MigrationIOError("legacy member output pins are not canonical ordered")

    def pins_for(self, table_name: str) -> tuple[I3MigrationParquetPin, ...]:
        return tuple(item for item in self.member_outputs if item.table_name == table_name)

    @property
    def complete_partition_pin_digest(self) -> str:
        return stable_digest(
            {
                "member_outputs": [item.to_dict() for item in self.member_outputs],
                "release_set_artifact": self.release_set_artifact.to_dict(),
                "release_set_id": self.release_set_id,
                "rule_version": "s7_5_i3_complete_v1_member_output_pin_set_v1",
            }
        )


@dataclass(frozen=True, slots=True)
class I3S4BasePins:
    """Exact S4 release member pins used for names and the terminal frontier."""

    release_set_id: str
    release_set_artifact: ArtifactPin
    universe_source_partitions: tuple[I3MigrationParquetPin, ...]
    terminal_partitions: tuple[I3MigrationParquetPin, ...]

    def __post_init__(self) -> None:
        _digest(self.release_set_id, "S4 release-set ID")
        if not isinstance(self.release_set_artifact, ArtifactPin):
            raise I3MigrationIOError("S4 release-set artifact is invalid")
        _explicit_path(self.release_set_artifact.path)
        if (
            type(self.universe_source_partitions) is not tuple
            or not self.universe_source_partitions
            or not all(
                type(item) is I3MigrationParquetPin and item.table_name == "universe_source_daily"
                for item in self.universe_source_partitions
            )
        ):
            raise I3MigrationIOError("S4 universe-source pin list is invalid")
        sessions = tuple(item.session_date for item in self.universe_source_partitions)
        if sessions != tuple(sorted(set(sessions))):
            raise I3MigrationIOError("S4 universe-source pins are not session ordered")
        if (
            type(self.terminal_partitions) is not tuple
            or tuple(item.table_name for item in self.terminal_partitions) != _S4_TABLES
        ):
            raise I3MigrationIOError("S4 terminal pins must use the fixed three-table order")
        terminal_sessions = {item.session_date for item in self.terminal_partitions}
        if len(terminal_sessions) != 1:
            raise I3MigrationIOError("S4 terminal pins do not share one session")
        terminal_universe = self.terminal_partitions[_S4_TABLES.index("universe_source_daily")]
        if terminal_universe != self.universe_source_partitions[-1]:
            raise I3MigrationIOError(
                "S4 terminal universe pin differs from the complete explicit history"
            )
        paths = [
            *(item.artifact.path for item in self.universe_source_partitions),
            *(item.artifact.path for item in self.terminal_partitions),
        ]
        # The terminal universe pin intentionally appears in both semantic roles.
        if len(set(paths)) != len(paths) - 1:
            raise I3MigrationIOError("S4 base pins contain an unexpected duplicate path")

    @property
    def terminal_session(self) -> date:
        value = self.terminal_partitions[0].session_date
        assert value is not None
        return value


@dataclass(frozen=True, slots=True)
class I3CompactBaseInputBinding:
    """Complete exact member-pin expansion of the two published base releases."""

    legacy: I3LegacyV1BasePins
    s4: I3S4BasePins

    def __post_init__(self) -> None:
        if not isinstance(self.legacy, I3LegacyV1BasePins) or not isinstance(self.s4, I3S4BasePins):
            raise I3MigrationIOError("compact-base input binding is invalid")
        legacy_terminal = self.legacy.pins_for("universe_daily")[-1].session_date
        if legacy_terminal != self.s4.terminal_session:
            raise I3MigrationIOError("S7 and S4 input bindings have different terminals")

    @property
    def input_binding_id(self) -> str:
        return stable_digest(
            {
                "legacy_complete_partition_pin_digest": (self.legacy.complete_partition_pin_digest),
                "rule_version": COMPACT_BASE_INPUT_BINDING_RULE_VERSION,
                "s4_release_set_artifact": self.s4.release_set_artifact.to_dict(),
                "s4_release_set_id": self.s4.release_set_id,
                "s4_terminal_session": self.s4.terminal_session.isoformat(),
                "s4_universe_source_partitions": [
                    item.to_dict() for item in self.s4.universe_source_partitions
                ],
                "s4_terminal_partitions": [item.to_dict() for item in self.s4.terminal_partitions],
            }
        )

    def materializer(self) -> CompactBaseMigrationMaterializer:
        return CompactBaseMigrationMaterializer(self.legacy, self.s4)


@dataclass(frozen=True, slots=True)
class I3CompactBaseResourceEstimate:
    """Conservative preflight estimate derived only from the supplied exact pins."""

    estimated_peak_rss_bytes: int
    estimated_output_bytes: int
    estimated_temporary_bytes: int
    minimum_free_disk_bytes_required: int
    source_bytes: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "estimated_output_bytes": self.estimated_output_bytes,
            "estimated_peak_rss_bytes": self.estimated_peak_rss_bytes,
            "estimated_temporary_bytes": self.estimated_temporary_bytes,
            "minimum_free_disk_bytes_required": self.minimum_free_disk_bytes_required,
            "rule_version": "s7_5_i3_compact_base_resource_estimate_v1",
            "source_bytes": self.source_bytes,
        }


@dataclass(frozen=True, slots=True, weakref_slot=True)
class CompactBaseMaterializationAttestation:
    """Process-local capability proving the official BASE adapter made a bundle.

    The logical fields are intentionally inspectable for executor diagnostics,
    but construction alone grants no authority.  A valid instance must also
    carry the module-private seal *and* be the exact object registered by the
    mint below.  This prevents a structural materializer, ``dataclasses.replace``
    copy, or self-consistent reimplementation from reporting its own proof.

    The capability is not a publish artifact and is deliberately not
    serializable.  Durable staging remains protected by the executor's exact
    controls and deep-verification attestation.
    """

    run_spec_id: str
    input_binding_id: str
    input_authority_digest: str
    source_digest: str
    migration_semantics_digest: str
    table_output_set_digest: str
    native_manifest_id: str
    native_manifest_artifact: ArtifactPin
    checkpoint_id: str
    checkpoint_artifact: ArtifactPin
    row_change_index_digest: str
    resource_observation_digest: str
    canonical_projection_digest: str
    canonical_projection_difference_count: int
    terminal_session: date
    availability_session: date
    _seal: object = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "BASE attestation RunSpec ID"),
            (self.input_binding_id, "BASE attestation input-binding ID"),
            (self.input_authority_digest, "BASE attestation input-authority digest"),
            (self.source_digest, "BASE attestation source digest"),
            (
                self.migration_semantics_digest,
                "BASE attestation migration-semantics digest",
            ),
            (self.table_output_set_digest, "BASE attestation table-output digest"),
            (self.native_manifest_id, "BASE attestation native-manifest ID"),
            (self.checkpoint_id, "BASE attestation checkpoint ID"),
            (self.row_change_index_digest, "BASE attestation row-change digest"),
            (
                self.resource_observation_digest,
                "BASE attestation resource-observation digest",
            ),
            (
                self.canonical_projection_digest,
                "BASE attestation canonical-projection digest",
            ),
        ):
            _digest(value, label)
        if not isinstance(self.native_manifest_artifact, ArtifactPin):
            raise I3MigrationIOError("BASE attestation native-manifest pin is invalid")
        if not isinstance(self.checkpoint_artifact, ArtifactPin):
            raise I3MigrationIOError("BASE attestation checkpoint pin is invalid")
        if self.canonical_projection_difference_count != 0:
            raise I3MigrationIOError("BASE attestation cannot authorize projection differences")
        if type(self.terminal_session) is not date:
            raise I3MigrationIOError("BASE attestation terminal session is invalid")
        if type(self.availability_session) is not date:
            raise I3MigrationIOError("BASE attestation availability session is invalid")
        if self.availability_session < self.terminal_session:
            raise I3MigrationIOError("BASE attestation availability precedes its terminal")

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "canonical_projection_difference_count": (self.canonical_projection_difference_count),
            "canonical_projection_digest": self.canonical_projection_digest,
            "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
            "checkpoint_id": self.checkpoint_id,
            "input_authority_digest": self.input_authority_digest,
            "input_binding_id": self.input_binding_id,
            "migration_semantics_digest": self.migration_semantics_digest,
            "native_manifest_artifact": self.native_manifest_artifact.to_dict(),
            "native_manifest_id": self.native_manifest_id,
            "resource_observation_digest": self.resource_observation_digest,
            "row_change_index_digest": self.row_change_index_digest,
            "rule_version": COMPACT_BASE_MATERIALIZATION_ATTESTATION_RULE_VERSION,
            "run_kind": I3ProductionRunKind.BASE.value,
            "run_spec_id": self.run_spec_id,
            "source_digest": self.source_digest,
            "table_output_set_digest": self.table_output_set_digest,
            "terminal_session": self.terminal_session.isoformat(),
        }

    @property
    def attestation_id(self) -> str:
        return stable_digest(self.logical_payload())


@dataclass(frozen=True, slots=True)
class CompactBasePreparedMaterialization(I3ProductionPreparedMaterialization):
    """Nominal BASE prepared bundle carrying the module-owned capability."""

    base_materialization_attestation: CompactBaseMaterializationAttestation


_BASE_ATTESTATION_SEAL: Final = object()
_MINTED_BASE_ATTESTATIONS: Final[
    weakref.WeakValueDictionary[int, CompactBaseMaterializationAttestation]
] = weakref.WeakValueDictionary()


class CompactBaseMigrationMaterializer:
    """Executor seam carrying the explicit, preselected production base pins."""

    def __init__(self, legacy: I3LegacyV1BasePins, s4: I3S4BasePins) -> None:
        if type(legacy) is not I3LegacyV1BasePins or type(s4) is not I3S4BasePins:
            raise I3MigrationIOError("compact base materializer inputs are invalid")
        self._legacy = legacy
        self._s4 = s4

    def prepare(
        self,
        *,
        data_root: Path,
        run_spec: I3ProductionRunSpec,
        parent: LoadedI3ProductionStaging | None,
        workspace: Path,
    ) -> CompactBasePreparedMaterialization:
        if parent is not None:
            raise I3MigrationIOError("compact base adapter cannot consume a v2 parent")
        return prepare_compact_base(
            data_root=data_root,
            run_spec=run_spec,
            workspace=workspace,
            legacy=self._legacy,
            s4=self._s4,
        )


def load_compact_base_input_binding(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
) -> I3CompactBaseInputBinding:
    """Expand the two exact release manifests; never enumerate the filesystem.

    The production executor authenticates the RunSpec dependencies before it
    calls a materializer.  This loader independently checks the exact pinned
    release bytes and expands only the member/output arrays named by those
    controls.  In particular, it performs no glob, directory walk, or latest
    resolution even though the resulting binding contains thousands of pins.
    """

    if not isinstance(run_spec, I3ProductionRunSpec):
        raise I3MigrationIOError("input binding requires a typed production RunSpec")
    if run_spec.run_kind is not I3ProductionRunKind.BASE:
        raise I3MigrationIOError("compact-base input loader accepts BASE only")
    root = data_root.expanduser().resolve()
    legacy = _load_legacy_release_member_pins(root, run_spec)
    s4 = _load_s4_release_member_pins(root, run_spec)
    binding = I3CompactBaseInputBinding(legacy=legacy, s4=s4)
    _require_base_controls(run_spec, legacy=legacy, s4=s4)
    _load_exact_base_frontier(root, run_spec, s4=s4)
    return binding


def load_compact_base_materializer(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
) -> CompactBaseMigrationMaterializer:
    """CLI-friendly exact-manifest constructor for the production BASE seam."""

    return load_compact_base_input_binding(
        data_root=data_root,
        run_spec=run_spec,
    ).materializer()


def verify_compact_base_materialization_attestation(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
) -> CompactBaseMaterializationAttestation:
    """Authenticate the exact official BASE capability for production staging.

    This is the production trust-boundary entrypoint.  It deliberately reloads
    the exact S7/S4 release manifests from the RunSpec rather than accepting an
    input binding reported by the materializer.  The returned attestation is
    useful only if it is the module-minted in-memory object carried by the
    nominal prepared bundle and every logical binding reproduces.
    """

    _require_module_sealed_base_attestation(prepared)
    binding = load_compact_base_input_binding(data_root=data_root, run_spec=run_spec)
    return _verify_compact_base_materialization_attestation_with_binding(
        run_spec=run_spec,
        prepared=prepared,
        binding=binding,
    )


def prepare_compact_base(
    *,
    data_root: Path,
    run_spec: I3ProductionRunSpec,
    workspace: Path,
    legacy: I3LegacyV1BasePins,
    s4: I3S4BasePins,
) -> CompactBasePreparedMaterialization:
    """Create one immutable/no-publish native-v2 BASE from explicit exact pins."""

    started = time.monotonic()
    root = data_root.expanduser().resolve()
    work = workspace.expanduser().resolve()
    _require_base_controls(run_spec, legacy=legacy, s4=s4)
    _require_empty_workspace(root, work)
    estimate = estimate_compact_base_resources(run_spec, legacy=legacy, s4=s4)
    _validate_resource_estimate(run_spec, estimate)
    minimum_disk = _check_preflight_resources(root, run_spec, estimate)

    source_digest = _compact_base_source_digest(run_spec, legacy=legacy, s4=s4)
    paths: dict[I3MigrationParquetPin, Path] = {}
    for pin in _unique_source_pins(legacy, s4):
        paths[pin] = _verify_source_parquet(root, pin)
        minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))
    base_frontier = _load_exact_base_frontier(root, run_spec, s4=s4)

    universe_paths = [str(paths[item]) for item in legacy.pins_for("universe_daily")]
    s4_universe_paths = [str(paths[item]) for item in s4.universe_source_partitions]
    asset_projection_by_id = _asset_aggregate_projections(
        universe_paths,
        legacy_release_set_id=legacy.release_set_id,
        legacy_partition_set_digest=legacy.complete_partition_pin_digest,
    )
    issuer_projection_by_id = _issuer_aggregate_projections(
        universe_paths,
        s4_universe_paths,
        legacy_release_set_id=legacy.release_set_id,
        legacy_partition_set_digest=legacy.complete_partition_pin_digest,
    )
    unresolved_subjects = _unresolved_tail_states(
        universe_paths,
        legacy_release_set_id=legacy.release_set_id,
        legacy_partition_set_digest=legacy.complete_partition_pin_digest,
        state_available_session=run_spec.run_available_session,
    )
    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    small_outputs: dict[str, I3ProductionTableOutput] = {}
    small_rows: dict[str, tuple[dict[str, object], ...]] = {}
    aliases_by_legacy_id: dict[str, MigratedAliasRoot] = {}
    asset_version_by_id: dict[str, str] = {}
    issuer_version_by_id: dict[str, str] = {}
    asset_states = []
    issuer_states = []

    alias_legacy = _read_compact_table(legacy.pins_for("ticker_alias"), paths)
    _validate_small_table_lineage(alias_legacy, run_spec)
    alias_rows: list[dict[str, object]] = []
    for row in alias_legacy.to_pylist():
        legacy_alias_id = str(row["ticker_alias_id"])
        seed = migration_source_seed_digest(
            table_name="ticker_alias",
            stable_row_key=legacy_alias_id,
            legacy_row=row,
            legacy_release_set_id=legacy.release_set_id,
            legacy_partition_set_digest=legacy.complete_partition_pin_digest,
        )
        migrated = migrate_alias_root(
            row,
            policy=run_spec.identity_policy_bundle,
            migration_available_session=run_spec.run_available_session,
            source_record_seed_digest=seed,
        )
        if legacy_alias_id in aliases_by_legacy_id:
            raise I3MigrationIOError("legacy ticker-alias ID is duplicated")
        aliases_by_legacy_id[legacy_alias_id] = migrated
        alias_rows.append(dict(migrated.row))
    small_rows["ticker_alias"] = tuple(alias_rows)

    asset_legacy = _read_compact_table(legacy.pins_for("asset_master"), paths)
    _validate_small_table_lineage(asset_legacy, run_spec)
    asset_rows: list[dict[str, object]] = []
    for row in asset_legacy.to_pylist():
        asset_id = str(row["asset_id"])
        projection = asset_projection_by_id.get(asset_id)
        if projection is None:
            raise I3MigrationIOError("legacy asset is absent from full universe aggregates")
        state = build_asset_aggregate_state(
            row,
            projection,
            migration_available_session=run_spec.run_available_session,
        )
        migrated_row, state = materialize_asset_root(
            row,
            state,
            available_session=run_spec.run_available_session,
        )
        asset_rows.append(migrated_row)
        asset_states.append(state)
        asset_version_by_id[state.asset_id] = state.terminal_row_version_id
    if set(asset_projection_by_id) != set(asset_version_by_id):
        raise I3MigrationIOError("universe aggregate contains an asset absent from v1 master")
    small_rows["asset_master"] = tuple(asset_rows)

    issuer_legacy = _read_compact_table(legacy.pins_for("issuer_master"), paths)
    _validate_small_table_lineage(issuer_legacy, run_spec)
    issuer_rows: list[dict[str, object]] = []
    for row in issuer_legacy.to_pylist():
        issuer_id = str(row["issuer_id"])
        projection = issuer_projection_by_id.get(issuer_id)
        if projection is None:
            raise I3MigrationIOError("legacy issuer is absent from full universe aggregates")
        # S4 v1 has no SIC column.  The v1 selected value authenticates only a
        # zero/singleton set; any multi-value count fails closed below.
        projection = _with_exact_legacy_sic_projection(row, projection)
        state = build_issuer_aggregate_state(
            row,
            projection,
            migration_available_session=run_spec.run_available_session,
        )
        migrated_row, state = materialize_issuer_root(
            row,
            state,
            available_session=run_spec.run_available_session,
        )
        issuer_rows.append(migrated_row)
        issuer_states.append(state)
        issuer_version_by_id[state.issuer_id] = state.terminal_row_version_id
    if set(issuer_projection_by_id) != set(issuer_version_by_id):
        raise I3MigrationIOError("universe aggregate contains an issuer absent from v1 master")
    small_rows["issuer_master"] = tuple(issuer_rows)

    for table_name in _SMALL_TABLES:
        v2_table = pa.Table.from_pylist(
            list(small_rows[table_name]),
            schema=I3_V2_CONTRACTS[table_name].arrow_schema,
        )
        relative = _workspace_relative(
            root,
            work / table_name / "base.parquet",
        )
        artifact = write_i3_migration_parquet_no_clobber(
            data_root=root,
            relative_path=relative,
            table=v2_table,
            run_spec=run_spec,
        )
        readback = readback_i3_migration_parquet_exact(
            data_root=root,
            artifact=artifact,
            table_name=table_name,
            row_count=v2_table.num_rows,
        )
        legacy_table = {
            "asset_master": asset_legacy,
            "ticker_alias": alias_legacy,
            "issuer_master": issuer_legacy,
        }[table_name]
        _assert_exact_legacy_projection(
            table_name,
            readback,
            legacy_table,
            aliases_by_legacy_id=aliases_by_legacy_id,
        )
        contract = I3_V2_CONTRACTS[table_name]
        small_outputs[table_name] = I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.PARQUET,
            manifest_output=NativeV2OutputArtifact(
                table_name=table_name,
                session_date=run_spec.terminal_session,
                row_count=readback.num_rows,
                contract_id=contract.contract_id,
                schema_digest=contract.schema_digest,
                artifact=artifact,
            ),
        )
        minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    alias_map = _alias_mapping_frame(aliases_by_legacy_id)
    alias_reverse_map = alias_map.select("alias_segment_id", "ticker_alias_id")
    asset_map = pl.DataFrame(
        {
            "asset_id": list(asset_version_by_id),
            "asset_master_version_id": list(asset_version_by_id.values()),
        },
        schema={"asset_id": pl.String, "asset_master_version_id": pl.String},
    )
    issuer_map = pl.DataFrame(
        {
            "issuer_id": list(issuer_version_by_id),
            "issuer_master_version_id": list(issuer_version_by_id.values()),
        },
        schema={"issuer_id": pl.String, "issuer_master_version_id": pl.String},
    )
    universe_partition_pins: list[I3ProductionPartitionPin] = []
    resolved_partition_map: list[ResolvedPartitionState] = []
    for source_pin in legacy.pins_for("universe_daily"):
        assert source_pin.session_date is not None
        legacy_table = (
            pq.ParquetFile(paths[source_pin])
            .read()
            .cast(
                S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema,
                safe=True,
            )
        )
        _validate_universe_lineage(legacy_table, run_spec)
        migrated = _migrate_universe_partition(
            legacy_table,
            alias_map=alias_map,
            asset_map=asset_map,
            issuer_map=issuer_map,
            identity_policy_bundle_id=(run_spec.identity_policy_bundle.identity_policy_bundle_id),
            row_available_session=run_spec.run_available_session,
        )
        relative = _workspace_relative(
            root,
            work
            / "universe_daily"
            / f"session_date={source_pin.session_date.isoformat()}"
            / "part-000.parquet",
        )
        artifact = write_i3_migration_parquet_no_clobber(
            data_root=root,
            relative_path=relative,
            table=migrated,
            run_spec=run_spec,
        )
        readback = readback_i3_migration_parquet_exact(
            data_root=root,
            artifact=artifact,
            table_name="universe_daily",
            row_count=source_pin.row_count,
            session_date=source_pin.session_date,
        )
        _assert_exact_legacy_projection(
            "universe_daily",
            readback,
            legacy_table,
            aliases_by_legacy_id=aliases_by_legacy_id,
            alias_reverse_map=alias_reverse_map,
        )
        receipt_id = stable_digest(
            {
                "artifact": artifact.to_dict(),
                "legacy_partition": source_pin.to_dict(),
                "legacy_release_set_id": legacy.release_set_id,
                "rule_version": COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION,
                "source_digest": source_digest,
            }
        )
        contract = I3_V2_CONTRACTS["universe_daily"]
        partition = I3ProductionPartitionPin(
            session_date=source_pin.session_date,
            partition_receipt_id=receipt_id,
            artifact=artifact,
            row_count=readback.num_rows,
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            availability_session=run_spec.run_available_session,
        )
        universe_partition_pins.append(partition)
        resolved_partition_map.append(
            ResolvedPartitionState(
                session_date=partition.session_date,
                partition_receipt_id=partition.partition_receipt_id,
                artifact=partition.artifact,
                row_count=partition.row_count,
                availability_session=partition.availability_session,
            )
        )
        minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))

    dataset_index = I3ProductionDatasetIndex(
        table_name="universe_daily",
        terminal_session=run_spec.terminal_session,
        partitions=tuple(universe_partition_pins),
    )
    dataset_index_relative = _workspace_relative(
        root,
        work / "universe_daily" / "index.json",
    )
    dataset_index_artifact = _write_bytes_no_clobber(
        root,
        dataset_index_relative,
        dataset_index.canonical_bytes(),
        run_spec=run_spec,
    )
    if dataset_index_artifact != dataset_index.exact_pin(path=dataset_index_relative):
        raise I3MigrationIOError("universe dataset-index bytes changed during write")
    universe_contract = I3_V2_CONTRACTS["universe_daily"]
    universe_output = I3ProductionTableOutput(
        storage=I3ProductionOutputStorage.DATASET_INDEX,
        manifest_output=NativeV2OutputArtifact(
            table_name="universe_daily",
            session_date=run_spec.terminal_session,
            row_count=dataset_index.row_count,
            contract_id=universe_contract.contract_id,
            schema_digest=universe_contract.schema_digest,
            artifact=dataset_index_artifact,
        ),
        dataset_index=dataset_index,
    )

    table_outputs = tuple(
        universe_output if table == "universe_daily" else small_outputs[table]
        for table in I3_V2_TABLE_ORDER
    )
    terminal_rows, prepared_row_versions = _terminal_and_prepared_row_versions(
        small_rows,
        small_outputs,
        availability_session=run_spec.run_available_session,
    )
    open_aliases = tuple(
        sorted(
            (
                item.state
                for item in aliases_by_legacy_id.values()
                if item.state.resolution.valid_through_session == run_spec.terminal_session
            ),
            key=lambda item: item.segment.alias_segment_id,
        )
    )
    s4_terminal_pins = _checkpoint_s4_terminal_pins(
        s4,
        base_frontier=base_frontier,
    )
    asset_state_tuple = tuple(sorted(asset_states, key=lambda item: item.asset_id))
    issuer_state_tuple = tuple(sorted(issuer_states, key=lambda item: item.issuer_id))
    unresolved_tuple = tuple(
        sorted(unresolved_subjects, key=lambda item: item.unresolved_subject_id)
    )
    resolved_tuple = tuple(resolved_partition_map)
    terminal_tuple = tuple(sorted(terminal_rows, key=lambda item: item.map_key))
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
        open_aliases=open_aliases,
        asset_aggregates=asset_state_tuple,
        issuer_aggregates=issuer_state_tuple,
        unresolved_subjects=unresolved_tuple,
        resolved_partition_map=resolved_tuple,
        terminal_row_versions=terminal_tuple,
    )
    native_manifest = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=run_spec.terminal_session,
        release_available_session=run_spec.run_available_session,
        native_v2_migration_id=run_spec.native_v2_migration_id,
        identity_policy_bundle_id=(run_spec.identity_policy_bundle.identity_policy_bundle_id),
        transform_semantics_digest=run_spec.transform_semantics_digest,
        resolved_state_digest=resolved_state_digest,
        output_artifacts=tuple(item.manifest_output for item in table_outputs),
        parent_release_id=None,
        source_checkpoint_id=None,
        legacy_oracle_release_set_id=legacy.release_set_id,
    )
    manifest_relative = _workspace_relative(root, work / "native-v2-release.json")
    native_manifest_artifact = _write_bytes_no_clobber(
        root,
        manifest_relative,
        native_manifest.canonical_bytes(),
        run_spec=run_spec,
    )
    if native_manifest_artifact != native_manifest.exact_pin(path=manifest_relative):
        raise I3MigrationIOError("native-v2 manifest bytes changed during write")
    parent_release = NativeV2ParentReleasePin.from_manifest(
        native_manifest,
        path=manifest_relative,
    )
    checkpoint = I3CheckpointState(
        parent_release=parent_release,
        last_session=run_spec.terminal_session,
        source_cutoff_session=run_spec.source_cutoff_session,
        availability_cutoff_session=run_spec.run_available_session,
        s4_terminal_pins=s4_terminal_pins,
        calendar_digest=run_spec.calendar.calendar_artifact_id,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=run_spec.transform_semantics_digest,
        identity_policy_bundle=run_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=run_spec.identity_policy_bundle_artifact,
        open_aliases=open_aliases,
        asset_aggregates=asset_state_tuple,
        issuer_aggregates=issuer_state_tuple,
        unresolved_subjects=unresolved_tuple,
        resolved_partition_map=resolved_tuple,
        terminal_row_versions=terminal_tuple,
    )
    checkpoint_relative = _workspace_relative(root, work / "checkpoint.json")
    checkpoint_artifact = _write_bytes_no_clobber(
        root,
        checkpoint_relative,
        checkpoint.canonical_bytes(),
        run_spec=run_spec,
    )
    if checkpoint_artifact != checkpoint.exact_pin(path=checkpoint_relative):
        raise I3MigrationIOError("checkpoint bytes changed during write")

    minimum_disk = min(minimum_disk, _check_live_resources(root, run_spec))
    output_bytes = _workspace_file_bytes(work)
    output_rows = sum(item.manifest_output.row_count for item in table_outputs)
    if output_bytes > run_spec.resource_caps.output_bytes_hard_cap:
        raise I3MigrationIOError("compact base output bytes exceed the hard cap")
    if output_rows > run_spec.resource_caps.output_rows_hard_cap:
        raise I3MigrationIOError("compact base output rows exceed the hard cap")
    observation = I3ProductionResourceObservation(
        peak_rss_bytes=_peak_rss_bytes(),
        elapsed_seconds=max(0, math.ceil(time.monotonic() - started)),
        minimum_disk_free_bytes=minimum_disk,
        temporary_bytes=0,
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
        canonical_projection_difference_count=0,
        row_versions=prepared_row_versions,
    )
    binding = I3CompactBaseInputBinding(legacy=legacy, s4=s4)
    attestation = _mint_compact_base_materialization_attestation(
        run_spec=run_spec,
        prepared=unsealed,
        binding=binding,
    )
    return CompactBasePreparedMaterialization(
        table_outputs=unsealed.table_outputs,
        native_manifest=unsealed.native_manifest,
        native_manifest_artifact=unsealed.native_manifest_artifact,
        checkpoint=unsealed.checkpoint,
        checkpoint_artifact=unsealed.checkpoint_artifact,
        source_digest=unsealed.source_digest,
        resource_observation=unsealed.resource_observation,
        canonical_projection_difference_count=(unsealed.canonical_projection_difference_count),
        row_versions=unsealed.row_versions,
        base_materialization_attestation=attestation,
    )


def estimate_compact_base_resources(
    run_spec: I3ProductionRunSpec,
    *,
    legacy: I3LegacyV1BasePins,
    s4: I3S4BasePins,
) -> I3CompactBaseResourceEstimate:
    """Return a stable, conservative full-base estimate before opening Parquet."""

    source_pins = _unique_source_pins(legacy, s4)
    source_bytes = sum(item.artifact.bytes for item in source_pins)
    rewritten_legacy_bytes = sum(item.artifact.bytes for item in legacy.member_outputs)
    small_bytes = sum(
        item.artifact.bytes for item in legacy.member_outputs if item.table_name in _SMALL_TABLES
    )
    largest_partition = max(item.artifact.bytes for item in source_pins)
    estimated_peak = max(
        _ESTIMATED_BASELINE_RSS,
        8 * small_bytes + 6 * largest_partition,
    )
    estimated_output = (
        rewritten_legacy_bytes
        * _ESTIMATED_OUTPUT_MULTIPLIER_NUMERATOR
        // _ESTIMATED_OUTPUT_MULTIPLIER_DENOMINATOR
    ) + 64 * 1024**2
    estimated_temporary = max(8 * largest_partition, 512 * 1024**2)
    return I3CompactBaseResourceEstimate(
        estimated_peak_rss_bytes=estimated_peak,
        estimated_output_bytes=estimated_output,
        estimated_temporary_bytes=estimated_temporary,
        minimum_free_disk_bytes_required=(
            max(
                _MINIMUM_PRODUCTION_DISK_FLOOR,
                run_spec.resource_caps.disk_free_bytes_hard_floor,
            )
            + estimated_output
            + estimated_temporary
        ),
        source_bytes=source_bytes,
    )


def _load_legacy_release_member_pins(
    root: Path,
    run_spec: I3ProductionRunSpec,
) -> I3LegacyV1BasePins:
    release_artifact = run_spec.i0_oracle.artifact
    marker = _strict_canonical_json(
        _read_exact_artifact(root, release_artifact), trailing_newline=False
    )
    if set(marker) != _S7_RELEASE_SET_FIELDS:
        raise I3MigrationIOError("S7 published release-set fields differ")
    payload = dict(marker)
    claimed_release_id = payload.pop("release_set_id", None)
    if (
        claimed_release_id != run_spec.i0_oracle.object_id
        or stable_digest(payload) != claimed_release_id
        or marker["artifact_type"] != "s7_four_table_atomic_release_set"
        or marker["state"] != "published"
        or marker["table_order"] != list(_V1_TABLE_ORDER)
        or marker["visibility_rule"] != "all_four_members_visible_only_through_this_exact_marker_v1"
    ):
        raise I3MigrationIOError("S7 published release-set identity differs")
    release_availability = _release_available_session(
        marker["release_availability"], label="S7 release availability"
    )
    if release_availability != run_spec.i0_oracle.available_session:
        raise I3MigrationIOError("S7 release availability differs from RunSpec")
    descriptors = _object_array(marker["members"], "S7 release-set members")
    if tuple(item.get("table_name") for item in descriptors) != _V1_TABLE_ORDER:
        raise I3MigrationIOError("S7 release-set member order differs")

    result: list[I3MigrationParquetPin] = []
    for descriptor in descriptors:
        if set(descriptor) != {
            "bytes",
            "path",
            "release_id",
            "sha256",
            "table_name",
        }:
            raise I3MigrationIOError("S7 member descriptor fields differ")
        table_name = str(descriptor["table_name"])
        descriptor_pin = ArtifactPin(
            path=str(descriptor["path"]),
            sha256=str(descriptor["sha256"]),
            bytes=_positive_int(descriptor["bytes"], "S7 member manifest bytes"),
        )
        member_content = _read_exact_artifact(root, descriptor_pin)
        member = _strict_canonical_json(member_content, trailing_newline=False)
        if set(member) != _S7_MEMBER_FIELDS:
            raise I3MigrationIOError("S7 member release fields differ")
        member_payload = dict(member)
        member_release_id = member_payload.pop("release_id", None)
        if (
            member_release_id != descriptor["release_id"]
            or stable_digest(member_payload) != member_release_id
            or member["table_name"] != table_name
            or member["artifact_type"] != "s7_four_table_hidden_member_release"
            or member["state"] != "published_hidden_until_release_set"
            or member["release_availability"] != marker["release_availability"]
        ):
            raise I3MigrationIOError("S7 member release identity differs")
        contract = _closed_object(
            member["contract"],
            {"contract_id", "resource_sha256", "schema_digest"},
            "S7 member contract",
        )
        expected_contract = S7_DERIVED_CONTRACTS[table_name]
        if (
            contract["contract_id"] != expected_contract.contract_id
            or contract["schema_digest"] != expected_contract.schema_digest
        ):
            raise I3MigrationIOError("S7 member release contract differs")
        receipts = _object_array(member["output_receipts"], "S7 output receipts")
        if stable_digest(receipts) != member["output_set_digest"]:
            raise I3MigrationIOError("S7 member output-set digest differs")
        if table_name != "universe_daily" and len(receipts) != 1:
            raise I3MigrationIOError("S7 compact member must have one output")
        table_pins = tuple(
            _s7_output_pin(
                receipt,
                table_name=table_name,
                availability_session=release_availability,
            )
            for receipt in receipts
        )
        if sum(item.row_count for item in table_pins) != member["row_count"]:
            raise I3MigrationIOError("S7 member row count differs from its outputs")
        result.extend(table_pins)
    return I3LegacyV1BasePins(
        release_set_id=run_spec.i0_oracle.object_id,
        release_set_artifact=release_artifact,
        member_outputs=tuple(result),
    )


def _s7_output_pin(
    receipt: Mapping[str, object],
    *,
    table_name: str,
    availability_session: date,
) -> I3MigrationParquetPin:
    if set(receipt) != {"bytes", "path", "row_count", "schema_digest", "sha256"}:
        raise I3MigrationIOError("S7 output receipt fields differ")
    contract = S7_DERIVED_CONTRACTS[table_name]
    if receipt["schema_digest"] != contract.schema_digest:
        raise I3MigrationIOError("S7 output receipt schema differs")
    path = str(receipt["path"])
    session_date = _partition_session(path) if table_name == "universe_daily" else None
    if table_name != "universe_daily":
        parts = PurePosixPath(path).parts
        if len(parts) < 2 or parts[-2:] != ("data", f"{table_name}.parquet"):
            raise I3MigrationIOError("S7 compact output path is not canonical")
    return I3MigrationParquetPin(
        table_name=table_name,
        artifact=ArtifactPin(
            path=path,
            sha256=str(receipt["sha256"]),
            bytes=_positive_int(receipt["bytes"], "S7 output bytes"),
        ),
        row_count=_nonnegative_int(receipt["row_count"], "S7 output row count"),
        contract_id=contract.contract_id,
        schema_digest=contract.schema_digest,
        availability_session=availability_session,
        session_date=session_date,
    )


def _load_s4_release_member_pins(
    root: Path,
    run_spec: I3ProductionRunSpec,
) -> I3S4BasePins:
    release_pin = run_spec.s4_v1_source.artifact
    try:
        release_set, document = load_exact_asset_release_set_control(
            root,
            release_set_id=run_spec.s4_v1_source.object_id,
            expected_sha256=release_pin.sha256,
            expected_bytes=release_pin.bytes,
        )
    except Exception as exc:
        raise I3MigrationIOError("cannot authenticate exact S4 release set") from exc
    if (
        not isinstance(release_set, AssetReleaseSet)
        or release_set.release_set_id != run_spec.s4_v1_source.object_id
        or ArtifactPin(
            path=document.path,
            sha256=document.sha256,
            bytes=document.bytes,
        )
        != release_pin
    ):
        raise I3MigrationIOError("S4 release-set pin differs from RunSpec")
    members = {item.table: item for item in release_set.members}
    if set(members) != set(_S4_TABLES):
        raise I3MigrationIOError("S4 release set does not contain exactly three tables")

    by_table: dict[str, tuple[I3MigrationParquetPin, ...]] = {}
    for table_name in _S4_TABLES:
        member = members[table_name]
        contract = ASSET_CONTRACTS[table_name]
        if member.contract_id != contract.contract_id:
            raise I3MigrationIOError("S4 release member contract differs")
        pins = []
        for output in member.outputs:
            if (
                output.table != table_name
                or output.row_count is None
                or output.schema_digest != contract.schema_digest
            ):
                raise I3MigrationIOError("S4 release member output differs")
            pins.append(
                I3MigrationParquetPin(
                    table_name=table_name,
                    artifact=ArtifactPin(
                        path=output.path,
                        sha256=output.sha256,
                        bytes=output.bytes,
                    ),
                    row_count=output.row_count,
                    contract_id=contract.contract_id,
                    schema_digest=contract.schema_digest,
                    availability_session=run_spec.s4_v1_source.available_session,
                    session_date=_partition_session(output.path),
                )
            )
        ordered = tuple(pins)
        sessions = tuple(item.session_date for item in ordered)
        if sessions != tuple(sorted(set(sessions))):
            raise I3MigrationIOError("S4 release output sessions repeat or are unordered")
        by_table[table_name] = ordered
    session_sets = {tuple(item.session_date for item in pins) for pins in by_table.values()}
    if len(session_sets) != 1:
        raise I3MigrationIOError("S4 release member session ranges differ")
    sessions = next(iter(session_sets))
    if not sessions or sessions[-1] != run_spec.terminal_session:
        raise I3MigrationIOError("S4 release output range differs from BASE terminal")
    return I3S4BasePins(
        release_set_id=release_set.release_set_id,
        release_set_artifact=release_pin,
        universe_source_partitions=by_table["universe_source_daily"],
        terminal_partitions=tuple(by_table[table][-1] for table in _S4_TABLES),
    )


def _require_base_controls(
    run_spec: I3ProductionRunSpec,
    *,
    legacy: I3LegacyV1BasePins,
    s4: I3S4BasePins,
) -> None:
    if not isinstance(run_spec, I3ProductionRunSpec):
        raise I3MigrationIOError("compact base requires a typed production RunSpec")
    if run_spec.run_kind is not I3ProductionRunKind.BASE:
        raise I3MigrationIOError("compact migration IO implements production BASE only")
    if run_spec.parent_release is not None:
        raise I3MigrationIOError("compact base cannot carry a native-v2 parent")
    if run_spec.i2_base_frontier is None or run_spec.i2_receipts:
        raise I3MigrationIOError("compact base requires only the exact I2 base frontier")
    if (
        legacy.release_set_id != run_spec.i0_oracle.object_id
        or legacy.release_set_artifact != run_spec.i0_oracle.artifact
    ):
        raise I3MigrationIOError("legacy base pins differ from the RunSpec I0 oracle")
    if (
        s4.release_set_id != run_spec.s4_v1_source.object_id
        or s4.release_set_artifact != run_spec.s4_v1_source.artifact
    ):
        raise I3MigrationIOError("S4 base pins differ from the RunSpec dependency")
    legacy_sessions = tuple(item.session_date for item in legacy.pins_for("universe_daily"))
    s4_sessions = tuple(item.session_date for item in s4.universe_source_partitions)
    if legacy_sessions != s4_sessions:
        raise I3MigrationIOError("S7 v1 and S4 universe histories have different sessions")
    if legacy_sessions[-1] != run_spec.terminal_session:
        raise I3MigrationIOError(
            "compact BASE must terminate at the exact v1 terminal session; "
            "later I2 sessions belong to DELTA"
        )
    if s4.terminal_session != run_spec.terminal_session:
        raise I3MigrationIOError("S4 terminal pins differ from the base RunSpec session")
    if run_spec.resource_caps.disk_free_bytes_hard_floor < _MINIMUM_PRODUCTION_DISK_FLOOR:
        raise I3MigrationIOError("production compact base requires at least a 40 GiB disk floor")


def _load_exact_base_frontier(
    root: Path,
    run_spec: I3ProductionRunSpec,
    *,
    s4: I3S4BasePins,
) -> S4BaseFrontier:
    pin = run_spec.i2_base_frontier
    if pin is None:  # pragma: no cover - guarded before IO
        raise I3MigrationIOError("I2 base-frontier pin is absent")
    content = _read_exact_artifact(root, pin.artifact)
    document = _strict_canonical_json(content, trailing_newline=True)
    try:
        frontier = S4BaseFrontier.from_dict(document)
    except Exception as exc:
        raise I3MigrationIOError("I2 S4BaseFrontier bytes are invalid") from exc
    terminal_digest = stable_digest(
        {
            "base_release_set_id": s4.release_set_id,
            "partitions": [
                {
                    "artifact": item.artifact.to_dict(),
                    "contract_id": ASSET_CONTRACTS[item.table_name].contract_id,
                    "table": item.table_name,
                }
                for item in s4.terminal_partitions
            ],
            "rule_version": S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
            "terminal_session": s4.terminal_session.isoformat(),
        }
    )
    expected_contracts = {table: ASSET_CONTRACTS[table].contract_id for table in _S4_TABLES}
    expected_schemas = {table: ASSET_CONTRACTS[table].schema_digest for table in _S4_TABLES}
    if (
        frontier.frontier_id != pin.frontier_id
        or frontier.terminal_session != pin.terminal_session
        or frontier.release_available_session != pin.frontier_available_session
        or frontier.base_release_set_id != s4.release_set_id
        or frontier.base_release_set_artifact != s4.release_set_artifact
        or frontier.calendar_artifact_id != run_spec.calendar.calendar_artifact_id
        or dict(frontier.contract_ids_by_table) != expected_contracts
        or dict(frontier.schema_digests_by_table) != expected_schemas
        or frontier.terminal_partition_set_digest != terminal_digest
    ):
        raise I3MigrationIOError("I2 base frontier differs from explicit S4 member pins")
    return frontier


def _asset_aggregate_projections(
    universe_paths: Sequence[str],
    *,
    legacy_release_set_id: str,
    legacy_partition_set_digest: str,
) -> dict[str, LegacyAssetAggregateProjection]:
    scan = _scan_explicit_parquet(universe_paths)
    eligible = scan.filter(pl.col("asset_id").is_not_null())
    scalar = (
        eligible.group_by("asset_id")
        .agg(
            pl.col("ticker").drop_nulls().unique().sort().alias("observed_tickers"),
            pl.col("observed_composite_figi")
            .drop_nulls()
            .unique()
            .sort()
            .alias("observed_composite_figis"),
            pl.col("observed_share_class_figi")
            .drop_nulls()
            .unique()
            .sort()
            .alias("observed_share_class_figis"),
            pl.col("issuer_id").drop_nulls().unique().sort().alias("observed_issuer_ids"),
            pl.col("canonical_share_class_figi")
            .drop_nulls()
            .unique()
            .sort()
            .alias("canonical_share_class_figis"),
            pl.col("identity_adjudication_id")
            .drop_nulls()
            .unique()
            .sort()
            .alias("identity_adjudication_ids"),
            pl.col("identity_adjudication_id")
            .filter(pl.col("identity_disposition") == "confirmed_genuine_transition")
            .drop_nulls()
            .unique()
            .sort()
            .alias("genuine_transition_identity_adjudication_ids"),
            pl.col("identity_adjudication_id")
            .filter(pl.col("identity_disposition") == "confirmed_provider_contamination")
            .drop_nulls()
            .unique()
            .sort()
            .alias("provider_contamination_identity_adjudication_ids"),
            pl.col("cross_market_adjudication_id")
            .drop_nulls()
            .unique()
            .sort()
            .alias("cross_market_adjudication_ids"),
            pl.col("provider_composite_override_id")
            .drop_nulls()
            .unique()
            .sort()
            .alias("provider_composite_override_ids"),
            pl.col("share_class_adjudication_id")
            .drop_nulls()
            .unique()
            .sort()
            .alias("share_class_adjudication_ids"),
        )
        .sort("asset_id")
    )
    transitions = (
        eligible.select("asset_id", "asset_transition_ids")
        .explode("asset_transition_ids", empty_as_null=True)
        .filter(pl.col("asset_transition_ids").is_not_null())
        .group_by("asset_id")
        .agg(pl.col("asset_transition_ids").unique().sort().alias("asset_transition_ids"))
    )
    frame = scalar.join(transitions, on="asset_id", how="left").collect(engine="streaming")
    result: dict[str, LegacyAssetAggregateProjection] = {}
    for row in frame.iter_rows(named=True):
        asset_id = str(row["asset_id"])
        seed = stable_digest(
            {
                "legacy_partition_set_digest": legacy_partition_set_digest,
                "legacy_release_set_id": legacy_release_set_id,
                "rule_version": "s7_5_i3_asset_aggregate_seed_from_base_pins_v1",
                "stable_row_key": asset_id,
            }
        )
        result[asset_id] = LegacyAssetAggregateProjection(
            observed_tickers=_strings(row["observed_tickers"]),
            observed_composite_figis=_strings(row["observed_composite_figis"]),
            observed_share_class_figis=_strings(row["observed_share_class_figis"]),
            observed_issuer_ids=_strings(row["observed_issuer_ids"]),
            canonical_share_class_figis=_strings(row["canonical_share_class_figis"]),
            identity_adjudication_ids=_strings(row["identity_adjudication_ids"]),
            genuine_transition_identity_adjudication_ids=_strings(
                row["genuine_transition_identity_adjudication_ids"]
            ),
            provider_contamination_identity_adjudication_ids=_strings(
                row["provider_contamination_identity_adjudication_ids"]
            ),
            cross_market_adjudication_ids=_strings(row["cross_market_adjudication_ids"]),
            provider_composite_override_ids=_strings(row["provider_composite_override_ids"]),
            share_class_adjudication_ids=_strings(row["share_class_adjudication_ids"]),
            asset_transition_ids=_strings(row["asset_transition_ids"]),
            source_record_seed_digest=seed,
        )
    return result


def _issuer_aggregate_projections(
    universe_paths: Sequence[str],
    s4_universe_paths: Sequence[str],
    *,
    legacy_release_set_id: str,
    legacy_partition_set_digest: str,
) -> dict[str, LegacyIssuerAggregateProjection]:
    universe = _scan_explicit_parquet(universe_paths)
    trusted = universe.filter(
        pl.col("issuer_id").is_not_null()
        & pl.col("backtest_identity_eligible")
        & (pl.col("identity_disposition") != "confirmed_provider_contamination")
        & pl.col("cross_market_adjudication_id").is_null()
    )
    issuer_sets = trusted.group_by("issuer_id", "canonical_cik_normalized").agg(
        pl.col("asset_id").drop_nulls().unique().sort().alias("observed_asset_ids"),
        pl.col("ticker").drop_nulls().unique().sort().alias("observed_tickers"),
    )
    s4 = _scan_explicit_parquet(s4_universe_paths)
    names = (
        s4.filter(
            pl.col("cik").is_not_null()
            & pl.col("cik").str.contains(r"^[0-9]{1,10}$")
            & pl.col("name").is_not_null()
            & (pl.col("name").str.strip_chars() != "")
        )
        .with_columns(
            pl.col("cik").str.pad_start(10, "0").alias("canonical_cik_normalized"),
            pl.col("name").str.strip_chars().alias("reference_name"),
        )
        .group_by("canonical_cik_normalized")
        .agg(pl.col("reference_name").drop_nulls().unique().sort().alias("reference_names"))
    )
    frame = (
        issuer_sets.join(names, on="canonical_cik_normalized", how="left")
        .sort("issuer_id")
        .collect(engine="streaming")
    )
    result: dict[str, LegacyIssuerAggregateProjection] = {}
    for row in frame.iter_rows(named=True):
        issuer_id = str(row["issuer_id"])
        seed = stable_digest(
            {
                "legacy_partition_set_digest": legacy_partition_set_digest,
                "legacy_release_set_id": legacy_release_set_id,
                "rule_version": "s7_5_i3_issuer_aggregate_seed_from_base_pins_v1",
                "stable_row_key": issuer_id,
            }
        )
        result[issuer_id] = LegacyIssuerAggregateProjection(
            observed_asset_ids=_strings(row["observed_asset_ids"]),
            observed_tickers=_strings(row["observed_tickers"]),
            reference_names=_strings(row["reference_names"]),
            sic_codes=(),
            source_record_seed_digest=seed,
        )
    return result


def _unresolved_tail_states(
    universe_paths: Sequence[str],
    *,
    legacy_release_set_id: str,
    legacy_partition_set_digest: str,
    state_available_session: date,
) -> tuple[UnresolvedSubjectState, ...]:
    scan = _scan_explicit_parquet(universe_paths).select(
        "ticker",
        "session_date",
        "backtest_identity_eligible",
        "composite_registry_collision",
        "identity_disposition",
    )
    unresolved = (~pl.col("backtest_identity_eligible")) | pl.col("composite_registry_collision")
    last_resolved = (
        scan.filter(~unresolved)
        .group_by("ticker")
        .agg(pl.col("session_date").max().alias("last_resolved_session"))
    )
    tails = (
        scan.filter(unresolved)
        .join(last_resolved, on="ticker", how="left")
        .filter(
            pl.col("last_resolved_session").is_null()
            | (pl.col("session_date") > pl.col("last_resolved_session"))
        )
        .with_columns(
            pl.when(pl.col("composite_registry_collision"))
            .then(pl.lit("registry_collision"))
            .otherwise(pl.col("identity_disposition"))
            .alias("raw_reason")
        )
        .group_by("ticker")
        .agg(
            pl.col("session_date").min().alias("first_observed_session"),
            pl.col("session_date").max().alias("last_observed_session"),
            pl.col("raw_reason").drop_nulls().unique().sort().alias("raw_reasons"),
        )
        .sort("ticker")
        .collect(engine="streaming")
    )
    states = []
    for row in tails.iter_rows(named=True):
        ticker = str(row["ticker"])
        first = row["first_observed_session"]
        last = row["last_observed_session"]
        if type(first) is not date or type(last) is not date:
            raise I3MigrationIOError("unresolved tail emitted a non-date boundary")
        reasons = tuple(sorted({_reason_code(str(value)) for value in row["raw_reasons"]}))
        seed = stable_digest(
            {
                "first_observed_session": first.isoformat(),
                "last_observed_session": last.isoformat(),
                "legacy_partition_set_digest": legacy_partition_set_digest,
                "legacy_release_set_id": legacy_release_set_id,
                "reason_codes": list(reasons),
                "rule_version": COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION,
                "subject_key": ticker,
                "subject_kind": "ticker_identity",
            }
        )
        states.append(
            UnresolvedSubjectState(
                subject_kind="ticker_identity",
                subject_key=ticker,
                first_observed_session=first,
                last_observed_session=last,
                reason_codes=reasons,
                source_record_set_digest=seed,
                state_available_session=state_available_session,
            )
        )
    return tuple(states)


def _migrate_universe_partition(
    legacy: pa.Table,
    *,
    alias_map: pl.DataFrame,
    asset_map: pl.DataFrame,
    issuer_map: pl.DataFrame,
    identity_policy_bundle_id: str,
    row_available_session: date,
) -> pa.Table:
    frame = pl.from_arrow(legacy).lazy().with_row_index("__row_index")
    joined = (
        frame.join(
            alias_map.lazy(),
            on="ticker_alias_id",
            how="left",
            maintain_order="left",
        )
        .join(
            asset_map.lazy(),
            on="asset_id",
            how="left",
            maintain_order="left",
        )
        .join(
            issuer_map.lazy(),
            on="issuer_id",
            how="left",
            maintain_order="left",
        )
    )
    failures = (
        joined.select(
            (pl.col("backtest_identity_eligible") & pl.col("alias_segment_id").is_null())
            .sum()
            .alias("eligible_missing_alias"),
            ((~pl.col("backtest_identity_eligible")) & pl.col("ticker_alias_id").is_not_null())
            .sum()
            .alias("ineligible_with_alias"),
            (pl.col("backtest_identity_eligible") & pl.col("asset_master_version_id").is_null())
            .sum()
            .alias("eligible_missing_asset"),
            (
                pl.col("backtest_identity_eligible")
                & pl.col("issuer_id").is_not_null()
                & pl.col("issuer_master_version_id").is_null()
            )
            .sum()
            .alias("eligible_missing_issuer"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    if any(int(value or 0) for value in failures.values()):
        raise I3MigrationIOError(f"universe v2 FK migration failed closed: {failures}")
    fields = [field.name for field in I3_V2_CONTRACTS["universe_daily"].arrow_schema]
    migrated = (
        joined.with_columns(
            pl.lit(identity_policy_bundle_id, dtype=pl.String).alias("identity_policy_bundle_id"),
            pl.lit(row_available_session, dtype=pl.Date).alias("row_available_session"),
        )
        .sort("__row_index")
        .select(fields)
        .collect(engine="streaming")
    )
    return migrated.to_arrow().cast(
        I3_V2_CONTRACTS["universe_daily"].arrow_schema,
        safe=True,
    )


def _terminal_and_prepared_row_versions(
    rows_by_table: Mapping[str, tuple[dict[str, object], ...]],
    outputs: Mapping[str, I3ProductionTableOutput],
    *,
    availability_session: date,
) -> tuple[list[TerminalRowVersionState], tuple[I3ProductionPreparedRowVersion, ...]]:
    terminal: list[TerminalRowVersionState] = []
    prepared: list[I3ProductionPreparedRowVersion] = []
    for table_name in _SMALL_TABLES:
        stable_field, version_field, predecessor_field = _VERSION_SHAPE[table_name]
        artifact = outputs[table_name].manifest_output.artifact
        validator_digest = stable_digest(
            {
                "migration_rule_version": MIGRATION_RULE_VERSION,
                "operation": RowVersionOperation.NEW_ROOT.value,
                "rule_version": COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION,
                "schema_digest": I3_V2_CONTRACTS[table_name].schema_digest,
                "table_name": table_name,
            }
        )
        for index, row in enumerate(rows_by_table[table_name]):
            stable_key = str(row[stable_field])
            version_id = str(row[version_field])
            predecessor = row[predecessor_field]
            payload_digest = stable_digest(_jsonable(row))
            terminal.append(
                TerminalRowVersionState(
                    table_name=table_name,
                    stable_row_key=stable_key,
                    row_version_id=version_id,
                    predecessor_row_version_id=(None if predecessor is None else str(predecessor)),
                    row_payload_digest=payload_digest,
                    index_artifact=artifact,
                    availability_session=availability_session,
                )
            )
            prepared.append(
                I3ProductionPreparedRowVersion(
                    table_name=table_name,
                    stable_row_key=stable_key,
                    row_version_id=version_id,
                    predecessor_row_version_id=(None if predecessor is None else str(predecessor)),
                    operation=RowVersionOperation.NEW_ROOT,
                    availability_session=availability_session,
                    index_artifact=artifact,
                    row_locator=f"row_index={index}",
                    row_payload_digest=payload_digest,
                    predecessor_payload_digest=None,
                    validator_semantics_digest=validator_digest,
                )
            )
    return terminal, tuple(prepared)


def _checkpoint_s4_terminal_pins(
    s4: I3S4BasePins,
    *,
    base_frontier: S4BaseFrontier,
) -> tuple[S4TerminalPartitionPin, ...]:
    return tuple(
        S4TerminalPartitionPin(
            table_name=item.table_name,
            session_date=s4.terminal_session,
            partition_receipt_id=stable_digest(
                {
                    "artifact": item.artifact.to_dict(),
                    "base_frontier_id": base_frontier.frontier_id,
                    "rule_version": COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION,
                    "table_name": item.table_name,
                    "terminal_session": s4.terminal_session.isoformat(),
                }
            ),
            artifact=item.artifact,
            availability_session=base_frontier.release_available_session,
        )
        for item in s4.terminal_partitions
    )


def _alias_mapping_frame(
    aliases: Mapping[str, MigratedAliasRoot],
) -> pl.DataFrame:
    rows = [
        {
            "ticker_alias_id": legacy_id,
            "alias_segment_id": item.state.segment.alias_segment_id,
            "alias_resolution_version_id": (item.state.resolution.alias_resolution_version_id),
        }
        for legacy_id, item in sorted(aliases.items())
    ]
    return pl.DataFrame(
        rows,
        schema={
            "ticker_alias_id": pl.String,
            "alias_segment_id": pl.String,
            "alias_resolution_version_id": pl.String,
        },
    )


def _assert_exact_legacy_projection(
    table_name: str,
    v2: pa.Table,
    legacy: pa.Table,
    *,
    aliases_by_legacy_id: Mapping[str, MigratedAliasRoot],
    alias_reverse_map: pl.DataFrame | None = None,
) -> None:
    expected_schema = S7_DERIVED_CONTRACTS[table_name].arrow_schema
    if table_name in {"asset_master", "issuer_master"}:
        names = [field.name for field in expected_schema]
        projected = v2.select(names).cast(expected_schema, safe=True)
    elif table_name == "universe_daily":
        # Keep the 69M-row projection check columnar.  Only the 33k-entry alias
        # reverse map is materialized in Python; no membership row is hashed or
        # converted to a Python object.
        common_schema = pa.schema(
            field for field in expected_schema if field.name != "ticker_alias_id"
        )
        projected = v2.select(common_schema.names).cast(common_schema, safe=True)
        expected_common = legacy.select(common_schema.names).cast(common_schema, safe=True)
        if not projected.equals(expected_common):
            raise I3MigrationIOError(
                "native-v2 universe_daily readback differs from exact v1 canonical projection"
            )
        if alias_reverse_map is None:
            raise I3MigrationIOError("universe projection lacks the fixed alias reverse map")
        aliases = (
            pl.from_arrow(v2.select(["alias_segment_id"]))
            .lazy()
            .with_row_index("__row_index")
            .join(
                alias_reverse_map.lazy(),
                on="alias_segment_id",
                how="left",
                maintain_order="left",
            )
            .sort("__row_index")
            .collect(engine="streaming")
        )
        unknown_segments = aliases.select(
            (pl.col("alias_segment_id").is_not_null() & pl.col("ticker_alias_id").is_null())
            .sum()
            .alias("count")
        ).item()
        actual_alias_ids = aliases["ticker_alias_id"].to_arrow().cast(pa.string())
        expected_alias_ids = legacy["ticker_alias_id"].combine_chunks().cast(pa.string())
        if int(unknown_segments or 0) or not actual_alias_ids.equals(expected_alias_ids):
            raise I3MigrationIOError(
                "native-v2 universe_daily alias projection differs from exact v1"
            )
        return
    else:
        legacy_rule_versions = (
            {
                str(row["ticker_alias_id"]): str(row["ticker_alias_id_rule_version"])
                for row in legacy.to_pylist()
            }
            if table_name == "ticker_alias"
            else {}
        )
        reverse = {
            item.state.segment.alias_segment_id: (
                legacy_id,
                legacy_rule_versions.get(legacy_id),
            )
            for legacy_id, item in aliases_by_legacy_id.items()
        }
        rows = []
        for row in v2.to_pylist():
            segment_id = row.get("alias_segment_id")
            alias_id = None if segment_id is None else reverse[str(segment_id)][0]
            projected_row = {
                key: value
                for key, value in row.items()
                if key not in _V2_ENVELOPE_FIELDS[table_name]
            }
            if table_name == "ticker_alias":
                if segment_id is None:
                    raise I3MigrationIOError("ticker_alias v2 row has no segment")
                projected_row["ticker_alias_id"] = alias_id
                projected_row["ticker_alias_id_rule_version"] = reverse[str(segment_id)][1]
            else:
                projected_row["ticker_alias_id"] = alias_id
            rows.append(projected_row)
        projected = pa.Table.from_pylist(rows, schema=expected_schema)
    expected = legacy.cast(expected_schema, safe=True)
    if not projected.equals(expected):
        raise I3MigrationIOError(
            f"native-v2 {table_name} readback differs from exact v1 canonical projection"
        )


def _validate_small_table_lineage(table: pa.Table, run_spec: I3ProductionRunSpec) -> None:
    rows = table.to_pylist()
    policy = {
        item.registry_kind.value: item for item in run_spec.identity_policy_bundle.registry_releases
    }
    for row in rows:
        _validate_row_lineage(row, run_spec, policy=policy)


def _validate_universe_lineage(table: pa.Table, run_spec: I3ProductionRunSpec) -> None:
    # One partition is bounded.  Validation remains vectorized and does not
    # build a Python hash chain over membership rows.
    frame = pl.from_arrow(table).lazy()
    policy = {
        item.registry_kind.value: item for item in run_spec.identity_policy_bundle.registry_releases
    }
    expressions = [
        (pl.col("source_s4_release_set_id") != run_spec.s4_v1_source.object_id)
        .sum()
        .alias("s4_release_mismatch")
    ]
    for kind, (release_field, availability_field) in _REGISTRY_SOURCE_FIELDS.items():
        pin = policy[kind]
        expressions.extend(
            (
                (pl.col(release_field) != pin.release_id).sum().alias(f"{kind}_id"),
                (pl.col(availability_field) != pin.release_available_session)
                .sum()
                .alias(f"{kind}_availability"),
            )
        )
    observed = frame.select(*expressions).collect(engine="streaming").row(0, named=True)
    mismatches = {key: int(value or 0) for key, value in observed.items() if int(value or 0)}
    if mismatches:
        raise I3MigrationIOError(f"v1 universe lineage differs from RunSpec: {mismatches}")


def _validate_row_lineage(
    row: Mapping[str, object],
    run_spec: I3ProductionRunSpec,
    *,
    policy: Mapping[str, object],
) -> None:
    if row.get("source_s4_release_set_id") != run_spec.s4_v1_source.object_id:
        raise I3MigrationIOError("v1 compact row binds another S4 release")
    for kind, (release_field, availability_field) in _REGISTRY_SOURCE_FIELDS.items():
        pin = policy[kind]
        if (
            row.get(release_field) != pin.release_id
            or row.get(availability_field) != pin.release_available_session
        ):
            raise I3MigrationIOError(f"v1 compact row {kind} lineage differs")


def _with_exact_legacy_sic_projection(
    row: Mapping[str, object],
    projection: LegacyIssuerAggregateProjection,
) -> LegacyIssuerAggregateProjection:
    from dataclasses import replace

    count = int(row["sic_code_variant_count"])
    selected = row["sic_code_current_reference"]
    if count == 0 and selected is None:
        return projection
    if count == 1 and isinstance(selected, str) and selected:
        return replace(projection, sic_codes=(selected,))
    raise I3MigrationIOError(
        "S4 universe_source_daily cannot authenticate multiple legacy SIC variants"
    )


def _read_compact_table(
    pins: Sequence[I3MigrationParquetPin],
    paths: Mapping[I3MigrationParquetPin, Path],
) -> pa.Table:
    if not pins:
        raise I3MigrationIOError("compact source table pin list is empty")
    table_name = pins[0].table_name
    tables = [pq.ParquetFile(paths[item]).read() for item in pins]
    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    expected = S7_DERIVED_CONTRACTS[table_name].arrow_schema
    try:
        return combined.cast(expected, safe=True)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise I3MigrationIOError(f"cannot read exact compact {table_name} rows") from exc


def _verify_source_parquet(root: Path, pin: I3MigrationParquetPin) -> Path:
    path = safe_relative_path(root, pin.artifact.path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.artifact.bytes
        or sha256_file(path) != pin.artifact.sha256
    ):
        raise I3MigrationIOError(f"exact source artifact differs: {pin.artifact.path}")
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, pa.ArrowException) as exc:
        raise I3MigrationIOError(f"source is not readable Parquet: {pin.artifact.path}") from exc
    contract = {**S7_DERIVED_CONTRACTS, **ASSET_CONTRACTS}[pin.table_name]
    if parquet.metadata.num_rows != pin.row_count or not parquet.schema_arrow.equals(
        contract.arrow_schema
    ):
        raise I3MigrationIOError(f"source schema or row count differs: {pin.artifact.path}")
    if pin.session_date is not None:
        sessions = pc.unique(parquet.read(columns=["session_date"])["session_date"])
        observed = set(sessions.to_pylist())
        if observed != ({pin.session_date} if pin.row_count else set()):
            raise I3MigrationIOError(
                f"source partition contains another session: {pin.artifact.path}"
            )
    return path


def readback_i3_migration_parquet_exact(
    *,
    data_root: Path,
    artifact: ArtifactPin,
    table_name: str,
    row_count: int,
    session_date: date | None = None,
) -> pa.Table:
    """Reusable delta/base readback guard for one exact native-v2 Parquet."""

    return _verify_output_parquet(
        data_root.expanduser().resolve(),
        artifact,
        table_name=table_name,
        row_count=row_count,
        session_date=session_date,
    )


def write_i3_migration_parquet_no_clobber(
    *,
    data_root: Path,
    relative_path: str,
    table: pa.Table,
    run_spec: I3ProductionRunSpec,
) -> ArtifactPin:
    """Reusable delta/base exclusive immutable Parquet writer with live guards."""

    return _write_parquet_no_clobber(
        data_root.expanduser().resolve(),
        relative_path,
        table,
        run_spec=run_spec,
    )


def _verify_output_parquet(
    root: Path,
    artifact: ArtifactPin,
    *,
    table_name: str,
    row_count: int,
    session_date: date | None = None,
) -> pa.Table:
    content = _read_exact_artifact(root, artifact)
    del content
    parquet = pq.ParquetFile(safe_relative_path(root, artifact.path))
    expected = I3_V2_CONTRACTS[table_name].arrow_schema
    if parquet.metadata.num_rows != row_count or not parquet.schema_arrow.equals(expected):
        raise I3MigrationIOError(f"{table_name} output schema or row count differs")
    table = parquet.read().cast(expected, safe=True)
    if session_date is not None:
        sessions = set(pc.unique(table["session_date"]).to_pylist())
        if sessions != ({session_date} if row_count else set()):
            raise I3MigrationIOError("universe output partition contains another session")
    return table


def _write_parquet_no_clobber(
    root: Path,
    relative: str,
    table: pa.Table,
    *,
    run_spec: I3ProductionRunSpec,
) -> ArtifactPin:
    path = safe_relative_path(root, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise I3MigrationIOError(f"immutable Parquet output already exists: {relative}")
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise I3MigrationIOError("stale compact-base temporary file already exists")
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            compression_level=6,
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
        )
        _check_live_resources(root, run_spec)
        os.chmod(temporary, 0o444, follow_symlinks=False)
        os.link(temporary, path)
    except Exception:
        if temporary.exists() and not temporary.is_symlink():
            temporary.unlink()
        raise
    temporary.unlink()
    return ArtifactPin(path=relative, sha256=sha256_file(path), bytes=path.stat().st_size)


def _write_bytes_no_clobber(
    root: Path,
    relative: str,
    content: bytes,
    *,
    run_spec: I3ProductionRunSpec,
) -> ArtifactPin:
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
        raise I3MigrationIOError(f"immutable output already exists: {relative}") from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        # Keep a partial no-clobber artifact as fail-closed audit evidence.
        raise
    _check_live_resources(root, run_spec)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _read_exact_artifact(root: Path, pin: ArtifactPin) -> bytes:
    path = safe_relative_path(root, pin.path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.bytes
        or sha256_file(path) != pin.sha256
    ):
        raise I3MigrationIOError(f"exact artifact differs from pin: {pin.path}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise I3MigrationIOError(f"cannot read exact artifact: {pin.path}") from exc


def _compact_base_source_digest(
    run_spec: I3ProductionRunSpec,
    *,
    legacy: I3LegacyV1BasePins,
    s4: I3S4BasePins,
) -> str:
    frontier = run_spec.i2_base_frontier
    if frontier is None:  # pragma: no cover - guarded before digest
        raise I3MigrationIOError("base frontier is absent")
    return stable_digest(
        {
            "calendar": run_spec.calendar.to_dict(),
            "i2_base_frontier": frontier.to_dict(),
            "identity_policy_bundle_id": (
                run_spec.identity_policy_bundle.identity_policy_bundle_id
            ),
            "legacy_complete_partition_pin_digest": (legacy.complete_partition_pin_digest),
            "legacy_release_set_artifact": legacy.release_set_artifact.to_dict(),
            "legacy_release_set_id": legacy.release_set_id,
            "rule_version": COMPACT_BASE_SOURCE_RULE_VERSION,
            "s4_release_set_artifact": s4.release_set_artifact.to_dict(),
            "s4_release_set_id": s4.release_set_id,
            "s4_universe_source_partition_pin_digest": stable_digest(
                [item.to_dict() for item in s4.universe_source_partitions]
            ),
            "s4_terminal_partition_pin_digest": stable_digest(
                [item.to_dict() for item in s4.terminal_partitions]
            ),
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "terminal_session": run_spec.terminal_session.isoformat(),
            "transform_semantics_digest": run_spec.transform_semantics_digest,
        }
    )


def _compact_base_input_authority_digest(run_spec: I3ProductionRunSpec) -> str:
    frontier = run_spec.i2_base_frontier
    if frontier is None:
        raise I3MigrationIOError("BASE materialization authority lacks its I2 frontier")
    return stable_digest(
        {
            "availability_session": run_spec.run_available_session.isoformat(),
            "calendar": run_spec.calendar.to_dict(),
            "i0_oracle": run_spec.i0_oracle.to_dict(),
            "i2_base_frontier": frontier.to_dict(),
            "identity_policy_bundle": run_spec.identity_policy_bundle.to_dict(),
            "identity_policy_bundle_artifact": (run_spec.identity_policy_bundle_artifact.to_dict()),
            "rule_version": "s7_5_i3_compact_base_input_authority_v1",
            "run_kind": run_spec.run_kind.value,
            "s4_v1_source": run_spec.s4_v1_source.to_dict(),
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "source_cutoff_session": run_spec.source_cutoff_session.isoformat(),
            "terminal_session": run_spec.terminal_session.isoformat(),
            "transform_semantics_digest": run_spec.transform_semantics_digest,
        }
    )


def _compact_base_migration_semantics_digest(run_spec: I3ProductionRunSpec) -> str:
    return stable_digest(
        {
            "adapter_rule_versions": {
                "canonical_projection": (
                    COMPACT_BASE_CANONICAL_PROJECTION_ATTESTATION_RULE_VERSION
                ),
                "input_binding": COMPACT_BASE_INPUT_BINDING_RULE_VERSION,
                "partition_receipt": COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION,
                "row_change_index": (COMPACT_BASE_ROW_CHANGE_INDEX_ATTESTATION_RULE_VERSION),
                "row_validator": COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION,
                "s4_terminal_receipt": (COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION),
                "source": COMPACT_BASE_SOURCE_RULE_VERSION,
                "unresolved_seed": COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION,
            },
            "legacy_contracts": [
                {
                    "contract_id": S7_DERIVED_CONTRACTS[table].contract_id,
                    "schema_digest": S7_DERIVED_CONTRACTS[table].schema_digest,
                    "table_name": table,
                }
                for table in _V1_TABLE_ORDER
            ],
            "migration_core_rule_version": MIGRATION_RULE_VERSION,
            "native_v2_contracts": [
                {
                    "contract_id": I3_V2_CONTRACTS[table].contract_id,
                    "schema_digest": I3_V2_CONTRACTS[table].schema_digest,
                    "table_name": table,
                }
                for table in I3_V2_TABLE_ORDER
            ],
            "rule_version": COMPACT_BASE_MIGRATION_SEMANTICS_RULE_VERSION,
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "transform_semantics_digest": run_spec.transform_semantics_digest,
        }
    )


def _compact_base_table_output_set_digest(
    prepared: I3ProductionPreparedMaterialization,
) -> str:
    return stable_digest(
        {
            "rule_version": "s7_5_i3_compact_base_table_output_set_v1",
            "table_outputs": [item.to_dict() for item in prepared.table_outputs],
        }
    )


def _compact_base_row_change_index_digest(
    prepared: I3ProductionPreparedMaterialization,
) -> str:
    return stable_digest(
        {
            "row_versions": [
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
            "rule_version": COMPACT_BASE_ROW_CHANGE_INDEX_ATTESTATION_RULE_VERSION,
        }
    )


def _compact_base_canonical_projection_digest(
    *,
    binding: I3CompactBaseInputBinding,
    prepared: I3ProductionPreparedMaterialization,
) -> str:
    outputs = {item.table_name: item for item in prepared.table_outputs}
    if tuple(item.table_name for item in prepared.table_outputs) != I3_V2_TABLE_ORDER:
        raise I3MigrationIOError("BASE attestation output order differs")
    universe_output = outputs["universe_daily"]
    if universe_output.dataset_index is None:
        raise I3MigrationIOError("BASE attestation lacks the universe dataset index")
    legacy_universe = binding.legacy.pins_for("universe_daily")
    native_universe = universe_output.dataset_index.partitions
    if len(legacy_universe) != len(native_universe) or tuple(
        item.session_date for item in legacy_universe
    ) != tuple(item.session_date for item in native_universe):
        raise I3MigrationIOError("BASE attestation projection session coverage differs")
    small_table_roots = []
    for table_name in _SMALL_TABLES:
        legacy_pins = binding.legacy.pins_for(table_name)
        if len(legacy_pins) != 1:
            raise I3MigrationIOError("BASE attestation compact source coverage differs")
        small_table_roots.append(
            {
                "legacy_source": legacy_pins[0].to_dict(),
                "native_output": outputs[table_name].to_dict(),
                "table_name": table_name,
            }
        )
    return stable_digest(
        {
            "canonical_projection_difference_count": (
                prepared.canonical_projection_difference_count
            ),
            "migration_core_rule_version": MIGRATION_RULE_VERSION,
            "rule_version": (COMPACT_BASE_CANONICAL_PROJECTION_ATTESTATION_RULE_VERSION),
            "small_table_roots": small_table_roots,
            "universe_partition_projections": [
                {
                    "legacy_source": legacy_pin.to_dict(),
                    "native_output": native_pin.to_dict(),
                    "session_date": native_pin.session_date.isoformat(),
                }
                for legacy_pin, native_pin in zip(legacy_universe, native_universe, strict=True)
            ],
        }
    )


def _compact_base_attestation_fields(
    *,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3CompactBaseInputBinding,
) -> dict[str, object]:
    if run_spec.run_kind is not I3ProductionRunKind.BASE:
        raise I3MigrationIOError("BASE attestation cannot bind a DELTA RunSpec")
    expected_source_digest = _compact_base_source_digest(
        run_spec,
        legacy=binding.legacy,
        s4=binding.s4,
    )
    if prepared.source_digest != expected_source_digest:
        raise I3MigrationIOError(
            "BASE materialization source differs from the exact expanded inputs"
        )
    return {
        "run_spec_id": run_spec.run_spec_id,
        "input_binding_id": binding.input_binding_id,
        "input_authority_digest": _compact_base_input_authority_digest(run_spec),
        "source_digest": expected_source_digest,
        "migration_semantics_digest": (_compact_base_migration_semantics_digest(run_spec)),
        "table_output_set_digest": _compact_base_table_output_set_digest(prepared),
        "native_manifest_id": prepared.native_manifest.release_id,
        "native_manifest_artifact": prepared.native_manifest_artifact,
        "checkpoint_id": prepared.checkpoint.checkpoint_id,
        "checkpoint_artifact": prepared.checkpoint_artifact,
        "row_change_index_digest": _compact_base_row_change_index_digest(prepared),
        "resource_observation_digest": stable_digest(
            {
                "observation": prepared.resource_observation.to_dict(),
                "rule_version": "s7_5_i3_compact_base_resource_observation_attestation_v1",
            }
        ),
        "canonical_projection_digest": _compact_base_canonical_projection_digest(
            binding=binding,
            prepared=prepared,
        ),
        "canonical_projection_difference_count": (prepared.canonical_projection_difference_count),
        "terminal_session": run_spec.terminal_session,
        "availability_session": run_spec.run_available_session,
    }


def _mint_compact_base_materialization_attestation(
    *,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3CompactBaseInputBinding,
) -> CompactBaseMaterializationAttestation:
    attestation = CompactBaseMaterializationAttestation(
        **_compact_base_attestation_fields(
            run_spec=run_spec,
            prepared=prepared,
            binding=binding,
        )
    )
    object.__setattr__(attestation, "_seal", _BASE_ATTESTATION_SEAL)
    _MINTED_BASE_ATTESTATIONS[id(attestation)] = attestation
    return attestation


def _verify_compact_base_materialization_attestation_with_binding(
    *,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    binding: I3CompactBaseInputBinding,
) -> CompactBaseMaterializationAttestation:
    attestation = _require_module_sealed_base_attestation(prepared)
    expected = CompactBaseMaterializationAttestation(
        **_compact_base_attestation_fields(
            run_spec=run_spec,
            prepared=prepared,
            binding=binding,
        )
    )
    if attestation.logical_payload() != expected.logical_payload():
        raise I3MigrationIOError(
            "BASE materialization attestation differs from exact inputs, outputs, or observation"
        )
    return attestation


def _require_module_sealed_base_attestation(
    prepared: I3ProductionPreparedMaterialization,
) -> CompactBaseMaterializationAttestation:
    if type(prepared) is not CompactBasePreparedMaterialization:
        raise I3MigrationIOError(
            "BASE materialization lacks the official nominal prepared capability"
        )
    attestation = prepared.base_materialization_attestation
    if (
        type(attestation) is not CompactBaseMaterializationAttestation
        or attestation._seal is not _BASE_ATTESTATION_SEAL
        or _MINTED_BASE_ATTESTATIONS.get(id(attestation)) is not attestation
    ):
        raise I3MigrationIOError("BASE materialization attestation is not module-sealed")
    return attestation


def _validate_resource_estimate(
    run_spec: I3ProductionRunSpec,
    estimate: I3CompactBaseResourceEstimate,
) -> None:
    caps = run_spec.resource_caps
    if estimate.estimated_peak_rss_bytes > caps.rss_bytes_hard_cap:
        raise I3MigrationIOError("estimated compact-base RSS exceeds the hard cap")
    if estimate.estimated_output_bytes > caps.output_bytes_hard_cap:
        raise I3MigrationIOError("estimated compact-base output exceeds the hard cap")
    if estimate.estimated_temporary_bytes > caps.temporary_bytes_hard_cap:
        raise I3MigrationIOError("estimated compact-base temporary bytes exceed the hard cap")


def _check_preflight_resources(
    root: Path,
    run_spec: I3ProductionRunSpec,
    estimate: I3CompactBaseResourceEstimate,
) -> int:
    peak = _peak_rss_bytes()
    free = shutil.disk_usage(root).free
    if peak > run_spec.resource_caps.rss_bytes_hard_cap:
        raise I3MigrationIOError("compact-base RSS exceeds the hard cap")
    if free < estimate.minimum_free_disk_bytes_required:
        raise I3MigrationIOError(
            "compact-base free disk cannot preserve the hard floor through "
            "estimated output and temporary peak"
        )
    return free


def _check_live_resources(root: Path, run_spec: I3ProductionRunSpec) -> int:
    peak = _peak_rss_bytes()
    free = shutil.disk_usage(root).free
    if peak > run_spec.resource_caps.rss_bytes_hard_cap:
        raise I3MigrationIOError("compact-base RSS exceeds the hard cap")
    if free < max(
        _MINIMUM_PRODUCTION_DISK_FLOOR,
        run_spec.resource_caps.disk_free_bytes_hard_floor,
    ):
        raise I3MigrationIOError("compact-base free disk is below the 40 GiB hard floor")
    return free


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _require_empty_workspace(root: Path, workspace: Path) -> None:
    try:
        workspace.relative_to(root)
    except ValueError as exc:
        raise I3MigrationIOError("compact-base workspace escapes data_root") from exc
    if not workspace.is_dir() or workspace.is_symlink():
        raise I3MigrationIOError("executor workspace must be an existing regular directory")
    try:
        if any(workspace.iterdir()):
            raise I3MigrationIOError("compact-base workspace is not empty; no-clobber enforced")
    except OSError as exc:
        raise I3MigrationIOError("cannot inspect compact-base workspace") from exc


def _unique_source_pins(
    legacy: I3LegacyV1BasePins,
    s4: I3S4BasePins,
) -> tuple[I3MigrationParquetPin, ...]:
    by_path: dict[str, I3MigrationParquetPin] = {}
    for item in (
        *legacy.member_outputs,
        *s4.universe_source_partitions,
        *s4.terminal_partitions,
    ):
        prior = by_path.get(item.artifact.path)
        if prior is not None and prior != item:
            raise I3MigrationIOError("one exact source path carries conflicting pins")
        by_path[item.artifact.path] = item
    return tuple(by_path[path] for path in sorted(by_path))


def _scan_explicit_parquet(paths: Sequence[str]) -> pl.LazyFrame:
    if not paths:
        raise I3MigrationIOError("explicit Parquet scan cannot be empty")
    return pl.scan_parquet(
        list(paths),
        glob=False,
        hive_partitioning=False,
        low_memory=True,
        cache=False,
        rechunk=False,
    )


def _workspace_relative(root: Path, path: Path) -> str:
    try:
        relative = path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise I3MigrationIOError("output path escapes data_root") from exc
    _explicit_path(relative)
    return relative


def _strict_canonical_json(content: bytes, *, trailing_newline: bool) -> Mapping[str, object]:
    def reject_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise I3MigrationIOError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(content, object_pairs_hook=reject_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3MigrationIOError("exact control is not valid JSON") from exc
    if not isinstance(value, dict):
        raise I3MigrationIOError("exact control JSON root is not an object")
    expected = (
        _canonical_json_bytes(value) if trailing_newline else _canonical_json_bytes_no_nl(value)
    )
    if expected != content:
        raise I3MigrationIOError("exact control JSON bytes are not canonical")
    return value


def _closed_object(
    value: object,
    fields: set[str],
    label: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise I3MigrationIOError(f"{label} fields differ")
    return dict(value)


def _object_array(value: object, label: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise I3MigrationIOError(f"{label} must be an object array")
    return tuple(dict(item) for item in value)


def _release_available_session(value: object, *, label: str) -> date:
    if not isinstance(value, Mapping):
        raise I3MigrationIOError(f"{label} is invalid")
    raw = value.get("release_available_session")
    if not isinstance(raw, str):
        raise I3MigrationIOError(f"{label} has no release session")
    try:
        result = date.fromisoformat(raw)
    except ValueError as exc:
        raise I3MigrationIOError(f"{label} session is invalid") from exc
    if result.isoformat() != raw:
        raise I3MigrationIOError(f"{label} session is not canonical")
    return result


def _partition_session(path: str) -> date:
    _explicit_path(path)
    match = _SESSION_PARTITION_PATH.search(path)
    if match is None:
        raise I3MigrationIOError("partition output path is not canonical")
    raw = match.group("session")
    try:
        result = date.fromisoformat(raw)
    except ValueError as exc:
        raise I3MigrationIOError("partition output path session is invalid") from exc
    if result.isoformat() != raw:
        raise I3MigrationIOError("partition output path session is not canonical")
    return result


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I3MigrationIOError(f"{label} must be a nonnegative integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _nonnegative_int(value, label)
    if result == 0:
        raise I3MigrationIOError(f"{label} must be positive")
    return result


def _canonical_json_bytes(value: object) -> bytes:
    return _canonical_json_bytes_no_nl(value) + b"\n"


def _canonical_json_bytes_no_nl(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _explicit_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or any("latest" in part.lower() for part in path.parts)
        or any(character in value for character in "*?[]{}")
    ):
        raise I3MigrationIOError("migration path must be explicit, normalized, and non-latest")


def _migration_pin_sort_key(item: I3MigrationParquetPin) -> tuple[int, date, str]:
    table_index = _V1_TABLE_ORDER.index(item.table_name)
    return (table_index, item.session_date or date.min, item.artifact.path)


def _strings(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise I3MigrationIOError("streaming aggregate emitted a non-list value")
    return tuple(sorted(str(item) for item in value if item is not None))


def _reason_code(value: str) -> str:
    cleaned = "".join(character if character.isalnum() else "_" for character in value.lower())
    cleaned = cleaned.strip("_") or "pending_review"
    return cleaned if cleaned[0].isalpha() else f"identity_{cleaned}"


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise I3MigrationIOError(f"{label} must be a lowercase SHA-256")
    try:
        int(value, 16)
    except ValueError as exc:
        raise I3MigrationIOError(f"{label} must be a lowercase SHA-256") from exc
    if value != value.lower():
        raise I3MigrationIOError(f"{label} must be a lowercase SHA-256")
    return value


def _workspace_file_bytes(workspace: Path) -> int:
    total = 0
    for directory, _, files in os.walk(workspace):
        for name in files:
            path = Path(directory, name)
            if path.is_symlink() or not path.is_file():
                raise I3MigrationIOError("compact-base workspace contains a non-regular file")
            total += path.stat().st_size
    return total


__all__ = [
    "CompactBaseMaterializationAttestation",
    "CompactBaseMigrationMaterializer",
    "CompactBasePreparedMaterialization",
    "I3CompactBaseInputBinding",
    "I3CompactBaseResourceEstimate",
    "I3LegacyV1BasePins",
    "I3MigrationIOError",
    "I3MigrationParquetPin",
    "I3S4BasePins",
    "estimate_compact_base_resources",
    "load_compact_base_input_binding",
    "load_compact_base_materializer",
    "prepare_compact_base",
    "readback_i3_migration_parquet_exact",
    "verify_compact_base_materialization_attestation",
    "write_i3_migration_parquet_no_clobber",
]
