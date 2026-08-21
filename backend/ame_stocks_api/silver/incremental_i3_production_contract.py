"""Production-only control contract for the S7.5 I3 native-v2 staging lane.

This module is deliberately separate from the local fixture runner.  Its public
objects bind one exact production base/delta run, its immutable outputs, and the
single ``awaiting_review`` completion marker.  It grants neither publication nor
cutover authority.

The four top-level native-v2 manifest roles remain fixed.  The three versioned
master tables are single-segment rowset indexes for a base staging run, while
``universe_daily`` is a canonical dataset-index JSON whose members are exact
session-partitioned Parquet pins.  This keeps clean append bounded without
weakening byte, schema, cardinality, or terminal-state reconciliation.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, Self

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.contracts import arrow_schema_digest
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    CheckpointReceipt,
    ContentAttestedRelease,
    ControlObjectKind,
    ControlObjectPin,
    IncrementalReleaseManifest,
    ManifestPin,
    PartitionReceipt,
    ReleaseType,
    RowSemanticProofReceipt,
    RowVersionChangeIndexPin,
    RowVersionOperation,
    RowVersionReceipt,
    RowVersionReference,
    RunReceipt,
    RunSpec,
    ViewKind,
    _mint_content_attested_release,
    _validate_i3_release_projection_after_row_attestation,
    control_object_pin,
)
from ame_stocks_api.silver.incremental_gate import (
    GateArtifactPin,
    QaCheckPolicy,
    QaCheckResult,
    QaPolicy,
    QaReceipt,
    QaSeverity,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    LEGACY_S7_V1_RELEASE_SET_ID,
    NATIVE_V2_RELEASE_FAMILY,
    I3CheckpointState,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    IdentityRegistryReleasePin,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    i3_checkpoint_storage_payload,
    i3_checkpoint_storage_pin,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACT_ID_BY_TABLE,
    I3_V2_CONTRACTS,
    I3_V2_RESOURCE_SHA256_BY_TABLE,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_PRODUCTION_DELTA_ALIAS_AVAILABILITY_PROGRESSION_RULE_VERSION,
    I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION,
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
    production_compact_base_initial_segment_id,
    production_compact_base_row_validator_digest,
    production_delta_append_segment_id,
    production_delta_row_validator_digest,
    production_native_v2_migration_id,
)

I3_PRODUCTION_NAMESPACE: Final = "ame_stocks.silver.s7_5.i3_production_staging"
I3_PRODUCTION_RUN_SPEC_RULE_VERSION: Final = "s7_5_i3_production_run_spec_v1"
I3_PRODUCTION_DELTA_RUN_SPEC_RULE_VERSION: Final = "s7_5_i3_production_delta_run_spec_v5"
I3_PRODUCTION_RUN_RECEIPT_RULE_VERSION: Final = "s7_5_i3_production_run_receipt_v1"
I3_PRODUCTION_COMPLETION_RULE_VERSION: Final = "s7_5_i3_production_completion_v1"
I3_PRODUCTION_OUTPUT_SET_RULE_VERSION: Final = "s7_5_i3_production_output_set_v1"
I3_PRODUCTION_DATASET_INDEX_RULE_VERSION: Final = "s7_5_i3_dataset_index_v1"
I3_PRODUCTION_ROWSET_INDEX_RULE_VERSION: Final = "s7_5_i3_rowset_index_v1"
I3_PRODUCTION_TERMINAL_DIGEST_RULE_VERSION: Final = "s7_5_i3_parquet_leaf_terminal_digest_v1"
I3_PRODUCTION_DEEP_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_production_deep_verification_attestation_v1"
)
I3_PRODUCTION_PHYSICAL_INDEX_DIGEST_RULE_VERSION: Final = (
    "s7_5_i3_production_physical_index_digest_v1"
)
I3_PRODUCTION_BASE_FK_SUMMARY_RULE_VERSION: Final = (
    "s7_5_i3_production_base_fk_verification_summary_v1"
)

I0_ORACLE_RELEASE_SET_SHA256: Final = (
    "3690046fc32801dc23e85d4713d90b476b188988ec2426bd1d1c13fcdd9f1c0b"
)
I0_ORACLE_RELEASE_SET_BYTES: Final = 4_668
I0_ORACLE_AVAILABLE_SESSION: Final = date(2026, 8, 3)
S4_V1_RELEASE_SET_ID: Final = "f81c7ee28939db3350fce809326723e911b6d486c6db166d2575fcc92cb2101d"
S4_V1_RELEASE_SET_SHA256: Final = "937eaf4ed502fb2786dafb0dce9ec613bcaccb2cd488812cc5900118238d6c13"
S4_V1_RELEASE_SET_BYTES: Final = 4_440_685

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_CONTROL_JSON_BYTES_CAP = 64 * 1024 * 1024
# The authenticated checkpoint contains the complete bounded native-v2
# frontier and is intentionally larger than ordinary control documents.  Give
# only this exact artifact class a separate finite envelope; do not widen the
# generic control reader or any caller-selected path.
_CHECKPOINT_JSON_BYTES_CAP = 192 * 1024 * 1024
_DELTA_BOUNDARY_PARTITION_COUNT: Final = 3
_VERSIONED_TABLES: Final = ("asset_master", "ticker_alias", "issuer_master")
_VERSION_FIELDS: Final[Mapping[str, tuple[str, str, str, str]]] = {
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
_ROW_VERSION_CHANGE_INDEX_SCHEMA: Final = pa.schema(
    [
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("stable_row_key", pa.string(), nullable=False),
        pa.field("row_version_id", pa.string(), nullable=False),
        pa.field("predecessor_row_version_id", pa.string(), nullable=True),
        pa.field("operation", pa.string(), nullable=False),
        pa.field("availability_session", pa.date32(), nullable=False),
        pa.field("index_artifact_path", pa.string(), nullable=False),
        pa.field("index_artifact_sha256", pa.string(), nullable=False),
        pa.field("index_artifact_bytes", pa.int64(), nullable=False),
        pa.field("row_locator", pa.string(), nullable=False),
        pa.field("row_payload_digest", pa.string(), nullable=False),
        pa.field("predecessor_payload_digest", pa.string(), nullable=True),
        pa.field("validator_semantics_digest", pa.string(), nullable=False),
        pa.field("semantic_proof_digest", pa.string(), nullable=False),
    ]
)
_ROW_VERSION_CHANGE_INDEX_SCHEMA_DIGEST: Final = arrow_schema_digest(
    _ROW_VERSION_CHANGE_INDEX_SCHEMA
)
_ROW_VERSION_CHANGE_LOGICAL_RECEIPTS_RULE_VERSION: Final = (
    "s7_5_i3_production_indexed_logical_row_receipts_v1"
)
_ROW_VERSION_CHANGE_SUPERSESSION_RULE_VERSION: Final = "s7_5_i3_production_indexed_supersession_v1"
_ROW_VERSION_CHANGE_PROOF_RULE_VERSION: Final = "s7_5_i3_production_indexed_row_proof_v1"
_ROW_VERSION_CHANGE_ATTESTATION_RULE_VERSION: Final = (
    "s7_5_i3_production_indexed_row_attestation_v1"
)
_BASE_FK_PARTITION_SET_RULE_VERSION: Final = "s7_5_i3_production_base_fk_partition_set_v1"
_BASE_FK_LOGICAL_REFERENCE_RULE_VERSION: Final = "s7_5_i3_production_base_fk_logical_reference_v1"


class I3ProductionContractError(ValueError):
    """Raised when production staging controls or exact bytes do not reconcile."""


class I3ProductionRunKind(StrEnum):
    BASE = "base"
    DELTA = "delta"


class I3ProductionRunState(StrEnum):
    FAILED = "failed"
    SUCCEEDED = "succeeded"


class I3ProductionCompletionState(StrEnum):
    AWAITING_REVIEW = "awaiting_review"


class I3ProductionParentAuthority(StrEnum):
    EXACT_STAGING = "exact_staging"
    MIGRATION_SHADOW = "migration_shadow"


class I3ProductionDependencyRole(StrEnum):
    I0_V1_ORACLE = "i0_v1_oracle"
    S4_V1_SOURCE = "s4_v1_source"


class I3ProductionOutputStorage(StrEnum):
    PARQUET = "parquet"
    DATASET_INDEX = "dataset_index"
    ROWSET_INDEX = "rowset_index"


@dataclass(frozen=True, slots=True)
class I3ProductionResourceCaps:
    """Hard execution limits frozen into the semantic RunSpec."""

    rss_bytes_hard_cap: int = 3 * 1024**3
    disk_free_bytes_hard_floor: int = 40 * 1024**3
    temporary_bytes_hard_cap: int = 32 * 1024**3
    output_bytes_hard_cap: int = 20 * 1024**3
    output_rows_hard_cap: int = 100_000_000

    def __post_init__(self) -> None:
        for value, label in (
            (self.rss_bytes_hard_cap, "RSS cap"),
            (self.disk_free_bytes_hard_floor, "disk floor"),
            (self.temporary_bytes_hard_cap, "temporary-bytes cap"),
            (self.output_bytes_hard_cap, "output-bytes cap"),
            (self.output_rows_hard_cap, "output-rows cap"),
        ):
            _positive_int(value, label)

    @property
    def digest(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, int]:
        return {
            "disk_free_bytes_hard_floor": self.disk_free_bytes_hard_floor,
            "output_bytes_hard_cap": self.output_bytes_hard_cap,
            "output_rows_hard_cap": self.output_rows_hard_cap,
            "rss_bytes_hard_cap": self.rss_bytes_hard_cap,
            "temporary_bytes_hard_cap": self.temporary_bytes_hard_cap,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "disk_free_bytes_hard_floor",
                "output_bytes_hard_cap",
                "output_rows_hard_cap",
                "rss_bytes_hard_cap",
                "temporary_bytes_hard_cap",
            },
            "production resource caps",
        )
        return cls(**{key: _integer(item[key], key) for key in item})


@dataclass(frozen=True, slots=True)
class I3ProductionDependencyPin:
    """One exact, published production dependency marker."""

    role: I3ProductionDependencyRole
    object_id: str
    artifact: ArtifactPin
    available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.role, I3ProductionDependencyRole):
            raise I3ProductionContractError("production dependency role is invalid")
        _digest(self.object_id, "production dependency object ID")
        _artifact(self.artifact, "production dependency artifact")
        _session(self.available_session, "production dependency availability")
        if _temporary_path(self.artifact.path):
            raise I3ProductionContractError(
                "temporary dependency cannot acquire production authority"
            )
        if self.role is I3ProductionDependencyRole.I0_V1_ORACLE:
            expected = (
                LEGACY_S7_V1_RELEASE_SET_ID,
                _i0_oracle_path(),
                I0_ORACLE_RELEASE_SET_SHA256,
                I0_ORACLE_RELEASE_SET_BYTES,
            )
            expected_available = I0_ORACLE_AVAILABLE_SESSION
        else:
            expected = (
                S4_V1_RELEASE_SET_ID,
                _s4_v1_path(),
                S4_V1_RELEASE_SET_SHA256,
                S4_V1_RELEASE_SET_BYTES,
            )
            expected_available = None
        if (
            self.object_id,
            self.artifact.path,
            self.artifact.sha256,
            self.artifact.bytes,
        ) != expected:
            raise I3ProductionContractError(
                f"{self.role.value} differs from the frozen production dependency"
            )
        if expected_available is not None and self.available_session != expected_available:
            raise I3ProductionContractError("I0 oracle availability differs from its freeze")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "available_session": self.available_session.isoformat(),
            "object_id": self.object_id,
            "role": self.role.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value, {"artifact", "available_session", "object_id", "role"}, "dependency pin"
        )
        try:
            role = I3ProductionDependencyRole(_text(item["role"], "dependency role"))
        except ValueError as exc:
            raise I3ProductionContractError("production dependency role is invalid") from exc
        return cls(
            role=role,
            object_id=_text(item["object_id"], "dependency object ID"),
            artifact=_artifact_from_dict(item["artifact"], "dependency artifact"),
            available_session=_date_from_json(item["available_session"], "dependency availability"),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionCalendarPin:
    calendar_artifact_id: str
    artifact: ArtifactPin
    available_session: date

    def __post_init__(self) -> None:
        _digest(self.calendar_artifact_id, "calendar artifact ID")
        _artifact(self.artifact, "calendar artifact")
        _session(self.available_session, "calendar availability")
        if _temporary_path(self.artifact.path):
            raise I3ProductionContractError(
                "temporary calendar cannot acquire production authority"
            )
        expected_path = (
            f"manifests/silver/xnys-calendars/calendar_artifact_id={self.calendar_artifact_id}.json"
        )
        if self.artifact.path != expected_path:
            raise I3ProductionContractError("calendar artifact path is not canonical")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "available_session": self.available_session.isoformat(),
            "calendar_artifact_id": self.calendar_artifact_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value, {"artifact", "available_session", "calendar_artifact_id"}, "calendar pin"
        )
        return cls(
            calendar_artifact_id=_text(item["calendar_artifact_id"], "calendar artifact ID"),
            artifact=_artifact_from_dict(item["artifact"], "calendar artifact"),
            available_session=_date_from_json(item["available_session"], "calendar availability"),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionI2ReceiptPin:
    """Exact authenticated I2 receipt selected as S4 append lineage."""

    session_date: date
    receipt_id: str
    artifact: ArtifactPin
    receipt_available_session: date

    def __post_init__(self) -> None:
        _session(self.session_date, "I2 receipt session")
        _digest(self.receipt_id, "I2 receipt ID")
        _artifact(self.artifact, "I2 receipt artifact")
        _session(self.receipt_available_session, "I2 receipt availability")
        if self.receipt_available_session < self.session_date:
            raise I3ProductionContractError("I2 receipt availability precedes its session")
        if "/fixtures/" in f"/{self.artifact.path}" or self.artifact.path.startswith("fixtures/"):
            raise I3ProductionContractError(
                "fixture I2 receipt cannot acquire production authority"
            )
        if _temporary_path(self.artifact.path):
            raise I3ProductionContractError(
                "temporary I2 receipt cannot acquire production authority"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "receipt_id": self.receipt_id,
            "session_date": self.session_date.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {"artifact", "receipt_available_session", "receipt_id", "session_date"},
            "I2 receipt pin",
        )
        return cls(
            session_date=_date_from_json(item["session_date"], "I2 receipt session"),
            receipt_id=_text(item["receipt_id"], "I2 receipt ID"),
            artifact=_artifact_from_dict(item["artifact"], "I2 receipt artifact"),
            receipt_available_session=_date_from_json(
                item["receipt_available_session"], "I2 receipt availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionI2BaseFrontierPin:
    """Exact authenticated I2/S4 base-frontier summary for production I3 base."""

    terminal_session: date
    frontier_id: str
    artifact: ArtifactPin
    frontier_available_session: date

    def __post_init__(self) -> None:
        _session(self.terminal_session, "I2 base-frontier terminal session")
        _digest(self.frontier_id, "I2 base-frontier ID")
        _artifact(self.artifact, "I2 base-frontier artifact")
        _session(self.frontier_available_session, "I2 base-frontier availability")
        if self.frontier_available_session < self.terminal_session:
            raise I3ProductionContractError(
                "I2 base-frontier availability precedes its terminal session"
            )
        if _fixture_path(self.artifact.path):
            raise I3ProductionContractError(
                "fixture I2 base frontier cannot acquire production authority"
            )
        if _temporary_path(self.artifact.path):
            raise I3ProductionContractError(
                "temporary I2 base frontier cannot acquire production authority"
            )
        expected_path = (
            "manifests/silver/incremental/s4/assets/base-frontiers/"
            f"frontier_id={self.frontier_id}/manifest.json"
        )
        if self.artifact.path != expected_path:
            raise I3ProductionContractError("I2 base-frontier artifact path is not canonical")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "frontier_available_session": self.frontier_available_session.isoformat(),
            "frontier_id": self.frontier_id,
            "terminal_session": self.terminal_session.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "frontier_available_session",
                "frontier_id",
                "terminal_session",
            },
            "I2 base-frontier pin",
        )
        return cls(
            terminal_session=_date_from_json(item["terminal_session"], "I2 base-frontier terminal"),
            frontier_id=_text(item["frontier_id"], "I2 base-frontier ID"),
            artifact=_artifact_from_dict(item["artifact"], "I2 base-frontier artifact"),
            frontier_available_session=_date_from_json(
                item["frontier_available_session"], "I2 base-frontier availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionV2ContractPin:
    table_name: str
    contract_id: str
    schema_digest: str
    resource_sha256: str

    def __post_init__(self) -> None:
        if self.table_name not in I3_V2_TABLE_ORDER:
            raise I3ProductionContractError("v2 contract table is invalid")
        expected = (
            I3_V2_CONTRACT_ID_BY_TABLE[self.table_name],
            I3_V2_CONTRACTS[self.table_name].schema_digest,
            I3_V2_RESOURCE_SHA256_BY_TABLE[self.table_name],
        )
        if (self.contract_id, self.schema_digest, self.resource_sha256) != expected:
            raise I3ProductionContractError("v2 contract pin differs from packaged schema")

    def to_dict(self) -> dict[str, str]:
        return {
            "contract_id": self.contract_id,
            "resource_sha256": self.resource_sha256,
            "schema_digest": self.schema_digest,
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {"contract_id", "resource_sha256", "schema_digest", "table_name"},
            "v2 contract pin",
        )
        return cls(**{key: _text(item[key], key) for key in item})


def production_v2_contract_pins() -> tuple[I3ProductionV2ContractPin, ...]:
    return tuple(
        I3ProductionV2ContractPin(
            table_name=table,
            contract_id=I3_V2_CONTRACT_ID_BY_TABLE[table],
            schema_digest=I3_V2_CONTRACTS[table].schema_digest,
            resource_sha256=I3_V2_RESOURCE_SHA256_BY_TABLE[table],
        )
        for table in I3_V2_TABLE_ORDER
    )


def production_gate_a_input_pins(
    run_spec: I3ProductionRunSpec,
) -> tuple[ArtifactPin, ...]:
    """Project the production envelope to Gate-A's exact canonical input set."""

    if not isinstance(run_spec, I3ProductionRunSpec):
        raise I3ProductionContractError("Gate-A input projection requires a production RunSpec")
    pins = [
        run_spec.i0_oracle.artifact,
        run_spec.s4_v1_source.artifact,
        run_spec.identity_policy_bundle_artifact,
        run_spec.calendar.artifact,
        *(item.artifact for item in run_spec.identity_policy_bundle.registry_releases),
        *(item.artifact for item in run_spec.i2_receipts),
        *(() if run_spec.i2_base_frontier is None else (run_spec.i2_base_frontier.artifact,)),
    ]
    if run_spec.run_kind is I3ProductionRunKind.DELTA:
        if (
            run_spec.parent_release is None
            or run_spec.parent_checkpoint_artifact is None
            or run_spec.parent_gate_a_manifest is None
            or run_spec.parent_shadow_completion_artifact is None
            or run_spec.parent_deep_attestation_artifact is None
        ):
            raise I3ProductionContractError("delta Gate-A input projection lacks its parent")
        pins.extend(
            (
                run_spec.parent_release.manifest,
                run_spec.parent_checkpoint_artifact,
                _artifact_from_manifest_pin(run_spec.parent_gate_a_manifest),
                run_spec.parent_shadow_completion_artifact,
                run_spec.parent_deep_attestation_artifact,
            )
        )
        if run_spec.parent_pointer_event_artifact is not None:
            pins.append(run_spec.parent_pointer_event_artifact)
    paths = [item.path for item in pins]
    if len(paths) != len(set(paths)):
        raise I3ProductionContractError("Gate-A input projection contains duplicate paths")
    return tuple(sorted(pins, key=lambda item: item.path))


@dataclass(frozen=True, slots=True)
class I3ProductionPartitionPin:
    """One immutable universe_daily session partition."""

    session_date: date
    partition_receipt_id: str
    artifact: ArtifactPin
    row_count: int
    contract_id: str
    schema_digest: str
    availability_session: date

    def __post_init__(self) -> None:
        _session(self.session_date, "output partition session")
        _digest(self.partition_receipt_id, "output partition receipt ID")
        _artifact(self.artifact, "output partition artifact")
        if not self.artifact.path.endswith(".parquet"):
            raise I3ProductionContractError("universe partition must be a Parquet artifact")
        _nonnegative_int(self.row_count, "output partition row count")
        contract = I3_V2_CONTRACTS["universe_daily"]
        if self.contract_id != contract.contract_id or self.schema_digest != contract.schema_digest:
            raise I3ProductionContractError("universe partition binds the wrong v2 contract")
        _session(self.availability_session, "output partition availability")
        if self.availability_session < self.session_date:
            raise I3ProductionContractError("output partition availability precedes its session")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "availability_session": self.availability_session.isoformat(),
            "contract_id": self.contract_id,
            "partition_receipt_id": self.partition_receipt_id,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": self.session_date.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "availability_session",
                "contract_id",
                "partition_receipt_id",
                "row_count",
                "schema_digest",
                "session_date",
            },
            "output partition pin",
        )
        return cls(
            session_date=_date_from_json(item["session_date"], "output partition session"),
            partition_receipt_id=_text(item["partition_receipt_id"], "output partition receipt ID"),
            artifact=_artifact_from_dict(item["artifact"], "output partition artifact"),
            row_count=_integer(item["row_count"], "output partition row count"),
            contract_id=_text(item["contract_id"], "output partition contract ID"),
            schema_digest=_text(item["schema_digest"], "output partition schema digest"),
            availability_session=_date_from_json(
                item["availability_session"], "output partition availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionDatasetIndex:
    table_name: str
    terminal_session: date
    partitions: tuple[I3ProductionPartitionPin, ...]

    def __post_init__(self) -> None:
        if self.table_name != "universe_daily":
            raise I3ProductionContractError("only universe_daily may use the dataset index")
        _session(self.terminal_session, "dataset-index terminal session")
        if type(self.partitions) is not tuple or not self.partitions:
            raise I3ProductionContractError("dataset index requires a typed nonempty partition set")
        if not all(type(item) is I3ProductionPartitionPin for item in self.partitions):
            raise I3ProductionContractError("dataset index contains an invalid partition")
        sessions = tuple(item.session_date for item in self.partitions)
        if sessions != tuple(sorted(set(sessions))) or sessions[-1] != self.terminal_session:
            raise I3ProductionContractError(
                "dataset-index partitions must be sorted, unique, and terminal-complete"
            )
        _unique_paths(tuple(item.artifact for item in self.partitions), "dataset partitions")

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.partitions)

    @property
    def dataset_index_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "contract_id": I3_V2_CONTRACTS[self.table_name].contract_id,
            "partitions": [item.to_dict() for item in self.partitions],
            "row_count": self.row_count,
            "rule_version": I3_PRODUCTION_DATASET_INDEX_RULE_VERSION,
            "schema_digest": I3_V2_CONTRACTS[self.table_name].schema_digest,
            "table_name": self.table_name,
            "terminal_session": self.terminal_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"dataset_index_id": self.dataset_index_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "contract_id",
                "dataset_index_id",
                "partitions",
                "row_count",
                "rule_version",
                "schema_digest",
                "table_name",
                "terminal_session",
            },
            "dataset index",
        )
        _literal(item["rule_version"], I3_PRODUCTION_DATASET_INDEX_RULE_VERSION, "index rule")
        result = cls(
            table_name=_text(item["table_name"], "dataset-index table"),
            terminal_session=_date_from_json(
                item["terminal_session"], "dataset-index terminal session"
            ),
            partitions=tuple(
                I3ProductionPartitionPin.from_dict(entry)
                for entry in _array(item["partitions"], "dataset partitions")
            ),
        )
        if item["contract_id"] != I3_V2_CONTRACTS[result.table_name].contract_id:
            raise I3ProductionContractError("dataset-index contract ID differs")
        if item["schema_digest"] != I3_V2_CONTRACTS[result.table_name].schema_digest:
            raise I3ProductionContractError("dataset-index schema digest differs")
        if item["row_count"] != result.row_count:
            raise I3ProductionContractError("dataset-index row count does not reproduce")
        if item["dataset_index_id"] != result.dataset_index_id:
            raise I3ProductionContractError("dataset-index ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionBaseFkVerificationSummary:
    """One compact receipt for direct FK replay across the historical base."""

    session_count: int
    rows_checked: int
    input_partition_set_digest: str
    logical_reference_digest: str
    summary_available_session: date

    def __post_init__(self) -> None:
        _positive_int(self.session_count, "base FK summary session count")
        _nonnegative_int(self.rows_checked, "base FK summary rows checked")
        _digest(self.input_partition_set_digest, "base FK input partition-set digest")
        _digest(self.logical_reference_digest, "base FK logical reference digest")
        _session(self.summary_available_session, "base FK summary availability")

    @property
    def summary_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "input_partition_set_digest": self.input_partition_set_digest,
            "logical_reference_digest": self.logical_reference_digest,
            "rows_checked": self.rows_checked,
            "rule_version": I3_PRODUCTION_BASE_FK_SUMMARY_RULE_VERSION,
            "session_count": self.session_count,
            "summary_available_session": self.summary_available_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"summary_id": self.summary_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "input_partition_set_digest",
                "logical_reference_digest",
                "rows_checked",
                "rule_version",
                "session_count",
                "summary_available_session",
                "summary_id",
            },
            "base FK verification summary",
        )
        _literal(
            item["rule_version"],
            I3_PRODUCTION_BASE_FK_SUMMARY_RULE_VERSION,
            "base FK summary rule",
        )
        result = cls(
            session_count=_integer(item["session_count"], "base FK session count"),
            rows_checked=_integer(item["rows_checked"], "base FK rows checked"),
            input_partition_set_digest=_text(
                item["input_partition_set_digest"], "base FK partition-set digest"
            ),
            logical_reference_digest=_text(
                item["logical_reference_digest"], "base FK logical reference digest"
            ),
            summary_available_session=_date_from_json(
                item["summary_available_session"], "base FK summary availability"
            ),
        )
        if item["summary_id"] != result.summary_id:
            raise I3ProductionContractError("base FK summary ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionSegmentPin:
    """One immutable Parquet segment in a compact versioned-table rowset."""

    table_name: str
    segment_id: str
    artifact: ArtifactPin
    row_count: int
    contract_id: str
    schema_digest: str
    availability_session: date

    def __post_init__(self) -> None:
        if self.table_name not in _VERSIONED_TABLES:
            raise I3ProductionContractError("rowset segment table is invalid")
        _digest(self.segment_id, "rowset segment ID")
        _artifact(self.artifact, "rowset segment artifact")
        if not self.artifact.path.endswith(".parquet"):
            raise I3ProductionContractError("rowset segment must be Parquet")
        _nonnegative_int(self.row_count, "rowset segment row count")
        contract = I3_V2_CONTRACTS[self.table_name]
        if self.contract_id != contract.contract_id or self.schema_digest != contract.schema_digest:
            raise I3ProductionContractError("rowset segment binds the wrong v2 contract")
        _session(self.availability_session, "rowset segment availability")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "availability_session": self.availability_session.isoformat(),
            "contract_id": self.contract_id,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "segment_id": self.segment_id,
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact",
                "availability_session",
                "contract_id",
                "row_count",
                "schema_digest",
                "segment_id",
                "table_name",
            },
            "rowset segment pin",
        )
        return cls(
            table_name=_text(item["table_name"], "rowset segment table"),
            segment_id=_text(item["segment_id"], "rowset segment ID"),
            artifact=_artifact_from_dict(item["artifact"], "rowset segment artifact"),
            row_count=_integer(item["row_count"], "rowset segment row count"),
            contract_id=_text(item["contract_id"], "rowset segment contract ID"),
            schema_digest=_text(item["schema_digest"], "rowset segment schema digest"),
            availability_session=_date_from_json(
                item["availability_session"], "rowset segment availability"
            ),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionRowsetIndex:
    """Append-only segment index for one small versioned table."""

    table_name: str
    terminal_session: date
    segments: tuple[I3ProductionSegmentPin, ...]

    def __post_init__(self) -> None:
        if self.table_name not in _VERSIONED_TABLES:
            raise I3ProductionContractError("rowset-index table is invalid")
        _session(self.terminal_session, "rowset-index terminal session")
        if type(self.segments) is not tuple or not self.segments:
            raise I3ProductionContractError("rowset index requires nonempty segments")
        if not all(
            type(item) is I3ProductionSegmentPin and item.table_name == self.table_name
            for item in self.segments
        ):
            raise I3ProductionContractError("rowset index contains another table")
        segment_ids = tuple(item.segment_id for item in self.segments)
        if len(segment_ids) != len(set(segment_ids)):
            raise I3ProductionContractError("rowset segment IDs must be unique")
        _unique_paths(tuple(item.artifact for item in self.segments), "rowset segments")

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.segments)

    @property
    def rowset_index_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        contract = I3_V2_CONTRACTS[self.table_name]
        return {
            "contract_id": contract.contract_id,
            "row_count": self.row_count,
            "rule_version": I3_PRODUCTION_ROWSET_INDEX_RULE_VERSION,
            "schema_digest": contract.schema_digest,
            "segments": [item.to_dict() for item in self.segments],
            "table_name": self.table_name,
            "terminal_session": self.terminal_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"rowset_index_id": self.rowset_index_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "contract_id",
                "row_count",
                "rowset_index_id",
                "rule_version",
                "schema_digest",
                "segments",
                "table_name",
                "terminal_session",
            },
            "rowset index",
        )
        _literal(item["rule_version"], I3_PRODUCTION_ROWSET_INDEX_RULE_VERSION, "rowset rule")
        result = cls(
            table_name=_text(item["table_name"], "rowset-index table"),
            terminal_session=_date_from_json(
                item["terminal_session"], "rowset-index terminal session"
            ),
            segments=tuple(
                I3ProductionSegmentPin.from_dict(entry)
                for entry in _array(item["segments"], "rowset segments")
            ),
        )
        contract = I3_V2_CONTRACTS[result.table_name]
        if (
            item["contract_id"] != contract.contract_id
            or item["schema_digest"] != contract.schema_digest
            or item["row_count"] != result.row_count
            or item["rowset_index_id"] != result.rowset_index_id
        ):
            raise I3ProductionContractError("rowset index does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionTableOutput:
    storage: I3ProductionOutputStorage
    manifest_output: NativeV2OutputArtifact
    dataset_index: I3ProductionDatasetIndex | None = None
    rowset_index: I3ProductionRowsetIndex | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.storage, I3ProductionOutputStorage):
            raise I3ProductionContractError("production output storage is invalid")
        if type(self.manifest_output) is not NativeV2OutputArtifact:
            raise I3ProductionContractError("production table output is not typed")
        table = self.manifest_output.table_name
        if table == "universe_daily":
            if self.storage is not I3ProductionOutputStorage.DATASET_INDEX:
                raise I3ProductionContractError("universe_daily must use a dataset index")
            if not isinstance(self.dataset_index, I3ProductionDatasetIndex):
                raise I3ProductionContractError("universe_daily dataset index is missing")
            if not self.manifest_output.artifact.path.endswith(".json"):
                raise I3ProductionContractError("universe dataset index must be JSON")
            if (
                self.dataset_index.terminal_session != self.manifest_output.session_date
                or self.dataset_index.row_count != self.manifest_output.row_count
                or self.dataset_index.exact_pin(path=self.manifest_output.artifact.path)
                != self.manifest_output.artifact
            ):
                raise I3ProductionContractError("universe manifest output differs from its index")
            if self.rowset_index is not None:
                raise I3ProductionContractError("universe output cannot carry a rowset index")
        elif table not in _VERSIONED_TABLES or self.dataset_index is not None:
            raise I3ProductionContractError("versioned master output role is invalid")
        elif self.storage is I3ProductionOutputStorage.PARQUET:
            if self.rowset_index is not None or not self.manifest_output.artifact.path.endswith(
                ".parquet"
            ):
                raise I3ProductionContractError("versioned Parquet output is invalid")
        elif self.storage is I3ProductionOutputStorage.ROWSET_INDEX:
            if not isinstance(self.rowset_index, I3ProductionRowsetIndex):
                raise I3ProductionContractError("versioned rowset index is missing")
            if (
                self.rowset_index.table_name != table
                or self.rowset_index.terminal_session != self.manifest_output.session_date
                or self.rowset_index.row_count != self.manifest_output.row_count
                or self.rowset_index.exact_pin(path=self.manifest_output.artifact.path)
                != self.manifest_output.artifact
            ):
                raise I3ProductionContractError("versioned manifest output differs from rowset")
        else:
            raise I3ProductionContractError(
                "versioned master output must be Parquet or rowset index"
            )

    @property
    def table_name(self) -> str:
        return self.manifest_output.table_name

    @property
    def table_output_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "dataset_index": self.dataset_index.to_dict() if self.dataset_index else None,
            "manifest_output": self.manifest_output.to_dict(),
            "rowset_index": self.rowset_index.to_dict() if self.rowset_index else None,
            "storage": self.storage.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"table_output_id": self.table_output_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "dataset_index",
                "manifest_output",
                "rowset_index",
                "storage",
                "table_output_id",
            },
            "production table output",
        )
        try:
            storage = I3ProductionOutputStorage(_text(item["storage"], "output storage"))
        except ValueError as exc:
            raise I3ProductionContractError("production output storage is invalid") from exc
        result = cls(
            storage=storage,
            manifest_output=NativeV2OutputArtifact.from_dict(item["manifest_output"]),
            dataset_index=(
                None
                if item["dataset_index"] is None
                else I3ProductionDatasetIndex.from_dict(item["dataset_index"])
            ),
            rowset_index=(
                None
                if item["rowset_index"] is None
                else I3ProductionRowsetIndex.from_dict(item["rowset_index"])
            ),
        )
        if item["table_output_id"] != result.table_output_id:
            raise I3ProductionContractError("production table-output ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionOutputSet:
    release_manifest_artifact: ArtifactPin
    checkpoint_artifact: ArtifactPin
    release_id: str
    checkpoint_id: str
    resolved_state_digest: str
    resolved_content_digest: str
    table_outputs: tuple[I3ProductionTableOutput, ...]
    gate_a_run_spec_pin: ControlObjectPin
    gate_a_run_receipt_pin: ControlObjectPin
    gate_a_manifest_pin: ManifestPin
    control_extension_artifacts: tuple[ArtifactPin, ...]

    def __post_init__(self) -> None:
        _artifact(self.release_manifest_artifact, "production release manifest")
        _artifact(self.checkpoint_artifact, "production checkpoint")
        if (
            not isinstance(self.gate_a_run_spec_pin, ControlObjectPin)
            or self.gate_a_run_spec_pin.object_kind is not ControlObjectKind.RUN_SPEC
            or not isinstance(self.gate_a_run_receipt_pin, ControlObjectPin)
            or self.gate_a_run_receipt_pin.object_kind is not ControlObjectKind.RUN_RECEIPT
            or not isinstance(self.gate_a_manifest_pin, ManifestPin)
        ):
            raise I3ProductionContractError("Gate-A control pins are incomplete")
        for value, label in (
            (self.release_id, "production release ID"),
            (self.checkpoint_id, "production checkpoint ID"),
            (self.resolved_state_digest, "production resolved-state digest"),
            (self.resolved_content_digest, "production resolved-content digest"),
        ):
            _digest(value, label)
        if (
            type(self.table_outputs) is not tuple
            or tuple(item.table_name for item in self.table_outputs) != I3_V2_TABLE_ORDER
        ):
            raise I3ProductionContractError("output set must use the exact four-table order")
        if not all(type(item) is I3ProductionTableOutput for item in self.table_outputs):
            raise I3ProductionContractError("output set contains an invalid table output")
        if (
            type(self.control_extension_artifacts) is not tuple
            or not all(type(item) is ArtifactPin for item in self.control_extension_artifacts)
            or tuple(item.path for item in self.control_extension_artifacts)
            != tuple(sorted({item.path for item in self.control_extension_artifacts}))
        ):
            raise I3ProductionContractError(
                "control extension artifacts must be sorted exact unique pins"
            )
        artifacts = [
            self.release_manifest_artifact,
            self.checkpoint_artifact,
            self.gate_a_run_spec_pin.artifact,
            self.gate_a_run_receipt_pin.artifact,
            _artifact_from_manifest_pin(self.gate_a_manifest_pin),
            *self.control_extension_artifacts,
        ]
        for item in self.table_outputs:
            artifacts.append(item.manifest_output.artifact)
            if item.dataset_index:
                artifacts.extend(part.artifact for part in item.dataset_index.partitions)
            if item.rowset_index:
                artifacts.extend(segment.artifact for segment in item.rowset_index.segments)
        _unique_paths(tuple(artifacts), "production output-set artifacts")
        if any(_fixture_path(item.path) for item in artifacts):
            raise I3ProductionContractError("fixture artifact cannot enter a production output set")
        if any(_temporary_path(item.path) for item in artifacts):
            raise I3ProductionContractError(
                "temporary artifact cannot enter a production output set"
            )

    @property
    def output_set_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def total_output_bytes(self) -> int:
        total = (
            self.release_manifest_artifact.bytes
            + self.checkpoint_artifact.bytes
            + self.gate_a_run_spec_pin.artifact.bytes
            + self.gate_a_run_receipt_pin.artifact.bytes
            + self.gate_a_manifest_pin.manifest_bytes
            + sum(item.bytes for item in self.control_extension_artifacts)
        )
        for item in self.table_outputs:
            total += item.manifest_output.artifact.bytes
            if item.dataset_index is not None:
                total += sum(part.artifact.bytes for part in item.dataset_index.partitions)
            if item.rowset_index is not None:
                total += sum(segment.artifact.bytes for segment in item.rowset_index.segments)
        return total

    @property
    def total_rows(self) -> int:
        return sum(item.manifest_output.row_count for item in self.table_outputs)

    def logical_payload(self) -> dict[str, object]:
        return {
            "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
            "checkpoint_id": self.checkpoint_id,
            "gate_a_manifest_pin": self.gate_a_manifest_pin.to_dict(),
            "gate_a_run_receipt_pin": self.gate_a_run_receipt_pin.to_dict(),
            "gate_a_run_spec_pin": self.gate_a_run_spec_pin.to_dict(),
            "control_extension_artifacts": [
                item.to_dict() for item in self.control_extension_artifacts
            ],
            "release_id": self.release_id,
            "release_manifest_artifact": self.release_manifest_artifact.to_dict(),
            "resolved_state_digest": self.resolved_state_digest,
            "resolved_content_digest": self.resolved_content_digest,
            "rule_version": I3_PRODUCTION_OUTPUT_SET_RULE_VERSION,
            "table_outputs": [item.to_dict() for item in self.table_outputs],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "output_set_id": self.output_set_id,
            "total_output_bytes": self.total_output_bytes,
            "total_rows": self.total_rows,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "checkpoint_artifact",
                "checkpoint_id",
                "gate_a_manifest_pin",
                "gate_a_run_receipt_pin",
                "gate_a_run_spec_pin",
                "control_extension_artifacts",
                "output_set_id",
                "release_id",
                "release_manifest_artifact",
                "resolved_state_digest",
                "resolved_content_digest",
                "rule_version",
                "table_outputs",
                "total_output_bytes",
                "total_rows",
            },
            "production output set",
        )
        _literal(item["rule_version"], I3_PRODUCTION_OUTPUT_SET_RULE_VERSION, "output-set rule")
        result = cls(
            release_manifest_artifact=_artifact_from_dict(
                item["release_manifest_artifact"], "production release manifest"
            ),
            checkpoint_artifact=_artifact_from_dict(
                item["checkpoint_artifact"], "production checkpoint"
            ),
            release_id=_text(item["release_id"], "production release ID"),
            checkpoint_id=_text(item["checkpoint_id"], "production checkpoint ID"),
            resolved_state_digest=_text(
                item["resolved_state_digest"], "production resolved-state digest"
            ),
            resolved_content_digest=_text(
                item["resolved_content_digest"], "production resolved-content digest"
            ),
            gate_a_run_spec_pin=_control_object_pin_from_dict(
                item["gate_a_run_spec_pin"], ControlObjectKind.RUN_SPEC
            ),
            gate_a_run_receipt_pin=_control_object_pin_from_dict(
                item["gate_a_run_receipt_pin"], ControlObjectKind.RUN_RECEIPT
            ),
            gate_a_manifest_pin=_manifest_pin_from_dict(item["gate_a_manifest_pin"]),
            control_extension_artifacts=tuple(
                _artifact_from_dict(entry, "control extension artifact")
                for entry in _array(
                    item["control_extension_artifacts"],
                    "control extension artifacts",
                )
            ),
            table_outputs=tuple(
                I3ProductionTableOutput.from_dict(entry)
                for entry in _array(item["table_outputs"], "production table outputs")
            ),
        )
        if item["output_set_id"] != result.output_set_id:
            raise I3ProductionContractError("production output-set ID does not reproduce")
        if item["total_output_bytes"] != result.total_output_bytes:
            raise I3ProductionContractError("production output byte total does not reproduce")
        if item["total_rows"] != result.total_rows:
            raise I3ProductionContractError("production output row total does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionRunSpec:
    run_kind: I3ProductionRunKind
    terminal_session: date
    source_cutoff_session: date
    run_available_session: date
    native_v2_migration_id: str
    transform_semantics_digest: str
    i0_oracle: I3ProductionDependencyPin
    s4_v1_source: I3ProductionDependencyPin
    identity_policy_bundle: IdentityPolicyBundle
    identity_policy_bundle_artifact: ArtifactPin
    calendar: I3ProductionCalendarPin
    v2_contracts: tuple[I3ProductionV2ContractPin, ...]
    i2_receipts: tuple[I3ProductionI2ReceiptPin, ...]
    resource_caps: I3ProductionResourceCaps
    i2_base_frontier: I3ProductionI2BaseFrontierPin | None = None
    parent_release: NativeV2ParentReleasePin | None = None
    parent_checkpoint_artifact: ArtifactPin | None = None
    parent_gate_a_manifest: ManifestPin | None = None
    parent_shadow_completion_artifact: ArtifactPin | None = None
    parent_deep_attestation_artifact: ArtifactPin | None = None
    parent_authority: I3ProductionParentAuthority | None = None
    parent_pointer_event_artifact: ArtifactPin | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.run_kind, I3ProductionRunKind):
            raise I3ProductionContractError("production run kind is invalid")
        terminal = _session(self.terminal_session, "production terminal session")
        source = _session(self.source_cutoff_session, "production source cutoff")
        available = _session(self.run_available_session, "production run availability")
        if not terminal <= source <= available:
            raise I3ProductionContractError("production session cutoffs are not ordered")
        for value, label in (
            (self.native_v2_migration_id, "native-v2 migration ID"),
            (self.transform_semantics_digest, "transform semantics digest"),
        ):
            _digest(value, label)
        if self.transform_semantics_digest != I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST:
            raise I3ProductionContractError(
                "production transform semantics must be the module-owned rule bundle"
            )
        if (
            not isinstance(self.i0_oracle, I3ProductionDependencyPin)
            or self.i0_oracle.role is not I3ProductionDependencyRole.I0_V1_ORACLE
            or not isinstance(self.s4_v1_source, I3ProductionDependencyPin)
            or self.s4_v1_source.role is not I3ProductionDependencyRole.S4_V1_SOURCE
        ):
            raise I3ProductionContractError("production source dependency roles are incomplete")
        if not isinstance(self.identity_policy_bundle, IdentityPolicyBundle):
            raise I3ProductionContractError("production identity policy bundle is invalid")
        _artifact(self.identity_policy_bundle_artifact, "identity policy bundle artifact")
        if (
            self.identity_policy_bundle.exact_pin(path=self.identity_policy_bundle_artifact.path)
            != self.identity_policy_bundle_artifact
        ):
            raise I3ProductionContractError("identity policy artifact does not reproduce")
        if not isinstance(self.calendar, I3ProductionCalendarPin):
            raise I3ProductionContractError("production calendar pin is invalid")
        control_artifacts = [
            self.identity_policy_bundle_artifact,
            self.calendar.artifact,
            *(item.artifact for item in self.identity_policy_bundle.registry_releases),
        ]
        if any(_fixture_path(item.path) for item in control_artifacts):
            raise I3ProductionContractError(
                "fixture control artifact cannot acquire production authority"
            )
        if any(_temporary_path(item.path) for item in control_artifacts):
            raise I3ProductionContractError(
                "temporary control artifact cannot acquire production authority"
            )
        if self.v2_contracts != production_v2_contract_pins():
            raise I3ProductionContractError("production run must bind the exact v2 schema bundle")
        if type(self.i2_receipts) is not tuple:
            raise I3ProductionContractError("production I2 receipts must be a typed tuple")
        if not all(type(item) is I3ProductionI2ReceiptPin for item in self.i2_receipts):
            raise I3ProductionContractError("production I2 receipt set contains an invalid item")
        sessions = tuple(item.session_date for item in self.i2_receipts)
        if sessions != tuple(sorted(set(sessions))):
            raise I3ProductionContractError("I2 receipts must be sorted and unique")
        if not isinstance(self.resource_caps, I3ProductionResourceCaps):
            raise I3ProductionContractError("production resource caps are invalid")
        availability_floor = max(
            terminal,
            self.i0_oracle.available_session,
            self.s4_v1_source.available_session,
            self.identity_policy_bundle.bundle_available_session,
            self.calendar.available_session,
            *(item.receipt_available_session for item in self.i2_receipts),
            *(
                ()
                if self.i2_base_frontier is None
                else (self.i2_base_frontier.frontier_available_session,)
            ),
        )
        if available < availability_floor:
            raise I3ProductionContractError("production run availability precedes an exact input")
        if self.run_kind is I3ProductionRunKind.BASE:
            if (
                not isinstance(self.i2_base_frontier, I3ProductionI2BaseFrontierPin)
                or self.i2_base_frontier.terminal_session != terminal
                or self.i2_receipts
            ):
                raise I3ProductionContractError(
                    "production base requires only its exact I2 base frontier"
                )
            expected_migration_id = production_native_v2_migration_id(
                i0_release_set_artifact=self.i0_oracle.artifact,
                s4_release_set_artifact=self.s4_v1_source.artifact,
                identity_policy_bundle=self.identity_policy_bundle,
                identity_policy_bundle_artifact=self.identity_policy_bundle_artifact,
                calendar_artifact=self.calendar.artifact,
                i2_base_frontier_artifact=self.i2_base_frontier.artifact,
            )
            if self.native_v2_migration_id != expected_migration_id:
                raise I3ProductionContractError(
                    "native-v2 migration ID does not reproduce from exact base inputs"
                )
            if any(
                value is not None
                for value in (
                    self.parent_release,
                    self.parent_checkpoint_artifact,
                    self.parent_gate_a_manifest,
                    self.parent_shadow_completion_artifact,
                    self.parent_deep_attestation_artifact,
                    self.parent_authority,
                    self.parent_pointer_event_artifact,
                )
            ):
                raise I3ProductionContractError("production base cannot carry a native-v2 parent")
        else:
            if (
                self.i2_base_frontier is not None
                or len(self.i2_receipts) != 1
                or self.i2_receipts[0].session_date != terminal
            ):
                raise I3ProductionContractError(
                    "production delta requires only the latest authenticated I2 receipt"
                )
            if (
                not isinstance(self.parent_release, NativeV2ParentReleasePin)
                or self.parent_release.release_family != NATIVE_V2_RELEASE_FAMILY
                or self.parent_checkpoint_artifact is None
                or not isinstance(self.parent_gate_a_manifest, ManifestPin)
                or self.parent_shadow_completion_artifact is None
                or self.parent_deep_attestation_artifact is None
                or not isinstance(self.parent_authority, I3ProductionParentAuthority)
            ):
                raise I3ProductionContractError(
                    "production delta requires exact physical and Gate-A parent authority"
                )
            _artifact(self.parent_checkpoint_artifact, "production parent checkpoint")
            _artifact(
                self.parent_shadow_completion_artifact,
                "production parent shadow completion",
            )
            _artifact(
                self.parent_deep_attestation_artifact,
                "production parent deep attestation",
            )
            if self.parent_authority in {
                I3ProductionParentAuthority.EXACT_STAGING,
                I3ProductionParentAuthority.MIGRATION_SHADOW,
            }:
                if self.parent_pointer_event_artifact is not None:
                    raise I3ProductionContractError(
                        "exact staging parent cannot claim a pointer event"
                    )
            else:
                raise I3ProductionContractError("production parent authority is unsupported")
            if (
                _fixture_path(self.parent_release.manifest.path)
                or _fixture_path(self.parent_checkpoint_artifact.path)
                or _fixture_path(self.parent_gate_a_manifest.manifest_path)
                or _fixture_path(self.parent_shadow_completion_artifact.path)
                or _fixture_path(self.parent_deep_attestation_artifact.path)
                or (
                    self.parent_pointer_event_artifact is not None
                    and _fixture_path(self.parent_pointer_event_artifact.path)
                )
            ):
                raise I3ProductionContractError(
                    "fixture parent artifact cannot acquire production authority"
                )
            if (
                _temporary_path(self.parent_release.manifest.path)
                or _temporary_path(self.parent_checkpoint_artifact.path)
                or _temporary_path(self.parent_gate_a_manifest.manifest_path)
                or _temporary_path(self.parent_shadow_completion_artifact.path)
                or _temporary_path(self.parent_deep_attestation_artifact.path)
                or (
                    self.parent_pointer_event_artifact is not None
                    and _temporary_path(self.parent_pointer_event_artifact.path)
                )
            ):
                raise I3ProductionContractError(
                    "temporary parent artifact cannot acquire production authority"
                )
            if self.parent_release.terminal_session >= terminal:
                raise I3ProductionContractError("production delta does not advance its parent")
            if (
                self.native_v2_migration_id != self.parent_release.native_v2_migration_id
                or self.transform_semantics_digest != self.parent_release.transform_semantics_digest
            ):
                raise I3ProductionContractError(
                    "production delta changed its parent's migration semantics"
                )

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "calendar": self.calendar.to_dict(),
            "i0_oracle": self.i0_oracle.to_dict(),
            "i2_receipts": [item.to_dict() for item in self.i2_receipts],
            "i2_base_frontier": (
                None if self.i2_base_frontier is None else self.i2_base_frontier.to_dict()
            ),
            "identity_policy_bundle": self.identity_policy_bundle.to_dict(),
            "identity_policy_bundle_artifact": self.identity_policy_bundle_artifact.to_dict(),
            "native_v2_migration_id": self.native_v2_migration_id,
            "namespace": I3_PRODUCTION_NAMESPACE,
            "parent_checkpoint_artifact": (
                self.parent_checkpoint_artifact.to_dict()
                if self.parent_checkpoint_artifact is not None
                else None
            ),
            "parent_gate_a_manifest": (
                self.parent_gate_a_manifest.to_dict()
                if self.parent_gate_a_manifest is not None
                else None
            ),
            "parent_release": self.parent_release.to_dict() if self.parent_release else None,
            "parent_authority": (
                None if self.parent_authority is None else self.parent_authority.value
            ),
            "parent_deep_attestation_artifact": (
                None
                if self.parent_deep_attestation_artifact is None
                else self.parent_deep_attestation_artifact.to_dict()
            ),
            "parent_pointer_event_artifact": (
                None
                if self.parent_pointer_event_artifact is None
                else self.parent_pointer_event_artifact.to_dict()
            ),
            "parent_shadow_completion_artifact": (
                self.parent_shadow_completion_artifact.to_dict()
                if self.parent_shadow_completion_artifact is not None
                else None
            ),
            "resource_caps": self.resource_caps.to_dict(),
            "rule_version": (
                I3_PRODUCTION_DELTA_RUN_SPEC_RULE_VERSION
                if self.run_kind is I3ProductionRunKind.DELTA
                else I3_PRODUCTION_RUN_SPEC_RULE_VERSION
            ),
            "run_available_session": self.run_available_session.isoformat(),
            "run_kind": self.run_kind.value,
            "s4_v1_source": self.s4_v1_source.to_dict(),
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "source_cutoff_session": self.source_cutoff_session.isoformat(),
            "terminal_session": self.terminal_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
            "v2_contracts": [item.to_dict() for item in self.v2_contracts],
        }
        if self.run_kind is I3ProductionRunKind.DELTA:
            payload["delta_alias_availability_progression_rule_version"] = (
                I3_PRODUCTION_DELTA_ALIAS_AVAILABILITY_PROGRESSION_RULE_VERSION
            )
            payload["delta_source_version_projection_rule_version"] = (
                I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION
            )
        return payload

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        common_fields = {
            "calendar",
            "i0_oracle",
            "i2_receipts",
            "i2_base_frontier",
            "identity_policy_bundle",
            "identity_policy_bundle_artifact",
            "namespace",
            "native_v2_migration_id",
            "parent_checkpoint_artifact",
            "parent_gate_a_manifest",
            "parent_authority",
            "parent_deep_attestation_artifact",
            "parent_pointer_event_artifact",
            "parent_release",
            "parent_shadow_completion_artifact",
            "resource_caps",
            "rule_version",
            "run_available_session",
            "run_kind",
            "run_spec_id",
            "s4_v1_source",
            "schema_bundle_digest",
            "source_cutoff_session",
            "terminal_session",
            "transform_semantics_digest",
            "v2_contracts",
        }
        raw_run_kind = value.get("run_kind") if isinstance(value, Mapping) else None
        is_delta = raw_run_kind == I3ProductionRunKind.DELTA.value
        expected_fields = common_fields | (
            {
                "delta_alias_availability_progression_rule_version",
                "delta_source_version_projection_rule_version",
            }
            if is_delta
            else set()
        )
        item = _closed_mapping(
            value,
            expected_fields,
            "production run spec",
        )
        _literal(item["namespace"], I3_PRODUCTION_NAMESPACE, "production namespace")
        _literal(
            item["rule_version"],
            (
                I3_PRODUCTION_DELTA_RUN_SPEC_RULE_VERSION
                if is_delta
                else I3_PRODUCTION_RUN_SPEC_RULE_VERSION
            ),
            "run-spec rule",
        )
        _literal(item["schema_bundle_digest"], I3_V2_SCHEMA_BUNDLE_DIGEST, "v2 schema bundle")
        if is_delta:
            _literal(
                item["delta_alias_availability_progression_rule_version"],
                I3_PRODUCTION_DELTA_ALIAS_AVAILABILITY_PROGRESSION_RULE_VERSION,
                "DELTA alias-availability progression rule",
            )
            _literal(
                item["delta_source_version_projection_rule_version"],
                I3_PRODUCTION_DELTA_SOURCE_VERSION_PROJECTION_RULE_VERSION,
                "DELTA source-version projection rule",
            )
        try:
            run_kind = I3ProductionRunKind(_text(item["run_kind"], "production run kind"))
        except ValueError as exc:
            raise I3ProductionContractError("production run kind is invalid") from exc
        result = cls(
            run_kind=run_kind,
            terminal_session=_date_from_json(item["terminal_session"], "terminal session"),
            source_cutoff_session=_date_from_json(item["source_cutoff_session"], "source cutoff"),
            run_available_session=_date_from_json(
                item["run_available_session"], "run availability"
            ),
            native_v2_migration_id=_text(item["native_v2_migration_id"], "native-v2 migration ID"),
            transform_semantics_digest=_text(
                item["transform_semantics_digest"], "transform semantics digest"
            ),
            i0_oracle=I3ProductionDependencyPin.from_dict(item["i0_oracle"]),
            s4_v1_source=I3ProductionDependencyPin.from_dict(item["s4_v1_source"]),
            identity_policy_bundle=IdentityPolicyBundle.from_dict(item["identity_policy_bundle"]),
            identity_policy_bundle_artifact=_artifact_from_dict(
                item["identity_policy_bundle_artifact"], "identity policy artifact"
            ),
            calendar=I3ProductionCalendarPin.from_dict(item["calendar"]),
            v2_contracts=tuple(
                I3ProductionV2ContractPin.from_dict(entry)
                for entry in _array(item["v2_contracts"], "v2 contract pins")
            ),
            i2_receipts=tuple(
                I3ProductionI2ReceiptPin.from_dict(entry)
                for entry in _array(item["i2_receipts"], "I2 receipt pins")
            ),
            i2_base_frontier=(
                None
                if item["i2_base_frontier"] is None
                else I3ProductionI2BaseFrontierPin.from_dict(item["i2_base_frontier"])
            ),
            resource_caps=I3ProductionResourceCaps.from_dict(item["resource_caps"]),
            parent_release=(
                None
                if item["parent_release"] is None
                else NativeV2ParentReleasePin.from_dict(item["parent_release"])
            ),
            parent_checkpoint_artifact=(
                None
                if item["parent_checkpoint_artifact"] is None
                else _artifact_from_dict(
                    item["parent_checkpoint_artifact"], "parent checkpoint artifact"
                )
            ),
            parent_gate_a_manifest=(
                None
                if item["parent_gate_a_manifest"] is None
                else _manifest_pin_from_dict(item["parent_gate_a_manifest"])
            ),
            parent_deep_attestation_artifact=(
                None
                if item["parent_deep_attestation_artifact"] is None
                else _artifact_from_dict(
                    item["parent_deep_attestation_artifact"],
                    "parent deep-attestation artifact",
                )
            ),
            parent_authority=(
                None
                if item["parent_authority"] is None
                else _parent_authority_from_json(item["parent_authority"])
            ),
            parent_pointer_event_artifact=(
                None
                if item["parent_pointer_event_artifact"] is None
                else _artifact_from_dict(
                    item["parent_pointer_event_artifact"],
                    "parent pointer-event artifact",
                )
            ),
            parent_shadow_completion_artifact=(
                None
                if item["parent_shadow_completion_artifact"] is None
                else _artifact_from_dict(
                    item["parent_shadow_completion_artifact"],
                    "parent shadow completion artifact",
                )
            ),
        )
        if item["run_spec_id"] != result.run_spec_id:
            raise I3ProductionContractError("production run-spec ID does not reproduce")
        return result


def validate_production_compact_base_initial_rowsets(
    run_spec: I3ProductionRunSpec,
    table_outputs: tuple[I3ProductionTableOutput, ...],
) -> None:
    """Require the sealed compact-base storage shape and segment identity."""

    if not isinstance(run_spec, I3ProductionRunSpec):
        raise I3ProductionContractError("compact BASE validation requires a production RunSpec")
    if run_spec.run_kind is not I3ProductionRunKind.BASE:
        return
    if (
        type(table_outputs) is not tuple
        or tuple(item.table_name for item in table_outputs) != I3_V2_TABLE_ORDER
    ):
        raise I3ProductionContractError("compact BASE outputs differ from the four-table order")
    for output in table_outputs[:-1]:
        rowset = output.rowset_index
        if (
            output.storage is not I3ProductionOutputStorage.ROWSET_INDEX
            or rowset is None
            or len(rowset.segments) != 1
        ):
            raise I3ProductionContractError(
                "compact BASE versioned outputs require exactly one rowset segment"
            )
        segment = rowset.segments[0]
        expected_segment_id = production_compact_base_initial_segment_id(
            table_name=output.table_name,
            artifact=segment.artifact,
            terminal_session=run_spec.terminal_session,
            availability_session=run_spec.run_available_session,
            native_v2_migration_id=run_spec.native_v2_migration_id,
        )
        if (
            rowset.terminal_session != run_spec.terminal_session
            or segment.availability_session != run_spec.run_available_session
            or segment.segment_id != expected_segment_id
        ):
            raise I3ProductionContractError(
                "compact BASE initial rowset segment differs from module-owned identity"
            )


def validate_production_delta_append_outputs(
    run_spec: I3ProductionRunSpec,
    table_outputs: tuple[I3ProductionTableOutput, ...],
    parent_output_set: I3ProductionOutputSet,
) -> None:
    """Require one exact, module-owned append beyond an authenticated parent."""

    if not isinstance(run_spec, I3ProductionRunSpec):
        raise I3ProductionContractError("DELTA append validation requires a production RunSpec")
    if run_spec.run_kind is not I3ProductionRunKind.DELTA:
        return
    if not isinstance(parent_output_set, I3ProductionOutputSet):
        raise I3ProductionContractError("DELTA append validation requires a parent OutputSet")
    if (
        type(table_outputs) is not tuple
        or tuple(item.table_name for item in table_outputs) != I3_V2_TABLE_ORDER
        or tuple(item.table_name for item in parent_output_set.table_outputs) != I3_V2_TABLE_ORDER
    ):
        raise I3ProductionContractError("DELTA outputs differ from the four-table order")
    for parent_output, output in zip(
        parent_output_set.table_outputs[:-1], table_outputs[:-1], strict=True
    ):
        parent_rowset = parent_output.rowset_index
        rowset = output.rowset_index
        if (
            parent_rowset is None
            or output.storage is not I3ProductionOutputStorage.ROWSET_INDEX
            or rowset is None
            or len(rowset.segments) != len(parent_rowset.segments) + 1
            or rowset.segments[:-1] != parent_rowset.segments
        ):
            raise I3ProductionContractError(
                "DELTA versioned outputs require the exact parent prefix and one suffix"
            )
        segment = rowset.segments[-1]
        expected_segment_id = production_delta_append_segment_id(
            table_name=output.table_name,
            parent_rowset_id=parent_rowset.rowset_index_id,
            parent_segment_ids=tuple(item.segment_id for item in parent_rowset.segments),
            artifact=segment.artifact,
            terminal_session=run_spec.terminal_session,
            availability_session=run_spec.run_available_session,
            native_v2_migration_id=run_spec.native_v2_migration_id,
        )
        if (
            rowset.terminal_session != run_spec.terminal_session
            or segment.availability_session != run_spec.run_available_session
            or segment.segment_id != expected_segment_id
        ):
            raise I3ProductionContractError(
                "DELTA append segment differs from module-owned identity"
            )
    parent_dataset = parent_output_set.table_outputs[-1].dataset_index
    dataset = table_outputs[-1].dataset_index
    if (
        parent_dataset is None
        or dataset is None
        or len(dataset.partitions) != len(parent_dataset.partitions) + 1
        or dataset.partitions[:-1] != parent_dataset.partitions
        or dataset.terminal_session != run_spec.terminal_session
        or dataset.partitions[-1].session_date != run_spec.terminal_session
        or dataset.partitions[-1].availability_session != run_spec.run_available_session
    ):
        raise I3ProductionContractError(
            "DELTA universe requires the exact parent prefix and target-session suffix"
        )


@dataclass(frozen=True, slots=True)
class I3ProductionResourceObservation:
    peak_rss_bytes: int
    elapsed_seconds: int
    minimum_disk_free_bytes: int
    temporary_bytes: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.peak_rss_bytes, "observed peak RSS"),
            (self.elapsed_seconds, "observed elapsed seconds"),
            (self.minimum_disk_free_bytes, "observed disk floor"),
            (self.temporary_bytes, "observed temporary bytes"),
        ):
            _nonnegative_int(value, label)

    def validate_caps(self, caps: I3ProductionResourceCaps) -> None:
        if (
            self.peak_rss_bytes > caps.rss_bytes_hard_cap
            or self.minimum_disk_free_bytes < caps.disk_free_bytes_hard_floor
            or self.temporary_bytes > caps.temporary_bytes_hard_cap
        ):
            raise I3ProductionContractError("production resource observation exceeds its caps")

    def to_dict(self) -> dict[str, int]:
        return {
            "elapsed_seconds": self.elapsed_seconds,
            "minimum_disk_free_bytes": self.minimum_disk_free_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "temporary_bytes": self.temporary_bytes,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "elapsed_seconds",
                "minimum_disk_free_bytes",
                "peak_rss_bytes",
                "temporary_bytes",
            },
            "resource observation",
        )
        return cls(**{key: _integer(item[key], key) for key in item})


@dataclass(frozen=True, slots=True)
class I3ProductionRunReceipt:
    run_spec_id: str
    run_spec_artifact: ArtifactPin
    state: I3ProductionRunState
    receipt_available_session: date
    resource_observation: I3ProductionResourceObservation
    output_set: I3ProductionOutputSet | None = None
    failure_code: str | None = None
    failure_detail_digest: str | None = None

    def __post_init__(self) -> None:
        _digest(self.run_spec_id, "receipt run-spec ID")
        _artifact(self.run_spec_artifact, "receipt run-spec artifact")
        if _fixture_path(self.run_spec_artifact.path):
            raise I3ProductionContractError(
                "fixture RunSpec cannot acquire production receipt authority"
            )
        if _temporary_path(self.run_spec_artifact.path):
            raise I3ProductionContractError(
                "temporary RunSpec cannot acquire production receipt authority"
            )
        if not isinstance(self.state, I3ProductionRunState):
            raise I3ProductionContractError("production receipt state is invalid")
        _session(self.receipt_available_session, "receipt availability")
        if not isinstance(self.resource_observation, I3ProductionResourceObservation):
            raise I3ProductionContractError("production resource observation is invalid")
        if self.state is I3ProductionRunState.SUCCEEDED:
            if not isinstance(self.output_set, I3ProductionOutputSet):
                raise I3ProductionContractError("successful receipt requires an output set")
            if self.failure_code is not None or self.failure_detail_digest is not None:
                raise I3ProductionContractError("successful receipt cannot carry failure details")
        else:
            if self.output_set is not None:
                raise I3ProductionContractError("failed receipt cannot expose an output set")
            _token(self.failure_code, "production failure code")
            _digest(self.failure_detail_digest, "production failure detail digest")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "failure_code": self.failure_code,
            "failure_detail_digest": self.failure_detail_digest,
            "output_set": self.output_set.to_dict() if self.output_set else None,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "resource_observation": self.resource_observation.to_dict(),
            "rule_version": I3_PRODUCTION_RUN_RECEIPT_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "state": self.state.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "failure_code",
                "failure_detail_digest",
                "output_set",
                "receipt_available_session",
                "receipt_id",
                "resource_observation",
                "rule_version",
                "run_spec_artifact",
                "run_spec_id",
                "state",
            },
            "production run receipt",
        )
        _literal(item["rule_version"], I3_PRODUCTION_RUN_RECEIPT_RULE_VERSION, "receipt rule")
        try:
            state = I3ProductionRunState(_text(item["state"], "production receipt state"))
        except ValueError as exc:
            raise I3ProductionContractError("production receipt state is invalid") from exc
        result = cls(
            run_spec_id=_text(item["run_spec_id"], "receipt run-spec ID"),
            run_spec_artifact=_artifact_from_dict(
                item["run_spec_artifact"], "receipt run-spec artifact"
            ),
            state=state,
            receipt_available_session=_date_from_json(
                item["receipt_available_session"], "receipt availability"
            ),
            resource_observation=I3ProductionResourceObservation.from_dict(
                item["resource_observation"]
            ),
            output_set=(
                None
                if item["output_set"] is None
                else I3ProductionOutputSet.from_dict(item["output_set"])
            ),
            failure_code=_optional_text(item["failure_code"], "production failure code"),
            failure_detail_digest=_optional_text(
                item["failure_detail_digest"], "production failure detail digest"
            ),
        )
        if item["receipt_id"] != result.receipt_id:
            raise I3ProductionContractError("production receipt ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionCompletion:
    run_spec_id: str
    receipt_id: str
    receipt_artifact: ArtifactPin
    output_set_id: str
    release_id: str
    native_v2_envelope_id: str
    checkpoint_id: str
    completion_available_session: date
    state: I3ProductionCompletionState = I3ProductionCompletionState.AWAITING_REVIEW

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "completion run-spec ID"),
            (self.receipt_id, "completion receipt ID"),
            (self.output_set_id, "completion output-set ID"),
            (self.release_id, "completion release ID"),
            (self.native_v2_envelope_id, "completion native-v2 envelope ID"),
            (self.checkpoint_id, "completion checkpoint ID"),
        ):
            _digest(value, label)
        _artifact(self.receipt_artifact, "completion receipt artifact")
        if _fixture_path(self.receipt_artifact.path):
            raise I3ProductionContractError(
                "fixture receipt cannot acquire production completion authority"
            )
        if _temporary_path(self.receipt_artifact.path):
            raise I3ProductionContractError(
                "temporary receipt cannot acquire production completion authority"
            )
        _session(self.completion_available_session, "completion availability")
        if self.state is not I3ProductionCompletionState.AWAITING_REVIEW:
            raise I3ProductionContractError("production completion must stop at awaiting_review")

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "completion_available_session": self.completion_available_session.isoformat(),
            "cutover_authorized": False,
            "native_v2_envelope_id": self.native_v2_envelope_id,
            "native_v2_manifest_role": "physical_checkpoint_envelope_not_release_authority",
            "output_set_id": self.output_set_id,
            "publish_authorized": False,
            "public_release_authority": "gate_a_incremental_release_manifest",
            "receipt_artifact": self.receipt_artifact.to_dict(),
            "receipt_id": self.receipt_id,
            "release_id": self.release_id,
            "rule_version": I3_PRODUCTION_COMPLETION_RULE_VERSION,
            "run_spec_id": self.run_spec_id,
            "state": self.state.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"completion_id": self.completion_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "checkpoint_id",
                "completion_available_session",
                "completion_id",
                "cutover_authorized",
                "native_v2_envelope_id",
                "native_v2_manifest_role",
                "output_set_id",
                "publish_authorized",
                "public_release_authority",
                "receipt_artifact",
                "receipt_id",
                "release_id",
                "rule_version",
                "run_spec_id",
                "state",
            },
            "production completion",
        )
        _literal(item["rule_version"], I3_PRODUCTION_COMPLETION_RULE_VERSION, "completion rule")
        if item["publish_authorized"] is not False or item["cutover_authorized"] is not False:
            raise I3ProductionContractError(
                "production staging completion cannot authorize publish"
            )
        _literal(
            item["public_release_authority"],
            "gate_a_incremental_release_manifest",
            "public release authority",
        )
        _literal(
            item["native_v2_manifest_role"],
            "physical_checkpoint_envelope_not_release_authority",
            "native-v2 manifest role",
        )
        try:
            state = I3ProductionCompletionState(_text(item["state"], "completion state"))
        except ValueError as exc:
            raise I3ProductionContractError("production completion state is invalid") from exc
        result = cls(
            run_spec_id=_text(item["run_spec_id"], "completion run-spec ID"),
            receipt_id=_text(item["receipt_id"], "completion receipt ID"),
            receipt_artifact=_artifact_from_dict(
                item["receipt_artifact"], "completion receipt artifact"
            ),
            output_set_id=_text(item["output_set_id"], "completion output-set ID"),
            release_id=_text(item["release_id"], "completion release ID"),
            native_v2_envelope_id=_text(
                item["native_v2_envelope_id"], "completion native-v2 envelope ID"
            ),
            checkpoint_id=_text(item["checkpoint_id"], "completion checkpoint ID"),
            completion_available_session=_date_from_json(
                item["completion_available_session"], "completion availability"
            ),
            state=state,
        )
        if item["completion_id"] != result.completion_id:
            raise I3ProductionContractError("production completion ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I3ProductionDeepVerificationAttestation:
    """Immutable proof that module-owned I3 deep verification completed.

    This object is a bounded-read parent frontier, not publication authority.
    A migration shadow may pair it with the exact awaiting-review completion;
    a published daily parent additionally requires an external pointer event.
    """

    completion_id: str
    completion_artifact: ArtifactPin
    gate_a_manifest_pin: ManifestPin
    native_v2_release: NativeV2ParentReleasePin
    checkpoint_id: str
    checkpoint_artifact: ArtifactPin
    output_set_id: str
    row_semantic_attestation_digest: str
    terminal_state_digest: str
    physical_index_digest: str
    parent_frontier_attestation_digest: str | None
    attestation_available_session: date
    verification_resource_observation: I3ProductionResourceObservation

    def __post_init__(self) -> None:
        for value, label in (
            (self.completion_id, "deep-attestation completion ID"),
            (self.checkpoint_id, "deep-attestation checkpoint ID"),
            (self.output_set_id, "deep-attestation output-set ID"),
            (
                self.row_semantic_attestation_digest,
                "deep-attestation row-semantic digest",
            ),
            (self.terminal_state_digest, "deep-attestation terminal-state digest"),
            (self.physical_index_digest, "deep-attestation physical-index digest"),
        ):
            _digest(value, label)
        _optional_text(
            self.parent_frontier_attestation_digest,
            "deep-attestation parent-frontier digest",
        )
        if self.parent_frontier_attestation_digest is not None:
            _digest(
                self.parent_frontier_attestation_digest,
                "deep-attestation parent-frontier digest",
            )
        _artifact(self.completion_artifact, "deep-attestation completion artifact")
        _artifact(self.checkpoint_artifact, "deep-attestation checkpoint artifact")
        if not isinstance(self.gate_a_manifest_pin, ManifestPin):
            raise I3ProductionContractError("deep-attestation Gate-A manifest pin is invalid")
        if not isinstance(self.native_v2_release, NativeV2ParentReleasePin):
            raise I3ProductionContractError("deep-attestation native-v2 release is invalid")
        authority_paths = (
            self.completion_artifact.path,
            self.checkpoint_artifact.path,
            self.gate_a_manifest_pin.manifest_path,
            self.native_v2_release.manifest.path,
        )
        if any(_temporary_path(path) for path in authority_paths):
            raise I3ProductionContractError(
                "temporary artifact cannot enter a production deep attestation"
            )
        _session(self.attestation_available_session, "deep-attestation availability")
        if not isinstance(
            self.verification_resource_observation,
            I3ProductionResourceObservation,
        ):
            raise I3ProductionContractError(
                "deep-attestation verification resource observation is invalid"
            )
        if self.attestation_available_session < max(
            self.gate_a_manifest_pin.release_available_session,
            self.native_v2_release.release_available_session,
        ):
            raise I3ProductionContractError(
                "deep-attestation availability precedes its verified releases"
            )

    @property
    def deep_attestation_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "attestation_available_session": self.attestation_available_session.isoformat(),
            "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
            "checkpoint_id": self.checkpoint_id,
            "completion_artifact": self.completion_artifact.to_dict(),
            "completion_id": self.completion_id,
            "gate_a_manifest_pin": self.gate_a_manifest_pin.to_dict(),
            "native_v2_release": self.native_v2_release.to_dict(),
            "output_set_id": self.output_set_id,
            "parent_frontier_attestation_digest": (self.parent_frontier_attestation_digest),
            "physical_index_digest": self.physical_index_digest,
            "publish_authorized": False,
            "row_semantic_attestation_digest": self.row_semantic_attestation_digest,
            "rule_version": I3_PRODUCTION_DEEP_ATTESTATION_RULE_VERSION,
            "terminal_state_digest": self.terminal_state_digest,
            "verification_resource_observation": (self.verification_resource_observation.to_dict()),
        }

    def to_dict(self) -> dict[str, object]:
        return {"deep_attestation_id": self.deep_attestation_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
        )

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "attestation_available_session",
                "checkpoint_artifact",
                "checkpoint_id",
                "completion_artifact",
                "completion_id",
                "deep_attestation_id",
                "gate_a_manifest_pin",
                "native_v2_release",
                "output_set_id",
                "parent_frontier_attestation_digest",
                "physical_index_digest",
                "publish_authorized",
                "row_semantic_attestation_digest",
                "rule_version",
                "terminal_state_digest",
                "verification_resource_observation",
            },
            "production deep-verification attestation",
        )
        _literal(
            item["rule_version"],
            I3_PRODUCTION_DEEP_ATTESTATION_RULE_VERSION,
            "deep-attestation rule",
        )
        _literal(item["publish_authorized"], False, "deep-attestation publish authority")
        result = cls(
            completion_id=_text(item["completion_id"], "deep-attestation completion ID"),
            completion_artifact=_artifact_from_dict(
                item["completion_artifact"], "deep-attestation completion artifact"
            ),
            gate_a_manifest_pin=_manifest_pin_from_dict(item["gate_a_manifest_pin"]),
            native_v2_release=NativeV2ParentReleasePin.from_dict(item["native_v2_release"]),
            checkpoint_id=_text(item["checkpoint_id"], "deep-attestation checkpoint ID"),
            checkpoint_artifact=_artifact_from_dict(
                item["checkpoint_artifact"], "deep-attestation checkpoint artifact"
            ),
            output_set_id=_text(item["output_set_id"], "deep-attestation output-set ID"),
            row_semantic_attestation_digest=_text(
                item["row_semantic_attestation_digest"],
                "deep-attestation row-semantic digest",
            ),
            terminal_state_digest=_text(
                item["terminal_state_digest"], "deep-attestation terminal-state digest"
            ),
            physical_index_digest=_text(
                item["physical_index_digest"], "deep-attestation physical-index digest"
            ),
            parent_frontier_attestation_digest=_optional_text(
                item["parent_frontier_attestation_digest"],
                "deep-attestation parent-frontier digest",
            ),
            attestation_available_session=_date_from_json(
                item["attestation_available_session"], "deep-attestation availability"
            ),
            verification_resource_observation=I3ProductionResourceObservation.from_dict(
                item["verification_resource_observation"]
            ),
        )
        if item["deep_attestation_id"] != result.deep_attestation_id:
            raise I3ProductionContractError("deep-attestation ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class LoadedI3ProductionStaging:
    """Fully reconciled staging result; intentionally carries no publish capability."""

    completion: I3ProductionCompletion
    receipt: I3ProductionRunReceipt
    run_spec: I3ProductionRunSpec
    manifest: NativeV2ReleaseManifest
    checkpoint: I3CheckpointState
    terminal_state_digest: str
    gate_a_run_spec: RunSpec
    gate_a_run_receipt: RunReceipt
    gate_a_manifest: IncrementalReleaseManifest
    gate_a_release: ContentAttestedRelease
    deep_attestation: I3ProductionDeepVerificationAttestation | None = None


def load_i3_production_run_spec_exact(
    pin: ArtifactPin, reader: Callable[[str], bytes]
) -> I3ProductionRunSpec:
    content = _read_exact_bytes(pin, reader, label="production run spec")
    result = I3ProductionRunSpec.from_dict(_strict_json_document(content))
    if result.canonical_bytes() != content:
        raise I3ProductionContractError("production run-spec bytes are not canonical JSON")
    return result


def load_i3_production_run_receipt_exact(
    pin: ArtifactPin, reader: Callable[[str], bytes]
) -> I3ProductionRunReceipt:
    content = _read_exact_bytes(pin, reader, label="production run receipt")
    result = I3ProductionRunReceipt.from_dict(_strict_json_document(content))
    if result.canonical_bytes() != content:
        raise I3ProductionContractError("production receipt bytes are not canonical JSON")
    return result


def load_i3_production_completion_exact(
    pin: ArtifactPin, reader: Callable[[str], bytes]
) -> I3ProductionCompletion:
    content = _read_exact_bytes(pin, reader, label="production completion")
    result = I3ProductionCompletion.from_dict(_strict_json_document(content))
    if result.canonical_bytes() != content:
        raise I3ProductionContractError("production completion bytes are not canonical JSON")
    return result


def load_i3_production_deep_attestation_exact(
    pin: ArtifactPin, reader: Callable[[str], bytes]
) -> I3ProductionDeepVerificationAttestation:
    content = _read_exact_bytes(pin, reader, label="production deep attestation")
    result = I3ProductionDeepVerificationAttestation.from_dict(_strict_json_document(content))
    if result.canonical_bytes() != content:
        raise I3ProductionContractError("production deep-attestation bytes are not canonical JSON")
    return result


def production_physical_index_digest(output_set: I3ProductionOutputSet) -> str:
    """Digest only immutable physical envelopes and member-index pins.

    A deep verifier hashes every referenced data artifact before minting the
    attestation.  Daily parent loading can subsequently recompute this bounded
    digest from the exact OutputSet plus dataset/rowset index documents without
    reopening historical Parquet members.
    """

    if not isinstance(output_set, I3ProductionOutputSet):
        raise I3ProductionContractError("physical-index digest requires an OutputSet")
    return stable_digest(
        {
            "checkpoint_artifact": output_set.checkpoint_artifact.to_dict(),
            "control_extension_artifacts": [
                item.to_dict() for item in output_set.control_extension_artifacts
            ],
            "gate_a_manifest_pin": output_set.gate_a_manifest_pin.to_dict(),
            "native_v2_manifest_artifact": (output_set.release_manifest_artifact.to_dict()),
            "rule_version": I3_PRODUCTION_PHYSICAL_INDEX_DIGEST_RULE_VERSION,
            "table_outputs": [item.to_dict() for item in output_set.table_outputs],
        }
    )


def _qa_policy_from_dict(value: object) -> QaPolicy:
    item = _closed_mapping(
        value,
        {"checks", "namespace", "qa_policy_id", "rule_version"},
        "QA policy",
    )
    _literal(item["namespace"], "ame_stocks.silver.incremental_qa_policy", "QA namespace")
    _literal(item["rule_version"], "s7_5_incremental_qa_policy_v1", "QA policy rule")
    checks: list[QaCheckPolicy] = []
    for entry in _array(item["checks"], "QA policy checks"):
        check = _closed_mapping(
            entry,
            {"check_id", "max_publish_failure_count", "semantics_digest", "severity"},
            "QA check policy",
        )
        try:
            severity = QaSeverity(_text(check["severity"], "QA severity"))
        except ValueError as exc:
            raise I3ProductionContractError("QA severity is invalid") from exc
        checks.append(
            QaCheckPolicy(
                check_id=_text(check["check_id"], "QA check ID"),
                severity=severity,
                semantics_digest=_text(check["semantics_digest"], "QA semantics digest"),
                max_publish_failure_count=_integer(
                    check["max_publish_failure_count"], "QA failure limit"
                ),
            )
        )
    result = QaPolicy(tuple(checks))
    if item["qa_policy_id"] != result.qa_policy_id:
        raise I3ProductionContractError("QA policy ID does not reproduce")
    return result


def _qa_receipt_from_dict(value: object) -> QaReceipt:
    item = _closed_mapping(
        value,
        {
            "change_set_digest",
            "qa_available_session",
            "qa_policy_id",
            "qa_receipt_id",
            "results",
            "run_spec_id",
            "source_binding_digest",
        },
        "QA receipt",
    )
    results: list[QaCheckResult] = []
    for entry in _array(item["results"], "QA results"):
        result = _closed_mapping(
            entry,
            {
                "check_id",
                "details_artifact",
                "failure_count",
                "observed_count",
                "semantics_digest",
            },
            "QA result",
        )
        artifact = _artifact_from_dict(result["details_artifact"], "QA details artifact")
        results.append(
            QaCheckResult(
                check_id=_text(result["check_id"], "QA result check ID"),
                semantics_digest=_text(result["semantics_digest"], "QA result semantics"),
                observed_count=_integer(result["observed_count"], "QA observed count"),
                failure_count=_integer(result["failure_count"], "QA failure count"),
                details_artifact=GateArtifactPin(**artifact.to_dict()),
            )
        )
    receipt = QaReceipt(
        qa_policy_id=_text(item["qa_policy_id"], "QA policy ID"),
        run_spec_id=_text(item["run_spec_id"], "QA run-spec ID"),
        source_binding_digest=_text(item["source_binding_digest"], "QA source binding"),
        change_set_digest=_text(item["change_set_digest"], "QA change-set digest"),
        qa_available_session=_date_from_json(item["qa_available_session"], "QA availability"),
        results=tuple(results),
    )
    if item["qa_receipt_id"] != receipt.qa_receipt_id:
        raise I3ProductionContractError("QA receipt ID does not reproduce")
    return receipt


def _gate_a_run_spec_from_dict(value: object) -> RunSpec:
    item = _closed_mapping(
        value,
        {
            "availability_cutoff_session",
            "calendar_digest",
            "correction_authorization",
            "correction_scope_digest",
            "disk_floor_bytes",
            "expected_change_set_digest",
            "identity_policy_bundle_id",
            "input_pins",
            "parent_identity_policy_bundle_id",
            "parent_release_pin",
            "qa_policy",
            "release_available_session",
            "release_type",
            "resolved_view",
            "rss_cap_bytes",
            "run_spec_id",
            "schema_digest",
            "source_binding_digest",
            "source_cutoff_session",
            "transform_semantics_digest",
            "wall_clock_cap_seconds",
        },
        "Gate-A RunSpec",
    )
    if item["correction_scope_digest"] is not None or item["correction_authorization"] is not None:
        raise I3ProductionContractError("I3 production staging does not accept correction controls")
    try:
        release_type = ReleaseType(_text(item["release_type"], "Gate-A release type"))
        resolved_view = ViewKind(_text(item["resolved_view"], "Gate-A resolved view"))
    except ValueError as exc:
        raise I3ProductionContractError("Gate-A RunSpec enum is invalid") from exc
    if release_type not in {ReleaseType.BASE, ReleaseType.DELTA}:
        raise I3ProductionContractError("I3 production staging accepts only base or delta")
    wall_clock = item["wall_clock_cap_seconds"]
    if wall_clock is not None:
        raise I3ProductionContractError(
            "production Gate-A RunSpec must not impose wall-clock cutoff"
        )
    result = RunSpec(
        release_type=release_type,
        parent_release_pin=(
            None
            if item["parent_release_pin"] is None
            else _manifest_pin_from_dict(item["parent_release_pin"])
        ),
        parent_identity_policy_bundle_id=_optional_text(
            item["parent_identity_policy_bundle_id"], "prior identity-policy ID"
        ),
        resolved_view=resolved_view,
        source_binding_digest=_text(item["source_binding_digest"], "source binding"),
        source_cutoff_session=_date_from_json(item["source_cutoff_session"], "source cutoff"),
        availability_cutoff_session=_date_from_json(
            item["availability_cutoff_session"], "availability cutoff"
        ),
        release_available_session=_date_from_json(
            item["release_available_session"], "release availability"
        ),
        schema_digest=_text(item["schema_digest"], "Gate-A schema digest"),
        transform_semantics_digest=_text(
            item["transform_semantics_digest"], "Gate-A transform digest"
        ),
        identity_policy_bundle_id=_text(
            item["identity_policy_bundle_id"], "identity-policy bundle ID"
        ),
        calendar_digest=_text(item["calendar_digest"], "calendar digest"),
        input_pins=tuple(
            _artifact_from_dict(entry, "Gate-A input pin")
            for entry in _array(item["input_pins"], "Gate-A input pins")
        ),
        expected_change_set_digest=_text(
            item["expected_change_set_digest"], "expected change-set digest"
        ),
        qa_policy=_qa_policy_from_dict(item["qa_policy"]),
        correction_scope_digest=None,
        correction_authorization=None,
        rss_cap_bytes=_integer(item["rss_cap_bytes"], "Gate-A RSS cap"),
        disk_floor_bytes=_integer(item["disk_floor_bytes"], "Gate-A disk floor"),
        wall_clock_cap_seconds=None,
    )
    if item["run_spec_id"] != result.run_spec_id:
        raise I3ProductionContractError("Gate-A RunSpec ID does not reproduce")
    return result


def _checkpoint_receipt_from_dict(value: object) -> CheckpointReceipt:
    item = _closed_mapping(
        value,
        {
            "artifact",
            "checkpoint_id",
            "last_session",
            "parent_release_id",
            "rebuild_basis_digest",
            "resolved_content_digest",
            "rule_version",
            "run_spec_id",
        },
        "Gate-A checkpoint receipt",
    )
    _literal(item["rule_version"], "s7_5_checkpoint_receipt_v1", "checkpoint rule")
    result = CheckpointReceipt(
        artifact=_artifact_from_dict(item["artifact"], "Gate-A checkpoint artifact"),
        parent_release_id=_optional_text(item["parent_release_id"], "checkpoint parent ID"),
        run_spec_id=_text(item["run_spec_id"], "checkpoint RunSpec ID"),
        last_session=_date_from_json(item["last_session"], "checkpoint last session"),
        resolved_content_digest=_text(
            item["resolved_content_digest"], "checkpoint resolved-content digest"
        ),
        rebuild_basis_digest=_text(item["rebuild_basis_digest"], "checkpoint rebuild-basis digest"),
    )
    if item["checkpoint_id"] != result.checkpoint_id:
        raise I3ProductionContractError("Gate-A checkpoint receipt ID does not reproduce")
    return result


def _gate_a_run_receipt_from_dict(value: object) -> RunReceipt:
    item = _closed_mapping(
        value,
        {
            "actual_input_set_digest",
            "checkpoint",
            "checkpoint_id",
            "error_codes",
            "output_set_digest",
            "qa_receipt",
            "qa_receipt_id",
            "receipt_available_session",
            "run_receipt_id",
            "run_spec_id",
            "runtime_observation",
            "succeeded",
        },
        "Gate-A RunReceipt",
    )
    runtime = _closed_mapping(
        item["runtime_observation"],
        {"minimum_free_disk_bytes", "peak_rss_bytes", "runtime_seconds"},
        "Gate-A runtime observation",
    )
    checkpoint = (
        None if item["checkpoint"] is None else _checkpoint_receipt_from_dict(item["checkpoint"])
    )
    qa_receipt = None if item["qa_receipt"] is None else _qa_receipt_from_dict(item["qa_receipt"])
    result = RunReceipt(
        run_spec_id=_text(item["run_spec_id"], "Gate-A receipt RunSpec ID"),
        actual_input_set_digest=_optional_text(
            item["actual_input_set_digest"], "actual input-set digest"
        ),
        output_set_digest=_optional_text(item["output_set_digest"], "output-set digest"),
        qa_receipt=qa_receipt,
        checkpoint=checkpoint,
        succeeded=item["succeeded"],
        error_codes=tuple(
            _text(code, "Gate-A error code")
            for code in _array(item["error_codes"], "Gate-A error codes")
        ),
        receipt_available_session=_date_from_json(
            item["receipt_available_session"], "Gate-A receipt availability"
        ),
        runtime_seconds=runtime["runtime_seconds"],
        peak_rss_bytes=(
            None
            if runtime["peak_rss_bytes"] is None
            else _integer(runtime["peak_rss_bytes"], "Gate-A peak RSS")
        ),
        minimum_free_disk_bytes=_integer(
            runtime["minimum_free_disk_bytes"], "Gate-A minimum free disk"
        ),
    )
    if (
        item["run_receipt_id"] != result.run_receipt_id
        or item["checkpoint_id"] != (checkpoint.checkpoint_id if checkpoint is not None else None)
        or item["qa_receipt_id"] != (qa_receipt.qa_receipt_id if qa_receipt is not None else None)
    ):
        raise I3ProductionContractError("Gate-A RunReceipt does not reproduce")
    return result


def _partition_receipt_from_dict(value: object) -> PartitionReceipt:
    item = _closed_mapping(
        value,
        {
            "availability_session",
            "partition_key",
            "receipt",
            "row_count",
            "row_version_references",
            "schema_digest",
            "table_name",
        },
        "Gate-A partition receipt",
    )
    references: list[RowVersionReference] = []
    for entry in _array(item["row_version_references"], "row-version references"):
        reference = _closed_mapping(
            entry, {"row_version_id", "table_name"}, "row-version reference"
        )
        references.append(
            RowVersionReference(
                table_name=_text(reference["table_name"], "reference table"),
                row_version_id=_text(reference["row_version_id"], "referenced row version"),
            )
        )
    return PartitionReceipt(
        table_name=_text(item["table_name"], "partition table"),
        partition_key=_text(item["partition_key"], "partition key"),
        receipt=_artifact_from_dict(item["receipt"], "partition artifact"),
        row_count=_integer(item["row_count"], "partition row count"),
        schema_digest=_text(item["schema_digest"], "partition schema digest"),
        availability_session=_date_from_json(
            item["availability_session"], "partition availability"
        ),
        row_version_references=tuple(references),
    )


def _row_semantic_proof_from_dict(value: object) -> RowSemanticProofReceipt:
    item = _closed_mapping(
        value,
        {
            "artifact",
            "operation",
            "predecessor_payload_digest",
            "predecessor_row_version_id",
            "row_payload_digest",
            "row_version_id",
            "stable_row_key",
            "table_name",
            "validator_semantics_digest",
        },
        "Gate-A row semantic proof",
    )
    try:
        operation = RowVersionOperation(_text(item["operation"], "row operation"))
    except ValueError as exc:
        raise I3ProductionContractError("row operation is invalid") from exc
    return RowSemanticProofReceipt(
        table_name=_text(item["table_name"], "row proof table"),
        stable_row_key=_text(item["stable_row_key"], "row proof stable key"),
        row_version_id=_text(item["row_version_id"], "row proof version ID"),
        predecessor_row_version_id=_optional_text(
            item["predecessor_row_version_id"], "row proof predecessor ID"
        ),
        operation=operation,
        row_payload_digest=_text(item["row_payload_digest"], "row proof payload digest"),
        predecessor_payload_digest=_optional_text(
            item["predecessor_payload_digest"], "row proof predecessor payload"
        ),
        validator_semantics_digest=_text(
            item["validator_semantics_digest"], "row proof validator semantics"
        ),
        artifact=_artifact_from_dict(item["artifact"], "row proof artifact"),
    )


def _row_version_receipt_from_dict(value: object) -> RowVersionReceipt:
    item = _closed_mapping(
        value,
        {
            "availability_session",
            "index_artifact",
            "operation",
            "predecessor_row_version_id",
            "row_locator",
            "row_payload_digest",
            "row_version_id",
            "semantic_proof",
            "stable_row_key",
            "table_name",
            "tombstone_reason",
        },
        "Gate-A row-version receipt",
    )
    try:
        operation = RowVersionOperation(_text(item["operation"], "row operation"))
    except ValueError as exc:
        raise I3ProductionContractError("row operation is invalid") from exc
    return RowVersionReceipt(
        table_name=_text(item["table_name"], "row-version table"),
        stable_row_key=_text(item["stable_row_key"], "row stable key"),
        row_version_id=_text(item["row_version_id"], "row-version ID"),
        predecessor_row_version_id=_optional_text(
            item["predecessor_row_version_id"], "row predecessor ID"
        ),
        operation=operation,
        availability_session=_date_from_json(
            item["availability_session"], "row-version availability"
        ),
        index_artifact=_artifact_from_dict(item["index_artifact"], "row index artifact"),
        row_locator=_text(item["row_locator"], "row locator"),
        row_payload_digest=_text(item["row_payload_digest"], "row payload digest"),
        semantic_proof=_row_semantic_proof_from_dict(item["semantic_proof"]),
        tombstone_reason=_optional_text(item["tombstone_reason"], "tombstone reason"),
    )


def _row_version_change_index_pin_from_dict(
    value: object,
) -> RowVersionChangeIndexPin:
    item = _closed_mapping(
        value,
        {
            "artifact",
            "availability_session",
            "index_id",
            "logical_receipts_digest",
            "row_count",
            "rule_version",
            "schema_digest",
            "superseded_row_version_count",
            "superseded_row_version_ids_digest",
        },
        "Gate-A row-version change index",
    )
    result = RowVersionChangeIndexPin(
        artifact=_artifact_from_dict(item["artifact"], "row-version change index artifact"),
        row_count=_integer(item["row_count"], "row-version change index row count"),
        logical_receipts_digest=_text(
            item["logical_receipts_digest"], "indexed logical receipts digest"
        ),
        superseded_row_version_count=_integer(
            item["superseded_row_version_count"], "indexed superseded row count"
        ),
        superseded_row_version_ids_digest=_text(
            item["superseded_row_version_ids_digest"],
            "indexed superseded row-version digest",
        ),
        schema_digest=_text(item["schema_digest"], "row-version change index schema"),
        availability_session=_date_from_json(
            item["availability_session"], "row-version change index availability"
        ),
        rule_version=_text(item["rule_version"], "row-version change index rule"),
    )
    if item["index_id"] != result.index_id:
        raise I3ProductionContractError("row-version change index ID does not reproduce")
    return result


def _gate_a_manifest_from_dict(value: object) -> IncrementalReleaseManifest:
    fields = {
        "added_partition_receipts",
        "added_row_version_receipts",
        "availability_cutoff_session",
        "calendar_digest",
        "control_provenance",
        "correction_authorization_id",
        "identity_policy_bundle_id",
        "parent_release_pin",
        "partition_replacements",
        "qa_policy_id",
        "release_available_session",
        "release_id",
        "release_type",
        "resolved_content_digest",
        "resolved_view",
        "schema_digest",
        "source_binding_digest",
        "source_cutoff_session",
        "superseded_row_version_ids",
        "transform_semantics_digest",
    }
    if isinstance(value, dict) and "row_version_change_index" in value:
        fields.add("row_version_change_index")
    item = _closed_mapping(
        value,
        fields,
        "Gate-A release manifest",
    )
    if (
        _array(item["partition_replacements"], "partition replacements")
        or item["correction_authorization_id"] is not None
    ):
        raise I3ProductionContractError(
            "I3 base/delta staging manifest contains unsupported correction operations"
        )
    provenance = _closed_mapping(
        item["control_provenance"],
        {"qa_receipt_id", "run_receipt_pin", "run_spec_pin"},
        "Gate-A control provenance",
    )
    try:
        release_type = ReleaseType(_text(item["release_type"], "manifest release type"))
        resolved_view = ViewKind(_text(item["resolved_view"], "manifest resolved view"))
    except ValueError as exc:
        raise I3ProductionContractError("Gate-A manifest enum is invalid") from exc
    result = IncrementalReleaseManifest(
        release_type=release_type,
        parent_release_pin=(
            None
            if item["parent_release_pin"] is None
            else _manifest_pin_from_dict(item["parent_release_pin"])
        ),
        resolved_view=resolved_view,
        schema_digest=_text(item["schema_digest"], "manifest schema digest"),
        transform_semantics_digest=_text(
            item["transform_semantics_digest"], "manifest transform digest"
        ),
        identity_policy_bundle_id=_text(
            item["identity_policy_bundle_id"], "manifest identity-policy ID"
        ),
        calendar_digest=_text(item["calendar_digest"], "manifest calendar digest"),
        source_binding_digest=_text(item["source_binding_digest"], "manifest source binding"),
        source_cutoff_session=_date_from_json(item["source_cutoff_session"], "source cutoff"),
        availability_cutoff_session=_date_from_json(
            item["availability_cutoff_session"], "availability cutoff"
        ),
        release_available_session=_date_from_json(
            item["release_available_session"], "release availability"
        ),
        added_partition_receipts=tuple(
            _partition_receipt_from_dict(entry)
            for entry in _array(item["added_partition_receipts"], "added partitions")
        ),
        partition_replacements=(),
        added_row_version_receipts=tuple(
            _row_version_receipt_from_dict(entry)
            for entry in _array(item["added_row_version_receipts"], "added row-version receipts")
        ),
        superseded_row_version_ids=tuple(
            _text(entry, "superseded row-version ID")
            for entry in _array(item["superseded_row_version_ids"], "superseded row versions")
        ),
        resolved_content_digest=_text(
            item["resolved_content_digest"], "manifest resolved-content digest"
        ),
        qa_policy_id=_text(item["qa_policy_id"], "manifest QA policy ID"),
        qa_receipt_id=_text(provenance["qa_receipt_id"], "manifest QA receipt ID"),
        correction_authorization_id=None,
        run_spec_pin=_control_object_pin_from_dict(
            provenance["run_spec_pin"], ControlObjectKind.RUN_SPEC
        ),
        run_receipt_pin=_control_object_pin_from_dict(
            provenance["run_receipt_pin"], ControlObjectKind.RUN_RECEIPT
        ),
        row_version_change_index=(
            None
            if "row_version_change_index" not in item
            else _row_version_change_index_pin_from_dict(item["row_version_change_index"])
        ),
    )
    if item["release_id"] != result.release_id:
        raise I3ProductionContractError("Gate-A release ID does not reproduce")
    return result


def _load_gate_a_controls_exact(
    root: Path, output_set: I3ProductionOutputSet
) -> tuple[RunSpec, RunReceipt, IncrementalReleaseManifest]:
    spec_content = _read_exact_root(root, output_set.gate_a_run_spec_pin.artifact)
    gate_spec = _gate_a_run_spec_from_dict(_strict_json_document(spec_content))
    if (
        _canonical_json_bytes(gate_spec.to_dict()) != spec_content
        or control_object_pin(gate_spec, path=output_set.gate_a_run_spec_pin.artifact.path)
        != output_set.gate_a_run_spec_pin
    ):
        raise I3ProductionContractError("Gate-A RunSpec exact pin does not reproduce")
    receipt_content = _read_exact_root(root, output_set.gate_a_run_receipt_pin.artifact)
    gate_receipt = _gate_a_run_receipt_from_dict(_strict_json_document(receipt_content))
    if (
        _canonical_json_bytes(gate_receipt.to_dict()) != receipt_content
        or control_object_pin(gate_receipt, path=output_set.gate_a_run_receipt_pin.artifact.path)
        != output_set.gate_a_run_receipt_pin
    ):
        raise I3ProductionContractError("Gate-A RunReceipt exact pin does not reproduce")
    manifest_artifact = _artifact_from_manifest_pin(output_set.gate_a_manifest_pin)
    manifest_content = _read_exact_root(root, manifest_artifact)
    gate_manifest = _gate_a_manifest_from_dict(_strict_json_document(manifest_content))
    if (
        gate_manifest.canonical_bytes() != manifest_content
        or gate_manifest.exact_pin(manifest_path=output_set.gate_a_manifest_pin.manifest_path)
        != output_set.gate_a_manifest_pin
    ):
        raise I3ProductionContractError("Gate-A release manifest exact pin does not reproduce")
    return gate_spec, gate_receipt, gate_manifest


def load_i3_production_parent_shallow_exact(
    root: Path, run_spec: I3ProductionRunSpec
) -> LoadedI3ProductionStaging | None:
    """Authenticate one exact parent frontier without reading historical data.

    The parent's deep-verification attestation is the immutable proof that all
    Parquet/proof bytes were previously replayed.  This bounded loader opens
    only exact controls, the native/checkpoint envelopes, dataset/rowset index
    JSON, the attestation, and (for a published daily parent) its exact I5/I6
    pointer event plus approval body.
    """

    if run_spec.run_kind is I3ProductionRunKind.BASE:
        return None
    if (
        run_spec.parent_release is None
        or run_spec.parent_checkpoint_artifact is None
        or run_spec.parent_gate_a_manifest is None
        or run_spec.parent_shadow_completion_artifact is None
        or run_spec.parent_deep_attestation_artifact is None
        or run_spec.parent_authority is None
    ):
        raise I3ProductionContractError("production delta parent controls are incomplete")
    root = root.expanduser().resolve()

    def reader(relative: str) -> bytes:
        return _read_root_bytes(root, relative)

    completion = load_i3_production_completion_exact(
        run_spec.parent_shadow_completion_artifact, reader
    )
    if completion.exact_pin(path=run_spec.parent_shadow_completion_artifact.path) != (
        run_spec.parent_shadow_completion_artifact
    ):
        raise I3ProductionContractError("parent completion exact pin does not reproduce")
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader)
    if (
        completion.receipt_id != receipt.receipt_id
        or receipt.state is not I3ProductionRunState.SUCCEEDED
        or receipt.output_set is None
    ):
        raise I3ProductionContractError("parent completion and receipt do not reconcile")
    parent_spec = load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader)
    output_set = receipt.output_set
    if (
        receipt.run_spec_id != parent_spec.run_spec_id
        or completion.run_spec_id != parent_spec.run_spec_id
        or completion.output_set_id != output_set.output_set_id
        or completion.release_id != output_set.gate_a_manifest_pin.release_id
        or completion.native_v2_envelope_id != output_set.release_id
        or completion.checkpoint_id != output_set.checkpoint_id
        or parent_spec.terminal_session >= run_spec.terminal_session
    ):
        raise I3ProductionContractError("parent bounded control chain differs")

    validate_production_compact_base_initial_rowsets(parent_spec, output_set.table_outputs)

    deep = load_i3_production_deep_attestation_exact(
        run_spec.parent_deep_attestation_artifact, reader
    )
    deep.verification_resource_observation.validate_caps(parent_spec.resource_caps)
    if (
        deep.completion_id != completion.completion_id
        or deep.completion_artifact != run_spec.parent_shadow_completion_artifact
        or deep.gate_a_manifest_pin != output_set.gate_a_manifest_pin
        or deep.checkpoint_id != output_set.checkpoint_id
        or deep.checkpoint_artifact != output_set.checkpoint_artifact
        or deep.output_set_id != output_set.output_set_id
        or deep.physical_index_digest != production_physical_index_digest(output_set)
        or deep.attestation_available_session > run_spec.run_available_session
    ):
        raise I3ProductionContractError("parent deep-verification attestation differs")

    gate_spec, gate_receipt, gate_manifest = _load_gate_a_controls_exact(root, output_set)
    if (
        completion.release_id != gate_manifest.release_id
        or output_set.gate_a_manifest_pin != run_spec.parent_gate_a_manifest
        or gate_manifest.resolved_content_digest != output_set.resolved_content_digest
        or gate_receipt.checkpoint is None
        or gate_receipt.checkpoint.artifact != output_set.checkpoint_artifact
        or gate_receipt.checkpoint.last_session != parent_spec.terminal_session
        or gate_spec.release_type.value != parent_spec.run_kind.value
        or gate_spec.parent_release_pin != parent_spec.parent_gate_a_manifest
        or gate_spec.source_cutoff_session != parent_spec.source_cutoff_session
        or gate_spec.availability_cutoff_session != parent_spec.run_available_session
        or gate_spec.release_available_session != parent_spec.run_available_session
        or gate_spec.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST
        or gate_spec.transform_semantics_digest != parent_spec.transform_semantics_digest
        or gate_spec.identity_policy_bundle_id
        != parent_spec.identity_policy_bundle.identity_policy_bundle_id
        or gate_spec.calendar_digest != parent_spec.calendar.calendar_artifact_id
        or gate_spec.input_pins != production_gate_a_input_pins(parent_spec)
        or gate_spec.wall_clock_cap_seconds is not None
    ):
        raise I3ProductionContractError("parent Gate-A controls differ")
    expected_row_attestation = _row_attestation_digest(gate_manifest, output_set.checkpoint_id)
    if deep.row_semantic_attestation_digest != expected_row_attestation:
        raise I3ProductionContractError("parent row-semantic attestation differs")
    if parent_spec.run_kind is I3ProductionRunKind.BASE:
        if deep.parent_frontier_attestation_digest is not None:
            raise I3ProductionContractError("base deep attestation carries a parent frontier")
    elif deep.parent_frontier_attestation_digest is None:
        raise I3ProductionContractError("delta deep attestation omits its parent frontier")

    manifest_content = _read_exact_root(root, output_set.release_manifest_artifact)
    manifest = NativeV2ReleaseManifest.from_dict(_strict_json_document(manifest_content))
    checkpoint_stored = _read_exact_checkpoint_root(root, output_set.checkpoint_artifact)
    checkpoint_content = i3_checkpoint_storage_payload(
        checkpoint_stored,
        path=output_set.checkpoint_artifact.path,
    )
    checkpoint = I3CheckpointState.from_dict(_strict_json_document(checkpoint_content))
    reproduced_native_parent = NativeV2ParentReleasePin.from_manifest(
        manifest, path=output_set.release_manifest_artifact.path
    )
    if (
        manifest.canonical_bytes() != manifest_content
        or manifest.release_id != output_set.release_id
        or reproduced_native_parent != run_spec.parent_release
        or deep.native_v2_release != reproduced_native_parent
        or tuple(item.manifest_output for item in output_set.table_outputs)
        != manifest.output_artifacts
        or checkpoint.canonical_bytes() != checkpoint_content
        or checkpoint.checkpoint_id != output_set.checkpoint_id
        or i3_checkpoint_storage_pin(checkpoint, path=output_set.checkpoint_artifact.path)
        != output_set.checkpoint_artifact
        or output_set.checkpoint_artifact != run_spec.parent_checkpoint_artifact
        or checkpoint.parent_release != reproduced_native_parent
        or checkpoint.last_session != parent_spec.terminal_session
        or checkpoint.resolved_state_digest != output_set.resolved_state_digest
        or checkpoint.resolved_content_digest != output_set.resolved_content_digest
    ):
        raise I3ProductionContractError("parent native/checkpoint envelope differs")

    for table_output in output_set.table_outputs:
        if table_output.dataset_index is not None:
            content = _read_exact_root(root, table_output.manifest_output.artifact)
            parsed = I3ProductionDatasetIndex.from_dict(_strict_json_document(content))
            if parsed != table_output.dataset_index or parsed.canonical_bytes() != content:
                raise I3ProductionContractError("parent dataset index bytes differ")
        elif table_output.rowset_index is not None:
            content = _read_exact_root(root, table_output.manifest_output.artifact)
            parsed_rowset = I3ProductionRowsetIndex.from_dict(_strict_json_document(content))
            if parsed_rowset != table_output.rowset_index or (
                parsed_rowset.canonical_bytes() != content
            ):
                raise I3ProductionContractError("parent rowset index bytes differ")

    if run_spec.parent_authority in {
        I3ProductionParentAuthority.EXACT_STAGING,
        I3ProductionParentAuthority.MIGRATION_SHADOW,
    }:
        if run_spec.parent_pointer_event_artifact is not None:
            raise I3ProductionContractError("exact staging parent carries a pointer event")
        if (
            run_spec.parent_authority is I3ProductionParentAuthority.MIGRATION_SHADOW
            and parent_spec.run_kind is not I3ProductionRunKind.BASE
        ):
            raise I3ProductionContractError(
                "legacy migration-shadow authority is restricted to the first delta after base"
            )
    else:
        raise I3ProductionContractError("production parent authority is unsupported")

    candidate = _validate_i3_release_projection_after_row_attestation(
        gate_spec,
        gate_receipt,
        gate_manifest,
        manifest_pin=output_set.gate_a_manifest_pin,
        parent_release=None,
        row_semantic_attestation_digest=deep.row_semantic_attestation_digest,
        parent_frontier_attestation_digest=(deep.parent_frontier_attestation_digest),
    )
    gate_release = _mint_content_attested_release(
        candidate,
        resolved_content_digest=gate_manifest.resolved_content_digest,
        snapshot_digest=deep.terminal_state_digest,
    )
    return LoadedI3ProductionStaging(
        completion=completion,
        receipt=receipt,
        run_spec=parent_spec,
        manifest=manifest,
        checkpoint=checkpoint,
        terminal_state_digest=deep.terminal_state_digest,
        gate_a_run_spec=gate_spec,
        gate_a_run_receipt=gate_receipt,
        gate_a_manifest=gate_manifest,
        gate_a_release=gate_release,
        deep_attestation=deep,
    )


def _row_attestation_digest(manifest: IncrementalReleaseManifest, checkpoint_id: str) -> str:
    if not manifest.added_row_version_receipts and manifest.row_version_change_index is None:
        raise I3ProductionContractError("Gate-A release omits versioned-table row changes")
    if manifest.row_version_change_index is not None:
        return _row_change_index_attestation_digest(
            manifest.row_version_change_index,
            checkpoint_id,
        )
    return stable_digest(
        {
            "checkpoint_id": checkpoint_id,
            "row_version_receipts": [
                item.to_dict() for item in manifest.added_row_version_receipts
            ],
            "rule_version": "s7_5_i3_production_row_semantic_attestation_v1",
        }
    )


def _row_change_index_attestation_digest(
    index: RowVersionChangeIndexPin,
    checkpoint_id: str,
) -> str:
    if not isinstance(index, RowVersionChangeIndexPin):
        raise I3ProductionContractError("row-version change index pin is invalid")
    _digest(checkpoint_id, "row-version change attestation checkpoint ID")
    return stable_digest(
        {
            "checkpoint_id": checkpoint_id,
            "row_version_change_index": index.to_dict(),
            "rule_version": _ROW_VERSION_CHANGE_ATTESTATION_RULE_VERSION,
        }
    )


def _row_change_index_proof_digest(row: Mapping[str, object]) -> str:
    payload = {key: value for key, value in row.items() if key != "semantic_proof_digest"}
    return stable_digest(
        {
            "row_change": _jsonable(payload),
            "rule_version": _ROW_VERSION_CHANGE_PROOF_RULE_VERSION,
        }
    )


def _row_change_index_logical_receipts_digest(
    rows: Sequence[Mapping[str, object]],
) -> str:
    return stable_digest(
        {
            "row_version_changes": [_jsonable(dict(row)) for row in rows],
            "rule_version": _ROW_VERSION_CHANGE_LOGICAL_RECEIPTS_RULE_VERSION,
        }
    )


def _row_change_index_supersession_digest(
    row_version_ids: Sequence[str],
) -> str:
    return stable_digest(
        {
            "rule_version": _ROW_VERSION_CHANGE_SUPERSESSION_RULE_VERSION,
            "superseded_row_version_ids": list(row_version_ids),
        }
    )


def load_i3_production_staging_exact(
    root: Path, completion_pin: ArtifactPin
) -> LoadedI3ProductionStaging:
    """Load and physically reconcile one production staging completion.

    The function performs exact-path reads only.  It authenticates the complete
    external production trust chain, the I2 receipts, all native-v2 outputs, the
    dataset index, the release manifest and the checkpoint.  It never discovers
    a latest artifact and returns no publication/cutover capability.
    """

    _artifact(completion_pin, "production completion pin")
    if _fixture_path(completion_pin.path):
        raise I3ProductionContractError(
            "fixture completion cannot acquire production staging authority"
        )
    if _temporary_path(completion_pin.path):
        raise I3ProductionContractError(
            "temporary completion cannot acquire production staging authority"
        )
    root = root.expanduser().resolve()

    def reader(relative: str) -> bytes:
        return _read_root_bytes(root, relative)

    completion = load_i3_production_completion_exact(completion_pin, reader)
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader)
    if (
        completion.receipt_id != receipt.receipt_id
        or completion.receipt_artifact != receipt.exact_pin(path=completion.receipt_artifact.path)
        or receipt.state is not I3ProductionRunState.SUCCEEDED
        or receipt.output_set is None
    ):
        raise I3ProductionContractError("completion and successful receipt do not reconcile")
    run_spec = load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader)
    if (
        receipt.run_spec_id != run_spec.run_spec_id
        or receipt.run_spec_artifact != run_spec.exact_pin(path=receipt.run_spec_artifact.path)
        or completion.run_spec_id != run_spec.run_spec_id
        or receipt.receipt_available_session < run_spec.run_available_session
        or completion.completion_available_session < receipt.receipt_available_session
    ):
        raise I3ProductionContractError("receipt and exact run spec do not reconcile")
    receipt.resource_observation.validate_caps(run_spec.resource_caps)
    output_set = receipt.output_set
    if (
        completion.output_set_id != output_set.output_set_id
        or completion.release_id != output_set.gate_a_manifest_pin.release_id
        or completion.native_v2_envelope_id != output_set.release_id
        or completion.checkpoint_id != output_set.checkpoint_id
        or output_set.total_output_bytes > run_spec.resource_caps.output_bytes_hard_cap
        or output_set.total_rows > run_spec.resource_caps.output_rows_hard_cap
    ):
        raise I3ProductionContractError("completion output identity or resource caps differ")

    validate_production_compact_base_initial_rowsets(run_spec, output_set.table_outputs)

    parent_staging = _verify_production_parent_exact(root, run_spec)
    if run_spec.run_kind is I3ProductionRunKind.DELTA:
        if parent_staging is None or parent_staging.receipt.output_set is None:
            raise I3ProductionContractError("DELTA staging lacks an authenticated parent OutputSet")
        validate_production_delta_append_outputs(
            run_spec,
            output_set.table_outputs,
            parent_staging.receipt.output_set,
        )
    calendar_sessions = _verify_external_production_dependencies(root, run_spec)
    _verify_i2_receipts_exact(
        root,
        run_spec,
        calendar_sessions=calendar_sessions,
        parent_staging=parent_staging,
    )
    source_checkpoint_id = (
        None if parent_staging is None else parent_staging.checkpoint.checkpoint_id
    )

    gate_spec, gate_receipt, gate_manifest = _load_gate_a_controls_exact(root, output_set)
    if (
        completion.release_id != gate_manifest.release_id
        or gate_manifest.release_id != output_set.gate_a_manifest_pin.release_id
        or gate_manifest.resolved_content_digest != output_set.resolved_content_digest
        or gate_receipt.checkpoint is None
        or gate_receipt.checkpoint.artifact != output_set.checkpoint_artifact
        or gate_receipt.checkpoint.last_session != run_spec.terminal_session
        or gate_spec.release_type.value != run_spec.run_kind.value
        or gate_spec.source_cutoff_session != run_spec.source_cutoff_session
        or gate_spec.availability_cutoff_session != run_spec.run_available_session
        or gate_spec.release_available_session != run_spec.run_available_session
        or gate_spec.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST
        or gate_spec.transform_semantics_digest != run_spec.transform_semantics_digest
        or gate_spec.identity_policy_bundle_id
        != run_spec.identity_policy_bundle.identity_policy_bundle_id
        or gate_spec.calendar_digest != run_spec.calendar.calendar_artifact_id
        or gate_spec.input_pins != production_gate_a_input_pins(run_spec)
        or gate_spec.rss_cap_bytes != run_spec.resource_caps.rss_bytes_hard_cap
        or gate_spec.disk_floor_bytes != run_spec.resource_caps.disk_free_bytes_hard_floor
        or gate_spec.wall_clock_cap_seconds is not None
    ):
        raise I3ProductionContractError("Gate-A controls differ from production staging controls")
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        if gate_spec.parent_release_pin is not None or parent_staging is not None:
            raise I3ProductionContractError("Gate-A base unexpectedly carries a parent")
    elif (
        parent_staging is None
        or gate_spec.parent_release_pin != run_spec.parent_gate_a_manifest
        or parent_staging.gate_a_release.manifest_pin != run_spec.parent_gate_a_manifest
    ):
        raise I3ProductionContractError("Gate-A delta parent authority differs")

    manifest_content = _read_exact_root(root, output_set.release_manifest_artifact)
    manifest = NativeV2ReleaseManifest.from_dict(_strict_json_document(manifest_content))
    if manifest.canonical_bytes() != manifest_content:
        raise I3ProductionContractError("production release manifest is not canonical JSON")
    if manifest.release_family != NATIVE_V2_RELEASE_FAMILY:
        raise I3ProductionContractError("fixture release cannot acquire production authority")
    if (
        manifest.release_id != output_set.release_id
        or manifest.terminal_session != run_spec.terminal_session
        or manifest.release_available_session != run_spec.run_available_session
        or manifest.identity_policy_bundle_id
        != run_spec.identity_policy_bundle.identity_policy_bundle_id
        or manifest.transform_semantics_digest != run_spec.transform_semantics_digest
        or manifest.resolved_state_digest != output_set.resolved_state_digest
        or manifest.native_v2_migration_id != run_spec.native_v2_migration_id
        or manifest.parent_release_id
        != (run_spec.parent_release.release_id if run_spec.parent_release else None)
        or manifest.source_checkpoint_id != source_checkpoint_id
        or tuple(item.manifest_output for item in output_set.table_outputs)
        != manifest.output_artifacts
    ):
        raise I3ProductionContractError("production manifest differs from RunSpec or OutputSet")

    checkpoint_stored = _read_exact_checkpoint_root(root, output_set.checkpoint_artifact)
    checkpoint_content = i3_checkpoint_storage_payload(
        checkpoint_stored,
        path=output_set.checkpoint_artifact.path,
    )
    checkpoint = I3CheckpointState.from_dict(_strict_json_document(checkpoint_content))
    if checkpoint.canonical_bytes() != checkpoint_content:
        raise I3ProductionContractError("production checkpoint is not canonical JSON")
    reproduced_parent = NativeV2ParentReleasePin.from_manifest(
        manifest, path=output_set.release_manifest_artifact.path
    )
    if (
        checkpoint.checkpoint_id != output_set.checkpoint_id
        or checkpoint.parent_release != reproduced_parent
        or checkpoint.resolved_state_digest != output_set.resolved_state_digest
        or checkpoint.resolved_content_digest != output_set.resolved_content_digest
        or checkpoint.last_session != run_spec.terminal_session
        or checkpoint.source_cutoff_session != run_spec.source_cutoff_session
        or checkpoint.availability_cutoff_session != run_spec.run_available_session
        or checkpoint.identity_policy_bundle != run_spec.identity_policy_bundle
        or checkpoint.identity_policy_bundle_artifact != run_spec.identity_policy_bundle_artifact
    ):
        raise I3ProductionContractError("production checkpoint differs from exact controls")
    if completion.completion_available_session < max(
        manifest.release_available_session,
        checkpoint.availability_cutoff_session,
    ):
        raise I3ProductionContractError("completion availability precedes native-v2 outputs")

    universe_output = output_set.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    if universe_output.dataset_index is None:  # defensive against post-construction corruption
        raise I3ProductionContractError("universe output lost its dataset index")
    index_content = _read_exact_root(root, universe_output.manifest_output.artifact)
    parsed_index = I3ProductionDatasetIndex.from_dict(_strict_json_document(index_content))
    if (
        parsed_index.canonical_bytes() != index_content
        or parsed_index != universe_output.dataset_index
    ):
        raise I3ProductionContractError("universe dataset-index bytes differ from OutputSet")
    _reconcile_universe_index(checkpoint, parsed_index)
    expected_calendar_sessions = tuple(
        session for session in calendar_sessions if session <= run_spec.terminal_session
    )
    if tuple(item.session_date for item in parsed_index.partitions) != expected_calendar_sessions:
        raise I3ProductionContractError(
            "universe dataset index does not cover the complete calendar through terminal"
        )
    expected_gate_partitions = (
        parsed_index.partitions
        if run_spec.run_kind is I3ProductionRunKind.BASE
        else parsed_index.partitions[-1:]
    )
    if len(gate_manifest.added_partition_receipts) != len(expected_gate_partitions):
        raise I3ProductionContractError(
            "Gate-A manifest does not bind the exact physical partition changes"
        )
    for gate_partition, physical_partition in zip(
        gate_manifest.added_partition_receipts, expected_gate_partitions, strict=True
    ):
        if (
            gate_partition.partition_key != physical_partition.session_date.isoformat()
            or gate_partition.receipt != physical_partition.artifact
            or gate_partition.row_count != physical_partition.row_count
            or gate_partition.schema_digest != physical_partition.schema_digest
            or gate_partition.availability_session != physical_partition.availability_session
        ):
            raise I3ProductionContractError(
                "Gate-A partition change differs from the physical dataset index"
            )
    parent_index: I3ProductionDatasetIndex | None = None
    if parent_staging is not None:
        parent_universe = parent_staging.receipt.output_set
        if parent_universe is None:  # pragma: no cover - successful parent invariant
            raise I3ProductionContractError("authenticated parent lost its output set")
        parent_index = parent_universe.table_outputs[
            I3_V2_TABLE_ORDER.index("universe_daily")
        ].dataset_index
        if parent_index is None or parsed_index.partitions[: len(parent_index.partitions)] != (
            parent_index.partitions
        ):
            raise I3ProductionContractError("delta changed an existing universe partition pin")

    expected_row_attestation_digest = _row_attestation_digest(
        gate_manifest, checkpoint.checkpoint_id
    )
    base_fk_summary = _load_gate_a_reference_extension_controls(
        root,
        run_spec,
        output_set,
        gate_spec,
        gate_receipt,
        gate_manifest,
        row_attestation_digest=expected_row_attestation_digest,
    )
    gate_partitions_by_session = {
        _date_from_json(item.partition_key, "Gate-A partition session"): item
        for item in gate_manifest.added_partition_receipts
    }
    verified_reference_sessions: set[date] = set()
    base_fk_session_digests: list[tuple[date, str]] = []
    base_fk_rows_checked = 0
    leaf_records: list[dict[str, object]] = []
    versioned_tables_by_artifact: dict[ArtifactPin, pa.Table] = {}
    available_row_versions: set[tuple[str, str]] = set()
    if parent_staging is not None:
        available_row_versions.update(
            (item.table_name, item.row_version_id)
            for item in parent_staging.checkpoint.terminal_row_versions
        )
    for table_output in output_set.table_outputs:
        if table_output.table_name == "universe_daily":
            partitions_to_verify = parsed_index.partitions
            if parent_index is not None:
                partitions_to_verify = (
                    parent_index.partitions[-_DELTA_BOUNDARY_PARTITION_COUNT:]
                    + parsed_index.partitions[len(parent_index.partitions) :]
                )
            for partition in partitions_to_verify:
                universe_table = _verify_parquet_exact(
                    root,
                    partition.artifact,
                    table_name="universe_daily",
                    row_count=partition.row_count,
                    session_date=partition.session_date,
                )
                gate_partition = gate_partitions_by_session.get(partition.session_date)
                if gate_partition is not None:
                    references = _universe_row_version_references(universe_table)
                    _verify_partition_row_references_exact(
                        run_spec,
                        gate_partition,
                        references=references,
                        available_row_versions=available_row_versions,
                    )
                    verified_reference_sessions.add(partition.session_date)
                    if run_spec.run_kind is I3ProductionRunKind.BASE:
                        base_fk_rows_checked += universe_table.num_rows
                        base_fk_session_digests.append(
                            (
                                partition.session_date,
                                _base_fk_session_reference_digest(
                                    partition.session_date,
                                    references,
                                ),
                            )
                        )
        elif table_output.storage is I3ProductionOutputStorage.PARQUET:
            if run_spec.run_kind is I3ProductionRunKind.BASE:
                raise I3ProductionContractError(
                    "compact BASE versioned outputs require rowset indexes"
                )
            if parent_staging is not None:
                raise I3ProductionContractError(
                    "delta versioned tables must use bounded append-only rowset indexes"
                )
            table = _verify_parquet_exact(
                root,
                table_output.manifest_output.artifact,
                table_name=table_output.table_name,
                row_count=table_output.manifest_output.row_count,
            )
            versioned_tables_by_artifact[table_output.manifest_output.artifact] = table
            available_row_versions.update(_physical_row_version_ids(table_output.table_name, table))
            leaf_records.extend(_terminal_leaf_records(table_output.table_name, table))
        else:
            rowset = table_output.rowset_index
            if rowset is None:  # pragma: no cover - constructor invariant
                raise I3ProductionContractError("versioned output lost its rowset index")
            rowset_content = _read_exact_root(root, table_output.manifest_output.artifact)
            parsed_rowset = I3ProductionRowsetIndex.from_dict(_strict_json_document(rowset_content))
            if parsed_rowset != rowset or parsed_rowset.canonical_bytes() != rowset_content:
                raise I3ProductionContractError("versioned rowset-index bytes differ")
            new_segment_offset = 0
            if parent_staging is not None:
                parent_output_set = parent_staging.receipt.output_set
                if parent_output_set is None:  # pragma: no cover
                    raise I3ProductionContractError("authenticated parent lost its output set")
                parent_output = parent_output_set.table_outputs[
                    I3_V2_TABLE_ORDER.index(table_output.table_name)
                ]
                parent_rowset = parent_output.rowset_index
                if parent_rowset is None or (
                    parsed_rowset.segments[: len(parent_rowset.segments)] != parent_rowset.segments
                ):
                    raise I3ProductionContractError(
                        "delta changed an existing versioned-table segment ID or pin"
                    )
                new_segment_offset = len(parent_rowset.segments)
            tables = []
            for segment in parsed_rowset.segments[new_segment_offset:]:
                table = _verify_parquet_exact(
                    root,
                    segment.artifact,
                    table_name=table_output.table_name,
                    row_count=segment.row_count,
                )
                versioned_tables_by_artifact[segment.artifact] = table
                available_row_versions.update(
                    _physical_row_version_ids(table_output.table_name, table)
                )
                tables.append(table)
            if parent_staging is None:
                leaf_records.extend(
                    _terminal_leaf_records(table_output.table_name, pa.concat_tables(tables))
                )
    terminal_state_digest = _checkpoint_terminal_state_digest(checkpoint)
    if parent_staging is None:
        _reconcile_terminal_rows(checkpoint, leaf_records)
    row_attestation_digest, changed_row_keys = _verify_gate_a_row_changes(
        root,
        gate_manifest,
        checkpoint,
        versioned_tables_by_artifact=versioned_tables_by_artifact,
        parent_staging=parent_staging,
    )
    if (
        row_attestation_digest != expected_row_attestation_digest
        or verified_reference_sessions != set(gate_partitions_by_session)
    ):
        raise I3ProductionContractError("Gate-A row/FK attestation coverage does not reproduce")
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        if (
            base_fk_summary is None
            or base_fk_summary.rows_checked != base_fk_rows_checked
            or base_fk_summary.logical_reference_digest
            != _base_fk_logical_reference_digest(base_fk_session_digests)
        ):
            raise I3ProductionContractError("base FK summary does not reproduce from data")
    elif base_fk_summary is not None:
        raise I3ProductionContractError("delta carries a base FK summary")
    if parent_staging is not None:
        _reconcile_delta_checkpoint(
            parent_staging.checkpoint,
            checkpoint,
            changed_row_keys,
        )
    candidate = _validate_i3_release_projection_after_row_attestation(
        gate_spec,
        gate_receipt,
        gate_manifest,
        manifest_pin=output_set.gate_a_manifest_pin,
        parent_release=(None if parent_staging is None else parent_staging.gate_a_release),
        row_semantic_attestation_digest=row_attestation_digest,
    )
    gate_a_release = _mint_content_attested_release(
        candidate,
        resolved_content_digest=gate_manifest.resolved_content_digest,
        snapshot_digest=terminal_state_digest,
    )
    return LoadedI3ProductionStaging(
        completion=completion,
        receipt=receipt,
        run_spec=run_spec,
        manifest=manifest,
        checkpoint=checkpoint,
        terminal_state_digest=terminal_state_digest,
        gate_a_run_spec=gate_spec,
        gate_a_run_receipt=gate_receipt,
        gate_a_manifest=gate_manifest,
        gate_a_release=gate_a_release,
    )


def _load_frozen_i0_oracle_marker_exact(
    root: Path,
    pin: ArtifactPin,
) -> dict[str, object]:
    """Exact-load the immutable v1 oracle without replaying its old runtime."""

    from ame_stocks_api.silver.incremental_i3_production_inputs import (
        I3ProductionInputError,
        load_frozen_s7_oracle_marker_exact,
    )

    content = _read_exact_root(root, pin)
    try:
        return load_frozen_s7_oracle_marker_exact(pin, content=content)
    except I3ProductionInputError as exc:
        raise I3ProductionContractError("frozen I0 oracle marker is invalid") from exc


def _verify_external_production_dependencies(
    root: Path, run_spec: I3ProductionRunSpec
) -> tuple[date, ...]:
    """Replay existing production loaders; no caller-injected fixture verifier exists."""

    from ame_stocks_api.silver import identity_materialization_streaming as streaming
    from ame_stocks_api.silver.asset_release_set import load_exact_asset_release_set_control
    from ame_stocks_api.silver.calendar_artifact import load_xnys_calendar_artifact
    from ame_stocks_api.silver.identity_registry_workflow import (
        RegistryReleasePin,
        load_registry_release_roots,
    )

    i0_marker = _load_frozen_i0_oracle_marker_exact(root, run_spec.i0_oracle.artifact)
    if i0_marker.get("release_set_id") != run_spec.i0_oracle.object_id:
        raise I3ProductionContractError("frozen I0 oracle returned another release set")

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
    source_binding_id = i0_marker["source_binding_id"]
    try:
        source_binding, _source_binding_pin = streaming._load_source_binding(
            root,
            source_binding_id,
        )
    except streaming.S7StreamingMaterializationError as exc:
        raise I3ProductionContractError("frozen I0 source binding is invalid") from exc
    bound_s4 = ArtifactPin(
        path=source_binding.s4_release_set_manifest.path,
        sha256=source_binding.s4_release_set_manifest.sha256,
        bytes=source_binding.s4_release_set_manifest.bytes,
    )
    expected_policy = IdentityPolicyBundle(
        registry_releases=tuple(
            IdentityRegistryReleasePin(
                registry_kind=IdentityRegistryKind(item.registry_name),
                release_id=item.release_id,
                artifact=ArtifactPin(
                    path=item.manifest_path,
                    sha256=item.manifest_sha256,
                    bytes=item.manifest_bytes,
                ),
                decision_cutoff_session=item.release_available_session,
                release_available_session=item.release_available_session,
            )
            for item in source_binding.registry_pins
        ),
        bundle_available_session=max(
            item.release_available_session for item in source_binding.registry_pins
        ),
    )
    expected_source_cutoff = max(
        expected_policy.decision_cutoff_session,
        run_spec.calendar.available_session,
    )
    if (
        source_binding.source_binding_id != source_binding_id
        or source_binding.mode != "production"
        or source_binding.s4_release_set_id != run_spec.s4_v1_source.object_id
        or bound_s4 != run_spec.s4_v1_source.artifact
        or tuple(source_binding.registry_pins) != registry_pins
        or source_binding.calendar_artifact_id != run_spec.calendar.calendar_artifact_id
        or source_binding.calendar_artifact_sha256 != run_spec.calendar.artifact.sha256
        or source_binding.cutoff_session != run_spec.s4_v1_source.available_session
        or source_binding.cutoff_session != run_spec.calendar.available_session
        or expected_policy != run_spec.identity_policy_bundle
        or run_spec.source_cutoff_session != expected_source_cutoff
    ):
        raise I3ProductionContractError(
            "frozen I0 source binding differs from exact S4, registry, policy, or calendar inputs"
        )

    _read_exact_root(root, run_spec.s4_v1_source.artifact)
    s4, s4_document = load_exact_asset_release_set_control(
        root,
        release_set_id=run_spec.s4_v1_source.object_id,
        expected_sha256=run_spec.s4_v1_source.artifact.sha256,
        expected_bytes=run_spec.s4_v1_source.artifact.bytes,
    )
    if (
        s4.release_set_id != run_spec.s4_v1_source.object_id
        or s4_document.path != run_spec.s4_v1_source.artifact.path
    ):
        raise I3ProductionContractError("S4 production control loader returned another release")

    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=run_spec.calendar.calendar_artifact_id,
        expected_sha256=run_spec.calendar.artifact.sha256,
    )
    observed_calendar_pin = ArtifactPin(
        path=calendar.relative_path,
        sha256=calendar.sha256,
        bytes=len(calendar.content),
    )
    if observed_calendar_pin != run_spec.calendar.artifact:
        raise I3ProductionContractError("calendar loader returned another exact artifact")
    required_sessions = {
        run_spec.terminal_session,
        *(item.session_date for item in run_spec.i2_receipts),
    }
    available_sessions = {item.session_date for item in calendar.sessions}
    if not required_sessions.issubset(available_sessions):
        raise I3ProductionContractError("production calendar omits a required session")

    # Generation already performs the one full registry decision/scope/evidence
    # replay needed to derive rows.  Re-expanding those historical trees after
    # materialization doubled daily runtime without changing a factor value.
    # Durable verification therefore rechecks the five immutable release roots
    # here, while continuing to replay S4, calendar, I2, checkpoint, Parquet,
    # and all materialized row/partition proofs below.
    load_registry_release_roots(root, registry_pins)
    policy_bytes = _read_exact_root(root, run_spec.identity_policy_bundle_artifact)
    if policy_bytes != run_spec.identity_policy_bundle.canonical_bytes():
        raise I3ProductionContractError("identity policy bundle bytes differ")
    return tuple(item.session_date for item in calendar.sessions)


def _verify_i2_receipts_exact(
    root: Path,
    run_spec: I3ProductionRunSpec,
    *,
    calendar_sessions: tuple[date, ...],
    parent_staging: LoadedI3ProductionStaging | None,
) -> None:
    from ame_stocks_api.silver.asset_incremental import load_completed_s4_asset_session_run
    from ame_stocks_api.silver.asset_incremental_contract import (
        S4BaseFrontier,
        S4ParentKind,
        S4SessionRunReceipt,
        S4SessionRunSpec,
    )

    calendar_index = {session: index for index, session in enumerate(calendar_sessions)}
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        expected_frontier = run_spec.i2_base_frontier
        if expected_frontier is None or run_spec.i2_receipts:
            raise I3ProductionContractError("base I2 frontier controls are incomplete")
        frontier_content = _read_exact_root(root, expected_frontier.artifact)
        frontier = S4BaseFrontier.from_dict(_strict_json_document(frontier_content))
        if (
            _canonical_json_bytes(frontier.to_dict()) != frontier_content
            or frontier.frontier_id != expected_frontier.frontier_id
            or frontier.terminal_session != expected_frontier.terminal_session
            or frontier.release_available_session != expected_frontier.frontier_available_session
            or frontier.base_release_set_id != run_spec.s4_v1_source.object_id
            or frontier.base_release_set_artifact != run_spec.s4_v1_source.artifact
            or frontier.calendar_artifact_id != run_spec.calendar.calendar_artifact_id
            or frontier.terminal_session != run_spec.terminal_session
        ):
            raise I3ProductionContractError(
                "I2 base frontier differs from exact S4 source or base RunSpec"
            )
        return
    if len(run_spec.i2_receipts) != 1:  # defensive against post-construction corruption
        raise I3ProductionContractError("I2 verification is bounded to the latest receipt")
    expected = run_spec.i2_receipts[0]
    for _ in (None,):
        receipt_content = _read_exact_root(root, expected.artifact)
        receipt = S4SessionRunReceipt.from_dict(_strict_json_document(receipt_content))
        if (
            _canonical_json_bytes(receipt.to_dict()) != receipt_content
            or receipt.receipt_id != expected.receipt_id
            or receipt.session_date != expected.session_date
            or receipt.receipt_available_session != expected.receipt_available_session
        ):
            raise I3ProductionContractError("I2 receipt does not reproduce its exact pin")
        spec_content = _read_exact_root(root, receipt.run_spec_artifact)
        spec = S4SessionRunSpec.from_dict(_strict_json_document(spec_content))
        if (
            _canonical_json_bytes(spec.to_dict()) != spec_content
            or spec.run_spec_id != receipt.run_spec_id
            or spec.source_binding.source_binding_id != receipt.source_binding_id
            or spec.source_binding.session_date != receipt.session_date
            or spec.receipt_available_session != receipt.receipt_available_session
        ):
            raise I3ProductionContractError("I2 RunSpec and receipt do not reconcile")
        loaded = load_completed_s4_asset_session_run(root, expected.session_date)
        if (
            loaded is None
            or loaded.receipt != receipt
            or loaded.receipt_artifact != expected.artifact
            or loaded.run_spec != spec
            or loaded.run_spec_artifact != receipt.run_spec_artifact
        ):
            raise I3ProductionContractError(
                "module-owned I2 production loader returned another completed run"
            )
        parent = spec.parent_frontier
        try:
            is_next = (
                calendar_index[expected.session_date] == calendar_index[parent.terminal_session] + 1
            )
        except KeyError as exc:
            raise I3ProductionContractError(
                "I2 parent/session is absent from the calendar"
            ) from exc
        if not is_next:
            raise I3ProductionContractError("I2 receipts do not advance by one exact session")
        if parent_staging is None:
            raise I3ProductionContractError("delta I2 verification lacks authenticated parent")
        prior_spec = parent_staging.run_spec
        if prior_spec.run_kind is I3ProductionRunKind.BASE:
            prior_frontier = prior_spec.i2_base_frontier
            if (
                prior_frontier is None
                or parent.parent_kind is not S4ParentKind.BASE_RELEASE
                or parent.terminal_session != prior_frontier.terminal_session
                or parent.terminal_receipt_id != prior_frontier.frontier_id
                or parent.artifact != prior_frontier.artifact
            ):
                raise I3ProductionContractError(
                    "first delta I2 receipt does not advance the authenticated base frontier"
                )
        else:
            prior_pin = prior_spec.i2_receipts[-1]
            if (
                parent.parent_kind is not S4ParentKind.SESSION_RECEIPT
                or parent.terminal_session != prior_pin.session_date
                or parent.terminal_receipt_id != prior_pin.receipt_id
                or parent.artifact != prior_pin.artifact
            ):
                raise I3ProductionContractError(
                    "latest I2 receipt does not advance the authenticated parent boundary"
                )
        for partition in receipt.partition_receipts:
            contract = spec.contract_ids_by_table[partition.table_name]
            schema = spec.schema_digests_by_table[partition.table_name]
            if partition.contract_id != contract or partition.schema_digest != schema:
                raise I3ProductionContractError("I2 partition contract differs from its RunSpec")
            _verify_exact_file(root, partition.artifact)
            path = safe_relative_path(root, partition.artifact.path)
            try:
                parquet = pq.ParquetFile(path)
            except (OSError, pa.ArrowException) as exc:
                raise I3ProductionContractError("I2 partition is not readable Parquet") from exc
            if (
                parquet.metadata.num_rows != partition.row_count
                or arrow_schema_digest(parquet.schema_arrow) != partition.schema_digest
            ):
                raise I3ProductionContractError("I2 partition bytes/schema/rows differ")
        _verify_exact_file(root, receipt.qa_details_artifact)


def _verify_production_parent_exact(
    root: Path, run_spec: I3ProductionRunSpec
) -> LoadedI3ProductionStaging | None:
    return load_i3_production_parent_shallow_exact(root, run_spec)


def _verify_parquet_exact(
    root: Path,
    artifact: ArtifactPin,
    *,
    table_name: str,
    row_count: int,
    session_date: date | None = None,
) -> pa.Table:
    _verify_exact_file(root, artifact)
    path = safe_relative_path(root, artifact.path)
    contract = I3_V2_CONTRACTS[table_name]
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, pa.ArrowException) as exc:
        raise I3ProductionContractError(f"{table_name} output is not readable Parquet") from exc
    if parquet.metadata.num_rows != row_count or not parquet.schema_arrow.equals(
        contract.arrow_schema
    ):
        raise I3ProductionContractError(f"{table_name} Parquet schema or row count differs")
    table = parquet.read()
    if session_date is not None:
        sessions = set(table["session_date"].to_pylist())
        if sessions != {session_date} and not (not sessions and row_count == 0):
            raise I3ProductionContractError("universe partition contains another session")
    return table


def _terminal_leaf_records(table_name: str, table: pa.Table) -> list[dict[str, object]]:
    key_field, version_field, predecessor_field, availability_field = _VERSION_FIELDS[table_name]
    rows = table.to_pylist()
    versions = {row[version_field] for row in rows}
    predecessors = {row[predecessor_field] for row in rows if row[predecessor_field] is not None}
    if not predecessors.issubset(versions):
        raise I3ProductionContractError(f"{table_name} contains an external predecessor")
    leaves: dict[str, dict[str, object]] = {}
    for row in rows:
        if row[version_field] in predecessors:
            continue
        stable_key = str(row[key_field])
        if stable_key in leaves:
            raise I3ProductionContractError(f"{table_name} contains multiple terminal leaves")
        leaves[stable_key] = {
            "availability_session": _jsonable(row[availability_field]),
            "predecessor_row_version_id": row[predecessor_field],
            "row_payload_digest": stable_digest(_jsonable(row)),
            "row_version_id": row[version_field],
            "stable_row_key": stable_key,
            "table_name": table_name,
        }
    if len(leaves) != len({str(row[key_field]) for row in rows}):
        raise I3ProductionContractError(f"{table_name} terminal leaf coverage is incomplete")
    return list(leaves.values())


def _reconcile_terminal_rows(
    checkpoint: I3CheckpointState, leaf_records: Sequence[Mapping[str, object]]
) -> None:
    expected = {
        (item.table_name, item.stable_row_key): {
            "availability_session": item.availability_session.isoformat(),
            "predecessor_row_version_id": item.predecessor_row_version_id,
            "row_payload_digest": item.row_payload_digest,
            "row_version_id": item.row_version_id,
            "stable_row_key": item.stable_row_key,
            "table_name": item.table_name,
        }
        for item in checkpoint.terminal_row_versions
    }
    observed = {
        (str(item["table_name"]), str(item["stable_row_key"])): dict(item) for item in leaf_records
    }
    if observed != expected:
        raise I3ProductionContractError("Parquet terminal leaves differ from checkpoint state")


def _checkpoint_terminal_state_digest(checkpoint: I3CheckpointState) -> str:
    records = [
        {
            "availability_session": item.availability_session.isoformat(),
            "predecessor_row_version_id": item.predecessor_row_version_id,
            "row_payload_digest": item.row_payload_digest,
            "row_version_id": item.row_version_id,
            "stable_row_key": item.stable_row_key,
            "table_name": item.table_name,
        }
        for item in checkpoint.terminal_row_versions
    ]
    return stable_digest(
        {
            "records": sorted(
                records,
                key=lambda item: (item["table_name"], item["stable_row_key"]),
            ),
            "rule_version": I3_PRODUCTION_TERMINAL_DIGEST_RULE_VERSION,
        }
    )


def _reconcile_delta_checkpoint(
    parent: I3CheckpointState,
    child: I3CheckpointState,
    changed_row_keys: tuple[tuple[str, str], ...],
) -> None:
    expected = {item.map_key: item for item in parent.terminal_row_versions}
    child_map = {item.map_key: item for item in child.terminal_row_versions}
    for key in changed_row_keys:
        terminal = child_map.get(key)
        if terminal is None:
            raise I3ProductionContractError(
                "delta row receipt is absent from the child terminal map"
            )
        expected[terminal.map_key] = terminal
    if child_map != expected:
        raise I3ProductionContractError("delta checkpoint changed an unattested terminal row")


def _physical_row_version_ids(table_name: str, table: pa.Table) -> set[tuple[str, str]]:
    version_field = _VERSION_FIELDS[table_name][1]
    return {
        (table_name, str(value)) for value in table[version_field].to_pylist() if value is not None
    }


def _universe_row_version_references(
    table: pa.Table,
) -> tuple[RowVersionReference, ...]:
    values: set[tuple[str, str]] = set()
    for field, table_name in (
        ("alias_resolution_version_id", "ticker_alias"),
        ("asset_master_version_id", "asset_master"),
        ("issuer_master_version_id", "issuer_master"),
    ):
        values.update(
            (table_name, str(value)) for value in table[field].to_pylist() if value is not None
        )
    return tuple(
        RowVersionReference(table_name=table_name, row_version_id=row_version_id)
        for table_name, row_version_id in sorted(values)
    )


def _base_fk_partition_set_digest(
    partitions: Sequence[I3ProductionPartitionPin],
) -> str:
    return stable_digest(
        {
            "partitions": [item.to_dict() for item in partitions],
            "rule_version": _BASE_FK_PARTITION_SET_RULE_VERSION,
        }
    )


def _base_fk_session_reference_digest(
    session: date,
    references: Sequence[RowVersionReference],
) -> str:
    return stable_digest(
        {
            "references": [item.to_dict() for item in references],
            "rule_version": _BASE_FK_LOGICAL_REFERENCE_RULE_VERSION,
            "session_date": session.isoformat(),
        }
    )


def _base_fk_logical_reference_digest(
    session_digests: Sequence[tuple[date, str]],
) -> str:
    return stable_digest(
        {
            "rule_version": _BASE_FK_LOGICAL_REFERENCE_RULE_VERSION,
            "session_reference_digests": [
                {"digest": digest, "session_date": session.isoformat()}
                for session, digest in session_digests
            ],
        }
    )


def _load_gate_a_reference_extension_controls(
    root: Path,
    run_spec: I3ProductionRunSpec,
    output_set: I3ProductionOutputSet,
    gate_spec: RunSpec,
    gate_receipt: RunReceipt,
    gate_manifest: IncrementalReleaseManifest,
    *,
    row_attestation_digest: str,
) -> I3ProductionBaseFkVerificationSummary | None:
    """Load bounded QA controls for I3's compact base FK extension.

    Generic Gate-A treats an empty base ``row_version_references`` tuple as no
    deep FK claim.  The data loop independently replays every physical
    universe row; this control stores only one compact digest receipt.
    """

    qa = gate_receipt.qa_receipt
    if qa is None or not qa.results:
        raise I3ProductionContractError("Gate-A QA receipt omits production details")
    detail_pins = {
        ArtifactPin(
            path=item.details_artifact.path,
            sha256=item.details_artifact.sha256,
            bytes=item.details_artifact.bytes,
        )
        for item in qa.results
    }
    if len(detail_pins) != 1:
        raise I3ProductionContractError("Gate-A QA checks bind different detail artifacts")
    details_pin = next(iter(detail_pins))
    details_content = _read_exact_root(root, details_pin)
    details = _closed_mapping(
        _strict_json_document(details_content),
        {
            "base_fk_verification_summary",
            "canonical_projection_difference_count",
            "native_v2_manifest_artifact",
            "physical_source_digest",
            "row_semantic_attestation_digest",
            "rule_version",
            "run_spec_id",
            "table_outputs",
        },
        "Gate-A production QA details",
    )
    _literal(
        details["rule_version"],
        "s7_5_i3_production_gate_a_qa_details_v1",
        "Gate-A QA details rule",
    )
    _digest(details["physical_source_digest"], "physical source digest")
    if (
        _integer(
            details["canonical_projection_difference_count"],
            "canonical projection difference count",
        )
        != 0
        or _artifact_from_dict(details["native_v2_manifest_artifact"], "native-v2 QA artifact")
        != output_set.release_manifest_artifact
        or _text(details["row_semantic_attestation_digest"], "row attestation")
        != row_attestation_digest
        or _text(details["run_spec_id"], "Gate-A QA RunSpec ID") != gate_spec.run_spec_id
        or details["table_outputs"] != [item.to_dict() for item in output_set.table_outputs]
        or details_content != _canonical_json_bytes(details)
    ):
        raise I3ProductionContractError("Gate-A production QA details differ")
    gate_by_session = {
        _date_from_json(item.partition_key, "Gate-A partition session"): item
        for item in gate_manifest.added_partition_receipts
    }
    row_change_index = gate_manifest.row_version_change_index
    if row_change_index is None:
        raise I3ProductionContractError(
            "production Gate-A manifest omits its compact row-change index"
        )
    if run_spec.run_kind is I3ProductionRunKind.DELTA:
        if details["base_fk_verification_summary"] is not None:
            raise I3ProductionContractError("delta unexpectedly carries a base FK summary")
        if set(output_set.control_extension_artifacts) != {
            details_pin,
            row_change_index.artifact,
        }:
            raise I3ProductionContractError("delta control extension pin set differs")
        return None
    if any(item.row_version_references for item in gate_by_session.values()):
        raise I3ProductionContractError("base Gate-A receipts repeat inline FK references")
    summary_pin = _artifact_from_dict(
        details["base_fk_verification_summary"], "base FK verification summary"
    )
    summary_content = _read_exact_root(root, summary_pin)
    summary = I3ProductionBaseFkVerificationSummary.from_dict(
        _strict_json_document(summary_content)
    )
    universe_output = output_set.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    dataset_index = universe_output.dataset_index
    if (
        dataset_index is None
        or summary.canonical_bytes() != summary_content
        or summary.session_count != len(dataset_index.partitions)
        or summary.input_partition_set_digest
        != _base_fk_partition_set_digest(dataset_index.partitions)
        or summary.summary_available_session != run_spec.run_available_session
    ):
        raise I3ProductionContractError("base FK summary differs from its dataset index")
    if set(output_set.control_extension_artifacts) != {
        details_pin,
        row_change_index.artifact,
        summary_pin,
    }:
        raise I3ProductionContractError("base control extension pin set differs")
    return summary


def _verify_partition_row_references_exact(
    run_spec: I3ProductionRunSpec,
    gate_partition: PartitionReceipt,
    *,
    references: tuple[RowVersionReference, ...],
    available_row_versions: set[tuple[str, str]],
) -> None:
    missing = {
        (item.table_name, item.row_version_id) for item in references
    } - available_row_versions
    if missing:
        raise I3ProductionContractError(
            "universe partition references an absent physical row version"
        )
    if run_spec.run_kind is I3ProductionRunKind.DELTA:
        if gate_partition.row_version_references != references:
            raise I3ProductionContractError(
                "delta partition inline row references differ from physical rows"
            )
        return
    if gate_partition.row_version_references:
        raise I3ProductionContractError("base Gate-A receipt repeats inline FK references")


def _verify_gate_a_row_changes(
    root: Path,
    manifest: IncrementalReleaseManifest,
    checkpoint: I3CheckpointState,
    *,
    versioned_tables_by_artifact: Mapping[ArtifactPin, pa.Table],
    parent_staging: LoadedI3ProductionStaging | None,
) -> tuple[str, tuple[tuple[str, str], ...]]:
    if manifest.row_version_change_index is None:
        digest = _verify_gate_a_inline_row_receipts(
            root,
            manifest,
            checkpoint,
            versioned_tables_by_artifact=versioned_tables_by_artifact,
            parent_staging=parent_staging,
        )
        return digest, tuple(
            (item.table_name, item.stable_row_key) for item in manifest.added_row_version_receipts
        )
    return _verify_gate_a_indexed_row_changes(
        root,
        manifest,
        checkpoint,
        versioned_tables_by_artifact=versioned_tables_by_artifact,
        parent_staging=parent_staging,
    )


def _verify_gate_a_indexed_row_changes(
    root: Path,
    manifest: IncrementalReleaseManifest,
    checkpoint: I3CheckpointState,
    *,
    versioned_tables_by_artifact: Mapping[ArtifactPin, pa.Table],
    parent_staging: LoadedI3ProductionStaging | None,
) -> tuple[str, tuple[tuple[str, str], ...]]:
    index = manifest.row_version_change_index
    if index is None:  # pragma: no cover - caller branch
        raise I3ProductionContractError("Gate-A row-version change index is absent")
    if index.schema_digest != _ROW_VERSION_CHANGE_INDEX_SCHEMA_DIGEST:
        raise I3ProductionContractError("row-version change index schema digest differs")
    _verify_exact_file(root, index.artifact)
    try:
        table = pq.read_table(safe_relative_path(root, index.artifact.path))
    except (OSError, pa.ArrowException) as exc:
        raise I3ProductionContractError("row-version change index is not readable Parquet") from exc
    rows = table.to_pylist()
    keys = tuple((str(item["table_name"]), str(item["stable_row_key"])) for item in rows)
    if (
        not table.schema.equals(_ROW_VERSION_CHANGE_INDEX_SCHEMA)
        or table.num_rows != index.row_count
        or keys != tuple(sorted(set(keys)))
        or _row_change_index_logical_receipts_digest(rows) != index.logical_receipts_digest
    ):
        raise I3ProductionContractError(
            "row-version change index schema/order/count/digest differs"
        )
    superseded = tuple(
        sorted(
            str(item["predecessor_row_version_id"])
            for item in rows
            if item["predecessor_row_version_id"] is not None
        )
    )
    if (
        len(superseded) != len(set(superseded))
        or len(superseded) != index.superseded_row_version_count
        or _row_change_index_supersession_digest(superseded)
        != index.superseded_row_version_ids_digest
    ):
        raise I3ProductionContractError("row-version change index supersession projection differs")

    parent_terminal = (
        {}
        if parent_staging is None
        else {item.map_key: item for item in parent_staging.checkpoint.terminal_row_versions}
    )
    child_terminal = {item.map_key: item for item in checkpoint.terminal_row_versions}
    observed_locators: set[tuple[ArtifactPin, int]] = set()
    for row in rows:
        table_name = str(row["table_name"])
        stable_key = str(row["stable_row_key"])
        if table_name not in _VERSIONED_TABLES:
            raise I3ProductionContractError("row-version change index contains another table")
        try:
            operation = RowVersionOperation(str(row["operation"]))
        except ValueError as exc:
            raise I3ProductionContractError(
                "row-version change index operation is invalid"
            ) from exc
        predecessor = row["predecessor_row_version_id"]
        predecessor_payload = row["predecessor_payload_digest"]
        availability = row["availability_session"]
        if type(availability) is not date or availability > manifest.availability_cutoff_session:
            raise I3ProductionContractError("indexed row availability exceeds the release cutoff")
        for value, label in (
            (str(row["row_version_id"]), "indexed row-version ID"),
            (str(row["row_payload_digest"]), "indexed row payload digest"),
            (
                str(row["validator_semantics_digest"]),
                "indexed validator semantics digest",
            ),
            (str(row["semantic_proof_digest"]), "indexed semantic proof digest"),
            (str(row["index_artifact_sha256"]), "indexed physical artifact digest"),
        ):
            _digest(value, label)
        if predecessor is not None:
            _digest(str(predecessor), "indexed predecessor row-version ID")
        if predecessor_payload is not None:
            _digest(str(predecessor_payload), "indexed predecessor payload digest")
        expected_proof = _row_change_index_proof_digest(row)
        if row["semantic_proof_digest"] != expected_proof:
            raise I3ProductionContractError("indexed row semantic proof digest does not reproduce")
        if manifest.release_type is ReleaseType.BASE:
            expected_validator = production_compact_base_row_validator_digest(
                table_name=table_name,
                schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
            )
            if (
                operation is not RowVersionOperation.NEW_ROOT
                or predecessor is not None
                or predecessor_payload is not None
                or row["validator_semantics_digest"] != expected_validator
            ):
                raise I3ProductionContractError(
                    "indexed compact-base row semantics are not module-owned new roots"
                )
        else:
            if operation not in {
                RowVersionOperation.NEW_ROOT,
                RowVersionOperation.MECHANICAL_SUCCESSOR,
            }:
                raise I3ProductionContractError("indexed DELTA row semantics are not module-owned")
            expected_validator = production_delta_row_validator_digest(
                table_name=table_name,
                schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                operation=operation.value,
            )
            if (
                availability != manifest.availability_cutoff_session
                or row["validator_semantics_digest"] != expected_validator
            ):
                raise I3ProductionContractError("indexed DELTA row semantics are not module-owned")
        artifact = ArtifactPin(
            path=str(row["index_artifact_path"]),
            sha256=str(row["index_artifact_sha256"]),
            bytes=int(row["index_artifact_bytes"]),
        )
        physical = versioned_tables_by_artifact.get(artifact)
        if physical is None:
            raise I3ProductionContractError(
                "indexed row physical artifact is absent from the new rowset"
            )
        row_index = _row_locator_index(str(row["row_locator"]))
        locator = (artifact, row_index)
        if locator in observed_locators or row_index >= physical.num_rows:
            raise I3ProductionContractError("indexed row locator is duplicate or out of range")
        observed_locators.add(locator)
        physical_row = physical.slice(row_index, 1).to_pylist()[0]
        key_field, version_field, predecessor_field, availability_field = _VERSION_FIELDS[
            table_name
        ]
        if (
            str(physical_row[key_field]) != stable_key
            or physical_row[version_field] != row["row_version_id"]
            or physical_row[predecessor_field] != predecessor
            or physical_row[availability_field] != availability
            or stable_digest(_jsonable(physical_row)) != row["row_payload_digest"]
        ):
            raise I3ProductionContractError(
                "indexed row differs from its exact physical Parquet row"
            )
        prior = parent_terminal.get((table_name, stable_key))
        if operation is RowVersionOperation.NEW_ROOT:
            if prior is not None:
                raise I3ProductionContractError("indexed new-root row reuses a parent stable key")
        elif (
            prior is None
            or prior.row_version_id != predecessor
            or prior.row_payload_digest != predecessor_payload
        ):
            raise I3ProductionContractError(
                "indexed successor differs from authenticated parent terminal state"
            )
        terminal = child_terminal.get((table_name, stable_key))
        if (
            terminal is None
            or terminal.row_version_id != row["row_version_id"]
            or terminal.predecessor_row_version_id != predecessor
            or terminal.row_payload_digest != row["row_payload_digest"]
            or terminal.availability_session != availability
        ):
            raise I3ProductionContractError(
                "indexed row differs from the child checkpoint terminal map"
            )

    expected_locators = _new_physical_row_locators(
        versioned_tables_by_artifact,
        parent_staging=parent_staging,
    )
    if observed_locators != expected_locators:
        raise I3ProductionContractError(
            "row-version change index does not cover the exact new physical rowset"
        )
    return _row_attestation_digest(manifest, checkpoint.checkpoint_id), keys


def _new_physical_row_locators(
    versioned_tables_by_artifact: Mapping[ArtifactPin, pa.Table],
    *,
    parent_staging: LoadedI3ProductionStaging | None,
) -> set[tuple[ArtifactPin, int]]:
    new_artifacts = set(versioned_tables_by_artifact)
    if parent_staging is not None:
        parent_output = parent_staging.receipt.output_set
        if parent_output is None:  # pragma: no cover
            raise I3ProductionContractError("authenticated parent lost its output set")
        for table_output in parent_output.table_outputs:
            if table_output.rowset_index is not None:
                new_artifacts.difference_update(
                    segment.artifact for segment in table_output.rowset_index.segments
                )
            elif table_output.table_name != "universe_daily":
                new_artifacts.discard(table_output.manifest_output.artifact)
    return {
        (artifact, row_index)
        for artifact in new_artifacts
        for row_index in range(versioned_tables_by_artifact[artifact].num_rows)
    }


def _verify_gate_a_inline_row_receipts(
    root: Path,
    manifest: IncrementalReleaseManifest,
    checkpoint: I3CheckpointState,
    *,
    versioned_tables_by_artifact: Mapping[ArtifactPin, pa.Table],
    parent_staging: LoadedI3ProductionStaging | None,
) -> str:
    if not manifest.added_row_version_receipts:
        raise I3ProductionContractError("Gate-A release omits versioned-table row changes")
    parent_terminal = (
        {}
        if parent_staging is None
        else {item.map_key: item for item in parent_staging.checkpoint.terminal_row_versions}
    )
    child_terminal = {item.map_key: item for item in checkpoint.terminal_row_versions}
    observed_locators: set[tuple[ArtifactPin, int]] = set()
    for receipt in manifest.added_row_version_receipts:
        proof_content = _read_exact_root(root, receipt.semantic_proof.artifact)
        proof_body = {
            "artifact_type": "s7_5_i3_production_row_semantic_proof",
            "operation": receipt.operation.value,
            "predecessor_payload_digest": (receipt.semantic_proof.predecessor_payload_digest),
            "predecessor_row_version_id": receipt.predecessor_row_version_id,
            "row_payload_digest": receipt.row_payload_digest,
            "row_version_id": receipt.row_version_id,
            "rule_version": "s7_5_i3_production_row_semantic_proof_v1",
            "stable_row_key": receipt.stable_row_key,
            "table_name": receipt.table_name,
            "validator_semantics_digest": (receipt.semantic_proof.validator_semantics_digest),
        }
        expected_proof = {
            "proof_id": stable_digest(proof_body),
            **proof_body,
        }
        if proof_content != _canonical_json_bytes(expected_proof):
            raise I3ProductionContractError("row semantic-proof bytes do not reproduce")
        table = versioned_tables_by_artifact.get(receipt.index_artifact)
        if table is None:
            raise I3ProductionContractError(
                "row receipt index artifact is absent from the physical rowset"
            )
        row_index = _row_locator_index(receipt.row_locator)
        locator = (receipt.index_artifact, row_index)
        if locator in observed_locators or row_index >= table.num_rows:
            raise I3ProductionContractError("row receipt locator is duplicate or out of range")
        observed_locators.add(locator)
        row = table.slice(row_index, 1).to_pylist()[0]
        key_field, version_field, predecessor_field, availability_field = _VERSION_FIELDS[
            receipt.table_name
        ]
        if (
            str(row[key_field]) != receipt.stable_row_key
            or row[version_field] != receipt.row_version_id
            or row[predecessor_field] != receipt.predecessor_row_version_id
            or row[availability_field] != receipt.availability_session
            or stable_digest(_jsonable(row)) != receipt.row_payload_digest
        ):
            raise I3ProductionContractError(
                "row receipt differs from its exact physical Parquet row"
            )
        prior = parent_terminal.get((receipt.table_name, receipt.stable_row_key))
        if receipt.operation is RowVersionOperation.NEW_ROOT:
            if prior is not None:
                raise I3ProductionContractError("new-root row reuses a parent stable key")
        elif (
            receipt.operation is not RowVersionOperation.MECHANICAL_SUCCESSOR
            or prior is None
            or prior.row_version_id != receipt.predecessor_row_version_id
            or prior.row_payload_digest != receipt.semantic_proof.predecessor_payload_digest
        ):
            raise I3ProductionContractError(
                "row successor does not match the authenticated parent terminal row"
            )
        terminal = child_terminal.get((receipt.table_name, receipt.stable_row_key))
        if (
            terminal is None
            or terminal.row_version_id != receipt.row_version_id
            or terminal.predecessor_row_version_id != receipt.predecessor_row_version_id
            or terminal.row_payload_digest != receipt.row_payload_digest
            or terminal.availability_session != receipt.availability_session
        ):
            raise I3ProductionContractError(
                "row receipt differs from the child checkpoint terminal map"
            )

    expected_locators = _new_physical_row_locators(
        versioned_tables_by_artifact,
        parent_staging=parent_staging,
    )
    if observed_locators != expected_locators:
        raise I3ProductionContractError(
            "Gate-A row receipts do not cover the exact new physical rowset"
        )
    return _row_attestation_digest(manifest, checkpoint.checkpoint_id)


def _row_locator_index(value: str) -> int:
    prefix = "row_index="
    if not value.startswith(prefix):
        raise I3ProductionContractError("row locator is not canonical")
    raw = value[len(prefix) :]
    if not raw.isdigit() or (raw != "0" and raw.startswith("0")):
        raise I3ProductionContractError("row locator is not canonical")
    return int(raw)


def _reconcile_universe_index(
    checkpoint: I3CheckpointState, index: I3ProductionDatasetIndex
) -> None:
    expected = tuple(
        (
            item.session_date,
            item.partition_receipt_id,
            item.artifact,
            item.row_count,
            item.availability_session,
        )
        for item in checkpoint.resolved_partition_map
    )
    observed = tuple(
        (
            item.session_date,
            item.partition_receipt_id,
            item.artifact,
            item.row_count,
            item.availability_session,
        )
        for item in index.partitions
    )
    if observed != expected:
        raise I3ProductionContractError(
            "universe dataset index differs from checkpoint resolved partitions"
        )


def _read_exact_bytes(pin: ArtifactPin, reader: Callable[[str], bytes], *, label: str) -> bytes:
    _artifact(pin, label)
    if not callable(reader):
        raise I3ProductionContractError("exact reader must be callable")
    content = reader(pin.path)
    if type(content) is not bytes:
        raise I3ProductionContractError("exact reader must return bytes")
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I3ProductionContractError(f"{label} bytes differ from their exact pin")
    return content


def _read_root_bytes(root: Path, relative: str) -> bytes:
    return _read_root_bytes_with_cap(
        root,
        relative,
        byte_cap=_CONTROL_JSON_BYTES_CAP,
        artifact_label="control JSON",
    )


def _read_checkpoint_root_bytes(root: Path, relative: str) -> bytes:
    return _read_root_bytes_with_cap(
        root,
        relative,
        byte_cap=_CHECKPOINT_JSON_BYTES_CAP,
        artifact_label="checkpoint JSON",
    )


def _read_root_bytes_with_cap(
    root: Path,
    relative: str,
    *,
    byte_cap: int,
    artifact_label: str,
) -> bytes:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionContractError(f"exact artifact is missing: {relative}")
    if type(byte_cap) is not int or byte_cap <= 0:
        raise I3ProductionContractError("exact artifact byte cap is invalid")
    if path.stat().st_size > byte_cap:
        raise I3ProductionContractError(f"{artifact_label} exceeds its hard byte cap")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise I3ProductionContractError(f"cannot read exact artifact: {relative}") from exc


def _read_exact_root(root: Path, pin: ArtifactPin) -> bytes:
    return _read_exact_bytes(pin, lambda path: _read_root_bytes(root, path), label=pin.path)


def _read_exact_checkpoint_root(root: Path, pin: ArtifactPin) -> bytes:
    return _read_exact_bytes(
        pin,
        lambda path: _read_checkpoint_root_bytes(root, path),
        label=pin.path,
    )


def _verify_exact_file(root: Path, pin: ArtifactPin) -> None:
    path = safe_relative_path(root, pin.path)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionContractError(f"exact artifact is missing: {pin.path}")
    stat = path.stat()
    if stat.st_size != pin.bytes or sha256_file(path) != pin.sha256:
        raise I3ProductionContractError(f"exact artifact differs from pin: {pin.path}")


def _strict_json_document(content: bytes) -> object:
    def reject_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise I3ProductionContractError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise I3ProductionContractError(f"non-finite JSON value is forbidden: {value}")

    try:
        return json.loads(
            content.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except I3ProductionContractError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3ProductionContractError("artifact is not strict UTF-8 JSON") from exc


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        + b"\n"
    )


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        instant = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return instant.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _i0_oracle_path() -> str:
    return (
        "manifests/silver/identity/s7-four-table-release-sets/"
        f"release_set_id={LEGACY_S7_V1_RELEASE_SET_ID}/manifest.json"
    )


def _s4_v1_path() -> str:
    return (
        f"manifests/silver/release-sets/assets/release_set_id={S4_V1_RELEASE_SET_ID}/manifest.json"
    )


def _fixture_path(path: str) -> bool:
    return path.startswith("fixtures/") or "/fixtures/" in f"/{path}"


def _temporary_path(path: str) -> bool:
    return path == "tmp" or path.startswith("tmp/")


def _closed_mapping(value: object, fields: set[str], label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise I3ProductionContractError(f"{label} must be an object")
    if set(value) != fields:
        raise I3ProductionContractError(f"{label} fields differ")
    return value


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise I3ProductionContractError(f"{label} must be an array")
    return value


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise I3ProductionContractError(f"{label} must be an exact ArtifactPin")
    return value


def _artifact_from_dict(value: object, label: str) -> ArtifactPin:
    item = _closed_mapping(value, {"bytes", "path", "sha256"}, label)
    try:
        return ArtifactPin(
            path=_text(item["path"], f"{label} path"),
            sha256=_text(item["sha256"], f"{label} SHA-256"),
            bytes=_integer(item["bytes"], f"{label} bytes"),
        )
    except ValueError as exc:
        raise I3ProductionContractError(f"{label} is invalid") from exc


def _artifact_from_manifest_pin(value: ManifestPin) -> ArtifactPin:
    if not isinstance(value, ManifestPin):
        raise I3ProductionContractError("Gate-A manifest pin is invalid")
    return ArtifactPin(path=value.path, sha256=value.sha256, bytes=value.bytes)


def _manifest_pin_from_dict(value: object) -> ManifestPin:
    item = _closed_mapping(
        value,
        {
            "manifest_bytes",
            "manifest_path",
            "manifest_sha256",
            "release_available_session",
            "release_id",
        },
        "Gate-A manifest pin",
    )
    try:
        return ManifestPin(
            release_id=_text(item["release_id"], "Gate-A release ID"),
            manifest_path=_text(item["manifest_path"], "Gate-A manifest path"),
            manifest_sha256=_text(item["manifest_sha256"], "Gate-A manifest SHA-256"),
            manifest_bytes=_integer(item["manifest_bytes"], "Gate-A manifest bytes"),
            release_available_session=_date_from_json(
                item["release_available_session"], "Gate-A release availability"
            ),
        )
    except ValueError as exc:
        raise I3ProductionContractError("Gate-A manifest pin is invalid") from exc


def _control_object_pin_from_dict(
    value: object, expected_kind: ControlObjectKind
) -> ControlObjectPin:
    item = _closed_mapping(
        value,
        {"artifact", "object_id", "object_kind"},
        "Gate-A control-object pin",
    )
    _literal(item["object_kind"], expected_kind.value, "Gate-A control-object kind")
    try:
        return ControlObjectPin(
            object_kind=expected_kind,
            object_id=_text(item["object_id"], "Gate-A control-object ID"),
            artifact=_artifact_from_dict(item["artifact"], "Gate-A control-object artifact"),
        )
    except ValueError as exc:
        raise I3ProductionContractError("Gate-A control-object pin is invalid") from exc


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I3ProductionContractError(f"{label} must be nonempty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _parent_authority_from_json(value: object) -> I3ProductionParentAuthority:
    try:
        return I3ProductionParentAuthority(_text(value, "production parent authority"))
    except ValueError as exc:
        raise I3ProductionContractError("production parent authority is invalid") from exc


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise I3ProductionContractError(f"{label} must be a lowercase SHA-256")
    return text


def _token(value: object, label: str) -> str:
    text = _text(value, label)
    if not _TOKEN.fullmatch(text):
        raise I3ProductionContractError(f"{label} is not a canonical token")
    return text


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise I3ProductionContractError(f"{label} must be an integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _integer(value, label)
    if result <= 0:
        raise I3ProductionContractError(f"{label} must be positive")
    return result


def _nonnegative_int(value: object, label: str) -> int:
    result = _integer(value, label)
    if result < 0:
        raise I3ProductionContractError(f"{label} must be nonnegative")
    return result


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise I3ProductionContractError(f"{label} must be a native date")
    return value


def _date_from_json(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        result = date.fromisoformat(text)
    except ValueError as exc:
        raise I3ProductionContractError(f"{label} is not an ISO date") from exc
    if result.isoformat() != text:
        raise I3ProductionContractError(f"{label} is not canonical")
    return result


def _literal(value: object, expected: object, label: str) -> None:
    if value != expected:
        raise I3ProductionContractError(f"{label} changed")


def _unique_paths(values: Sequence[ArtifactPin], label: str) -> None:
    paths = [item.path for item in values]
    if len(paths) != len(set(paths)):
        raise I3ProductionContractError(f"{label} paths must be unique")


__all__ = [
    "I0_ORACLE_AVAILABLE_SESSION",
    "I0_ORACLE_RELEASE_SET_BYTES",
    "I0_ORACLE_RELEASE_SET_SHA256",
    "I3_PRODUCTION_COMPLETION_RULE_VERSION",
    "I3_PRODUCTION_DATASET_INDEX_RULE_VERSION",
    "I3_PRODUCTION_DEEP_ATTESTATION_RULE_VERSION",
    "I3_PRODUCTION_DELTA_RUN_SPEC_RULE_VERSION",
    "I3_PRODUCTION_NAMESPACE",
    "I3_PRODUCTION_OUTPUT_SET_RULE_VERSION",
    "I3_PRODUCTION_PHYSICAL_INDEX_DIGEST_RULE_VERSION",
    "I3_PRODUCTION_ROWSET_INDEX_RULE_VERSION",
    "I3_PRODUCTION_RUN_RECEIPT_RULE_VERSION",
    "I3_PRODUCTION_RUN_SPEC_RULE_VERSION",
    "I3_PRODUCTION_TERMINAL_DIGEST_RULE_VERSION",
    "S4_V1_RELEASE_SET_BYTES",
    "S4_V1_RELEASE_SET_ID",
    "S4_V1_RELEASE_SET_SHA256",
    "I3ProductionCalendarPin",
    "I3ProductionCompletion",
    "I3ProductionCompletionState",
    "I3ProductionContractError",
    "I3ProductionDatasetIndex",
    "I3ProductionDeepVerificationAttestation",
    "I3ProductionDependencyPin",
    "I3ProductionDependencyRole",
    "I3ProductionI2BaseFrontierPin",
    "I3ProductionI2ReceiptPin",
    "I3ProductionOutputSet",
    "I3ProductionOutputStorage",
    "I3ProductionParentAuthority",
    "I3ProductionPartitionPin",
    "I3ProductionResourceCaps",
    "I3ProductionResourceObservation",
    "I3ProductionRowsetIndex",
    "I3ProductionRunKind",
    "I3ProductionRunReceipt",
    "I3ProductionRunSpec",
    "I3ProductionRunState",
    "I3ProductionSegmentPin",
    "I3ProductionTableOutput",
    "I3ProductionV2ContractPin",
    "LoadedI3ProductionStaging",
    "load_i3_production_completion_exact",
    "load_i3_production_deep_attestation_exact",
    "load_i3_production_parent_shallow_exact",
    "load_i3_production_run_receipt_exact",
    "load_i3_production_run_spec_exact",
    "load_i3_production_staging_exact",
    "production_gate_a_input_pins",
    "production_physical_index_digest",
    "production_v2_contract_pins",
    "validate_production_compact_base_initial_rowsets",
    "validate_production_delta_append_outputs",
]
