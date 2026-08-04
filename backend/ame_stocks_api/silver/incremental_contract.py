"""Minimal, pure contracts for S7.5 base/delta/correction releases.

Only three objects are durable workflow records: :class:`RunSpec`,
:class:`RunReceipt`, and :class:`IncrementalReleaseManifest`.  A checkpoint is
an immutable, rebuildable output pinned by a receipt, not another approval
layer.  Release manifest bodies never contain their own byte pin; the external
pin is computed after canonical serialization, avoiding a self-hash.

The module performs no discovery and no IO. It validates logical shapes,
derives content IDs, and verifies that the three control objects form one
consistent structural projection. A production writer must additionally pass
the I2/I3 exact-byte and append-only publication/approval trust boundaries.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field, replace
from datetime import date
from enum import StrEnum
from pathlib import PurePosixPath

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.incremental_gate import (
    IncrementalGateError,
    PinnedCorrectionAuthorization,
    QaPolicy,
    QaReceipt,
    QaSeverity,
    validate_correction_authorization,
    validate_qa_for_publish,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_VERSIONED_TABLES = frozenset({"ticker_alias", "asset_master", "issuer_master"})
_PARTITION_TABLE = "universe_daily"
_CONTROL_VALIDATION_RULE_VERSION = "s7_5_control_projection_v1"
_CONTROL_VALIDATION_SEAL = object()
_CONTENT_ATTESTATION_RULE_VERSION = "s7_5_content_attestation_v1"
_CONTENT_ATTESTATION_SEAL = object()
CHECKPOINT_RECEIPT_RULE_VERSION = "s7_5_checkpoint_receipt_v1"
_REQUIRED_QA_CHECKS = {
    "partition_session_calendar_contiguous": QaSeverity.HIGH,
    "row_semantic_proof_complete": QaSeverity.CRITICAL,
}


class IncrementalContractError(SilverContractError):
    """Raised when an incremental contract is ambiguous or unsafe."""


class ReleaseType(StrEnum):
    """Closed release channel."""

    BASE = "base"
    DELTA = "delta"
    CORRECTION = "correction"


class ViewKind(StrEnum):
    """Explicit research view; readers have no implicit default."""

    HISTORICAL_AS_KNOWN = "historical_as_known"
    LATEST_REVIEWED_RESEARCH = "latest_reviewed_research"


class ControlObjectKind(StrEnum):
    """Durable objects that a release manifest must locate exactly."""

    RUN_SPEC = "run_spec"
    RUN_RECEIPT = "run_receipt"


class RowVersionOperation(StrEnum):
    """Closed row-version operation domain."""

    NEW_ROOT = "new_root"
    MECHANICAL_SUCCESSOR = "mechanical_successor"
    REVIEWED_CORRECTION = "reviewed_correction"
    TOMBSTONE = "tombstone"


@dataclass(frozen=True, slots=True)
class ArtifactPin:
    """Exact immutable artifact locator."""

    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.path, "artifact path")
        _digest(self.sha256, "artifact SHA-256")
        _positive_int(self.bytes, "artifact bytes")

    def to_dict(self) -> dict[str, object]:
        return {"bytes": self.bytes, "path": self.path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class ControlObjectPin:
    """Exact locator plus logical ID for one durable control object."""

    object_kind: ControlObjectKind
    object_id: str
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.object_kind, ControlObjectKind):
            raise IncrementalContractError("control object kind is invalid")
        _digest(self.object_id, "control object ID")
        if not isinstance(self.artifact, ArtifactPin):
            raise IncrementalContractError("control object artifact pin is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "object_id": self.object_id,
            "object_kind": self.object_kind.value,
        }


@dataclass(frozen=True, slots=True)
class ManifestPin:
    """External exact pin for a release manifest body.

    This object is carried by readers and child releases.  It is not serialized
    inside the manifest body that it hashes.
    """

    release_id: str
    manifest_path: str
    manifest_sha256: str
    manifest_bytes: int
    release_available_session: date

    def __post_init__(self) -> None:
        _digest(self.release_id, "release ID")
        _relative_path(self.manifest_path, "manifest path")
        _digest(self.manifest_sha256, "manifest SHA-256")
        _positive_int(self.manifest_bytes, "manifest bytes")
        _session(self.release_available_session, "release available session")

    @property
    def path(self) -> str:
        return self.manifest_path

    @property
    def sha256(self) -> str:
        return self.manifest_sha256

    @property
    def bytes(self) -> int:
        return self.manifest_bytes

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_bytes": self.manifest_bytes,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "release_available_session": self.release_available_session.isoformat(),
            "release_id": self.release_id,
        }


@dataclass(frozen=True, slots=True)
class RowSemanticProofReceipt:
    """Exact frozen proof locator for one row-operation classification.

    Gate A defines the receipt and binds it into each row version, but leaves
    every production row dispatcher disabled.  I3 must add module-owned table
    validators before any row-bearing release can obtain a validated runtime
    capability; callers cannot inject an arbitrary callback.
    """

    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    operation: RowVersionOperation
    row_payload_digest: str
    predecessor_payload_digest: str | None
    validator_semantics_digest: str
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if self.table_name not in _VERSIONED_TABLES:
            raise IncrementalContractError("row semantic-proof table is invalid")
        _digest(self.stable_row_key, "semantic-proof stable row key")
        _digest(self.row_version_id, "semantic-proof row-version ID")
        _optional_digest(
            self.predecessor_row_version_id,
            "semantic-proof predecessor row-version ID",
        )
        if not isinstance(self.operation, RowVersionOperation):
            raise IncrementalContractError("row semantic-proof operation is invalid")
        _digest(self.row_payload_digest, "semantic-proof row payload digest")
        _optional_digest(
            self.predecessor_payload_digest,
            "semantic-proof predecessor payload digest",
        )
        _digest(
            self.validator_semantics_digest,
            "row semantic-proof validator semantics digest",
        )
        if not isinstance(self.artifact, ArtifactPin):
            raise IncrementalContractError("row semantic-proof artifact is invalid")
        if self.operation is RowVersionOperation.NEW_ROOT:
            if (
                self.predecessor_row_version_id is not None
                or self.predecessor_payload_digest is not None
            ):
                raise IncrementalContractError(
                    "new-root semantic proof cannot carry predecessor facts"
                )
        elif self.predecessor_row_version_id is None or self.predecessor_payload_digest is None:
            raise IncrementalContractError(
                "successor semantic proof requires exact predecessor facts"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "operation": self.operation.value,
            "predecessor_payload_digest": self.predecessor_payload_digest,
            "predecessor_row_version_id": self.predecessor_row_version_id,
            "row_payload_digest": self.row_payload_digest,
            "row_version_id": self.row_version_id,
            "stable_row_key": self.stable_row_key,
            "table_name": self.table_name,
            "validator_semantics_digest": self.validator_semantics_digest,
        }


@dataclass(frozen=True, slots=True)
class RowVersionReference:
    """Historical FK retained by a session partition."""

    table_name: str
    row_version_id: str

    def __post_init__(self) -> None:
        if self.table_name not in _VERSIONED_TABLES:
            raise IncrementalContractError("row-version reference table is invalid")
        _digest(self.row_version_id, "referenced row-version ID")

    def to_dict(self) -> dict[str, str]:
        return {"row_version_id": self.row_version_id, "table_name": self.table_name}


@dataclass(frozen=True, slots=True)
class PartitionReceipt:
    """One immutable ``universe_daily`` session partition."""

    table_name: str
    partition_key: str
    receipt: ArtifactPin
    row_count: int
    schema_digest: str
    availability_session: date
    row_version_references: tuple[RowVersionReference, ...] = ()

    def __post_init__(self) -> None:
        if self.table_name != _PARTITION_TABLE:
            raise IncrementalContractError("only universe_daily is session-partitioned")
        partition_session = _session_text(self.partition_key, "partition key")
        if not isinstance(self.receipt, ArtifactPin):
            raise IncrementalContractError("partition receipt artifact is invalid")
        _positive_int(self.row_count, "partition row count")
        _digest(self.schema_digest, "partition schema digest")
        availability_session = _session(
            self.availability_session,
            "partition availability session",
        )
        if partition_session > availability_session:
            raise IncrementalContractError(
                "partition session exceeds partition receipt availability"
            )
        _typed_tuple(
            self.row_version_references,
            RowVersionReference,
            "row-version references",
        )
        reference_keys = [
            (item.table_name, item.row_version_id) for item in self.row_version_references
        ]
        if reference_keys != sorted(set(reference_keys)):
            raise IncrementalContractError(
                "partition row-version references must be sorted and unique"
            )

    @property
    def key(self) -> tuple[str, str]:
        return (self.table_name, self.partition_key)

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "partition_key": self.partition_key,
            "receipt": self.receipt.to_dict(),
            "row_count": self.row_count,
            "row_version_references": [item.to_dict() for item in self.row_version_references],
            "schema_digest": self.schema_digest,
            "table_name": self.table_name,
        }


@dataclass(frozen=True, slots=True)
class PartitionReplacement:
    """Exact old/new receipt pair for one correction partition."""

    replaced_receipt: PartitionReceipt
    replacement_receipt: PartitionReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.replaced_receipt, PartitionReceipt) or not isinstance(
            self.replacement_receipt, PartitionReceipt
        ):
            raise IncrementalContractError("partition replacement receipts are invalid")
        if self.replaced_receipt.key != self.replacement_receipt.key:
            raise IncrementalContractError("partition replacement crossed a logical key")
        if self.replaced_receipt.receipt == self.replacement_receipt.receipt:
            raise IncrementalContractError("partition replacement must change the artifact")

    @property
    def key(self) -> tuple[str, str]:
        return self.replacement_receipt.key

    def to_dict(self) -> dict[str, object]:
        return {
            "replaced_receipt": self.replaced_receipt.to_dict(),
            "replacement_receipt": self.replacement_receipt.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RowVersionReceipt:
    """Exact index entry for one append-only non-session row version."""

    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    operation: RowVersionOperation
    availability_session: date
    index_artifact: ArtifactPin
    row_locator: str
    row_payload_digest: str
    semantic_proof: RowSemanticProofReceipt
    tombstone_reason: str | None = None

    def __post_init__(self) -> None:
        if self.table_name not in _VERSIONED_TABLES:
            raise IncrementalContractError("row-version table is invalid")
        _digest(self.stable_row_key, "stable row key")
        _digest(self.row_version_id, "row-version ID")
        _optional_digest(self.predecessor_row_version_id, "predecessor row-version ID")
        if self.predecessor_row_version_id == self.row_version_id:
            raise IncrementalContractError("row version cannot supersede itself")
        if not isinstance(self.operation, RowVersionOperation):
            raise IncrementalContractError("row-version operation is invalid")
        _session(self.availability_session, "row-version availability session")
        if not isinstance(self.index_artifact, ArtifactPin):
            raise IncrementalContractError("row-version index artifact is invalid")
        _relative_path(self.row_locator, "row locator")
        _digest(self.row_payload_digest, "row payload digest")
        if not isinstance(self.semantic_proof, RowSemanticProofReceipt):
            raise IncrementalContractError("row semantic proof is invalid")
        _optional_text(self.tombstone_reason, "tombstone reason")

        proof_pairs = (
            (self.semantic_proof.table_name, self.table_name, "table"),
            (
                self.semantic_proof.stable_row_key,
                self.stable_row_key,
                "stable row key",
            ),
            (
                self.semantic_proof.row_version_id,
                self.row_version_id,
                "row-version ID",
            ),
            (
                self.semantic_proof.predecessor_row_version_id,
                self.predecessor_row_version_id,
                "predecessor row-version ID",
            ),
            (self.semantic_proof.operation, self.operation, "operation"),
            (
                self.semantic_proof.row_payload_digest,
                self.row_payload_digest,
                "row payload digest",
            ),
        )
        for actual, expected, label in proof_pairs:
            if actual != expected:
                raise IncrementalContractError(
                    f"row semantic proof {label} differs from row receipt"
                )

        if self.operation is RowVersionOperation.NEW_ROOT:
            if self.predecessor_row_version_id is not None:
                raise IncrementalContractError("new-root row version cannot have a predecessor")
        elif self.predecessor_row_version_id is None:
            raise IncrementalContractError(
                "successor/correction/tombstone row version requires a predecessor"
            )
        if self.operation is RowVersionOperation.TOMBSTONE:
            if self.tombstone_reason is None:
                raise IncrementalContractError("tombstone row version requires a reason")
        elif self.tombstone_reason is not None:
            raise IncrementalContractError(
                "non-tombstone row version cannot carry a tombstone reason"
            )

    @property
    def key(self) -> tuple[str, str]:
        return (self.table_name, self.stable_row_key)

    @property
    def is_tombstone(self) -> bool:
        return self.operation is RowVersionOperation.TOMBSTONE

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "index_artifact": self.index_artifact.to_dict(),
            "operation": self.operation.value,
            "predecessor_row_version_id": self.predecessor_row_version_id,
            "row_locator": self.row_locator,
            "row_payload_digest": self.row_payload_digest,
            "row_version_id": self.row_version_id,
            "semantic_proof": self.semantic_proof.to_dict(),
            "stable_row_key": self.stable_row_key,
            "table_name": self.table_name,
            "tombstone_reason": self.tombstone_reason,
        }


@dataclass(frozen=True, slots=True)
class CheckpointReceipt:
    """Rebuildable state artifact produced by a run.

    Gate A freezes the control envelope and its rule version. I2/I3 must still
    verify the pinned checkpoint bytes against the corresponding fixed content
    schema before a production loader may consume them.
    """

    artifact: ArtifactPin
    parent_release_id: str | None
    run_spec_id: str
    last_session: date
    resolved_content_digest: str
    rebuild_basis_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, ArtifactPin):
            raise IncrementalContractError("checkpoint artifact is invalid")
        _optional_digest(self.parent_release_id, "checkpoint parent release ID")
        _digest(self.run_spec_id, "checkpoint run-spec ID")
        _session(self.last_session, "checkpoint last session")
        _digest(self.resolved_content_digest, "checkpoint resolved-content digest")
        _digest(self.rebuild_basis_digest, "checkpoint rebuild-basis digest")

    @property
    def checkpoint_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "last_session": self.last_session.isoformat(),
            "parent_release_id": self.parent_release_id,
            "rebuild_basis_digest": self.rebuild_basis_digest,
            "resolved_content_digest": self.resolved_content_digest,
            "rule_version": CHECKPOINT_RECEIPT_RULE_VERSION,
            "run_spec_id": self.run_spec_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"checkpoint_id": self.checkpoint_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Immutable execution intent without recursive approval objects."""

    release_type: ReleaseType
    parent_release_pin: ManifestPin | None
    parent_identity_policy_bundle_id: str | None
    resolved_view: ViewKind
    source_binding_digest: str
    source_cutoff_session: date
    availability_cutoff_session: date
    release_available_session: date
    schema_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    input_pins: tuple[ArtifactPin, ...]
    expected_change_set_digest: str
    qa_policy: QaPolicy
    correction_scope_digest: str | None
    correction_authorization: PinnedCorrectionAuthorization | None
    rss_cap_bytes: int
    disk_floor_bytes: int
    wall_clock_cap_seconds: int | None

    def __post_init__(self) -> None:
        _release_parent_shape(self.release_type, self.parent_release_pin)
        _parent_identity_policy_shape(
            self.release_type,
            self.parent_identity_policy_bundle_id,
        )
        if not isinstance(self.resolved_view, ViewKind):
            raise IncrementalContractError("run-spec resolved view is invalid")
        for value, label in (
            (self.source_binding_digest, "source binding digest"),
            (self.schema_digest, "schema digest"),
            (self.transform_semantics_digest, "transform semantics digest"),
            (self.identity_policy_bundle_id, "identity-policy bundle ID"),
            (self.calendar_digest, "calendar digest"),
            (self.expected_change_set_digest, "expected change-set digest"),
        ):
            _digest(value, label)
        _cutoffs(self.source_cutoff_session, self.availability_cutoff_session)
        release_available = _session(
            self.release_available_session,
            "release available session",
        )
        if release_available < self.availability_cutoff_session:
            raise IncrementalContractError("release availability precedes run availability cutoff")
        if (
            self.parent_release_pin is not None
            and self.availability_cutoff_session < self.parent_release_pin.release_available_session
        ):
            raise IncrementalContractError(
                "run availability cutoff precedes parent release availability"
            )
        if (
            self.parent_release_pin is not None
            and release_available < self.parent_release_pin.release_available_session
        ):
            raise IncrementalContractError(
                "release availability precedes parent release availability"
            )
        _typed_tuple(self.input_pins, ArtifactPin, "run-spec input pins")
        if not self.input_pins:
            raise IncrementalContractError("run-spec requires at least one exact input pin")
        paths = [item.path for item in self.input_pins]
        if paths != sorted(set(paths)):
            raise IncrementalContractError(
                "run-spec input pins must be sorted and have unique paths"
            )
        expected_source_binding = input_set_digest(self.input_pins)
        if self.source_binding_digest != expected_source_binding:
            raise IncrementalContractError(
                "source binding digest does not reproduce from exact input pins"
            )
        if not isinstance(self.qa_policy, QaPolicy):
            raise IncrementalContractError("run-spec QA policy is invalid")
        _required_qa_policy(self.qa_policy)
        _correction_authorization_shape(
            self.release_type,
            self.correction_scope_digest,
            self.correction_authorization,
        )
        _positive_int(self.rss_cap_bytes, "RSS cap bytes")
        _positive_int(self.disk_floor_bytes, "disk floor bytes")
        _optional_positive_int(self.wall_clock_cap_seconds, "wall-clock cap seconds")

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_cutoff_session": self.availability_cutoff_session.isoformat(),
            "calendar_digest": self.calendar_digest,
            "correction_authorization": (
                self.correction_authorization.to_dict()
                if self.correction_authorization is not None
                else None
            ),
            "correction_scope_digest": self.correction_scope_digest,
            "disk_floor_bytes": self.disk_floor_bytes,
            "expected_change_set_digest": self.expected_change_set_digest,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "input_pins": [item.to_dict() for item in self.input_pins],
            "parent_release_pin": (
                self.parent_release_pin.to_dict() if self.parent_release_pin is not None else None
            ),
            "parent_identity_policy_bundle_id": self.parent_identity_policy_bundle_id,
            "qa_policy": self.qa_policy.to_dict(),
            "release_available_session": self.release_available_session.isoformat(),
            "release_type": self.release_type.value,
            "resolved_view": self.resolved_view.value,
            "rss_cap_bytes": self.rss_cap_bytes,
            "schema_digest": self.schema_digest,
            "source_binding_digest": self.source_binding_digest,
            "source_cutoff_session": self.source_cutoff_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
            "wall_clock_cap_seconds": self.wall_clock_cap_seconds,
        }

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class RunReceipt:
    """Immutable result; failed runs may stop before outputs or checkpoint exist."""

    run_spec_id: str
    actual_input_set_digest: str | None
    output_set_digest: str | None
    qa_receipt: QaReceipt | None
    checkpoint: CheckpointReceipt | None
    succeeded: bool
    error_codes: tuple[str, ...]
    receipt_available_session: date
    runtime_seconds: int | float
    peak_rss_bytes: int | None
    minimum_free_disk_bytes: int

    def __post_init__(self) -> None:
        _digest(self.run_spec_id, "run-spec ID")
        _optional_digest(self.actual_input_set_digest, "actual input-set digest")
        _optional_digest(self.output_set_digest, "output-set digest")
        if self.output_set_digest is not None and self.actual_input_set_digest is None:
            raise IncrementalContractError("run outputs require an exact actual input-set digest")
        if self.qa_receipt is not None and not isinstance(self.qa_receipt, QaReceipt):
            raise IncrementalContractError("run receipt QA receipt is invalid")
        receipt_available = _session(
            self.receipt_available_session,
            "run-receipt availability session",
        )
        if self.qa_receipt is not None:
            if self.actual_input_set_digest is None or self.output_set_digest is None:
                raise IncrementalContractError(
                    "QA receipt requires actual inputs and exact outputs"
                )
            if self.qa_receipt.run_spec_id != self.run_spec_id:
                raise IncrementalContractError("QA receipt belongs to another run spec")
            if self.qa_receipt.source_binding_digest != self.actual_input_set_digest:
                raise IncrementalContractError(
                    "QA receipt source binding differs from actual inputs"
                )
            if self.qa_receipt.change_set_digest != self.output_set_digest:
                raise IncrementalContractError("QA receipt change set differs from run outputs")
            if self.qa_receipt.qa_available_session > receipt_available:
                raise IncrementalContractError("QA availability exceeds run-receipt availability")
        if self.checkpoint is not None:
            if not isinstance(self.checkpoint, CheckpointReceipt):
                raise IncrementalContractError("run receipt checkpoint is invalid")
            if self.qa_receipt is None:
                raise IncrementalContractError(
                    "checkpoint requires a completed structured QA receipt"
                )
            if self.checkpoint.run_spec_id != self.run_spec_id:
                raise IncrementalContractError("checkpoint belongs to another run spec")
            if self.output_set_digest is None:
                raise IncrementalContractError("checkpoint requires an exact output-set digest")
        if type(self.succeeded) is not bool:
            raise IncrementalContractError("run succeeded must be a boolean")
        _token_tuple(self.error_codes, "run error codes")
        if self.succeeded:
            if self.error_codes:
                raise IncrementalContractError("successful run cannot carry error codes")
            if (
                self.actual_input_set_digest is None
                or self.output_set_digest is None
                or self.qa_receipt is None
                or self.checkpoint is None
            ):
                raise IncrementalContractError(
                    "successful run requires inputs, outputs, QA, and checkpoint"
                )
        elif not self.error_codes:
            raise IncrementalContractError("failed run requires at least one error code")
        _nonnegative_number(self.runtime_seconds, "runtime seconds")
        if self.peak_rss_bytes is not None:
            _nonnegative_int(self.peak_rss_bytes, "peak RSS bytes")
        _nonnegative_int(self.minimum_free_disk_bytes, "minimum free disk bytes")

    @property
    def run_receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "actual_input_set_digest": self.actual_input_set_digest,
            "checkpoint_id": (
                self.checkpoint.checkpoint_id if self.checkpoint is not None else None
            ),
            "error_codes": list(self.error_codes),
            "output_set_digest": self.output_set_digest,
            "qa_receipt_id": (
                self.qa_receipt.qa_receipt_id if self.qa_receipt is not None else None
            ),
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "run_spec_id": self.run_spec_id,
            "succeeded": self.succeeded,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "run_receipt_id": self.run_receipt_id,
            **self.logical_payload(),
            "checkpoint": (self.checkpoint.to_dict() if self.checkpoint is not None else None),
            "qa_receipt": (self.qa_receipt.to_dict() if self.qa_receipt is not None else None),
            "runtime_observation": {
                "minimum_free_disk_bytes": self.minimum_free_disk_bytes,
                "peak_rss_bytes": self.peak_rss_bytes,
                "runtime_seconds": self.runtime_seconds,
            },
        }


@dataclass(frozen=True, slots=True)
class IncrementalReleaseManifest:
    """Release manifest body; its exact byte pin is external."""

    release_type: ReleaseType
    parent_release_pin: ManifestPin | None
    resolved_view: ViewKind
    schema_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    source_binding_digest: str
    source_cutoff_session: date
    availability_cutoff_session: date
    release_available_session: date
    added_partition_receipts: tuple[PartitionReceipt, ...]
    partition_replacements: tuple[PartitionReplacement, ...]
    added_row_version_receipts: tuple[RowVersionReceipt, ...]
    superseded_row_version_ids: tuple[str, ...]
    resolved_content_digest: str
    qa_policy_id: str
    qa_receipt_id: str
    correction_authorization_id: str | None
    run_spec_pin: ControlObjectPin
    run_receipt_pin: ControlObjectPin

    def __post_init__(self) -> None:
        _release_parent_shape(self.release_type, self.parent_release_pin)
        if not isinstance(self.resolved_view, ViewKind):
            raise IncrementalContractError("release resolved view is invalid")
        for value, label in (
            (self.schema_digest, "schema digest"),
            (self.transform_semantics_digest, "transform semantics digest"),
            (self.identity_policy_bundle_id, "identity-policy bundle ID"),
            (self.calendar_digest, "calendar digest"),
            (self.source_binding_digest, "source binding digest"),
            (self.resolved_content_digest, "resolved-content digest"),
            (self.qa_policy_id, "QA policy ID"),
            (self.qa_receipt_id, "QA receipt ID"),
        ):
            _digest(value, label)
        _cutoffs(self.source_cutoff_session, self.availability_cutoff_session)
        release_available = _session(
            self.release_available_session,
            "release available session",
        )
        if release_available < self.availability_cutoff_session:
            raise IncrementalContractError(
                "release availability precedes manifest availability cutoff"
            )
        if self.parent_release_pin is not None:
            if self.availability_cutoff_session < self.parent_release_pin.release_available_session:
                raise IncrementalContractError(
                    "manifest availability cutoff precedes parent release availability"
                )
            if release_available < self.parent_release_pin.release_available_session:
                raise IncrementalContractError(
                    "manifest release availability precedes parent release availability"
                )
        _correction_authorization_id_shape(
            self.release_type,
            self.correction_authorization_id,
        )
        _typed_tuple(
            self.added_partition_receipts,
            PartitionReceipt,
            "added partition receipts",
        )
        _typed_tuple(
            self.partition_replacements,
            PartitionReplacement,
            "partition replacements",
        )
        _typed_tuple(
            self.added_row_version_receipts,
            RowVersionReceipt,
            "added row-version receipts",
        )
        _digest_tuple(self.superseded_row_version_ids, "superseded row-version IDs")
        if not isinstance(self.run_spec_pin, ControlObjectPin) or (
            self.run_spec_pin.object_kind is not ControlObjectKind.RUN_SPEC
        ):
            raise IncrementalContractError("release requires an exact run-spec pin")
        if not isinstance(self.run_receipt_pin, ControlObjectPin) or (
            self.run_receipt_pin.object_kind is not ControlObjectKind.RUN_RECEIPT
        ):
            raise IncrementalContractError("release requires an exact run-receipt pin")
        self._validate_changes()

    @property
    def release_id(self) -> str:
        return stable_digest(self.release_identity_payload())

    @property
    def added_row_versions(self) -> tuple[RowVersionReceipt, ...]:
        """Compatibility alias used by the pure resolver."""

        return self.added_row_version_receipts

    def release_identity_payload(self) -> dict[str, object]:
        """Return logical release facts, excluding control/runtime envelopes."""

        return {
            "added_partition_receipts": [item.to_dict() for item in self.added_partition_receipts],
            "added_row_version_receipts": [
                item.to_dict() for item in self.added_row_version_receipts
            ],
            "availability_cutoff_session": self.availability_cutoff_session.isoformat(),
            "calendar_digest": self.calendar_digest,
            "correction_authorization_id": self.correction_authorization_id,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "parent_release_pin": (
                self.parent_release_pin.to_dict() if self.parent_release_pin is not None else None
            ),
            "partition_replacements": [item.to_dict() for item in self.partition_replacements],
            "qa_policy_id": self.qa_policy_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_type": self.release_type.value,
            "resolved_content_digest": self.resolved_content_digest,
            "resolved_view": self.resolved_view.value,
            "schema_digest": self.schema_digest,
            "source_binding_digest": self.source_binding_digest,
            "source_cutoff_session": self.source_cutoff_session.isoformat(),
            "superseded_row_version_ids": list(self.superseded_row_version_ids),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "control_provenance": {
                "qa_receipt_id": self.qa_receipt_id,
                "run_receipt_pin": self.run_receipt_pin.to_dict(),
                "run_spec_pin": self.run_spec_pin.to_dict(),
            },
            "release_id": self.release_id,
            **self.release_identity_payload(),
        }

    def canonical_bytes(self) -> bytes:
        """Canonical bytes covered by the external manifest pin."""

        return _canonical_json_bytes(self.to_dict())

    def exact_pin(
        self,
        *,
        manifest_path: str,
    ) -> ManifestPin:
        """Create the non-self-referential exact pin for this body."""

        _relative_path(manifest_path, "manifest path")
        release_available = self.release_available_session
        if (
            self.parent_release_pin is not None
            and release_available < self.parent_release_pin.release_available_session
        ):
            raise IncrementalContractError(
                "child release availability precedes parent release availability"
            )
        for receipt in self.added_partition_receipts:
            if receipt.availability_session > release_available:
                raise IncrementalContractError(
                    "partition receipt availability exceeds release availability"
                )
        for replacement in self.partition_replacements:
            if replacement.replacement_receipt.availability_session > release_available:
                raise IncrementalContractError(
                    "replacement receipt availability exceeds release availability"
                )
        for receipt in self.added_row_version_receipts:
            if receipt.availability_session > release_available:
                raise IncrementalContractError(
                    "row-version receipt availability exceeds release availability"
                )
        content = self.canonical_bytes()
        return ManifestPin(
            release_id=self.release_id,
            manifest_path=manifest_path,
            manifest_sha256=hashlib.sha256(content).hexdigest(),
            manifest_bytes=len(content),
            release_available_session=release_available,
        )

    def _validate_changes(self) -> None:
        partition_keys = [item.key for item in self.added_partition_receipts]
        replacement_keys = [item.key for item in self.partition_replacements]
        if partition_keys != sorted(set(partition_keys)):
            raise IncrementalContractError("added partitions must be sorted and unique")
        if replacement_keys != sorted(set(replacement_keys)):
            raise IncrementalContractError("partition replacements must be sorted and unique")
        if set(partition_keys) & set(replacement_keys):
            raise IncrementalContractError("one release cannot add and replace the same partition")
        row_keys = [item.key for item in self.added_row_version_receipts]
        if row_keys != sorted(set(row_keys)):
            raise IncrementalContractError(
                "one release may add at most one row version per stable key"
            )
        projected_superseded = tuple(
            sorted(
                item.predecessor_row_version_id
                for item in self.added_row_version_receipts
                if item.predecessor_row_version_id is not None
            )
        )
        if projected_superseded != self.superseded_row_version_ids:
            raise IncrementalContractError(
                "superseded row-version IDs must equal the predecessor projection"
            )
        if not (
            self.added_partition_receipts
            or self.partition_replacements
            or self.added_row_version_receipts
        ):
            raise IncrementalContractError("release must contain at least one logical change")

        partition_receipts = self.added_partition_receipts + tuple(
            item.replacement_receipt for item in self.partition_replacements
        )
        for receipt in partition_receipts:
            partition_session = _session_text(receipt.partition_key, "partition key")
            if partition_session > self.source_cutoff_session:
                raise IncrementalContractError("partition session exceeds manifest source cutoff")
            if receipt.availability_session > self.availability_cutoff_session:
                raise IncrementalContractError(
                    "partition receipt availability exceeds manifest availability cutoff"
                )
        for receipt in self.added_row_version_receipts:
            if receipt.availability_session > self.availability_cutoff_session:
                raise IncrementalContractError(
                    "row-version receipt availability exceeds manifest availability cutoff"
                )

        operations = {item.operation for item in self.added_row_version_receipts}
        if self.release_type is ReleaseType.BASE:
            if not self.added_partition_receipts:
                raise IncrementalContractError(
                    "base release requires at least one universe session partition"
                )
            if self.partition_replacements or self.superseded_row_version_ids:
                raise IncrementalContractError(
                    "base releases cannot replace partitions or supersede row versions"
                )
            if operations - {RowVersionOperation.NEW_ROOT}:
                raise IncrementalContractError("base release rows must be new roots")
        elif self.release_type is ReleaseType.DELTA:
            if self.partition_replacements:
                raise IncrementalContractError("delta release cannot replace partitions")
            if not self.added_partition_receipts:
                raise IncrementalContractError(
                    "clean delta must add at least one session partition"
                )
            disallowed = operations & {
                RowVersionOperation.REVIEWED_CORRECTION,
                RowVersionOperation.TOMBSTONE,
            }
            if disallowed:
                raise IncrementalContractError(
                    "delta release cannot perform reviewed correction or tombstone operations"
                )


@dataclass(frozen=True, slots=True, init=False)
class ControlValidatedCandidate:
    """Runtime-only capability proving one exact local control projection.

    It is deliberately absent from every durable schema.  Only
    :func:`validate_release_projection` can mint it.  It is only a candidate
    for content attestation; the consumer resolver requires the stronger
    :class:`ContentAttestedRelease` capability.
    """

    manifest_pin: ManifestPin
    run_spec: RunSpec
    run_receipt: RunReceipt
    manifest: IncrementalReleaseManifest
    parent_release: ContentAttestedRelease | None
    control_projection_digest: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        manifest_pin: ManifestPin,
        run_spec: RunSpec,
        run_receipt: RunReceipt,
        manifest: IncrementalReleaseManifest,
        parent_release: ContentAttestedRelease | None,
        control_projection_digest: str,
        _seal: object,
    ) -> None:
        if _seal is not _CONTROL_VALIDATION_SEAL:
            raise IncrementalContractError(
                "control-validated candidates can only be minted by projection validation"
            )
        object.__setattr__(self, "manifest_pin", manifest_pin)
        object.__setattr__(self, "run_spec", run_spec)
        object.__setattr__(self, "run_receipt", run_receipt)
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "parent_release", parent_release)
        object.__setattr__(
            self,
            "control_projection_digest",
            _digest(control_projection_digest, "control projection digest"),
        )
        object.__setattr__(self, "_seal", _seal)


@dataclass(frozen=True, slots=True, init=False)
class ContentAttestedRelease:
    """Runtime-only capability proving controls plus a resolved receipt graph.

    This pure module attests the content-addressed receipt/index graph and its
    selected snapshot digest. It does not claim that referenced Parquet,
    checkpoint, proof, QA-detail, or evidence bytes were opened and hashed.
    A production I2/I3 loader must establish that separate IO trust boundary
    before exposing this capability to readers.
    """

    candidate: ControlValidatedCandidate
    attested_resolved_content_digest: str
    attested_snapshot_digest: str
    content_attestation_digest: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        candidate: ControlValidatedCandidate,
        attested_resolved_content_digest: str,
        attested_snapshot_digest: str,
        content_attestation_digest: str,
        _seal: object,
    ) -> None:
        if _seal is not _CONTENT_ATTESTATION_SEAL:
            raise IncrementalContractError(
                "content-attested releases can only be minted by content attestation"
            )
        object.__setattr__(self, "candidate", candidate)
        object.__setattr__(
            self,
            "attested_resolved_content_digest",
            _digest(
                attested_resolved_content_digest,
                "attested resolved-content digest",
            ),
        )
        object.__setattr__(
            self,
            "attested_snapshot_digest",
            _digest(attested_snapshot_digest, "attested snapshot digest"),
        )
        object.__setattr__(
            self,
            "content_attestation_digest",
            _digest(content_attestation_digest, "content attestation digest"),
        )
        object.__setattr__(self, "_seal", _seal)

    @property
    def manifest_pin(self) -> ManifestPin:
        return self.candidate.manifest_pin

    @property
    def manifest(self) -> IncrementalReleaseManifest:
        return self.candidate.manifest

    @property
    def run_spec(self) -> RunSpec:
        return self.candidate.run_spec

    @property
    def run_receipt(self) -> RunReceipt:
        return self.candidate.run_receipt


def _mint_control_validated_release(
    *,
    manifest_pin: ManifestPin,
    run_spec: RunSpec,
    run_receipt: RunReceipt,
    manifest: IncrementalReleaseManifest,
    parent_release: ContentAttestedRelease | None,
) -> ControlValidatedCandidate:
    projection_digest = stable_digest(
        {
            "correction_authorization_id": manifest.correction_authorization_id,
            "manifest_pin": manifest_pin.to_dict(),
            "qa_policy_id": manifest.qa_policy_id,
            "qa_receipt_id": manifest.qa_receipt_id,
            "rule_version": _CONTROL_VALIDATION_RULE_VERSION,
            "run_receipt_pin": manifest.run_receipt_pin.to_dict(),
            "run_spec_pin": manifest.run_spec_pin.to_dict(),
        }
    )
    return ControlValidatedCandidate(
        manifest_pin=manifest_pin,
        run_spec=run_spec,
        run_receipt=run_receipt,
        manifest=manifest,
        parent_release=parent_release,
        control_projection_digest=projection_digest,
        _seal=_CONTROL_VALIDATION_SEAL,
    )


def verify_control_validated_candidate(value: object) -> ControlValidatedCandidate:
    """Re-run one complete control chain iteratively from base to candidate.

    A daily release chain can span thousands of sessions.  Walking it before
    validation and replaying base-to-top avoids Python recursion limits while
    still proving every embedded parent exactly once.
    """

    candidate = _require_control_candidate_shape(value)
    parent_chain: list[ContentAttestedRelease] = []
    seen_candidate_ids = {id(candidate)}
    seen_pins = {candidate.manifest_pin}
    seen_release_ids = {candidate.manifest_pin.release_id: candidate.manifest_pin}
    cursor = candidate.parent_release
    while cursor is not None:
        attested = _require_content_attested_shape(cursor)
        parent_candidate = attested.candidate
        if id(parent_candidate) in seen_candidate_ids or parent_candidate.manifest_pin in seen_pins:
            raise IncrementalContractError("validated release parent cycle detected")
        prior_pin = seen_release_ids.get(parent_candidate.manifest_pin.release_id)
        if prior_pin is not None:
            raise IncrementalContractError(
                "one release ID appeared with conflicting parent manifest pins"
            )
        seen_candidate_ids.add(id(parent_candidate))
        seen_pins.add(parent_candidate.manifest_pin)
        seen_release_ids[parent_candidate.manifest_pin.release_id] = parent_candidate.manifest_pin
        parent_chain.append(attested)
        cursor = parent_candidate.parent_release

    verified_parent: ContentAttestedRelease | None = None
    for attested in reversed(parent_chain):
        reproduced_parent = _validate_release_projection_once(
            attested.run_spec,
            attested.run_receipt,
            attested.manifest,
            manifest_pin=attested.manifest_pin,
            parent_release=verified_parent,
        )
        _verify_control_projection_fingerprint(attested.candidate, reproduced_parent)
        _verify_content_attestation_fingerprint(attested, reproduced_parent)
        verified_parent = attested

    reproduced = _validate_release_projection_once(
        candidate.run_spec,
        candidate.run_receipt,
        candidate.manifest,
        manifest_pin=candidate.manifest_pin,
        parent_release=verified_parent,
    )
    _verify_control_projection_fingerprint(candidate, reproduced)
    return candidate


def _require_control_candidate_shape(value: object) -> ControlValidatedCandidate:
    if not isinstance(value, ControlValidatedCandidate) or (
        value._seal is not _CONTROL_VALIDATION_SEAL
    ):
        raise IncrementalContractError("content attestation requires a control-validated candidate")
    if not isinstance(value.manifest_pin, ManifestPin):
        raise IncrementalContractError("control-validated candidate manifest pin is invalid")
    if not isinstance(value.run_spec, RunSpec):
        raise IncrementalContractError("control-validated candidate run spec is invalid")
    if not isinstance(value.run_receipt, RunReceipt):
        raise IncrementalContractError("control-validated candidate run receipt is invalid")
    if not isinstance(value.manifest, IncrementalReleaseManifest):
        raise IncrementalContractError("control-validated candidate manifest is invalid")
    if value.parent_release is not None and not isinstance(
        value.parent_release,
        ContentAttestedRelease,
    ):
        raise IncrementalContractError("control-validated candidate parent is invalid")
    return value


def _require_content_attested_shape(value: object) -> ContentAttestedRelease:
    if not isinstance(value, ContentAttestedRelease) or (
        value._seal is not _CONTENT_ATTESTATION_SEAL
    ):
        raise IncrementalContractError("resolver requires a content-attested release capability")
    candidate = _require_control_candidate_shape(value.candidate)
    if candidate.manifest.release_type is ReleaseType.CORRECTION:
        raise IncrementalContractError(
            "Gate A correction reader capability is disabled without trusted approval-event "
            "attestation"
        )
    return value


def _verify_control_projection_fingerprint(
    candidate: ControlValidatedCandidate,
    reproduced: ControlValidatedCandidate,
) -> None:
    if candidate.control_projection_digest != reproduced.control_projection_digest:
        raise IncrementalContractError(
            "validated release projection fingerprint does not reproduce"
        )


def _verify_content_attestation_fingerprint(
    value: ContentAttestedRelease,
    candidate: ControlValidatedCandidate,
) -> None:
    expected = stable_digest(
        {
            "control_projection_digest": candidate.control_projection_digest,
            "manifest_pin": candidate.manifest_pin.to_dict(),
            "resolved_content_digest": value.attested_resolved_content_digest,
            "rule_version": _CONTENT_ATTESTATION_RULE_VERSION,
            "snapshot_digest": value.attested_snapshot_digest,
        }
    )
    if value.attested_resolved_content_digest != candidate.manifest.resolved_content_digest:
        raise IncrementalContractError("content-attested release no longer matches its manifest")
    if value.content_attestation_digest != expected:
        raise IncrementalContractError("content-attested release fingerprint does not reproduce")


def _mint_content_attested_release(
    candidate: ControlValidatedCandidate,
    *,
    resolved_content_digest: str,
    snapshot_digest: str,
) -> ContentAttestedRelease:
    verified = verify_control_validated_candidate(candidate)
    return _mint_content_attested_release_from_verified(
        verified,
        resolved_content_digest=resolved_content_digest,
        snapshot_digest=snapshot_digest,
    )


def _mint_content_attested_release_from_verified(
    candidate: ControlValidatedCandidate,
    *,
    resolved_content_digest: str,
    snapshot_digest: str,
) -> ContentAttestedRelease:
    """Mint after the caller has replayed the exact control chain once."""

    verified = _require_control_candidate_shape(candidate)
    _digest(resolved_content_digest, "resolved-content attestation digest")
    _digest(snapshot_digest, "snapshot attestation digest")
    if resolved_content_digest != verified.manifest.resolved_content_digest:
        raise IncrementalContractError(
            "resolved content does not match the candidate manifest attestation"
        )
    attestation_digest = stable_digest(
        {
            "control_projection_digest": verified.control_projection_digest,
            "manifest_pin": verified.manifest_pin.to_dict(),
            "resolved_content_digest": resolved_content_digest,
            "rule_version": _CONTENT_ATTESTATION_RULE_VERSION,
            "snapshot_digest": snapshot_digest,
        }
    )
    return ContentAttestedRelease(
        candidate=verified,
        attested_resolved_content_digest=resolved_content_digest,
        attested_snapshot_digest=snapshot_digest,
        content_attestation_digest=attestation_digest,
        _seal=_CONTENT_ATTESTATION_SEAL,
    )


def verify_content_attested_release(value: object) -> ContentAttestedRelease:
    """Re-verify the complete control chain and top content attestation."""

    attested = _require_content_attested_shape(value)
    candidate = verify_control_validated_candidate(attested.candidate)
    _verify_content_attestation_fingerprint(attested, candidate)
    return attested


def input_set_digest(pins: tuple[ArtifactPin, ...]) -> str:
    """Digest exact source inputs in their already validated canonical order."""

    _typed_tuple(pins, ArtifactPin, "input pins")
    return stable_digest({"input_pins": [item.to_dict() for item in pins]})


def release_change_set_digest(manifest: IncrementalReleaseManifest) -> str:
    """Digest only the exact logical output changes of a release."""

    if not isinstance(manifest, IncrementalReleaseManifest):
        raise IncrementalContractError("release change-set digest requires a manifest")
    return logical_change_set_digest(
        added_partition_receipts=manifest.added_partition_receipts,
        partition_replacements=manifest.partition_replacements,
        added_row_version_receipts=manifest.added_row_version_receipts,
        superseded_row_version_ids=manifest.superseded_row_version_ids,
    )


def logical_change_set_digest(
    *,
    added_partition_receipts: tuple[PartitionReceipt, ...],
    partition_replacements: tuple[PartitionReplacement, ...],
    added_row_version_receipts: tuple[RowVersionReceipt, ...],
    superseded_row_version_ids: tuple[str, ...],
) -> str:
    """Digest a change set before its run receipt/control envelope exists."""

    _typed_tuple(
        added_partition_receipts,
        PartitionReceipt,
        "added partition receipts",
    )
    _typed_tuple(
        partition_replacements,
        PartitionReplacement,
        "partition replacements",
    )
    _typed_tuple(
        added_row_version_receipts,
        RowVersionReceipt,
        "added row-version receipts",
    )
    _digest_tuple(superseded_row_version_ids, "superseded row-version IDs")
    return stable_digest(
        {
            "added_partition_receipts": [item.to_dict() for item in added_partition_receipts],
            "added_row_version_receipts": [item.to_dict() for item in added_row_version_receipts],
            "partition_replacements": [item.to_dict() for item in partition_replacements],
            "superseded_row_version_ids": list(superseded_row_version_ids),
        }
    )


def correction_scope_digest(
    *,
    parent_release_id: str,
    change_set_digest: str,
) -> str:
    """Derive the only correction scope accepted by Gate A."""

    _digest(parent_release_id, "correction-scope parent release ID")
    _digest(change_set_digest, "correction-scope change-set digest")
    return stable_digest(
        {
            "change_set_digest": change_set_digest,
            "namespace": "ame_stocks.silver.incremental_correction_scope",
            "parent_release_id": parent_release_id,
            "rule_version": "s7_5_incremental_correction_scope_v1",
        }
    )


def checkpoint_rebuild_basis_digest(
    spec: RunSpec,
    manifest: IncrementalReleaseManifest,
) -> str:
    """Bind rebuildable state to parent, source, semantics, and exact changes."""

    if not isinstance(spec, RunSpec) or not isinstance(manifest, IncrementalReleaseManifest):
        raise IncrementalContractError("checkpoint rebuild basis requires spec and manifest")
    return checkpoint_rebuild_basis_from_change_digest(
        spec,
        change_set_digest=release_change_set_digest(manifest),
    )


def checkpoint_rebuild_basis_from_change_digest(
    spec: RunSpec,
    *,
    change_set_digest: str,
) -> str:
    """Compute checkpoint lineage before a run receipt or manifest envelope exists."""

    if not isinstance(spec, RunSpec):
        raise IncrementalContractError("checkpoint rebuild basis requires a run spec")
    _digest(change_set_digest, "change-set digest")
    return stable_digest(
        {
            "availability_cutoff_session": spec.availability_cutoff_session.isoformat(),
            "calendar_digest": spec.calendar_digest,
            "change_set_digest": change_set_digest,
            "identity_policy_bundle_id": spec.identity_policy_bundle_id,
            "parent_release_pin": (
                spec.parent_release_pin.to_dict() if spec.parent_release_pin is not None else None
            ),
            "resolved_view": spec.resolved_view.value,
            "release_available_session": spec.release_available_session.isoformat(),
            "schema_digest": spec.schema_digest,
            "source_binding_digest": spec.source_binding_digest,
            "source_cutoff_session": spec.source_cutoff_session.isoformat(),
            "transform_semantics_digest": spec.transform_semantics_digest,
        }
    )


def control_object_pin(
    value: RunSpec | RunReceipt,
    *,
    path: str,
) -> ControlObjectPin:
    """Canonical-serialize and pin a run spec or receipt exactly."""

    _relative_path(path, "control object path")
    if isinstance(value, RunSpec):
        kind = ControlObjectKind.RUN_SPEC
        object_id = value.run_spec_id
    elif isinstance(value, RunReceipt):
        kind = ControlObjectKind.RUN_RECEIPT
        object_id = value.run_receipt_id
    else:  # pragma: no cover - type boundary
        raise IncrementalContractError("unsupported control object")
    content = _canonical_json_bytes(value.to_dict())
    return ControlObjectPin(
        object_kind=kind,
        object_id=object_id,
        artifact=ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        ),
    )


def validate_release_projection(
    spec: RunSpec,
    receipt: RunReceipt,
    manifest: IncrementalReleaseManifest,
    *,
    manifest_pin: ManifestPin,
    parent_release: ContentAttestedRelease | None,
) -> ControlValidatedCandidate:
    """Validate and mint a structural runtime candidate for one exact release.

    Gate A deliberately has no row semantic dispatcher.  Therefore any
    row-bearing candidate remains fail-closed until I3 supplies module-owned,
    table-specific proof verification; an arbitrary caller callback cannot
    bypass the boundary. This pure candidate is not production publication
    authority and does not establish that any pinned storage bytes exist.
    """

    verified_parent = (
        None if parent_release is None else verify_content_attested_release(parent_release)
    )
    return _validate_release_projection_once(
        spec,
        receipt,
        manifest,
        manifest_pin=manifest_pin,
        parent_release=verified_parent,
    )


def _validate_release_projection_once(
    spec: RunSpec,
    receipt: RunReceipt,
    manifest: IncrementalReleaseManifest,
    *,
    manifest_pin: ManifestPin,
    parent_release: ContentAttestedRelease | None,
) -> ControlValidatedCandidate:
    """Validate one node after its parent chain has already been attested."""

    if (
        not isinstance(spec, RunSpec)
        or not isinstance(receipt, RunReceipt)
        or not isinstance(manifest, IncrementalReleaseManifest)
    ):
        raise IncrementalContractError("release projection objects have invalid types")
    if not receipt.succeeded:
        raise IncrementalContractError("failed run receipts cannot materialize a release")
    if not isinstance(manifest_pin, ManifestPin):
        raise IncrementalContractError("release projection requires an exact manifest pin")
    if spec.release_type is ReleaseType.BASE:
        if parent_release is not None:
            raise IncrementalContractError("base projection cannot carry a validated parent")
    else:
        if not isinstance(parent_release, ContentAttestedRelease):
            raise IncrementalContractError(
                "non-base projection requires its exact content-attested parent"
            )
        if parent_release.manifest_pin != spec.parent_release_pin:
            raise IncrementalContractError(
                "validated parent differs from the run-spec exact parent pin"
            )
        parent_manifest = parent_release.manifest
        if spec.parent_identity_policy_bundle_id != (parent_manifest.identity_policy_bundle_id):
            raise IncrementalContractError(
                "run-spec prior identity policy differs from validated parent"
            )
        immutable_pairs = (
            (spec.resolved_view, parent_manifest.resolved_view, "resolved view"),
            (spec.schema_digest, parent_manifest.schema_digest, "schema digest"),
            (
                spec.transform_semantics_digest,
                parent_manifest.transform_semantics_digest,
                "transform semantics digest",
            ),
            (spec.calendar_digest, parent_manifest.calendar_digest, "calendar digest"),
            (spec.qa_policy.qa_policy_id, parent_manifest.qa_policy_id, "QA policy"),
        )
        for actual, expected, label in immutable_pairs:
            if actual != expected:
                raise IncrementalContractError(f"non-base release changed its parent {label}")
        if spec.source_cutoff_session < parent_manifest.source_cutoff_session:
            raise IncrementalContractError("child source cutoff precedes parent cutoff")
        if spec.availability_cutoff_session < parent_manifest.availability_cutoff_session:
            raise IncrementalContractError("child availability cutoff precedes parent cutoff")
        if (
            spec.release_type is ReleaseType.DELTA
            and spec.identity_policy_bundle_id != parent_manifest.identity_policy_bundle_id
        ):
            raise IncrementalContractError("clean delta changed its parent's identity policy")
    if receipt.run_spec_id != spec.run_spec_id:
        raise IncrementalContractError("run receipt belongs to another run spec")
    if receipt.peak_rss_bytes is None:
        raise IncrementalContractError("successful release requires peak RSS observation")
    if receipt.peak_rss_bytes > spec.rss_cap_bytes:
        raise IncrementalContractError("successful release exceeded RSS hard cap")
    if receipt.minimum_free_disk_bytes < spec.disk_floor_bytes:
        raise IncrementalContractError("successful release crossed disk hard floor")
    if (
        spec.wall_clock_cap_seconds is not None
        and receipt.runtime_seconds > spec.wall_clock_cap_seconds
    ):
        raise IncrementalContractError("successful release exceeded wall-clock cap")
    _verify_control_object_pin(spec, manifest.run_spec_pin)
    _verify_control_object_pin(receipt, manifest.run_receipt_pin)

    exact_pairs = (
        (manifest.release_type, spec.release_type, "release type"),
        (manifest.parent_release_pin, spec.parent_release_pin, "parent release pin"),
        (manifest.resolved_view, spec.resolved_view, "resolved view"),
        (manifest.source_binding_digest, spec.source_binding_digest, "source binding"),
        (manifest.source_cutoff_session, spec.source_cutoff_session, "source cutoff"),
        (
            manifest.availability_cutoff_session,
            spec.availability_cutoff_session,
            "availability cutoff",
        ),
        (
            manifest.release_available_session,
            spec.release_available_session,
            "release availability",
        ),
        (manifest.schema_digest, spec.schema_digest, "schema digest"),
        (
            manifest.transform_semantics_digest,
            spec.transform_semantics_digest,
            "transform semantics digest",
        ),
        (
            manifest.identity_policy_bundle_id,
            spec.identity_policy_bundle_id,
            "identity-policy bundle ID",
        ),
        (manifest.calendar_digest, spec.calendar_digest, "calendar digest"),
        (manifest.qa_policy_id, spec.qa_policy.qa_policy_id, "QA policy ID"),
    )
    for actual, expected, label in exact_pairs:
        if actual != expected:
            raise IncrementalContractError(f"manifest {label} differs from run spec")

    if receipt.actual_input_set_digest != spec.source_binding_digest:
        raise IncrementalContractError("actual inputs differ from run-spec source binding")
    change_set_digest = release_change_set_digest(manifest)
    if spec.expected_change_set_digest != change_set_digest:
        raise IncrementalContractError(
            "run-spec expected change set differs from release change set"
        )
    if receipt.output_set_digest != change_set_digest:
        raise IncrementalContractError("run output digest differs from release change set")
    qa_receipt = receipt.qa_receipt
    if qa_receipt is None:  # pragma: no cover - successful receipt invariant
        raise IncrementalContractError("successful release is missing structured QA")
    if manifest.qa_receipt_id != qa_receipt.qa_receipt_id:
        raise IncrementalContractError("manifest QA receipt ID differs from run receipt")
    try:
        validate_qa_for_publish(
            spec.qa_policy,
            qa_receipt,
            run_spec_id=spec.run_spec_id,
            source_binding_digest=spec.source_binding_digest,
            change_set_digest=change_set_digest,
            availability_cutoff_session=spec.availability_cutoff_session,
        )
    except IncrementalGateError as exc:
        raise IncrementalContractError(f"structured QA rejected publication: {exc}") from exc
    qa_results = {item.check_id: item for item in qa_receipt.results}
    expected_observations = {
        "partition_session_calendar_contiguous": len(manifest.added_partition_receipts)
        + len(manifest.partition_replacements),
        "row_semantic_proof_complete": len(manifest.added_row_version_receipts),
    }
    for check_id, expected_count in expected_observations.items():
        if qa_results[check_id].observed_count != expected_count:
            raise IncrementalContractError(
                f"structured QA {check_id} observation count does not cover exact changes"
            )
    if receipt.receipt_available_session > spec.release_available_session:
        raise IncrementalContractError("run-receipt availability exceeds release availability")
    if receipt.receipt_available_session < spec.availability_cutoff_session:
        raise IncrementalContractError(
            "run-receipt availability precedes the run availability cutoff"
        )
    checkpoint = receipt.checkpoint
    if checkpoint is None:  # pragma: no cover - successful receipt invariant
        raise IncrementalContractError("successful release is missing checkpoint")
    expected_parent = (
        spec.parent_release_pin.release_id if spec.parent_release_pin is not None else None
    )
    if checkpoint.parent_release_id != expected_parent:
        raise IncrementalContractError("checkpoint parent differs from run-spec parent")
    if checkpoint.last_session > spec.source_cutoff_session:
        raise IncrementalContractError("checkpoint last session exceeds source cutoff")
    checkpoint_sessions = [
        _session_text(item.partition_key, "checkpoint added partition key")
        for item in manifest.added_partition_receipts
    ] + [
        _session_text(
            item.replacement_receipt.partition_key,
            "checkpoint replacement partition key",
        )
        for item in manifest.partition_replacements
    ]
    if parent_release is not None:
        parent_checkpoint = parent_release.run_receipt.checkpoint
        if parent_checkpoint is None:  # pragma: no cover - attested parent invariant
            raise IncrementalContractError("attested parent is missing its checkpoint")
        checkpoint_sessions.append(parent_checkpoint.last_session)
    expected_last_session = max(checkpoint_sessions)
    if checkpoint.last_session != expected_last_session:
        raise IncrementalContractError(
            "checkpoint last session does not equal the resolved partition frontier"
        )
    if checkpoint.resolved_content_digest != manifest.resolved_content_digest:
        raise IncrementalContractError("checkpoint resolved content differs from release content")
    if checkpoint.rebuild_basis_digest != checkpoint_rebuild_basis_digest(spec, manifest):
        raise IncrementalContractError("checkpoint rebuild basis does not reproduce")

    if manifest.added_row_version_receipts:
        raise IncrementalContractError(
            "Gate A row semantic dispatcher is disabled; row-bearing releases are not "
            "publication-capable"
        )

    expected_authorization_id = (
        spec.correction_authorization.authorization.authorization_id
        if spec.correction_authorization is not None
        else None
    )
    if manifest.correction_authorization_id != expected_authorization_id:
        raise IncrementalContractError("manifest correction authorization ID differs from run spec")
    if spec.release_type is ReleaseType.CORRECTION:
        if parent_release is None:  # pragma: no cover - parent invariant above
            raise IncrementalContractError("correction requires a validated parent")
        authorization = spec.correction_authorization
        scope_digest = spec.correction_scope_digest
        if authorization is None or scope_digest is None:  # pragma: no cover
            raise IncrementalContractError("correction authority is incomplete")
        expected_scope = correction_scope_digest(
            parent_release_id=parent_release.manifest_pin.release_id,
            change_set_digest=change_set_digest,
        )
        if scope_digest != expected_scope:
            raise IncrementalContractError(
                "correction scope digest does not reproduce from exact changes"
            )
        try:
            validate_correction_authorization(
                authorization,
                parent_release_id=parent_release.manifest_pin.release_id,
                change_set_digest=change_set_digest,
                source_binding_digest=spec.source_binding_digest,
                schema_digest=spec.schema_digest,
                transform_semantics_digest=spec.transform_semantics_digest,
                calendar_digest=spec.calendar_digest,
                identity_policy_before_id=parent_release.manifest.identity_policy_bundle_id,
                identity_policy_after_id=spec.identity_policy_bundle_id,
                scope_digest=scope_digest,
                availability_cutoff_session=spec.availability_cutoff_session,
            )
        except IncrementalGateError as exc:
            raise IncrementalContractError(
                f"correction authorization rejected publication: {exc}"
            ) from exc

    expected_pin = manifest.exact_pin(manifest_path=manifest_pin.manifest_path)
    if manifest_pin != expected_pin:
        raise IncrementalContractError("external manifest pin does not reproduce")
    return _mint_control_validated_release(
        manifest_pin=manifest_pin,
        run_spec=spec,
        run_receipt=receipt,
        manifest=manifest,
        parent_release=parent_release,
    )


def replace_runtime_observation(receipt: RunReceipt, **changes: object) -> RunReceipt:
    """Change runtime-only observations without changing ``run_receipt_id``."""

    allowed = {"runtime_seconds", "peak_rss_bytes", "minimum_free_disk_bytes"}
    if not changes or not set(changes) <= allowed:
        raise IncrementalContractError("only runtime observations may be replaced")
    updated = replace(receipt, **changes)
    if updated.run_receipt_id != receipt.run_receipt_id:  # pragma: no cover
        raise IncrementalContractError("runtime observation changed the logical receipt ID")
    return updated


def _verify_control_object_pin(
    value: RunSpec | RunReceipt,
    pin: ControlObjectPin,
) -> None:
    expected = control_object_pin(value, path=pin.artifact.path)
    if pin != expected:
        raise IncrementalContractError("control object exact pin does not reproduce")


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _release_parent_shape(
    release_type: object,
    parent: ManifestPin | None,
) -> None:
    if not isinstance(release_type, ReleaseType):
        raise IncrementalContractError("release type is invalid")
    if release_type is ReleaseType.BASE:
        if parent is not None:
            raise IncrementalContractError("base release must not have a parent")
    elif not isinstance(parent, ManifestPin):
        raise IncrementalContractError("non-base release requires an exact parent pin")


def _parent_identity_policy_shape(
    release_type: ReleaseType,
    parent_identity_policy_bundle_id: str | None,
) -> None:
    if release_type is ReleaseType.BASE:
        if parent_identity_policy_bundle_id is not None:
            raise IncrementalContractError("base release cannot carry a prior identity-policy ID")
        return
    if parent_identity_policy_bundle_id is None:
        raise IncrementalContractError("non-base release requires its prior identity-policy ID")
    _digest(parent_identity_policy_bundle_id, "prior identity-policy bundle ID")


def _required_qa_policy(policy: QaPolicy) -> None:
    by_id = {item.check_id: item for item in policy.checks}
    for check_id, severity in _REQUIRED_QA_CHECKS.items():
        item = by_id.get(check_id)
        if item is None or item.severity is not severity:
            raise IncrementalContractError(
                f"QA policy requires {check_id} at severity {severity.value}"
            )


def _correction_authorization_shape(
    release_type: ReleaseType,
    scope_digest: str | None,
    authorization: PinnedCorrectionAuthorization | None,
) -> None:
    _optional_digest(scope_digest, "correction scope digest")
    if authorization is not None and not isinstance(
        authorization,
        PinnedCorrectionAuthorization,
    ):
        raise IncrementalContractError("correction authorization is invalid")
    if release_type is ReleaseType.CORRECTION:
        if scope_digest is None or authorization is None:
            raise IncrementalContractError(
                "correction release requires exact scope and typed authorization"
            )
    elif scope_digest is not None or authorization is not None:
        raise IncrementalContractError(
            "base/delta release cannot carry correction scope or authorization"
        )


def _correction_authorization_id_shape(
    release_type: ReleaseType,
    authorization_id: str | None,
) -> None:
    _optional_digest(authorization_id, "correction authorization ID")
    if release_type is ReleaseType.CORRECTION:
        if authorization_id is None:
            raise IncrementalContractError("correction manifest requires authorization ID")
    elif authorization_id is not None:
        raise IncrementalContractError(
            "base/delta manifest cannot carry correction authorization ID"
        )


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise IncrementalContractError(f"{label} must be a normalized relative path")
    return text


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IncrementalContractError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _digest(value, label)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise IncrementalContractError(f"{label} must be trimmed nonempty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise IncrementalContractError(f"{label} must be a date")
    return value


def _session_text(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise IncrementalContractError(f"{label} must be an ISO session date") from exc


def _cutoffs(source: object, availability: object) -> None:
    source_session = _session(source, "source cutoff session")
    availability_session = _session(availability, "availability cutoff session")
    if source_session > availability_session:
        raise IncrementalContractError("source cutoff exceeds availability cutoff")


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IncrementalContractError(f"{label} must be a positive integer")
    return value


def _optional_positive_int(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, label)


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IncrementalContractError(f"{label} must be a nonnegative integer")
    return value


def _nonnegative_number(value: object, label: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise IncrementalContractError(f"{label} must be finite and nonnegative")
    return value


def _typed_tuple(value: object, expected: type[object], label: str) -> tuple[object, ...]:
    if not isinstance(value, tuple) or any(not isinstance(item, expected) for item in value):
        raise IncrementalContractError(f"{label} must be a tuple of {expected.__name__}")
    return value


def _digest_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise IncrementalContractError(f"{label} must be a tuple")
    for item in value:
        _digest(item, label)
    if tuple(sorted(set(value))) != value:
        raise IncrementalContractError(f"{label} must be sorted and unique")
    return value


def _token_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise IncrementalContractError(f"{label} must be a tuple")
    if any(not isinstance(item, str) or _TOKEN.fullmatch(item) is None for item in value):
        raise IncrementalContractError(f"{label} must contain lowercase tokens")
    if tuple(sorted(set(value))) != value:
        raise IncrementalContractError(f"{label} must be sorted and unique")
    return value
