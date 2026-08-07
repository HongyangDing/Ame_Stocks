"""Pure lifecycle contracts for S7.5 shadow validation and cutover.

I5--I7 need a small control plane around the I2/I3 data contracts.  This
module deliberately performs no discovery, publication, or pointer mutation.
Exact artifact bytes enter only through a caller-supplied reader boundary; the
module defines content-addressed records and validates that a caller supplied
one complete, internally consistent control chain.

The two pointer-event types are immutable compare-and-swap facts.  Applying an
event to an external append-only ledger remains the responsibility of a
production writer; constructing one of these Python objects never grants that
authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.incremental_contract import ArtifactPin, ViewKind

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")

SHADOW_POINTER_NAME = "s7_5_shadow_top"
RESEARCH_POINTER_NAME = "s7_5_research_top"
GATE_B_LITERAL_VERSION = "s7_5_gate_b_exact_approval_v1"
GATE_C_LITERAL_VERSION = "s7_5_gate_c_exact_cutover_approval_v1"
LIFECYCLE_RULE_VERSION = "s7_5_i5_i7_lifecycle_v1"
I7_TABLE_ORDER = ("asset_master", "ticker_alias", "issuer_master", "universe_daily")

ExactArtifactReader = Callable[[str], bytes]


class IncrementalLifecycleError(SilverContractError):
    """Raised when an I5--I7 control object is incomplete or unsafe."""


class EquivalenceProjection(StrEnum):
    """The only two comparison layers accepted by Gate B."""

    CANONICAL_RESEARCH = "canonical_research_projection"
    PHYSICAL_REUSE = "physical_reuse_projection"


class FailureScenario(StrEnum):
    """Required shadow failure and recovery exercises."""

    CHECKPOINT_CORRUPTION = "checkpoint_corruption"
    CONCURRENT_LOCK = "concurrent_lock"
    DISK_HARD_FLOOR = "disk_hard_floor"
    DUPLICATE_RETRY = "duplicate_retry"
    INTERRUPTED_RUN = "interrupted_run"
    MISSING_PARENT = "missing_parent"


class GateBAction(StrEnum):
    """Gate B grants only creation of a non-research shadow pointer."""

    PUBLISH_EXACT_SHADOW_POINTER = "publish_exact_s7_5_shadow_pointer"


class GateCAction(StrEnum):
    """Gate C grants one exact compare-and-swap of the research top pointer."""

    CUTOVER_EXACT_RESEARCH_POINTER = "cutover_exact_s7_5_research_pointer"


class ReconciliationCadence(StrEnum):
    """Closed periodic full-reconciliation cadence."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


@dataclass(frozen=True, slots=True)
class ProjectionPolicy:
    """Frozen semantics for one Gate B comparison layer."""

    projection: EquivalenceProjection
    semantics_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.projection, EquivalenceProjection):
            raise IncrementalLifecycleError("equivalence projection is invalid")
        _digest(self.semantics_digest, "projection semantics digest")

    def to_dict(self) -> dict[str, str]:
        return {
            "projection": self.projection.value,
            "semantics_digest": self.semantics_digest,
        }


@dataclass(frozen=True, slots=True)
class ResourceGatePolicy:
    """Hard resource bounds used by the shadow run."""

    max_wall_clock_seconds: int
    max_peak_rss_bytes: int
    min_free_disk_bytes: int
    max_read_bytes: int
    max_write_bytes: int
    max_chain_resolution_milliseconds: int

    def __post_init__(self) -> None:
        _positive_int(self.max_wall_clock_seconds, "maximum wall-clock seconds")
        _positive_int(self.max_peak_rss_bytes, "maximum peak RSS bytes")
        _positive_int(self.min_free_disk_bytes, "minimum free disk bytes")
        _positive_int(self.max_read_bytes, "maximum read bytes")
        _positive_int(self.max_write_bytes, "maximum write bytes")
        _positive_int(
            self.max_chain_resolution_milliseconds,
            "maximum chain-resolution milliseconds",
        )

    @property
    def policy_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "max_chain_resolution_milliseconds": self.max_chain_resolution_milliseconds,
            "max_peak_rss_bytes": self.max_peak_rss_bytes,
            "max_read_bytes": self.max_read_bytes,
            "max_wall_clock_seconds": self.max_wall_clock_seconds,
            "max_write_bytes": self.max_write_bytes,
            "min_free_disk_bytes": self.min_free_disk_bytes,
            "rule_version": "s7_5_i5_resource_gate_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {"policy_id": self.policy_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    """Observed resource usage for one complete shadow run."""

    wall_clock_seconds: int
    peak_rss_bytes: int
    free_disk_bytes_at_floor: int
    read_bytes: int
    write_bytes: int
    chain_resolution_milliseconds: int

    def __post_init__(self) -> None:
        _nonnegative_int(self.wall_clock_seconds, "observed wall-clock seconds")
        _nonnegative_int(self.peak_rss_bytes, "observed peak RSS bytes")
        _nonnegative_int(self.free_disk_bytes_at_floor, "observed free disk bytes")
        _nonnegative_int(self.read_bytes, "observed read bytes")
        _nonnegative_int(self.write_bytes, "observed write bytes")
        _nonnegative_int(
            self.chain_resolution_milliseconds,
            "observed chain-resolution milliseconds",
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "chain_resolution_milliseconds": self.chain_resolution_milliseconds,
            "free_disk_bytes_at_floor": self.free_disk_bytes_at_floor,
            "peak_rss_bytes": self.peak_rss_bytes,
            "read_bytes": self.read_bytes,
            "wall_clock_seconds": self.wall_clock_seconds,
            "write_bytes": self.write_bytes,
        }


@dataclass(frozen=True, slots=True)
class ProjectionComparisonReceipt:
    """One deterministic comparison result."""

    projection: EquivalenceProjection
    semantics_digest: str
    compared_row_count: int
    incremental_projection_digest: str
    oracle_projection_digest: str
    unexpected_difference_count: int
    details_artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.projection, EquivalenceProjection):
            raise IncrementalLifecycleError("comparison projection is invalid")
        _digest(self.semantics_digest, "comparison semantics digest")
        _nonnegative_int(self.compared_row_count, "compared row count")
        _digest(self.incremental_projection_digest, "incremental projection digest")
        _digest(self.oracle_projection_digest, "oracle projection digest")
        _nonnegative_int(
            self.unexpected_difference_count,
            "unexpected difference count",
        )
        if not isinstance(self.details_artifact, ArtifactPin):
            raise IncrementalLifecycleError("comparison details artifact pin is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "compared_row_count": self.compared_row_count,
            "details_artifact": self.details_artifact.to_dict(),
            "incremental_projection_digest": self.incremental_projection_digest,
            "oracle_projection_digest": self.oracle_projection_digest,
            "projection": self.projection.value,
            "semantics_digest": self.semantics_digest,
            "unexpected_difference_count": self.unexpected_difference_count,
        }


@dataclass(frozen=True, slots=True)
class FailureRecoveryReceipt:
    """Evidence that one failure does not leak or damage the parent."""

    scenario: FailureScenario
    exercise_digest: str
    parent_reader_before_digest: str
    parent_reader_after_digest: str
    unpublished_visible_count: int
    deleted_artifact_count: int
    details_artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.scenario, FailureScenario):
            raise IncrementalLifecycleError("failure scenario is invalid")
        _digest(self.exercise_digest, "failure exercise digest")
        _digest(self.parent_reader_before_digest, "parent reader before digest")
        _digest(self.parent_reader_after_digest, "parent reader after digest")
        _nonnegative_int(self.unpublished_visible_count, "unpublished visible count")
        _nonnegative_int(self.deleted_artifact_count, "deleted artifact count")
        if not isinstance(self.details_artifact, ArtifactPin):
            raise IncrementalLifecycleError("failure details artifact pin is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "deleted_artifact_count": self.deleted_artifact_count,
            "details_artifact": self.details_artifact.to_dict(),
            "exercise_digest": self.exercise_digest,
            "parent_reader_after_digest": self.parent_reader_after_digest,
            "parent_reader_before_digest": self.parent_reader_before_digest,
            "scenario": self.scenario.value,
            "unpublished_visible_count": self.unpublished_visible_count,
        }


@dataclass(frozen=True, slots=True)
class IdempotencyReceipt:
    """The two attempts that must reproduce every durable identity."""

    first_run_receipt_id: str
    second_run_receipt_id: str
    first_checkpoint_id: str
    second_checkpoint_id: str
    first_release_id: str
    second_release_id: str
    first_manifest_sha256: str
    second_manifest_sha256: str

    def __post_init__(self) -> None:
        for label, value in (
            ("first run receipt ID", self.first_run_receipt_id),
            ("second run receipt ID", self.second_run_receipt_id),
            ("first checkpoint ID", self.first_checkpoint_id),
            ("second checkpoint ID", self.second_checkpoint_id),
            ("first release ID", self.first_release_id),
            ("second release ID", self.second_release_id),
            ("first manifest SHA-256", self.first_manifest_sha256),
            ("second manifest SHA-256", self.second_manifest_sha256),
        ):
            _digest(value, label)

    @property
    def reproduces(self) -> bool:
        return (
            self.first_run_receipt_id == self.second_run_receipt_id
            and self.first_checkpoint_id == self.second_checkpoint_id
            and self.first_release_id == self.second_release_id
            and self.first_manifest_sha256 == self.second_manifest_sha256
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "first_checkpoint_id": self.first_checkpoint_id,
            "first_manifest_sha256": self.first_manifest_sha256,
            "first_release_id": self.first_release_id,
            "first_run_receipt_id": self.first_run_receipt_id,
            "reproduces": self.reproduces,
            "second_checkpoint_id": self.second_checkpoint_id,
            "second_manifest_sha256": self.second_manifest_sha256,
            "second_release_id": self.second_release_id,
            "second_run_receipt_id": self.second_run_receipt_id,
        }


@dataclass(frozen=True, slots=True)
class ShadowEquivalenceSpec:
    """Exact I5 comparison inputs and policies."""

    incremental_release_id: str
    full_oracle_release_id: str
    common_parent_release_id: str
    source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    view: ViewKind
    comparison_cutoff_session: date
    comparison_sessions: tuple[date, ...]
    projection_policies: tuple[ProjectionPolicy, ...]
    resource_policy: ResourceGatePolicy

    def __post_init__(self) -> None:
        for label, value in (
            ("incremental release ID", self.incremental_release_id),
            ("full oracle release ID", self.full_oracle_release_id),
            ("common parent release ID", self.common_parent_release_id),
            ("source binding digest", self.source_binding_digest),
            ("schema bundle digest", self.schema_bundle_digest),
            ("transform semantics digest", self.transform_semantics_digest),
            ("identity policy bundle ID", self.identity_policy_bundle_id),
            ("calendar digest", self.calendar_digest),
        ):
            _digest(value, label)
        if not isinstance(self.view, ViewKind):
            raise IncrementalLifecycleError("shadow view is invalid")
        cutoff = _session(self.comparison_cutoff_session, "comparison cutoff session")
        _sessions(self.comparison_sessions, "comparison sessions")
        if self.comparison_sessions[-1] > cutoff:
            raise IncrementalLifecycleError("comparison session exceeds cutoff")
        _exact_projection_policies(self.projection_policies)
        if not isinstance(self.resource_policy, ResourceGatePolicy):
            raise IncrementalLifecycleError("shadow resource policy is invalid")

    @property
    def spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "calendar_digest": self.calendar_digest,
            "common_parent_release_id": self.common_parent_release_id,
            "comparison_cutoff_session": self.comparison_cutoff_session.isoformat(),
            "comparison_sessions": [item.isoformat() for item in self.comparison_sessions],
            "full_oracle_release_id": self.full_oracle_release_id,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "incremental_release_id": self.incremental_release_id,
            "projection_policies": [item.to_dict() for item in self.projection_policies],
            "resource_policy": self.resource_policy.to_dict(),
            "rule_version": "s7_5_i5_shadow_equivalence_spec_v1",
            "schema_bundle_digest": self.schema_bundle_digest,
            "source_binding_digest": self.source_binding_digest,
            "transform_semantics_digest": self.transform_semantics_digest,
            "view": self.view.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"spec_id": self.spec_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class ShadowEquivalenceReceipt:
    """Complete I5 result; Gate B derives success instead of trusting a flag."""

    spec_id: str
    incremental_release_id: str
    full_oracle_release_id: str
    source_binding_digest: str
    comparisons: tuple[ProjectionComparisonReceipt, ...]
    resource_observation: ResourceObservation
    failure_recovery: tuple[FailureRecoveryReceipt, ...]
    idempotency: IdempotencyReceipt
    receipt_available_session: date

    def __post_init__(self) -> None:
        for label, value in (
            ("shadow spec ID", self.spec_id),
            ("shadow incremental release ID", self.incremental_release_id),
            ("shadow full-oracle release ID", self.full_oracle_release_id),
            ("shadow source binding digest", self.source_binding_digest),
        ):
            _digest(value, label)
        _exact_comparison_receipts(self.comparisons)
        if not isinstance(self.resource_observation, ResourceObservation):
            raise IncrementalLifecycleError("shadow resource observation is invalid")
        _exact_failure_receipts(self.failure_recovery)
        if not isinstance(self.idempotency, IdempotencyReceipt):
            raise IncrementalLifecycleError("shadow idempotency receipt is invalid")
        _session(self.receipt_available_session, "shadow receipt availability")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "comparisons": [item.to_dict() for item in self.comparisons],
            "failure_recovery": [item.to_dict() for item in self.failure_recovery],
            "full_oracle_release_id": self.full_oracle_release_id,
            "idempotency": self.idempotency.to_dict(),
            "incremental_release_id": self.incremental_release_id,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "resource_observation": self.resource_observation.to_dict(),
            "source_binding_digest": self.source_binding_digest,
            "spec_id": self.spec_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}


def validate_shadow_equivalence(
    spec: ShadowEquivalenceSpec,
    receipt: ShadowEquivalenceReceipt,
    *,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    """Fail closed unless both comparison layers and every gate pass."""

    if not isinstance(spec, ShadowEquivalenceSpec) or not isinstance(
        receipt, ShadowEquivalenceReceipt
    ):
        raise IncrementalLifecycleError("shadow validation requires typed spec and receipt")
    expected = (
        (receipt.spec_id, spec.spec_id, "spec"),
        (
            receipt.incremental_release_id,
            spec.incremental_release_id,
            "incremental release",
        ),
        (
            receipt.full_oracle_release_id,
            spec.full_oracle_release_id,
            "full-oracle release",
        ),
        (receipt.source_binding_digest, spec.source_binding_digest, "source binding"),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalLifecycleError(f"shadow receipt {label} differs")
    cutoff = _session(availability_cutoff_session, "shadow availability cutoff")
    if receipt.receipt_available_session < spec.comparison_cutoff_session:
        raise IncrementalLifecycleError("shadow receipt predates its comparison cutoff")
    if receipt.receipt_available_session > cutoff:
        raise IncrementalLifecycleError("shadow receipt was unavailable at cutoff")
    policies = {item.projection: item for item in spec.projection_policies}
    for comparison in receipt.comparisons:
        _read_exact_pin(
            comparison.details_artifact,
            artifact_reader,
            f"{comparison.projection.value} comparison details",
        )
        policy = policies[comparison.projection]
        if comparison.semantics_digest != policy.semantics_digest:
            raise IncrementalLifecycleError("shadow comparison semantics differ")
        if comparison.compared_row_count == 0:
            raise IncrementalLifecycleError("shadow comparison covered no rows")
        if comparison.unexpected_difference_count != 0:
            raise IncrementalLifecycleError("shadow comparison has unexpected differences")
        if comparison.incremental_projection_digest != comparison.oracle_projection_digest:
            raise IncrementalLifecycleError("shadow projection digests differ")
    _validate_resources(spec.resource_policy, receipt.resource_observation)
    for failure in receipt.failure_recovery:
        _read_exact_pin(
            failure.details_artifact,
            artifact_reader,
            f"{failure.scenario.value} failure-recovery details",
        )
        if failure.parent_reader_before_digest != failure.parent_reader_after_digest:
            raise IncrementalLifecycleError("failure exercise changed the parent reader")
        if failure.unpublished_visible_count != 0:
            raise IncrementalLifecycleError("failure exercise exposed unpublished output")
        if failure.deleted_artifact_count != 0:
            raise IncrementalLifecycleError("failure exercise deleted an artifact")
    if not receipt.idempotency.reproduces:
        raise IncrementalLifecycleError("shadow retry was not idempotent")
    if receipt.idempotency.first_release_id != spec.incremental_release_id:
        raise IncrementalLifecycleError("idempotency receipt belongs to another release")


@dataclass(frozen=True, slots=True)
class GateBApproval:
    """Exact approval of one validated I5 receipt for shadow-only visibility."""

    spec_id: str
    receipt_id: str
    shadow_release_id: str
    full_oracle_release_id: str
    approver_id: str
    approval_available_session: date
    authorized_action: GateBAction = GateBAction.PUBLISH_EXACT_SHADOW_POINTER
    literal_version: str = GATE_B_LITERAL_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("Gate B spec ID", self.spec_id),
            ("Gate B receipt ID", self.receipt_id),
            ("Gate B shadow release ID", self.shadow_release_id),
            ("Gate B full-oracle release ID", self.full_oracle_release_id),
        ):
            _digest(value, label)
        _token(self.approver_id, "Gate B approver ID")
        _session(self.approval_available_session, "Gate B approval availability")
        if self.authorized_action is not GateBAction.PUBLISH_EXACT_SHADOW_POINTER:
            raise IncrementalLifecycleError("Gate B authorized action is invalid")
        if self.literal_version != GATE_B_LITERAL_VERSION:
            raise IncrementalLifecycleError("Gate B literal version is invalid")

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_available_session": self.approval_available_session.isoformat(),
            "approver_id": self.approver_id,
            "authorized_action": self.authorized_action.value,
            "full_oracle_release_id": self.full_oracle_release_id,
            "literal_version": self.literal_version,
            "receipt_id": self.receipt_id,
            "shadow_release_id": self.shadow_release_id,
            "spec_id": self.spec_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"approval_id": self.approval_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class PinnedGateBApproval:
    """Canonical Gate B body plus its exact immutable bytes pin."""

    approval: GateBApproval
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.approval, GateBApproval):
            raise IncrementalLifecycleError("Gate B approval body is invalid")
        _verify_exact_body_pin(self.approval.to_dict(), self.artifact, "Gate B approval")

    @classmethod
    def freeze(cls, approval: GateBApproval, *, path: str) -> PinnedGateBApproval:
        if not isinstance(approval, GateBApproval):
            raise IncrementalLifecycleError("Gate B approval body is invalid")
        return cls(approval=approval, artifact=_pin_body(approval.to_dict(), path=path))

    def to_dict(self) -> dict[str, object]:
        return {"approval": self.approval.to_dict(), "artifact": self.artifact.to_dict()}


def validate_gate_b_approval(
    pinned: PinnedGateBApproval,
    *,
    spec: ShadowEquivalenceSpec,
    receipt: ShadowEquivalenceReceipt,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    """Bind Gate B to the exact validated shadow result."""

    if not isinstance(pinned, PinnedGateBApproval):
        raise IncrementalLifecycleError("Gate B approval pin is invalid")
    _read_exact_body_pin(
        pinned.approval.to_dict(),
        pinned.artifact,
        artifact_reader,
        "Gate B approval",
    )
    validate_shadow_equivalence(
        spec,
        receipt,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    approval = pinned.approval
    expected = (
        (approval.spec_id, spec.spec_id, "spec"),
        (approval.receipt_id, receipt.receipt_id, "receipt"),
        (approval.shadow_release_id, spec.incremental_release_id, "shadow release"),
        (
            approval.full_oracle_release_id,
            spec.full_oracle_release_id,
            "full-oracle release",
        ),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalLifecycleError(f"Gate B approval {label} differs")
    cutoff = _session(availability_cutoff_session, "Gate B availability cutoff")
    if receipt.receipt_available_session > approval.approval_available_session:
        raise IncrementalLifecycleError("Gate B approval predates its receipt")
    if approval.approval_available_session > cutoff:
        raise IncrementalLifecycleError("Gate B approval was unavailable at cutoff")


@dataclass(frozen=True, slots=True)
class ShadowPointerEvent:
    """One immutable append-only shadow pointer compare-and-swap event."""

    gate_b_approval_id: str
    gate_b_approval_artifact: ArtifactPin
    expected_previous_event_id: str | None
    previous_release_id: str | None
    new_release_id: str
    pointer_revision: int
    event_available_session: date
    pointer_name: str = SHADOW_POINTER_NAME

    def __post_init__(self) -> None:
        _digest(self.gate_b_approval_id, "shadow Gate B approval ID")
        if not isinstance(self.gate_b_approval_artifact, ArtifactPin):
            raise IncrementalLifecycleError("shadow Gate B approval artifact is invalid")
        _optional_digest(self.expected_previous_event_id, "prior shadow pointer event ID")
        _optional_digest(self.previous_release_id, "prior shadow release ID")
        _digest(self.new_release_id, "new shadow release ID")
        _positive_int(self.pointer_revision, "shadow pointer revision")
        _session(self.event_available_session, "shadow event availability")
        if self.pointer_name != SHADOW_POINTER_NAME:
            raise IncrementalLifecycleError("shadow event cannot target a research pointer")
        if (self.expected_previous_event_id is None) != (self.previous_release_id is None):
            raise IncrementalLifecycleError("shadow prior event and release must coexist")
        if self.pointer_revision == 1 and self.expected_previous_event_id is not None:
            raise IncrementalLifecycleError("first shadow revision cannot have a prior event")
        if self.pointer_revision > 1 and self.expected_previous_event_id is None:
            raise IncrementalLifecycleError("later shadow revision requires a prior event")
        if self.previous_release_id == self.new_release_id:
            raise IncrementalLifecycleError("shadow pointer event is a no-op")

    @property
    def event_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "event_available_session": self.event_available_session.isoformat(),
            "expected_previous_event_id": self.expected_previous_event_id,
            "gate_b_approval_artifact": self.gate_b_approval_artifact.to_dict(),
            "gate_b_approval_id": self.gate_b_approval_id,
            "new_release_id": self.new_release_id,
            "pointer_name": self.pointer_name,
            "pointer_revision": self.pointer_revision,
            "previous_release_id": self.previous_release_id,
            "rule_version": "s7_5_i6_shadow_pointer_event_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.logical_payload()}


def validate_shadow_pointer_event(
    event: ShadowPointerEvent,
    *,
    gate_b: PinnedGateBApproval,
    spec: ShadowEquivalenceSpec,
    receipt: ShadowEquivalenceReceipt,
    observed_current_event_id: str | None,
    observed_current_release_id: str | None,
    observed_current_pointer_revision: int,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    """Validate shadow compare-and-swap without mutating any pointer."""

    if not isinstance(event, ShadowPointerEvent):
        raise IncrementalLifecycleError("shadow pointer event is invalid")
    validate_gate_b_approval(
        gate_b,
        spec=spec,
        receipt=receipt,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    if event.gate_b_approval_id != gate_b.approval.approval_id:
        raise IncrementalLifecycleError("shadow event Gate B approval ID differs")
    if event.gate_b_approval_artifact != gate_b.artifact:
        raise IncrementalLifecycleError("shadow event Gate B approval pin differs")
    if event.new_release_id != gate_b.approval.shadow_release_id:
        raise IncrementalLifecycleError("shadow event targets an unapproved release")
    if event.expected_previous_event_id != observed_current_event_id:
        raise IncrementalLifecycleError("shadow pointer event lost compare-and-swap race")
    if event.previous_release_id != observed_current_release_id:
        raise IncrementalLifecycleError("shadow pointer prior release differs")
    _nonnegative_int(observed_current_pointer_revision, "observed shadow pointer revision")
    if event.pointer_revision != observed_current_pointer_revision + 1:
        raise IncrementalLifecycleError("shadow pointer revision lost compare-and-swap race")
    if gate_b.approval.approval_available_session > event.event_available_session:
        raise IncrementalLifecycleError("shadow pointer event predates Gate B")
    cutoff = _session(availability_cutoff_session, "shadow event cutoff")
    if event.event_available_session > cutoff:
        raise IncrementalLifecycleError("shadow pointer event was unavailable at cutoff")


@dataclass(frozen=True, slots=True)
class RollbackPointerEvent:
    """Exact CAS event that moves the shadow pointer back to its parent release."""

    forward_shadow_event_id: str
    expected_previous_event_id: str
    previous_release_id: str
    new_release_id: str
    pointer_revision: int
    event_available_session: date
    pointer_name: str = SHADOW_POINTER_NAME

    def __post_init__(self) -> None:
        for label, value in (
            ("rollback forward shadow event ID", self.forward_shadow_event_id),
            ("rollback expected previous event ID", self.expected_previous_event_id),
            ("rollback previous release ID", self.previous_release_id),
            ("rollback new release ID", self.new_release_id),
        ):
            _digest(value, label)
        if self.forward_shadow_event_id != self.expected_previous_event_id:
            raise IncrementalLifecycleError("rollback CAS must immediately follow the shadow event")
        if self.previous_release_id == self.new_release_id:
            raise IncrementalLifecycleError("rollback pointer event is a no-op")
        _positive_int(self.pointer_revision, "rollback pointer revision")
        if self.pointer_revision < 2:
            raise IncrementalLifecycleError(
                "rollback pointer event requires a prior shadow revision"
            )
        _session(self.event_available_session, "rollback pointer-event availability")
        if self.pointer_name != SHADOW_POINTER_NAME:
            raise IncrementalLifecycleError("rollback event must target the shadow pointer")

    @property
    def event_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "event_available_session": self.event_available_session.isoformat(),
            "expected_previous_event_id": self.expected_previous_event_id,
            "forward_shadow_event_id": self.forward_shadow_event_id,
            "new_release_id": self.new_release_id,
            "pointer_name": self.pointer_name,
            "pointer_revision": self.pointer_revision,
            "previous_release_id": self.previous_release_id,
            "rule_version": "s7_5_i6_shadow_rollback_pointer_event_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class RollbackReceipt:
    """Proof that one exact rollback CAS restores the parent without deletion."""

    shadow_pointer_event_id: str
    rollback_pointer_event_id: str
    rolled_back_release_id: str
    selected_parent_release_id: str
    parent_reader_before_digest: str
    parent_reader_after_digest: str
    deleted_artifact_count: int
    surviving_artifact_set_digest: str
    details_artifact: ArtifactPin
    receipt_available_session: date

    def __post_init__(self) -> None:
        for label, value in (
            ("rollback shadow pointer event ID", self.shadow_pointer_event_id),
            ("rollback pointer event ID", self.rollback_pointer_event_id),
            ("rollback release ID", self.rolled_back_release_id),
            ("rollback selected parent release ID", self.selected_parent_release_id),
            ("rollback parent reader before digest", self.parent_reader_before_digest),
            ("rollback parent reader after digest", self.parent_reader_after_digest),
            ("rollback surviving artifact-set digest", self.surviving_artifact_set_digest),
        ):
            _digest(value, label)
        if self.rolled_back_release_id == self.selected_parent_release_id:
            raise IncrementalLifecycleError("rollback release and parent cannot be equal")
        _nonnegative_int(self.deleted_artifact_count, "rollback deleted artifact count")
        if not isinstance(self.details_artifact, ArtifactPin):
            raise IncrementalLifecycleError("rollback details artifact pin is invalid")
        _session(self.receipt_available_session, "rollback receipt availability")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "deleted_artifact_count": self.deleted_artifact_count,
            "details_artifact": self.details_artifact.to_dict(),
            "parent_reader_after_digest": self.parent_reader_after_digest,
            "parent_reader_before_digest": self.parent_reader_before_digest,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "rolled_back_release_id": self.rolled_back_release_id,
            "rollback_pointer_event_id": self.rollback_pointer_event_id,
            "selected_parent_release_id": self.selected_parent_release_id,
            "shadow_pointer_event_id": self.shadow_pointer_event_id,
            "surviving_artifact_set_digest": self.surviving_artifact_set_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}


def validate_rollback_receipt(
    receipt: RollbackReceipt,
    *,
    shadow_event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
    expected_parent_release_id: str,
    observed_current_event_id: str,
    observed_current_release_id: str,
    observed_current_pointer_revision: int,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    if (
        not isinstance(receipt, RollbackReceipt)
        or not isinstance(shadow_event, ShadowPointerEvent)
        or not isinstance(rollback_event, RollbackPointerEvent)
    ):
        raise IncrementalLifecycleError("rollback validation requires typed records")
    _digest(expected_parent_release_id, "expected rollback parent release ID")
    if receipt.shadow_pointer_event_id != shadow_event.event_id:
        raise IncrementalLifecycleError("rollback belongs to another shadow event")
    if receipt.rollback_pointer_event_id != rollback_event.event_id:
        raise IncrementalLifecycleError("rollback receipt binds another rollback pointer event")
    if receipt.rolled_back_release_id != shadow_event.new_release_id:
        raise IncrementalLifecycleError("rollback belongs to another shadow release")
    if receipt.selected_parent_release_id != expected_parent_release_id:
        raise IncrementalLifecycleError("rollback selected the wrong parent")
    if rollback_event.forward_shadow_event_id != shadow_event.event_id:
        raise IncrementalLifecycleError("rollback event follows another shadow event")
    if rollback_event.expected_previous_event_id != observed_current_event_id:
        raise IncrementalLifecycleError("rollback pointer event lost compare-and-swap race")
    if rollback_event.previous_release_id != observed_current_release_id:
        raise IncrementalLifecycleError("rollback pointer prior release differs")
    _positive_int(observed_current_pointer_revision, "observed rollback pointer revision")
    if rollback_event.pointer_revision != observed_current_pointer_revision + 1:
        raise IncrementalLifecycleError("rollback pointer revision lost compare-and-swap race")
    if rollback_event.pointer_revision != shadow_event.pointer_revision + 1:
        raise IncrementalLifecycleError(
            "rollback pointer revision must immediately increment the shadow event"
        )
    if rollback_event.previous_release_id != shadow_event.new_release_id:
        raise IncrementalLifecycleError("rollback event does not remove the shadow release")
    if rollback_event.new_release_id != expected_parent_release_id:
        raise IncrementalLifecycleError("rollback event does not select the exact parent")
    if receipt.parent_reader_before_digest != receipt.parent_reader_after_digest:
        raise IncrementalLifecycleError("rollback did not reproduce the parent reader")
    if receipt.deleted_artifact_count != 0:
        raise IncrementalLifecycleError("rollback deleted immutable artifacts")
    _read_exact_pin(receipt.details_artifact, artifact_reader, "rollback details")
    if shadow_event.event_available_session > rollback_event.event_available_session:
        raise IncrementalLifecycleError("rollback pointer event predates the shadow event")
    if rollback_event.event_available_session > receipt.receipt_available_session:
        raise IncrementalLifecycleError("rollback receipt predates the shadow event")
    cutoff = _session(availability_cutoff_session, "rollback availability cutoff")
    if receipt.receipt_available_session > cutoff:
        raise IncrementalLifecycleError("rollback receipt was unavailable at cutoff")


@dataclass(frozen=True, slots=True)
class GateCApproval:
    """Exact approval for one atomic research-top pointer transition."""

    gate_b_approval_id: str
    shadow_pointer_event_id: str
    rollback_receipt_id: str
    expected_previous_pointer_event_id: str
    expected_previous_release_id: str
    expected_previous_pointer_revision: int
    target_pointer_revision: int
    target_release_id: str
    approver_id: str
    approval_available_session: date
    authorized_action: GateCAction = GateCAction.CUTOVER_EXACT_RESEARCH_POINTER
    literal_version: str = GATE_C_LITERAL_VERSION
    pointer_name: str = RESEARCH_POINTER_NAME

    def __post_init__(self) -> None:
        for label, value in (
            ("Gate C Gate B approval ID", self.gate_b_approval_id),
            ("Gate C shadow pointer event ID", self.shadow_pointer_event_id),
            ("Gate C rollback receipt ID", self.rollback_receipt_id),
            (
                "Gate C expected previous pointer event ID",
                self.expected_previous_pointer_event_id,
            ),
            ("Gate C expected previous release ID", self.expected_previous_release_id),
            ("Gate C target release ID", self.target_release_id),
        ):
            _digest(value, label)
        if self.expected_previous_release_id == self.target_release_id:
            raise IncrementalLifecycleError("Gate C cannot approve a no-op cutover")
        _positive_int(
            self.expected_previous_pointer_revision,
            "Gate C expected previous pointer revision",
        )
        _positive_int(self.target_pointer_revision, "Gate C target pointer revision")
        if self.target_pointer_revision != self.expected_previous_pointer_revision + 1:
            raise IncrementalLifecycleError("Gate C target revision must increment exactly once")
        _token(self.approver_id, "Gate C approver ID")
        _session(self.approval_available_session, "Gate C approval availability")
        if self.authorized_action is not GateCAction.CUTOVER_EXACT_RESEARCH_POINTER:
            raise IncrementalLifecycleError("Gate C authorized action is invalid")
        if self.literal_version != GATE_C_LITERAL_VERSION:
            raise IncrementalLifecycleError("Gate C literal version is invalid")
        if self.pointer_name != RESEARCH_POINTER_NAME:
            raise IncrementalLifecycleError("Gate C must target the research pointer")

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_available_session": self.approval_available_session.isoformat(),
            "approver_id": self.approver_id,
            "authorized_action": self.authorized_action.value,
            "expected_previous_pointer_event_id": self.expected_previous_pointer_event_id,
            "expected_previous_pointer_revision": self.expected_previous_pointer_revision,
            "expected_previous_release_id": self.expected_previous_release_id,
            "gate_b_approval_id": self.gate_b_approval_id,
            "literal_version": self.literal_version,
            "pointer_name": self.pointer_name,
            "rollback_receipt_id": self.rollback_receipt_id,
            "shadow_pointer_event_id": self.shadow_pointer_event_id,
            "target_release_id": self.target_release_id,
            "target_pointer_revision": self.target_pointer_revision,
        }

    def to_dict(self) -> dict[str, object]:
        return {"approval_id": self.approval_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class PinnedGateCApproval:
    """Canonical Gate C body plus exact immutable bytes."""

    approval: GateCApproval
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.approval, GateCApproval):
            raise IncrementalLifecycleError("Gate C approval body is invalid")
        _verify_exact_body_pin(self.approval.to_dict(), self.artifact, "Gate C approval")

    @classmethod
    def freeze(cls, approval: GateCApproval, *, path: str) -> PinnedGateCApproval:
        if not isinstance(approval, GateCApproval):
            raise IncrementalLifecycleError("Gate C approval body is invalid")
        return cls(approval=approval, artifact=_pin_body(approval.to_dict(), path=path))

    def to_dict(self) -> dict[str, object]:
        return {"approval": self.approval.to_dict(), "artifact": self.artifact.to_dict()}


@dataclass(frozen=True, slots=True)
class TopPointerEvent:
    """One immutable compare-and-swap event for the public research top."""

    gate_c_approval_id: str
    gate_c_approval_artifact: ArtifactPin
    expected_previous_event_id: str
    previous_release_id: str
    new_release_id: str
    pointer_revision: int
    event_available_session: date
    pointer_name: str = RESEARCH_POINTER_NAME

    def __post_init__(self) -> None:
        _digest(self.gate_c_approval_id, "top-pointer Gate C approval ID")
        if not isinstance(self.gate_c_approval_artifact, ArtifactPin):
            raise IncrementalLifecycleError("top-pointer Gate C artifact is invalid")
        for label, value in (
            ("top-pointer previous event ID", self.expected_previous_event_id),
            ("top-pointer previous release ID", self.previous_release_id),
            ("top-pointer new release ID", self.new_release_id),
        ):
            _digest(value, label)
        if self.previous_release_id == self.new_release_id:
            raise IncrementalLifecycleError("top-pointer event is a no-op")
        _positive_int(self.pointer_revision, "top-pointer revision")
        if self.pointer_revision < 2:
            raise IncrementalLifecycleError("research cutover must follow a prior pointer")
        _session(self.event_available_session, "top-pointer event availability")
        if self.pointer_name != RESEARCH_POINTER_NAME:
            raise IncrementalLifecycleError("top-pointer event targets the wrong namespace")

    @property
    def event_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "event_available_session": self.event_available_session.isoformat(),
            "expected_previous_event_id": self.expected_previous_event_id,
            "gate_c_approval_artifact": self.gate_c_approval_artifact.to_dict(),
            "gate_c_approval_id": self.gate_c_approval_id,
            "new_release_id": self.new_release_id,
            "pointer_name": self.pointer_name,
            "pointer_revision": self.pointer_revision,
            "previous_release_id": self.previous_release_id,
            "rule_version": "s7_5_i6_research_top_pointer_event_v1",
        }

    def to_dict(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.logical_payload()}


def validate_atomic_cutover(
    gate_c: PinnedGateCApproval,
    event: TopPointerEvent,
    *,
    gate_b: PinnedGateBApproval,
    shadow_spec: ShadowEquivalenceSpec,
    shadow_receipt: ShadowEquivalenceReceipt,
    shadow_event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
    rollback_receipt: RollbackReceipt,
    shadow_observed_previous_event_id: str | None,
    shadow_observed_previous_release_id: str | None,
    shadow_observed_previous_pointer_revision: int,
    rollback_observed_current_event_id: str,
    rollback_observed_current_release_id: str,
    rollback_observed_current_pointer_revision: int,
    observed_current_event_id: str,
    observed_current_release_id: str,
    observed_current_pointer_revision: int,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    """Validate one exact research-pointer CAS; never apply it."""

    if not isinstance(gate_c, PinnedGateCApproval) or not isinstance(event, TopPointerEvent):
        raise IncrementalLifecycleError("cutover requires typed approval and event")
    _read_exact_body_pin(
        gate_c.approval.to_dict(),
        gate_c.artifact,
        artifact_reader,
        "Gate C approval",
    )
    validate_shadow_pointer_event(
        shadow_event,
        gate_b=gate_b,
        spec=shadow_spec,
        receipt=shadow_receipt,
        observed_current_event_id=shadow_observed_previous_event_id,
        observed_current_release_id=shadow_observed_previous_release_id,
        observed_current_pointer_revision=shadow_observed_previous_pointer_revision,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    approval = gate_c.approval
    if approval.gate_b_approval_id != gate_b.approval.approval_id:
        raise IncrementalLifecycleError("Gate C binds another Gate B approval")
    if approval.shadow_pointer_event_id != shadow_event.event_id:
        raise IncrementalLifecycleError("Gate C binds another shadow event")
    if approval.rollback_receipt_id != rollback_receipt.receipt_id:
        raise IncrementalLifecycleError("Gate C binds another rollback receipt")
    if approval.target_release_id != gate_b.approval.shadow_release_id:
        raise IncrementalLifecycleError("Gate C target was not approved by Gate B")
    if shadow_event.new_release_id != approval.target_release_id:
        raise IncrementalLifecycleError("Gate C target differs from shadow publication")
    validate_rollback_receipt(
        rollback_receipt,
        shadow_event=shadow_event,
        rollback_event=rollback_event,
        expected_parent_release_id=approval.expected_previous_release_id,
        observed_current_event_id=rollback_observed_current_event_id,
        observed_current_release_id=rollback_observed_current_release_id,
        observed_current_pointer_revision=rollback_observed_current_pointer_revision,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    if approval.approval_available_session < rollback_receipt.receipt_available_session:
        raise IncrementalLifecycleError("Gate C approval predates rollback evidence")
    if event.gate_c_approval_id != approval.approval_id:
        raise IncrementalLifecycleError("top-pointer event Gate C ID differs")
    if event.gate_c_approval_artifact != gate_c.artifact:
        raise IncrementalLifecycleError("top-pointer event Gate C pin differs")
    expected = (
        (
            approval.expected_previous_pointer_event_id,
            observed_current_event_id,
            "approval prior event",
        ),
        (
            approval.expected_previous_release_id,
            observed_current_release_id,
            "approval prior release",
        ),
        (event.expected_previous_event_id, observed_current_event_id, "event prior event"),
        (event.previous_release_id, observed_current_release_id, "event prior release"),
        (event.new_release_id, approval.target_release_id, "event target release"),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalLifecycleError(f"atomic cutover {label} differs")
    _positive_int(observed_current_pointer_revision, "observed research pointer revision")
    if approval.expected_previous_pointer_revision != observed_current_pointer_revision:
        raise IncrementalLifecycleError("atomic cutover approval prior revision differs")
    if event.pointer_revision != approval.target_pointer_revision:
        raise IncrementalLifecycleError("atomic cutover event target revision differs")
    if approval.approval_available_session > event.event_available_session:
        raise IncrementalLifecycleError("top-pointer event predates Gate C")
    cutoff = _session(availability_cutoff_session, "cutover availability cutoff")
    if event.event_available_session > cutoff:
        raise IncrementalLifecycleError("top-pointer event was unavailable at cutoff")


@dataclass(frozen=True, slots=True)
class FullReconciliationTableScope:
    """Closed logical partition scope for one I7 table."""

    table_name: str
    partition_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.table_name not in I7_TABLE_ORDER:
            raise IncrementalLifecycleError("reconciliation scope table is invalid")
        if not isinstance(self.partition_keys, tuple) or not self.partition_keys:
            raise IncrementalLifecycleError("reconciliation scope requires partition keys")
        if any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in self.partition_keys
        ):
            raise IncrementalLifecycleError("reconciliation scope partition key is invalid")
        if self.partition_keys != tuple(sorted(set(self.partition_keys))):
            raise IncrementalLifecycleError(
                "reconciliation scope partition keys must be sorted and unique"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "partition_keys": list(self.partition_keys),
            "table_name": self.table_name,
        }


@dataclass(frozen=True, slots=True)
class FullReconciliationSpec:
    """Independent I7 full rebuild comparison contract."""

    incremental_top_release_id: str
    independent_full_candidate_release_id: str
    bronze_source_binding_digest: str
    s4_source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    view: ViewKind
    reconciliation_cutoff_session: date
    canonical_projection_semantics_digest: str
    checkpoint_rebase_semantics_digest: str
    trigger_policy_digest: str
    table_scopes: tuple[FullReconciliationTableScope, ...]
    cadence: ReconciliationCadence

    def __post_init__(self) -> None:
        for label, value in (
            ("reconciliation incremental top release ID", self.incremental_top_release_id),
            (
                "reconciliation independent full release ID",
                self.independent_full_candidate_release_id,
            ),
            ("reconciliation Bronze source binding", self.bronze_source_binding_digest),
            ("reconciliation S4 source binding", self.s4_source_binding_digest),
            ("reconciliation schema bundle", self.schema_bundle_digest),
            ("reconciliation transform semantics", self.transform_semantics_digest),
            ("reconciliation identity policy", self.identity_policy_bundle_id),
            ("reconciliation calendar", self.calendar_digest),
            (
                "reconciliation canonical projection semantics",
                self.canonical_projection_semantics_digest,
            ),
            (
                "reconciliation checkpoint-rebase semantics",
                self.checkpoint_rebase_semantics_digest,
            ),
            ("reconciliation trigger policy", self.trigger_policy_digest),
        ):
            _digest(value, label)
        if not isinstance(self.view, ViewKind):
            raise IncrementalLifecycleError("reconciliation view is invalid")
        _session(self.reconciliation_cutoff_session, "reconciliation cutoff")
        if not isinstance(self.cadence, ReconciliationCadence):
            raise IncrementalLifecycleError("reconciliation cadence is invalid")
        if not isinstance(self.table_scopes, tuple) or any(
            not isinstance(item, FullReconciliationTableScope) for item in self.table_scopes
        ):
            raise IncrementalLifecycleError("reconciliation table scopes must be typed records")
        if tuple(item.table_name for item in self.table_scopes) != I7_TABLE_ORDER:
            raise IncrementalLifecycleError(
                "reconciliation scopes must cover the exact four-table order"
            )
        if self.incremental_top_release_id == self.independent_full_candidate_release_id:
            raise IncrementalLifecycleError("full oracle must be independently identified")

    @property
    def spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "bronze_source_binding_digest": self.bronze_source_binding_digest,
            "cadence": self.cadence.value,
            "calendar_digest": self.calendar_digest,
            "canonical_projection_semantics_digest": (self.canonical_projection_semantics_digest),
            "checkpoint_rebase_semantics_digest": self.checkpoint_rebase_semantics_digest,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "incremental_top_release_id": self.incremental_top_release_id,
            "independent_full_candidate_release_id": (self.independent_full_candidate_release_id),
            "reconciliation_cutoff_session": self.reconciliation_cutoff_session.isoformat(),
            "rule_version": "s7_5_i7_full_reconciliation_spec_v1",
            "s4_source_binding_digest": self.s4_source_binding_digest,
            "schema_bundle_digest": self.schema_bundle_digest,
            "table_scopes": [item.to_dict() for item in self.table_scopes],
            "transform_semantics_digest": self.transform_semantics_digest,
            "trigger_policy_digest": self.trigger_policy_digest,
            "view": self.view.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"spec_id": self.spec_id, **self.logical_payload()}

    def table_semantics_digest(self, table_name: str) -> str:
        if table_name not in I7_TABLE_ORDER:
            raise IncrementalLifecycleError("reconciliation table is invalid")
        table_scope = self.table_scopes[I7_TABLE_ORDER.index(table_name)]
        return stable_digest(
            {
                "canonical_projection_semantics_digest": (
                    self.canonical_projection_semantics_digest
                ),
                "partition_keys": list(table_scope.partition_keys),
                "rule_version": "s7_5_i7_table_reconciliation_semantics_v1",
                "table_name": table_name,
            }
        )


@dataclass(frozen=True, slots=True)
class FullReconciliationPartitionEvidence:
    """Exact per-partition I7 comparison evidence."""

    table_name: str
    partition_key: str
    compared_row_count: int
    incremental_projection_digest: str
    full_projection_digest: str
    unexpected_difference_count: int
    details_artifact: ArtifactPin

    def __post_init__(self) -> None:
        if self.table_name not in I7_TABLE_ORDER:
            raise IncrementalLifecycleError("reconciliation partition table is invalid")
        if (
            not isinstance(self.partition_key, str)
            or not self.partition_key
            or self.partition_key != self.partition_key.strip()
        ):
            raise IncrementalLifecycleError("reconciliation partition key is invalid")
        _positive_int(self.compared_row_count, "partition compared row count")
        _digest(self.incremental_projection_digest, "partition incremental projection digest")
        _digest(self.full_projection_digest, "partition full projection digest")
        _nonnegative_int(
            self.unexpected_difference_count,
            "partition unexpected difference count",
        )
        if not isinstance(self.details_artifact, ArtifactPin):
            raise IncrementalLifecycleError("partition reconciliation details pin is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "compared_row_count": self.compared_row_count,
            "details_artifact": self.details_artifact.to_dict(),
            "full_projection_digest": self.full_projection_digest,
            "incremental_projection_digest": self.incremental_projection_digest,
            "partition_key": self.partition_key,
            "table_name": self.table_name,
            "unexpected_difference_count": self.unexpected_difference_count,
        }


@dataclass(frozen=True, slots=True)
class FullReconciliationTableEvidence:
    """Closed per-table evidence containing every compared logical partition."""

    table_name: str
    semantics_digest: str
    partitions: tuple[FullReconciliationPartitionEvidence, ...]

    def __post_init__(self) -> None:
        if self.table_name not in I7_TABLE_ORDER:
            raise IncrementalLifecycleError("reconciliation table is invalid")
        _digest(self.semantics_digest, "table reconciliation semantics digest")
        if not isinstance(self.partitions, tuple) or not self.partitions:
            raise IncrementalLifecycleError("table reconciliation requires partition evidence")
        if any(
            not isinstance(item, FullReconciliationPartitionEvidence)
            or item.table_name != self.table_name
            for item in self.partitions
        ):
            raise IncrementalLifecycleError("table reconciliation crossed a table boundary")
        keys = tuple(item.partition_key for item in self.partitions)
        if keys != tuple(sorted(set(keys))):
            raise IncrementalLifecycleError(
                "table reconciliation partition keys must be sorted and unique"
            )

    @property
    def compared_row_count(self) -> int:
        return sum(item.compared_row_count for item in self.partitions)

    @property
    def incremental_projection_digest(self) -> str:
        return stable_digest(
            [
                {
                    "partition_key": item.partition_key,
                    "projection_digest": item.incremental_projection_digest,
                }
                for item in self.partitions
            ]
        )

    @property
    def full_projection_digest(self) -> str:
        return stable_digest(
            [
                {
                    "partition_key": item.partition_key,
                    "projection_digest": item.full_projection_digest,
                }
                for item in self.partitions
            ]
        )

    @property
    def unexpected_difference_count(self) -> int:
        return sum(item.unexpected_difference_count for item in self.partitions)

    def to_dict(self) -> dict[str, object]:
        return {
            "compared_partition_count": len(self.partitions),
            "compared_row_count": self.compared_row_count,
            "full_projection_digest": self.full_projection_digest,
            "incremental_projection_digest": self.incremental_projection_digest,
            "partitions": [item.to_dict() for item in self.partitions],
            "semantics_digest": self.semantics_digest,
            "table_name": self.table_name,
            "unexpected_difference_count": self.unexpected_difference_count,
        }


@dataclass(frozen=True, slots=True)
class FullReconciliationReceipt:
    """I7 result proving full-oracle and resolved-chain equivalence."""

    spec_id: str
    incremental_top_release_id: str
    independent_full_candidate_release_id: str
    table_evidence: tuple[FullReconciliationTableEvidence, ...]
    checkpoint_before_projection_digest: str
    checkpoint_rebased_projection_digest: str
    qa_artifact: ArtifactPin
    details_artifact: ArtifactPin
    receipt_available_session: date

    def __post_init__(self) -> None:
        for label, value in (
            ("full reconciliation spec ID", self.spec_id),
            (
                "full reconciliation incremental release ID",
                self.incremental_top_release_id,
            ),
            (
                "full reconciliation independent release ID",
                self.independent_full_candidate_release_id,
            ),
            (
                "full reconciliation checkpoint-before digest",
                self.checkpoint_before_projection_digest,
            ),
            (
                "full reconciliation checkpoint-rebased digest",
                self.checkpoint_rebased_projection_digest,
            ),
        ):
            _digest(value, label)
        if not isinstance(self.table_evidence, tuple) or any(
            not isinstance(item, FullReconciliationTableEvidence) for item in self.table_evidence
        ):
            raise IncrementalLifecycleError(
                "full reconciliation evidence must contain typed table records"
            )
        if tuple(item.table_name for item in self.table_evidence) != I7_TABLE_ORDER:
            raise IncrementalLifecycleError(
                "full reconciliation evidence must cover the exact four-table order"
            )
        if self.compared_row_count <= 0:
            raise IncrementalLifecycleError("full reconciliation row count must be positive")
        if not isinstance(self.qa_artifact, ArtifactPin):
            raise IncrementalLifecycleError("full reconciliation QA pin is invalid")
        if not isinstance(self.details_artifact, ArtifactPin):
            raise IncrementalLifecycleError("full reconciliation details pin is invalid")
        _session(self.receipt_available_session, "full reconciliation availability")

    @property
    def compared_row_count(self) -> int:
        return sum(item.compared_row_count for item in self.table_evidence)

    @property
    def compared_partition_count(self) -> int:
        return sum(len(item.partitions) for item in self.table_evidence)

    @property
    def incremental_projection_digest(self) -> str:
        return stable_digest(
            [
                {
                    "projection_digest": item.incremental_projection_digest,
                    "table_name": item.table_name,
                }
                for item in self.table_evidence
            ]
        )

    @property
    def full_projection_digest(self) -> str:
        return stable_digest(
            [
                {
                    "projection_digest": item.full_projection_digest,
                    "table_name": item.table_name,
                }
                for item in self.table_evidence
            ]
        )

    @property
    def unexpected_difference_count(self) -> int:
        return sum(item.unexpected_difference_count for item in self.table_evidence)

    @property
    def qa_receipt_id(self) -> str:
        return stable_digest(
            {
                "artifact": self.qa_artifact.to_dict(),
                "rule_version": "s7_5_i7_full_reconciliation_qa_receipt_v1",
            }
        )

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "checkpoint_before_projection_digest": self.checkpoint_before_projection_digest,
            "checkpoint_rebased_projection_digest": (self.checkpoint_rebased_projection_digest),
            "compared_partition_count": self.compared_partition_count,
            "compared_row_count": self.compared_row_count,
            "details_artifact": self.details_artifact.to_dict(),
            "full_projection_digest": self.full_projection_digest,
            "incremental_projection_digest": self.incremental_projection_digest,
            "incremental_top_release_id": self.incremental_top_release_id,
            "independent_full_candidate_release_id": (self.independent_full_candidate_release_id),
            "qa_artifact": self.qa_artifact.to_dict(),
            "qa_receipt_id": self.qa_receipt_id,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "spec_id": self.spec_id,
            "table_evidence": [item.to_dict() for item in self.table_evidence],
            "unexpected_difference_count": self.unexpected_difference_count,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}


def validate_full_reconciliation(
    spec: FullReconciliationSpec,
    receipt: FullReconciliationReceipt,
    *,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    if not isinstance(spec, FullReconciliationSpec) or not isinstance(
        receipt, FullReconciliationReceipt
    ):
        raise IncrementalLifecycleError("full reconciliation requires typed records")
    expected = (
        (receipt.spec_id, spec.spec_id, "spec"),
        (
            receipt.incremental_top_release_id,
            spec.incremental_top_release_id,
            "incremental release",
        ),
        (
            receipt.independent_full_candidate_release_id,
            spec.independent_full_candidate_release_id,
            "independent full release",
        ),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalLifecycleError(f"full reconciliation {label} differs")
    cutoff = _session(availability_cutoff_session, "full reconciliation cutoff")
    if receipt.receipt_available_session < spec.reconciliation_cutoff_session:
        raise IncrementalLifecycleError("full reconciliation predates its data cutoff")
    if receipt.receipt_available_session > cutoff:
        raise IncrementalLifecycleError("full reconciliation was unavailable at cutoff")
    _read_exact_pin(receipt.qa_artifact, artifact_reader, "full reconciliation QA")
    _read_exact_pin(receipt.details_artifact, artifact_reader, "full reconciliation details")
    scopes = {item.table_name: item for item in spec.table_scopes}
    for table in receipt.table_evidence:
        if (
            tuple(item.partition_key for item in table.partitions)
            != scopes[table.table_name].partition_keys
        ):
            raise IncrementalLifecycleError(
                "full reconciliation partition scope differs from the spec"
            )
        if table.semantics_digest != spec.table_semantics_digest(table.table_name):
            raise IncrementalLifecycleError("full reconciliation table semantics differ")
        for partition in table.partitions:
            _read_exact_pin(
                partition.details_artifact,
                artifact_reader,
                f"{table.table_name}/{partition.partition_key} reconciliation details",
            )
            if partition.unexpected_difference_count != 0:
                raise IncrementalLifecycleError(
                    "full reconciliation partition has unexpected differences"
                )
            if partition.incremental_projection_digest != partition.full_projection_digest:
                raise IncrementalLifecycleError(
                    "full reconciliation partition projection digests differ"
                )
        if table.unexpected_difference_count != 0:
            raise IncrementalLifecycleError("full reconciliation table has unexpected differences")
        if table.incremental_projection_digest != table.full_projection_digest:
            raise IncrementalLifecycleError("full reconciliation table projection digests differ")
    if receipt.unexpected_difference_count != 0:
        raise IncrementalLifecycleError("full reconciliation has unexpected differences")
    if receipt.incremental_projection_digest != receipt.full_projection_digest:
        raise IncrementalLifecycleError("full reconciliation projection digests differ")
    if receipt.checkpoint_before_projection_digest != receipt.checkpoint_rebased_projection_digest:
        raise IncrementalLifecycleError("checkpoint rebase changed logical rows")


ACCEPTANCE_CRITERIA: tuple[str, ...] = (
    "legacy_s7_byte_exact_and_deep_valid",
    "clean_session_does_not_rebuild_history",
    "single_session_io_is_boundary_bounded",
    "clean_append_preserves_prior_partitions_and_segment_ids",
    "single_session_resource_targets_pass",
    "durable_retry_is_idempotent",
    "unknown_identity_and_collision_fail_closed",
    "historical_correction_is_exact_scope_and_reproducible",
    "alias_and_identity_quality_safety_invariants_pass",
    "incremental_and_full_oracle_are_equivalent",
    "failed_delta_is_invisible_and_rollback_is_lossless",
    "schema_transform_policy_and_runtime_provenance_are_separate",
    "s8_new_listing_membership_is_preserved_and_research_ineligible",
)


@dataclass(frozen=True, slots=True)
class AcceptanceCriterionReceipt:
    """Evidence-backed result for one of the fixed thirteen exit criteria."""

    criterion_number: int
    criterion_id: str
    semantics_digest: str
    evidence_ids: tuple[str, ...]
    passed: bool

    def __post_init__(self) -> None:
        if type(self.criterion_number) is not int or not 1 <= self.criterion_number <= 13:
            raise IncrementalLifecycleError("acceptance criterion number must be 1..13")
        if self.criterion_id != ACCEPTANCE_CRITERIA[self.criterion_number - 1]:
            raise IncrementalLifecycleError("acceptance criterion ID does not match number")
        _digest(self.semantics_digest, "acceptance criterion semantics digest")
        _digests(self.evidence_ids, "acceptance evidence IDs")
        if type(self.passed) is not bool:
            raise IncrementalLifecycleError("acceptance criterion passed must be boolean")

    def to_dict(self) -> dict[str, object]:
        return {
            "criterion_id": self.criterion_id,
            "criterion_number": self.criterion_number,
            "evidence_ids": list(self.evidence_ids),
            "passed": self.passed,
            "semantics_digest": self.semantics_digest,
        }


@dataclass(frozen=True, slots=True)
class S75CompletionManifest:
    """Final S7.5 completion marker binding the complete I0--I7 evidence graph."""

    legacy_s7_release_set_id: str
    gate_a_approval_id: str
    i2_acceptance_receipt_id: str
    i3_acceptance_receipt_id: str
    i4_acceptance_receipt_id: str
    shadow_equivalence_receipt_id: str
    gate_b_approval_id: str
    shadow_pointer_event_id: str
    rollback_pointer_event_id: str
    rollback_receipt_id: str
    gate_c_approval_id: str
    top_pointer_event_id: str
    full_reconciliation_receipt_id: str
    final_top_release_id: str
    acceptance_criteria: tuple[AcceptanceCriterionReceipt, ...]
    completion_available_session: date

    def __post_init__(self) -> None:
        for label, value in (
            ("legacy S7 release-set ID", self.legacy_s7_release_set_id),
            ("Gate A approval ID", self.gate_a_approval_id),
            ("I2 acceptance receipt ID", self.i2_acceptance_receipt_id),
            ("I3 acceptance receipt ID", self.i3_acceptance_receipt_id),
            ("I4 acceptance receipt ID", self.i4_acceptance_receipt_id),
            ("shadow equivalence receipt ID", self.shadow_equivalence_receipt_id),
            ("Gate B approval ID", self.gate_b_approval_id),
            ("shadow pointer event ID", self.shadow_pointer_event_id),
            ("rollback pointer event ID", self.rollback_pointer_event_id),
            ("rollback receipt ID", self.rollback_receipt_id),
            ("Gate C approval ID", self.gate_c_approval_id),
            ("top pointer event ID", self.top_pointer_event_id),
            ("full reconciliation receipt ID", self.full_reconciliation_receipt_id),
            ("final top release ID", self.final_top_release_id),
        ):
            _digest(value, label)
        _exact_acceptance_criteria(self.acceptance_criteria)
        _session(self.completion_available_session, "S7.5 completion availability")

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "acceptance_criteria": [item.to_dict() for item in self.acceptance_criteria],
            "completion_available_session": self.completion_available_session.isoformat(),
            "final_top_release_id": self.final_top_release_id,
            "full_reconciliation_receipt_id": self.full_reconciliation_receipt_id,
            "gate_a_approval_id": self.gate_a_approval_id,
            "gate_b_approval_id": self.gate_b_approval_id,
            "gate_c_approval_id": self.gate_c_approval_id,
            "i2_acceptance_receipt_id": self.i2_acceptance_receipt_id,
            "i3_acceptance_receipt_id": self.i3_acceptance_receipt_id,
            "i4_acceptance_receipt_id": self.i4_acceptance_receipt_id,
            "legacy_s7_release_set_id": self.legacy_s7_release_set_id,
            "rollback_pointer_event_id": self.rollback_pointer_event_id,
            "rollback_receipt_id": self.rollback_receipt_id,
            "rule_version": LIFECYCLE_RULE_VERSION,
            "shadow_equivalence_receipt_id": self.shadow_equivalence_receipt_id,
            "shadow_pointer_event_id": self.shadow_pointer_event_id,
            "top_pointer_event_id": self.top_pointer_event_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"completion_id": self.completion_id, **self.logical_payload()}


def validate_s75_completion(
    manifest: S75CompletionManifest,
    *,
    shadow_spec: ShadowEquivalenceSpec,
    shadow_receipt: ShadowEquivalenceReceipt,
    gate_b: PinnedGateBApproval,
    shadow_event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
    rollback_receipt: RollbackReceipt,
    gate_c: PinnedGateCApproval,
    top_event: TopPointerEvent,
    full_reconciliation_spec: FullReconciliationSpec,
    full_reconciliation_receipt: FullReconciliationReceipt,
    shadow_observed_previous_event_id: str | None,
    shadow_observed_previous_release_id: str | None,
    shadow_observed_previous_pointer_revision: int,
    rollback_observed_current_event_id: str,
    rollback_observed_current_release_id: str,
    rollback_observed_current_pointer_revision: int,
    research_observed_current_event_id: str,
    research_observed_current_release_id: str,
    research_observed_current_pointer_revision: int,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
) -> None:
    """Cross-bind all lifecycle records and the fixed thirteen criteria."""

    if not isinstance(manifest, S75CompletionManifest):
        raise IncrementalLifecycleError("S7.5 completion manifest is invalid")
    validate_gate_b_approval(
        gate_b,
        spec=shadow_spec,
        receipt=shadow_receipt,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    validate_atomic_cutover(
        gate_c,
        top_event,
        gate_b=gate_b,
        shadow_spec=shadow_spec,
        shadow_receipt=shadow_receipt,
        shadow_event=shadow_event,
        rollback_event=rollback_event,
        rollback_receipt=rollback_receipt,
        shadow_observed_previous_event_id=shadow_observed_previous_event_id,
        shadow_observed_previous_release_id=shadow_observed_previous_release_id,
        shadow_observed_previous_pointer_revision=(shadow_observed_previous_pointer_revision),
        rollback_observed_current_event_id=rollback_observed_current_event_id,
        rollback_observed_current_release_id=rollback_observed_current_release_id,
        rollback_observed_current_pointer_revision=(rollback_observed_current_pointer_revision),
        observed_current_event_id=research_observed_current_event_id,
        observed_current_release_id=research_observed_current_release_id,
        observed_current_pointer_revision=research_observed_current_pointer_revision,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    validate_full_reconciliation(
        full_reconciliation_spec,
        full_reconciliation_receipt,
        availability_cutoff_session=availability_cutoff_session,
        artifact_reader=artifact_reader,
    )
    expected = (
        (
            manifest.shadow_equivalence_receipt_id,
            shadow_receipt.receipt_id,
            "shadow receipt",
        ),
        (manifest.gate_b_approval_id, gate_b.approval.approval_id, "Gate B approval"),
        (manifest.shadow_pointer_event_id, shadow_event.event_id, "shadow event"),
        (
            manifest.rollback_pointer_event_id,
            rollback_event.event_id,
            "rollback pointer event",
        ),
        (manifest.rollback_receipt_id, rollback_receipt.receipt_id, "rollback receipt"),
        (manifest.gate_c_approval_id, gate_c.approval.approval_id, "Gate C approval"),
        (manifest.top_pointer_event_id, top_event.event_id, "top-pointer event"),
        (
            manifest.full_reconciliation_receipt_id,
            full_reconciliation_receipt.receipt_id,
            "full reconciliation receipt",
        ),
        (manifest.final_top_release_id, top_event.new_release_id, "final top release"),
    )
    for actual, required, label in expected:
        if actual != required:
            raise IncrementalLifecycleError(f"S7.5 completion {label} differs")
    structural_relations = (
        (
            gate_b.approval.receipt_id,
            shadow_receipt.receipt_id,
            "Gate B to shadow receipt",
        ),
        (
            shadow_event.gate_b_approval_id,
            gate_b.approval.approval_id,
            "shadow event to Gate B",
        ),
        (
            shadow_event.new_release_id,
            gate_b.approval.shadow_release_id,
            "shadow event target",
        ),
        (
            rollback_receipt.shadow_pointer_event_id,
            shadow_event.event_id,
            "rollback to shadow event",
        ),
        (
            rollback_receipt.rollback_pointer_event_id,
            rollback_event.event_id,
            "rollback receipt to rollback event",
        ),
        (
            rollback_event.expected_previous_event_id,
            shadow_event.event_id,
            "rollback event to shadow event",
        ),
        (
            rollback_event.new_release_id,
            rollback_receipt.selected_parent_release_id,
            "rollback event target",
        ),
        (
            rollback_receipt.rolled_back_release_id,
            shadow_event.new_release_id,
            "rollback release",
        ),
        (
            gate_c.approval.gate_b_approval_id,
            gate_b.approval.approval_id,
            "Gate C to Gate B",
        ),
        (
            gate_c.approval.shadow_pointer_event_id,
            shadow_event.event_id,
            "Gate C to shadow event",
        ),
        (
            gate_c.approval.rollback_receipt_id,
            rollback_receipt.receipt_id,
            "Gate C to rollback",
        ),
        (
            gate_c.approval.target_release_id,
            shadow_event.new_release_id,
            "Gate C target",
        ),
        (
            top_event.gate_c_approval_id,
            gate_c.approval.approval_id,
            "top event to Gate C",
        ),
        (
            top_event.new_release_id,
            gate_c.approval.target_release_id,
            "top event target",
        ),
        (
            full_reconciliation_receipt.incremental_top_release_id,
            top_event.new_release_id,
            "I7 reconciled top",
        ),
    )
    for actual, required, label in structural_relations:
        if actual != required:
            raise IncrementalLifecycleError(f"S7.5 completion {label} differs")
    if shadow_event.gate_b_approval_artifact != gate_b.artifact:
        raise IncrementalLifecycleError("S7.5 completion shadow Gate B pin differs")
    if top_event.gate_c_approval_artifact != gate_c.artifact:
        raise IncrementalLifecycleError("S7.5 completion top Gate C pin differs")
    if rollback_receipt.selected_parent_release_id != gate_c.approval.expected_previous_release_id:
        raise IncrementalLifecycleError("S7.5 completion rollback parent differs")
    if rollback_receipt.parent_reader_before_digest != rollback_receipt.parent_reader_after_digest:
        raise IncrementalLifecycleError("S7.5 completion rollback changed the parent reader")
    if rollback_receipt.deleted_artifact_count != 0:
        raise IncrementalLifecycleError("S7.5 completion rollback deleted immutable artifacts")
    availability = (
        shadow_receipt.receipt_available_session,
        gate_b.approval.approval_available_session,
        shadow_event.event_available_session,
        rollback_event.event_available_session,
        rollback_receipt.receipt_available_session,
        gate_c.approval.approval_available_session,
        top_event.event_available_session,
        full_reconciliation_receipt.receipt_available_session,
    )
    if max(availability) > manifest.completion_available_session:
        raise IncrementalLifecycleError("S7.5 completion predates lifecycle evidence")
    cutoff = _session(availability_cutoff_session, "S7.5 completion cutoff")
    if manifest.completion_available_session > cutoff:
        raise IncrementalLifecycleError("S7.5 completion was unavailable at cutoff")
    if any(not item.passed for item in manifest.acceptance_criteria):
        raise IncrementalLifecycleError("S7.5 acceptance criterion failed")
    evidence = {
        item.criterion_number: set(item.evidence_ids) for item in manifest.acceptance_criteria
    }
    required_evidence = {
        1: {manifest.legacy_s7_release_set_id},
        2: {manifest.i2_acceptance_receipt_id, manifest.i3_acceptance_receipt_id},
        3: {manifest.i2_acceptance_receipt_id, manifest.i3_acceptance_receipt_id},
        4: {manifest.shadow_equivalence_receipt_id},
        5: {manifest.shadow_equivalence_receipt_id},
        6: {manifest.shadow_equivalence_receipt_id},
        7: {manifest.i3_acceptance_receipt_id},
        8: {manifest.i4_acceptance_receipt_id},
        9: {manifest.i3_acceptance_receipt_id, manifest.i4_acceptance_receipt_id},
        10: {
            manifest.shadow_equivalence_receipt_id,
            manifest.full_reconciliation_receipt_id,
        },
        11: {
            manifest.rollback_pointer_event_id,
            manifest.rollback_receipt_id,
        },
        12: {
            manifest.i2_acceptance_receipt_id,
            manifest.i3_acceptance_receipt_id,
            manifest.i4_acceptance_receipt_id,
        },
        13: {manifest.i3_acceptance_receipt_id},
    }
    for number, required in required_evidence.items():
        if not required.issubset(evidence[number]):
            raise IncrementalLifecycleError(
                f"S7.5 criterion {number} lacks required lifecycle evidence"
            )


def _validate_resources(policy: ResourceGatePolicy, observed: ResourceObservation) -> None:
    if observed.wall_clock_seconds > policy.max_wall_clock_seconds:
        raise IncrementalLifecycleError("shadow wall-clock resource gate failed")
    if observed.peak_rss_bytes > policy.max_peak_rss_bytes:
        raise IncrementalLifecycleError("shadow peak-RSS resource gate failed")
    if observed.free_disk_bytes_at_floor < policy.min_free_disk_bytes:
        raise IncrementalLifecycleError("shadow disk-floor resource gate failed")
    if observed.read_bytes > policy.max_read_bytes:
        raise IncrementalLifecycleError("shadow read-byte resource gate failed")
    if observed.write_bytes > policy.max_write_bytes:
        raise IncrementalLifecycleError("shadow write-byte resource gate failed")
    if observed.chain_resolution_milliseconds > policy.max_chain_resolution_milliseconds:
        raise IncrementalLifecycleError("shadow chain-resolution resource gate failed")


def _exact_projection_policies(value: object) -> tuple[ProjectionPolicy, ...]:
    if not isinstance(value, tuple) or any(
        not isinstance(item, ProjectionPolicy) for item in value
    ):
        raise IncrementalLifecycleError("projection policies must be a typed tuple")
    observed = tuple(item.projection for item in value)
    required = tuple(EquivalenceProjection)
    if observed != required:
        raise IncrementalLifecycleError("projection policies must cover the exact closed order")
    return value


def _exact_comparison_receipts(
    value: object,
) -> tuple[ProjectionComparisonReceipt, ...]:
    if not isinstance(value, tuple) or any(
        not isinstance(item, ProjectionComparisonReceipt) for item in value
    ):
        raise IncrementalLifecycleError("comparison receipts must be a typed tuple")
    if tuple(item.projection for item in value) != tuple(EquivalenceProjection):
        raise IncrementalLifecycleError("comparison receipts must cover the exact closed order")
    return value


def _exact_failure_receipts(value: object) -> tuple[FailureRecoveryReceipt, ...]:
    if not isinstance(value, tuple) or any(
        not isinstance(item, FailureRecoveryReceipt) for item in value
    ):
        raise IncrementalLifecycleError("failure receipts must be a typed tuple")
    if tuple(item.scenario for item in value) != tuple(FailureScenario):
        raise IncrementalLifecycleError("failure receipts must cover the exact closed order")
    return value


def _exact_acceptance_criteria(
    value: object,
) -> tuple[AcceptanceCriterionReceipt, ...]:
    if not isinstance(value, tuple) or any(
        not isinstance(item, AcceptanceCriterionReceipt) for item in value
    ):
        raise IncrementalLifecycleError("acceptance criteria must be a typed tuple")
    if tuple(item.criterion_number for item in value) != tuple(range(1, 14)):
        raise IncrementalLifecycleError("acceptance criteria must cover exact order 1..13")
    return value


def _pin_body(value: object, *, path: str) -> ArtifactPin:
    content = _canonical_json_bytes(value)
    return ArtifactPin(
        path=path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _verify_exact_body_pin(value: object, pin: object, label: str) -> None:
    if not isinstance(pin, ArtifactPin):
        raise IncrementalLifecycleError(f"{label} artifact pin is invalid")
    content = _canonical_json_bytes(value)
    if pin.sha256 != hashlib.sha256(content).hexdigest() or pin.bytes != len(content):
        raise IncrementalLifecycleError(f"{label} exact bytes do not reproduce")


def _read_exact_body_pin(
    value: object,
    pin: ArtifactPin,
    artifact_reader: ExactArtifactReader,
    label: str,
) -> bytes:
    _verify_exact_body_pin(value, pin, label)
    content = _read_exact_pin(pin, artifact_reader, label)
    if content != _canonical_json_bytes(value):
        raise IncrementalLifecycleError(f"{label} stored bytes differ from canonical body")
    return content


def _read_exact_pin(
    pin: ArtifactPin,
    artifact_reader: ExactArtifactReader,
    label: str,
) -> bytes:
    if not isinstance(pin, ArtifactPin):
        raise IncrementalLifecycleError(f"{label} artifact pin is invalid")
    if not callable(artifact_reader):
        raise IncrementalLifecycleError("exact artifact reader must be callable")
    content = artifact_reader(pin.path)
    if type(content) is not bytes:
        raise IncrementalLifecycleError("exact artifact reader must return bytes")
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise IncrementalLifecycleError(f"{label} stored bytes differ from exact pin")
    return content


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


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IncrementalLifecycleError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _digest(value, label)


def _digests(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not value:
        raise IncrementalLifecycleError(f"{label} must be a nonempty tuple")
    for item in value:
        _digest(item, label)
    if value != tuple(sorted(set(value))):
        raise IncrementalLifecycleError(f"{label} must be sorted and unique")
    return value


def _token(value: object, label: str) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise IncrementalLifecycleError(f"{label} must be a lowercase token")
    return value


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise IncrementalLifecycleError(f"{label} must be a date")
    return value


def _sessions(value: object, label: str) -> tuple[date, ...]:
    if not isinstance(value, tuple) or not value or any(type(item) is not date for item in value):
        raise IncrementalLifecycleError(f"{label} must be a nonempty date tuple")
    if value != tuple(sorted(set(value))):
        raise IncrementalLifecycleError(f"{label} must be sorted and unique")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IncrementalLifecycleError(f"{label} must be a positive integer")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IncrementalLifecycleError(f"{label} must be a nonnegative integer")
    return value


__all__ = [
    "ACCEPTANCE_CRITERIA",
    "GATE_B_LITERAL_VERSION",
    "GATE_C_LITERAL_VERSION",
    "I7_TABLE_ORDER",
    "LIFECYCLE_RULE_VERSION",
    "RESEARCH_POINTER_NAME",
    "SHADOW_POINTER_NAME",
    "AcceptanceCriterionReceipt",
    "EquivalenceProjection",
    "FailureRecoveryReceipt",
    "FailureScenario",
    "FullReconciliationPartitionEvidence",
    "FullReconciliationReceipt",
    "FullReconciliationSpec",
    "FullReconciliationTableEvidence",
    "FullReconciliationTableScope",
    "GateBAction",
    "GateBApproval",
    "GateCAction",
    "GateCApproval",
    "IdempotencyReceipt",
    "IncrementalLifecycleError",
    "PinnedGateBApproval",
    "PinnedGateCApproval",
    "ProjectionComparisonReceipt",
    "ProjectionPolicy",
    "ReconciliationCadence",
    "ResourceGatePolicy",
    "ResourceObservation",
    "RollbackPointerEvent",
    "RollbackReceipt",
    "S75CompletionManifest",
    "ShadowEquivalenceReceipt",
    "ShadowEquivalenceSpec",
    "ShadowPointerEvent",
    "TopPointerEvent",
    "validate_atomic_cutover",
    "validate_full_reconciliation",
    "validate_gate_b_approval",
    "validate_rollback_receipt",
    "validate_s75_completion",
    "validate_shadow_equivalence",
    "validate_shadow_pointer_event",
]
