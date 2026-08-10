"""Exact append-only pointer runtime for S7.5 I6 shadow and cutover.

This module is deliberately the only S7.5 component that may mutate a pointer.
All authorities and release inputs are exact path/SHA/byte pins.  Gate B and
Gate C approvals must already exist as immutable canonical JSON; this runtime
can request an approval but cannot create, sign, or infer one.

Pointer events and successful receipts are content-addressed immutable files.
The only mutable files are the two ``current.json`` selectors.  The production
trust boundary is a single writer running as the data-root owner, protected by
filesystem ACLs that deny group/world writes.  The advisory lock and immutable
revision claim coordinate writers following this protocol; they do not claim
to defeat an arbitrary process that can bypass those ACLs.  A selector is only
a validated ledger head, and shadow selectors are never consulted by the
research reader.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Final, Self
from uuid import uuid4

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin, input_set_digest
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionCompletion,
    I3ProductionDeepVerificationAttestation,
    I3ProductionOutputSet,
    I3ProductionRunKind,
    I3ProductionRunSpec,
    I3ProductionRunState,
    I3ProductionTableOutput,
    load_i3_production_completion_exact,
    load_i3_production_deep_attestation_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
    production_gate_a_input_pins,
    production_physical_index_digest,
    validate_production_compact_base_initial_rowsets,
    validate_production_delta_append_outputs,
)
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    RESEARCH_POINTER_NAME,
    SHADOW_POINTER_NAME,
    GateBAction,
    GateBApproval,
    GateCAction,
    GateCApproval,
    IncrementalLifecycleError,
    PinnedGateBApproval,
    PinnedGateCApproval,
    ResourceGatePolicy,
    RollbackPointerEvent,
    RollbackReceipt,
    ShadowPointerEvent,
    TopPointerEvent,
    validate_atomic_cutover,
    validate_gate_b_approval,
    validate_rollback_receipt,
    validate_shadow_pointer_event,
)
from ame_stocks_api.silver.incremental_i5_shadow_runtime import (
    I5_PRODUCTION_AUTHORITY,
    I5_SCOPE_ARTIFACT,
    I5_SHADOW_RUN_SPEC_RULE_VERSION,
    ShadowRunCompletion,
    ShadowRunSpec,
    load_i5_shadow_completion_exact,
)

I6_POINTER_RUNTIME_RULE_VERSION: Final = "s7_5_i6_exact_pointer_runtime_v1"
I6_ACTION_PACKAGE_RULE_VERSION: Final = "s7_5_i6_pointer_action_package_v1"
I6_EVENT_ENVELOPE_RULE_VERSION: Final = "s7_5_i6_pointer_event_envelope_v1"
I6_CURRENT_POINTER_RULE_VERSION: Final = "s7_5_i6_current_pointer_v1"
I6_STAGE_RECEIPT_RULE_VERSION: Final = "s7_5_i6_pointer_stage_receipt_v1"
I6_ROLLBACK_DETAILS_RULE_VERSION: Final = "s7_5_i6_rollback_details_v1"

_CONTROL_ROOT: Final = "manifests/silver/incremental/i6"
_PACKAGE_ROOT: Final = f"{_CONTROL_ROOT}/pointer-action-packages"
_EVENT_ROOT: Final = f"{_CONTROL_ROOT}/pointer-events"
_RECEIPT_ROOT: Final = f"{_CONTROL_ROOT}/pointer-stage-receipts"
_ROLLBACK_ROOT: Final = f"{_CONTROL_ROOT}/rollback-receipts"
_POINTER_ROOT: Final = f"{_CONTROL_ROOT}/pointers"
_LOCK_ROOT: Final = "locks/silver/incremental/i6/pointers"
_REVISION_ROOT: Final = f"{_CONTROL_ROOT}/pointer-revisions"
_GATE_B_ROOT: Final = "manifests/silver/incremental/i5/gate-b/approvals"
_GATE_C_ROOT: Final = f"{_CONTROL_ROOT}/gate-c/approvals"
_MAX_CONTROL_BYTES: Final = 32 * 1024 * 1024
_MIN_FREE_DISK_BYTES: Final = 64 * 1024 * 1024
_MAX_LEDGER_DEPTH: Final = 4096
_TRUSTED_APPROVER_IDS: Final = frozenset({"research_owner"})
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_FORBIDDEN_AUTHORITY_PARTS = frozenset({"latest", "tmp", ".tmp", "fixture", "fixtures"})


class I6PointerRuntimeError(RuntimeError):
    """Raised before an unsafe pointer action can affect visibility."""


class I6LostCompareAndSwap(I6PointerRuntimeError):
    """The mutable selector changed after the action package was frozen."""


class PointerAction(StrEnum):
    SHADOW_PUBLISH = "shadow_publish"
    SHADOW_ROLLBACK = "shadow_rollback"
    RESEARCH_CUTOVER = "research_cutover"


class PointerPackageState(StrEnum):
    AWAITING_APPROVAL = "awaiting_approval"
    READY = "ready"


@dataclass(frozen=True, slots=True)
class ReleaseAuthorityPin:
    """Exact I3 completion plus its already-created deep attestation."""

    completion_artifact: ArtifactPin
    deep_attestation_artifact: ArtifactPin

    def __post_init__(self) -> None:
        _artifact(self.completion_artifact, "release completion")
        _artifact(self.deep_attestation_artifact, "release deep attestation")
        _production_path(self.completion_artifact.path, "release completion")
        _production_path(self.deep_attestation_artifact.path, "release deep attestation")

    def to_dict(self) -> dict[str, object]:
        return {
            "completion_artifact": self.completion_artifact.to_dict(),
            "deep_attestation_artifact": self.deep_attestation_artifact.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _mapping(value, "release authority")
        _keys(item, {"completion_artifact", "deep_attestation_artifact"}, "release authority")
        return cls(
            completion_artifact=_artifact_from_dict(
                item["completion_artifact"], "release completion"
            ),
            deep_attestation_artifact=_artifact_from_dict(
                item["deep_attestation_artifact"], "release deep attestation"
            ),
        )


@dataclass(frozen=True, slots=True)
class ReleaseChainBinding:
    """Exact BASE and one exact DELTA; no discovery or implicit latest."""

    base: ReleaseAuthorityPin
    delta: ReleaseAuthorityPin

    def __post_init__(self) -> None:
        if not isinstance(self.base, ReleaseAuthorityPin) or not isinstance(
            self.delta, ReleaseAuthorityPin
        ):
            raise I6PointerRuntimeError("release chain requires typed authorities")
        if self.base == self.delta:
            raise I6PointerRuntimeError("BASE and DELTA authorities must differ")

    def to_dict(self) -> dict[str, object]:
        return {"base": self.base.to_dict(), "delta": self.delta.to_dict()}

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _mapping(value, "release chain")
        _keys(item, {"base", "delta"}, "release chain")
        return cls(
            base=ReleaseAuthorityPin.from_dict(item["base"]),
            delta=ReleaseAuthorityPin.from_dict(item["delta"]),
        )


@dataclass(frozen=True, slots=True)
class ResolvedRelease:
    release_id: str
    native_v2_release_id: str
    run_kind: I3ProductionRunKind
    terminal_session: date
    parent_release_id: str | None
    reader_digest: str

    def __post_init__(self) -> None:
        _digest(self.release_id, "resolved release ID")
        _digest(self.native_v2_release_id, "resolved native-v2 release ID")
        if not isinstance(self.run_kind, I3ProductionRunKind):
            raise I6PointerRuntimeError("resolved release kind is invalid")
        _session(self.terminal_session, "resolved release terminal session")
        _optional_digest(self.parent_release_id, "resolved parent release ID")
        _digest(self.reader_digest, "resolved release reader digest")

    def to_dict(self) -> dict[str, object]:
        return {
            "native_v2_release_id": self.native_v2_release_id,
            "parent_release_id": self.parent_release_id,
            "reader_digest": self.reader_digest,
            "release_id": self.release_id,
            "run_kind": self.run_kind.value,
            "terminal_session": self.terminal_session.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ResolvedReleaseChain:
    base: ResolvedRelease
    delta: ResolvedRelease

    def __post_init__(self) -> None:
        if self.base.run_kind is not I3ProductionRunKind.BASE:
            raise I6PointerRuntimeError("release chain base is not BASE")
        if self.delta.run_kind is not I3ProductionRunKind.DELTA:
            raise I6PointerRuntimeError("release chain delta is not DELTA")
        if self.delta.parent_release_id != self.base.release_id:
            raise I6PointerRuntimeError("DELTA does not name the exact BASE parent")
        if self.delta.terminal_session <= self.base.terminal_session:
            raise I6PointerRuntimeError("DELTA terminal session does not follow BASE")

    @property
    def chain_digest(self) -> str:
        return stable_digest(
            {
                "base": self.base.to_dict(),
                "delta": self.delta.to_dict(),
                "rule_version": "s7_5_i6_exact_release_chain_v1",
            }
        )


@dataclass(frozen=True, slots=True)
class CurrentExpectation:
    exists: bool
    current_sha256: str | None
    event_id: str | None
    event_artifact: ArtifactPin | None
    release_id: str | None
    pointer_revision: int
    updated_session: date | None

    def __post_init__(self) -> None:
        if type(self.exists) is not bool:
            raise I6PointerRuntimeError("current expectation exists flag is invalid")
        _optional_digest(self.current_sha256, "current selector SHA")
        _optional_digest(self.event_id, "current event ID")
        _optional_digest(self.release_id, "current release ID")
        if self.event_artifact is not None:
            _artifact(self.event_artifact, "current predecessor event")
        if self.updated_session is not None:
            _session(self.updated_session, "current predecessor availability")
        _nonnegative_int(self.pointer_revision, "current pointer revision")
        if self.exists:
            if (
                self.current_sha256 is None
                or self.event_id is None
                or self.event_artifact is None
                or self.release_id is None
                or self.updated_session is None
            ):
                raise I6PointerRuntimeError("existing selector expectation is incomplete")
            if self.pointer_revision == 0:
                raise I6PointerRuntimeError("existing selector revision must be positive")
        elif (
            any(
                item is not None
                for item in (
                    self.current_sha256,
                    self.event_id,
                    self.event_artifact,
                    self.release_id,
                    self.updated_session,
                )
            )
            or self.pointer_revision != 0
        ):
            raise I6PointerRuntimeError("missing selector expectation carries current facts")

    def to_dict(self) -> dict[str, object]:
        return {
            "current_sha256": self.current_sha256,
            "event_id": self.event_id,
            "event_artifact": (
                None if self.event_artifact is None else self.event_artifact.to_dict()
            ),
            "exists": self.exists,
            "pointer_revision": self.pointer_revision,
            "release_id": self.release_id,
            "updated_session": (
                None if self.updated_session is None else self.updated_session.isoformat()
            ),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _mapping(value, "current expectation")
        _keys(
            item,
            {
                "current_sha256",
                "event_artifact",
                "event_id",
                "exists",
                "pointer_revision",
                "release_id",
                "updated_session",
            },
            "current expectation",
        )
        return cls(
            exists=_boolean(item["exists"], "current expectation exists"),
            current_sha256=_optional_text(item["current_sha256"], "current selector SHA"),
            event_id=_optional_text(item["event_id"], "current event ID"),
            event_artifact=_optional_artifact(item["event_artifact"], "current predecessor event"),
            release_id=_optional_text(item["release_id"], "current release ID"),
            pointer_revision=_integer(item["pointer_revision"], "current revision"),
            updated_session=(
                None
                if item["updated_session"] is None
                else _date_value(item["updated_session"], "current predecessor availability")
            ),
        )


@dataclass(frozen=True, slots=True)
class CurrentPointer:
    pointer_name: str
    event_id: str
    event_artifact: ArtifactPin
    release_id: str
    pointer_revision: int
    updated_session: date

    def __post_init__(self) -> None:
        if self.pointer_name not in {SHADOW_POINTER_NAME, RESEARCH_POINTER_NAME}:
            raise I6PointerRuntimeError("current selector pointer name is invalid")
        _digest(self.event_id, "current selector event ID")
        _artifact(self.event_artifact, "current selector event artifact")
        _digest(self.release_id, "current selector release ID")
        _positive_int(self.pointer_revision, "current selector revision")
        _session(self.updated_session, "current selector availability")
        expected = _event_path(self.pointer_name, self.event_id)
        if self.event_artifact.path != expected:
            raise I6PointerRuntimeError("current selector event path is not canonical")

    @property
    def current_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i6_current_pointer",
            "event_artifact": self.event_artifact.to_dict(),
            "event_id": self.event_id,
            "pointer_name": self.pointer_name,
            "pointer_revision": self.pointer_revision,
            "release_id": self.release_id,
            "rule_version": I6_CURRENT_POINTER_RULE_VERSION,
            "updated_session": self.updated_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"current_id": self.current_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class PointerActionPackage:
    action: PointerAction
    state: PointerPackageState
    pointer_name: str
    event_available_session: date
    expected_current: CurrentExpectation
    release_chain: ReleaseChainBinding
    approval_artifact: ArtifactPin | None
    shadow_completion_artifact: ArtifactPin | None
    source_stage_receipt_artifact: ArtifactPin | None
    lifecycle_event: ShadowPointerEvent | RollbackPointerEvent | TopPointerEvent | None

    def __post_init__(self) -> None:
        if not isinstance(self.action, PointerAction) or not isinstance(
            self.state, PointerPackageState
        ):
            raise I6PointerRuntimeError("pointer package enum is invalid")
        if self.pointer_name != _pointer_for_action(self.action):
            raise I6PointerRuntimeError("pointer package targets the wrong selector")
        _session(self.event_available_session, "pointer package event availability")
        if not isinstance(self.expected_current, CurrentExpectation) or not isinstance(
            self.release_chain, ReleaseChainBinding
        ):
            raise I6PointerRuntimeError("pointer package bindings are invalid")
        if self.expected_current.exists and (
            _required(self.expected_current.event_artifact, "package predecessor").path
            != _event_path(
                self.pointer_name,
                _required(self.expected_current.event_id, "package predecessor ID"),
            )
        ):
            raise I6PointerRuntimeError("pointer package predecessor path differs")
        for pin, label in (
            (self.approval_artifact, "pointer approval"),
            (self.shadow_completion_artifact, "shadow completion"),
            (self.source_stage_receipt_artifact, "source stage receipt"),
        ):
            if pin is not None:
                _artifact(pin, label)
                _production_path(pin.path, label)
        if self.state is PointerPackageState.AWAITING_APPROVAL:
            if self.action is PointerAction.SHADOW_ROLLBACK:
                raise I6PointerRuntimeError("rollback does not require an approval")
            if self.approval_artifact is not None or self.lifecycle_event is not None:
                raise I6PointerRuntimeError("awaiting package cannot carry approval authority")
        elif self.lifecycle_event is None:
            raise I6PointerRuntimeError("ready pointer package omits its lifecycle event")
        if self.action is PointerAction.SHADOW_PUBLISH:
            if self.shadow_completion_artifact is None or self.source_stage_receipt_artifact:
                raise I6PointerRuntimeError("shadow-publish package authority differs")
            if self.state is PointerPackageState.READY and (
                self.approval_artifact is None
                or type(self.lifecycle_event) is not ShadowPointerEvent
            ):
                raise I6PointerRuntimeError("ready shadow publication is incomplete")
        elif self.action is PointerAction.SHADOW_ROLLBACK:
            if (
                self.approval_artifact is not None
                or self.shadow_completion_artifact is not None
                or self.source_stage_receipt_artifact is None
                or type(self.lifecycle_event) is not RollbackPointerEvent
            ):
                raise I6PointerRuntimeError("rollback package authority differs")
        elif self.source_stage_receipt_artifact is None:
            raise I6PointerRuntimeError("research cutover lacks rollback evidence")
        elif self.state is PointerPackageState.READY and (
            self.approval_artifact is None or type(self.lifecycle_event) is not TopPointerEvent
        ):
            raise I6PointerRuntimeError("ready research cutover is incomplete")

    @property
    def package_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "action": self.action.value,
            "approval_artifact": (
                None if self.approval_artifact is None else self.approval_artifact.to_dict()
            ),
            "artifact_type": "s7_5_i6_pointer_action_package",
            "event_available_session": self.event_available_session.isoformat(),
            "expected_current": self.expected_current.to_dict(),
            "lifecycle_event": (
                None if self.lifecycle_event is None else self.lifecycle_event.to_dict()
            ),
            "pointer_name": self.pointer_name,
            "release_chain": self.release_chain.to_dict(),
            "rule_version": I6_ACTION_PACKAGE_RULE_VERSION,
            "shadow_completion_artifact": (
                None
                if self.shadow_completion_artifact is None
                else self.shadow_completion_artifact.to_dict()
            ),
            "source_stage_receipt_artifact": (
                None
                if self.source_stage_receipt_artifact is None
                else self.source_stage_receipt_artifact.to_dict()
            ),
            "state": self.state.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"package_id": self.package_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class PointerStageReceipt:
    action: PointerAction
    package_id: str
    package_artifact: ArtifactPin
    event_id: str
    event_artifact: ArtifactPin
    selected_release_id: str
    pointer_revision: int
    pointer_current_id: str
    stage_available_session: date
    rollback_receipt_artifact: ArtifactPin | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.action, PointerAction):
            raise I6PointerRuntimeError("pointer stage action is invalid")
        for value, label in (
            (self.package_id, "stage package ID"),
            (self.event_id, "stage event ID"),
            (self.selected_release_id, "stage release ID"),
            (self.pointer_current_id, "stage current ID"),
        ):
            _digest(value, label)
        _artifact(self.package_artifact, "stage package artifact")
        _artifact(self.event_artifact, "stage event artifact")
        _positive_int(self.pointer_revision, "stage pointer revision")
        _session(self.stage_available_session, "stage receipt availability")
        if self.event_artifact.path != _event_path(_pointer_for_action(self.action), self.event_id):
            raise I6PointerRuntimeError("stage event artifact path is not canonical")
        if self.rollback_receipt_artifact is not None:
            _artifact(self.rollback_receipt_artifact, "rollback lifecycle receipt")
        if (self.action is PointerAction.SHADOW_ROLLBACK) != (
            self.rollback_receipt_artifact is not None
        ):
            raise I6PointerRuntimeError("rollback lifecycle receipt presence differs")

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "action": self.action.value,
            "artifact_type": "s7_5_i6_pointer_stage_receipt",
            "event_artifact": self.event_artifact.to_dict(),
            "event_id": self.event_id,
            "package_artifact": self.package_artifact.to_dict(),
            "package_id": self.package_id,
            "pointer_current_id": self.pointer_current_id,
            "pointer_revision": self.pointer_revision,
            "rollback_receipt_artifact": (
                None
                if self.rollback_receipt_artifact is None
                else self.rollback_receipt_artifact.to_dict()
            ),
            "rule_version": I6_STAGE_RECEIPT_RULE_VERSION,
            "selected_release_id": self.selected_release_id,
            "stage_available_session": self.stage_available_session.isoformat(),
            "state": "succeeded",
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class ResolvedPointerView:
    pointer_name: str
    event_id: str
    release_id: str
    pointer_revision: int
    release: ResolvedRelease
    chain_digest: str


@dataclass(frozen=True, slots=True)
class ResearchTopSnapshot:
    """Fully resolved Gate-C research authority for I7 reconciliation.

    ``table_outputs`` are the exact typed I3 rowset/dataset indexes.  Their
    segment/partition pins plus the checkpoint and deep physical-index digest
    are sufficient for I7 to open only explicitly named members.
    """

    pointer_current_id: str
    pointer_revision: int
    research_top_event_artifact: ArtifactPin
    research_top_event: TopPointerEvent
    gate_c_approval_artifact: ArtifactPin
    gate_c_approval: GateCApproval
    gate_b_approval_artifact: ArtifactPin
    gate_b_approval: GateBApproval
    shadow_stage_receipt_artifact: ArtifactPin
    shadow_stage_receipt: PointerStageReceipt
    shadow_pointer_event_artifact: ArtifactPin
    shadow_pointer_event: ShadowPointerEvent
    rollback_stage_receipt_artifact: ArtifactPin
    rollback_stage_receipt: PointerStageReceipt
    rollback_pointer_event_artifact: ArtifactPin
    rollback_pointer_event: RollbackPointerEvent
    rollback_receipt_artifact: ArtifactPin
    rollback_receipt: RollbackReceipt
    research_top_stage_receipt_artifact: ArtifactPin
    research_top_stage_receipt: PointerStageReceipt
    release_completion_artifact: ArtifactPin
    deep_attestation_artifact: ArtifactPin
    release_id: str
    native_v2_release_id: str
    terminal_session: date
    source_cutoff_session: date
    release_available_session: date
    completion_available_session: date
    deep_attestation_available_session: date
    producer_available_session: date
    source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    checkpoint_id: str
    checkpoint_artifact: ArtifactPin
    resolved_state_digest: str
    resolved_content_digest: str
    physical_index_digest: str
    row_semantic_attestation_digest: str
    table_outputs: tuple[I3ProductionTableOutput, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.pointer_current_id, "snapshot current ID"),
            (self.release_id, "snapshot release ID"),
            (self.native_v2_release_id, "snapshot native-v2 release ID"),
            (self.source_binding_digest, "snapshot source binding"),
            (self.schema_bundle_digest, "snapshot schema bundle"),
            (self.transform_semantics_digest, "snapshot transform semantics"),
            (self.identity_policy_bundle_id, "snapshot identity policy"),
            (self.calendar_digest, "snapshot calendar"),
            (self.checkpoint_id, "snapshot checkpoint ID"),
            (self.resolved_state_digest, "snapshot resolved-state digest"),
            (self.resolved_content_digest, "snapshot resolved-content digest"),
            (self.physical_index_digest, "snapshot physical-index digest"),
            (
                self.row_semantic_attestation_digest,
                "snapshot row-semantic attestation",
            ),
        ):
            _digest(value, label)
        _positive_int(self.pointer_revision, "snapshot pointer revision")
        for value, label in (
            (self.terminal_session, "snapshot terminal session"),
            (self.source_cutoff_session, "snapshot source cutoff"),
            (self.release_available_session, "snapshot release availability"),
            (self.completion_available_session, "snapshot completion availability"),
            (
                self.deep_attestation_available_session,
                "snapshot deep-attestation availability",
            ),
            (self.producer_available_session, "snapshot producer availability"),
        ):
            _session(value, label)
        expected_producer_available_session = max(
            self.release_available_session,
            self.completion_available_session,
            self.deep_attestation_available_session,
            self.research_top_event.event_available_session,
            self.gate_c_approval.approval_available_session,
            self.gate_b_approval.approval_available_session,
            self.shadow_pointer_event.event_available_session,
            self.rollback_pointer_event.event_available_session,
            self.rollback_receipt.receipt_available_session,
            self.shadow_stage_receipt.stage_available_session,
            self.rollback_stage_receipt.stage_available_session,
            self.research_top_stage_receipt.stage_available_session,
        )
        if self.producer_available_session != expected_producer_available_session:
            raise I6PointerRuntimeError(
                "snapshot producer availability is not the exact authority maximum"
            )
        if not (
            self.terminal_session
            <= self.source_cutoff_session
            <= self.release_available_session
            <= self.completion_available_session
            <= self.deep_attestation_available_session
            <= self.producer_available_session
        ):
            raise I6PointerRuntimeError("snapshot producer sessions are not ordered")
        if not (
            self.gate_b_approval.approval_available_session
            <= self.shadow_pointer_event.event_available_session
            <= self.rollback_pointer_event.event_available_session
            <= self.rollback_receipt.receipt_available_session
            <= self.research_top_event.event_available_session
            <= self.producer_available_session
        ):
            raise I6PointerRuntimeError("snapshot pointer authority sessions are not ordered")
        if not (
            self.gate_c_approval.approval_available_session
            <= self.research_top_event.event_available_session
        ):
            raise I6PointerRuntimeError("snapshot Gate C postdates the research-top event")
        for pin, label in (
            (self.research_top_event_artifact, "snapshot top event"),
            (self.gate_c_approval_artifact, "snapshot Gate C"),
            (self.gate_b_approval_artifact, "snapshot Gate B"),
            (self.shadow_stage_receipt_artifact, "snapshot shadow stage receipt"),
            (self.shadow_pointer_event_artifact, "snapshot shadow event"),
            (self.rollback_stage_receipt_artifact, "snapshot rollback stage receipt"),
            (self.rollback_pointer_event_artifact, "snapshot rollback event"),
            (self.rollback_receipt_artifact, "snapshot rollback receipt"),
            (
                self.research_top_stage_receipt_artifact,
                "snapshot research-top stage receipt",
            ),
            (self.release_completion_artifact, "snapshot release completion"),
            (self.deep_attestation_artifact, "snapshot deep attestation"),
            (self.checkpoint_artifact, "snapshot checkpoint"),
        ):
            _artifact(pin, label)
        if not isinstance(self.research_top_event, TopPointerEvent):
            raise I6PointerRuntimeError("snapshot top event is invalid")
        if not isinstance(self.shadow_pointer_event, ShadowPointerEvent) or not isinstance(
            self.rollback_pointer_event, RollbackPointerEvent
        ):
            raise I6PointerRuntimeError("snapshot shadow/rollback event is invalid")
        if not isinstance(self.gate_c_approval, GateCApproval) or not isinstance(
            self.gate_b_approval, GateBApproval
        ):
            raise I6PointerRuntimeError("snapshot approval bodies are invalid")
        if not isinstance(self.rollback_receipt, RollbackReceipt):
            raise I6PointerRuntimeError("snapshot rollback receipt is invalid")
        if not all(
            isinstance(value, PointerStageReceipt)
            for value in (
                self.shadow_stage_receipt,
                self.rollback_stage_receipt,
                self.research_top_stage_receipt,
            )
        ):
            raise I6PointerRuntimeError("snapshot stage receipt is invalid")
        if (
            self.shadow_pointer_event.gate_b_approval_id != self.gate_b_approval.approval_id
            or self.shadow_pointer_event.gate_b_approval_artifact != self.gate_b_approval_artifact
            or self.rollback_pointer_event.forward_shadow_event_id
            != self.shadow_pointer_event.event_id
            or self.rollback_receipt.shadow_pointer_event_id != self.shadow_pointer_event.event_id
            or self.rollback_receipt.rollback_pointer_event_id
            != self.rollback_pointer_event.event_id
            or self.research_top_event.gate_c_approval_id != self.gate_c_approval.approval_id
            or self.research_top_event.gate_c_approval_artifact != self.gate_c_approval_artifact
            or self.gate_c_approval.gate_b_approval_id != self.gate_b_approval.approval_id
            or self.gate_c_approval.shadow_pointer_event_id != self.shadow_pointer_event.event_id
            or self.gate_c_approval.rollback_receipt_id != self.rollback_receipt.receipt_id
            or self.gate_c_approval.target_release_id != self.release_id
            or self.research_top_event.new_release_id != self.release_id
            or self.shadow_stage_receipt.event_id != self.shadow_pointer_event.event_id
            or self.rollback_stage_receipt.event_id != self.rollback_pointer_event.event_id
            or self.research_top_stage_receipt.event_id != self.research_top_event.event_id
        ):
            raise I6PointerRuntimeError("snapshot lifecycle authority does not close")
        if (
            type(self.table_outputs) is not tuple
            or tuple(item.table_name for item in self.table_outputs) != I3_V2_TABLE_ORDER
            or not all(type(item) is I3ProductionTableOutput for item in self.table_outputs)
        ):
            raise I6PointerRuntimeError("snapshot does not contain the exact four-table order")
        if self.schema_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I6PointerRuntimeError("snapshot schema bundle is not native-v2")

    @property
    def snapshot_id(self) -> str:
        return stable_digest(
            {
                "calendar_digest": self.calendar_digest,
                "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
                "checkpoint_id": self.checkpoint_id,
                "deep_attestation_artifact": self.deep_attestation_artifact.to_dict(),
                "gate_b_approval_artifact": self.gate_b_approval_artifact.to_dict(),
                "gate_c_approval_artifact": self.gate_c_approval_artifact.to_dict(),
                "completion_available_session": self.completion_available_session.isoformat(),
                "identity_policy_bundle_id": self.identity_policy_bundle_id,
                "physical_index_digest": self.physical_index_digest,
                "pointer_current_id": self.pointer_current_id,
                "release_id": self.release_id,
                "producer_available_session": self.producer_available_session.isoformat(),
                "deep_attestation_available_session": (
                    self.deep_attestation_available_session.isoformat()
                ),
                "research_top_event_artifact": (self.research_top_event_artifact.to_dict()),
                "resolved_content_digest": self.resolved_content_digest,
                "resolved_state_digest": self.resolved_state_digest,
                "rollback_receipt_artifact": self.rollback_receipt_artifact.to_dict(),
                "rollback_pointer_event_artifact": (self.rollback_pointer_event_artifact.to_dict()),
                "rollback_stage_receipt_artifact": (self.rollback_stage_receipt_artifact.to_dict()),
                "research_top_stage_receipt_artifact": (
                    self.research_top_stage_receipt_artifact.to_dict()
                ),
                "row_semantic_attestation_digest": (self.row_semantic_attestation_digest),
                "rule_version": "s7_5_i6_resolved_research_top_snapshot_v1",
                "schema_bundle_digest": self.schema_bundle_digest,
                "shadow_pointer_event_artifact": (self.shadow_pointer_event_artifact.to_dict()),
                "shadow_stage_receipt_artifact": (self.shadow_stage_receipt_artifact.to_dict()),
                "shadow_pointer_event_id": self.shadow_pointer_event.event_id,
                "rollback_pointer_event_id": self.rollback_pointer_event.event_id,
                "source_binding_digest": self.source_binding_digest,
                "source_cutoff_session": self.source_cutoff_session.isoformat(),
                "table_outputs": [item.to_dict() for item in self.table_outputs],
                "terminal_session": self.terminal_session.isoformat(),
                "transform_semantics_digest": self.transform_semantics_digest,
            }
        )


@dataclass(frozen=True, slots=True)
class _LoadedReleaseAuthority:
    resolved: ResolvedRelease
    completion: I3ProductionCompletion
    run_spec: I3ProductionRunSpec
    output_set: I3ProductionOutputSet
    deep: I3ProductionDeepVerificationAttestation


@dataclass(frozen=True, slots=True)
class ResearchParentAnchor:
    """Exact immutable import of the research selector that predates I6.

    The production initializer may freeze the already-published research
    parent once, after which Gate C binds its exact event/release/revision.
    Keeping this parser here lets failed cutovers read the old parent without
    granting shadow output research visibility.
    """

    event_id: str
    release_chain: ReleaseChainBinding
    selected_release_id: str
    pointer_revision: int
    available_session: date
    source_publication_artifact: ArtifactPin

    def __post_init__(self) -> None:
        _digest(self.event_id, "research-parent anchor event ID")
        if not isinstance(self.release_chain, ReleaseChainBinding):
            raise I6PointerRuntimeError("research-parent anchor release chain is invalid")
        _digest(self.selected_release_id, "research-parent anchor release ID")
        _positive_int(self.pointer_revision, "research-parent anchor revision")
        _session(self.available_session, "research-parent anchor availability")
        _artifact(self.source_publication_artifact, "research-parent source publication")

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i6_imported_research_parent",
            "available_session": self.available_session.isoformat(),
            "pointer_name": RESEARCH_POINTER_NAME,
            "pointer_revision": self.pointer_revision,
            "release_chain": self.release_chain.to_dict(),
            "rule_version": "s7_5_i6_imported_research_parent_v1",
            "selected_release_id": self.selected_release_id,
            "source_publication_artifact": self.source_publication_artifact.to_dict(),
        }

    @property
    def reproduced_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    def to_dict(self) -> dict[str, object]:
        return {"event_id": self.event_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class _ShadowAuthority:
    spec: ShadowRunSpec
    completion: ShadowRunCompletion
    gate_b: PinnedGateBApproval


@dataclass(frozen=True, slots=True)
class _EventEnvelope:
    action: PointerAction
    event: ShadowPointerEvent | RollbackPointerEvent | TopPointerEvent
    release_chain: ReleaseChainBinding
    package_artifact: ArtifactPin
    predecessor_event_artifact: ArtifactPin | None
    approval_artifact: ArtifactPin | None
    source_event_artifact: ArtifactPin | None
    rollback_receipt_artifact: ArtifactPin | None

    def __post_init__(self) -> None:
        if not isinstance(self.action, PointerAction):
            raise I6PointerRuntimeError("event-envelope action is invalid")
        expected_type = {
            PointerAction.SHADOW_PUBLISH: ShadowPointerEvent,
            PointerAction.SHADOW_ROLLBACK: RollbackPointerEvent,
            PointerAction.RESEARCH_CUTOVER: TopPointerEvent,
        }[self.action]
        if type(self.event) is not expected_type:
            raise I6PointerRuntimeError("event-envelope lifecycle type differs")
        if not isinstance(self.release_chain, ReleaseChainBinding):
            raise I6PointerRuntimeError("event-envelope release chain is invalid")
        _artifact(self.package_artifact, "event-envelope package")
        for pin, label in (
            (self.approval_artifact, "event-envelope approval"),
            (self.predecessor_event_artifact, "event-envelope predecessor"),
            (self.source_event_artifact, "event-envelope source event"),
            (self.rollback_receipt_artifact, "event-envelope rollback receipt"),
        ):
            if pin is not None:
                _artifact(pin, label)
        previous_event_id = self.event.expected_previous_event_id
        if (previous_event_id is None) != (self.predecessor_event_artifact is None):
            raise I6PointerRuntimeError("event-envelope predecessor presence differs")
        if self.predecessor_event_artifact is not None and self.predecessor_event_artifact.path != (
            _event_path(
                _pointer_for_action(self.action),
                _required(previous_event_id, "prior event"),
            )
        ):
            raise I6PointerRuntimeError("event-envelope predecessor path differs")
        if self.action is PointerAction.SHADOW_PUBLISH:
            if (
                self.approval_artifact is None
                or self.source_event_artifact is not None
                or self.rollback_receipt_artifact is not None
            ):
                raise I6PointerRuntimeError("shadow event-envelope authority differs")
        elif self.action is PointerAction.SHADOW_ROLLBACK:
            if (
                self.approval_artifact is not None
                or self.source_event_artifact is None
                or self.rollback_receipt_artifact is None
            ):
                raise I6PointerRuntimeError("rollback event-envelope authority differs")
        elif (
            self.approval_artifact is None
            or self.source_event_artifact is None
            or self.rollback_receipt_artifact is not None
        ):
            raise I6PointerRuntimeError("cutover event-envelope authority differs")

    @property
    def event_id(self) -> str:
        return self.event.event_id

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action.value,
            "approval_artifact": (
                None if self.approval_artifact is None else self.approval_artifact.to_dict()
            ),
            "artifact_type": "s7_5_i6_pointer_event_envelope",
            "event": self.event.to_dict(),
            "event_id": self.event_id,
            "package_artifact": self.package_artifact.to_dict(),
            "predecessor_event_artifact": (
                None
                if self.predecessor_event_artifact is None
                else self.predecessor_event_artifact.to_dict()
            ),
            "pointer_name": _pointer_for_action(self.action),
            "release_chain": self.release_chain.to_dict(),
            "rollback_receipt_artifact": (
                None
                if self.rollback_receipt_artifact is None
                else self.rollback_receipt_artifact.to_dict()
            ),
            "rule_version": I6_EVENT_ENVELOPE_RULE_VERSION,
            "source_event_artifact": (
                None if self.source_event_artifact is None else self.source_event_artifact.to_dict()
            ),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class _LedgerNode:
    event_id: str
    event_artifact: ArtifactPin
    release_id: str
    pointer_revision: int
    available_session: date
    predecessor_event_artifact: ArtifactPin | None
    envelope: _EventEnvelope | None


def prepare_shadow_publish(
    data_root: Path,
    *,
    shadow_completion_artifact: ArtifactPin,
    gate_b_approval_artifact: ArtifactPin | None,
    release_chain: ReleaseChainBinding,
    event_available_session: date,
) -> ArtifactPin:
    """Freeze a first/later shadow CAS, or an approval request with no event."""

    root = _root(data_root)
    chain = _load_release_chain_exact(root, release_chain)
    shadow = _load_shadow_authority(
        root,
        completion_pin=shadow_completion_artifact,
        approval_pin=gate_b_approval_artifact,
        availability_cutoff_session=event_available_session,
    )
    if shadow is not None and shadow.gate_b.approval.shadow_release_id != chain.delta.release_id:
        raise I6PointerRuntimeError("Gate B shadow release differs from exact DELTA")
    if shadow is not None:
        _validate_shadow_release_binding(
            shadow=shadow,
            release_chain=release_chain,
            resolved_chain=chain,
        )
    current = _read_current_optional(root, SHADOW_POINTER_NAME)
    if current is not None:
        _replay_pointer_ledger_exact(root, current)
    expectation = _expectation(current)
    if gate_b_approval_artifact is None:
        state = PointerPackageState.AWAITING_APPROVAL
        event = None
    else:
        if shadow is None:  # pragma: no cover - loader invariant
            raise I6PointerRuntimeError("Gate B authority disappeared")
        state = PointerPackageState.READY
        event = ShadowPointerEvent(
            gate_b_approval_id=shadow.gate_b.approval.approval_id,
            gate_b_approval_artifact=shadow.gate_b.artifact,
            expected_previous_event_id=expectation.event_id,
            previous_release_id=expectation.release_id,
            new_release_id=chain.delta.release_id,
            pointer_revision=expectation.pointer_revision + 1,
            event_available_session=event_available_session,
        )
        _validate_shadow_event(root, event, shadow=shadow, expectation=expectation)
    package = PointerActionPackage(
        action=PointerAction.SHADOW_PUBLISH,
        state=state,
        pointer_name=SHADOW_POINTER_NAME,
        event_available_session=event_available_session,
        expected_current=expectation,
        release_chain=release_chain,
        approval_artifact=gate_b_approval_artifact,
        shadow_completion_artifact=shadow_completion_artifact,
        source_stage_receipt_artifact=None,
        lifecycle_event=event,
    )
    return _write_package(root, package)


def stage_shadow_publish(data_root: Path, package_artifact: ArtifactPin) -> ArtifactPin:
    package = _load_package_exact(_root(data_root), package_artifact)
    if package.action is not PointerAction.SHADOW_PUBLISH:
        raise I6PointerRuntimeError("package is not a shadow publication")
    return _stage_ready_package(_root(data_root), package_artifact, package)


def verify_shadow_publish(
    data_root: Path, stage_receipt_artifact: ArtifactPin
) -> ResolvedPointerView:
    return _verify_stage_current(
        _root(data_root),
        stage_receipt_artifact,
        expected_action=PointerAction.SHADOW_PUBLISH,
    )


def prepare_shadow_rollback(
    data_root: Path,
    *,
    shadow_publish_receipt_artifact: ArtifactPin,
    event_available_session: date,
) -> ArtifactPin:
    root = _root(data_root)
    forward_receipt = _load_stage_receipt_exact(root, shadow_publish_receipt_artifact)
    if forward_receipt.action is not PointerAction.SHADOW_PUBLISH:
        raise I6PointerRuntimeError("rollback source is not a shadow publication")
    forward_package = _load_package_exact(root, forward_receipt.package_artifact)
    forward_envelope = _load_event_envelope_exact(root, forward_receipt.event_artifact)
    if (
        forward_package.action is not PointerAction.SHADOW_PUBLISH
        or forward_envelope.action is not PointerAction.SHADOW_PUBLISH
        or forward_envelope.event_id != forward_receipt.event_id
        or forward_package.release_chain != forward_envelope.release_chain
    ):
        raise I6PointerRuntimeError("rollback source chain differs")
    chain = _load_release_chain_exact(root, forward_package.release_chain)
    current = _read_current_required(root, SHADOW_POINTER_NAME)
    _replay_pointer_ledger_exact(root, current)
    if current.event_id != forward_receipt.event_id:
        raise I6LostCompareAndSwap("shadow pointer moved before rollback preparation")
    event = RollbackPointerEvent(
        forward_shadow_event_id=forward_receipt.event_id,
        expected_previous_event_id=current.event_id,
        previous_release_id=chain.delta.release_id,
        new_release_id=chain.base.release_id,
        pointer_revision=current.pointer_revision + 1,
        event_available_session=event_available_session,
    )
    package = PointerActionPackage(
        action=PointerAction.SHADOW_ROLLBACK,
        state=PointerPackageState.READY,
        pointer_name=SHADOW_POINTER_NAME,
        event_available_session=event_available_session,
        expected_current=_expectation(current),
        release_chain=forward_package.release_chain,
        approval_artifact=None,
        shadow_completion_artifact=None,
        source_stage_receipt_artifact=shadow_publish_receipt_artifact,
        lifecycle_event=event,
    )
    return _write_package(root, package)


def stage_shadow_rollback(data_root: Path, package_artifact: ArtifactPin) -> ArtifactPin:
    root = _root(data_root)
    package = _load_package_exact(root, package_artifact)
    if package.action is not PointerAction.SHADOW_ROLLBACK:
        raise I6PointerRuntimeError("package is not a shadow rollback")
    return _stage_ready_package(root, package_artifact, package)


def verify_shadow_rollback(
    data_root: Path, stage_receipt_artifact: ArtifactPin
) -> ResolvedPointerView:
    return _verify_stage_current(
        _root(data_root),
        stage_receipt_artifact,
        expected_action=PointerAction.SHADOW_ROLLBACK,
    )


def prepare_research_cutover(
    data_root: Path,
    *,
    shadow_rollback_receipt_artifact: ArtifactPin,
    gate_c_approval_artifact: ArtifactPin | None,
    event_available_session: date,
) -> ArtifactPin:
    """Freeze a Gate-C research CAS, or a Gate-C request with no event."""

    root = _root(data_root)
    rollback_stage = _load_stage_receipt_exact(root, shadow_rollback_receipt_artifact)
    if (
        rollback_stage.action is not PointerAction.SHADOW_ROLLBACK
        or rollback_stage.rollback_receipt_artifact is None
    ):
        raise I6PointerRuntimeError("cutover source is not a completed rollback")
    rollback_package = _load_package_exact(root, rollback_stage.package_artifact)
    chain = _load_release_chain_exact(root, rollback_package.release_chain)
    current = _read_current_required(root, RESEARCH_POINTER_NAME)
    _replay_pointer_ledger_exact(root, current)
    expectation = _expectation(current)
    if current.release_id != chain.base.release_id:
        raise I6PointerRuntimeError("research top no longer selects the approved BASE parent")
    if gate_c_approval_artifact is None:
        event = None
        state = PointerPackageState.AWAITING_APPROVAL
    else:
        gate_c = _load_gate_c_approval_exact(root, gate_c_approval_artifact)
        event = TopPointerEvent(
            gate_c_approval_id=gate_c.approval.approval_id,
            gate_c_approval_artifact=gate_c.artifact,
            expected_previous_event_id=current.event_id,
            previous_release_id=current.release_id,
            new_release_id=chain.delta.release_id,
            pointer_revision=current.pointer_revision + 1,
            event_available_session=event_available_session,
        )
        _validate_cutover_event(
            root,
            gate_c=gate_c,
            event=event,
            rollback_stage=rollback_stage,
            rollback_package=rollback_package,
            resolved_chain=chain,
            research_expectation=expectation,
            cutoff=event_available_session,
        )
        state = PointerPackageState.READY
    package = PointerActionPackage(
        action=PointerAction.RESEARCH_CUTOVER,
        state=state,
        pointer_name=RESEARCH_POINTER_NAME,
        event_available_session=event_available_session,
        expected_current=expectation,
        release_chain=rollback_package.release_chain,
        approval_artifact=gate_c_approval_artifact,
        shadow_completion_artifact=None,
        source_stage_receipt_artifact=shadow_rollback_receipt_artifact,
        lifecycle_event=event,
    )
    return _write_package(root, package)


def stage_research_cutover(data_root: Path, package_artifact: ArtifactPin) -> ArtifactPin:
    root = _root(data_root)
    package = _load_package_exact(root, package_artifact)
    if package.action is not PointerAction.RESEARCH_CUTOVER:
        raise I6PointerRuntimeError("package is not a research cutover")
    return _stage_ready_package(root, package_artifact, package)


def verify_research_cutover(
    data_root: Path, stage_receipt_artifact: ArtifactPin
) -> ResolvedPointerView:
    return _verify_stage_current(
        _root(data_root),
        stage_receipt_artifact,
        expected_action=PointerAction.RESEARCH_CUTOVER,
    )


def read_shadow_pointer(data_root: Path) -> ResolvedPointerView:
    return _read_visible_pointer(_root(data_root), SHADOW_POINTER_NAME)


def read_research_pointer(data_root: Path) -> ResolvedPointerView:
    """Resolve only the research selector; shadow/pending output is invisible."""

    return _read_visible_pointer(_root(data_root), RESEARCH_POINTER_NAME)


def initialize_research_parent(
    data_root: Path,
    *,
    release_chain: ReleaseChainBinding,
    source_publication_artifact: ArtifactPin,
) -> ResolvedPointerView:
    """Import the exact published BASE as the one immutable research root.

    This can only create revision one and can only select the BASE in a fully
    replayed BASE-to-DELTA chain.  A different existing selector is never
    replaced or adopted implicitly.
    """

    root = _root(data_root)
    chain = _load_release_chain_exact(root, release_chain)
    base = _load_release_authority_exact(root, release_chain.base)
    if base.resolved != chain.base or base.run_spec.run_kind is not I3ProductionRunKind.BASE:
        raise I6PointerRuntimeError("research-parent authority is not the exact BASE")
    if source_publication_artifact != base.run_spec.i0_oracle.artifact:
        raise I6PointerRuntimeError("research-parent source publication differs from BASE")
    _ExactReader(root).read_pin(
        source_publication_artifact,
        label="research-parent source publication",
    )
    available = max(
        base.run_spec.i0_oracle.available_session,
        base.run_spec.run_available_session,
        base.completion.completion_available_session,
        base.deep.attestation_available_session,
    )
    provisional = ResearchParentAnchor(
        event_id="0" * 64,
        release_chain=release_chain,
        selected_release_id=chain.base.release_id,
        pointer_revision=1,
        available_session=available,
        source_publication_artifact=source_publication_artifact,
    )
    anchor = ResearchParentAnchor(
        event_id=provisional.reproduced_event_id,
        release_chain=release_chain,
        selected_release_id=chain.base.release_id,
        pointer_revision=1,
        available_session=available,
        source_publication_artifact=source_publication_artifact,
    )
    event_relative = _event_path(RESEARCH_POINTER_NAME, anchor.event_id)

    with _pointer_lock(root, RESEARCH_POINTER_NAME):
        current = _read_current_optional(root, RESEARCH_POINTER_NAME)
        if current is not None:
            if (
                current.event_id != anchor.event_id
                or current.event_artifact.path != event_relative
                or current.release_id != chain.base.release_id
                or current.pointer_revision != 1
                or current.updated_session != available
            ):
                raise I6PointerRuntimeError("research selector already has another authority")
            return _read_visible_pointer(root, RESEARCH_POINTER_NAME)

        event_pin = _write_immutable(
            root,
            event_relative,
            anchor.canonical_bytes(),
            label="research-parent anchor",
        )
        replacement = CurrentPointer(
            pointer_name=RESEARCH_POINTER_NAME,
            event_id=anchor.event_id,
            event_artifact=event_pin,
            release_id=chain.base.release_id,
            pointer_revision=1,
            updated_session=available,
        )
        _atomic_compare_and_swap_current(
            root,
            pointer=RESEARCH_POINTER_NAME,
            expected=_expectation(None),
            replacement=replacement,
        )
        return _read_visible_pointer(root, RESEARCH_POINTER_NAME)


def load_research_top_snapshot_exact(data_root: Path) -> ResearchTopSnapshot:
    """Replay Gate C through exact I3 target controls and return an I7 snapshot."""

    root = _root(data_root)
    current = _read_current_required(root, RESEARCH_POINTER_NAME)
    _replay_pointer_ledger_exact(root, current)
    envelope = _load_event_envelope_exact(root, current.event_artifact)
    if envelope.action is not PointerAction.RESEARCH_CUTOVER:
        raise I6PointerRuntimeError("research top has not completed an exact Gate-C cutover")
    chain = _load_release_chain_exact(root, envelope.release_chain)
    _validate_event_envelope_authority(root, envelope=envelope, chain=chain)
    if current.release_id != chain.delta.release_id:
        raise I6PointerRuntimeError("research top does not select the Gate-C DELTA")
    package = _load_package_exact(root, envelope.package_artifact)
    if package.approval_artifact is None or package.source_stage_receipt_artifact is None:
        raise I6PointerRuntimeError("research top package lost Gate-C authority")
    gate_c = _load_gate_c_approval_exact(root, package.approval_artifact)
    top_stage_pin = _pin_existing(
        root,
        _receipt_path(PointerAction.RESEARCH_CUTOVER, package.package_id),
        "research-top stage receipt",
    )
    top_stage = _verify_stage_receipt_exact(
        root,
        top_stage_pin,
        package=package,
        require_current=True,
    )
    rollback_stage = _load_stage_receipt_exact(root, package.source_stage_receipt_artifact)
    if rollback_stage.rollback_receipt_artifact is None:
        raise I6PointerRuntimeError("research top package lost rollback evidence")
    rollback = _load_rollback_receipt_exact(root, rollback_stage.rollback_receipt_artifact)
    rollback_package = _load_package_exact(root, rollback_stage.package_artifact)
    if rollback_package.source_stage_receipt_artifact is None:
        raise I6PointerRuntimeError("rollback package lost its shadow stage receipt")
    forward_stage, forward_package, _forward_envelope = _load_forward_context(
        root, rollback_package
    )
    if forward_package.approval_artifact is None:
        raise I6PointerRuntimeError("research top package lost Gate-B authority")
    gate_b = _load_gate_b_approval_exact(root, forward_package.approval_artifact)
    target = _load_release_authority_exact(root, envelope.release_chain.delta)
    output = target.output_set
    shadow_event = _shadow_event(_forward_envelope.event)
    rollback_event = _rollback_event(rollback_package.lifecycle_event)
    producer_available_session = max(
        target.run_spec.run_available_session,
        target.completion.completion_available_session,
        target.deep.attestation_available_session,
        gate_b.approval.approval_available_session,
        shadow_event.event_available_session,
        rollback_event.event_available_session,
        rollback.receipt_available_session,
        forward_stage.stage_available_session,
        rollback_stage.stage_available_session,
        gate_c.approval.approval_available_session,
        _top_event(envelope.event).event_available_session,
        top_stage.stage_available_session,
    )
    return ResearchTopSnapshot(
        pointer_current_id=current.current_id,
        pointer_revision=current.pointer_revision,
        research_top_event_artifact=current.event_artifact,
        research_top_event=_top_event(envelope.event),
        gate_c_approval_artifact=gate_c.artifact,
        gate_c_approval=gate_c.approval,
        gate_b_approval_artifact=gate_b.artifact,
        gate_b_approval=gate_b.approval,
        shadow_stage_receipt_artifact=(rollback_package.source_stage_receipt_artifact),
        shadow_stage_receipt=forward_stage,
        shadow_pointer_event_artifact=forward_stage.event_artifact,
        shadow_pointer_event=shadow_event,
        rollback_stage_receipt_artifact=package.source_stage_receipt_artifact,
        rollback_stage_receipt=rollback_stage,
        rollback_pointer_event_artifact=rollback_stage.event_artifact,
        rollback_pointer_event=rollback_event,
        rollback_receipt_artifact=rollback_stage.rollback_receipt_artifact,
        rollback_receipt=rollback,
        research_top_stage_receipt_artifact=top_stage_pin,
        research_top_stage_receipt=top_stage,
        release_completion_artifact=envelope.release_chain.delta.completion_artifact,
        deep_attestation_artifact=(envelope.release_chain.delta.deep_attestation_artifact),
        release_id=target.resolved.release_id,
        native_v2_release_id=target.resolved.native_v2_release_id,
        terminal_session=target.run_spec.terminal_session,
        source_cutoff_session=target.run_spec.source_cutoff_session,
        release_available_session=target.run_spec.run_available_session,
        completion_available_session=target.completion.completion_available_session,
        deep_attestation_available_session=target.deep.attestation_available_session,
        producer_available_session=producer_available_session,
        source_binding_digest=input_set_digest(production_gate_a_input_pins(target.run_spec)),
        schema_bundle_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=target.run_spec.transform_semantics_digest,
        identity_policy_bundle_id=(
            target.run_spec.identity_policy_bundle.identity_policy_bundle_id
        ),
        calendar_digest=target.run_spec.calendar.calendar_artifact_id,
        checkpoint_id=output.checkpoint_id,
        checkpoint_artifact=output.checkpoint_artifact,
        resolved_state_digest=output.resolved_state_digest,
        resolved_content_digest=output.resolved_content_digest,
        physical_index_digest=target.deep.physical_index_digest,
        row_semantic_attestation_digest=(target.deep.row_semantic_attestation_digest),
        table_outputs=output.table_outputs,
    )


def _stage_ready_package(
    root: Path, package_pin: ArtifactPin, package: PointerActionPackage
) -> ArtifactPin:
    if package.state is not PointerPackageState.READY:
        raise I6PointerRuntimeError("pointer package is awaiting an external approval")
    if package.lifecycle_event is None:  # pragma: no cover - dataclass invariant
        raise I6PointerRuntimeError("ready package lost its lifecycle event")
    pointer = package.pointer_name
    event_id = package.lifecycle_event.event_id
    existing_receipt = _receipt_path(package.action, package.package_id)
    with _pointer_lock(root, pointer):
        current = _read_current_optional(root, pointer)
        if current is not None and current.event_id == event_id:
            return _resume_stage_after_pointer(
                root,
                package_pin=package_pin,
                package=package,
                current=current,
                receipt_path=existing_receipt,
            )
        _require_current_expectation(current, package.expected_current)
        chain = _load_release_chain_exact(root, package.release_chain)
        _validate_ready_package_runtime(root, package, chain=chain)
        source_event = _source_event_pin(root, package)
        rollback_pin = None
        if package.action is PointerAction.SHADOW_ROLLBACK:
            rollback_pin = _freeze_rollback_evidence(
                root,
                package=package,
                chain=chain,
            )
        envelope = _EventEnvelope(
            action=package.action,
            event=package.lifecycle_event,
            release_chain=package.release_chain,
            package_artifact=package_pin,
            predecessor_event_artifact=package.expected_current.event_artifact,
            approval_artifact=package.approval_artifact,
            source_event_artifact=source_event,
            rollback_receipt_artifact=rollback_pin,
        )
        event_pin = _write_immutable(
            root,
            _event_path(pointer, event_id),
            envelope.canonical_bytes(),
            label="pointer event",
        )
        _claim_event_revision(root, envelope=envelope, event_artifact=event_pin)
        selected_release = (
            chain.base.release_id
            if package.action is PointerAction.SHADOW_ROLLBACK
            else chain.delta.release_id
        )
        new_current = CurrentPointer(
            pointer_name=pointer,
            event_id=event_id,
            event_artifact=event_pin,
            release_id=selected_release,
            pointer_revision=package.lifecycle_event.pointer_revision,
            updated_session=package.event_available_session,
        )
        _atomic_compare_and_swap_current(
            root,
            pointer=pointer,
            expected=package.expected_current,
            replacement=new_current,
        )
        receipt = PointerStageReceipt(
            action=package.action,
            package_id=package.package_id,
            package_artifact=package_pin,
            event_id=event_id,
            event_artifact=event_pin,
            selected_release_id=selected_release,
            pointer_revision=new_current.pointer_revision,
            pointer_current_id=new_current.current_id,
            stage_available_session=package.event_available_session,
            rollback_receipt_artifact=rollback_pin,
        )
        receipt_pin = _write_immutable(
            root,
            existing_receipt,
            receipt.canonical_bytes(),
            label="pointer stage receipt",
        )
        _verify_stage_receipt_exact(root, receipt_pin, package=package, require_current=True)
        return receipt_pin


def _resume_stage_after_pointer(
    root: Path,
    *,
    package_pin: ArtifactPin,
    package: PointerActionPackage,
    current: CurrentPointer,
    receipt_path: str,
) -> ArtifactPin:
    """Finish immutable receipts after a selector replacement was interrupted."""

    if package.lifecycle_event is None:  # pragma: no cover - ready invariant
        raise I6PointerRuntimeError("resumed package lost its event")
    event_pin = current.event_artifact
    envelope = _load_event_envelope_exact(root, event_pin)
    if (
        envelope.event_id != package.lifecycle_event.event_id
        or envelope.package_artifact != package_pin
        or envelope.release_chain != package.release_chain
        or current.pointer_revision != package.lifecycle_event.pointer_revision
    ):
        raise I6LostCompareAndSwap("existing target event differs from prepared CAS")
    chain = _load_release_chain_exact(root, package.release_chain)
    _replay_pointer_ledger_exact(
        root,
        current,
        head_require_stage_receipt=False,
    )
    _validate_event_envelope_authority(
        root,
        envelope=envelope,
        chain=chain,
        require_stage_receipt=False,
    )
    selected_release = (
        chain.base.release_id
        if package.action is PointerAction.SHADOW_ROLLBACK
        else chain.delta.release_id
    )
    if current.release_id != selected_release:
        raise I6LostCompareAndSwap("existing target selector chose another release")
    rollback_pin = envelope.rollback_receipt_artifact
    if package.action is PointerAction.SHADOW_ROLLBACK:
        if rollback_pin is None:  # pragma: no cover - envelope invariant
            raise I6PointerRuntimeError("resumed rollback event lost its receipt")
        _validate_rollback_evidence(
            root,
            package=package,
            receipt=_load_rollback_receipt_exact(root, rollback_pin),
            chain=chain,
        )
    receipt = PointerStageReceipt(
        action=package.action,
        package_id=package.package_id,
        package_artifact=package_pin,
        event_id=current.event_id,
        event_artifact=event_pin,
        selected_release_id=selected_release,
        pointer_revision=current.pointer_revision,
        pointer_current_id=current.current_id,
        stage_available_session=package.event_available_session,
        rollback_receipt_artifact=rollback_pin,
    )
    receipt_pin = _write_immutable(
        root,
        receipt_path,
        receipt.canonical_bytes(),
        label="resumed pointer stage receipt",
    )
    _verify_stage_receipt_exact(root, receipt_pin, package=package, require_current=True)
    return receipt_pin


def _validate_ready_package_runtime(
    root: Path, package: PointerActionPackage, *, chain: ResolvedReleaseChain
) -> None:
    if package.action is PointerAction.SHADOW_PUBLISH:
        if package.approval_artifact is None or package.shadow_completion_artifact is None:
            raise I6PointerRuntimeError("shadow package lost Gate B authority")
        shadow = _load_shadow_authority(
            root,
            completion_pin=package.shadow_completion_artifact,
            approval_pin=package.approval_artifact,
            availability_cutoff_session=package.event_available_session,
        )
        if shadow is None or shadow.gate_b.approval.shadow_release_id != chain.delta.release_id:
            raise I6PointerRuntimeError("shadow package Gate B release differs")
        _validate_shadow_release_binding(
            shadow=shadow,
            release_chain=package.release_chain,
            resolved_chain=chain,
        )
        _validate_shadow_event(
            root,
            _shadow_event(package.lifecycle_event),
            shadow=shadow,
            expectation=package.expected_current,
        )
    elif package.action is PointerAction.SHADOW_ROLLBACK:
        _load_forward_context(root, package)
    else:
        if package.approval_artifact is None or package.source_stage_receipt_artifact is None:
            raise I6PointerRuntimeError("cutover package lost Gate C authority")
        rollback_stage = _load_stage_receipt_exact(root, package.source_stage_receipt_artifact)
        rollback_package = _load_package_exact(root, rollback_stage.package_artifact)
        _validate_cutover_event(
            root,
            gate_c=_load_gate_c_approval_exact(root, package.approval_artifact),
            event=_top_event(package.lifecycle_event),
            rollback_stage=rollback_stage,
            rollback_package=rollback_package,
            resolved_chain=chain,
            research_expectation=package.expected_current,
            cutoff=package.event_available_session,
        )


def _freeze_rollback_evidence(
    root: Path,
    *,
    package: PointerActionPackage,
    chain: ResolvedReleaseChain,
) -> ArtifactPin:
    rollback_event = _rollback_event(package.lifecycle_event)
    details = {
        "artifact_type": "s7_5_i6_shadow_rollback_details",
        "deleted_artifact_count": 0,
        "parent_reader_after_digest": chain.base.reader_digest,
        "parent_reader_before_digest": chain.base.reader_digest,
        "release_chain_digest": chain.chain_digest,
        "rollback_event_id": rollback_event.event_id,
        "rule_version": I6_ROLLBACK_DETAILS_RULE_VERSION,
        "surviving_artifact_set_digest": _surviving_artifact_set_digest(package.release_chain),
    }
    details_pin = _write_immutable(
        root,
        f"{_ROLLBACK_ROOT}/event_id={rollback_event.event_id}/details.json",
        _canonical_json_bytes(details),
        label="rollback details",
    )
    receipt = RollbackReceipt(
        shadow_pointer_event_id=rollback_event.forward_shadow_event_id,
        rollback_pointer_event_id=rollback_event.event_id,
        rolled_back_release_id=chain.delta.release_id,
        selected_parent_release_id=chain.base.release_id,
        parent_reader_before_digest=chain.base.reader_digest,
        parent_reader_after_digest=chain.base.reader_digest,
        deleted_artifact_count=0,
        surviving_artifact_set_digest=details["surviving_artifact_set_digest"],
        details_artifact=details_pin,
        receipt_available_session=package.event_available_session,
    )
    pin = _write_immutable(
        root,
        f"{_ROLLBACK_ROOT}/event_id={rollback_event.event_id}/receipt.json",
        _canonical_json_bytes(receipt.to_dict()),
        label="rollback lifecycle receipt",
    )
    _validate_rollback_evidence(root, package=package, receipt=receipt, chain=chain)
    return pin


def _validate_rollback_evidence(
    root: Path,
    *,
    package: PointerActionPackage,
    receipt: RollbackReceipt,
    chain: ResolvedReleaseChain,
) -> None:
    forward_stage, _forward_package, forward_envelope = _load_forward_context(root, package)
    rollback_event = _rollback_event(package.lifecycle_event)
    shadow_event = _shadow_event(forward_envelope.event)
    details_content = _ExactReader(root).read_pin(
        receipt.details_artifact, label="rollback details"
    )
    details = _closed_json(details_content, "rollback details")
    _keys(
        details,
        {
            "artifact_type",
            "deleted_artifact_count",
            "parent_reader_after_digest",
            "parent_reader_before_digest",
            "release_chain_digest",
            "rollback_event_id",
            "rule_version",
            "surviving_artifact_set_digest",
        },
        "rollback details",
    )
    expected = {
        "artifact_type": "s7_5_i6_shadow_rollback_details",
        "deleted_artifact_count": 0,
        "parent_reader_after_digest": chain.base.reader_digest,
        "parent_reader_before_digest": chain.base.reader_digest,
        "release_chain_digest": chain.chain_digest,
        "rollback_event_id": rollback_event.event_id,
        "rule_version": I6_ROLLBACK_DETAILS_RULE_VERSION,
        "surviving_artifact_set_digest": _surviving_artifact_set_digest(package.release_chain),
    }
    if details != expected:
        raise I6PointerRuntimeError("rollback details differ from the exact parent")
    validate_rollback_receipt(
        receipt,
        shadow_event=shadow_event,
        rollback_event=rollback_event,
        expected_parent_release_id=chain.base.release_id,
        observed_current_event_id=forward_stage.event_id,
        observed_current_release_id=forward_stage.selected_release_id,
        observed_current_pointer_revision=forward_stage.pointer_revision,
        availability_cutoff_session=package.event_available_session,
        artifact_reader=_ExactReader(root).read_path,
    )


def _validate_cutover_event(
    root: Path,
    *,
    gate_c: PinnedGateCApproval,
    event: TopPointerEvent,
    rollback_stage: PointerStageReceipt,
    rollback_package: PointerActionPackage,
    resolved_chain: ResolvedReleaseChain,
    research_expectation: CurrentExpectation,
    cutoff: date,
) -> None:
    if rollback_stage.rollback_receipt_artifact is None:
        raise I6PointerRuntimeError("cutover lacks a rollback lifecycle receipt")
    rollback_stage_pin = _pin_existing(
        root,
        _receipt_path(PointerAction.SHADOW_ROLLBACK, rollback_package.package_id),
        "cutover rollback stage receipt",
    )
    if _load_stage_receipt_exact(root, rollback_stage_pin) != rollback_stage:
        raise I6PointerRuntimeError("cutover rollback stage receipt differs")
    _verify_stage_receipt_exact(
        root,
        rollback_stage_pin,
        package=rollback_package,
        require_current=False,
    )
    rollback_envelope = _load_event_envelope_exact(root, rollback_stage.event_artifact)
    rollback_chain = _load_release_chain_exact(root, rollback_package.release_chain)
    _validate_event_envelope_authority(
        root,
        envelope=rollback_envelope,
        chain=rollback_chain,
    )
    rollback_receipt = _load_rollback_receipt_exact(root, rollback_stage.rollback_receipt_artifact)
    forward_stage, forward_package, forward_envelope = _load_forward_context(root, rollback_package)
    if (
        forward_package.approval_artifact is None
        or forward_package.shadow_completion_artifact is None
    ):
        raise I6PointerRuntimeError("forward shadow package lost Gate B")
    shadow = _load_shadow_authority(
        root,
        completion_pin=forward_package.shadow_completion_artifact,
        approval_pin=forward_package.approval_artifact,
        availability_cutoff_session=cutoff,
    )
    if shadow is None:  # pragma: no cover - exact pins supplied
        raise I6PointerRuntimeError("forward Gate B is unavailable")
    _validate_shadow_release_binding(
        shadow=shadow,
        release_chain=rollback_package.release_chain,
        resolved_chain=resolved_chain,
    )
    validate_atomic_cutover(
        gate_c,
        event,
        gate_b=shadow.gate_b,
        shadow_spec=shadow.spec.lifecycle_spec,
        shadow_receipt=shadow.completion.receipt,
        shadow_event=_shadow_event(forward_envelope.event),
        rollback_event=_rollback_event(rollback_package.lifecycle_event),
        rollback_receipt=rollback_receipt,
        shadow_observed_previous_event_id=forward_package.expected_current.event_id,
        shadow_observed_previous_release_id=forward_package.expected_current.release_id,
        shadow_observed_previous_pointer_revision=(
            forward_package.expected_current.pointer_revision
        ),
        rollback_observed_current_event_id=forward_stage.event_id,
        rollback_observed_current_release_id=forward_stage.selected_release_id,
        rollback_observed_current_pointer_revision=forward_stage.pointer_revision,
        observed_current_event_id=_required(research_expectation.event_id, "research event"),
        observed_current_release_id=_required(research_expectation.release_id, "research release"),
        observed_current_pointer_revision=research_expectation.pointer_revision,
        availability_cutoff_session=cutoff,
        artifact_reader=_ExactReader(root).read_path,
    )


def _validate_shadow_event(
    root: Path,
    event: ShadowPointerEvent,
    *,
    shadow: _ShadowAuthority,
    expectation: CurrentExpectation,
) -> None:
    validate_shadow_pointer_event(
        event,
        gate_b=shadow.gate_b,
        spec=shadow.spec.lifecycle_spec,
        receipt=shadow.completion.receipt,
        observed_current_event_id=expectation.event_id,
        observed_current_release_id=expectation.release_id,
        observed_current_pointer_revision=expectation.pointer_revision,
        availability_cutoff_session=event.event_available_session,
        artifact_reader=_ExactReader(root).read_path,
    )


def _load_shadow_authority(
    root: Path,
    *,
    completion_pin: ArtifactPin,
    approval_pin: ArtifactPin | None,
    availability_cutoff_session: date,
) -> _ShadowAuthority | None:
    completion = load_i5_shadow_completion_exact(root, completion_pin, production=True)
    spec_content = _ExactReader(root).read_pin(
        completion.run_spec_artifact, label="I5 shadow RunSpec"
    )
    spec = _shadow_run_spec_from_dict(_closed_json(spec_content, "I5 shadow RunSpec"))
    if (
        spec.authority != I5_PRODUCTION_AUTHORITY
        or spec.canonical_bytes() != spec_content
        or spec.run_spec_id != completion.run_spec_id
        or spec.exact_pin(path=completion.run_spec_artifact.path) != completion.run_spec_artifact
    ):
        raise I6PointerRuntimeError("I5 shadow RunSpec binding differs")
    if approval_pin is None:
        return None
    gate_b = _load_gate_b_approval_exact(root, approval_pin)
    try:
        validate_gate_b_approval(
            gate_b,
            spec=spec.lifecycle_spec,
            receipt=completion.receipt,
            availability_cutoff_session=availability_cutoff_session,
            artifact_reader=_ExactReader(root).read_path,
        )
    except IncrementalLifecycleError as exc:
        raise I6PointerRuntimeError("Gate B validation failed") from exc
    return _ShadowAuthority(spec=spec, completion=completion, gate_b=gate_b)


def _validate_shadow_release_binding(
    *,
    shadow: _ShadowAuthority,
    release_chain: ReleaseChainBinding,
    resolved_chain: ResolvedReleaseChain,
) -> None:
    """Close I5 producer replay onto the exact I3 chain selected by I6."""

    if (
        shadow.spec.incremental_completion_artifact != release_chain.delta.completion_artifact
        or shadow.spec.incremental_deep_attestation_artifact
        != release_chain.delta.deep_attestation_artifact
        or shadow.spec.incremental_release_id != resolved_chain.delta.release_id
        or shadow.spec.common_parent_release_id != resolved_chain.base.release_id
    ):
        raise I6PointerRuntimeError("I5 producer authority differs from the exact I3 chain")


def _load_release_chain_exact(root: Path, binding: ReleaseChainBinding) -> ResolvedReleaseChain:
    base_authority = _load_release_authority_exact(root, binding.base)
    delta_authority = _load_release_authority_exact(root, binding.delta)
    base = base_authority.resolved
    delta = delta_authority.resolved
    if delta.parent_release_id != base.release_id:
        raise I6PointerRuntimeError("exact DELTA does not descend from exact BASE")
    delta_spec = _load_release_run_spec(root, binding.delta)
    if (
        delta_spec.parent_shadow_completion_artifact != binding.base.completion_artifact
        or delta_spec.parent_deep_attestation_artifact != binding.base.deep_attestation_artifact
        or delta_spec.parent_gate_a_manifest is None
        or delta_spec.parent_gate_a_manifest.release_id != base.release_id
        or delta_authority.deep.parent_frontier_attestation_digest
        != base_authority.deep.deep_attestation_id
    ):
        raise I6PointerRuntimeError("DELTA exact parent authority differs from BASE binding")
    validate_production_compact_base_initial_rowsets(
        base_authority.run_spec, base_authority.output_set.table_outputs
    )
    validate_production_delta_append_outputs(
        delta_authority.run_spec,
        delta_authority.output_set.table_outputs,
        base_authority.output_set,
    )
    return ResolvedReleaseChain(base=base, delta=delta)


def _load_release_run_spec(root: Path, binding: ReleaseAuthorityPin):
    reader = _ExactReader(root)
    completion = load_i3_production_completion_exact(binding.completion_artifact, reader.read_path)
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader.read_path)
    return load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader.read_path)


def _load_release_exact(root: Path, binding: ReleaseAuthorityPin) -> ResolvedRelease:
    return _load_release_authority_exact(root, binding).resolved


def _load_release_authority_exact(
    root: Path, binding: ReleaseAuthorityPin
) -> _LoadedReleaseAuthority:
    reader = _ExactReader(root)
    completion = load_i3_production_completion_exact(binding.completion_artifact, reader.read_path)
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader.read_path)
    run_spec = load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader.read_path)
    deep = load_i3_production_deep_attestation_exact(
        binding.deep_attestation_artifact, reader.read_path
    )
    output = receipt.output_set
    expected_root = (
        f"manifests/silver/identity/s7-5-native-v2-staging/run_spec_id={run_spec.run_spec_id}"
    )
    if binding.completion_artifact.path != f"{expected_root}/completion.json" or (
        binding.deep_attestation_artifact.path
        != f"{expected_root}/deep-verification-attestation.json"
    ):
        raise I6PointerRuntimeError("I3 release authority was copied from its canonical path")
    if (
        receipt.state is not I3ProductionRunState.SUCCEEDED
        or output is None
        or completion.run_spec_id != run_spec.run_spec_id
        or completion.receipt_id != receipt.receipt_id
        or completion.output_set_id != output.output_set_id
        or completion.release_id != output.gate_a_manifest_pin.release_id
        or completion.native_v2_envelope_id != output.release_id
        or completion.checkpoint_id != output.checkpoint_id
        or deep.completion_id != completion.completion_id
        or deep.completion_artifact != binding.completion_artifact
        or deep.gate_a_manifest_pin != output.gate_a_manifest_pin
        or deep.native_v2_release.release_id != output.release_id
        or deep.checkpoint_id != output.checkpoint_id
        or deep.checkpoint_artifact != output.checkpoint_artifact
        or deep.output_set_id != output.output_set_id
        or deep.physical_index_digest != production_physical_index_digest(output)
    ):
        raise I6PointerRuntimeError("I3 release exact control chain differs")
    parent = (
        None
        if run_spec.parent_gate_a_manifest is None
        else run_spec.parent_gate_a_manifest.release_id
    )
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        if parent is not None or deep.parent_frontier_attestation_digest is not None:
            raise I6PointerRuntimeError("BASE release carries a parent frontier")
    elif run_spec.run_kind is I3ProductionRunKind.DELTA:
        if parent is None or deep.parent_frontier_attestation_digest is None:
            raise I6PointerRuntimeError("DELTA release omits its parent frontier")
    else:  # pragma: no cover - closed enum
        raise I6PointerRuntimeError("pointer accepts only BASE or DELTA")
    resolved = ResolvedRelease(
        release_id=completion.release_id,
        native_v2_release_id=completion.native_v2_envelope_id,
        run_kind=run_spec.run_kind,
        terminal_session=run_spec.terminal_session,
        parent_release_id=parent,
        reader_digest=stable_digest(
            {
                "completion": binding.completion_artifact.to_dict(),
                "deep_attestation": binding.deep_attestation_artifact.to_dict(),
                "output_set_id": output.output_set_id,
                "physical_index_digest": deep.physical_index_digest,
                "release_id": completion.release_id,
                "rule_version": "s7_5_i6_exact_release_reader_v1",
            }
        ),
    )
    return _LoadedReleaseAuthority(
        resolved=resolved,
        completion=completion,
        run_spec=run_spec,
        output_set=output,
        deep=deep,
    )


def _load_gate_b_approval_exact(root: Path, pin: ArtifactPin) -> PinnedGateBApproval:
    _validate_approval_acl(root, pin)
    content = _ExactReader(root).read_pin(pin, label="Gate B approval")
    item = _closed_json(content, "Gate B approval")
    approval = _gate_b_from_dict(item)
    expected = f"{_GATE_B_ROOT}/approval_id={approval.approval_id}/approval.json"
    if pin.path != expected or _canonical_json_bytes(approval.to_dict()) != content:
        raise I6PointerRuntimeError("Gate B approval path or bytes are not canonical")
    if approval.approver_id not in _TRUSTED_APPROVER_IDS:
        raise I6PointerRuntimeError("Gate B approver is outside the closed trust root")
    return PinnedGateBApproval(approval=approval, artifact=pin)


def _load_gate_c_approval_exact(root: Path, pin: ArtifactPin) -> PinnedGateCApproval:
    _validate_approval_acl(root, pin)
    content = _ExactReader(root).read_pin(pin, label="Gate C approval")
    item = _closed_json(content, "Gate C approval")
    approval = _gate_c_from_dict(item)
    expected = f"{_GATE_C_ROOT}/approval_id={approval.approval_id}/approval.json"
    if pin.path != expected or _canonical_json_bytes(approval.to_dict()) != content:
        raise I6PointerRuntimeError("Gate C approval path or bytes are not canonical")
    if approval.approver_id not in _TRUSTED_APPROVER_IDS:
        raise I6PointerRuntimeError("Gate C approver is outside the closed trust root")
    return PinnedGateCApproval(approval=approval, artifact=pin)


def _read_visible_pointer(root: Path, pointer_name: str) -> ResolvedPointerView:
    current = _read_current_required(root, pointer_name)
    _replay_pointer_ledger_exact(root, current)
    if pointer_name == RESEARCH_POINTER_NAME:
        anchor = _load_research_anchor_optional(root, current.event_artifact)
        if anchor is not None:
            if (
                anchor.event_id != current.event_id
                or anchor.pointer_revision != current.pointer_revision
                or anchor.selected_release_id != current.release_id
                or anchor.available_session != current.updated_session
            ):
                raise I6PointerRuntimeError("research-parent anchor differs from current")
            chain = _load_release_chain_exact(root, anchor.release_chain)
            if current.release_id != chain.base.release_id:
                raise I6PointerRuntimeError("research-parent anchor does not select BASE")
            return ResolvedPointerView(
                pointer_name=pointer_name,
                event_id=current.event_id,
                release_id=current.release_id,
                pointer_revision=current.pointer_revision,
                release=chain.base,
                chain_digest=chain.chain_digest,
            )
    envelope = _load_event_envelope_exact(root, current.event_artifact)
    if (
        envelope.event_id != current.event_id
        or envelope.event.pointer_revision != current.pointer_revision
        or envelope.event.event_available_session != current.updated_session
    ):
        raise I6PointerRuntimeError("current selector differs from immutable event")
    if (
        pointer_name == RESEARCH_POINTER_NAME
        and envelope.action is not PointerAction.RESEARCH_CUTOVER
    ):
        raise I6PointerRuntimeError("research selector cannot expose shadow output")
    if pointer_name == SHADOW_POINTER_NAME and envelope.action is PointerAction.RESEARCH_CUTOVER:
        raise I6PointerRuntimeError("shadow selector references a research event")
    chain = _load_release_chain_exact(root, envelope.release_chain)
    _validate_event_envelope_authority(root, envelope=envelope, chain=chain)
    selected = chain.base if current.release_id == chain.base.release_id else chain.delta
    if current.release_id not in {chain.base.release_id, chain.delta.release_id}:
        raise I6PointerRuntimeError("current release is outside its exact chain")
    if envelope.action is PointerAction.SHADOW_PUBLISH and selected is not chain.delta:
        raise I6PointerRuntimeError("shadow publication does not select DELTA")
    if envelope.action is PointerAction.SHADOW_ROLLBACK and selected is not chain.base:
        raise I6PointerRuntimeError("rollback does not select BASE")
    if envelope.action is PointerAction.RESEARCH_CUTOVER and selected is not chain.delta:
        raise I6PointerRuntimeError("research cutover does not select DELTA")
    return ResolvedPointerView(
        pointer_name=pointer_name,
        event_id=current.event_id,
        release_id=current.release_id,
        pointer_revision=current.pointer_revision,
        release=selected,
        chain_digest=chain.chain_digest,
    )


def _load_ledger_node_exact(
    root: Path,
    *,
    pointer_name: str,
    event_artifact: ArtifactPin,
    require_stage_receipt: bool = True,
) -> _LedgerNode:
    if pointer_name == RESEARCH_POINTER_NAME:
        anchor = _load_research_anchor_optional(root, event_artifact)
        if anchor is not None:
            return _LedgerNode(
                event_id=anchor.event_id,
                event_artifact=event_artifact,
                release_id=anchor.selected_release_id,
                pointer_revision=anchor.pointer_revision,
                available_session=anchor.available_session,
                predecessor_event_artifact=None,
                envelope=None,
            )
    envelope = _load_event_envelope_exact(root, event_artifact)
    if _pointer_for_action(envelope.action) != pointer_name:
        raise I6PointerRuntimeError("pointer ledger crosses selector namespaces")
    chain = _load_release_chain_exact(root, envelope.release_chain)
    _validate_event_envelope_authority(
        root,
        envelope=envelope,
        chain=chain,
        require_stage_receipt=require_stage_receipt,
    )
    return _LedgerNode(
        event_id=envelope.event_id,
        event_artifact=event_artifact,
        release_id=envelope.event.new_release_id,
        pointer_revision=envelope.event.pointer_revision,
        available_session=envelope.event.event_available_session,
        predecessor_event_artifact=envelope.predecessor_event_artifact,
        envelope=envelope,
    )


def _replay_pointer_ledger_exact(
    root: Path,
    current: CurrentPointer,
    *,
    head_require_stage_receipt: bool = True,
) -> None:
    node = _load_ledger_node_exact(
        root,
        pointer_name=current.pointer_name,
        event_artifact=current.event_artifact,
        require_stage_receipt=head_require_stage_receipt,
    )
    if (
        node.event_id != current.event_id
        or node.release_id != current.release_id
        or node.pointer_revision != current.pointer_revision
        or node.available_session != current.updated_session
    ):
        raise I6PointerRuntimeError("current selector is not its exact ledger head")
    seen = {node.event_artifact.path}
    for _depth in range(_MAX_LEDGER_DEPTH):
        predecessor_pin = node.predecessor_event_artifact
        if predecessor_pin is None:
            if node.envelope is not None:
                package = _load_package_exact(root, node.envelope.package_artifact)
                event = node.envelope.event
                if (
                    package.expected_current.exists
                    or event.expected_previous_event_id is not None
                    or event.previous_release_id is not None
                    or event.pointer_revision != 1
                    or node.envelope.action is not PointerAction.SHADOW_PUBLISH
                ):
                    raise I6PointerRuntimeError("pointer ledger root is not a first shadow event")
            return
        if predecessor_pin.path in seen:
            raise I6PointerRuntimeError("pointer ledger contains a predecessor cycle")
        seen.add(predecessor_pin.path)
        predecessor = _load_ledger_node_exact(
            root,
            pointer_name=current.pointer_name,
            event_artifact=predecessor_pin,
            require_stage_receipt=True,
        )
        if node.envelope is None:  # pragma: no cover - anchor is always a root
            raise I6PointerRuntimeError("research-parent anchor cannot have a predecessor")
        package = _load_package_exact(root, node.envelope.package_artifact)
        expected_predecessor_current = CurrentPointer(
            pointer_name=current.pointer_name,
            event_id=predecessor.event_id,
            event_artifact=predecessor.event_artifact,
            release_id=predecessor.release_id,
            pointer_revision=predecessor.pointer_revision,
            updated_session=predecessor.available_session,
        )
        event = node.envelope.event
        if (
            package.expected_current != _expectation(expected_predecessor_current)
            or event.expected_previous_event_id != predecessor.event_id
            or event.previous_release_id != predecessor.release_id
            or event.pointer_revision != predecessor.pointer_revision + 1
            or event.event_available_session < predecessor.available_session
        ):
            raise I6PointerRuntimeError("pointer ledger predecessor continuity differs")
        node = predecessor
    raise I6PointerRuntimeError("pointer ledger exceeds the bounded replay depth")


def _validate_event_envelope_authority(
    root: Path,
    *,
    envelope: _EventEnvelope,
    chain: ResolvedReleaseChain,
    require_stage_receipt: bool = True,
) -> None:
    event_artifact = _pin_existing(
        root,
        _event_path(_pointer_for_action(envelope.action), envelope.event_id),
        "pointer event",
    )
    _validate_revision_claim_exact(
        root,
        envelope=envelope,
        event_artifact=event_artifact,
    )
    package = _load_package_exact(root, envelope.package_artifact)
    if (
        package.state is not PointerPackageState.READY
        or package.action is not envelope.action
        or package.lifecycle_event != envelope.event
        or package.release_chain != envelope.release_chain
        or package.approval_artifact != envelope.approval_artifact
        or package.expected_current.event_artifact != envelope.predecessor_event_artifact
    ):
        raise I6PointerRuntimeError("pointer event differs from its exact action package")
    expected_source = _source_event_pin(root, package)
    if expected_source != envelope.source_event_artifact:
        raise I6PointerRuntimeError("pointer event source-event binding differs")
    _validate_ready_package_runtime(root, package, chain=chain)
    if envelope.action is PointerAction.SHADOW_ROLLBACK:
        if envelope.rollback_receipt_artifact is None:  # pragma: no cover - invariant
            raise I6PointerRuntimeError("rollback event omits rollback evidence")
        _validate_rollback_evidence(
            root,
            package=package,
            receipt=_load_rollback_receipt_exact(root, envelope.rollback_receipt_artifact),
            chain=chain,
        )
    if require_stage_receipt:
        stage_pin = _pin_existing(
            root,
            _receipt_path(envelope.action, package.package_id),
            "historical pointer stage receipt",
        )
        _verify_stage_receipt_exact(
            root,
            stage_pin,
            package=package,
            require_current=False,
        )


def _load_research_anchor_optional(root: Path, pin: ArtifactPin) -> ResearchParentAnchor | None:
    content = _ExactReader(root).read_pin(pin, label="research pointer event")
    item = _closed_json(content, "research pointer event")
    if item.get("artifact_type") != "s7_5_i6_imported_research_parent":
        return None
    _keys(
        item,
        {
            "artifact_type",
            "available_session",
            "event_id",
            "pointer_name",
            "pointer_revision",
            "release_chain",
            "rule_version",
            "selected_release_id",
            "source_publication_artifact",
        },
        "research-parent anchor",
    )
    _literal(item["pointer_name"], RESEARCH_POINTER_NAME, "anchor pointer")
    _literal(
        item["rule_version"],
        "s7_5_i6_imported_research_parent_v1",
        "anchor rule",
    )
    anchor = ResearchParentAnchor(
        event_id=_text(item["event_id"], "anchor event ID"),
        release_chain=ReleaseChainBinding.from_dict(item["release_chain"]),
        selected_release_id=_text(item["selected_release_id"], "anchor release ID"),
        pointer_revision=_integer(item["pointer_revision"], "anchor revision"),
        available_session=_date_value(item["available_session"], "anchor availability"),
        source_publication_artifact=_artifact_from_dict(
            item["source_publication_artifact"], "anchor source publication"
        ),
    )
    if (
        anchor.event_id != anchor.reproduced_event_id
        or pin.path != _event_path(RESEARCH_POINTER_NAME, anchor.event_id)
        or anchor.canonical_bytes() != content
    ):
        raise I6PointerRuntimeError("research-parent anchor path or ID differs")
    _ExactReader(root).read_pin(
        anchor.source_publication_artifact,
        label="research-parent source publication",
    )
    return anchor


def _verify_stage_current(
    root: Path, pin: ArtifactPin, *, expected_action: PointerAction
) -> ResolvedPointerView:
    receipt = _load_stage_receipt_exact(root, pin)
    if receipt.action is not expected_action:
        raise I6PointerRuntimeError("stage receipt action differs")
    package = _load_package_exact(root, receipt.package_artifact)
    _verify_stage_receipt_exact(root, pin, package=package, require_current=True)
    return _read_visible_pointer(root, _pointer_for_action(expected_action))


def _verify_stage_receipt_exact(
    root: Path,
    pin: ArtifactPin,
    *,
    package: PointerActionPackage,
    require_current: bool,
) -> PointerStageReceipt:
    receipt = _load_stage_receipt_exact(root, pin)
    if (
        receipt.action is not package.action
        or receipt.package_id != package.package_id
        or receipt.package_artifact.path != _package_path(package)
        or receipt.event_id != _required(package.lifecycle_event, "lifecycle event").event_id
    ):
        raise I6PointerRuntimeError("stage receipt package binding differs")
    envelope = _load_event_envelope_exact(root, receipt.event_artifact)
    event = _required(package.lifecycle_event, "lifecycle event")
    expected_current = CurrentPointer(
        pointer_name=package.pointer_name,
        event_id=receipt.event_id,
        event_artifact=receipt.event_artifact,
        release_id=event.new_release_id,
        pointer_revision=event.pointer_revision,
        updated_session=package.event_available_session,
    )
    if (
        envelope.event_id != receipt.event_id
        or envelope.action is not package.action
        or envelope.package_artifact != receipt.package_artifact
        or envelope.event != package.lifecycle_event
        or envelope.release_chain != package.release_chain
        or envelope.approval_artifact != package.approval_artifact
        or envelope.predecessor_event_artifact != package.expected_current.event_artifact
        or envelope.source_event_artifact != _source_event_pin(root, package)
        or receipt.selected_release_id != event.new_release_id
        or receipt.pointer_revision != event.pointer_revision
        or receipt.stage_available_session != package.event_available_session
        or event.event_available_session != package.event_available_session
        or receipt.pointer_current_id != expected_current.current_id
        or receipt.rollback_receipt_artifact != envelope.rollback_receipt_artifact
    ):
        raise I6PointerRuntimeError("stage receipt event binding differs")
    if require_current:
        current = _read_current_required(root, _pointer_for_action(receipt.action))
        if (
            current.event_id != receipt.event_id
            or current.current_id != receipt.pointer_current_id
            or current.release_id != receipt.selected_release_id
            or current.pointer_revision != receipt.pointer_revision
        ):
            raise I6PointerRuntimeError("stage receipt is not the current selector")
    if receipt.action is PointerAction.SHADOW_ROLLBACK:
        if receipt.rollback_receipt_artifact is None:
            raise I6PointerRuntimeError("rollback stage receipt omits lifecycle evidence")
        _load_rollback_receipt_exact(root, receipt.rollback_receipt_artifact)
    return receipt


def _load_forward_context(
    root: Path, rollback_package: PointerActionPackage
) -> tuple[PointerStageReceipt, PointerActionPackage, _EventEnvelope]:
    if rollback_package.source_stage_receipt_artifact is None:
        raise I6PointerRuntimeError("rollback package lost its forward receipt")
    forward_stage = _load_stage_receipt_exact(root, rollback_package.source_stage_receipt_artifact)
    forward_package = _load_package_exact(root, forward_stage.package_artifact)
    _verify_stage_receipt_exact(
        root,
        rollback_package.source_stage_receipt_artifact,
        package=forward_package,
        require_current=False,
    )
    forward_envelope = _load_event_envelope_exact(root, forward_stage.event_artifact)
    if (
        forward_stage.action is not PointerAction.SHADOW_PUBLISH
        or forward_package.action is not PointerAction.SHADOW_PUBLISH
        or forward_envelope.action is not PointerAction.SHADOW_PUBLISH
        or forward_stage.event_id != forward_envelope.event_id
        or forward_package.release_chain != rollback_package.release_chain
    ):
        raise I6PointerRuntimeError("rollback forward publication differs")
    chain = _load_release_chain_exact(root, forward_package.release_chain)
    _validate_event_envelope_authority(
        root,
        envelope=forward_envelope,
        chain=chain,
    )
    return forward_stage, forward_package, forward_envelope


def _source_event_pin(root: Path, package: PointerActionPackage) -> ArtifactPin | None:
    if package.action is PointerAction.SHADOW_PUBLISH:
        return None
    if package.source_stage_receipt_artifact is None:
        raise I6PointerRuntimeError("pointer action lacks source stage receipt")
    stage = _load_stage_receipt_exact(root, package.source_stage_receipt_artifact)
    return stage.event_artifact


def _write_package(root: Path, package: PointerActionPackage) -> ArtifactPin:
    return _write_immutable(
        root,
        _package_path(package),
        package.canonical_bytes(),
        label="pointer action package",
    )


def _revision_claim_payload(
    envelope: _EventEnvelope, event_artifact: ArtifactPin
) -> dict[str, object]:
    return {
        "action": envelope.action.value,
        "artifact_type": "s7_5_i6_pointer_revision_claim",
        "event_artifact": event_artifact.to_dict(),
        "event_id": envelope.event_id,
        "new_release_id": envelope.event.new_release_id,
        "pointer_name": _pointer_for_action(envelope.action),
        "pointer_revision": envelope.event.pointer_revision,
        "predecessor_event_artifact": (
            None
            if envelope.predecessor_event_artifact is None
            else envelope.predecessor_event_artifact.to_dict()
        ),
        "previous_release_id": envelope.event.previous_release_id,
        "rule_version": "s7_5_i6_pointer_revision_claim_v1",
    }


def _claim_event_revision(
    root: Path, *, envelope: _EventEnvelope, event_artifact: ArtifactPin
) -> ArtifactPin:
    relative = _revision_claim_path(
        _pointer_for_action(envelope.action), envelope.event.pointer_revision
    )
    content = _canonical_json_bytes(_revision_claim_payload(envelope, event_artifact))
    path = safe_relative_path(root, relative)
    if path.exists() or path.is_symlink():
        existing = _ExactReader(root).read_path(relative)
        if existing != content:
            raise I6LostCompareAndSwap("pointer revision already has another event winner")
        return ArtifactPin(
            path=relative,
            sha256=hashlib.sha256(existing).hexdigest(),
            bytes=len(existing),
        )
    try:
        return _write_immutable(root, relative, content, label="pointer revision claim")
    except I6PointerRuntimeError as exc:
        if path.exists() and not path.is_symlink():
            existing = _ExactReader(root).read_path(relative)
            if existing != content:
                raise I6LostCompareAndSwap(
                    "pointer revision lost its immutable event claim"
                ) from exc
            return ArtifactPin(
                path=relative,
                sha256=hashlib.sha256(existing).hexdigest(),
                bytes=len(existing),
            )
        raise


def _validate_revision_claim_exact(
    root: Path, *, envelope: _EventEnvelope, event_artifact: ArtifactPin
) -> None:
    relative = _revision_claim_path(
        _pointer_for_action(envelope.action), envelope.event.pointer_revision
    )
    content = _ExactReader(root).read_path(relative)
    if content != _canonical_json_bytes(_revision_claim_payload(envelope, event_artifact)):
        raise I6PointerRuntimeError("pointer revision claim differs from its exact event")


def _load_package_exact(root: Path, pin: ArtifactPin) -> PointerActionPackage:
    content = _ExactReader(root).read_pin(pin, label="pointer action package")
    item = _closed_json(content, "pointer action package")
    package = _package_from_dict(item)
    if pin.path != _package_path(package) or package.canonical_bytes() != content:
        raise I6PointerRuntimeError("pointer package path or bytes are not canonical")
    return package


def _load_event_envelope_exact(root: Path, pin: ArtifactPin) -> _EventEnvelope:
    content = _ExactReader(root).read_pin(pin, label="pointer event")
    item = _closed_json(content, "pointer event")
    envelope = _event_envelope_from_dict(item)
    if pin.path != _event_path(_pointer_for_action(envelope.action), envelope.event_id):
        raise I6PointerRuntimeError("pointer event was copied from its canonical path")
    if envelope.canonical_bytes() != content:
        raise I6PointerRuntimeError("pointer event bytes are not canonical")
    return envelope


def _load_stage_receipt_exact(root: Path, pin: ArtifactPin) -> PointerStageReceipt:
    content = _ExactReader(root).read_pin(pin, label="pointer stage receipt")
    receipt = _stage_receipt_from_dict(_closed_json(content, "pointer stage receipt"))
    expected = _receipt_path(receipt.action, receipt.package_id)
    if pin.path != expected or receipt.canonical_bytes() != content:
        raise I6PointerRuntimeError("pointer stage receipt path or bytes are not canonical")
    return receipt


def _load_rollback_receipt_exact(root: Path, pin: ArtifactPin) -> RollbackReceipt:
    content = _ExactReader(root).read_pin(pin, label="rollback lifecycle receipt")
    item = _closed_json(content, "rollback lifecycle receipt")
    receipt = _rollback_receipt_from_dict(item)
    expected = f"{_ROLLBACK_ROOT}/event_id={receipt.rollback_pointer_event_id}/receipt.json"
    if pin.path != expected or _canonical_json_bytes(receipt.to_dict()) != content:
        raise I6PointerRuntimeError("rollback receipt path or bytes are not canonical")
    return receipt


def _read_current_optional(root: Path, pointer_name: str) -> CurrentPointer | None:
    relative = _current_path(pointer_name)
    path = safe_relative_path(root, relative)
    if not path.exists() and not path.is_symlink():
        return None
    content = _ExactReader(root).read_path(relative)
    current = _current_from_dict(_closed_json(content, "current pointer"))
    if current.pointer_name != pointer_name or current.canonical_bytes() != content:
        raise I6PointerRuntimeError("current pointer bytes or namespace differ")
    return current


def _read_current_required(root: Path, pointer_name: str) -> CurrentPointer:
    current = _read_current_optional(root, pointer_name)
    if current is None:
        raise I6PointerRuntimeError(f"{pointer_name} current selector is missing")
    return current


def _atomic_compare_and_swap_current(
    root: Path,
    *,
    pointer: str,
    expected: CurrentExpectation,
    replacement: CurrentPointer,
) -> None:
    observed = _read_current_optional(root, pointer)
    _require_current_expectation(observed, expected)
    path = safe_relative_path(root, _current_path(pointer))
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_parents(root, path)
    temporary = path.with_name(f".{path.name}.swap-{os.getpid()}-{uuid4().hex}")
    content = replacement.canonical_bytes()
    if len(content) > _MAX_CONTROL_BYTES:
        raise I6PointerRuntimeError("current pointer exceeds control byte cap")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(fd)
        # The hook is only a test seam.  It runs before the final validation;
        # there is deliberately no callback between that validation and replace.
        _before_pointer_replace(path, temporary)
        _require_current_expectation(_read_current_optional(root, pointer), expected)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
        if _read_current_required(root, pointer) != replacement:
            raise I6LostCompareAndSwap("pointer replacement is not the validated ledger head")
    finally:
        temporary.unlink(missing_ok=True)


def _before_pointer_replace(_path: Path, _temporary: Path) -> None:
    """Fault-injection seam; production implementation intentionally does nothing."""


@contextmanager
def _pointer_lock(root: Path, pointer: str) -> Iterator[None]:
    path = safe_relative_path(root, f"{_LOCK_ROOT}/{pointer}.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_parents(root, path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        status = os.fstat(fd)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_nlink != 1
            or status.st_uid != os.geteuid()
            or stat.S_IMODE(status.st_mode) & 0o077
        ):
            raise I6PointerRuntimeError("pointer lock is outside the single-writer ACL")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise I6LostCompareAndSwap("another pointer writer holds the CAS lock") from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class _ExactReader:
    def __init__(self, root: Path) -> None:
        self.root = root

    def read_pin(self, pin: ArtifactPin, *, label: str) -> bytes:
        _artifact(pin, label)
        content = self.read_path(pin.path)
        if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
            raise I6PointerRuntimeError(f"{label} exact pin differs")
        return content

    def read_path(self, relative: str) -> bytes:
        try:
            path = safe_relative_path(self.root, _relative(relative, "artifact path"))
        except ArtifactError as exc:
            raise I6PointerRuntimeError(f"exact artifact is missing or unsafe: {relative}") from exc
        _reject_symlink_parents(self.root, path)
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError as exc:
            raise I6PointerRuntimeError(f"exact artifact is missing or unsafe: {relative}") from exc
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_size <= 0
                or before.st_size > _MAX_CONTROL_BYTES
            ):
                raise I6PointerRuntimeError("exact control artifact is not a single regular file")
            chunks: list[bytes] = []
            remaining = before.st_size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    raise I6PointerRuntimeError("exact control artifact was truncated during read")
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise I6PointerRuntimeError("exact control artifact grew during read")
            after = os.fstat(descriptor)
            try:
                named = os.stat(path, follow_symlinks=False)
            except OSError as exc:
                raise I6PointerRuntimeError("exact control path changed during read") from exc
            identity_before = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            identity_after = (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            if (
                identity_before != identity_after
                or (
                    named.st_dev,
                    named.st_ino,
                    named.st_size,
                    named.st_mtime_ns,
                    named.st_ctime_ns,
                )
                != identity_after
            ):
                raise I6PointerRuntimeError("exact control path changed during same-fd read")
            return b"".join(chunks)
        finally:
            os.close(descriptor)


def _require_current_expectation(
    observed: CurrentPointer | None, expected: CurrentExpectation
) -> None:
    actual = _expectation(observed)
    if actual != expected:
        raise I6LostCompareAndSwap("pointer selector lost compare-and-swap race")


def _expectation(current: CurrentPointer | None) -> CurrentExpectation:
    if current is None:
        return CurrentExpectation(
            exists=False,
            current_sha256=None,
            event_id=None,
            event_artifact=None,
            release_id=None,
            pointer_revision=0,
            updated_session=None,
        )
    content = current.canonical_bytes()
    return CurrentExpectation(
        True,
        hashlib.sha256(content).hexdigest(),
        current.event_id,
        current.event_artifact,
        current.release_id,
        current.pointer_revision,
        current.updated_session,
    )


def _write_immutable(root: Path, relative: str, content: bytes, *, label: str) -> ArtifactPin:
    if len(content) <= 0 or len(content) > _MAX_CONTROL_BYTES:
        raise I6PointerRuntimeError(f"{label} exceeds immutable byte bounds")
    if _disk_free(root) < _MIN_FREE_DISK_BYTES:
        raise I6PointerRuntimeError("pointer control disk floor would be breached")
    _production_path(relative, label)
    try:
        result = write_bytes_immutable(root, safe_relative_path(root, relative), content)
    except Exception as exc:
        raise I6PointerRuntimeError(f"cannot write immutable {label}") from exc
    pin = ArtifactPin(
        path=str(result["path"]),
        sha256=str(result["sha256"]),
        bytes=int(result["bytes"]),
    )
    if _ExactReader(root).read_pin(pin, label=label) != content:
        raise I6PointerRuntimeError(f"immutable {label} did not survive exact publication")
    return pin


def _pin_existing(root: Path, relative: str, label: str) -> ArtifactPin:
    content = _ExactReader(root).read_path(relative)
    return ArtifactPin(
        path=relative, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
    )


def _package_from_dict(value: object) -> PointerActionPackage:
    item = _mapping(value, "pointer action package")
    _keys(
        item,
        {
            "action",
            "approval_artifact",
            "artifact_type",
            "event_available_session",
            "expected_current",
            "lifecycle_event",
            "package_id",
            "pointer_name",
            "release_chain",
            "rule_version",
            "shadow_completion_artifact",
            "source_stage_receipt_artifact",
            "state",
        },
        "pointer action package",
    )
    _literal(item["artifact_type"], "s7_5_i6_pointer_action_package", "package type")
    _literal(item["rule_version"], I6_ACTION_PACKAGE_RULE_VERSION, "package rule")
    action = _enum(PointerAction, item["action"], "pointer action")
    event_value = item["lifecycle_event"]
    event = None if event_value is None else _lifecycle_event_from_dict(action, event_value)
    package = PointerActionPackage(
        action=action,
        state=_enum(PointerPackageState, item["state"], "package state"),
        pointer_name=_text(item["pointer_name"], "package pointer"),
        event_available_session=_date_value(item["event_available_session"], "package date"),
        expected_current=CurrentExpectation.from_dict(item["expected_current"]),
        release_chain=ReleaseChainBinding.from_dict(item["release_chain"]),
        approval_artifact=_optional_artifact(item["approval_artifact"], "package approval"),
        shadow_completion_artifact=_optional_artifact(
            item["shadow_completion_artifact"], "package shadow completion"
        ),
        source_stage_receipt_artifact=_optional_artifact(
            item["source_stage_receipt_artifact"], "package source receipt"
        ),
        lifecycle_event=event,
    )
    if item["package_id"] != package.package_id:
        raise I6PointerRuntimeError("pointer package ID does not reproduce")
    return package


def _event_envelope_from_dict(value: object) -> _EventEnvelope:
    item = _mapping(value, "pointer event")
    _keys(
        item,
        {
            "action",
            "approval_artifact",
            "artifact_type",
            "event",
            "event_id",
            "package_artifact",
            "pointer_name",
            "predecessor_event_artifact",
            "release_chain",
            "rollback_receipt_artifact",
            "rule_version",
            "source_event_artifact",
        },
        "pointer event",
    )
    _literal(item["artifact_type"], "s7_5_i6_pointer_event_envelope", "event type")
    _literal(item["rule_version"], I6_EVENT_ENVELOPE_RULE_VERSION, "event rule")
    action = _enum(PointerAction, item["action"], "event action")
    _literal(item["pointer_name"], _pointer_for_action(action), "event pointer")
    event = _lifecycle_event_from_dict(action, item["event"])
    result = _EventEnvelope(
        action=action,
        event=event,
        release_chain=ReleaseChainBinding.from_dict(item["release_chain"]),
        package_artifact=_artifact_from_dict(item["package_artifact"], "event package"),
        predecessor_event_artifact=_optional_artifact(
            item["predecessor_event_artifact"], "event predecessor"
        ),
        approval_artifact=_optional_artifact(item["approval_artifact"], "event approval"),
        source_event_artifact=_optional_artifact(
            item["source_event_artifact"], "event source event"
        ),
        rollback_receipt_artifact=_optional_artifact(
            item["rollback_receipt_artifact"], "event rollback receipt"
        ),
    )
    if item["event_id"] != result.event_id:
        raise I6PointerRuntimeError("pointer event ID does not reproduce")
    return result


def _stage_receipt_from_dict(value: object) -> PointerStageReceipt:
    item = _mapping(value, "pointer stage receipt")
    _keys(
        item,
        {
            "action",
            "artifact_type",
            "event_artifact",
            "event_id",
            "package_artifact",
            "package_id",
            "pointer_current_id",
            "pointer_revision",
            "receipt_id",
            "rollback_receipt_artifact",
            "rule_version",
            "selected_release_id",
            "stage_available_session",
            "state",
        },
        "pointer stage receipt",
    )
    _literal(item["artifact_type"], "s7_5_i6_pointer_stage_receipt", "stage type")
    _literal(item["rule_version"], I6_STAGE_RECEIPT_RULE_VERSION, "stage rule")
    _literal(item["state"], "succeeded", "stage state")
    receipt = PointerStageReceipt(
        action=_enum(PointerAction, item["action"], "stage action"),
        package_id=_text(item["package_id"], "stage package ID"),
        package_artifact=_artifact_from_dict(item["package_artifact"], "stage package"),
        event_id=_text(item["event_id"], "stage event ID"),
        event_artifact=_artifact_from_dict(item["event_artifact"], "stage event"),
        selected_release_id=_text(item["selected_release_id"], "stage release ID"),
        pointer_revision=_integer(item["pointer_revision"], "stage revision"),
        pointer_current_id=_text(item["pointer_current_id"], "stage current ID"),
        stage_available_session=_date_value(item["stage_available_session"], "stage availability"),
        rollback_receipt_artifact=_optional_artifact(
            item["rollback_receipt_artifact"], "rollback lifecycle receipt"
        ),
    )
    if item["receipt_id"] != receipt.receipt_id:
        raise I6PointerRuntimeError("pointer stage receipt ID does not reproduce")
    return receipt


def _current_from_dict(value: object) -> CurrentPointer:
    item = _mapping(value, "current pointer")
    _keys(
        item,
        {
            "artifact_type",
            "current_id",
            "event_artifact",
            "event_id",
            "pointer_name",
            "pointer_revision",
            "release_id",
            "rule_version",
            "updated_session",
        },
        "current pointer",
    )
    _literal(item["artifact_type"], "s7_5_i6_current_pointer", "current type")
    _literal(item["rule_version"], I6_CURRENT_POINTER_RULE_VERSION, "current rule")
    result = CurrentPointer(
        pointer_name=_text(item["pointer_name"], "current pointer name"),
        event_id=_text(item["event_id"], "current event ID"),
        event_artifact=_artifact_from_dict(item["event_artifact"], "current event"),
        release_id=_text(item["release_id"], "current release ID"),
        pointer_revision=_integer(item["pointer_revision"], "current revision"),
        updated_session=_date_value(item["updated_session"], "current availability"),
    )
    if item["current_id"] != result.current_id:
        raise I6PointerRuntimeError("current pointer ID does not reproduce")
    return result


def _gate_b_from_dict(value: object) -> GateBApproval:
    item = _mapping(value, "Gate B approval")
    _keys(
        item,
        {
            "approval_available_session",
            "approval_id",
            "approver_id",
            "authorized_action",
            "full_oracle_release_id",
            "literal_version",
            "receipt_id",
            "shadow_release_id",
            "spec_id",
        },
        "Gate B approval",
    )
    approval = GateBApproval(
        spec_id=_text(item["spec_id"], "Gate B spec ID"),
        receipt_id=_text(item["receipt_id"], "Gate B receipt ID"),
        shadow_release_id=_text(item["shadow_release_id"], "Gate B release ID"),
        full_oracle_release_id=_text(item["full_oracle_release_id"], "Gate B oracle release ID"),
        approver_id=_text(item["approver_id"], "Gate B approver"),
        approval_available_session=_date_value(
            item["approval_available_session"], "Gate B availability"
        ),
        authorized_action=_enum(GateBAction, item["authorized_action"], "Gate B action"),
        literal_version=_text(item["literal_version"], "Gate B literal version"),
    )
    if item["approval_id"] != approval.approval_id:
        raise I6PointerRuntimeError("Gate B approval ID does not reproduce")
    return approval


def _gate_c_from_dict(value: object) -> GateCApproval:
    item = _mapping(value, "Gate C approval")
    _keys(
        item,
        {
            "approval_available_session",
            "approval_id",
            "approver_id",
            "authorized_action",
            "expected_previous_pointer_event_id",
            "expected_previous_pointer_revision",
            "expected_previous_release_id",
            "gate_b_approval_id",
            "literal_version",
            "pointer_name",
            "rollback_receipt_id",
            "shadow_pointer_event_id",
            "target_pointer_revision",
            "target_release_id",
        },
        "Gate C approval",
    )
    approval = GateCApproval(
        gate_b_approval_id=_text(item["gate_b_approval_id"], "Gate C Gate B ID"),
        shadow_pointer_event_id=_text(item["shadow_pointer_event_id"], "Gate C shadow ID"),
        rollback_receipt_id=_text(item["rollback_receipt_id"], "Gate C rollback ID"),
        expected_previous_pointer_event_id=_text(
            item["expected_previous_pointer_event_id"], "Gate C prior event"
        ),
        expected_previous_release_id=_text(
            item["expected_previous_release_id"], "Gate C prior release"
        ),
        expected_previous_pointer_revision=_integer(
            item["expected_previous_pointer_revision"], "Gate C prior revision"
        ),
        target_pointer_revision=_integer(item["target_pointer_revision"], "Gate C target revision"),
        target_release_id=_text(item["target_release_id"], "Gate C target release"),
        approver_id=_text(item["approver_id"], "Gate C approver"),
        approval_available_session=_date_value(
            item["approval_available_session"], "Gate C availability"
        ),
        authorized_action=_enum(GateCAction, item["authorized_action"], "Gate C action"),
        literal_version=_text(item["literal_version"], "Gate C literal version"),
        pointer_name=_text(item["pointer_name"], "Gate C pointer"),
    )
    if item["approval_id"] != approval.approval_id:
        raise I6PointerRuntimeError("Gate C approval ID does not reproduce")
    return approval


def _lifecycle_event_from_dict(
    action: PointerAction, value: object
) -> ShadowPointerEvent | RollbackPointerEvent | TopPointerEvent:
    item = _mapping(value, "lifecycle event")
    if action is PointerAction.SHADOW_PUBLISH:
        return _shadow_event_from_dict(item)
    if action is PointerAction.SHADOW_ROLLBACK:
        return _rollback_event_from_dict(item)
    return _top_event_from_dict(item)


def _shadow_event_from_dict(item: Mapping[str, object]) -> ShadowPointerEvent:
    _keys(
        item,
        {
            "event_available_session",
            "event_id",
            "expected_previous_event_id",
            "gate_b_approval_artifact",
            "gate_b_approval_id",
            "new_release_id",
            "pointer_name",
            "pointer_revision",
            "previous_release_id",
            "rule_version",
        },
        "shadow event",
    )
    event = ShadowPointerEvent(
        gate_b_approval_id=_text(item["gate_b_approval_id"], "shadow Gate B ID"),
        gate_b_approval_artifact=_artifact_from_dict(
            item["gate_b_approval_artifact"], "shadow Gate B artifact"
        ),
        expected_previous_event_id=_optional_text(
            item["expected_previous_event_id"], "shadow prior event"
        ),
        previous_release_id=_optional_text(item["previous_release_id"], "shadow prior release"),
        new_release_id=_text(item["new_release_id"], "shadow new release"),
        pointer_revision=_integer(item["pointer_revision"], "shadow revision"),
        event_available_session=_date_value(item["event_available_session"], "shadow availability"),
        pointer_name=_text(item["pointer_name"], "shadow pointer"),
    )
    _literal(item["rule_version"], "s7_5_i6_shadow_pointer_event_v1", "shadow rule")
    if item["event_id"] != event.event_id:
        raise I6PointerRuntimeError("shadow event ID does not reproduce")
    return event


def _rollback_event_from_dict(item: Mapping[str, object]) -> RollbackPointerEvent:
    _keys(
        item,
        {
            "event_available_session",
            "event_id",
            "expected_previous_event_id",
            "forward_shadow_event_id",
            "new_release_id",
            "pointer_name",
            "pointer_revision",
            "previous_release_id",
            "rule_version",
        },
        "rollback event",
    )
    event = RollbackPointerEvent(
        forward_shadow_event_id=_text(item["forward_shadow_event_id"], "forward event"),
        expected_previous_event_id=_text(item["expected_previous_event_id"], "rollback prior"),
        previous_release_id=_text(item["previous_release_id"], "rollback old release"),
        new_release_id=_text(item["new_release_id"], "rollback new release"),
        pointer_revision=_integer(item["pointer_revision"], "rollback revision"),
        event_available_session=_date_value(
            item["event_available_session"], "rollback availability"
        ),
        pointer_name=_text(item["pointer_name"], "rollback pointer"),
    )
    _literal(item["rule_version"], "s7_5_i6_shadow_rollback_pointer_event_v1", "rollback rule")
    if item["event_id"] != event.event_id:
        raise I6PointerRuntimeError("rollback event ID does not reproduce")
    return event


def _top_event_from_dict(item: Mapping[str, object]) -> TopPointerEvent:
    _keys(
        item,
        {
            "event_available_session",
            "event_id",
            "expected_previous_event_id",
            "gate_c_approval_artifact",
            "gate_c_approval_id",
            "new_release_id",
            "pointer_name",
            "pointer_revision",
            "previous_release_id",
            "rule_version",
        },
        "top event",
    )
    event = TopPointerEvent(
        gate_c_approval_id=_text(item["gate_c_approval_id"], "top Gate C ID"),
        gate_c_approval_artifact=_artifact_from_dict(
            item["gate_c_approval_artifact"], "top Gate C artifact"
        ),
        expected_previous_event_id=_text(item["expected_previous_event_id"], "top prior event"),
        previous_release_id=_text(item["previous_release_id"], "top prior release"),
        new_release_id=_text(item["new_release_id"], "top new release"),
        pointer_revision=_integer(item["pointer_revision"], "top revision"),
        event_available_session=_date_value(item["event_available_session"], "top availability"),
        pointer_name=_text(item["pointer_name"], "top pointer"),
    )
    _literal(item["rule_version"], "s7_5_i6_research_top_pointer_event_v1", "top rule")
    if item["event_id"] != event.event_id:
        raise I6PointerRuntimeError("top event ID does not reproduce")
    return event


def _rollback_receipt_from_dict(value: object) -> RollbackReceipt:
    item = _mapping(value, "rollback receipt")
    _keys(
        item,
        {
            "deleted_artifact_count",
            "details_artifact",
            "parent_reader_after_digest",
            "parent_reader_before_digest",
            "receipt_available_session",
            "receipt_id",
            "rolled_back_release_id",
            "rollback_pointer_event_id",
            "selected_parent_release_id",
            "shadow_pointer_event_id",
            "surviving_artifact_set_digest",
        },
        "rollback receipt",
    )
    receipt = RollbackReceipt(
        shadow_pointer_event_id=_text(item["shadow_pointer_event_id"], "rollback shadow ID"),
        rollback_pointer_event_id=_text(item["rollback_pointer_event_id"], "rollback event ID"),
        rolled_back_release_id=_text(item["rolled_back_release_id"], "rolled release"),
        selected_parent_release_id=_text(item["selected_parent_release_id"], "rollback parent"),
        parent_reader_before_digest=_text(
            item["parent_reader_before_digest"], "rollback reader before"
        ),
        parent_reader_after_digest=_text(
            item["parent_reader_after_digest"], "rollback reader after"
        ),
        deleted_artifact_count=_integer(item["deleted_artifact_count"], "rollback deletion count"),
        surviving_artifact_set_digest=_text(
            item["surviving_artifact_set_digest"], "rollback survivors"
        ),
        details_artifact=_artifact_from_dict(item["details_artifact"], "rollback details"),
        receipt_available_session=_date_value(
            item["receipt_available_session"], "rollback availability"
        ),
    )
    if item["receipt_id"] != receipt.receipt_id:
        raise I6PointerRuntimeError("rollback receipt ID does not reproduce")
    return receipt


def _shadow_run_spec_from_dict(value: object) -> ShadowRunSpec:
    item = _mapping(value, "I5 shadow RunSpec")
    _keys(
        item,
        {
            "authority",
            "calendar_digest",
            "common_parent_release_id",
            "comparison_sessions",
            "full_oracle_completion_artifact",
            "full_oracle_release_id",
            "identity_policy_bundle_id",
            "incremental_completion_artifact",
            "incremental_deep_attestation_artifact",
            "incremental_release_id",
            "projection_policies",
            "receipt_available_session",
            "resource_policy",
            "rule_version",
            "run_spec_id",
            "schema_bundle_digest",
            "scope_artifact",
            "source_binding_digest",
            "transform_semantics_digest",
        },
        "I5 shadow RunSpec",
    )
    policy = _mapping(item["resource_policy"], "I5 resource policy")
    _keys(
        policy,
        {
            "max_chain_resolution_milliseconds",
            "max_peak_rss_bytes",
            "max_read_bytes",
            "max_wall_clock_seconds",
            "max_write_bytes",
            "min_free_disk_bytes",
            "policy_id",
            "rule_version",
        },
        "I5 resource policy",
    )
    _literal(policy["rule_version"], "s7_5_i5_resource_gate_v1", "resource rule")
    resource_policy = ResourceGatePolicy(
        max_wall_clock_seconds=_integer(policy["max_wall_clock_seconds"], "wall clock"),
        max_peak_rss_bytes=_integer(policy["max_peak_rss_bytes"], "RSS"),
        min_free_disk_bytes=_integer(policy["min_free_disk_bytes"], "disk floor"),
        max_read_bytes=_integer(policy["max_read_bytes"], "read bytes"),
        max_write_bytes=_integer(policy["max_write_bytes"], "write bytes"),
        max_chain_resolution_milliseconds=_integer(
            policy["max_chain_resolution_milliseconds"], "chain resolution"
        ),
    )
    if policy["policy_id"] != resource_policy.policy_id:
        raise I6PointerRuntimeError("I5 resource policy ID does not reproduce")
    spec = ShadowRunSpec(
        authority=_text(item["authority"], "I5 authority"),
        incremental_completion_artifact=_artifact_from_dict(
            item["incremental_completion_artifact"], "I5 incremental completion"
        ),
        incremental_deep_attestation_artifact=_artifact_from_dict(
            item["incremental_deep_attestation_artifact"], "I5 incremental deep"
        ),
        full_oracle_completion_artifact=_artifact_from_dict(
            item["full_oracle_completion_artifact"], "I5 oracle completion"
        ),
        incremental_release_id=_text(item["incremental_release_id"], "I5 release"),
        full_oracle_release_id=_text(item["full_oracle_release_id"], "I5 oracle release"),
        common_parent_release_id=_text(item["common_parent_release_id"], "I5 parent"),
        source_binding_digest=_text(item["source_binding_digest"], "I5 source binding"),
        schema_bundle_digest=_text(item["schema_bundle_digest"], "I5 schema bundle"),
        transform_semantics_digest=_text(
            item["transform_semantics_digest"], "I5 transform semantics"
        ),
        identity_policy_bundle_id=_text(item["identity_policy_bundle_id"], "I5 identity policy"),
        calendar_digest=_text(item["calendar_digest"], "I5 calendar"),
        scope_artifact=_artifact_from_dict(item["scope_artifact"], "I5 scope artifact"),
        comparison_sessions=tuple(
            _date_value(value, "I5 comparison session")
            for value in _array(item["comparison_sessions"], "I5 sessions")
        ),
        receipt_available_session=_date_value(item["receipt_available_session"], "I5 availability"),
        resource_policy=resource_policy,
    )
    # Projection policies are fully derived by ShadowRunSpec and may not be caller-selected.
    if (
        item["projection_policies"] != [entry.to_dict() for entry in spec.projection_policies]
        or item["run_spec_id"] != spec.run_spec_id
        or spec.scope_artifact != I5_SCOPE_ARTIFACT
        or item["rule_version"] != I5_SHADOW_RUN_SPEC_RULE_VERSION
    ):
        raise I6PointerRuntimeError("I5 shadow RunSpec does not reproduce")
    return spec


def _package_path(package: PointerActionPackage) -> str:
    return (
        f"{_PACKAGE_ROOT}/action={package.action.value}/"
        f"package_id={package.package_id}/package.json"
    )


def _event_path(pointer: str, event_id: str) -> str:
    _pointer(pointer)
    _digest(event_id, "event path ID")
    return f"{_EVENT_ROOT}/pointer={pointer}/event_id={event_id}/event.json"


def _revision_claim_path(pointer: str, revision: int) -> str:
    if pointer not in {SHADOW_POINTER_NAME, RESEARCH_POINTER_NAME}:
        raise I6PointerRuntimeError("revision claim pointer differs")
    _positive_int(revision, "revision claim revision")
    return f"{_REVISION_ROOT}/pointer={pointer}/revision={revision}/claim.json"


def _receipt_path(action: PointerAction, package_id: str) -> str:
    _digest(package_id, "receipt package ID")
    return f"{_RECEIPT_ROOT}/action={action.value}/package_id={package_id}/receipt.json"


def _current_path(pointer: str) -> str:
    _pointer(pointer)
    return f"{_POINTER_ROOT}/pointer={pointer}/current.json"


def _pointer_for_action(action: PointerAction) -> str:
    return (
        RESEARCH_POINTER_NAME if action is PointerAction.RESEARCH_CUTOVER else SHADOW_POINTER_NAME
    )


def _surviving_artifact_set_digest(binding: ReleaseChainBinding) -> str:
    return stable_digest(
        {
            "artifacts": sorted(
                [
                    binding.base.completion_artifact.to_dict(),
                    binding.base.deep_attestation_artifact.to_dict(),
                    binding.delta.completion_artifact.to_dict(),
                    binding.delta.deep_attestation_artifact.to_dict(),
                ],
                key=lambda item: str(item["path"]),
            ),
            "rule_version": "s7_5_i6_surviving_artifact_set_v1",
        }
    )


def _pointer(value: object) -> str:
    if value not in {SHADOW_POINTER_NAME, RESEARCH_POINTER_NAME}:
        raise I6PointerRuntimeError("pointer namespace is invalid")
    return str(value)


def _shadow_event(value: object) -> ShadowPointerEvent:
    if type(value) is not ShadowPointerEvent:
        raise I6PointerRuntimeError("expected a shadow pointer event")
    return value


def _rollback_event(value: object) -> RollbackPointerEvent:
    if type(value) is not RollbackPointerEvent:
        raise I6PointerRuntimeError("expected a rollback pointer event")
    return value


def _top_event(value: object) -> TopPointerEvent:
    if type(value) is not TopPointerEvent:
        raise I6PointerRuntimeError("expected a top pointer event")
    return value


def _root(value: Path) -> Path:
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise I6PointerRuntimeError("data root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir() or root.is_symlink():
        raise I6PointerRuntimeError("data root is missing or unsafe")
    status = os.stat(root, follow_symlinks=False)
    if (
        not stat.S_ISDIR(status.st_mode)
        or status.st_uid != os.geteuid()
        or stat.S_IMODE(status.st_mode) & 0o022
    ):
        raise I6PointerRuntimeError(
            "data root must be owner-controlled and deny group/world writes"
        )
    return root


def _validate_approval_acl(root: Path, pin: ArtifactPin) -> None:
    """Use the owner-only data-root ACL as the unsigned approval trust root."""

    try:
        path = safe_relative_path(root, pin.path)
    except ArtifactError as exc:
        raise I6PointerRuntimeError("approval path is outside the ACL trust root") from exc
    expected_uid = os.stat(root, follow_symlinks=False).st_uid
    current = root
    for part in path.relative_to(root).parts:
        current /= part
        try:
            status = os.stat(current, follow_symlinks=False)
        except OSError as exc:
            raise I6PointerRuntimeError("approval ACL path is missing") from exc
        if (
            stat.S_ISLNK(status.st_mode)
            or status.st_uid != expected_uid
            or stat.S_IMODE(status.st_mode) & 0o022
        ):
            raise I6PointerRuntimeError("approval path is outside the owner-only ACL trust root")
    if not stat.S_ISREG(status.st_mode) or status.st_nlink != 1:
        raise I6PointerRuntimeError("approval must be one owner-controlled regular file")


def _production_path(value: object, label: str) -> str:
    path = _relative(value, label)
    parts = set(PurePosixPath(path).parts)
    if parts.intersection(_FORBIDDEN_AUTHORITY_PARTS):
        raise I6PointerRuntimeError(f"{label} uses forbidden discovery/temporary path")
    return path


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or str(path) != text or text in {"", "."} or ".." in path.parts:
        raise I6PointerRuntimeError(f"{label} is not a canonical relative path")
    return text


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise I6PointerRuntimeError(f"{label} is not an ArtifactPin")
    _production_path(value.path, label)
    if value.bytes <= 0 or value.bytes > _MAX_CONTROL_BYTES:
        raise I6PointerRuntimeError(f"{label} exceeds pointer control bounds")
    return value


def _artifact_from_dict(value: object, label: str) -> ArtifactPin:
    item = _mapping(value, label)
    _keys(item, {"bytes", "path", "sha256"}, label)
    return ArtifactPin(
        path=_text(item["path"], f"{label} path"),
        sha256=_text(item["sha256"], f"{label} SHA"),
        bytes=_integer(item["bytes"], f"{label} bytes"),
    )


def _optional_artifact(value: object, label: str) -> ArtifactPin | None:
    return None if value is None else _artifact_from_dict(value, label)


def _closed_json(content: bytes, label: str) -> dict[str, object]:
    if not content or len(content) > _MAX_CONTROL_BYTES:
        raise I6PointerRuntimeError(f"{label} byte bounds are invalid")

    def pairs(entries: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in entries:
            if key in result:
                raise I6PointerRuntimeError(f"{label} contains a duplicate key")
            result[key] = value
        return result

    try:
        parsed = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                I6PointerRuntimeError(f"{label} contains {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I6PointerRuntimeError(f"{label} is not strict JSON") from exc
    if type(parsed) is not dict or _canonical_json_bytes(parsed) != content:
        raise I6PointerRuntimeError(f"{label} is not canonical JSON")
    return parsed


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
    except (TypeError, ValueError) as exc:
        raise I6PointerRuntimeError("control is not canonical-JSON serializable") from exc


def _mapping(value: object, label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise I6PointerRuntimeError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> tuple[object, ...]:
    if type(value) is not list:
        raise I6PointerRuntimeError(f"{label} must be an array")
    return tuple(value)


def _keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise I6PointerRuntimeError(f"{label} keys differ from the closed schema")


def _literal(value: object, expected: object, label: str) -> None:
    if value != expected:
        raise I6PointerRuntimeError(f"{label} differs")


def _text(value: object, label: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise I6PointerRuntimeError(f"{label} must be non-empty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise I6PointerRuntimeError(f"{label} must be an integer")
    return value


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise I6PointerRuntimeError(f"{label} must be a boolean")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise I6PointerRuntimeError(f"{label} must be a SHA-256 digest")
    return text


def _optional_digest(value: object, label: str) -> str | None:
    return None if value is None else _digest(value, label)


def _positive_int(value: object, label: str) -> int:
    integer = _integer(value, label)
    if integer <= 0:
        raise I6PointerRuntimeError(f"{label} must be positive")
    return integer


def _nonnegative_int(value: object, label: str) -> int:
    integer = _integer(value, label)
    if integer < 0:
        raise I6PointerRuntimeError(f"{label} must be nonnegative")
    return integer


def _date_value(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise I6PointerRuntimeError(f"{label} is not an ISO date") from exc
    if parsed.isoformat() != text:
        raise I6PointerRuntimeError(f"{label} is not canonical")
    return parsed


def _session(value: object, label: str) -> date:
    if not isinstance(value, date):
        raise I6PointerRuntimeError(f"{label} must be a date")
    return value


def _enum(enum_type, value: object, label: str):
    try:
        return enum_type(_text(value, label))
    except ValueError as exc:
        raise I6PointerRuntimeError(f"{label} is outside the closed domain") from exc


def _required(value, label: str):
    if value is None:
        raise I6PointerRuntimeError(f"{label} is required")
    return value


def _disk_free(root: Path) -> int:
    return os.statvfs(root).f_bavail * os.statvfs(root).f_frsize


def _reject_symlink_parents(root: Path, path: Path) -> None:
    current = root
    for part in path.relative_to(root).parts[:-1]:
        current /= part
        if current.is_symlink():
            raise I6PointerRuntimeError("pointer path crosses a symlink")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "CurrentExpectation",
    "CurrentPointer",
    "I6LostCompareAndSwap",
    "I6PointerRuntimeError",
    "PointerAction",
    "PointerActionPackage",
    "PointerPackageState",
    "PointerStageReceipt",
    "ReleaseAuthorityPin",
    "ReleaseChainBinding",
    "ResearchTopSnapshot",
    "ResolvedPointerView",
    "ResolvedRelease",
    "ResolvedReleaseChain",
    "initialize_research_parent",
    "load_research_top_snapshot_exact",
    "prepare_research_cutover",
    "prepare_shadow_publish",
    "prepare_shadow_rollback",
    "read_research_pointer",
    "read_shadow_pointer",
    "stage_research_cutover",
    "stage_shadow_publish",
    "stage_shadow_rollback",
    "verify_research_cutover",
    "verify_shadow_publish",
    "verify_shadow_rollback",
]
