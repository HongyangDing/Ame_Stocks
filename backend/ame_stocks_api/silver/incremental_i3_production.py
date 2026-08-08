"""Local-only production staging executor for S7.5 I3 native-v2 base/delta.

The materializer seam owns the expensive physical conversion.  This module
owns production controls: exact input authentication, one non-blocking lock,
resource hard guards, immutable control writes, Gate-A release construction,
failed receipts, an ``awaiting_review`` completion written last, and exact
post-write verification.  It has no publish, pointer, network, or cutover API.
"""

from __future__ import annotations

import fcntl
import hashlib
import math
import os
import resource
import shutil
import stat
import sys
import time
import weakref
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import date
from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver import incremental_i3_production_contract as contract
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    CheckpointReceipt,
    IncrementalReleaseManifest,
    PartitionReceipt,
    ReleaseType,
    RowVersionChangeIndexPin,
    RowVersionOperation,
    RowVersionReference,
    RunReceipt,
    RunSpec,
    ViewKind,
    checkpoint_rebuild_basis_from_change_digest,
    control_object_pin,
    input_set_digest,
    logical_change_set_digest,
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
    I3CheckpointState,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionBaseFkVerificationSummary,
    I3ProductionCompletion,
    I3ProductionDeepVerificationAttestation,
    I3ProductionOutputSet,
    I3ProductionResourceObservation,
    I3ProductionRunKind,
    I3ProductionRunReceipt,
    I3ProductionRunSpec,
    I3ProductionRunState,
    I3ProductionTableOutput,
    LoadedI3ProductionStaging,
    load_i3_production_deep_attestation_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
    load_i3_production_staging_exact,
    production_gate_a_input_pins,
    production_physical_index_digest,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    production_compact_base_row_validator_digest,
    production_delta_row_validator_digest,
)

_CONTROL_ROOT = "manifests/silver/identity/s7-5-native-v2-staging"
_DELTA_RUN_SPEC_ROOT = f"{_CONTROL_ROOT}/run-specs"
_LOCK_ROOT = "locks/silver/identity/s7-5-native-v2-staging"
_TEMP_ROOT = "tmp/silver-identity-s7-5-native-v2-staging"
_OUTPUT_ROOT = "silver/schema=v2/identity/native_v2_staging"
_INTERRUPTED_RETRY_OUTPUT_ROOT = "silver/schema=v2/identity/native_v2_interrupted_retry"
_DELTA_BOUNDARY_PARTITION_COUNT = 3
FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION: str = "failed_receipt_durable_before_completion"
I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION = "s7_5_i3_interrupted_retry_exercise_v1"
_PARTITION_QA_ID = "partition_session_calendar_contiguous"
_ROW_QA_ID = "row_semantic_proof_complete"
_PARTITION_QA_SEMANTICS = stable_digest(
    {"check_id": _PARTITION_QA_ID, "rule_version": "s7_5_i3_production_gate_a_v1"}
)
_ROW_QA_SEMANTICS = stable_digest(
    {"check_id": _ROW_QA_ID, "rule_version": "s7_5_i3_production_gate_a_v1"}
)


class I3ProductionStageError(RuntimeError):
    """Raised after fail-closed staging; may carry an immutable failed receipt."""

    def __init__(self, message: str, *, failed_receipt_pin: ArtifactPin | None = None) -> None:
        super().__init__(message)
        self.failed_receipt_pin = failed_receipt_pin


@dataclass(frozen=True, slots=True)
class I3ProductionPreparedMaterialization:
    """Physical candidate returned by a production migration/delta adapter.

    Every referenced artifact must already exist below ``data_root`` at its
    exact immutable pin.  The executor adds controls but never rewrites these
    data or checkpoint artifacts.
    """

    table_outputs: tuple[I3ProductionTableOutput, ...]
    native_manifest: NativeV2ReleaseManifest
    native_manifest_artifact: ArtifactPin
    checkpoint: I3CheckpointState
    checkpoint_artifact: ArtifactPin
    source_digest: str
    resource_observation: I3ProductionResourceObservation
    canonical_projection_difference_count: int
    row_versions: tuple[I3ProductionPreparedRowVersion, ...]


@dataclass(frozen=True, slots=True)
class I3ProductionPreparedRowVersion:
    """Adapter lineage for one physical row; executor owns the proof artifact."""

    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    operation: RowVersionOperation
    availability_session: date
    index_artifact: ArtifactPin
    row_locator: str
    row_payload_digest: str
    predecessor_payload_digest: str | None
    validator_semantics_digest: str


@runtime_checkable
class I3ProductionMaterializer(Protocol):
    """Seam implemented by compact base IO and bounded delta adapters.

    ``workspace`` is a durable, content-addressed staging root.  Adapters may
    use ``data_root/tmp`` for scratch, but no returned authoritative pin may
    reference that cleanup-eligible tree.
    """

    def prepare(
        self,
        *,
        data_root: Path,
        run_spec: I3ProductionRunSpec,
        parent: LoadedI3ProductionStaging | None,
        workspace: Path,
    ) -> I3ProductionPreparedMaterialization: ...


@dataclass(frozen=True, slots=True)
class I3ProductionStageResult:
    completion_pin: ArtifactPin
    deep_attestation_pin: ArtifactPin
    loaded: LoadedI3ProductionStaging
    reused: bool


class I3ProductionInterruptedRetryPending(I3ProductionStageError):
    """Expected first-process stop after the failed receipt becomes durable."""

    def __init__(
        self,
        *,
        phase_one_artifact: ArtifactPin,
        failed_receipt_artifact: ArtifactPin,
    ) -> None:
        super().__init__(
            "DELTA interrupted-retry phase one stopped after its failed receipt became durable",
            failed_receipt_pin=failed_receipt_artifact,
        )
        self.phase_one_artifact = phase_one_artifact
        self.failed_receipt_artifact = failed_receipt_artifact


@dataclass(frozen=True, slots=True)
class I3ProductionInterruptedRetryReceipt:
    """Durable proof that a failed DELTA attempt recovered without parent damage."""

    run_spec_id: str
    run_spec_artifact: ArtifactPin
    phase_one_artifact: ArtifactPin
    failed_receipt_id: str
    failed_receipt_artifact: ArtifactPin
    frozen_envelope_digest: str
    parent_reader_before_digest: str
    parent_reader_after_digest: str
    parent_artifact_set_digest: str
    parent_artifact_count: int
    deleted_artifact_count: int
    unpublished_visible_count: int
    completion_id: str
    completion_artifact: ArtifactPin
    deep_attestation_id: str
    deep_attestation_artifact: ArtifactPin
    fail_after: str = FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "interrupted-retry RunSpec ID"),
            (self.failed_receipt_id, "interrupted-retry failed-receipt ID"),
            (self.frozen_envelope_digest, "interrupted-retry frozen-envelope digest"),
            (self.parent_reader_before_digest, "interrupted-retry parent-before digest"),
            (self.parent_reader_after_digest, "interrupted-retry parent-after digest"),
            (self.parent_artifact_set_digest, "interrupted-retry parent-artifact digest"),
            (self.completion_id, "interrupted-retry completion ID"),
            (self.deep_attestation_id, "interrupted-retry deep-attestation ID"),
        ):
            _require_lower_sha256(value, label)
        for value, label in (
            (self.run_spec_artifact, "interrupted-retry RunSpec artifact"),
            (self.phase_one_artifact, "interrupted-retry phase-one artifact"),
            (self.failed_receipt_artifact, "interrupted-retry failed-receipt artifact"),
            (self.completion_artifact, "interrupted-retry completion artifact"),
            (self.deep_attestation_artifact, "interrupted-retry deep-attestation artifact"),
        ):
            if not isinstance(value, ArtifactPin):
                raise I3ProductionStageError(f"{label} is invalid")
        if self.fail_after != FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION:
            raise I3ProductionStageError("interrupted-retry failpoint is invalid")
        if (
            type(self.parent_artifact_count) is not int
            or self.parent_artifact_count <= 0
            or self.deleted_artifact_count != 0
            or self.unpublished_visible_count != 0
            or self.parent_reader_before_digest != self.parent_reader_after_digest
        ):
            raise I3ProductionStageError("interrupted-retry safety evidence is invalid")
        validate_production_delta_run_spec_artifact_path(
            self.run_spec_artifact,
            run_spec_id=self.run_spec_id,
        )

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i3_production_interrupted_retry_receipt",
            "completion_artifact": self.completion_artifact.to_dict(),
            "completion_id": self.completion_id,
            "deep_attestation_artifact": self.deep_attestation_artifact.to_dict(),
            "deep_attestation_id": self.deep_attestation_id,
            "deleted_artifact_count": self.deleted_artifact_count,
            "fail_after": self.fail_after,
            "failed_receipt_artifact": self.failed_receipt_artifact.to_dict(),
            "failed_receipt_id": self.failed_receipt_id,
            "frozen_envelope_digest": self.frozen_envelope_digest,
            "parent_artifact_count": self.parent_artifact_count,
            "parent_artifact_set_digest": self.parent_artifact_set_digest,
            "parent_reader_after_digest": self.parent_reader_after_digest,
            "parent_reader_before_digest": self.parent_reader_before_digest,
            "phase_one_artifact": self.phase_one_artifact.to_dict(),
            "publish_authorized": False,
            "rule_version": I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "unpublished_visible_count": self.unpublished_visible_count,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class I3ProductionInterruptedRetryResult:
    receipt: I3ProductionInterruptedRetryReceipt
    receipt_artifact: ArtifactPin
    stage_result: I3ProductionStageResult
    reused: bool


@dataclass(frozen=True, slots=True, weakref_slot=True)
class _I3ProductionInterruptionCapability:
    run_spec_id: str
    fail_after: str
    phase_one_id: str | None
    _seal: object = dataclass_field(default=None, init=False, repr=False, compare=False)


_INTERRUPTION_CAPABILITY_SEAL = object()
_ACTIVE_INTERRUPTION_CAPABILITIES: weakref.WeakValueDictionary[
    int, _I3ProductionInterruptionCapability
] = weakref.WeakValueDictionary()


@dataclass(frozen=True, slots=True)
class _InterruptedRetryPhaseOne:
    run_spec_id: str
    run_spec_artifact: ArtifactPin
    failed_receipt_id: str
    failed_receipt_artifact: ArtifactPin
    frozen_envelope_digest: str
    frozen_artifacts: tuple[ArtifactPin, ...]
    parent_reader_before_digest: str
    parent_reader_after_digest: str
    parent_artifact_set_digest: str
    parent_artifact_count: int
    deleted_artifact_count: int
    unpublished_visible_count: int
    fail_after: str = FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "phase-one RunSpec ID"),
            (self.failed_receipt_id, "phase-one failed-receipt ID"),
            (self.frozen_envelope_digest, "phase-one frozen-envelope digest"),
            (self.parent_reader_before_digest, "phase-one parent-before digest"),
            (self.parent_reader_after_digest, "phase-one parent-after digest"),
            (self.parent_artifact_set_digest, "phase-one parent-artifact digest"),
        ):
            _require_lower_sha256(value, label)
        if not isinstance(self.run_spec_artifact, ArtifactPin) or not isinstance(
            self.failed_receipt_artifact, ArtifactPin
        ):
            raise I3ProductionStageError("phase-one control artifact is invalid")
        if (
            type(self.frozen_artifacts) is not tuple
            or not self.frozen_artifacts
            or not all(isinstance(item, ArtifactPin) for item in self.frozen_artifacts)
            or tuple(item.path for item in self.frozen_artifacts)
            != tuple(sorted({item.path for item in self.frozen_artifacts}))
        ):
            raise I3ProductionStageError("phase-one frozen artifacts are invalid")
        if self.fail_after != FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION:
            raise I3ProductionStageError("phase-one failpoint is invalid")
        if (
            type(self.parent_artifact_count) is not int
            or self.parent_artifact_count <= 0
            or self.deleted_artifact_count != 0
            or self.unpublished_visible_count != 0
            or self.parent_reader_before_digest != self.parent_reader_after_digest
        ):
            raise I3ProductionStageError("phase-one safety evidence is invalid")
        validate_production_delta_run_spec_artifact_path(
            self.run_spec_artifact,
            run_spec_id=self.run_spec_id,
        )

    @property
    def phase_one_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i3_production_interrupted_retry_phase_one",
            "deleted_artifact_count": self.deleted_artifact_count,
            "fail_after": self.fail_after,
            "failed_receipt_artifact": self.failed_receipt_artifact.to_dict(),
            "failed_receipt_id": self.failed_receipt_id,
            "frozen_artifacts": [item.to_dict() for item in self.frozen_artifacts],
            "frozen_envelope_digest": self.frozen_envelope_digest,
            "parent_artifact_count": self.parent_artifact_count,
            "parent_artifact_set_digest": self.parent_artifact_set_digest,
            "parent_reader_after_digest": self.parent_reader_after_digest,
            "parent_reader_before_digest": self.parent_reader_before_digest,
            "rule_version": I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "unpublished_visible_count": self.unpublished_visible_count,
        }

    def to_dict(self) -> dict[str, object]:
        return {"phase_one_id": self.phase_one_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


def stage_i3_production_base(
    data_root: Path,
    run_spec_pin: ArtifactPin,
    *,
    materializer: I3ProductionMaterializer,
) -> I3ProductionStageResult:
    return stage_i3_production(
        data_root,
        run_spec_pin,
        expected_kind=I3ProductionRunKind.BASE,
        materializer=materializer,
    )


def stage_i3_production_delta(
    data_root: Path,
    run_spec_pin: ArtifactPin,
    *,
    materializer: I3ProductionMaterializer,
) -> I3ProductionStageResult:
    return stage_i3_production(
        data_root,
        run_spec_pin,
        expected_kind=I3ProductionRunKind.DELTA,
        materializer=materializer,
    )


def stage_i3_production(
    data_root: Path,
    run_spec_pin: ArtifactPin,
    *,
    expected_kind: I3ProductionRunKind,
    materializer: I3ProductionMaterializer,
) -> I3ProductionStageResult:
    """Stage one exact production base/delta and stop at ``awaiting_review``."""

    root = data_root.expanduser().resolve()
    if expected_kind is I3ProductionRunKind.DELTA:
        validate_production_delta_run_spec_artifact_path(run_spec_pin)
    _reject_temporary_authority_pin(run_spec_pin, "RunSpec")
    run_spec = load_i3_production_run_spec_exact(
        run_spec_pin, lambda relative: _read_control(root, relative)
    )
    if run_spec.run_kind is not expected_kind:
        raise I3ProductionStageError(
            f"requested {expected_kind.value} command received a {run_spec.run_kind.value} RunSpec"
        )
    if expected_kind is I3ProductionRunKind.DELTA:
        validate_production_delta_run_spec_artifact_path(
            run_spec_pin,
            run_spec_id=run_spec.run_spec_id,
        )
    if not isinstance(materializer, I3ProductionMaterializer):
        raise I3ProductionStageError("production materializer does not implement the sealed seam")
    completion_relative = _completion_relative(run_spec)
    lock_path = safe_relative_path(root, _lock_relative(run_spec))
    with _exclusive_lock(lock_path):
        started = time.monotonic()
        minimum_disk = shutil.disk_usage(root).free
        try:
            if safe_relative_path(root, completion_relative).exists():
                completion_pin = _pin_existing(root, completion_relative)
                loaded = verify_i3_production(
                    root,
                    completion_pin,
                    expected_kind=expected_kind,
                )
                deep_pin = _write_or_verify_deep_attestation(
                    root,
                    completion_pin,
                    loaded,
                    started=started,
                )
                return I3ProductionStageResult(
                    completion_pin=completion_pin,
                    deep_attestation_pin=deep_pin,
                    loaded=loaded,
                    reused=True,
                )
            _check_live_resources(root, run_spec)
            calendar_sessions = contract._verify_external_production_dependencies(root, run_spec)
            parent = contract._verify_production_parent_exact(root, run_spec)
            contract._verify_i2_receipts_exact(
                root,
                run_spec,
                calendar_sessions=calendar_sessions,
                parent_staging=parent,
            )
            workspace = safe_relative_path(root, _workspace_relative(run_spec))
            workspace.mkdir(parents=True, exist_ok=True)
            prepared = materializer.prepare(
                data_root=root,
                run_spec=run_spec,
                parent=parent,
                workspace=workspace,
            )
            _verify_prepared_materialization_authority(
                root,
                run_spec,
                prepared,
                parent=parent,
            )
            _validate_prepared(root, run_spec, prepared, parent=parent)
            observation = _combined_observation(
                root, prepared.resource_observation, started=started
            )
            minimum_disk = min(minimum_disk, observation.minimum_disk_free_bytes)
            observation.validate_caps(run_spec.resource_caps)
            result = _write_success_controls(
                root,
                run_spec,
                run_spec_pin,
                prepared,
                parent=parent,
                observation=observation,
            )
            loaded = verify_i3_production(root, result, expected_kind=expected_kind)
            deep_pin = _write_or_verify_deep_attestation(
                root,
                result,
                loaded,
                started=started,
            )
            return I3ProductionStageResult(
                completion_pin=result,
                deep_attestation_pin=deep_pin,
                loaded=loaded,
                reused=False,
            )
        except Exception as exc:
            if isinstance(exc, I3ProductionStageError) and exc.failed_receipt_pin is not None:
                raise
            failed_pin = _write_failed_receipt(
                root,
                run_spec,
                run_spec_pin,
                exc,
                started=started,
                minimum_disk_free_bytes=minimum_disk,
                failure_code_override=(
                    "completion_verification_failed_requires_new_run_spec"
                    if safe_relative_path(root, completion_relative).exists()
                    else None
                ),
            )
            qualifier = (
                "existing immutable completion failed verification; repair requires a new RunSpec: "
                if safe_relative_path(root, completion_relative).exists()
                else ""
            )
            raise I3ProductionStageError(
                f"{expected_kind.value} staging failed: {qualifier}{exc}",
                failed_receipt_pin=failed_pin,
            ) from exc


def exercise_i3_production_interrupted_retry(
    data_root: Path,
    run_spec_pin: ArtifactPin,
    *,
    fail_after: str = FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
) -> I3ProductionInterruptedRetryResult:
    """Exercise a real two-process DELTA interruption and exact normal retry.

    The first call always raises :class:`I3ProductionInterruptedRetryPending`
    after the failed receipt and phase-one metadata are immutable and before a
    completion/deep attestation exists.  A later call, including in a fresh
    process, exact-replays that evidence, independently rematerializes through
    the official DELTA adapter, compares a path-neutral frozen envelope digest,
    and invokes the ordinary staging entrypoint.  Calls after completion only
    exact-replay the durable exercise receipt.
    """

    if fail_after != FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION:
        raise I3ProductionStageError("interrupted-retry failpoint is not module-owned")
    root = data_root.expanduser().resolve()
    validate_production_delta_run_spec_artifact_path(run_spec_pin)
    run_spec = load_i3_production_run_spec_exact(
        run_spec_pin,
        lambda relative: _read_control(root, relative),
    )
    if run_spec.run_kind is not I3ProductionRunKind.DELTA:
        raise I3ProductionStageError("interrupted-retry harness requires a DELTA RunSpec")
    validate_production_delta_run_spec_artifact_path(
        run_spec_pin,
        run_spec_id=run_spec.run_spec_id,
    )
    phase_relative = _interrupted_retry_phase_one_relative(run_spec)
    receipt_relative = _interrupted_retry_receipt_relative(run_spec)
    phase_exists = safe_relative_path(root, phase_relative).exists()
    receipt_exists = safe_relative_path(root, receipt_relative).exists()
    completion_exists = safe_relative_path(root, _completion_relative(run_spec)).exists()
    deep_exists = safe_relative_path(root, _deep_attestation_relative(run_spec)).exists()
    if receipt_exists:
        if not phase_exists or not completion_exists or not deep_exists:
            raise I3ProductionStageError(
                "interrupted-retry receipt has an incomplete durable control chain"
            )
        return _replay_interrupted_retry_result(
            root,
            run_spec=run_spec,
            run_spec_pin=run_spec_pin,
            reused=True,
        )
    if not phase_exists and (completion_exists or deep_exists):
        raise I3ProductionStageError(
            "interrupted-retry cannot adopt an existing completion without phase-one evidence"
        )
    if phase_exists:
        return _resume_i3_production_interrupted_retry(
            root,
            run_spec=run_spec,
            run_spec_pin=run_spec_pin,
        )

    for relative in (
        _workspace_relative(run_spec),
        _interrupted_retry_workspace_relative(run_spec),
    ):
        path = safe_relative_path(root, relative)
        if path.exists() or path.is_symlink():
            raise I3ProductionStageError(
                "interrupted-retry requires absent clean phase-one and normal workspaces"
            )
    capability = _I3ProductionInterruptionCapability(
        run_spec_id=run_spec.run_spec_id,
        fail_after=fail_after,
        phase_one_id=None,
    )
    object.__setattr__(capability, "_seal", _INTERRUPTION_CAPABILITY_SEAL)
    _ACTIVE_INTERRUPTION_CAPABILITIES[id(capability)] = capability
    try:
        _execute_interrupted_retry_phase_one(
            root,
            run_spec=run_spec,
            run_spec_pin=run_spec_pin,
            capability=capability,
        )
    except _InjectedProductionInterruption as interruption:
        if interruption.capability is not capability:
            raise I3ProductionStageError(
                "interrupted-retry exception lost its module seal"
            ) from None
        raise I3ProductionInterruptedRetryPending(
            phase_one_artifact=interruption.phase_one_artifact,
            failed_receipt_artifact=interruption.failed_receipt_artifact,
        ) from None
    finally:
        _ACTIVE_INTERRUPTION_CAPABILITIES.pop(id(capability), None)
    raise I3ProductionStageError("interrupted-retry phase one did not interrupt")


class _InjectedProductionInterruption(RuntimeError):
    def __init__(
        self,
        *,
        capability: _I3ProductionInterruptionCapability,
        phase_one_artifact: ArtifactPin,
        failed_receipt_artifact: ArtifactPin,
    ) -> None:
        super().__init__(FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION)
        self.capability = capability
        self.phase_one_artifact = phase_one_artifact
        self.failed_receipt_artifact = failed_receipt_artifact


class _InterruptedRetryResumeMaterializer:
    def __init__(
        self,
        *,
        capability: _I3ProductionInterruptionCapability,
        delegate: I3ProductionMaterializer,
        phase_one: _InterruptedRetryPhaseOne,
    ) -> None:
        _require_interruption_capability(capability, phase_one_id=phase_one.phase_one_id)
        self._capability = capability
        self._delegate = delegate
        self._phase_one = phase_one
        self.prepared: I3ProductionPreparedMaterialization | None = None

    def prepare(
        self,
        *,
        data_root: Path,
        run_spec: I3ProductionRunSpec,
        parent: LoadedI3ProductionStaging | None,
        workspace: Path,
    ) -> I3ProductionPreparedMaterialization:
        _require_interruption_capability(
            self._capability,
            phase_one_id=self._phase_one.phase_one_id,
        )
        if (
            run_spec.run_spec_id != self._phase_one.run_spec_id
            or parent is None
            or _production_parent_evidence(parent)[0] != self._phase_one.parent_reader_before_digest
        ):
            raise I3ProductionStageError("interrupted-retry resume authority differs")
        prepared = self._delegate.prepare(
            data_root=data_root,
            run_spec=run_spec,
            parent=parent,
            workspace=workspace,
        )
        observed = _delta_frozen_envelope_digest(prepared, parent=parent)
        if observed != self._phase_one.frozen_envelope_digest:
            raise I3ProductionStageError(
                "interrupted-retry rematerialized envelope differs from phase one"
            )
        self.prepared = prepared
        return prepared


def _execute_interrupted_retry_phase_one(
    root: Path,
    *,
    run_spec: I3ProductionRunSpec,
    run_spec_pin: ArtifactPin,
    capability: _I3ProductionInterruptionCapability,
) -> None:
    _require_interruption_capability(capability, phase_one_id=None)
    from ame_stocks_api.silver.incremental_i3_delta_io import (
        load_production_delta_materializer,
    )

    lock_path = safe_relative_path(root, _lock_relative(run_spec))
    with _exclusive_lock(lock_path):
        for relative in (
            _completion_relative(run_spec),
            _deep_attestation_relative(run_spec),
            _interrupted_retry_phase_one_relative(run_spec),
            _interrupted_retry_receipt_relative(run_spec),
        ):
            path = safe_relative_path(root, relative)
            if path.exists() or path.is_symlink():
                raise I3ProductionStageError(
                    "interrupted-retry phase one found an existing durable result"
                )
        started = time.monotonic()
        minimum_disk = shutil.disk_usage(root).free
        _check_live_resources(root, run_spec)
        calendar_sessions = contract._verify_external_production_dependencies(root, run_spec)
        parent = contract._verify_production_parent_exact(root, run_spec)
        if parent is None:
            raise I3ProductionStageError("interrupted-retry DELTA lacks an exact parent")
        contract._verify_i2_receipts_exact(
            root,
            run_spec,
            calendar_sessions=calendar_sessions,
            parent_staging=parent,
        )
        parent_before, parent_set_digest, parent_count = _production_parent_evidence(parent)
        materializer = load_production_delta_materializer(
            data_root=root,
            run_spec=run_spec,
        )
        workspace = safe_relative_path(root, _interrupted_retry_workspace_relative(run_spec))
        workspace.mkdir(parents=True, exist_ok=False)
        prepared = materializer.prepare(
            data_root=root,
            run_spec=run_spec,
            parent=parent,
            workspace=workspace,
        )
        _verify_prepared_materialization_authority(
            root,
            run_spec,
            prepared,
            parent=parent,
        )
        _validate_prepared(root, run_spec, prepared, parent=parent)
        observation = _combined_observation(
            root,
            prepared.resource_observation,
            started=started,
        )
        observation.validate_caps(run_spec.resource_caps)
        minimum_disk = min(minimum_disk, observation.minimum_disk_free_bytes)
        frozen_digest = _delta_frozen_envelope_digest(prepared, parent=parent)
        frozen_artifacts = _delta_new_prepared_artifacts(prepared, parent=parent)
        reloaded_parent = contract._verify_production_parent_exact(root, run_spec)
        if reloaded_parent is None:  # pragma: no cover - DELTA invariant
            raise I3ProductionStageError("interrupted-retry parent disappeared")
        parent_after, after_set_digest, after_count = _production_parent_evidence(reloaded_parent)
        if (
            parent_after != parent_before
            or after_set_digest != parent_set_digest
            or after_count != parent_count
        ):
            raise I3ProductionStageError("interrupted-retry phase one changed its parent")
        detail_digest = _interruption_failure_detail_digest(run_spec, frozen_digest)
        failed_receipt = I3ProductionRunReceipt(
            run_spec_id=run_spec.run_spec_id,
            run_spec_artifact=run_spec_pin,
            state=I3ProductionRunState.FAILED,
            receipt_available_session=run_spec.run_available_session,
            resource_observation=I3ProductionResourceObservation(
                peak_rss_bytes=max(observation.peak_rss_bytes, _peak_rss_bytes()),
                elapsed_seconds=max(
                    observation.elapsed_seconds,
                    max(0, math.ceil(time.monotonic() - started)),
                ),
                minimum_disk_free_bytes=min(minimum_disk, shutil.disk_usage(root).free),
                temporary_bytes=observation.temporary_bytes,
            ),
            failure_code="interrupted_retry_injected",
            failure_detail_digest=detail_digest,
        )
        failed_relative = (
            f"{_run_root(run_spec)}/failed-receipts/receipt_id={failed_receipt.receipt_id}.json"
        )
        failed_pin = failed_receipt.exact_pin(path=failed_relative)
        phase_one = _InterruptedRetryPhaseOne(
            run_spec_id=run_spec.run_spec_id,
            run_spec_artifact=run_spec_pin,
            failed_receipt_id=failed_receipt.receipt_id,
            failed_receipt_artifact=failed_pin,
            frozen_envelope_digest=frozen_digest,
            frozen_artifacts=frozen_artifacts,
            parent_reader_before_digest=parent_before,
            parent_reader_after_digest=parent_after,
            parent_artifact_set_digest=parent_set_digest,
            parent_artifact_count=parent_count,
            deleted_artifact_count=0,
            unpublished_visible_count=0,
        )
        phase_relative = _interrupted_retry_phase_one_relative(run_spec)
        phase_pin = _write_immutable(root, phase_relative, phase_one.canonical_bytes())
        if phase_pin != phase_one.exact_pin(path=phase_relative):
            raise I3ProductionStageError("interrupted-retry phase-one bytes changed")
        observed_failed_pin = _write_immutable(
            root,
            failed_relative,
            failed_receipt.canonical_bytes(),
        )
        if observed_failed_pin != failed_pin:
            raise I3ProductionStageError("interrupted-retry failed-receipt bytes changed")
        _verify_interrupted_failed_receipt(
            root,
            run_spec=run_spec,
            phase_one=phase_one,
        )
        if any(
            safe_relative_path(root, relative).exists()
            or safe_relative_path(root, relative).is_symlink()
            for relative in (
                _completion_relative(run_spec),
                _deep_attestation_relative(run_spec),
            )
        ):
            raise I3ProductionStageError(
                "interrupted-retry phase one exposed a completion before interruption"
            )
        raise _InjectedProductionInterruption(
            capability=capability,
            phase_one_artifact=phase_pin,
            failed_receipt_artifact=failed_pin,
        )


def _resume_i3_production_interrupted_retry(
    root: Path,
    *,
    run_spec: I3ProductionRunSpec,
    run_spec_pin: ArtifactPin,
) -> I3ProductionInterruptedRetryResult:
    from ame_stocks_api.silver.incremental_i3_delta_io import (
        load_production_delta_materializer,
    )

    phase_one, phase_pin = _load_interrupted_retry_phase_one(root, run_spec)
    if phase_one.run_spec_artifact != run_spec_pin:
        raise I3ProductionStageError("interrupted-retry phase one names another RunSpec pin")
    _verify_interrupted_failed_receipt(root, run_spec=run_spec, phase_one=phase_one)
    for artifact in phase_one.frozen_artifacts:
        _verify_file(root, artifact)
    parent = contract._verify_production_parent_exact(root, run_spec)
    if parent is None:
        raise I3ProductionStageError("interrupted-retry resume lacks its exact parent")
    parent_before, parent_set_digest, parent_count = _production_parent_evidence(parent)
    if (
        parent_before != phase_one.parent_reader_before_digest
        or parent_set_digest != phase_one.parent_artifact_set_digest
        or parent_count != phase_one.parent_artifact_count
    ):
        raise I3ProductionStageError("interrupted-retry resume parent differs from phase one")
    completion_exists = safe_relative_path(root, _completion_relative(run_spec)).exists()
    if not completion_exists:
        normal_workspace = safe_relative_path(root, _workspace_relative(run_spec))
        if normal_workspace.exists() or normal_workspace.is_symlink():
            raise I3ProductionStageError(
                "interrupted-retry normal retry workspace is partial or foreign"
            )
    delegate = load_production_delta_materializer(data_root=root, run_spec=run_spec)
    capability = _I3ProductionInterruptionCapability(
        run_spec_id=run_spec.run_spec_id,
        fail_after=FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
        phase_one_id=phase_one.phase_one_id,
    )
    object.__setattr__(capability, "_seal", _INTERRUPTION_CAPABILITY_SEAL)
    _ACTIVE_INTERRUPTION_CAPABILITIES[id(capability)] = capability
    wrapper = _InterruptedRetryResumeMaterializer(
        capability=capability,
        delegate=delegate,
        phase_one=phase_one,
    )
    try:
        stage_result = stage_i3_production_delta(
            root,
            run_spec_pin,
            materializer=wrapper,
        )
    finally:
        _ACTIVE_INTERRUPTION_CAPABILITIES.pop(id(capability), None)
    observed_digest = _delta_frozen_envelope_digest_from_loaded(
        stage_result.loaded,
        parent=parent,
    )
    if observed_digest != phase_one.frozen_envelope_digest:
        raise I3ProductionStageError("interrupted-retry completed envelope differs from phase one")
    reloaded_parent = contract._verify_production_parent_exact(root, run_spec)
    if reloaded_parent is None:  # pragma: no cover
        raise I3ProductionStageError("interrupted-retry parent disappeared after retry")
    parent_after, after_set_digest, after_count = _production_parent_evidence(reloaded_parent)
    if (
        parent_after != parent_before
        or after_set_digest != parent_set_digest
        or after_count != parent_count
    ):
        raise I3ProductionStageError("interrupted-retry normal retry changed its parent")
    deep = load_i3_production_deep_attestation_exact(
        stage_result.deep_attestation_pin,
        lambda relative: _read_control(root, relative),
    )
    receipt = I3ProductionInterruptedRetryReceipt(
        run_spec_id=run_spec.run_spec_id,
        run_spec_artifact=run_spec_pin,
        phase_one_artifact=phase_pin,
        failed_receipt_id=phase_one.failed_receipt_id,
        failed_receipt_artifact=phase_one.failed_receipt_artifact,
        frozen_envelope_digest=phase_one.frozen_envelope_digest,
        parent_reader_before_digest=parent_before,
        parent_reader_after_digest=parent_after,
        parent_artifact_set_digest=parent_set_digest,
        parent_artifact_count=parent_count,
        deleted_artifact_count=0,
        unpublished_visible_count=phase_one.unpublished_visible_count,
        completion_id=stage_result.loaded.completion.completion_id,
        completion_artifact=stage_result.completion_pin,
        deep_attestation_id=deep.deep_attestation_id,
        deep_attestation_artifact=stage_result.deep_attestation_pin,
    )
    receipt_relative = _interrupted_retry_receipt_relative(run_spec)
    receipt_pin = _write_immutable(root, receipt_relative, receipt.canonical_bytes())
    if receipt_pin != receipt.exact_pin(path=receipt_relative):
        raise I3ProductionStageError("interrupted-retry receipt bytes changed")
    return _replay_interrupted_retry_result(
        root,
        run_spec=run_spec,
        run_spec_pin=run_spec_pin,
        reused=False,
        stage_result=stage_result,
    )


def _replay_interrupted_retry_result(
    root: Path,
    *,
    run_spec: I3ProductionRunSpec,
    run_spec_pin: ArtifactPin,
    reused: bool,
    stage_result: I3ProductionStageResult | None = None,
) -> I3ProductionInterruptedRetryResult:
    phase_one, phase_pin = _load_interrupted_retry_phase_one(root, run_spec)
    receipt, receipt_pin = _load_interrupted_retry_receipt(root, run_spec)
    if (
        phase_one.run_spec_artifact != run_spec_pin
        or receipt.run_spec_artifact != run_spec_pin
        or receipt.phase_one_artifact != phase_pin
        or receipt.failed_receipt_id != phase_one.failed_receipt_id
        or receipt.failed_receipt_artifact != phase_one.failed_receipt_artifact
        or receipt.frozen_envelope_digest != phase_one.frozen_envelope_digest
    ):
        raise I3ProductionStageError("interrupted-retry durable controls do not reconcile")
    _verify_interrupted_failed_receipt(root, run_spec=run_spec, phase_one=phase_one)
    for artifact in phase_one.frozen_artifacts:
        _verify_file(root, artifact)
    loaded = verify_i3_production_deep_attestation(
        root,
        receipt.completion_artifact,
        receipt.deep_attestation_artifact,
        expected_kind=I3ProductionRunKind.DELTA,
    )
    deep = load_i3_production_deep_attestation_exact(
        receipt.deep_attestation_artifact,
        lambda relative: _read_control(root, relative),
    )
    parent = contract._verify_production_parent_exact(root, run_spec)
    if parent is None:
        raise I3ProductionStageError("interrupted-retry replay lacks its exact parent")
    parent_digest, parent_set_digest, parent_count = _production_parent_evidence(parent)
    if (
        receipt.completion_id != loaded.completion.completion_id
        or receipt.deep_attestation_id != deep.deep_attestation_id
        or receipt.parent_reader_before_digest != parent_digest
        or receipt.parent_reader_after_digest != parent_digest
        or receipt.parent_artifact_set_digest != parent_set_digest
        or receipt.parent_artifact_count != parent_count
        or receipt.frozen_envelope_digest
        != _delta_frozen_envelope_digest_from_loaded(loaded, parent=parent)
    ):
        raise I3ProductionStageError("interrupted-retry exact replay differs")
    if stage_result is None:
        stage_result = I3ProductionStageResult(
            completion_pin=receipt.completion_artifact,
            deep_attestation_pin=receipt.deep_attestation_artifact,
            loaded=loaded,
            reused=True,
        )
    return I3ProductionInterruptedRetryResult(
        receipt=receipt,
        receipt_artifact=receipt_pin,
        stage_result=stage_result,
        reused=reused,
    )


def _verify_prepared_materialization_authority(
    root: Path,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging | None,
) -> None:
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        from ame_stocks_api.silver.incremental_i3_migration_io import (
            verify_compact_base_materialization_attestation,
        )

        verify_compact_base_materialization_attestation(
            data_root=root,
            run_spec=run_spec,
            prepared=prepared,
        )
        return
    try:
        from ame_stocks_api.silver.incremental_i3_delta_io import (
            verify_delta_materialization_attestation,
        )
    except ModuleNotFoundError as exc:
        if exc.name != "ame_stocks_api.silver.incremental_i3_delta_io":
            raise
        raise I3ProductionStageError(
            "production delta materialization attestation verifier is unavailable"
        ) from exc
    verify_delta_materialization_attestation(
        data_root=root,
        run_spec=run_spec,
        parent=parent,
        prepared=prepared,
    )


def verify_i3_production_base(
    data_root: Path, completion_pin: ArtifactPin
) -> LoadedI3ProductionStaging:
    return verify_i3_production(data_root, completion_pin, expected_kind=I3ProductionRunKind.BASE)


def verify_i3_production_delta(
    data_root: Path, completion_pin: ArtifactPin
) -> LoadedI3ProductionStaging:
    return verify_i3_production(data_root, completion_pin, expected_kind=I3ProductionRunKind.DELTA)


def verify_i3_production(
    data_root: Path,
    completion_pin: ArtifactPin,
    *,
    expected_kind: I3ProductionRunKind,
) -> LoadedI3ProductionStaging:
    """Idempotently exact-verify staging; no files or pointers are written."""

    _reject_temporary_authority_pin(completion_pin, "completion")
    loaded = load_i3_production_staging_exact(data_root, completion_pin)
    if loaded.run_spec.run_kind is not expected_kind:
        raise I3ProductionStageError(
            f"expected {expected_kind.value} completion, found {loaded.run_spec.run_kind.value}"
        )
    return loaded


def verify_i3_production_deep_attestation(
    data_root: Path,
    completion_pin: ArtifactPin,
    deep_attestation_pin: ArtifactPin,
    *,
    expected_kind: I3ProductionRunKind,
) -> LoadedI3ProductionStaging:
    """Deep-replay staging and require its immutable attestation to reproduce."""

    root = data_root.expanduser().resolve()
    _reject_temporary_authority_pin(completion_pin, "completion")
    _reject_temporary_authority_pin(deep_attestation_pin, "deep attestation")
    loaded = verify_i3_production(root, completion_pin, expected_kind=expected_kind)
    _verify_deep_attestation(root, completion_pin, deep_attestation_pin, loaded)
    return loaded


def _write_or_verify_deep_attestation(
    root: Path,
    completion_pin: ArtifactPin,
    loaded: LoadedI3ProductionStaging,
    *,
    started: float,
) -> ArtifactPin:
    relative = _deep_attestation_relative(loaded.run_spec)
    if safe_relative_path(root, relative).exists():
        pin = _pin_existing(root, relative)
        _verify_deep_attestation(root, completion_pin, pin, loaded)
        _check_live_resources(root, loaded.run_spec)
        return pin
    observation = _combined_observation(
        root,
        loaded.receipt.resource_observation,
        started=started,
    )
    observation.validate_caps(loaded.run_spec.resource_caps)
    expected = _expected_deep_attestation(
        root,
        completion_pin,
        loaded,
        verification_observation=observation,
    )
    pin = _write_immutable(root, relative, expected.canonical_bytes())
    _verify_deep_attestation(root, completion_pin, pin, loaded)
    return pin


def _verify_deep_attestation(
    root: Path,
    completion_pin: ArtifactPin,
    deep_attestation_pin: ArtifactPin,
    loaded: LoadedI3ProductionStaging,
) -> I3ProductionDeepVerificationAttestation:
    _reject_temporary_authority_pin(completion_pin, "completion")
    _reject_temporary_authority_pin(deep_attestation_pin, "deep attestation")
    observed = load_i3_production_deep_attestation_exact(
        deep_attestation_pin, lambda relative: _read_control(root, relative)
    )
    observed.verification_resource_observation.validate_caps(loaded.run_spec.resource_caps)
    expected = _expected_deep_attestation(
        root,
        completion_pin,
        loaded,
        verification_observation=observed.verification_resource_observation,
    )
    if observed != expected or deep_attestation_pin != expected.exact_pin(
        path=deep_attestation_pin.path
    ):
        raise I3ProductionStageError(
            "deep-verification attestation differs from exact staging bytes"
        )
    return observed


def _expected_deep_attestation(
    root: Path,
    completion_pin: ArtifactPin,
    loaded: LoadedI3ProductionStaging,
    *,
    verification_observation: I3ProductionResourceObservation,
) -> I3ProductionDeepVerificationAttestation:
    output_set = loaded.receipt.output_set
    if output_set is None:  # pragma: no cover - loaded successful invariant
        raise I3ProductionStageError("successful staging lost its OutputSet")
    row_digest = loaded.gate_a_release.candidate.row_semantic_attestation_digest
    if row_digest is None:
        raise I3ProductionStageError("deep staging lacks row-semantic attestation")
    parent_frontier_digest: str | None = None
    if loaded.run_spec.run_kind is I3ProductionRunKind.DELTA:
        parent_pin = loaded.run_spec.parent_deep_attestation_artifact
        if parent_pin is None:  # pragma: no cover - RunSpec invariant
            raise I3ProductionStageError("delta lacks its parent deep attestation")
        parent_deep = load_i3_production_deep_attestation_exact(
            parent_pin, lambda relative: _read_control(root, relative)
        )
        parent_frontier_digest = parent_deep.deep_attestation_id
    return I3ProductionDeepVerificationAttestation(
        completion_id=loaded.completion.completion_id,
        completion_artifact=completion_pin,
        gate_a_manifest_pin=output_set.gate_a_manifest_pin,
        native_v2_release=NativeV2ParentReleasePin.from_manifest(
            loaded.manifest, path=output_set.release_manifest_artifact.path
        ),
        checkpoint_id=output_set.checkpoint_id,
        checkpoint_artifact=output_set.checkpoint_artifact,
        output_set_id=output_set.output_set_id,
        row_semantic_attestation_digest=row_digest,
        terminal_state_digest=loaded.terminal_state_digest,
        physical_index_digest=production_physical_index_digest(output_set),
        parent_frontier_attestation_digest=parent_frontier_digest,
        attestation_available_session=loaded.completion.completion_available_session,
        verification_resource_observation=verification_observation,
    )


def _write_success_controls(
    root: Path,
    run_spec: I3ProductionRunSpec,
    run_spec_pin: ArtifactPin,
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging | None,
    observation: I3ProductionResourceObservation,
) -> ArtifactPin:
    partitions, base_fk_summary = _gate_a_partition_changes(root, run_spec, prepared)
    row_change_index, row_attestation_digest = _gate_a_row_version_change_index(
        root, run_spec, prepared, parent=parent
    )
    change_digest = logical_change_set_digest(
        added_partition_receipts=partitions,
        partition_replacements=(),
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
        row_version_change_index=row_change_index,
    )
    gate_policy = _gate_a_qa_policy()
    gate_inputs = production_gate_a_input_pins(run_spec)
    gate_spec = RunSpec(
        release_type=ReleaseType(run_spec.run_kind.value),
        parent_release_pin=run_spec.parent_gate_a_manifest,
        parent_identity_policy_bundle_id=(
            None if parent is None else parent.gate_a_manifest.identity_policy_bundle_id
        ),
        resolved_view=ViewKind.LATEST_REVIEWED_RESEARCH,
        source_binding_digest=input_set_digest(gate_inputs),
        source_cutoff_session=run_spec.source_cutoff_session,
        availability_cutoff_session=run_spec.run_available_session,
        release_available_session=run_spec.run_available_session,
        schema_digest=I3_V2_SCHEMA_BUNDLE_DIGEST,
        transform_semantics_digest=run_spec.transform_semantics_digest,
        identity_policy_bundle_id=run_spec.identity_policy_bundle.identity_policy_bundle_id,
        calendar_digest=run_spec.calendar.calendar_artifact_id,
        input_pins=gate_inputs,
        expected_change_set_digest=change_digest,
        qa_policy=gate_policy,
        correction_scope_digest=None,
        correction_authorization=None,
        rss_cap_bytes=run_spec.resource_caps.rss_bytes_hard_cap,
        disk_floor_bytes=run_spec.resource_caps.disk_free_bytes_hard_floor,
        wall_clock_cap_seconds=None,
    )
    gate_spec_path = _control_relative(run_spec, "gate-a-run-spec.json")
    gate_spec_pin = control_object_pin(gate_spec, path=gate_spec_path)
    _write_immutable(root, gate_spec_path, _canonical_json_bytes(gate_spec.to_dict()))

    qa_details = {
        "canonical_projection_difference_count": (prepared.canonical_projection_difference_count),
        "native_v2_manifest_artifact": prepared.native_manifest_artifact.to_dict(),
        "physical_source_digest": prepared.source_digest,
        "row_semantic_attestation_digest": row_attestation_digest,
        "base_fk_verification_summary": (
            None if base_fk_summary is None else base_fk_summary.to_dict()
        ),
        "rule_version": "s7_5_i3_production_gate_a_qa_details_v1",
        "run_spec_id": gate_spec.run_spec_id,
        "table_outputs": [item.to_dict() for item in prepared.table_outputs],
    }
    qa_details_path = _control_relative(run_spec, "gate-a-qa-details.json")
    qa_details_pin = _write_immutable(root, qa_details_path, _canonical_json_bytes(qa_details))
    gate_qa = QaReceipt(
        qa_policy_id=gate_policy.qa_policy_id,
        run_spec_id=gate_spec.run_spec_id,
        source_binding_digest=gate_spec.source_binding_digest,
        change_set_digest=change_digest,
        qa_available_session=run_spec.run_available_session,
        results=(
            QaCheckResult(
                check_id=_PARTITION_QA_ID,
                semantics_digest=_PARTITION_QA_SEMANTICS,
                observed_count=len(partitions),
                failure_count=0,
                details_artifact=GateArtifactPin(**qa_details_pin.to_dict()),
            ),
            QaCheckResult(
                check_id=_ROW_QA_ID,
                semantics_digest=_ROW_QA_SEMANTICS,
                observed_count=row_change_index.row_count,
                failure_count=0,
                details_artifact=GateArtifactPin(**qa_details_pin.to_dict()),
            ),
        ),
    )
    checkpoint_receipt = CheckpointReceipt(
        artifact=prepared.checkpoint_artifact,
        parent_release_id=(
            None
            if run_spec.parent_gate_a_manifest is None
            else run_spec.parent_gate_a_manifest.release_id
        ),
        run_spec_id=gate_spec.run_spec_id,
        last_session=run_spec.terminal_session,
        resolved_content_digest=prepared.checkpoint.resolved_content_digest,
        rebuild_basis_digest=checkpoint_rebuild_basis_from_change_digest(
            gate_spec, change_set_digest=change_digest
        ),
    )
    gate_receipt = RunReceipt(
        run_spec_id=gate_spec.run_spec_id,
        actual_input_set_digest=gate_spec.source_binding_digest,
        output_set_digest=change_digest,
        qa_receipt=gate_qa,
        checkpoint=checkpoint_receipt,
        succeeded=True,
        error_codes=(),
        receipt_available_session=run_spec.run_available_session,
        runtime_seconds=observation.elapsed_seconds,
        peak_rss_bytes=observation.peak_rss_bytes,
        minimum_free_disk_bytes=observation.minimum_disk_free_bytes,
    )
    gate_receipt_path = _control_relative(run_spec, "gate-a-run-receipt.json")
    gate_receipt_pin = control_object_pin(gate_receipt, path=gate_receipt_path)
    _write_immutable(root, gate_receipt_path, _canonical_json_bytes(gate_receipt.to_dict()))

    gate_manifest = IncrementalReleaseManifest(
        release_type=gate_spec.release_type,
        parent_release_pin=gate_spec.parent_release_pin,
        resolved_view=gate_spec.resolved_view,
        schema_digest=gate_spec.schema_digest,
        transform_semantics_digest=gate_spec.transform_semantics_digest,
        identity_policy_bundle_id=gate_spec.identity_policy_bundle_id,
        calendar_digest=gate_spec.calendar_digest,
        source_binding_digest=gate_spec.source_binding_digest,
        source_cutoff_session=gate_spec.source_cutoff_session,
        availability_cutoff_session=gate_spec.availability_cutoff_session,
        release_available_session=gate_spec.release_available_session,
        added_partition_receipts=partitions,
        partition_replacements=(),
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
        resolved_content_digest=prepared.checkpoint.resolved_content_digest,
        qa_policy_id=gate_policy.qa_policy_id,
        qa_receipt_id=gate_qa.qa_receipt_id,
        correction_authorization_id=None,
        run_spec_pin=gate_spec_pin,
        run_receipt_pin=gate_receipt_pin,
        row_version_change_index=row_change_index,
    )
    gate_manifest_path = _control_relative(run_spec, "gate-a-release-manifest.json")
    gate_manifest_pin = gate_manifest.exact_pin(manifest_path=gate_manifest_path)
    _write_immutable(root, gate_manifest_path, gate_manifest.canonical_bytes())

    output_set = I3ProductionOutputSet(
        release_manifest_artifact=prepared.native_manifest_artifact,
        checkpoint_artifact=prepared.checkpoint_artifact,
        release_id=prepared.native_manifest.release_id,
        checkpoint_id=prepared.checkpoint.checkpoint_id,
        resolved_state_digest=prepared.checkpoint.resolved_state_digest,
        resolved_content_digest=prepared.checkpoint.resolved_content_digest,
        table_outputs=prepared.table_outputs,
        gate_a_run_spec_pin=gate_spec_pin,
        gate_a_run_receipt_pin=gate_receipt_pin,
        gate_a_manifest_pin=gate_manifest_pin,
        control_extension_artifacts=tuple(
            sorted(
                (
                    qa_details_pin,
                    row_change_index.artifact,
                    *((base_fk_summary,) if base_fk_summary is not None else ()),
                ),
                key=lambda item: item.path,
            )
        ),
    )
    if output_set.total_output_bytes > run_spec.resource_caps.output_bytes_hard_cap:
        raise I3ProductionStageError("physical/control output bytes exceed the RunSpec hard cap")
    if output_set.total_rows > run_spec.resource_caps.output_rows_hard_cap:
        raise I3ProductionStageError("physical output rows exceed the RunSpec hard cap")
    receipt = I3ProductionRunReceipt(
        run_spec_id=run_spec.run_spec_id,
        run_spec_artifact=run_spec_pin,
        state=I3ProductionRunState.SUCCEEDED,
        receipt_available_session=run_spec.run_available_session,
        resource_observation=observation,
        output_set=output_set,
    )
    receipt_path = _control_relative(run_spec, "production-run-receipt.json")
    receipt_pin = _write_immutable(root, receipt_path, receipt.canonical_bytes())
    if receipt_pin != receipt.exact_pin(path=receipt_path):
        raise I3ProductionStageError("production receipt immutable write changed its bytes")
    completion = I3ProductionCompletion(
        run_spec_id=run_spec.run_spec_id,
        receipt_id=receipt.receipt_id,
        receipt_artifact=receipt_pin,
        output_set_id=output_set.output_set_id,
        release_id=gate_manifest.release_id,
        native_v2_envelope_id=prepared.native_manifest.release_id,
        checkpoint_id=prepared.checkpoint.checkpoint_id,
        completion_available_session=run_spec.run_available_session,
    )
    completion_path = _completion_relative(run_spec)
    completion_pin = _write_immutable(root, completion_path, completion.canonical_bytes())
    if completion_pin != completion.exact_pin(path=completion_path):
        raise I3ProductionStageError("completion immutable write changed its bytes")
    return completion_pin


def _gate_a_partition_changes(
    root: Path,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
) -> tuple[tuple[PartitionReceipt, ...], ArtifactPin | None]:
    universe = prepared.table_outputs[I3_V2_TABLE_ORDER.index("universe_daily")]
    if universe.dataset_index is None:
        raise I3ProductionStageError("prepared universe output has no dataset index")
    physical = (
        universe.dataset_index.partitions
        if run_spec.run_kind is I3ProductionRunKind.BASE
        else universe.dataset_index.partitions[-1:]
    )
    receipts: list[PartitionReceipt] = []
    session_reference_digests: list[tuple[date, str]] = []
    rows_checked = 0
    available = {
        (item.table_name, item.row_version_id) for item in prepared.checkpoint.terminal_row_versions
    }
    for item in physical:
        _verify_file(root, item.artifact)
        path = safe_relative_path(root, item.artifact.path)
        columns = pq.read_table(
            path,
            columns=[
                "alias_resolution_version_id",
                "asset_master_version_id",
                "issuer_master_version_id",
            ],
        )
        references: set[tuple[str, str]] = set()
        for field, table_name in (
            ("alias_resolution_version_id", "ticker_alias"),
            ("asset_master_version_id", "asset_master"),
            ("issuer_master_version_id", "issuer_master"),
        ):
            references.update(
                (table_name, str(value))
                for value in columns[field].to_pylist()
                if value is not None
            )
        typed_references = tuple(
            RowVersionReference(table_name=table, row_version_id=version)
            for table, version in sorted(references)
        )
        if references - available:
            raise I3ProductionStageError(
                "universe partition references a row version absent from the checkpoint"
            )
        if columns.num_rows != item.row_count:
            raise I3ProductionStageError(
                "universe FK scan row count differs from its exact partition pin"
            )
        rows_checked += columns.num_rows
        receipts.append(
            PartitionReceipt(
                table_name="universe_daily",
                partition_key=item.session_date.isoformat(),
                receipt=item.artifact,
                row_count=item.row_count,
                schema_digest=item.schema_digest,
                availability_session=item.availability_session,
                row_version_references=(
                    () if run_spec.run_kind is I3ProductionRunKind.BASE else typed_references
                ),
            )
        )
        if run_spec.run_kind is I3ProductionRunKind.BASE:
            session_reference_digests.append(
                (
                    item.session_date,
                    contract._base_fk_session_reference_digest(
                        item.session_date,
                        typed_references,
                    ),
                )
            )
    if run_spec.run_kind is I3ProductionRunKind.DELTA:
        return tuple(receipts), None
    summary = I3ProductionBaseFkVerificationSummary(
        session_count=len(physical),
        rows_checked=rows_checked,
        input_partition_set_digest=contract._base_fk_partition_set_digest(physical),
        logical_reference_digest=contract._base_fk_logical_reference_digest(
            session_reference_digests
        ),
        summary_available_session=run_spec.run_available_session,
    )
    summary_pin = _write_immutable(
        root,
        f"{_run_root(run_spec)}/base-fk-verification-summary.json",
        summary.canonical_bytes(),
    )
    return tuple(receipts), summary_pin


def _gate_a_qa_policy() -> QaPolicy:
    return QaPolicy(
        checks=(
            QaCheckPolicy(
                check_id=_PARTITION_QA_ID,
                severity=QaSeverity.HIGH,
                semantics_digest=_PARTITION_QA_SEMANTICS,
                max_publish_failure_count=0,
            ),
            QaCheckPolicy(
                check_id=_ROW_QA_ID,
                severity=QaSeverity.CRITICAL,
                semantics_digest=_ROW_QA_SEMANTICS,
                max_publish_failure_count=0,
            ),
        )
    )


def _gate_a_row_version_change_index(
    root: Path,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging | None,
) -> tuple[RowVersionChangeIndexPin, str]:
    parent_terminal = (
        {}
        if parent is None
        else {item.map_key: item for item in parent.checkpoint.terminal_row_versions}
    )
    child_terminal = {item.map_key: item for item in prepared.checkpoint.terminal_row_versions}
    rows: list[dict[str, object]] = []
    locators: set[tuple[str, int]] = set()
    tables_by_artifact: dict[ArtifactPin, pa.Table] = {}
    for item in prepared.row_versions:
        if not isinstance(item, I3ProductionPreparedRowVersion):
            raise I3ProductionStageError("prepared row lineage contains an invalid item")
        if run_spec.run_kind is I3ProductionRunKind.BASE:
            if (
                item.operation is not RowVersionOperation.NEW_ROOT
                or item.predecessor_row_version_id is not None
                or item.predecessor_payload_digest is not None
            ):
                raise I3ProductionStageError("production base row versions must be new roots")
        elif item.operation not in {
            RowVersionOperation.NEW_ROOT,
            RowVersionOperation.MECHANICAL_SUCCESSOR,
        }:
            raise I3ProductionStageError("clean delta contains a non-mechanical row operation")
        row_index = _row_index(item.row_locator)
        locator_key = (item.index_artifact.path, row_index)
        if locator_key in locators:
            raise I3ProductionStageError("two row receipts locate the same physical row")
        locators.add(locator_key)
        table = tables_by_artifact.get(item.index_artifact)
        if table is None:
            _verify_file(root, item.index_artifact)
            table = pq.read_table(safe_relative_path(root, item.index_artifact.path))
            tables_by_artifact[item.index_artifact] = table
        if row_index >= table.num_rows:
            raise I3ProductionStageError("row receipt locator exceeds its Parquet segment")
        row = table.slice(row_index, 1).to_pylist()[0]
        key_field, version_field, predecessor_field, availability_field = contract._VERSION_FIELDS[
            item.table_name
        ]
        if (
            str(row[key_field]) != item.stable_row_key
            or row[version_field] != item.row_version_id
            or row[predecessor_field] != item.predecessor_row_version_id
            or row[availability_field] != item.availability_session
            or stable_digest(contract._jsonable(row)) != item.row_payload_digest
        ):
            raise I3ProductionStageError(
                "prepared row lineage differs from its exact physical Parquet row"
            )
        prior = parent_terminal.get((item.table_name, item.stable_row_key))
        if item.operation is RowVersionOperation.NEW_ROOT:
            if prior is not None:
                raise I3ProductionStageError("new-root row reuses an existing stable key")
        elif (
            prior is None
            or prior.row_version_id != item.predecessor_row_version_id
            or prior.row_payload_digest != item.predecessor_payload_digest
        ):
            raise I3ProductionStageError(
                "mechanical successor differs from the authenticated parent terminal row"
            )
        terminal = child_terminal.get((item.table_name, item.stable_row_key))
        if (
            terminal is None
            or terminal.row_version_id != item.row_version_id
            or terminal.predecessor_row_version_id != item.predecessor_row_version_id
            or terminal.row_payload_digest != item.row_payload_digest
            or terminal.availability_session != item.availability_session
        ):
            raise I3ProductionStageError(
                "prepared row lineage differs from the child checkpoint terminal map"
            )
        if run_spec.run_kind is I3ProductionRunKind.BASE:
            expected_validator = production_compact_base_row_validator_digest(
                table_name=item.table_name,
                schema_digest=contract.I3_V2_CONTRACTS[item.table_name].schema_digest,
            )
            if item.validator_semantics_digest != expected_validator:
                raise I3ProductionStageError(
                    "compact-base row validator semantics are not module-owned"
                )
        else:
            expected_validator = production_delta_row_validator_digest(
                table_name=item.table_name,
                schema_digest=contract.I3_V2_CONTRACTS[item.table_name].schema_digest,
                operation=item.operation.value,
            )
            if (
                item.availability_session != run_spec.run_available_session
                or item.validator_semantics_digest != expected_validator
            ):
                raise I3ProductionStageError("DELTA row validator semantics are not module-owned")
        row: dict[str, object] = {
            "availability_session": item.availability_session,
            "index_artifact_bytes": item.index_artifact.bytes,
            "index_artifact_path": item.index_artifact.path,
            "index_artifact_sha256": item.index_artifact.sha256,
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
        row["semantic_proof_digest"] = contract._row_change_index_proof_digest(row)
        rows.append(row)
    rows.sort(key=lambda value: (str(value["table_name"]), str(value["stable_row_key"])))
    if len({(str(item["table_name"]), str(item["stable_row_key"])) for item in rows}) != len(rows):
        raise I3ProductionStageError("prepared row lineage repeats a stable key")
    _verify_row_lineage_coverage(prepared, parent=parent, locators=locators)
    if not rows:
        raise I3ProductionStageError("production I3 materialization has no row-version receipts")
    superseded = tuple(
        sorted(
            str(item["predecessor_row_version_id"])
            for item in rows
            if item["predecessor_row_version_id"] is not None
        )
    )
    if len(superseded) != len(set(superseded)):
        raise I3ProductionStageError("two indexed rows supersede one predecessor")
    table = pa.Table.from_pylist(rows, schema=contract._ROW_VERSION_CHANGE_INDEX_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    artifact = _write_immutable(
        root,
        f"{_run_root(run_spec)}/row-version-change-index.parquet",
        sink.getvalue().to_pybytes(),
    )
    index = RowVersionChangeIndexPin(
        artifact=artifact,
        row_count=len(rows),
        logical_receipts_digest=contract._row_change_index_logical_receipts_digest(rows),
        superseded_row_version_count=len(superseded),
        superseded_row_version_ids_digest=(
            contract._row_change_index_supersession_digest(superseded)
        ),
        schema_digest=contract._ROW_VERSION_CHANGE_INDEX_SCHEMA_DIGEST,
        availability_session=run_spec.run_available_session,
    )
    return index, contract._row_change_index_attestation_digest(
        index,
        prepared.checkpoint.checkpoint_id,
    )


def _verify_row_lineage_coverage(
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging | None,
    locators: set[tuple[str, int]],
) -> None:
    expected: set[tuple[str, int]] = set()
    for output in prepared.table_outputs:
        if output.table_name == "universe_daily":
            continue
        if output.rowset_index is None:
            if parent is not None:
                raise I3ProductionStageError(
                    "delta versioned tables must use append-only rowset indexes"
                )
            segments = ((output.manifest_output.artifact, output.manifest_output.row_count),)
        else:
            parent_count = 0
            if parent is not None:
                parent_output_set = parent.receipt.output_set
                if parent_output_set is None:  # pragma: no cover
                    raise I3ProductionStageError("authenticated parent lost its output set")
                parent_output = parent_output_set.table_outputs[
                    I3_V2_TABLE_ORDER.index(output.table_name)
                ]
                if parent_output.rowset_index is None:
                    raise I3ProductionStageError(
                        "authenticated parent lacks an append-only rowset index"
                    )
                parent_count = len(parent_output.rowset_index.segments)
            segments = tuple(
                (segment.artifact, segment.row_count)
                for segment in output.rowset_index.segments[parent_count:]
            )
        for artifact, row_count in segments:
            expected.update((artifact.path, index) for index in range(row_count))
    if locators != expected:
        raise I3ProductionStageError(
            "Gate-A row receipts do not cover the exact newly materialized rowset"
        )


def _row_index(locator: str) -> int:
    prefix = "row_index="
    if not locator.startswith(prefix):
        raise I3ProductionStageError("row locator must use row_index=<nonnegative integer>")
    raw = locator[len(prefix) :]
    if not raw.isdigit() or (raw != "0" and raw.startswith("0")):
        raise I3ProductionStageError("row locator is not canonical")
    return int(raw)


def _reject_temporary_authority_pin(pin: ArtifactPin, label: str) -> None:
    if pin.path == "tmp" or pin.path.startswith("tmp/"):
        raise I3ProductionStageError(f"temporary {label} cannot acquire production authority")


def validate_production_delta_run_spec_artifact_path(
    pin: ArtifactPin,
    *,
    run_spec_id: str | None = None,
) -> str:
    """Validate the module-owned DELTA RunSpec locator before staging reads.

    The path shape is rejected before any artifact is opened.  Once the exact
    RunSpec has been parsed, ``run_spec_id`` closes the embedded path identity.
    BASE staging deliberately does not use this DELTA-only locator contract.
    """

    if not isinstance(pin, ArtifactPin):
        raise I3ProductionStageError("DELTA RunSpec artifact pin is invalid")
    prefix = f"{_DELTA_RUN_SPEC_ROOT}/run_spec_id="
    suffix = "/run-spec.json"
    if not pin.path.startswith(prefix) or not pin.path.endswith(suffix):
        raise I3ProductionStageError("DELTA RunSpec artifact path is not module-owned canonical")
    embedded_id = pin.path[len(prefix) : -len(suffix)]
    if (
        len(embedded_id) != 64
        or any(character not in "0123456789abcdef" for character in embedded_id)
        or "/" in embedded_id
    ):
        raise I3ProductionStageError("DELTA RunSpec artifact path is not module-owned canonical")
    if run_spec_id is not None and (
        not isinstance(run_spec_id, str)
        or len(run_spec_id) != 64
        or any(character not in "0123456789abcdef" for character in run_spec_id)
        or embedded_id != run_spec_id
    ):
        raise I3ProductionStageError(
            "DELTA RunSpec artifact path does not match its exact RunSpec ID"
        )
    return embedded_id


def _require_interruption_capability(
    capability: _I3ProductionInterruptionCapability,
    *,
    phase_one_id: str | None,
) -> None:
    if (
        type(capability) is not _I3ProductionInterruptionCapability
        or capability._seal is not _INTERRUPTION_CAPABILITY_SEAL
        or _ACTIVE_INTERRUPTION_CAPABILITIES.get(id(capability)) is not capability
        or capability.fail_after != FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION
        or capability.phase_one_id != phase_one_id
    ):
        raise I3ProductionStageError("interrupted-retry capability is not module-sealed")


def _production_parent_evidence(
    parent: LoadedI3ProductionStaging,
) -> tuple[str, str, int]:
    output_set = parent.receipt.output_set
    if output_set is None:
        raise I3ProductionStageError("interrupted-retry parent lacks an OutputSet")
    reader_digest = stable_digest(
        {
            "checkpoint_id": parent.checkpoint.checkpoint_id,
            "completion_id": parent.completion.completion_id,
            "gate_a_release_id": parent.gate_a_manifest.release_id,
            "output_set_id": output_set.output_set_id,
            "rule_version": "s7_5_i3_interrupted_retry_parent_reader_v1",
        }
    )
    artifacts: dict[str, ArtifactPin] = {}
    for output in output_set.table_outputs:
        if output.dataset_index is not None:
            selected = tuple(item.artifact for item in output.dataset_index.partitions)
        elif output.rowset_index is not None:
            selected = tuple(item.artifact for item in output.rowset_index.segments)
        else:
            selected = (output.manifest_output.artifact,)
        for artifact in selected:
            prior = artifacts.get(artifact.path)
            if prior is not None and prior != artifact:
                raise I3ProductionStageError(
                    "interrupted-retry parent path has conflicting exact pins"
                )
            artifacts[artifact.path] = artifact
    ordered = tuple(artifacts[path] for path in sorted(artifacts))
    if not ordered:
        raise I3ProductionStageError("interrupted-retry parent artifact set is empty")
    return (
        reader_digest,
        stable_digest(
            {
                "artifacts": [item.to_dict() for item in ordered],
                "rule_version": "s7_5_i3_interrupted_retry_parent_artifact_set_v1",
            }
        ),
        len(ordered),
    )


def _delta_frozen_envelope_digest(
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging,
) -> str:
    return _delta_frozen_envelope_digest_from_parts(
        prepared.table_outputs,
        prepared.native_manifest,
        prepared.checkpoint,
        parent=parent,
    )


def _delta_frozen_envelope_digest_from_loaded(
    loaded: LoadedI3ProductionStaging,
    *,
    parent: LoadedI3ProductionStaging,
) -> str:
    output_set = loaded.receipt.output_set
    if output_set is None:
        raise I3ProductionStageError("interrupted-retry completion lacks an OutputSet")
    return _delta_frozen_envelope_digest_from_parts(
        output_set.table_outputs,
        loaded.manifest,
        loaded.checkpoint,
        parent=parent,
    )


def _delta_frozen_envelope_digest_from_parts(
    table_outputs: tuple[I3ProductionTableOutput, ...],
    native_manifest: NativeV2ReleaseManifest,
    checkpoint: I3CheckpointState,
    *,
    parent: LoadedI3ProductionStaging,
) -> str:
    parent_output_set = parent.receipt.output_set
    if parent_output_set is None:
        raise I3ProductionStageError("interrupted-retry parent lost its OutputSet")
    physical: list[dict[str, object]] = []
    for parent_output, output in zip(
        parent_output_set.table_outputs,
        table_outputs,
        strict=True,
    ):
        if output.table_name == "universe_daily":
            parent_index = parent_output.dataset_index
            child_index = output.dataset_index
            if (
                parent_index is None
                or child_index is None
                or child_index.partitions[:-1] != parent_index.partitions
                or len(child_index.partitions) != len(parent_index.partitions) + 1
            ):
                raise I3ProductionStageError(
                    "interrupted-retry universe envelope is not one exact append"
                )
            partition = child_index.partitions[-1]
            physical.append(
                {
                    "artifact_bytes": partition.artifact.bytes,
                    "artifact_sha256": partition.artifact.sha256,
                    "availability_session": partition.availability_session.isoformat(),
                    "contract_id": partition.contract_id,
                    "row_count": partition.row_count,
                    "schema_digest": partition.schema_digest,
                    "session_date": partition.session_date.isoformat(),
                    "table_name": output.table_name,
                }
            )
        else:
            parent_rowset = parent_output.rowset_index
            child_rowset = output.rowset_index
            if (
                parent_rowset is None
                or child_rowset is None
                or child_rowset.segments[:-1] != parent_rowset.segments
                or len(child_rowset.segments) != len(parent_rowset.segments) + 1
            ):
                raise I3ProductionStageError(
                    "interrupted-retry rowset envelope is not one exact append"
                )
            segment = child_rowset.segments[-1]
            physical.append(
                {
                    "artifact_bytes": segment.artifact.bytes,
                    "artifact_sha256": segment.artifact.sha256,
                    "availability_session": segment.availability_session.isoformat(),
                    "contract_id": segment.contract_id,
                    "row_count": segment.row_count,
                    "schema_digest": segment.schema_digest,
                    "table_name": output.table_name,
                }
            )
    terminal = [
        {
            "artifact_bytes": item.index_artifact.bytes,
            "artifact_sha256": item.index_artifact.sha256,
            "availability_session": item.availability_session.isoformat(),
            "predecessor_row_version_id": item.predecessor_row_version_id,
            "row_payload_digest": item.row_payload_digest,
            "row_version_id": item.row_version_id,
            "stable_row_key": item.stable_row_key,
            "table_name": item.table_name,
        }
        for item in checkpoint.terminal_row_versions
    ]
    checkpoint_semantics = {
        "asset_aggregates": [item.to_dict() for item in checkpoint.asset_aggregates],
        "availability_cutoff_session": checkpoint.availability_cutoff_session.isoformat(),
        "calendar_digest": checkpoint.calendar_digest,
        "identity_policy_bundle": checkpoint.identity_policy_bundle.to_dict(),
        "identity_policy_bundle_artifact": (checkpoint.identity_policy_bundle_artifact.to_dict()),
        "issuer_aggregates": [item.to_dict() for item in checkpoint.issuer_aggregates],
        "last_session": checkpoint.last_session.isoformat(),
        "open_aliases": [item.to_dict() for item in checkpoint.open_aliases],
        "resolved_partition_map": [
            {
                "artifact_bytes": item.artifact.bytes,
                "artifact_sha256": item.artifact.sha256,
                "availability_session": item.availability_session.isoformat(),
                "row_count": item.row_count,
                "session_date": item.session_date.isoformat(),
            }
            for item in checkpoint.resolved_partition_map
        ],
        "s4_terminal_pins": [item.to_dict() for item in checkpoint.s4_terminal_pins],
        "schema_digest": checkpoint.schema_digest,
        "source_cutoff_session": checkpoint.source_cutoff_session.isoformat(),
        "transform_semantics_digest": checkpoint.transform_semantics_digest,
        "unresolved_subjects": [item.to_dict() for item in checkpoint.unresolved_subjects],
    }
    return stable_digest(
        {
            "availability_session": native_manifest.release_available_session.isoformat(),
            "checkpoint_semantics": checkpoint_semantics,
            "identity_policy_bundle_id": native_manifest.identity_policy_bundle_id,
            "native_v2_migration_id": native_manifest.native_v2_migration_id,
            "physical_suffixes": physical,
            "rule_version": "s7_5_i3_interrupted_retry_frozen_envelope_v1",
            "terminal_row_versions": terminal,
            "terminal_session": native_manifest.terminal_session.isoformat(),
            "transform_semantics_digest": native_manifest.transform_semantics_digest,
        }
    )


def _delta_new_prepared_artifacts(
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging,
) -> tuple[ArtifactPin, ...]:
    parent_output_set = parent.receipt.output_set
    if parent_output_set is None:
        raise I3ProductionStageError("interrupted-retry parent lost its OutputSet")
    pins = [prepared.native_manifest_artifact, prepared.checkpoint_artifact]
    for parent_output, output in zip(
        parent_output_set.table_outputs,
        prepared.table_outputs,
        strict=True,
    ):
        pins.append(output.manifest_output.artifact)
        if output.dataset_index is not None:
            parent_index = parent_output.dataset_index
            if parent_index is None:
                raise I3ProductionStageError("interrupted-retry parent dataset index is absent")
            pins.extend(
                item.artifact
                for item in output.dataset_index.partitions[len(parent_index.partitions) :]
            )
        elif output.rowset_index is not None:
            parent_rowset = parent_output.rowset_index
            if parent_rowset is None:
                raise I3ProductionStageError("interrupted-retry parent rowset index is absent")
            pins.extend(
                item.artifact
                for item in output.rowset_index.segments[len(parent_rowset.segments) :]
            )
    pins.extend(item.index_artifact for item in prepared.row_versions)
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        if not isinstance(pin, ArtifactPin):
            raise I3ProductionStageError("interrupted-retry frozen artifact is invalid")
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise I3ProductionStageError("interrupted-retry artifact path has conflicting pins")
        by_path[pin.path] = pin
    return tuple(by_path[path] for path in sorted(by_path))


def _interruption_failure_detail_digest(
    run_spec: I3ProductionRunSpec,
    frozen_envelope_digest: str,
) -> str:
    return stable_digest(
        {
            "fail_after": FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
            "frozen_envelope_digest": frozen_envelope_digest,
            "rule_version": I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION,
            "run_spec_id": run_spec.run_spec_id,
        }
    )


def _verify_interrupted_failed_receipt(
    root: Path,
    *,
    run_spec: I3ProductionRunSpec,
    phase_one: _InterruptedRetryPhaseOne,
) -> I3ProductionRunReceipt:
    expected_relative = (
        f"{_run_root(run_spec)}/failed-receipts/receipt_id={phase_one.failed_receipt_id}.json"
    )
    if phase_one.failed_receipt_artifact.path != expected_relative:
        raise I3ProductionStageError(
            "interrupted-retry failed receipt is not at its canonical locator"
        )
    try:
        receipt = load_i3_production_run_receipt_exact(
            phase_one.failed_receipt_artifact,
            lambda relative: _read_control(root, relative),
        )
    except contract.I3ProductionContractError as exc:
        raise I3ProductionStageError(
            "interrupted-retry failed receipt does not exact-replay"
        ) from exc
    expected_detail_digest = _interruption_failure_detail_digest(
        run_spec,
        phase_one.frozen_envelope_digest,
    )
    if (
        receipt.receipt_id != phase_one.failed_receipt_id
        or receipt.exact_pin(path=expected_relative) != phase_one.failed_receipt_artifact
        or receipt.run_spec_id != run_spec.run_spec_id
        or receipt.run_spec_artifact != phase_one.run_spec_artifact
        or receipt.state is not I3ProductionRunState.FAILED
        or receipt.failure_code != "interrupted_retry_injected"
        or receipt.failure_detail_digest != expected_detail_digest
        or receipt.output_set is not None
        or receipt.receipt_available_session != run_spec.run_available_session
    ):
        raise I3ProductionStageError(
            "interrupted-retry failed receipt differs from its phase-one authority"
        )
    return receipt


def _validate_prepared(
    root: Path,
    run_spec: I3ProductionRunSpec,
    prepared: I3ProductionPreparedMaterialization,
    *,
    parent: LoadedI3ProductionStaging | None,
) -> None:
    if not isinstance(prepared, I3ProductionPreparedMaterialization):
        raise I3ProductionStageError("materializer returned an invalid prepared bundle")
    if prepared.canonical_projection_difference_count != 0:
        raise I3ProductionStageError("canonical v1 projection contains unexpected differences")
    if not isinstance(prepared.resource_observation, I3ProductionResourceObservation):
        raise I3ProductionStageError("prepared resource observation is invalid")
    if tuple(item.table_name for item in prepared.table_outputs) != I3_V2_TABLE_ORDER:
        raise I3ProductionStageError("prepared output table order differs")
    try:
        contract.validate_production_compact_base_initial_rowsets(run_spec, prepared.table_outputs)
        if run_spec.run_kind is I3ProductionRunKind.DELTA:
            if parent is None or parent.receipt.output_set is None:
                raise contract.I3ProductionContractError(
                    "DELTA staging lacks an authenticated parent OutputSet"
                )
            contract.validate_production_delta_append_outputs(
                run_spec,
                prepared.table_outputs,
                parent.receipt.output_set,
            )
    except contract.I3ProductionContractError as exc:
        raise I3ProductionStageError(str(exc)) from exc
    authoritative_pins = [
        prepared.native_manifest_artifact,
        prepared.checkpoint_artifact,
        *(item.manifest_output.artifact for item in prepared.table_outputs),
        *(item.index_artifact for item in prepared.row_versions),
    ]
    for output in prepared.table_outputs:
        if output.dataset_index is not None:
            authoritative_pins.extend(item.artifact for item in output.dataset_index.partitions)
        if output.rowset_index is not None:
            authoritative_pins.extend(item.artifact for item in output.rowset_index.segments)
    if any(item.path == "tmp" or item.path.startswith("tmp/") for item in authoritative_pins):
        raise I3ProductionStageError(
            "authoritative native-v2 staging artifact cannot live below tmp"
        )
    if (
        prepared.native_manifest.release_id
        != NativeV2ReleaseManifest.from_dict(
            _read_canonical_json(root, prepared.native_manifest_artifact)
        ).release_id
        or prepared.native_manifest.exact_pin(path=prepared.native_manifest_artifact.path)
        != prepared.native_manifest_artifact
        or tuple(item.manifest_output for item in prepared.table_outputs)
        != prepared.native_manifest.output_artifacts
    ):
        raise I3ProductionStageError("prepared native-v2 physical envelope does not reproduce")
    loaded_checkpoint = I3CheckpointState.from_dict(
        _read_canonical_json(root, prepared.checkpoint_artifact)
    )
    if (
        loaded_checkpoint != prepared.checkpoint
        or prepared.checkpoint.exact_pin(path=prepared.checkpoint_artifact.path)
        != prepared.checkpoint_artifact
        or prepared.checkpoint.parent_release.manifest != prepared.native_manifest_artifact
        or prepared.checkpoint.parent_release.release_id != prepared.native_manifest.release_id
        or prepared.checkpoint.last_session != run_spec.terminal_session
        or prepared.checkpoint.source_cutoff_session != run_spec.source_cutoff_session
        or prepared.checkpoint.availability_cutoff_session != run_spec.run_available_session
        or prepared.checkpoint.calendar_digest != run_spec.calendar.calendar_artifact_id
        or prepared.checkpoint.schema_digest != I3_V2_SCHEMA_BUNDLE_DIGEST
        or prepared.checkpoint.transform_semantics_digest != run_spec.transform_semantics_digest
        or prepared.checkpoint.identity_policy_bundle != run_spec.identity_policy_bundle
        or prepared.checkpoint.identity_policy_bundle_artifact
        != run_spec.identity_policy_bundle_artifact
        or prepared.checkpoint.resolved_state_digest
        != prepared.native_manifest.resolved_state_digest
    ):
        raise I3ProductionStageError("prepared checkpoint differs from exact production controls")
    if run_spec.run_kind is I3ProductionRunKind.BASE:
        if parent is not None or prepared.native_manifest.parent_release_id is not None:
            raise I3ProductionStageError("prepared base unexpectedly carries a release parent")
    elif (
        parent is None
        or prepared.native_manifest.parent_release_id != parent.manifest.release_id
        or prepared.native_manifest.source_checkpoint_id != parent.checkpoint.checkpoint_id
    ):
        raise I3ProductionStageError("prepared delta differs from authenticated parent")
    _verify_prepared_output_bytes(root, prepared.table_outputs, parent=parent)


def _verify_prepared_output_bytes(
    root: Path,
    table_outputs: tuple[I3ProductionTableOutput, ...],
    *,
    parent: LoadedI3ProductionStaging | None,
) -> None:
    for output in table_outputs:
        _verify_file(root, output.manifest_output.artifact)
        if output.dataset_index is not None:
            if _read_pin(root, output.manifest_output.artifact) != (
                output.dataset_index.canonical_bytes()
            ):
                raise I3ProductionStageError("universe dataset-index bytes differ")
            partitions = output.dataset_index.partitions
            if parent is not None:
                parent_output_set = parent.receipt.output_set
                if parent_output_set is None:  # pragma: no cover
                    raise I3ProductionStageError("authenticated parent lost its OutputSet")
                parent_index = parent_output_set.table_outputs[
                    I3_V2_TABLE_ORDER.index("universe_daily")
                ].dataset_index
                if parent_index is None or partitions[: len(parent_index.partitions)] != (
                    parent_index.partitions
                ):
                    raise I3ProductionStageError(
                        "prepared delta changed an existing universe partition pin"
                    )
                partitions = (
                    parent_index.partitions[-_DELTA_BOUNDARY_PARTITION_COUNT:]
                    + partitions[len(parent_index.partitions) :]
                )
            for item in partitions:
                contract._verify_parquet_exact(
                    root,
                    item.artifact,
                    table_name="universe_daily",
                    row_count=item.row_count,
                    session_date=item.session_date,
                )
        elif output.rowset_index is not None:
            if _read_pin(root, output.manifest_output.artifact) != (
                output.rowset_index.canonical_bytes()
            ):
                raise I3ProductionStageError("versioned rowset-index bytes differ")
            segments = output.rowset_index.segments
            if parent is not None:
                parent_output_set = parent.receipt.output_set
                if parent_output_set is None:  # pragma: no cover
                    raise I3ProductionStageError("authenticated parent lost its OutputSet")
                parent_rowset = parent_output_set.table_outputs[
                    I3_V2_TABLE_ORDER.index(output.table_name)
                ].rowset_index
                if parent_rowset is None or segments[: len(parent_rowset.segments)] != (
                    parent_rowset.segments
                ):
                    raise I3ProductionStageError(
                        "prepared delta changed an existing rowset segment pin"
                    )
                segments = segments[len(parent_rowset.segments) :]
            for item in segments:
                contract._verify_parquet_exact(
                    root,
                    item.artifact,
                    table_name=output.table_name,
                    row_count=item.row_count,
                )
        else:
            if parent is not None:
                raise I3ProductionStageError(
                    "delta versioned tables require append-only rowset indexes"
                )
            contract._verify_parquet_exact(
                root,
                output.manifest_output.artifact,
                table_name=output.table_name,
                row_count=output.manifest_output.row_count,
            )


def _write_failed_receipt(
    root: Path,
    run_spec: I3ProductionRunSpec,
    run_spec_pin: ArtifactPin,
    error: Exception,
    *,
    started: float,
    minimum_disk_free_bytes: int,
    failure_code_override: str | None = None,
) -> ArtifactPin | None:
    try:
        observation = I3ProductionResourceObservation(
            peak_rss_bytes=_peak_rss_bytes(),
            elapsed_seconds=max(0, math.ceil(time.monotonic() - started)),
            minimum_disk_free_bytes=min(minimum_disk_free_bytes, shutil.disk_usage(root).free),
            temporary_bytes=0,
        )
        detail = stable_digest(
            {
                "error_message": str(error),
                "error_type": type(error).__name__,
                "run_spec_id": run_spec.run_spec_id,
            }
        )
        receipt = I3ProductionRunReceipt(
            run_spec_id=run_spec.run_spec_id,
            run_spec_artifact=run_spec_pin,
            state=I3ProductionRunState.FAILED,
            receipt_available_session=run_spec.run_available_session,
            resource_observation=observation,
            failure_code=(
                failure_code_override
                or (
                    "resource_cap_exceeded"
                    if "cap" in str(error).lower() or "disk" in str(error).lower()
                    else "materialization_failed"
                )
            ),
            failure_detail_digest=detail,
        )
        relative = f"{_run_root(run_spec)}/failed-receipts/receipt_id={receipt.receipt_id}.json"
        return _write_immutable(root, relative, receipt.canonical_bytes())
    except Exception:
        return None


def _combined_observation(
    root: Path,
    prepared: I3ProductionResourceObservation,
    *,
    started: float,
) -> I3ProductionResourceObservation:
    return I3ProductionResourceObservation(
        peak_rss_bytes=max(prepared.peak_rss_bytes, _peak_rss_bytes()),
        elapsed_seconds=max(
            prepared.elapsed_seconds, max(0, math.ceil(time.monotonic() - started))
        ),
        minimum_disk_free_bytes=min(prepared.minimum_disk_free_bytes, shutil.disk_usage(root).free),
        temporary_bytes=prepared.temporary_bytes,
    )


def _check_live_resources(root: Path, run_spec: I3ProductionRunSpec) -> None:
    if _peak_rss_bytes() > run_spec.resource_caps.rss_bytes_hard_cap:
        raise I3ProductionStageError("current RSS already exceeds the hard cap")
    if shutil.disk_usage(root).free < run_spec.resource_caps.disk_free_bytes_hard_floor:
        raise I3ProductionStageError("current free disk is below the hard floor")


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise I3ProductionStageError("production staging lock is not a regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise I3ProductionStageError(
                "another production I3 run holds the exact session lock"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _run_root(run_spec: I3ProductionRunSpec) -> str:
    return f"{_CONTROL_ROOT}/run_spec_id={run_spec.run_spec_id}"


def _control_relative(run_spec: I3ProductionRunSpec, name: str) -> str:
    return f"{_run_root(run_spec)}/{name}"


def _completion_relative(run_spec: I3ProductionRunSpec) -> str:
    return _control_relative(run_spec, "completion.json")


def _deep_attestation_relative(run_spec: I3ProductionRunSpec) -> str:
    return _control_relative(run_spec, "deep-verification-attestation.json")


def _lock_relative(run_spec: I3ProductionRunSpec) -> str:
    return (
        f"{_LOCK_ROOT}/{run_spec.run_kind.value}/"
        f"session_date={run_spec.terminal_session.isoformat()}.lock"
    )


def _workspace_relative(run_spec: I3ProductionRunSpec) -> str:
    return f"{_OUTPUT_ROOT}/run_spec_id={run_spec.run_spec_id}"


def _interrupted_retry_workspace_relative(run_spec: I3ProductionRunSpec) -> str:
    return f"{_INTERRUPTED_RETRY_OUTPUT_ROOT}/run_spec_id={run_spec.run_spec_id}/phase-one"


def _interrupted_retry_phase_one_relative(run_spec: I3ProductionRunSpec) -> str:
    return _control_relative(
        run_spec,
        "failure-exercises/interrupted-retry/phase-one.json",
    )


def _interrupted_retry_receipt_relative(run_spec: I3ProductionRunSpec) -> str:
    return _control_relative(
        run_spec,
        "failure-exercises/interrupted-retry/receipt.json",
    )


def _load_interrupted_retry_phase_one(
    root: Path,
    run_spec: I3ProductionRunSpec,
) -> tuple[_InterruptedRetryPhaseOne, ArtifactPin]:
    relative = _interrupted_retry_phase_one_relative(run_spec)
    pin = _pin_existing(root, relative)
    item = _closed_control_mapping(
        _read_canonical_json(root, pin),
        {
            "artifact_type",
            "deleted_artifact_count",
            "fail_after",
            "failed_receipt_artifact",
            "failed_receipt_id",
            "frozen_artifacts",
            "frozen_envelope_digest",
            "parent_artifact_count",
            "parent_artifact_set_digest",
            "parent_reader_after_digest",
            "parent_reader_before_digest",
            "phase_one_id",
            "rule_version",
            "run_spec_artifact",
            "run_spec_id",
            "unpublished_visible_count",
        },
        "interrupted-retry phase one",
    )
    _require_literal(
        item["artifact_type"],
        "s7_5_i3_production_interrupted_retry_phase_one",
        "interrupted-retry phase-one artifact type",
    )
    _require_literal(
        item["rule_version"],
        I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION,
        "interrupted-retry phase-one rule",
    )
    frozen = item["frozen_artifacts"]
    if type(frozen) is not list or not frozen:
        raise I3ProductionStageError("interrupted-retry frozen-artifact list is invalid")
    result = _InterruptedRetryPhaseOne(
        run_spec_id=_require_json_text(item["run_spec_id"], "phase-one RunSpec ID"),
        run_spec_artifact=_artifact_pin_from_json(
            item["run_spec_artifact"],
            "phase-one RunSpec artifact",
        ),
        failed_receipt_id=_require_json_text(
            item["failed_receipt_id"],
            "phase-one failed-receipt ID",
        ),
        failed_receipt_artifact=_artifact_pin_from_json(
            item["failed_receipt_artifact"],
            "phase-one failed-receipt artifact",
        ),
        frozen_envelope_digest=_require_json_text(
            item["frozen_envelope_digest"],
            "phase-one frozen-envelope digest",
        ),
        frozen_artifacts=tuple(
            _artifact_pin_from_json(value, "phase-one frozen artifact") for value in frozen
        ),
        parent_reader_before_digest=_require_json_text(
            item["parent_reader_before_digest"],
            "phase-one parent-before digest",
        ),
        parent_reader_after_digest=_require_json_text(
            item["parent_reader_after_digest"],
            "phase-one parent-after digest",
        ),
        parent_artifact_set_digest=_require_json_text(
            item["parent_artifact_set_digest"],
            "phase-one parent-artifact digest",
        ),
        parent_artifact_count=_require_json_int(
            item["parent_artifact_count"],
            "phase-one parent-artifact count",
        ),
        deleted_artifact_count=_require_json_int(
            item["deleted_artifact_count"],
            "phase-one deleted-artifact count",
        ),
        unpublished_visible_count=_require_json_int(
            item["unpublished_visible_count"],
            "phase-one unpublished-visible count",
        ),
        fail_after=_require_json_text(item["fail_after"], "phase-one failpoint"),
    )
    claimed_id = _require_json_text(item["phase_one_id"], "phase-one ID")
    if (
        claimed_id != result.phase_one_id
        or result.run_spec_id != run_spec.run_spec_id
        or result.exact_pin(path=relative) != pin
    ):
        raise I3ProductionStageError("interrupted-retry phase one does not reproduce")
    return result, pin


def _load_interrupted_retry_receipt(
    root: Path,
    run_spec: I3ProductionRunSpec,
) -> tuple[I3ProductionInterruptedRetryReceipt, ArtifactPin]:
    relative = _interrupted_retry_receipt_relative(run_spec)
    pin = _pin_existing(root, relative)
    item = _closed_control_mapping(
        _read_canonical_json(root, pin),
        {
            "artifact_type",
            "completion_artifact",
            "completion_id",
            "deep_attestation_artifact",
            "deep_attestation_id",
            "deleted_artifact_count",
            "fail_after",
            "failed_receipt_artifact",
            "failed_receipt_id",
            "frozen_envelope_digest",
            "parent_artifact_count",
            "parent_artifact_set_digest",
            "parent_reader_after_digest",
            "parent_reader_before_digest",
            "phase_one_artifact",
            "publish_authorized",
            "receipt_id",
            "rule_version",
            "run_spec_artifact",
            "run_spec_id",
            "unpublished_visible_count",
        },
        "interrupted-retry receipt",
    )
    _require_literal(
        item["artifact_type"],
        "s7_5_i3_production_interrupted_retry_receipt",
        "interrupted-retry receipt artifact type",
    )
    _require_literal(
        item["rule_version"],
        I3_PRODUCTION_INTERRUPTED_RETRY_RULE_VERSION,
        "interrupted-retry receipt rule",
    )
    if item["publish_authorized"] is not False:
        raise I3ProductionStageError("interrupted-retry receipt cannot authorize publication")
    result = I3ProductionInterruptedRetryReceipt(
        run_spec_id=_require_json_text(item["run_spec_id"], "receipt RunSpec ID"),
        run_spec_artifact=_artifact_pin_from_json(
            item["run_spec_artifact"],
            "receipt RunSpec artifact",
        ),
        phase_one_artifact=_artifact_pin_from_json(
            item["phase_one_artifact"],
            "receipt phase-one artifact",
        ),
        failed_receipt_id=_require_json_text(
            item["failed_receipt_id"],
            "receipt failed-receipt ID",
        ),
        failed_receipt_artifact=_artifact_pin_from_json(
            item["failed_receipt_artifact"],
            "receipt failed-receipt artifact",
        ),
        frozen_envelope_digest=_require_json_text(
            item["frozen_envelope_digest"],
            "receipt frozen-envelope digest",
        ),
        parent_reader_before_digest=_require_json_text(
            item["parent_reader_before_digest"],
            "receipt parent-before digest",
        ),
        parent_reader_after_digest=_require_json_text(
            item["parent_reader_after_digest"],
            "receipt parent-after digest",
        ),
        parent_artifact_set_digest=_require_json_text(
            item["parent_artifact_set_digest"],
            "receipt parent-artifact digest",
        ),
        parent_artifact_count=_require_json_int(
            item["parent_artifact_count"],
            "receipt parent-artifact count",
        ),
        deleted_artifact_count=_require_json_int(
            item["deleted_artifact_count"],
            "receipt deleted-artifact count",
        ),
        unpublished_visible_count=_require_json_int(
            item["unpublished_visible_count"],
            "receipt unpublished-visible count",
        ),
        completion_id=_require_json_text(item["completion_id"], "receipt completion ID"),
        completion_artifact=_artifact_pin_from_json(
            item["completion_artifact"],
            "receipt completion artifact",
        ),
        deep_attestation_id=_require_json_text(
            item["deep_attestation_id"],
            "receipt deep-attestation ID",
        ),
        deep_attestation_artifact=_artifact_pin_from_json(
            item["deep_attestation_artifact"],
            "receipt deep-attestation artifact",
        ),
        fail_after=_require_json_text(item["fail_after"], "receipt failpoint"),
    )
    claimed_id = _require_json_text(item["receipt_id"], "interrupted-retry receipt ID")
    if (
        claimed_id != result.receipt_id
        or result.run_spec_id != run_spec.run_spec_id
        or result.exact_pin(path=relative) != pin
    ):
        raise I3ProductionStageError("interrupted-retry receipt does not reproduce")
    return result, pin


def _closed_control_mapping(
    value: object,
    expected_keys: set[str],
    label: str,
) -> Mapping[str, object]:
    if type(value) is not dict or set(value) != expected_keys:
        raise I3ProductionStageError(f"{label} is not a closed-schema control")
    return value


def _artifact_pin_from_json(value: object, label: str) -> ArtifactPin:
    item = _closed_control_mapping(value, {"bytes", "path", "sha256"}, label)
    path = _require_json_text(item["path"], f"{label} path")
    sha256 = _require_json_text(item["sha256"], f"{label} SHA-256")
    byte_count = _require_json_int(item["bytes"], f"{label} bytes")
    try:
        return ArtifactPin(path=path, sha256=sha256, bytes=byte_count)
    except Exception as exc:
        raise I3ProductionStageError(f"{label} is invalid") from exc


def _require_json_text(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise I3ProductionStageError(f"{label} is invalid")
    return value


def _require_json_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I3ProductionStageError(f"{label} is invalid")
    return value


def _require_literal(value: object, expected: object, label: str) -> None:
    if value != expected or type(value) is not type(expected):
        raise I3ProductionStageError(f"{label} is invalid")


def _require_lower_sha256(value: object, label: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise I3ProductionStageError(f"{label} is not a lowercase SHA-256 digest")
    return value


def _write_immutable(root: Path, relative: str, content: bytes) -> ArtifactPin:
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        content,
        temporary_directory=safe_relative_path(root, _TEMP_ROOT),
    )
    return ArtifactPin(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _pin_existing(root: Path, relative: str) -> ArtifactPin:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionStageError(f"exact artifact is missing: {relative}")
    return ArtifactPin(path=relative, sha256=sha256_file(path), bytes=path.stat().st_size)


def _verify_file(root: Path, pin: ArtifactPin) -> None:
    path = safe_relative_path(root, pin.path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.bytes
        or sha256_file(path) != pin.sha256
    ):
        raise I3ProductionStageError(f"prepared artifact differs from exact pin: {pin.path}")


def _read_pin(root: Path, pin: ArtifactPin) -> bytes:
    _verify_file(root, pin)
    return safe_relative_path(root, pin.path).read_bytes()


def _read_control(root: Path, relative: str) -> bytes:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionStageError(f"exact control is missing: {relative}")
    return path.read_bytes()


def _read_canonical_json(root: Path, pin: ArtifactPin) -> Mapping[str, object]:
    import json

    content = _read_pin(root, pin)
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I3ProductionStageError("prepared control is not valid JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != content:
        raise I3ProductionStageError("prepared control is not canonical JSON")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    import json

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


__all__ = [
    "FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION",
    "I3ProductionInterruptedRetryPending",
    "I3ProductionInterruptedRetryReceipt",
    "I3ProductionInterruptedRetryResult",
    "I3ProductionMaterializer",
    "I3ProductionPreparedMaterialization",
    "I3ProductionPreparedRowVersion",
    "I3ProductionStageError",
    "I3ProductionStageResult",
    "exercise_i3_production_interrupted_retry",
    "stage_i3_production",
    "stage_i3_production_base",
    "stage_i3_production_delta",
    "validate_production_delta_run_spec_artifact_path",
    "verify_i3_production",
    "verify_i3_production_base",
    "verify_i3_production_deep_attestation",
    "verify_i3_production_delta",
]
