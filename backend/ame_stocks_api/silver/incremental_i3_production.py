"""Local-only production staging executor for S7.5 I3 native-v2 base/delta.

The materializer seam owns the expensive physical conversion.  This module
owns production controls: exact input authentication, one non-blocking lock,
resource hard guards, immutable control writes, Gate-A release construction,
failed receipts, an ``awaiting_review`` completion written last, and exact
post-write verification.  It has no publish, pointer, network, or cutover API.
"""

from __future__ import annotations

import fcntl
import math
import os
import resource
import shutil
import stat
import sys
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
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
    i3_checkpoint_storage_payload,
    i3_checkpoint_storage_pin,
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
_LOCK_ROOT = "locks/silver/identity/s7-5-native-v2-staging"
_TEMP_ROOT = "tmp/silver-identity-s7-5-native-v2-staging"
_OUTPUT_ROOT = "silver/schema=v2/identity/native_v2_staging"
_DELTA_BOUNDARY_PARTITION_COUNT = 3
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
            parent = contract._verify_production_parent_exact(root, run_spec)
            if run_spec.run_kind is I3ProductionRunKind.BASE:
                calendar_sessions = contract._verify_external_production_dependencies(
                    root, run_spec
                )
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
            verify_delta_materialization_seal,
        )
    except ModuleNotFoundError as exc:
        if exc.name != "ame_stocks_api.silver.incremental_i3_delta_io":
            raise
        raise I3ProductionStageError(
            "production delta materialization attestation verifier is unavailable"
        ) from exc
    if parent is None:
        raise I3ProductionStageError("production delta materialization lacks its parent")
    verify_delta_materialization_seal(
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
    """Validate DELTA RunSpec identity without prescribing a storage directory.

    ``ArtifactPin`` already binds normalized path, bytes and SHA-256.  Requiring
    an additional hard-coded directory shape did not improve factor correctness
    and made harmless moves/copies fail.  The logical RunSpec ID remains checked
    after parsing; its physical locator is intentionally not semantic.
    """

    if not isinstance(pin, ArtifactPin):
        raise I3ProductionStageError("DELTA RunSpec artifact pin is invalid")
    if run_spec_id is not None and (
        not isinstance(run_spec_id, str)
        or len(run_spec_id) != 64
        or any(character not in "0123456789abcdef" for character in run_spec_id)
    ):
        raise I3ProductionStageError("DELTA RunSpec ID is invalid")
    return run_spec_id or pin.sha256


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
        _read_checkpoint_json(root, prepared.checkpoint_artifact)
    )
    if (
        loaded_checkpoint != prepared.checkpoint
        or i3_checkpoint_storage_pin(
            prepared.checkpoint,
            path=prepared.checkpoint_artifact.path,
        )
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


def _read_checkpoint_json(root: Path, pin: ArtifactPin) -> Mapping[str, object]:
    import json

    stored = _read_pin(root, pin)
    try:
        content = i3_checkpoint_storage_payload(stored, path=pin.path)
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise I3ProductionStageError("prepared checkpoint is not valid canonical storage") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != content:
        raise I3ProductionStageError("prepared checkpoint is not canonical JSON")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    import json

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


__all__ = [
    "I3ProductionMaterializer",
    "I3ProductionPreparedMaterialization",
    "I3ProductionPreparedRowVersion",
    "I3ProductionStageError",
    "I3ProductionStageResult",
    "stage_i3_production",
    "stage_i3_production_base",
    "stage_i3_production_delta",
    "validate_production_delta_run_spec_artifact_path",
    "verify_i3_production",
    "verify_i3_production_base",
    "verify_i3_production_deep_attestation",
    "verify_i3_production_delta",
]
