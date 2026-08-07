from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin, ViewKind
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    ACCEPTANCE_CRITERIA,
    RESEARCH_POINTER_NAME,
    SHADOW_POINTER_NAME,
    AcceptanceCriterionReceipt,
    EquivalenceProjection,
    FailureRecoveryReceipt,
    FailureScenario,
    FullReconciliationPartitionEvidence,
    FullReconciliationReceipt,
    FullReconciliationSpec,
    FullReconciliationTableEvidence,
    FullReconciliationTableScope,
    GateBApproval,
    GateCApproval,
    IdempotencyReceipt,
    IncrementalLifecycleError,
    PinnedGateBApproval,
    PinnedGateCApproval,
    ProjectionComparisonReceipt,
    ProjectionPolicy,
    ReconciliationCadence,
    ResourceGatePolicy,
    ResourceObservation,
    RollbackPointerEvent,
    RollbackReceipt,
    S75CompletionManifest,
    ShadowEquivalenceReceipt,
    ShadowEquivalenceSpec,
    ShadowPointerEvent,
    TopPointerEvent,
    validate_atomic_cutover,
    validate_full_reconciliation,
    validate_gate_b_approval,
    validate_rollback_receipt,
    validate_s75_completion,
    validate_shadow_equivalence,
    validate_shadow_pointer_event,
)

CUTOFF = date(2026, 8, 12)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _pin(label: str) -> ArtifactPin:
    content = f"{label}\n".encode()
    return ArtifactPin(
        path=f"fixtures/s7_5/lifecycle/{label}.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _reader(
    *approvals: PinnedGateBApproval | PinnedGateCApproval,
):
    exact = {item.artifact.path: _canonical_bytes(item.approval.to_dict()) for item in approvals}

    def read(path: str) -> bytes:
        if path in exact:
            return exact[path]
        label = path.rsplit("/", 1)[-1].removesuffix(".json")
        return f"{label}\n".encode()

    return read


def _resource_policy() -> ResourceGatePolicy:
    return ResourceGatePolicy(
        max_wall_clock_seconds=1800,
        max_peak_rss_bytes=2 * 1024**3,
        min_free_disk_bytes=40 * 1024**3,
        max_read_bytes=512 * 1024**2,
        max_write_bytes=256 * 1024**2,
        max_chain_resolution_milliseconds=5000,
    )


def _spec() -> ShadowEquivalenceSpec:
    return ShadowEquivalenceSpec(
        incremental_release_id=_digest("incremental-release"),
        full_oracle_release_id=_digest("full-oracle-release"),
        common_parent_release_id=_digest("common-parent-release"),
        source_binding_digest=_digest("shadow-source-binding"),
        schema_bundle_digest=_digest("schema-bundle"),
        transform_semantics_digest=_digest("transform-semantics"),
        identity_policy_bundle_id=_digest("identity-policy-bundle"),
        calendar_digest=_digest("calendar"),
        view=ViewKind.LATEST_REVIEWED_RESEARCH,
        comparison_cutoff_session=date(2026, 8, 7),
        comparison_sessions=(
            date(2022, 2, 8),
            date(2025, 1, 2),
            date(2026, 7, 10),
        ),
        projection_policies=tuple(
            ProjectionPolicy(
                projection=projection,
                semantics_digest=_digest(f"projection-{projection.value}"),
            )
            for projection in EquivalenceProjection
        ),
        resource_policy=_resource_policy(),
    )


def _shadow_receipt(spec: ShadowEquivalenceSpec) -> ShadowEquivalenceReceipt:
    comparisons = tuple(
        ProjectionComparisonReceipt(
            projection=policy.projection,
            semantics_digest=policy.semantics_digest,
            compared_row_count=100
            if policy.projection is EquivalenceProjection.CANONICAL_RESEARCH
            else 20,
            incremental_projection_digest=_digest(f"result-{policy.projection.value}"),
            oracle_projection_digest=_digest(f"result-{policy.projection.value}"),
            unexpected_difference_count=0,
            details_artifact=_pin(f"comparison-{policy.projection.value}"),
        )
        for policy in spec.projection_policies
    )
    failures = tuple(
        FailureRecoveryReceipt(
            scenario=scenario,
            exercise_digest=_digest(f"exercise-{scenario.value}"),
            parent_reader_before_digest=_digest("parent-reader"),
            parent_reader_after_digest=_digest("parent-reader"),
            unpublished_visible_count=0,
            deleted_artifact_count=0,
            details_artifact=_pin(f"failure-{scenario.value}"),
        )
        for scenario in FailureScenario
    )
    return ShadowEquivalenceReceipt(
        spec_id=spec.spec_id,
        incremental_release_id=spec.incremental_release_id,
        full_oracle_release_id=spec.full_oracle_release_id,
        source_binding_digest=spec.source_binding_digest,
        comparisons=comparisons,
        resource_observation=ResourceObservation(
            wall_clock_seconds=600,
            peak_rss_bytes=1024**3,
            free_disk_bytes_at_floor=80 * 1024**3,
            read_bytes=128 * 1024**2,
            write_bytes=64 * 1024**2,
            chain_resolution_milliseconds=1000,
        ),
        failure_recovery=failures,
        idempotency=IdempotencyReceipt(
            first_run_receipt_id=_digest("run-receipt"),
            second_run_receipt_id=_digest("run-receipt"),
            first_checkpoint_id=_digest("checkpoint"),
            second_checkpoint_id=_digest("checkpoint"),
            first_release_id=spec.incremental_release_id,
            second_release_id=spec.incremental_release_id,
            first_manifest_sha256=_digest("release-manifest"),
            second_manifest_sha256=_digest("release-manifest"),
        ),
        receipt_available_session=date(2026, 8, 8),
    )


def _gate_b(spec: ShadowEquivalenceSpec, receipt: ShadowEquivalenceReceipt) -> PinnedGateBApproval:
    return PinnedGateBApproval.freeze(
        GateBApproval(
            spec_id=spec.spec_id,
            receipt_id=receipt.receipt_id,
            shadow_release_id=spec.incremental_release_id,
            full_oracle_release_id=spec.full_oracle_release_id,
            approver_id="research_owner",
            approval_available_session=date(2026, 8, 9),
        ),
        path="controls/s7_5/gate-b.json",
    )


def _shadow_event(gate_b: PinnedGateBApproval) -> ShadowPointerEvent:
    return ShadowPointerEvent(
        gate_b_approval_id=gate_b.approval.approval_id,
        gate_b_approval_artifact=gate_b.artifact,
        expected_previous_event_id=None,
        previous_release_id=None,
        new_release_id=gate_b.approval.shadow_release_id,
        pointer_revision=1,
        event_available_session=date(2026, 8, 9),
    )


def _rollback_event(
    spec: ShadowEquivalenceSpec,
    event: ShadowPointerEvent,
) -> RollbackPointerEvent:
    return RollbackPointerEvent(
        forward_shadow_event_id=event.event_id,
        expected_previous_event_id=event.event_id,
        previous_release_id=event.new_release_id,
        new_release_id=spec.common_parent_release_id,
        pointer_revision=event.pointer_revision + 1,
        event_available_session=date(2026, 8, 10),
    )


def _rollback(
    spec: ShadowEquivalenceSpec,
    event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
) -> RollbackReceipt:
    return RollbackReceipt(
        shadow_pointer_event_id=event.event_id,
        rollback_pointer_event_id=rollback_event.event_id,
        rolled_back_release_id=event.new_release_id,
        selected_parent_release_id=spec.common_parent_release_id,
        parent_reader_before_digest=_digest("rollback-parent-reader"),
        parent_reader_after_digest=_digest("rollback-parent-reader"),
        deleted_artifact_count=0,
        surviving_artifact_set_digest=_digest("surviving-artifacts"),
        details_artifact=_pin("rollback-details"),
        receipt_available_session=date(2026, 8, 10),
    )


def _gate_c(
    spec: ShadowEquivalenceSpec,
    gate_b: PinnedGateBApproval,
    shadow_event: ShadowPointerEvent,
    rollback: RollbackReceipt,
) -> PinnedGateCApproval:
    return PinnedGateCApproval.freeze(
        GateCApproval(
            gate_b_approval_id=gate_b.approval.approval_id,
            shadow_pointer_event_id=shadow_event.event_id,
            rollback_receipt_id=rollback.receipt_id,
            expected_previous_pointer_event_id=_digest("legacy-top-event"),
            expected_previous_release_id=spec.common_parent_release_id,
            expected_previous_pointer_revision=1,
            target_pointer_revision=2,
            target_release_id=spec.incremental_release_id,
            approver_id="research_owner",
            approval_available_session=date(2026, 8, 10),
        ),
        path="controls/s7_5/gate-c.json",
    )


def _top_event(gate_c: PinnedGateCApproval) -> TopPointerEvent:
    return TopPointerEvent(
        gate_c_approval_id=gate_c.approval.approval_id,
        gate_c_approval_artifact=gate_c.artifact,
        expected_previous_event_id=gate_c.approval.expected_previous_pointer_event_id,
        previous_release_id=gate_c.approval.expected_previous_release_id,
        new_release_id=gate_c.approval.target_release_id,
        pointer_revision=2,
        event_available_session=date(2026, 8, 10),
    )


def _full_spec(spec: ShadowEquivalenceSpec) -> FullReconciliationSpec:
    table_scopes = tuple(
        FullReconciliationTableScope(
            table_name=table,
            partition_keys=("2026-08-07" if table == "universe_daily" else "__table__",),
        )
        for table in ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
    )
    return FullReconciliationSpec(
        incremental_top_release_id=spec.incremental_release_id,
        independent_full_candidate_release_id=_digest("i7-full-candidate"),
        bronze_source_binding_digest=_digest("i7-bronze-binding"),
        s4_source_binding_digest=_digest("i7-s4-binding"),
        schema_bundle_digest=spec.schema_bundle_digest,
        transform_semantics_digest=spec.transform_semantics_digest,
        identity_policy_bundle_id=spec.identity_policy_bundle_id,
        calendar_digest=spec.calendar_digest,
        view=spec.view,
        reconciliation_cutoff_session=date(2026, 8, 7),
        canonical_projection_semantics_digest=_digest("i7-canonical-projection"),
        checkpoint_rebase_semantics_digest=_digest("i7-checkpoint-rebase"),
        trigger_policy_digest=_digest("i7-trigger-policy"),
        table_scopes=table_scopes,
        cadence=ReconciliationCadence.MONTHLY,
    )


def _full_receipt(spec: FullReconciliationSpec) -> FullReconciliationReceipt:
    rows = {
        "asset_master": 14_865,
        "ticker_alias": 33_081,
        "issuer_master": 14_955,
        "universe_daily": 70_000_000,
    }
    evidence = tuple(
        FullReconciliationTableEvidence(
            table_name=table,
            semantics_digest=spec.table_semantics_digest(table),
            partitions=(
                FullReconciliationPartitionEvidence(
                    table_name=table,
                    partition_key=("2026-08-07" if table == "universe_daily" else "__table__"),
                    compared_row_count=rows[table],
                    incremental_projection_digest=_digest(f"i7-{table}-projection"),
                    full_projection_digest=_digest(f"i7-{table}-projection"),
                    unexpected_difference_count=0,
                    details_artifact=_pin(f"i7-{table}-partition-details"),
                ),
            ),
        )
        for table in ("asset_master", "ticker_alias", "issuer_master", "universe_daily")
    )
    return FullReconciliationReceipt(
        spec_id=spec.spec_id,
        incremental_top_release_id=spec.incremental_top_release_id,
        independent_full_candidate_release_id=spec.independent_full_candidate_release_id,
        table_evidence=evidence,
        checkpoint_before_projection_digest=_digest("i7-checkpoint-projection"),
        checkpoint_rebased_projection_digest=_digest("i7-checkpoint-projection"),
        qa_artifact=_pin("i7-qa-receipt"),
        details_artifact=_pin("i7-reconciliation-details"),
        receipt_available_session=date(2026, 8, 11),
    )


def _completion(
    shadow_receipt: ShadowEquivalenceReceipt,
    gate_b: PinnedGateBApproval,
    shadow_event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
    rollback: RollbackReceipt,
    gate_c: PinnedGateCApproval,
    top_event: TopPointerEvent,
    full_receipt: FullReconciliationReceipt,
) -> S75CompletionManifest:
    ids = {
        "legacy": _digest("legacy-s7-release-set"),
        "i2": _digest("i2-acceptance"),
        "i3": _digest("i3-acceptance"),
        "i4": _digest("i4-acceptance"),
    }
    required = {
        1: {ids["legacy"]},
        2: {ids["i2"], ids["i3"]},
        3: {ids["i2"], ids["i3"]},
        4: {shadow_receipt.receipt_id},
        5: {shadow_receipt.receipt_id},
        6: {shadow_receipt.receipt_id},
        7: {ids["i3"]},
        8: {ids["i4"]},
        9: {ids["i3"], ids["i4"]},
        10: {shadow_receipt.receipt_id, full_receipt.receipt_id},
        11: {rollback_event.event_id, rollback.receipt_id},
        12: {ids["i2"], ids["i3"], ids["i4"]},
        13: {ids["i3"]},
    }
    criteria = tuple(
        AcceptanceCriterionReceipt(
            criterion_number=number,
            criterion_id=criterion_id,
            semantics_digest=_digest(f"criterion-{number}-semantics"),
            evidence_ids=tuple(sorted(required[number])),
            passed=True,
        )
        for number, criterion_id in enumerate(ACCEPTANCE_CRITERIA, 1)
    )
    return S75CompletionManifest(
        legacy_s7_release_set_id=ids["legacy"],
        gate_a_approval_id=_digest("gate-a-approval"),
        i2_acceptance_receipt_id=ids["i2"],
        i3_acceptance_receipt_id=ids["i3"],
        i4_acceptance_receipt_id=ids["i4"],
        shadow_equivalence_receipt_id=shadow_receipt.receipt_id,
        gate_b_approval_id=gate_b.approval.approval_id,
        shadow_pointer_event_id=shadow_event.event_id,
        rollback_pointer_event_id=rollback_event.event_id,
        rollback_receipt_id=rollback.receipt_id,
        gate_c_approval_id=gate_c.approval.approval_id,
        top_pointer_event_id=top_event.event_id,
        full_reconciliation_receipt_id=full_receipt.receipt_id,
        final_top_release_id=top_event.new_release_id,
        acceptance_criteria=criteria,
        completion_available_session=date(2026, 8, 12),
    )


def _chain():
    spec = _spec()
    receipt = _shadow_receipt(spec)
    gate_b = _gate_b(spec, receipt)
    shadow_event = _shadow_event(gate_b)
    rollback_event = _rollback_event(spec, shadow_event)
    rollback = _rollback(spec, shadow_event, rollback_event)
    gate_c = _gate_c(spec, gate_b, shadow_event, rollback)
    top_event = _top_event(gate_c)
    full_spec = _full_spec(spec)
    full_receipt = _full_receipt(full_spec)
    completion = _completion(
        receipt,
        gate_b,
        shadow_event,
        rollback_event,
        rollback,
        gate_c,
        top_event,
        full_receipt,
    )
    return (
        spec,
        receipt,
        gate_b,
        shadow_event,
        rollback_event,
        rollback,
        gate_c,
        top_event,
        full_spec,
        full_receipt,
        completion,
    )


def _atomic_kwargs(
    spec: ShadowEquivalenceSpec,
    receipt: ShadowEquivalenceReceipt,
    gate_b: PinnedGateBApproval,
    shadow_event: ShadowPointerEvent,
    rollback_event: RollbackPointerEvent,
    gate_c: PinnedGateCApproval,
) -> dict[str, object]:
    return {
        "gate_b": gate_b,
        "shadow_spec": spec,
        "shadow_receipt": receipt,
        "shadow_event": shadow_event,
        "rollback_event": rollback_event,
        "shadow_observed_previous_event_id": None,
        "shadow_observed_previous_release_id": None,
        "shadow_observed_previous_pointer_revision": 0,
        "rollback_observed_current_event_id": shadow_event.event_id,
        "rollback_observed_current_release_id": shadow_event.new_release_id,
        "rollback_observed_current_pointer_revision": shadow_event.pointer_revision,
        "observed_current_event_id": gate_c.approval.expected_previous_pointer_event_id,
        "observed_current_release_id": spec.common_parent_release_id,
        "observed_current_pointer_revision": 1,
        "availability_cutoff_session": CUTOFF,
        "artifact_reader": _reader(gate_b, gate_c),
    }


def _completion_kwargs(chain: tuple[object, ...]) -> dict[str, object]:
    (
        spec,
        receipt,
        gate_b,
        shadow_event,
        rollback_event,
        rollback,
        gate_c,
        _top_event_value,
        full_spec,
        full_receipt,
        _completion_value,
    ) = chain
    assert isinstance(spec, ShadowEquivalenceSpec)
    assert isinstance(receipt, ShadowEquivalenceReceipt)
    assert isinstance(gate_b, PinnedGateBApproval)
    assert isinstance(shadow_event, ShadowPointerEvent)
    assert isinstance(rollback_event, RollbackPointerEvent)
    assert isinstance(gate_c, PinnedGateCApproval)
    assert isinstance(full_spec, FullReconciliationSpec)
    return {
        "shadow_spec": spec,
        "shadow_receipt": receipt,
        "gate_b": gate_b,
        "shadow_event": shadow_event,
        "rollback_event": rollback_event,
        "rollback_receipt": rollback,
        "gate_c": gate_c,
        "full_reconciliation_spec": full_spec,
        "full_reconciliation_receipt": full_receipt,
        "shadow_observed_previous_event_id": None,
        "shadow_observed_previous_release_id": None,
        "shadow_observed_previous_pointer_revision": 0,
        "rollback_observed_current_event_id": shadow_event.event_id,
        "rollback_observed_current_release_id": shadow_event.new_release_id,
        "rollback_observed_current_pointer_revision": shadow_event.pointer_revision,
        "research_observed_current_event_id": (gate_c.approval.expected_previous_pointer_event_id),
        "research_observed_current_release_id": spec.common_parent_release_id,
        "research_observed_current_pointer_revision": 1,
        "availability_cutoff_session": CUTOFF,
        "artifact_reader": _reader(gate_b, gate_c),
    }


def test_shadow_contracts_have_canonical_ids_and_exact_projection_order() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    assert spec.spec_id == stable_digest(spec.logical_payload())
    assert receipt.receipt_id == stable_digest(receipt.logical_payload())
    assert tuple(item.projection for item in spec.projection_policies) == tuple(
        EquivalenceProjection
    )
    assert tuple(item.scenario for item in receipt.failure_recovery) == tuple(FailureScenario)
    validate_shadow_equivalence(
        spec,
        receipt,
        availability_cutoff_session=CUTOFF,
        artifact_reader=_reader(),
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unexpected_difference_count", 1, "unexpected differences"),
        ("oracle_projection_digest", _digest("different"), "projection digests differ"),
        ("compared_row_count", 0, "covered no rows"),
    ],
)
def test_shadow_projection_failures_block(field: str, value: object, message: str) -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    comparisons = (replace(receipt.comparisons[0], **{field: value}), receipt.comparisons[1])
    with pytest.raises(IncrementalLifecycleError, match=message):
        validate_shadow_equivalence(
            spec,
            replace(receipt, comparisons=comparisons),
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_projection_policy_must_cover_both_layers_in_closed_order() -> None:
    spec = _spec()
    with pytest.raises(IncrementalLifecycleError, match="exact closed order"):
        replace(spec, projection_policies=tuple(reversed(spec.projection_policies)))


@pytest.mark.parametrize(
    "observation_patch",
    [
        {"wall_clock_seconds": 1801},
        {"peak_rss_bytes": 2 * 1024**3 + 1},
        {"free_disk_bytes_at_floor": 40 * 1024**3 - 1},
        {"read_bytes": 512 * 1024**2 + 1},
        {"write_bytes": 256 * 1024**2 + 1},
        {"chain_resolution_milliseconds": 5001},
    ],
)
def test_each_resource_hard_gate_blocks(observation_patch: dict[str, int]) -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    broken = replace(receipt.resource_observation, **observation_patch)
    with pytest.raises(IncrementalLifecycleError, match="resource gate failed"):
        validate_shadow_equivalence(
            spec,
            replace(receipt, resource_observation=broken),
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("parent_reader_after_digest", _digest("changed-parent"), "changed the parent"),
        ("unpublished_visible_count", 1, "exposed unpublished"),
        ("deleted_artifact_count", 1, "deleted an artifact"),
    ],
)
def test_failure_recovery_gates_block(field: str, value: object, message: str) -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    failures = (
        replace(receipt.failure_recovery[0], **{field: value}),
        *receipt.failure_recovery[1:],
    )
    with pytest.raises(IncrementalLifecycleError, match=message):
        validate_shadow_equivalence(
            spec,
            replace(receipt, failure_recovery=failures),
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_idempotency_requires_manifest_sha_and_all_durable_ids() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    assert receipt.idempotency.reproduces
    broken = replace(receipt.idempotency, second_manifest_sha256=_digest("retry-changed"))
    with pytest.raises(IncrementalLifecycleError, match="not idempotent"):
        validate_shadow_equivalence(
            spec,
            replace(receipt, idempotency=broken),
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_gate_b_exact_pin_and_receipt_binding() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    gate_b = _gate_b(spec, receipt)
    approval_bytes = (
        json.dumps(
            gate_b.approval.to_dict(),
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    assert gate_b.artifact.sha256 == hashlib.sha256(approval_bytes).hexdigest()
    assert gate_b.artifact.bytes == len(approval_bytes)
    validate_gate_b_approval(
        gate_b,
        spec=spec,
        receipt=receipt,
        availability_cutoff_session=CUTOFF,
        artifact_reader=_reader(gate_b),
    )
    wrong_gate_b = PinnedGateBApproval.freeze(
        replace(gate_b.approval, receipt_id=_digest("wrong")),
        path="controls/s7_5/gate-b-wrong.json",
    )
    with pytest.raises(IncrementalLifecycleError, match="receipt differs"):
        validate_gate_b_approval(
            wrong_gate_b,
            spec=spec,
            receipt=receipt,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(wrong_gate_b),
        )
    with pytest.raises(IncrementalLifecycleError, match="stored bytes differ"):
        validate_gate_b_approval(
            gate_b,
            spec=spec,
            receipt=receipt,
            availability_cutoff_session=CUTOFF,
            artifact_reader=lambda _path: b"tampered\n",
        )


def test_resigned_gate_b_body_with_stale_pin_is_rejected() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    gate_b = _gate_b(spec, receipt)
    with pytest.raises(IncrementalLifecycleError, match="exact bytes"):
        PinnedGateBApproval(
            approval=replace(gate_b.approval, approver_id="another_owner"),
            artifact=gate_b.artifact,
        )


def test_shadow_pointer_isolated_namespace_and_compare_and_swap() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    gate_b = _gate_b(spec, receipt)
    event = _shadow_event(gate_b)
    assert event.pointer_name == SHADOW_POINTER_NAME
    validate_shadow_pointer_event(
        event,
        gate_b=gate_b,
        spec=spec,
        receipt=receipt,
        observed_current_event_id=None,
        observed_current_release_id=None,
        observed_current_pointer_revision=0,
        availability_cutoff_session=CUTOFF,
        artifact_reader=_reader(gate_b),
    )
    with pytest.raises(IncrementalLifecycleError, match="lost compare-and-swap race"):
        validate_shadow_pointer_event(
            event,
            gate_b=gate_b,
            spec=spec,
            receipt=receipt,
            observed_current_event_id=_digest("racing-event"),
            observed_current_release_id=None,
            observed_current_pointer_revision=0,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(gate_b),
        )
    with pytest.raises(IncrementalLifecycleError, match="research pointer"):
        replace(event, pointer_name=RESEARCH_POINTER_NAME)


def test_rollback_selects_exact_parent_without_deletion() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    gate_b = _gate_b(spec, receipt)
    event = _shadow_event(gate_b)
    rollback_event = _rollback_event(spec, event)
    rollback = _rollback(spec, event, rollback_event)
    validate_rollback_receipt(
        rollback,
        shadow_event=event,
        rollback_event=rollback_event,
        expected_parent_release_id=spec.common_parent_release_id,
        observed_current_event_id=event.event_id,
        observed_current_release_id=event.new_release_id,
        observed_current_pointer_revision=event.pointer_revision,
        availability_cutoff_session=CUTOFF,
        artifact_reader=_reader(),
    )
    with pytest.raises(IncrementalLifecycleError, match="deleted immutable"):
        validate_rollback_receipt(
            replace(rollback, deleted_artifact_count=1),
            shadow_event=event,
            rollback_event=rollback_event,
            expected_parent_release_id=spec.common_parent_release_id,
            observed_current_event_id=event.event_id,
            observed_current_release_id=event.new_release_id,
            observed_current_pointer_revision=event.pointer_revision,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )
    with pytest.raises(IncrementalLifecycleError, match="lost compare-and-swap race"):
        validate_rollback_receipt(
            rollback,
            shadow_event=event,
            rollback_event=rollback_event,
            expected_parent_release_id=spec.common_parent_release_id,
            observed_current_event_id=_digest("racing-shadow-event"),
            observed_current_release_id=event.new_release_id,
            observed_current_pointer_revision=event.pointer_revision,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_rollback_revision_must_immediately_follow_shadow_event() -> None:
    spec, _, _, shadow_event, rollback_event, rollback, *_ = _chain()
    skipped_revision = replace(
        rollback_event,
        pointer_revision=rollback_event.pointer_revision + 1,
    )
    rebound_receipt = replace(
        rollback,
        rollback_pointer_event_id=skipped_revision.event_id,
    )
    with pytest.raises(IncrementalLifecycleError, match="immediately increment"):
        validate_rollback_receipt(
            rebound_receipt,
            shadow_event=shadow_event,
            rollback_event=skipped_revision,
            expected_parent_release_id=spec.common_parent_release_id,
            observed_current_event_id=shadow_event.event_id,
            observed_current_release_id=shadow_event.new_release_id,
            observed_current_pointer_revision=skipped_revision.pointer_revision - 1,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_gate_c_atomic_top_pointer_chain_passes() -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, gate_c, top_event, *_ = _chain()
    validate_atomic_cutover(
        gate_c,
        top_event,
        rollback_receipt=rollback,
        **_atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c),
    )
    assert top_event.pointer_name == RESEARCH_POINTER_NAME
    assert top_event.new_release_id == receipt.incremental_release_id


def test_gate_c_reads_the_exact_pinned_approval_bytes() -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, gate_c, top_event, *_ = _chain()
    exact_reader = _reader(gate_b, gate_c)

    def tamper_gate_c(path: str) -> bytes:
        if path == gate_c.artifact.path:
            return b"tampered\n"
        return exact_reader(path)

    kwargs = _atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c)
    kwargs["artifact_reader"] = tamper_gate_c
    with pytest.raises(IncrementalLifecycleError, match="Gate C approval stored bytes differ"):
        validate_atomic_cutover(
            gate_c,
            top_event,
            rollback_receipt=rollback,
            **kwargs,
        )


@pytest.mark.parametrize(
    ("observed_event", "observed_release", "message"),
    [
        (_digest("racing-top-event"), None, "approval prior event differs"),
        (None, _digest("racing-release"), "approval prior release differs"),
    ],
)
def test_gate_c_lost_compare_and_swap_race_blocks(
    observed_event: str | None, observed_release: str | None, message: str
) -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, gate_c, top_event, *_ = _chain()
    current_event = observed_event or gate_c.approval.expected_previous_pointer_event_id
    current_release = observed_release or spec.common_parent_release_id
    kwargs = _atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c)
    kwargs["observed_current_event_id"] = current_event
    kwargs["observed_current_release_id"] = current_release
    with pytest.raises(IncrementalLifecycleError, match=message):
        validate_atomic_cutover(
            gate_c,
            top_event,
            rollback_receipt=rollback,
            **kwargs,
        )


def test_gate_c_revision_compare_and_swap_race_blocks() -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, gate_c, top_event, *_ = _chain()
    kwargs = _atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c)
    kwargs["observed_current_pointer_revision"] = 2
    with pytest.raises(IncrementalLifecycleError, match="prior revision differs"):
        validate_atomic_cutover(
            gate_c,
            top_event,
            rollback_receipt=rollback,
            **kwargs,
        )


def test_gate_c_cannot_skip_rollback_receipt() -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, gate_c, top_event, *_ = _chain()
    wrong = replace(rollback, surviving_artifact_set_digest=_digest("other-artifact-set"))
    with pytest.raises(IncrementalLifecycleError, match="another rollback"):
        validate_atomic_cutover(
            gate_c,
            top_event,
            rollback_receipt=wrong,
            **_atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c),
        )


def test_gate_c_rejects_exactly_approved_but_destructive_rollback() -> None:
    spec, receipt, gate_b, shadow_event, rollback_event, rollback, *_ = _chain()
    destructive = replace(rollback, deleted_artifact_count=1)
    gate_c = _gate_c(spec, gate_b, shadow_event, destructive)
    top_event = _top_event(gate_c)
    with pytest.raises(IncrementalLifecycleError, match="deleted immutable"):
        validate_atomic_cutover(
            gate_c,
            top_event,
            rollback_receipt=destructive,
            **_atomic_kwargs(spec, receipt, gate_b, shadow_event, rollback_event, gate_c),
        )


def test_full_reconciliation_and_checkpoint_rebase_pass() -> None:
    spec = _full_spec(_spec())
    receipt = _full_receipt(spec)
    assert spec.spec_id == stable_digest(spec.logical_payload())
    assert receipt.receipt_id == stable_digest(receipt.logical_payload())
    validate_full_reconciliation(
        spec,
        receipt,
        availability_cutoff_session=CUTOFF,
        artifact_reader=_reader(),
    )


def test_full_reconciliation_requires_exact_table_coverage() -> None:
    spec = _full_spec(_spec())
    receipt = _full_receipt(spec)
    with pytest.raises(IncrementalLifecycleError, match="exact four-table order"):
        replace(receipt, table_evidence=receipt.table_evidence[:-1])


def test_full_reconciliation_rejects_partial_or_wrong_partition_scope() -> None:
    spec = _full_spec(_spec())
    receipt = _full_receipt(spec)
    table = receipt.table_evidence[-1]
    wrong_partition = replace(table.partitions[0], partition_key="2026-08-06")
    wrong_table = replace(table, partitions=(wrong_partition,))
    wrong_receipt = replace(
        receipt,
        table_evidence=(*receipt.table_evidence[:-1], wrong_table),
    )
    with pytest.raises(IncrementalLifecycleError, match="partition scope differs"):
        validate_full_reconciliation(
            spec,
            wrong_receipt,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_full_reconciliation_reads_each_partition_evidence_pin() -> None:
    spec = _full_spec(_spec())
    receipt = _full_receipt(spec)
    target = receipt.table_evidence[-1].partitions[0].details_artifact.path
    exact_reader = _reader()

    def tamper_partition(path: str) -> bytes:
        if path == target:
            return b"tampered\n"
        return exact_reader(path)

    with pytest.raises(IncrementalLifecycleError, match="stored bytes differ"):
        validate_full_reconciliation(
            spec,
            receipt,
            availability_cutoff_session=CUTOFF,
            artifact_reader=tamper_partition,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unexpected_difference_count", 1, "partition has unexpected differences"),
        ("full_projection_digest", _digest("different-full"), "partition projection digests"),
    ],
)
def test_full_reconciliation_failures_block(field: str, value: object, message: str) -> None:
    spec = _full_spec(_spec())
    receipt = _full_receipt(spec)
    table = receipt.table_evidence[0]
    partition = replace(table.partitions[0], **{field: value})
    broken_table = replace(table, partitions=(partition,))
    broken = replace(receipt, table_evidence=(broken_table, *receipt.table_evidence[1:]))
    with pytest.raises(IncrementalLifecycleError, match=message):
        validate_full_reconciliation(
            spec,
            broken,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_full_reconciliation_rejects_checkpoint_rebase_change() -> None:
    spec = _full_spec(_spec())
    receipt = replace(
        _full_receipt(spec),
        checkpoint_rebased_projection_digest=_digest("changed-checkpoint"),
    )
    with pytest.raises(IncrementalLifecycleError, match="checkpoint rebase changed"):
        validate_full_reconciliation(
            spec,
            receipt,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )


def test_completion_manifest_binds_exact_thirteen_criteria_and_chain() -> None:
    chain = _chain()
    completion = chain[-1]
    assert len(completion.acceptance_criteria) == 13
    assert completion.completion_id == stable_digest(completion.logical_payload())
    validate_s75_completion(
        completion,
        top_event=chain[7],
        **_completion_kwargs(chain),
    )


def test_completion_rejects_failed_criterion() -> None:
    chain = _chain()
    completion = chain[-1]
    criteria = list(completion.acceptance_criteria)
    criteria[12] = replace(criteria[12], passed=False)
    broken = replace(completion, acceptance_criteria=tuple(criteria))
    with pytest.raises(IncrementalLifecycleError, match="criterion failed"):
        validate_s75_completion(
            broken,
            top_event=chain[7],
            **_completion_kwargs(chain),
        )


def test_completion_rejects_missing_required_evidence_even_if_passed() -> None:
    chain = _chain()
    completion = chain[-1]
    criteria = list(completion.acceptance_criteria)
    criteria[9] = replace(
        criteria[9],
        evidence_ids=(completion.shadow_equivalence_receipt_id,),
    )
    with pytest.raises(IncrementalLifecycleError, match="criterion 10 lacks"):
        validate_s75_completion(
            replace(completion, acceptance_criteria=tuple(criteria)),
            top_event=chain[7],
            **_completion_kwargs(chain),
        )


def test_completion_rejects_swapped_top_event() -> None:
    chain = _chain()
    completion = chain[-1]
    wrong_event = replace(chain[7], event_available_session=date(2026, 8, 11))
    with pytest.raises(IncrementalLifecycleError, match="top-pointer event differs"):
        validate_s75_completion(
            completion,
            top_event=wrong_event,
            **_completion_kwargs(chain),
        )


def test_completion_replays_gate_b_shadow_validator() -> None:
    chain = _chain()
    receipt = chain[1]
    invalid_comparison = replace(
        receipt.comparisons[0],
        unexpected_difference_count=1,
    )
    invalid_receipt = replace(
        receipt,
        comparisons=(invalid_comparison, *receipt.comparisons[1:]),
    )
    kwargs = _completion_kwargs(chain)
    kwargs["shadow_receipt"] = invalid_receipt
    with pytest.raises(IncrementalLifecycleError, match="unexpected differences"):
        validate_s75_completion(
            chain[-1],
            top_event=chain[7],
            **kwargs,
        )


def test_completion_replays_exact_gate_b_artifact_read() -> None:
    chain = _chain()
    gate_b = chain[2]
    exact_reader = _reader(gate_b, chain[6])

    def tamper_gate_b(path: str) -> bytes:
        if path == gate_b.artifact.path:
            return b"tampered\n"
        return exact_reader(path)

    kwargs = _completion_kwargs(chain)
    kwargs["artifact_reader"] = tamper_gate_b
    with pytest.raises(IncrementalLifecycleError, match="Gate B approval stored bytes differ"):
        validate_s75_completion(
            chain[-1],
            top_event=chain[7],
            **kwargs,
        )


def test_completion_replays_gate_c_and_rollback_cas_validator() -> None:
    chain = _chain()
    kwargs = _completion_kwargs(chain)
    kwargs["rollback_observed_current_event_id"] = _digest("racing-shadow-event")
    with pytest.raises(IncrementalLifecycleError, match="lost compare-and-swap race"):
        validate_s75_completion(
            chain[-1],
            top_event=chain[7],
            **kwargs,
        )


def test_completion_replays_i7_partition_validator() -> None:
    chain = _chain()
    receipt = chain[9]
    table = receipt.table_evidence[-1]
    partition = replace(table.partitions[0], unexpected_difference_count=1)
    invalid_table = replace(table, partitions=(partition,))
    invalid_receipt = replace(
        receipt,
        table_evidence=(*receipt.table_evidence[:-1], invalid_table),
    )
    kwargs = _completion_kwargs(chain)
    kwargs["full_reconciliation_receipt"] = invalid_receipt
    with pytest.raises(IncrementalLifecycleError, match="partition has unexpected differences"):
        validate_s75_completion(
            chain[-1],
            top_event=chain[7],
            **kwargs,
        )


def test_availability_time_cannot_be_backdated() -> None:
    spec = _spec()
    receipt = _shadow_receipt(spec)
    with pytest.raises(IncrementalLifecycleError, match="unavailable at cutoff"):
        validate_shadow_equivalence(
            spec,
            receipt,
            availability_cutoff_session=date(2026, 8, 7),
            artifact_reader=_reader(),
        )
    backdated = replace(receipt, receipt_available_session=date(2026, 8, 6))
    with pytest.raises(IncrementalLifecycleError, match="predates its comparison cutoff"):
        validate_shadow_equivalence(
            spec,
            backdated,
            availability_cutoff_session=CUTOFF,
            artifact_reader=_reader(),
        )
