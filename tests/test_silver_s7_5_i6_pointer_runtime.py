from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_silver_s7_5_i3_production_contract import (
    _delta_append_outputs,
    _run_spec,
)

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i6_pointer_runtime as runtime
from ame_stocks_api.silver.incremental_contract import ArtifactPin, ManifestPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    NATIVE_V2_RELEASE_FAMILY,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionCompletion,
    I3ProductionDeepVerificationAttestation,
    I3ProductionOutputSet,
    I3ProductionResourceObservation,
    I3ProductionRunKind,
    I3ProductionRunReceipt,
    I3ProductionRunState,
    production_physical_index_digest,
)
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    FailureRecoveryReceipt,
    FailureScenario,
    GateBApproval,
    GateCApproval,
    IdempotencyReceipt,
    PinnedGateBApproval,
    ProjectionComparisonReceipt,
    ResourceGatePolicy,
    ResourceObservation,
    ShadowEquivalenceReceipt,
)
from ame_stocks_api.silver.incremental_i5_shadow_runtime import (
    I5_PRODUCTION_AUTHORITY,
    I5_REQUIRED_COMPARISON_SESSIONS,
    I5_SCOPE_ARTIFACT,
    ShadowRunCompletion,
    ShadowRunSpec,
)

SESSION = date(2026, 7, 10)
AVAILABLE = date(2026, 8, 8)


def _digest(label: str) -> str:
    return stable_digest({"test": label})


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    )


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _authority_pin(label: str) -> runtime.ReleaseAuthorityPin:
    run_spec_id = _digest(f"{label}-run-spec")
    prefix = f"manifests/silver/identity/s7-5-native-v2-staging/run_spec_id={run_spec_id}"
    return runtime.ReleaseAuthorityPin(
        completion_artifact=ArtifactPin(
            path=f"{prefix}/completion.json",
            sha256=_digest(f"{label}-completion"),
            bytes=100,
        ),
        deep_attestation_artifact=ArtifactPin(
            path=f"{prefix}/deep-verification-attestation.json",
            sha256=_digest(f"{label}-deep"),
            bytes=100,
        ),
    )


def _freeze_i3_authority(
    root: Path,
    *,
    run_spec,
    output_set: I3ProductionOutputSet,
    parent_native: NativeV2ParentReleasePin | None,
    parent_checkpoint_id: str | None,
    parent_deep_id: str | None,
) -> tuple[runtime.ReleaseAuthorityPin, NativeV2ParentReleasePin, str]:
    native = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=run_spec.terminal_session,
        release_available_session=run_spec.run_available_session,
        native_v2_migration_id=run_spec.native_v2_migration_id,
        identity_policy_bundle_id=(run_spec.identity_policy_bundle.identity_policy_bundle_id),
        transform_semantics_digest=run_spec.transform_semantics_digest,
        resolved_state_digest=output_set.resolved_state_digest,
        output_artifacts=tuple(item.manifest_output for item in output_set.table_outputs),
        parent_release_id=(None if parent_native is None else parent_native.release_id),
        source_checkpoint_id=parent_checkpoint_id,
    )
    native_path = (
        "manifests/silver/identity/s7-5-native-v2-staging/native-releases/"
        f"release_id={native.release_id}/manifest.json"
    )
    native_pin = NativeV2ParentReleasePin.from_manifest(native, path=native_path)
    output_set = replace(
        output_set,
        release_manifest_artifact=native_pin.manifest,
        release_id=native.release_id,
    )
    control_root = (
        f"manifests/silver/identity/s7-5-native-v2-staging/run_spec_id={run_spec.run_spec_id}"
    )
    spec_pin = _write(root, f"{control_root}/run-spec.json", run_spec.canonical_bytes())
    receipt = I3ProductionRunReceipt(
        run_spec_id=run_spec.run_spec_id,
        run_spec_artifact=spec_pin,
        state=I3ProductionRunState.SUCCEEDED,
        receipt_available_session=run_spec.run_available_session,
        resource_observation=I3ProductionResourceObservation(
            peak_rss_bytes=1,
            elapsed_seconds=1,
            minimum_disk_free_bytes=100 * 1024**3,
            temporary_bytes=1,
        ),
        output_set=output_set,
    )
    receipt_pin = _write(
        root, f"{control_root}/production-run-receipt.json", receipt.canonical_bytes()
    )
    completion = I3ProductionCompletion(
        run_spec_id=run_spec.run_spec_id,
        receipt_id=receipt.receipt_id,
        receipt_artifact=receipt_pin,
        output_set_id=output_set.output_set_id,
        release_id=output_set.gate_a_manifest_pin.release_id,
        native_v2_envelope_id=output_set.release_id,
        checkpoint_id=output_set.checkpoint_id,
        completion_available_session=run_spec.run_available_session,
    )
    completion_pin = _write(root, f"{control_root}/completion.json", completion.canonical_bytes())
    deep = I3ProductionDeepVerificationAttestation(
        completion_id=completion.completion_id,
        completion_artifact=completion_pin,
        gate_a_manifest_pin=output_set.gate_a_manifest_pin,
        native_v2_release=native_pin,
        checkpoint_id=output_set.checkpoint_id,
        checkpoint_artifact=output_set.checkpoint_artifact,
        output_set_id=output_set.output_set_id,
        row_semantic_attestation_digest=_digest(f"row-semantics-{run_spec.run_kind.value}"),
        terminal_state_digest=_digest(f"terminal-{run_spec.run_kind.value}"),
        physical_index_digest=production_physical_index_digest(output_set),
        parent_frontier_attestation_digest=parent_deep_id,
        attestation_available_session=run_spec.run_available_session,
        verification_resource_observation=I3ProductionResourceObservation(
            peak_rss_bytes=1,
            elapsed_seconds=1,
            minimum_disk_free_bytes=100 * 1024**3,
            temporary_bytes=1,
        ),
    )
    deep_pin = _write(
        root,
        f"{control_root}/deep-verification-attestation.json",
        deep.canonical_bytes(),
    )
    return (
        runtime.ReleaseAuthorityPin(completion_pin, deep_pin),
        native_pin,
        deep.deep_attestation_id,
    )


def _freeze_exact_i3_chain(root: Path) -> runtime.ReleaseChainBinding:
    delta_template, base_output, delta_outputs = _delta_append_outputs()
    base_spec = _run_spec()
    assert base_spec.i2_base_frontier is not None
    base_spec = replace(
        base_spec,
        terminal_session=date(2026, 7, 9),
        i2_base_frontier=replace(
            base_spec.i2_base_frontier,
            terminal_session=date(2026, 7, 9),
        ),
    )
    base_binding, base_native, base_deep_id = _freeze_i3_authority(
        root,
        run_spec=base_spec,
        output_set=base_output,
        parent_native=None,
        parent_checkpoint_id=None,
        parent_deep_id=None,
    )
    delta_spec = replace(
        delta_template,
        parent_release=base_native,
        parent_checkpoint_artifact=base_output.checkpoint_artifact,
        parent_gate_a_manifest=base_output.gate_a_manifest_pin,
        parent_shadow_completion_artifact=base_binding.completion_artifact,
        parent_deep_attestation_artifact=base_binding.deep_attestation_artifact,
    )
    delta_gate_release = _digest("delta-gate-a-release")
    delta_output = replace(
        base_output,
        release_manifest_artifact=ArtifactPin(
            path="manifests/silver/identity/s7-5-native-v2-staging/delta-release.json",
            sha256=_digest("delta-release-placeholder"),
            bytes=100,
        ),
        checkpoint_artifact=ArtifactPin(
            path="manifests/silver/identity/s7-5-native-v2-staging/delta-checkpoint.json",
            sha256=_digest("delta-checkpoint-bytes"),
            bytes=100,
        ),
        release_id=_digest("delta-native-placeholder"),
        checkpoint_id=_digest("delta-checkpoint"),
        resolved_state_digest=_digest("delta-resolved-state"),
        resolved_content_digest=_digest("delta-resolved-content"),
        table_outputs=delta_outputs,
        gate_a_manifest_pin=ManifestPin(
            release_id=delta_gate_release,
            manifest_path=(
                "manifests/silver/incremental/i3/gate-a/"
                f"release_id={delta_gate_release}/manifest.json"
            ),
            manifest_sha256=_digest("delta-gate-a-bytes"),
            manifest_bytes=100,
            release_available_session=delta_spec.run_available_session,
        ),
    )
    delta_binding, _, _ = _freeze_i3_authority(
        root,
        run_spec=delta_spec,
        output_set=delta_output,
        parent_native=base_native,
        parent_checkpoint_id=base_output.checkpoint_id,
        parent_deep_id=base_deep_id,
    )
    return runtime.ReleaseChainBinding(base=base_binding, delta=delta_binding)


class Fixture:
    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.root = root
        self.base = runtime.ResolvedRelease(
            release_id=_digest("base-release"),
            native_v2_release_id=_digest("base-native"),
            run_kind=I3ProductionRunKind.BASE,
            terminal_session=date(2026, 7, 9),
            parent_release_id=None,
            reader_digest=_digest("base-reader"),
        )
        self.delta = runtime.ResolvedRelease(
            release_id=_digest("delta-release"),
            native_v2_release_id=_digest("delta-native"),
            run_kind=I3ProductionRunKind.DELTA,
            terminal_session=SESSION,
            parent_release_id=self.base.release_id,
            reader_digest=_digest("delta-reader"),
        )
        self.resolved_chain = runtime.ResolvedReleaseChain(self.base, self.delta)
        self.binding = runtime.ReleaseChainBinding(
            base=_authority_pin("base"), delta=_authority_pin("delta")
        )
        monkeypatch.setattr(
            runtime,
            "_load_release_chain_exact",
            lambda _root, binding: self._resolve(binding),
        )
        self.shadow = self._shadow_authority()

        def load_shadow(
            _root: Path,
            *,
            completion_pin: ArtifactPin,
            approval_pin: ArtifactPin | None,
            availability_cutoff_session: date,
        ):
            assert completion_pin == self.shadow_completion_pin
            assert availability_cutoff_session >= AVAILABLE
            if approval_pin is None:
                return None
            assert approval_pin == self.gate_b_pin
            return self.shadow

        monkeypatch.setattr(runtime, "_load_shadow_authority", load_shadow)

    def _resolve(self, binding: runtime.ReleaseChainBinding):
        assert binding == self.binding
        return self.resolved_chain

    def _details(self, label: str) -> ArtifactPin:
        return _write(
            self.root,
            f"manifests/silver/incremental/i5/details/{label}.json",
            _canonical({"label": label}),
        )

    def _shadow_authority(self):
        resource = ResourceGatePolicy(
            max_wall_clock_seconds=100,
            max_peak_rss_bytes=1_000_000,
            min_free_disk_bytes=1,
            max_read_bytes=1_000_000,
            max_write_bytes=1_000_000,
            max_chain_resolution_milliseconds=100,
        )
        incremental_pin = self.binding.delta.completion_artifact
        deep_pin = self.binding.delta.deep_attestation_artifact
        oracle_pin = ArtifactPin(
            path="manifests/silver/identity/full/oracle-completion.json",
            sha256=_digest("oracle-completion"),
            bytes=100,
        )
        spec = ShadowRunSpec(
            authority=I5_PRODUCTION_AUTHORITY,
            incremental_completion_artifact=incremental_pin,
            incremental_deep_attestation_artifact=deep_pin,
            full_oracle_completion_artifact=oracle_pin,
            incremental_release_id=self.delta.release_id,
            full_oracle_release_id=_digest("oracle-release"),
            common_parent_release_id=self.base.release_id,
            source_binding_digest=_digest("source-binding"),
            schema_bundle_digest=_digest("schema-bundle"),
            transform_semantics_digest=_digest("transform-semantics"),
            identity_policy_bundle_id=_digest("identity-policy"),
            calendar_digest=_digest("calendar"),
            scope_artifact=I5_SCOPE_ARTIFACT,
            comparison_sessions=I5_REQUIRED_COMPARISON_SESSIONS,
            receipt_available_session=AVAILABLE,
            resource_policy=resource,
        )
        projection_digest = _digest("projection")
        comparisons = tuple(
            ProjectionComparisonReceipt(
                projection=policy.projection,
                semantics_digest=policy.semantics_digest,
                compared_row_count=1,
                incremental_projection_digest=projection_digest,
                oracle_projection_digest=projection_digest,
                unexpected_difference_count=0,
                details_artifact=self._details(f"comparison-{policy.projection.value}"),
            )
            for policy in spec.projection_policies
        )
        parent_reader = _digest("parent-reader")
        failures = tuple(
            FailureRecoveryReceipt(
                scenario=scenario,
                exercise_digest=_digest(f"exercise-{scenario.value}"),
                parent_reader_before_digest=parent_reader,
                parent_reader_after_digest=parent_reader,
                unpublished_visible_count=0,
                deleted_artifact_count=0,
                details_artifact=self._details(f"failure-{scenario.value}"),
            )
            for scenario in FailureScenario
        )
        receipt_id = _digest("idempotent-run")
        checkpoint_id = _digest("idempotent-checkpoint")
        manifest_sha = _digest("idempotent-manifest")
        receipt = ShadowEquivalenceReceipt(
            spec_id=spec.lifecycle_spec.spec_id,
            incremental_release_id=self.delta.release_id,
            full_oracle_release_id=spec.full_oracle_release_id,
            source_binding_digest=spec.source_binding_digest,
            comparisons=comparisons,
            resource_observation=ResourceObservation(
                wall_clock_seconds=1,
                peak_rss_bytes=1,
                free_disk_bytes_at_floor=1,
                read_bytes=1,
                write_bytes=1,
                chain_resolution_milliseconds=1,
            ),
            failure_recovery=failures,
            idempotency=IdempotencyReceipt(
                first_run_receipt_id=receipt_id,
                second_run_receipt_id=receipt_id,
                first_checkpoint_id=checkpoint_id,
                second_checkpoint_id=checkpoint_id,
                first_release_id=self.delta.release_id,
                second_release_id=self.delta.release_id,
                first_manifest_sha256=manifest_sha,
                second_manifest_sha256=manifest_sha,
            ),
            receipt_available_session=AVAILABLE,
        )
        spec_path = (
            "manifests/silver/incremental/i5/shadow-runs/"
            f"run_spec_id={spec.run_spec_id}/run-spec.json"
        )
        spec_pin = ArtifactPin(
            path=spec_path,
            sha256=hashlib.sha256(spec.canonical_bytes()).hexdigest(),
            bytes=len(spec.canonical_bytes()),
        )
        completion = ShadowRunCompletion(
            run_spec_id=spec.run_spec_id,
            run_spec_artifact=spec_pin,
            receipt=receipt,
            receipt_available_session=AVAILABLE,
            authority=I5_PRODUCTION_AUTHORITY,
        )
        self.shadow_completion_pin = ArtifactPin(
            path=(
                "manifests/silver/incremental/i5/shadow-runs/"
                f"run_spec_id={spec.run_spec_id}/completion.json"
            ),
            sha256=hashlib.sha256(completion.canonical_bytes()).hexdigest(),
            bytes=len(completion.canonical_bytes()),
        )
        approval = GateBApproval(
            spec_id=spec.lifecycle_spec.spec_id,
            receipt_id=receipt.receipt_id,
            shadow_release_id=self.delta.release_id,
            full_oracle_release_id=spec.full_oracle_release_id,
            approver_id="research_owner",
            approval_available_session=AVAILABLE,
        )
        self.gate_b_pin = _write(
            self.root,
            (
                "manifests/silver/incremental/i5/gate-b/approvals/"
                f"approval_id={approval.approval_id}/approval.json"
            ),
            _canonical(approval.to_dict()),
        )
        return runtime._ShadowAuthority(
            spec=spec,
            completion=completion,
            gate_b=PinnedGateBApproval(approval=approval, artifact=self.gate_b_pin),
        )

    def install_research_parent(self) -> runtime.CurrentPointer:
        source_pin = _write(
            self.root,
            "manifests/silver/identity/s7-published/source-release.json",
            _canonical({"release_id": self.base.release_id}),
        )
        dummy = runtime.ResearchParentAnchor(
            event_id="0" * 64,
            release_chain=self.binding,
            selected_release_id=self.base.release_id,
            pointer_revision=1,
            available_session=AVAILABLE,
            source_publication_artifact=source_pin,
        )
        anchor = replace(dummy, event_id=dummy.reproduced_event_id)
        event_pin = _write(
            self.root,
            runtime._event_path(runtime.RESEARCH_POINTER_NAME, anchor.event_id),
            anchor.canonical_bytes(),
        )
        current = runtime.CurrentPointer(
            pointer_name=runtime.RESEARCH_POINTER_NAME,
            event_id=anchor.event_id,
            event_artifact=event_pin,
            release_id=self.base.release_id,
            pointer_revision=1,
            updated_session=AVAILABLE,
        )
        _write(
            self.root,
            runtime._current_path(runtime.RESEARCH_POINTER_NAME),
            current.canonical_bytes(),
        )
        return current


@pytest.fixture
def fx(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fixture:
    return Fixture(tmp_path, monkeypatch)


def _shadow_publish(fx: Fixture) -> tuple[ArtifactPin, ArtifactPin]:
    package = runtime.prepare_shadow_publish(
        fx.root,
        shadow_completion_artifact=fx.shadow_completion_pin,
        gate_b_approval_artifact=fx.gate_b_pin,
        release_chain=fx.binding,
        event_available_session=AVAILABLE,
    )
    receipt = runtime.stage_shadow_publish(fx.root, package)
    return package, receipt


def _rollback(fx: Fixture, forward: ArtifactPin) -> tuple[ArtifactPin, ArtifactPin]:
    package = runtime.prepare_shadow_rollback(
        fx.root,
        shadow_publish_receipt_artifact=forward,
        event_available_session=AVAILABLE,
    )
    receipt = runtime.stage_shadow_rollback(fx.root, package)
    return package, receipt


def _gate_c(fx: Fixture, rollback_stage_pin: ArtifactPin) -> ArtifactPin:
    rollback_stage = runtime._load_stage_receipt_exact(fx.root, rollback_stage_pin)
    assert rollback_stage.rollback_receipt_artifact is not None
    rollback = runtime._load_rollback_receipt_exact(
        fx.root, rollback_stage.rollback_receipt_artifact
    )
    rollback_package = runtime._load_package_exact(fx.root, rollback_stage.package_artifact)
    forward_stage, _, envelope = runtime._load_forward_context(fx.root, rollback_package)
    approval = GateCApproval(
        gate_b_approval_id=fx.shadow.gate_b.approval.approval_id,
        shadow_pointer_event_id=envelope.event_id,
        rollback_receipt_id=rollback.receipt_id,
        expected_previous_pointer_event_id=fx.research_parent.event_id,
        expected_previous_release_id=fx.base.release_id,
        expected_previous_pointer_revision=fx.research_parent.pointer_revision,
        target_pointer_revision=fx.research_parent.pointer_revision + 1,
        target_release_id=fx.delta.release_id,
        approver_id="research_owner",
        approval_available_session=AVAILABLE,
    )
    assert forward_stage.selected_release_id == fx.delta.release_id
    return _write(
        fx.root,
        (
            "manifests/silver/incremental/i6/gate-c/approvals/"
            f"approval_id={approval.approval_id}/approval.json"
        ),
        _canonical(approval.to_dict()),
    )


def test_missing_gate_b_freezes_only_awaiting_package(fx: Fixture) -> None:
    package_pin = runtime.prepare_shadow_publish(
        fx.root,
        shadow_completion_artifact=fx.shadow_completion_pin,
        gate_b_approval_artifact=None,
        release_chain=fx.binding,
        event_available_session=AVAILABLE,
    )
    package = runtime._load_package_exact(fx.root, package_pin)
    assert package.state is runtime.PointerPackageState.AWAITING_APPROVAL
    assert package.approval_artifact is None
    assert package.lifecycle_event is None
    with pytest.raises(runtime.I6PointerRuntimeError, match="awaiting"):
        runtime.stage_shadow_publish(fx.root, package_pin)
    assert not (fx.root / runtime._current_path(runtime.SHADOW_POINTER_NAME)).exists()


def test_initialize_research_parent_is_exact_idempotent_and_no_clobber(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _write(
        fx.root,
        "manifests/silver/identity/s7-published/release-set.json",
        _canonical({"release_id": fx.base.release_id}),
    )
    loaded_base = SimpleNamespace(
        resolved=fx.base,
        run_spec=SimpleNamespace(
            run_kind=I3ProductionRunKind.BASE,
            i0_oracle=SimpleNamespace(
                artifact=source,
                available_session=date(2026, 8, 3),
            ),
            run_available_session=date(2026, 8, 8),
        ),
        completion=SimpleNamespace(completion_available_session=date(2026, 8, 9)),
        deep=SimpleNamespace(attestation_available_session=date(2026, 8, 10)),
    )
    monkeypatch.setattr(
        runtime,
        "_load_release_authority_exact",
        lambda _root, binding: (
            loaded_base
            if binding == fx.binding.base
            else pytest.fail("initializer loaded a non-BASE authority")
        ),
    )

    first = runtime.initialize_research_parent(
        fx.root,
        release_chain=fx.binding,
        source_publication_artifact=source,
    )
    assert first.release_id == fx.base.release_id
    assert first.pointer_revision == 1
    assert runtime._read_current_required(
        fx.root, runtime.RESEARCH_POINTER_NAME
    ).updated_session == date(2026, 8, 10)
    event_paths = tuple((fx.root / runtime._EVENT_ROOT).rglob("event.json"))
    assert len(event_paths) == 1
    event_bytes = event_paths[0].read_bytes()
    current_path = fx.root / runtime._current_path(runtime.RESEARCH_POINTER_NAME)
    current_bytes = current_path.read_bytes()

    assert (
        runtime.initialize_research_parent(
            fx.root,
            release_chain=fx.binding,
            source_publication_artifact=source,
        )
        == first
    )
    assert event_paths[0].read_bytes() == event_bytes
    assert current_path.read_bytes() == current_bytes
    assert len(tuple((fx.root / runtime._EVENT_ROOT).rglob("event.json"))) == 1

    decoy = _write(
        fx.root,
        "manifests/silver/identity/s7-published/decoy.json",
        (fx.root / source.path).read_bytes(),
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="source publication"):
        runtime.initialize_research_parent(
            fx.root,
            release_chain=fx.binding,
            source_publication_artifact=decoy,
        )
    assert event_paths[0].read_bytes() == event_bytes
    assert current_path.read_bytes() == current_bytes


def test_initialize_research_parent_rejects_hardlink_and_existing_other_selector(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _write(
        fx.root,
        "manifests/silver/identity/s7-published/release-set.json",
        _canonical({"release_id": fx.base.release_id}),
    )
    loaded_base = SimpleNamespace(
        resolved=fx.base,
        run_spec=SimpleNamespace(
            run_kind=I3ProductionRunKind.BASE,
            i0_oracle=SimpleNamespace(artifact=source, available_session=AVAILABLE),
            run_available_session=AVAILABLE,
        ),
        completion=SimpleNamespace(completion_available_session=AVAILABLE),
        deep=SimpleNamespace(attestation_available_session=AVAILABLE),
    )
    monkeypatch.setattr(
        runtime,
        "_load_release_authority_exact",
        lambda _root, _binding: loaded_base,
    )
    hardlink = fx.root / "manifests/silver/identity/s7-published/hardlink.json"
    os.link(fx.root / source.path, hardlink)
    with pytest.raises(runtime.I6PointerRuntimeError, match="single regular file"):
        runtime.initialize_research_parent(
            fx.root,
            release_chain=fx.binding,
            source_publication_artifact=source,
        )
    assert not (fx.root / runtime._current_path(runtime.RESEARCH_POINTER_NAME)).exists()
    hardlink.unlink()

    other = runtime.CurrentPointer(
        pointer_name=runtime.RESEARCH_POINTER_NAME,
        event_id=_digest("other-anchor"),
        event_artifact=ArtifactPin(
            path=runtime._event_path(runtime.RESEARCH_POINTER_NAME, _digest("other-anchor")),
            sha256=_digest("other-anchor-bytes"),
            bytes=100,
        ),
        release_id=_digest("other-release"),
        pointer_revision=1,
        updated_session=AVAILABLE,
    )
    current_path = fx.root / runtime._current_path(runtime.RESEARCH_POINTER_NAME)
    _write(fx.root, runtime._current_path(runtime.RESEARCH_POINTER_NAME), other.canonical_bytes())
    before = current_path.read_bytes()
    with pytest.raises(runtime.I6PointerRuntimeError, match="another authority"):
        runtime.initialize_research_parent(
            fx.root,
            release_chain=fx.binding,
            source_publication_artifact=source,
        )
    assert current_path.read_bytes() == before
    assert not (fx.root / runtime._EVENT_ROOT).exists()


def test_first_shadow_publish_is_append_only_idempotent_and_not_research_visible(
    fx: Fixture,
) -> None:
    fx.research_parent = fx.install_research_parent()
    before = runtime.read_research_pointer(fx.root)
    package, receipt = _shadow_publish(fx)
    shadow = runtime.verify_shadow_publish(fx.root, receipt)
    assert shadow.release_id == fx.delta.release_id
    assert runtime.read_research_pointer(fx.root) == before
    assert runtime.stage_shadow_publish(fx.root, package) == receipt
    events = list((fx.root / runtime._EVENT_ROOT).rglob("event.json"))
    assert len(events) == 2  # imported research parent plus first shadow event
    assert not list((fx.root / runtime._POINTER_ROOT).rglob("*.swap-*"))


def test_rollback_restores_parent_without_deleting_delta(fx: Fixture) -> None:
    fx.research_parent = fx.install_research_parent()
    _, forward = _shadow_publish(fx)
    _, rollback = _rollback(fx, forward)
    view = runtime.verify_shadow_rollback(fx.root, rollback)
    assert view.release_id == fx.base.release_id
    receipt = runtime._load_stage_receipt_exact(fx.root, rollback)
    assert receipt.rollback_receipt_artifact is not None
    lifecycle = runtime._load_rollback_receipt_exact(fx.root, receipt.rollback_receipt_artifact)
    assert lifecycle.deleted_artifact_count == 0
    assert lifecycle.rolled_back_release_id == fx.delta.release_id
    forward_event = runtime._load_stage_receipt_exact(fx.root, forward).event_artifact
    assert (fx.root / forward_event.path).is_file()


def test_gate_c_cutover_lost_cas_retry_and_failed_replace_preserve_old_reader(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx.research_parent = fx.install_research_parent()
    _, forward = _shadow_publish(fx)
    _, rollback = _rollback(fx, forward)
    gate_c = _gate_c(fx, rollback)
    package = runtime.prepare_research_cutover(
        fx.root,
        shadow_rollback_receipt_artifact=rollback,
        gate_c_approval_artifact=gate_c,
        event_available_session=AVAILABLE,
    )
    old = runtime.read_research_pointer(fx.root)

    def fail(_path: Path, _temporary: Path) -> None:
        raise OSError("injected pointer replacement failure")

    monkeypatch.setattr(runtime, "_before_pointer_replace", fail)
    with pytest.raises(OSError, match="injected"):
        runtime.stage_research_cutover(fx.root, package)
    assert runtime.read_research_pointer(fx.root) == old

    old_current = runtime._read_current_required(fx.root, runtime.RESEARCH_POINTER_NAME)
    racer_event_id = _digest("noncooperating-racer-event")
    racer_current = replace(
        old_current,
        event_id=racer_event_id,
        event_artifact=ArtifactPin(
            path=runtime._event_path(runtime.RESEARCH_POINTER_NAME, racer_event_id),
            sha256=_digest("noncooperating-racer-bytes"),
            bytes=100,
        ),
    )

    def race(path: Path, _temporary: Path) -> None:
        path.write_bytes(racer_current.canonical_bytes())

    monkeypatch.setattr(runtime, "_before_pointer_replace", race)
    with pytest.raises(runtime.I6LostCompareAndSwap, match="race"):
        runtime.stage_research_cutover(fx.root, package)
    assert runtime._read_current_required(fx.root, runtime.RESEARCH_POINTER_NAME) == racer_current
    (fx.root / runtime._current_path(runtime.RESEARCH_POINTER_NAME)).write_bytes(
        old_current.canonical_bytes()
    )

    monkeypatch.setattr(runtime, "_before_pointer_replace", lambda _path, _temporary: None)
    receipt = runtime.stage_research_cutover(fx.root, package)
    view = runtime.verify_research_cutover(fx.root, receipt)
    assert view.release_id == fx.delta.release_id
    assert runtime.stage_research_cutover(fx.root, package) == receipt

    exact_binding = _freeze_exact_i3_chain(fx.root)
    exact_target = runtime._load_release_authority_exact(fx.root, exact_binding.delta)
    monkeypatch.setattr(
        runtime,
        "_load_release_authority_exact",
        lambda _root, _binding: replace(exact_target, resolved=fx.delta),
    )
    snapshot = runtime.load_research_top_snapshot_exact(fx.root)
    assert snapshot.release_id == fx.delta.release_id
    assert snapshot.producer_available_session == AVAILABLE
    assert tuple(item.table_name for item in snapshot.table_outputs) == (
        "asset_master",
        "ticker_alias",
        "issuer_master",
        "universe_daily",
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="exact authority maximum"):
        replace(snapshot, producer_available_session=date(2026, 8, 9))

    stale = runtime._load_package_exact(fx.root, package)
    changed = runtime._read_current_required(fx.root, runtime.RESEARCH_POINTER_NAME)
    with pytest.raises(runtime.I6LostCompareAndSwap):
        runtime._require_current_expectation(changed, stale.expected_current)


def test_gate_approval_parsers_reject_tamper_noncanonical_and_copied_path(
    fx: Fixture,
) -> None:
    loaded = runtime._load_gate_b_approval_exact(fx.root, fx.gate_b_pin)
    assert loaded.approval.approval_id == fx.shadow.gate_b.approval.approval_id
    original = (fx.root / fx.gate_b_pin.path).read_bytes()

    copied = _write(
        fx.root,
        "manifests/silver/incremental/i5/gate-b/approvals/copied/approval.json",
        original,
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="path"):
        runtime._load_gate_b_approval_exact(fx.root, copied)

    noncanonical_path = fx.gate_b_pin.path.replace("approval.json", "noncanonical.json")
    noncanonical = _write(
        fx.root,
        noncanonical_path,
        json.dumps(fx.shadow.gate_b.approval.to_dict(), indent=2).encode() + b"\n",
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="canonical JSON"):
        runtime._load_gate_b_approval_exact(fx.root, noncanonical)

    (fx.root / fx.gate_b_pin.path).write_bytes(original + b" ")
    with pytest.raises(runtime.I6PointerRuntimeError, match="exact pin"):
        runtime._load_gate_b_approval_exact(fx.root, fx.gate_b_pin)


def test_event_tamper_and_copied_package_fail_closed(fx: Fixture) -> None:
    fx.research_parent = fx.install_research_parent()
    package, receipt = _shadow_publish(fx)
    copied = _write(
        fx.root,
        "manifests/silver/incremental/i6/pointer-action-packages/copied/package.json",
        (fx.root / package.path).read_bytes(),
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="path"):
        runtime._load_package_exact(fx.root, copied)

    stage = runtime._load_stage_receipt_exact(fx.root, receipt)
    event_path = fx.root / stage.event_artifact.path
    event_path.chmod(0o600)
    event_path.write_bytes(event_path.read_bytes() + b" ")
    with pytest.raises(runtime.I6PointerRuntimeError, match="exact pin"):
        runtime.read_shadow_pointer(fx.root)


def test_no_latest_tmp_or_approval_writer_capability() -> None:
    source = Path(runtime.__file__).read_text(encoding="utf-8")
    assert "glob(" not in source
    assert "rglob(" not in source
    assert "create_gate_b" not in source
    assert "create_gate_c" not in source
    assert "sign_approval" not in source
    assert "latest/" not in source
    assert "fixtures/" not in source


def test_exact_i3_control_loader_replays_base_delta_and_rejects_copied_authority(
    tmp_path: Path,
) -> None:
    binding = _freeze_exact_i3_chain(tmp_path)
    chain = runtime._load_release_chain_exact(tmp_path, binding)
    assert chain.base.run_kind is I3ProductionRunKind.BASE
    assert chain.delta.run_kind is I3ProductionRunKind.DELTA
    assert chain.delta.parent_release_id == chain.base.release_id
    assert chain.delta.terminal_session == SESSION

    copied = _write(
        tmp_path,
        "manifests/silver/identity/s7-5-native-v2-staging/copied/completion.json",
        (tmp_path / binding.delta.completion_artifact.path).read_bytes(),
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="copied"):
        runtime._load_release_chain_exact(
            tmp_path,
            replace(
                binding,
                delta=replace(binding.delta, completion_artifact=copied),
            ),
        )


def test_i5_authority_loader_requires_production_replay_and_exact_i3_pins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_loader = runtime._load_shadow_authority
    fixture = Fixture(tmp_path, monkeypatch)
    _write(
        tmp_path,
        fixture.shadow.completion.run_spec_artifact.path,
        fixture.shadow.spec.canonical_bytes(),
    )
    observed: list[bool] = []

    def load_completion(
        _root: Path, completion_pin: ArtifactPin, *, production: bool = True
    ) -> ShadowRunCompletion:
        assert completion_pin == fixture.shadow_completion_pin
        observed.append(production)
        return fixture.shadow.completion

    monkeypatch.setattr(runtime, "load_i5_shadow_completion_exact", load_completion)
    loaded = original_loader(
        tmp_path,
        completion_pin=fixture.shadow_completion_pin,
        approval_pin=fixture.gate_b_pin,
        availability_cutoff_session=AVAILABLE,
    )
    assert loaded == fixture.shadow
    assert observed == [True]
    runtime._validate_shadow_release_binding(
        shadow=loaded,
        release_chain=fixture.binding,
        resolved_chain=fixture.resolved_chain,
    )

    wrong = replace(
        loaded,
        spec=replace(
            loaded.spec,
            incremental_deep_attestation_artifact=ArtifactPin(
                path="manifests/silver/incremental/i3/wrong/deep.json",
                sha256=_digest("wrong-deep"),
                bytes=100,
            ),
        ),
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="producer authority"):
        runtime._validate_shadow_release_binding(
            shadow=wrong,
            release_chain=fixture.binding,
            resolved_chain=fixture.resolved_chain,
        )


def test_approval_trust_requires_closed_approver_and_owner_only_acl(fx: Fixture) -> None:
    rogue = replace(fx.shadow.gate_b.approval, approver_id="rogue_reviewer")
    rogue_pin = _write(
        fx.root,
        (
            "manifests/silver/incremental/i5/gate-b/approvals/"
            f"approval_id={rogue.approval_id}/approval.json"
        ),
        _canonical(rogue.to_dict()),
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="trust root"):
        runtime._load_gate_b_approval_exact(fx.root, rogue_pin)

    approval_path = fx.root / fx.gate_b_pin.path
    approval_path.chmod(0o666)
    with pytest.raises(runtime.I6PointerRuntimeError, match="ACL trust root"):
        runtime._load_gate_b_approval_exact(fx.root, fx.gate_b_pin)


def test_rollback_reader_recursively_requires_forward_production_authority(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx.research_parent = fx.install_research_parent()
    _, forward = _shadow_publish(fx)
    _rollback(fx, forward)

    def reject_producer(*_args, **_kwargs):
        raise runtime.I6PointerRuntimeError("injected production I5 replay failure")

    monkeypatch.setattr(runtime, "_load_shadow_authority", reject_producer)
    with pytest.raises(runtime.I6PointerRuntimeError, match="production I5 replay"):
        runtime.read_shadow_pointer(fx.root)


def test_historical_receipt_and_envelope_mismatch_fail_closed(fx: Fixture) -> None:
    fx.research_parent = fx.install_research_parent()
    _, receipt_pin = _shadow_publish(fx)
    receipt = runtime._load_stage_receipt_exact(fx.root, receipt_pin)
    receipt_path = fx.root / receipt_pin.path
    receipt_path.chmod(0o600)
    receipt_path.write_bytes(
        replace(receipt, selected_release_id=fx.base.release_id).canonical_bytes()
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="stage receipt"):
        runtime.read_shadow_pointer(fx.root)

    receipt_path.write_bytes(receipt.canonical_bytes())
    event_path = fx.root / receipt.event_artifact.path
    envelope = runtime._load_event_envelope_exact(fx.root, receipt.event_artifact)
    event_path.chmod(0o600)
    tampered_envelope = replace(
        envelope,
        approval_artifact=ArtifactPin(
            path="manifests/silver/incremental/i5/gate-b/approvals/"
            f"approval_id={_digest('wrong-approval')}/approval.json",
            sha256=_digest("wrong-approval-bytes"),
            bytes=100,
        ),
    )
    tampered_event_bytes = tampered_envelope.canonical_bytes()
    event_path.write_bytes(tampered_event_bytes)
    tampered_event_pin = ArtifactPin(
        path=receipt.event_artifact.path,
        sha256=hashlib.sha256(tampered_event_bytes).hexdigest(),
        bytes=len(tampered_event_bytes),
    )
    current = runtime._read_current_required(fx.root, runtime.SHADOW_POINTER_NAME)
    (fx.root / runtime._current_path(runtime.SHADOW_POINTER_NAME)).write_bytes(
        replace(current, event_artifact=tampered_event_pin).canonical_bytes()
    )
    claim_path = fx.root / runtime._revision_claim_path(
        runtime.SHADOW_POINTER_NAME, envelope.event.pointer_revision
    )
    claim_path.chmod(0o600)
    claim_path.write_bytes(
        _canonical(runtime._revision_claim_payload(tampered_envelope, tampered_event_pin))
    )
    with pytest.raises(runtime.I6PointerRuntimeError, match="action package"):
        runtime.read_shadow_pointer(fx.root)


def test_current_updated_session_must_equal_head_event_availability(fx: Fixture) -> None:
    fx.research_parent = fx.install_research_parent()
    _shadow_publish(fx)
    current = runtime._read_current_required(fx.root, runtime.SHADOW_POINTER_NAME)
    current_path = fx.root / runtime._current_path(runtime.SHADOW_POINTER_NAME)
    current_path.write_bytes(replace(current, updated_session=date(2026, 8, 9)).canonical_bytes())
    with pytest.raises(runtime.I6PointerRuntimeError, match="ledger head"):
        runtime.read_shadow_pointer(fx.root)


def test_exact_reader_rejects_symlink_hardlink_and_path_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = _canonical({"control": "one"})
    target = tmp_path / "controls" / "control.json"
    target.parent.mkdir(parents=True)
    target.write_bytes(content)
    link = tmp_path / "controls" / "symlink.json"
    link.symlink_to(target)
    with pytest.raises(runtime.I6PointerRuntimeError, match="missing or unsafe"):
        runtime._ExactReader(tmp_path).read_path("controls/symlink.json")

    hardlink = tmp_path / "controls" / "hardlink.json"
    os.link(target, hardlink)
    with pytest.raises(runtime.I6PointerRuntimeError, match="single regular file"):
        runtime._ExactReader(tmp_path).read_path("controls/control.json")
    hardlink.unlink()

    replacement = tmp_path / "controls" / "replacement.json"
    replacement.write_bytes(_canonical({"control": "two"}))
    original_read = os.read
    swapped = False

    def swap_after_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        result = original_read(descriptor, size)
        if not swapped:
            swapped = True
            os.replace(replacement, target)
        return result

    monkeypatch.setattr(runtime.os, "read", swap_after_read)
    with pytest.raises(runtime.I6PointerRuntimeError, match="changed during same-fd read"):
        runtime._ExactReader(tmp_path).read_path("controls/control.json")


def test_resume_replays_full_authority_before_writing_success_receipt(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx.research_parent = fx.install_research_parent()
    package = runtime.prepare_shadow_publish(
        fx.root,
        shadow_completion_artifact=fx.shadow_completion_pin,
        gate_b_approval_artifact=fx.gate_b_pin,
        release_chain=fx.binding,
        event_available_session=AVAILABLE,
    )
    package_body = runtime._load_package_exact(fx.root, package)
    original_write = runtime._write_immutable

    def interrupt_receipt(root: Path, relative: str, content: bytes, *, label: str) -> ArtifactPin:
        if label == "pointer stage receipt":
            raise runtime.I6PointerRuntimeError("injected receipt interruption")
        return original_write(root, relative, content, label=label)

    monkeypatch.setattr(runtime, "_write_immutable", interrupt_receipt)
    with pytest.raises(runtime.I6PointerRuntimeError, match="receipt interruption"):
        runtime.stage_shadow_publish(fx.root, package)
    monkeypatch.setattr(runtime, "_write_immutable", original_write)

    def reject_resume(*_args, **_kwargs):
        raise runtime.I6PointerRuntimeError("resume producer authority tamper")

    monkeypatch.setattr(runtime, "_load_shadow_authority", reject_resume)
    with pytest.raises(runtime.I6PointerRuntimeError, match="producer authority tamper"):
        runtime.stage_shadow_publish(fx.root, package)
    assert not (
        fx.root
        / runtime._receipt_path(runtime.PointerAction.SHADOW_PUBLISH, package_body.package_id)
    ).exists()


@pytest.mark.parametrize("attack", ("missing_predecessor", "cycle", "revision_gap"))
def test_bounded_ledger_replay_rejects_revision_41_attacks(
    fx: Fixture, monkeypatch: pytest.MonkeyPatch, attack: str
) -> None:
    fx.research_parent = fx.install_research_parent()
    _, forward_pin = _shadow_publish(fx)
    _, rollback_pin = _rollback(fx, forward_pin)
    rollback_stage = runtime._load_stage_receipt_exact(fx.root, rollback_pin)
    head_envelope = runtime._load_event_envelope_exact(fx.root, rollback_stage.event_artifact)
    forward_stage = runtime._load_stage_receipt_exact(fx.root, forward_pin)
    predecessor_envelope = runtime._load_event_envelope_exact(fx.root, forward_stage.event_artifact)
    revision_41_event = replace(
        head_envelope.event,
        pointer_revision=41,
        expected_previous_event_id=predecessor_envelope.event_id,
        previous_release_id=predecessor_envelope.event.new_release_id,
    )
    revision_41_envelope = replace(
        head_envelope,
        event=revision_41_event,
        predecessor_event_artifact=forward_stage.event_artifact,
    )
    head_pin = ArtifactPin(
        path=runtime._event_path(runtime.SHADOW_POINTER_NAME, revision_41_event.event_id),
        sha256=_digest("revision-41-event"),
        bytes=100,
    )
    head = runtime._LedgerNode(
        event_id=revision_41_event.event_id,
        event_artifact=head_pin,
        release_id=revision_41_event.new_release_id,
        pointer_revision=41,
        available_session=revision_41_event.event_available_session,
        predecessor_event_artifact=forward_stage.event_artifact,
        envelope=revision_41_envelope,
    )
    predecessor = runtime._LedgerNode(
        event_id=predecessor_envelope.event_id,
        event_artifact=forward_stage.event_artifact,
        release_id=predecessor_envelope.event.new_release_id,
        pointer_revision=(40 if attack != "revision_gap" else 39),
        available_session=predecessor_envelope.event.event_available_session,
        predecessor_event_artifact=None,
        envelope=predecessor_envelope,
    )
    if attack == "cycle":
        head = replace(head, predecessor_event_artifact=head_pin)

    def load_node(
        _root: Path,
        *,
        pointer_name: str,
        event_artifact: ArtifactPin,
        require_stage_receipt: bool = True,
    ):
        assert type(require_stage_receipt) is bool
        assert pointer_name == runtime.SHADOW_POINTER_NAME
        if event_artifact == head_pin:
            return head
        if attack == "missing_predecessor":
            raise runtime.I6PointerRuntimeError("exact predecessor is missing")
        return predecessor

    monkeypatch.setattr(runtime, "_load_ledger_node_exact", load_node)
    current = runtime.CurrentPointer(
        pointer_name=runtime.SHADOW_POINTER_NAME,
        event_id=head.event_id,
        event_artifact=head.event_artifact,
        release_id=head.release_id,
        pointer_revision=41,
        updated_session=head.available_session,
    )
    message = {
        "missing_predecessor": "missing",
        "cycle": "cycle",
        "revision_gap": "continuity",
    }[attack]
    with pytest.raises(runtime.I6PointerRuntimeError, match=message):
        runtime._replay_pointer_ledger_exact(fx.root, current)
