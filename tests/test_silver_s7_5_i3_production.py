from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i3_migration_io import _base_fixture
from test_silver_s7_5_i3_production_contract import _delta_run_spec

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import (
    asset_release_set,
    calendar_artifact,
    identity_materialization_publish,
    identity_registry_workflow,
)
from ame_stocks_api.silver import identity_materialization_streaming as streaming
from ame_stocks_api.silver import incremental_i3_production as production
from ame_stocks_api.silver import incremental_i3_production_contract as contract
from ame_stocks_api.silver import incremental_i3_production_inputs as production_inputs
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    IncrementalContractError,
    RowVersionChangeIndexPin,
    RowVersionOperation,
    RowVersionReference,
    verify_content_attested_release,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import NativeV2ParentReleasePin
from ame_stocks_api.silver.incremental_i3_migration_io import (
    CompactBaseMigrationMaterializer,
)
from ame_stocks_api.silver.incremental_i3_production import (
    I3ProductionPreparedMaterialization,
    I3ProductionStageError,
    stage_i3_production_base,
    verify_i3_production_deep_attestation,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionI2ReceiptPin,
    I3ProductionOutputStorage,
    I3ProductionParentAuthority,
    I3ProductionRunKind,
    load_i3_production_parent_shallow_exact,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    production_native_v2_migration_id,
)


def _write_run_spec(root: Path, run_spec) -> object:
    relative = f"controls/i3/run_spec_id={run_spec.run_spec_id}/run-spec.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(run_spec.canonical_bytes())
    return run_spec.exact_pin(path=relative)


def _copy_exact_pin(root: Path, source: ArtifactPin, relative: str) -> ArtifactPin:
    content = (root / source.path).read_bytes()
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _patch_exact_upstreams(monkeypatch: pytest.MonkeyPatch, terminal) -> None:
    _patch_exact_dependencies(monkeypatch, terminal)
    monkeypatch.setattr(
        production,
        "_verify_prepared_materialization_authority",
        lambda _root, _spec, _prepared, *, parent: None,
    )


def _patch_exact_dependencies(monkeypatch: pytest.MonkeyPatch, terminal) -> None:
    monkeypatch.setattr(
        contract,
        "_verify_external_production_dependencies",
        lambda _root, _spec: (terminal,),
    )
    monkeypatch.setattr(
        contract,
        "_verify_i2_receipts_exact",
        lambda _root, _spec, *, calendar_sessions, parent_staging: None,
    )


def _patch_frozen_external_dependency_loaders(
    monkeypatch: pytest.MonkeyPatch,
    run_spec,
    *,
    source_binding_mutation: str | None = None,
) -> None:
    registry_pins = tuple(
        identity_registry_workflow.RegistryReleasePin(
            registry_name=item.registry_kind.value,
            release_id=item.release_id,
            manifest_path=item.artifact.path,
            manifest_sha256=item.artifact.sha256,
            manifest_bytes=item.artifact.bytes,
            release_available_session=item.release_available_session,
        )
        for item in run_spec.identity_policy_bundle.registry_releases
    )
    source_binding_id = stable_digest({"fixture": "frozen-source-binding"})
    bound_s4 = SimpleNamespace(
        path=run_spec.s4_v1_source.artifact.path,
        sha256=run_spec.s4_v1_source.artifact.sha256,
        bytes=run_spec.s4_v1_source.artifact.bytes,
    )
    bound_registries = registry_pins
    bound_calendar_sha256 = run_spec.calendar.artifact.sha256
    loaded_calendar_path = run_spec.calendar.artifact.path
    returned_source_binding_id = source_binding_id
    if source_binding_mutation == "source_binding":
        returned_source_binding_id = stable_digest({"tampered": "source-binding"})
    elif source_binding_mutation == "s4":
        bound_s4 = SimpleNamespace(
            path=bound_s4.path,
            sha256=stable_digest({"tampered": "s4"}),
            bytes=bound_s4.bytes,
        )
    elif source_binding_mutation == "registry":
        bound_registries = (
            *registry_pins[:-1],
            replace(
                registry_pins[-1],
                manifest_sha256=stable_digest({"tampered": "registry"}),
            ),
        )
    elif source_binding_mutation == "calendar":
        bound_calendar_sha256 = stable_digest({"tampered": "calendar"})
    elif source_binding_mutation == "calendar_path":
        loaded_calendar_path = "manifests/silver/xnys-calendars/copied-calendar.json"

    marker_content = b"frozen-s7-marker\n"

    def load_frozen_marker(pin: ArtifactPin, *, content: bytes):
        assert pin == run_spec.i0_oracle.artifact
        assert content == marker_content
        return {
            "release_set_id": run_spec.i0_oracle.object_id,
            "source_binding_id": source_binding_id,
        }

    monkeypatch.setattr(
        production_inputs,
        "load_frozen_s7_oracle_marker_exact",
        load_frozen_marker,
    )
    original_read_exact = contract._read_exact_root

    def read_exact(root: Path, pin: ArtifactPin) -> bytes:
        if pin == run_spec.i0_oracle.artifact:
            return marker_content
        if pin == run_spec.s4_v1_source.artifact:
            return b"frozen-s4-release-set\n"
        if pin == run_spec.identity_policy_bundle_artifact:
            return run_spec.identity_policy_bundle.canonical_bytes()
        return original_read_exact(root, pin)

    monkeypatch.setattr(contract, "_read_exact_root", read_exact)
    monkeypatch.setattr(
        streaming,
        "_load_source_binding",
        lambda _root, identifier: (
            SimpleNamespace(
                source_binding_id=returned_source_binding_id,
                mode="production",
                s4_release_set_id=run_spec.s4_v1_source.object_id,
                s4_release_set_manifest=bound_s4,
                registry_pins=bound_registries,
                calendar_artifact_id=run_spec.calendar.calendar_artifact_id,
                calendar_artifact_sha256=bound_calendar_sha256,
                cutoff_session=run_spec.s4_v1_source.available_session,
            ),
            SimpleNamespace(source_binding_id=identifier),
        ),
    )
    monkeypatch.setattr(
        streaming,
        "_repository_runtime_binding",
        lambda: (_ for _ in ()).throw(AssertionError("current runtime must not be probed")),
    )
    monkeypatch.setattr(
        identity_materialization_publish,
        "load_published_s7_release_set",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("publication-time loader must not be called")
        ),
    )
    monkeypatch.setattr(
        asset_release_set,
        "load_exact_asset_release_set_control",
        lambda *_args, **_kwargs: (
            SimpleNamespace(release_set_id=run_spec.s4_v1_source.object_id),
            SimpleNamespace(path=run_spec.s4_v1_source.artifact.path),
        ),
    )
    monkeypatch.setattr(
        calendar_artifact,
        "load_xnys_calendar_artifact",
        lambda *_args, **_kwargs: SimpleNamespace(
            content=b"x" * run_spec.calendar.artifact.bytes,
            relative_path=loaded_calendar_path,
            sha256=run_spec.calendar.artifact.sha256,
            sessions=(SimpleNamespace(session_date=run_spec.terminal_session),),
        ),
    )

    def load_historical_registries(
        _root: Path,
        pins,
        *,
        revalidate_current_runtime: bool,
    ) -> SimpleNamespace:
        assert revalidate_current_runtime is False
        return SimpleNamespace(
            releases=tuple(SimpleNamespace(manifest_pin=pin, source_scopes={}) for pin in pins)
        )

    monkeypatch.setattr(
        identity_registry_workflow,
        "load_registry_release_set",
        load_historical_registries,
    )


def _replace_identity_policy(run_spec, policy):
    policy_artifact = policy.exact_pin(path=run_spec.identity_policy_bundle_artifact.path)
    migration_id = production_native_v2_migration_id(
        i0_release_set_artifact=run_spec.i0_oracle.artifact,
        s4_release_set_artifact=run_spec.s4_v1_source.artifact,
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_artifact,
        calendar_artifact=run_spec.calendar.artifact,
        i2_base_frontier_artifact=run_spec.i2_base_frontier.artifact,
    )
    return replace(
        run_spec,
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_artifact,
        native_v2_migration_id=migration_id,
    )


def _canonical_external_run_spec(run_spec):
    releases = tuple(
        replace(
            item,
            decision_cutoff_session=item.release_available_session,
        )
        for item in run_spec.identity_policy_bundle.registry_releases
    )
    policy = replace(
        run_spec.identity_policy_bundle,
        registry_releases=releases,
        bundle_available_session=max(item.release_available_session for item in releases),
    )
    normalized = _replace_identity_policy(run_spec, policy)
    return replace(
        normalized,
        source_cutoff_session=max(
            policy.decision_cutoff_session,
            normalized.calendar.available_session,
        ),
    )


def _prepare_official_base(tmp_path: Path, run_spec, legacy, s4):
    workspace = tmp_path / "silver/schema=v2/identity/native_v2_staging" / "fixture-prepared-base"
    workspace.mkdir(parents=True, exist_ok=True)
    return CompactBaseMigrationMaterializer(legacy, s4).prepare(
        data_root=tmp_path,
        run_spec=run_spec,
        parent=None,
        workspace=workspace,
    )


def _malformed_compact_base_outputs(table_outputs, mutation: str):
    output = table_outputs[0]
    rowset = output.rowset_index
    assert rowset is not None
    assert len(rowset.segments) == 1
    segment = rowset.segments[0]
    if mutation == "direct_parquet":
        malformed = replace(
            output,
            storage=I3ProductionOutputStorage.PARQUET,
            manifest_output=replace(
                output.manifest_output,
                artifact=segment.artifact,
                row_count=segment.row_count,
            ),
            rowset_index=None,
        )
    elif mutation == "wrong_segment_id":
        bad_rowset = replace(
            rowset,
            segments=(
                replace(
                    segment,
                    segment_id=stable_digest({"malicious-segment-id": output.table_name}),
                ),
            ),
        )
        malformed = replace(
            output,
            manifest_output=replace(
                output.manifest_output,
                artifact=bad_rowset.exact_pin(path=output.manifest_output.artifact.path),
            ),
            rowset_index=bad_rowset,
        )
    elif mutation == "two_segments":
        second_artifact = ArtifactPin(
            path=segment.artifact.path.removesuffix(".parquet") + "-extra.parquet",
            sha256=stable_digest({"malicious-extra-segment": output.table_name}),
            bytes=segment.artifact.bytes,
        )
        bad_rowset = replace(
            rowset,
            segments=(
                segment,
                replace(
                    segment,
                    segment_id=stable_digest({"extra-segment-id": output.table_name}),
                    artifact=second_artifact,
                    row_count=0,
                ),
            ),
        )
        malformed = replace(
            output,
            manifest_output=replace(
                output.manifest_output,
                artifact=bad_rowset.exact_pin(path=output.manifest_output.artifact.path),
            ),
            rowset_index=bad_rowset,
        )
    else:  # pragma: no cover - test helper guard
        raise AssertionError(f"unknown compact-base mutation: {mutation}")
    return (malformed, *table_outputs[1:])


def _versioned_tables(result, root: Path) -> dict[ArtifactPin, pa.Table]:
    tables: dict[ArtifactPin, pa.Table] = {}
    output_set = result.loaded.receipt.output_set
    assert output_set is not None
    for output in output_set.table_outputs:
        if output.table_name == "universe_daily":
            continue
        if output.rowset_index is None:
            artifacts = (output.manifest_output.artifact,)
        else:
            artifacts = tuple(item.artifact for item in output.rowset_index.segments)
        for artifact in artifacts:
            tables[artifact] = pq.read_table(root / artifact.path)
    return tables


def _write_row_change_index_variant(
    root: Path,
    original: RowVersionChangeIndexPin,
    rows: list[dict[str, object]],
    *,
    label: str,
) -> RowVersionChangeIndexPin:
    table = pa.Table.from_pylist(rows, schema=contract._ROW_VERSION_CHANGE_INDEX_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6", write_statistics=True)
    content = sink.getvalue().to_pybytes()
    relative = f"silver/schema=v2/identity/native_v2_staging/tamper/{label}.parquet"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    superseded = tuple(
        sorted(
            str(item["predecessor_row_version_id"])
            for item in rows
            if item["predecessor_row_version_id"] is not None
        )
    )
    return RowVersionChangeIndexPin(
        artifact=ArtifactPin(
            path=relative,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        ),
        row_count=len(rows),
        logical_receipts_digest=contract._row_change_index_logical_receipts_digest(rows),
        superseded_row_version_count=len(superseded),
        superseded_row_version_ids_digest=(
            contract._row_change_index_supersession_digest(superseded)
        ),
        schema_digest=original.schema_digest,
        availability_session=original.availability_session,
    )


def test_stage_base_uses_frozen_exact_oracle_without_current_runtime_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    run_spec = _canonical_external_run_spec(run_spec)
    _patch_frozen_external_dependency_loaders(monkeypatch, run_spec)
    monkeypatch.setattr(
        contract,
        "_verify_i2_receipts_exact",
        lambda _root, _spec, *, calendar_sessions, parent_staging: None,
    )
    monkeypatch.setattr(
        production,
        "_verify_prepared_materialization_authority",
        lambda _root, _spec, _prepared, *, parent: None,
    )

    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )

    assert result.loaded.run_spec == run_spec
    assert result.loaded.completion.state.value == "awaiting_review"


@pytest.mark.parametrize(
    "mutation",
    ("source_binding", "s4", "registry", "calendar"),
)
def test_external_dependency_gate_rejects_frozen_source_binding_crosslink_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    run_spec = _canonical_external_run_spec(run_spec)
    _patch_frozen_external_dependency_loaders(
        monkeypatch,
        run_spec,
        source_binding_mutation=mutation,
    )

    with pytest.raises(
        contract.I3ProductionContractError,
        match="source binding differs from exact S4, registry, policy, or calendar inputs",
    ):
        contract._verify_external_production_dependencies(tmp_path, run_spec)


@pytest.mark.parametrize("mutation", ("decision_cutoff", "bundle_availability"))
def test_external_dependency_gate_rejects_noncanonical_identity_policy_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    run_spec = _canonical_external_run_spec(run_spec)
    policy = run_spec.identity_policy_bundle
    if mutation == "decision_cutoff":
        first = policy.registry_releases[0]
        releases = (
            replace(
                first,
                decision_cutoff_session=first.release_available_session - timedelta(days=1),
            ),
            *policy.registry_releases[1:],
        )
        policy = replace(policy, registry_releases=releases)
    else:
        policy = replace(
            policy,
            bundle_available_session=policy.bundle_available_session + timedelta(days=1),
        )
    forged_run_spec = _replace_identity_policy(run_spec, policy)
    _patch_frozen_external_dependency_loaders(monkeypatch, forged_run_spec)

    with pytest.raises(
        contract.I3ProductionContractError,
        match="source binding differs from exact S4, registry, policy, or calendar inputs",
    ):
        contract._verify_external_production_dependencies(tmp_path, forged_run_spec)


def test_external_dependency_gate_rejects_earlier_declared_source_cutoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    run_spec = _canonical_external_run_spec(run_spec)
    forged_run_spec = replace(
        run_spec,
        source_cutoff_session=run_spec.source_cutoff_session - timedelta(days=1),
    )
    _patch_frozen_external_dependency_loaders(monkeypatch, forged_run_spec)

    with pytest.raises(
        contract.I3ProductionContractError,
        match="source binding differs from exact S4, registry, policy, or calendar inputs",
    ):
        contract._verify_external_production_dependencies(tmp_path, forged_run_spec)


def test_external_dependency_gate_rejects_calendar_loader_path_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    run_spec = _canonical_external_run_spec(run_spec)
    _patch_frozen_external_dependency_loaders(
        monkeypatch,
        run_spec,
        source_binding_mutation="calendar_path",
    )

    with pytest.raises(
        contract.I3ProductionContractError,
        match="calendar loader returned another exact artifact",
    ):
        contract._verify_external_production_dependencies(tmp_path, run_spec)


def test_stage_base_writes_compact_row_index_single_fk_summary_and_deep_attestation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    run_spec_pin = _write_run_spec(tmp_path, run_spec)
    materializer = CompactBaseMigrationMaterializer(legacy, s4)

    first = stage_i3_production_base(
        tmp_path,
        run_spec_pin,
        materializer=materializer,
    )
    assert first.reused is False
    assert first.loaded.completion.release_id == first.loaded.gate_a_manifest.release_id
    assert first.loaded.completion.native_v2_envelope_id == first.loaded.manifest.release_id
    assert first.loaded.gate_a_manifest.added_row_version_receipts == ()
    row_change_index = first.loaded.gate_a_manifest.row_version_change_index
    assert row_change_index is not None
    assert row_change_index.row_count == 3
    assert row_change_index.superseded_row_version_count == 0
    assert (tmp_path / row_change_index.artifact.path).is_file()
    assert (
        verify_content_attested_release(first.loaded.gate_a_release) is first.loaded.gate_a_release
    )
    assert all(
        not item.row_version_references
        for item in first.loaded.gate_a_manifest.added_partition_receipts
    )
    assert first.deep_attestation_pin.path.endswith("/deep-verification-attestation.json")
    deep = contract.load_i3_production_deep_attestation_exact(
        first.deep_attestation_pin,
        lambda path: (tmp_path / path).read_bytes(),
    )
    deep.verification_resource_observation.validate_caps(run_spec.resource_caps)
    output_set = first.loaded.receipt.output_set
    assert output_set is not None
    authoritative_paths = {
        output_set.release_manifest_artifact.path,
        output_set.checkpoint_artifact.path,
        *(item.manifest_output.artifact.path for item in output_set.table_outputs),
        *(item.path for item in output_set.control_extension_artifacts),
    }
    for output in output_set.table_outputs:
        if output.dataset_index is not None:
            authoritative_paths.update(
                item.artifact.path for item in output.dataset_index.partitions
            )
        if output.rowset_index is not None:
            authoritative_paths.update(item.artifact.path for item in output.rowset_index.segments)
    assert not any(path == "tmp" or path.startswith("tmp/") for path in authoritative_paths)
    verify_i3_production_deep_attestation(
        tmp_path,
        first.completion_pin,
        first.deep_attestation_pin,
        expected_kind=I3ProductionRunKind.BASE,
    )

    qa = first.loaded.gate_a_run_receipt.qa_receipt
    assert qa is not None
    details_path = tmp_path / qa.results[0].details_artifact.path
    details = json.loads(details_path.read_bytes())
    summary = details["base_fk_verification_summary"]
    assert (tmp_path / summary["path"]).is_file()
    assert not list(tmp_path.rglob("base-partition-reference-index/*.parquet"))
    assert row_change_index.artifact in output_set.control_extension_artifacts
    assert any(item.path == summary["path"] for item in output_set.control_extension_artifacts)

    second = stage_i3_production_base(
        tmp_path,
        run_spec_pin,
        materializer=materializer,
    )
    assert second.reused is True
    assert second.completion_pin == first.completion_pin
    assert second.deep_attestation_pin == first.deep_attestation_pin


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("direct_parquet", "exactly one rowset segment"),
        ("wrong_segment_id", "module-owned identity"),
        ("two_segments", "exactly one rowset segment"),
    ),
)
def test_stage_base_rejects_malformed_initial_rowset_before_success_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    prepared = _prepare_official_base(tmp_path, run_spec, legacy, s4)
    malformed = replace(
        prepared,
        table_outputs=_malformed_compact_base_outputs(prepared.table_outputs, mutation),
    )

    class MaliciousMaterializer:
        def prepare(self, **_kwargs):
            return malformed

    with pytest.raises(I3ProductionStageError, match=message) as raised:
        stage_i3_production_base(
            tmp_path,
            _write_run_spec(tmp_path, run_spec),
            materializer=MaliciousMaterializer(),
        )
    assert raised.value.failed_receipt_pin is not None
    assert not (tmp_path / production._completion_relative(run_spec)).exists()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("direct_parquet", "exactly one rowset segment"),
        ("wrong_segment_id", "module-owned identity"),
        ("two_segments", "exactly one rowset segment"),
    ),
)
def test_durable_deep_loader_rejects_malformed_base_initial_rowset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    staged = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    output_set = staged.loaded.receipt.output_set
    assert output_set is not None
    bad_output_set = replace(
        output_set,
        table_outputs=_malformed_compact_base_outputs(output_set.table_outputs, mutation),
    )
    bad_receipt = replace(staged.loaded.receipt, output_set=bad_output_set)
    control_root = (
        f"manifests/silver/identity/s7-5-native-v2-staging/malicious-base-rowset-{mutation}"
    )
    receipt_path = f"{control_root}/run-receipt.json"
    receipt_pin = bad_receipt.exact_pin(path=receipt_path)
    receipt_target = tmp_path / receipt_path
    receipt_target.parent.mkdir(parents=True, exist_ok=True)
    receipt_target.write_bytes(bad_receipt.canonical_bytes())
    bad_completion = replace(
        staged.loaded.completion,
        receipt_id=bad_receipt.receipt_id,
        receipt_artifact=receipt_pin,
        output_set_id=bad_output_set.output_set_id,
    )
    completion_path = f"{control_root}/completion.json"
    completion_pin = bad_completion.exact_pin(path=completion_path)
    (tmp_path / completion_path).write_bytes(bad_completion.canonical_bytes())

    with pytest.raises(contract.I3ProductionContractError, match=message):
        contract.load_i3_production_staging_exact(tmp_path, completion_pin)


def test_stage_base_rejects_structural_unsealed_materializer_at_real_authority_seam(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_dependencies(monkeypatch, run_spec.terminal_session)
    official = _prepare_official_base(tmp_path, run_spec, legacy, s4)
    unsealed = I3ProductionPreparedMaterialization(
        table_outputs=official.table_outputs,
        native_manifest=official.native_manifest,
        native_manifest_artifact=official.native_manifest_artifact,
        checkpoint=official.checkpoint,
        checkpoint_artifact=official.checkpoint_artifact,
        source_digest=official.source_digest,
        resource_observation=official.resource_observation,
        canonical_projection_difference_count=0,
        row_versions=official.row_versions,
    )

    class StructuralMaterializer:
        def prepare(self, **_kwargs):
            return unsealed

    with pytest.raises(
        I3ProductionStageError, match="official nominal prepared capability"
    ) as raised:
        stage_i3_production_base(
            tmp_path,
            _write_run_spec(tmp_path, run_spec),
            materializer=StructuralMaterializer(),
        )
    assert raised.value.failed_receipt_pin is not None
    assert not (tmp_path / production._completion_relative(run_spec)).exists()


def test_external_authority_entrypoints_reject_exact_controls_copied_below_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    materializer = CompactBaseMigrationMaterializer(legacy, s4)
    source_run_spec = _write_run_spec(tmp_path, run_spec)
    temporary_run_spec = _copy_exact_pin(
        tmp_path,
        source_run_spec,
        "tmp/copied-run-spec.json",
    )
    with pytest.raises(I3ProductionStageError, match="temporary RunSpec"):
        stage_i3_production_base(
            tmp_path,
            temporary_run_spec,
            materializer=materializer,
        )

    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        source_run_spec,
        materializer=materializer,
    )
    temporary_completion = _copy_exact_pin(
        tmp_path,
        result.completion_pin,
        "tmp/copied-completion.json",
    )
    with pytest.raises(contract.I3ProductionContractError, match="temporary completion"):
        contract.load_i3_production_staging_exact(tmp_path, temporary_completion)
    with pytest.raises(I3ProductionStageError, match="temporary completion"):
        production.verify_i3_production_base(tmp_path, temporary_completion)

    temporary_deep = _copy_exact_pin(
        tmp_path,
        result.deep_attestation_pin,
        "tmp/copied-deep-attestation.json",
    )
    with pytest.raises(I3ProductionStageError, match="temporary deep attestation"):
        verify_i3_production_deep_attestation(
            tmp_path,
            result.completion_pin,
            temporary_deep,
            expected_kind=I3ProductionRunKind.BASE,
        )


@pytest.mark.parametrize(
    "relative",
    (
        "manifests/latest/i3/delta/run-spec.json",
        "manifests/silver/identity/s7-5-native-v2-staging/copied/run-spec.json",
    ),
)
def test_delta_run_spec_locator_is_not_factor_semantics(
    relative: str,
) -> None:
    run_spec = _delta_run_spec()
    pin = run_spec.exact_pin(path=relative)
    assert (
        production.validate_production_delta_run_spec_artifact_path(
            pin,
            run_spec_id=run_spec.run_spec_id,
        )
        == run_spec.run_spec_id
    )


def test_compact_index_writer_hashes_and_reads_each_physical_artifact_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    prepared = _prepare_official_base(tmp_path, run_spec, legacy, s4)
    first = prepared.row_versions[0]
    repeated_artifact = replace(
        prepared,
        row_versions=(first, replace(first, row_locator="row_index=1")),
    )
    verify_calls = 0
    read_calls = 0
    original_verify = production._verify_file
    original_read = production.pq.read_table

    def recording_verify(root: Path, artifact: ArtifactPin) -> None:
        nonlocal verify_calls
        if artifact == first.index_artifact:
            verify_calls += 1
        original_verify(root, artifact)

    def recording_read(where, *args, **kwargs):
        nonlocal read_calls
        if Path(where) == tmp_path / first.index_artifact.path:
            read_calls += 1
        return original_read(where, *args, **kwargs)

    monkeypatch.setattr(production, "_verify_file", recording_verify)
    monkeypatch.setattr(production.pq, "read_table", recording_read)
    with pytest.raises(I3ProductionStageError, match="locator exceeds"):
        production._gate_a_row_version_change_index(
            tmp_path,
            run_spec,
            repeated_artifact,
            parent=None,
        )
    assert verify_calls == 1
    assert read_calls == 1


def test_compact_gate_a_manifest_stays_small_at_real_62901_base_row_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    original = result.loaded.gate_a_manifest.row_version_change_index
    assert original is not None
    scaled = replace(
        original,
        row_count=62_901,
        logical_receipts_digest=stable_digest({"declared-row-count": 62_901}),
    )
    manifest = replace(result.loaded.gate_a_manifest, row_version_change_index=scaled)
    assert manifest.added_row_version_receipts == ()
    assert len(manifest.canonical_bytes()) < 64 * 1024
    assert 62_901 * 1_545 > 64 * 1024**2


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("order", "schema/order/count/digest"),
        ("omission", "does not cover the exact new physical rowset"),
        ("payload", "differs from its exact physical Parquet row"),
        ("supersession", "module-owned new roots"),
    ),
)
def test_compact_index_deep_replay_rejects_semantic_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    original = result.loaded.gate_a_manifest.row_version_change_index
    assert original is not None
    rows = pq.read_table(tmp_path / original.artifact.path).to_pylist()
    if mutation == "order":
        rows.reverse()
    elif mutation == "omission":
        rows.pop()
    elif mutation == "payload":
        rows[0]["row_payload_digest"] = stable_digest({"tamper": "payload"})
        rows[0]["semantic_proof_digest"] = contract._row_change_index_proof_digest(rows[0])
    else:
        rows[0]["operation"] = RowVersionOperation.MECHANICAL_SUCCESSOR.value
        rows[0]["predecessor_row_version_id"] = stable_digest({"tamper": "predecessor"})
        rows[0]["predecessor_payload_digest"] = stable_digest({"tamper": "predecessor-payload"})
        rows[0]["semantic_proof_digest"] = contract._row_change_index_proof_digest(rows[0])
    variant = _write_row_change_index_variant(
        tmp_path,
        original,
        rows,
        label=mutation,
    )
    manifest = replace(result.loaded.gate_a_manifest, row_version_change_index=variant)
    with pytest.raises(contract.I3ProductionContractError, match=message):
        contract._verify_gate_a_indexed_row_changes(
            tmp_path,
            manifest,
            result.loaded.checkpoint,
            versioned_tables_by_artifact=_versioned_tables(result, tmp_path),
            parent_staging=None,
        )


def test_base_fk_replay_rejects_absent_row_version_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    partition = result.loaded.gate_a_manifest.added_partition_receipts[0]
    missing = RowVersionReference(
        table_name="asset_master",
        row_version_id=stable_digest({"tamper": "missing-fk"}),
    )
    with pytest.raises(contract.I3ProductionContractError, match="absent physical row version"):
        contract._verify_partition_row_references_exact(
            run_spec,
            partition,
            references=(missing,),
            available_row_versions=set(),
        )


def test_corrupt_existing_completion_emits_failed_audit_without_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    run_spec_pin = _write_run_spec(tmp_path, run_spec)
    materializer = CompactBaseMigrationMaterializer(legacy, s4)
    first = stage_i3_production_base(tmp_path, run_spec_pin, materializer=materializer)
    completion_path = tmp_path / first.completion_pin.path
    corrupted = completion_path.read_bytes() + b"corrupt"
    completion_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    completion_path.write_bytes(corrupted)

    with pytest.raises(I3ProductionStageError, match="requires a new RunSpec") as raised:
        stage_i3_production_base(tmp_path, run_spec_pin, materializer=materializer)
    assert completion_path.read_bytes() == corrupted
    failed_pin = raised.value.failed_receipt_pin
    assert failed_pin is not None
    failed = contract.load_i3_production_run_receipt_exact(
        failed_pin,
        lambda path: (tmp_path / path).read_bytes(),
    )
    assert failed.failure_code == "completion_verification_failed_requires_new_run_spec"


def test_post_deep_resource_cap_failure_leaves_no_attestation_and_requires_new_run_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    observations = 0

    def controlled_observation(root: Path, prepared, *, started: float):
        nonlocal observations
        observations += 1
        if observations == 1:
            return prepared
        return replace(
            prepared,
            peak_rss_bytes=run_spec.resource_caps.rss_bytes_hard_cap + 1,
        )

    monkeypatch.setattr(production, "_combined_observation", controlled_observation)
    with pytest.raises(I3ProductionStageError, match="requires a new RunSpec") as raised:
        stage_i3_production_base(
            tmp_path,
            _write_run_spec(tmp_path, run_spec),
            materializer=CompactBaseMigrationMaterializer(legacy, s4),
        )
    assert observations == 2
    assert raised.value.failed_receipt_pin is not None
    assert (tmp_path / production._completion_relative(run_spec)).is_file()
    assert not (tmp_path / production._deep_attestation_relative(run_spec)).exists()


def test_deep_verify_rejects_tampered_and_missing_row_change_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    index = result.loaded.gate_a_manifest.row_version_change_index
    assert index is not None
    path = tmp_path / index.artifact.path
    original = path.read_bytes()
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    path.write_bytes(original + b"tamper")
    with pytest.raises(contract.I3ProductionContractError, match="differs from pin"):
        verify_i3_production_deep_attestation(
            tmp_path,
            result.completion_pin,
            result.deep_attestation_pin,
            expected_kind=I3ProductionRunKind.BASE,
        )
    path.write_bytes(original)
    missing = path.with_suffix(".missing")
    path.rename(missing)
    with pytest.raises(contract.I3ProductionContractError, match="missing"):
        verify_i3_production_deep_attestation(
            tmp_path,
            result.completion_pin,
            result.deep_attestation_pin,
            expected_kind=I3ProductionRunKind.BASE,
        )


def test_gate_a_base_parser_rejects_inline_supersession_beside_compact_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    value = result.loaded.gate_a_manifest.to_dict()
    value["superseded_row_version_ids"] = [stable_digest({"fixture": "wrong"})]
    with pytest.raises(IncrementalContractError, match="mutually exclusive"):
        contract._gate_a_manifest_from_dict(value)


def test_failed_materializer_emits_receipt_without_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    run_spec_pin = _write_run_spec(tmp_path, run_spec)

    class FailingMaterializer:
        def prepare(self, **_kwargs):
            raise RuntimeError("fixture materializer stopped")

    with pytest.raises(I3ProductionStageError) as raised:
        stage_i3_production_base(
            tmp_path,
            run_spec_pin,
            materializer=FailingMaterializer(),
        )
    assert raised.value.failed_receipt_pin is not None
    assert (tmp_path / raised.value.failed_receipt_pin.path).is_file()
    completion = (
        tmp_path
        / "manifests/silver/identity/s7-5-native-v2-staging"
        / f"run_spec_id={run_spec.run_spec_id}"
        / "completion.json"
    )
    assert not completion.exists()


def test_delta_parent_shallow_loader_reads_no_historical_parquet_or_row_proofs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec, legacy, s4 = _base_fixture(tmp_path)
    _patch_exact_upstreams(monkeypatch, run_spec.terminal_session)
    result = stage_i3_production_base(
        tmp_path,
        _write_run_spec(tmp_path, run_spec),
        materializer=CompactBaseMigrationMaterializer(legacy, s4),
    )
    output = result.loaded.receipt.output_set
    assert output is not None
    receipt_content = b"latest-i2-receipt\n"
    receipt_path = "manifests/silver/incremental/s4/assets/session=2026-01-07/receipt.json"
    (tmp_path / receipt_path).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / receipt_path).write_bytes(receipt_content)
    receipt_pin = ArtifactPin(
        path=receipt_path,
        sha256=hashlib.sha256(receipt_content).hexdigest(),
        bytes=len(receipt_content),
    )
    delta = replace(
        run_spec,
        run_kind=I3ProductionRunKind.DELTA,
        terminal_session=run_spec.terminal_session + timedelta(days=1),
        i2_base_frontier=None,
        i2_receipts=(
            I3ProductionI2ReceiptPin(
                session_date=run_spec.terminal_session + timedelta(days=1),
                receipt_id=stable_digest({"fixture": "latest-i2"}),
                artifact=receipt_pin,
                receipt_available_session=run_spec.run_available_session,
            ),
        ),
        parent_release=NativeV2ParentReleasePin.from_manifest(
            result.loaded.manifest,
            path=output.release_manifest_artifact.path,
        ),
        parent_checkpoint_artifact=output.checkpoint_artifact,
        parent_gate_a_manifest=output.gate_a_manifest_pin,
        parent_shadow_completion_artifact=result.completion_pin,
        parent_deep_attestation_artifact=result.deep_attestation_pin,
        parent_authority=I3ProductionParentAuthority.MIGRATION_SHADOW,
    )
    reads: list[str] = []
    original = contract._read_root_bytes

    def recording_reader(root: Path, relative: str) -> bytes:
        reads.append(relative)
        return original(root, relative)

    monkeypatch.setattr(contract, "_read_root_bytes", recording_reader)
    parent = load_i3_production_parent_shallow_exact(tmp_path, delta)
    assert parent is not None and parent.deep_attestation is not None
    assert not any(path.endswith(".parquet") for path in reads)
    assert not any("/row-semantic-proofs/" in path for path in reads)
