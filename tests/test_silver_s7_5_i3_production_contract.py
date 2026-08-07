from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_production_contract as production
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    ControlObjectKind,
    ControlObjectPin,
    ManifestPin,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    LEGACY_S7_V1_RELEASE_SET_ID,
    NATIVE_V2_RELEASE_FAMILY,
    IdentityPolicyBundle,
    IdentityRegistryReleasePin,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
)
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_CONTRACTS,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I0_ORACLE_RELEASE_SET_BYTES,
    I0_ORACLE_RELEASE_SET_SHA256,
    S4_V1_RELEASE_SET_BYTES,
    S4_V1_RELEASE_SET_ID,
    S4_V1_RELEASE_SET_SHA256,
    I3ProductionCalendarPin,
    I3ProductionCompletion,
    I3ProductionContractError,
    I3ProductionDatasetIndex,
    I3ProductionDeepVerificationAttestation,
    I3ProductionDependencyPin,
    I3ProductionDependencyRole,
    I3ProductionI2BaseFrontierPin,
    I3ProductionI2ReceiptPin,
    I3ProductionOutputSet,
    I3ProductionOutputStorage,
    I3ProductionParentAuthority,
    I3ProductionPartitionPin,
    I3ProductionResourceCaps,
    I3ProductionResourceObservation,
    I3ProductionRowsetIndex,
    I3ProductionRunKind,
    I3ProductionRunReceipt,
    I3ProductionRunSpec,
    I3ProductionRunState,
    I3ProductionSegmentPin,
    I3ProductionTableOutput,
    load_i3_production_completion_exact,
    load_i3_production_deep_attestation_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
    production_physical_index_digest,
    production_v2_contract_pins,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
    production_compact_base_initial_segment_id,
    production_native_v2_migration_id,
)

TERMINAL = date(2026, 7, 10)
POLICY_CUTOFF = date(2026, 7, 29)
AVAILABLE = date(2026, 8, 5)


def _digest(label: str) -> str:
    return stable_digest({"production-test": label})


def _pin(path: str, label: str | None = None) -> ArtifactPin:
    content = f"{label or path}\n".encode()
    return ArtifactPin(
        path=path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _policy() -> IdentityPolicyBundle:
    return IdentityPolicyBundle(
        tuple(
            IdentityRegistryReleasePin(
                registry_kind=kind,
                release_id=_digest(f"{kind.value}-release"),
                artifact=_pin(
                    "manifests/silver/identity/registry-releases/"
                    f"registry={kind.value}/release_id={_digest(f'{kind.value}-release')}/"
                    "manifest.json"
                ),
                decision_cutoff_session=POLICY_CUTOFF,
                release_available_session=date(2026, 8, 2),
            )
            for kind in IDENTITY_REGISTRY_ORDER
        ),
        bundle_available_session=date(2026, 8, 3),
    )


def _run_spec() -> I3ProductionRunSpec:
    policy = _policy()
    i0 = I3ProductionDependencyPin(
        role=I3ProductionDependencyRole.I0_V1_ORACLE,
        object_id=LEGACY_S7_V1_RELEASE_SET_ID,
        artifact=ArtifactPin(
            path=(
                "manifests/silver/identity/s7-four-table-release-sets/"
                f"release_set_id={LEGACY_S7_V1_RELEASE_SET_ID}/manifest.json"
            ),
            sha256=I0_ORACLE_RELEASE_SET_SHA256,
            bytes=I0_ORACLE_RELEASE_SET_BYTES,
        ),
        available_session=date(2026, 8, 3),
    )
    s4 = I3ProductionDependencyPin(
        role=I3ProductionDependencyRole.S4_V1_SOURCE,
        object_id=S4_V1_RELEASE_SET_ID,
        artifact=ArtifactPin(
            path=(
                "manifests/silver/release-sets/assets/"
                f"release_set_id={S4_V1_RELEASE_SET_ID}/manifest.json"
            ),
            sha256=S4_V1_RELEASE_SET_SHA256,
            bytes=S4_V1_RELEASE_SET_BYTES,
        ),
        available_session=date(2026, 7, 29),
    )
    policy_artifact = policy.exact_pin(path="manifests/silver/identity/s7-5-native-v2/policy.json")
    calendar = I3ProductionCalendarPin(
        calendar_artifact_id=_digest("calendar"),
        artifact=_pin(
            f"manifests/silver/xnys-calendars/calendar_artifact_id={_digest('calendar')}.json"
        ),
        available_session=date(2026, 7, 29),
    )
    frontier = I3ProductionI2BaseFrontierPin(
        terminal_session=TERMINAL,
        frontier_id=_digest("i2-base-frontier"),
        artifact=_pin(
            "manifests/silver/incremental/s4/assets/base-frontiers/"
            f"frontier_id={_digest('i2-base-frontier')}/manifest.json"
        ),
        frontier_available_session=date(2026, 8, 4),
    )
    return I3ProductionRunSpec(
        run_kind=I3ProductionRunKind.BASE,
        terminal_session=TERMINAL,
        source_cutoff_session=POLICY_CUTOFF,
        run_available_session=AVAILABLE,
        native_v2_migration_id=production_native_v2_migration_id(
            i0_release_set_artifact=i0.artifact,
            s4_release_set_artifact=s4.artifact,
            identity_policy_bundle=policy,
            identity_policy_bundle_artifact=policy_artifact,
            calendar_artifact=calendar.artifact,
            i2_base_frontier_artifact=frontier.artifact,
        ),
        transform_semantics_digest=I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
        i0_oracle=i0,
        s4_v1_source=s4,
        identity_policy_bundle=policy,
        identity_policy_bundle_artifact=policy_artifact,
        calendar=calendar,
        v2_contracts=production_v2_contract_pins(),
        i2_receipts=(),
        i2_base_frontier=frontier,
        resource_caps=I3ProductionResourceCaps(),
    )


def _latest_i2_receipt() -> I3ProductionI2ReceiptPin:
    return I3ProductionI2ReceiptPin(
        session_date=TERMINAL,
        receipt_id=_digest("i2-receipt"),
        artifact=_pin(
            f"manifests/silver/assets-incremental/receipt_id={_digest('i2-receipt')}/manifest.json"
        ),
        receipt_available_session=date(2026, 8, 4),
    )


def _partition(session: date) -> I3ProductionPartitionPin:
    contract = I3_V2_CONTRACTS["universe_daily"]
    return I3ProductionPartitionPin(
        session_date=session,
        partition_receipt_id=_digest(f"universe-{session.isoformat()}"),
        artifact=_pin(
            "silver/identity/s7-5-native-v2-staging/universe_daily/"
            f"session_date={session.isoformat()}/part-000.parquet"
        ),
        row_count=3,
        contract_id=contract.contract_id,
        schema_digest=contract.schema_digest,
        availability_session=AVAILABLE,
    )


def _output_set() -> I3ProductionOutputSet:
    partitions = (_partition(date(2026, 7, 9)), _partition(TERMINAL))
    index = I3ProductionDatasetIndex(
        table_name="universe_daily", terminal_session=TERMINAL, partitions=partitions
    )
    table_outputs: list[I3ProductionTableOutput] = []
    for table in I3_V2_TABLE_ORDER:
        contract = I3_V2_CONTRACTS[table]
        if table == "universe_daily":
            artifact = index.exact_pin(
                path="silver/identity/s7-5-native-v2-staging/universe_daily/index.json"
            )
            table_outputs.append(
                I3ProductionTableOutput(
                    storage=I3ProductionOutputStorage.DATASET_INDEX,
                    manifest_output=NativeV2OutputArtifact(
                        table_name=table,
                        session_date=TERMINAL,
                        row_count=index.row_count,
                        contract_id=contract.contract_id,
                        schema_digest=contract.schema_digest,
                        artifact=artifact,
                    ),
                    dataset_index=index,
                )
            )
        else:
            table_outputs.append(
                I3ProductionTableOutput(
                    storage=I3ProductionOutputStorage.PARQUET,
                    manifest_output=NativeV2OutputArtifact(
                        table_name=table,
                        session_date=TERMINAL,
                        row_count=2,
                        contract_id=contract.contract_id,
                        schema_digest=contract.schema_digest,
                        artifact=_pin(
                            f"silver/identity/s7-5-native-v2-staging/{table}/base.parquet"
                        ),
                    ),
                )
            )
    gate_a_spec = ControlObjectPin(
        object_kind=ControlObjectKind.RUN_SPEC,
        object_id=_digest("gate-a-spec"),
        artifact=_pin("manifests/silver/incremental/i3/gate-a/run-spec.json"),
    )
    gate_a_receipt = ControlObjectPin(
        object_kind=ControlObjectKind.RUN_RECEIPT,
        object_id=_digest("gate-a-receipt"),
        artifact=_pin("manifests/silver/incremental/i3/gate-a/run-receipt.json"),
    )
    gate_a_manifest = ManifestPin(
        release_id=_digest("gate-a-release"),
        manifest_path="manifests/silver/incremental/i3/gate-a/release.json",
        manifest_sha256=_digest("gate-a-release-bytes"),
        manifest_bytes=123,
        release_available_session=AVAILABLE,
    )
    return I3ProductionOutputSet(
        release_manifest_artifact=_pin(
            "manifests/silver/identity/s7-5-native-v2-staging/release.json"
        ),
        checkpoint_artifact=_pin(
            "manifests/silver/identity/s7-5-native-v2-staging/checkpoint.json"
        ),
        release_id=_digest("native-v2-release"),
        checkpoint_id=_digest("native-v2-checkpoint"),
        resolved_state_digest=_digest("resolved-state"),
        resolved_content_digest=_digest("resolved-content"),
        table_outputs=tuple(table_outputs),
        gate_a_run_spec_pin=gate_a_spec,
        gate_a_run_receipt_pin=gate_a_receipt,
        gate_a_manifest_pin=gate_a_manifest,
        control_extension_artifacts=(),
    )


def _compact_base_outputs(
    run_spec: I3ProductionRunSpec,
) -> tuple[I3ProductionTableOutput, ...]:
    outputs = list(_output_set().table_outputs)
    for index, output in enumerate(outputs[:-1]):
        artifact = output.manifest_output.artifact
        segment = I3ProductionSegmentPin(
            table_name=output.table_name,
            segment_id=production_compact_base_initial_segment_id(
                table_name=output.table_name,
                artifact=artifact,
                terminal_session=run_spec.terminal_session,
                availability_session=run_spec.run_available_session,
                native_v2_migration_id=run_spec.native_v2_migration_id,
            ),
            artifact=artifact,
            row_count=output.manifest_output.row_count,
            contract_id=output.manifest_output.contract_id,
            schema_digest=output.manifest_output.schema_digest,
            availability_session=run_spec.run_available_session,
        )
        rowset = I3ProductionRowsetIndex(
            table_name=output.table_name,
            terminal_session=run_spec.terminal_session,
            segments=(segment,),
        )
        outputs[index] = I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.ROWSET_INDEX,
            manifest_output=replace(
                output.manifest_output,
                session_date=run_spec.terminal_session,
                artifact=rowset.exact_pin(
                    path=(f"silver/identity/s7-5-native-v2-staging/{output.table_name}/index.json")
                ),
            ),
            rowset_index=rowset,
        )
    return tuple(outputs)


def _malformed_compact_base_outputs(
    outputs: tuple[I3ProductionTableOutput, ...],
    mutation: str,
) -> tuple[I3ProductionTableOutput, ...]:
    first = outputs[0]
    rowset = first.rowset_index
    assert rowset is not None
    segment = rowset.segments[0]
    if mutation == "direct_parquet":
        malformed = replace(
            first,
            storage=I3ProductionOutputStorage.PARQUET,
            manifest_output=replace(first.manifest_output, artifact=segment.artifact),
            rowset_index=None,
        )
    elif mutation == "wrong_segment_id":
        bad_rowset = replace(
            rowset,
            segments=(replace(segment, segment_id=_digest("wrong-base-segment")),),
        )
        malformed = replace(
            first,
            manifest_output=replace(
                first.manifest_output,
                artifact=bad_rowset.exact_pin(path=first.manifest_output.artifact.path),
            ),
            rowset_index=bad_rowset,
        )
    elif mutation == "two_segments":
        extra = replace(
            segment,
            segment_id=_digest("extra-base-segment"),
            artifact=_pin("silver/identity/s7-5-native-v2-staging/asset_master/extra.parquet"),
            row_count=0,
        )
        bad_rowset = replace(rowset, segments=(segment, extra))
        malformed = replace(
            first,
            manifest_output=replace(
                first.manifest_output,
                artifact=bad_rowset.exact_pin(path=first.manifest_output.artifact.path),
            ),
            rowset_index=bad_rowset,
        )
    else:  # pragma: no cover - test helper guard
        raise AssertionError(f"unknown compact-base mutation: {mutation}")
    return (malformed, *outputs[1:])


def _success_controls() -> tuple[
    I3ProductionRunSpec, I3ProductionRunReceipt, I3ProductionCompletion
]:
    spec = _run_spec()
    spec_pin = spec.exact_pin(
        path=f"manifests/silver/identity/s7-5-native-v2-staging/{spec.run_spec_id}/run-spec.json"
    )
    output = _output_set()
    receipt = I3ProductionRunReceipt(
        run_spec_id=spec.run_spec_id,
        run_spec_artifact=spec_pin,
        state=I3ProductionRunState.SUCCEEDED,
        receipt_available_session=AVAILABLE,
        resource_observation=I3ProductionResourceObservation(
            peak_rss_bytes=1024**3,
            elapsed_seconds=3600,
            minimum_disk_free_bytes=50 * 1024**3,
            temporary_bytes=1024**3,
        ),
        output_set=output,
    )
    receipt_pin = receipt.exact_pin(
        path=f"manifests/silver/identity/s7-5-native-v2-staging/{spec.run_spec_id}/receipt.json"
    )
    completion = I3ProductionCompletion(
        run_spec_id=spec.run_spec_id,
        receipt_id=receipt.receipt_id,
        receipt_artifact=receipt_pin,
        output_set_id=output.output_set_id,
        release_id=output.gate_a_manifest_pin.release_id,
        native_v2_envelope_id=output.release_id,
        checkpoint_id=output.checkpoint_id,
        completion_available_session=AVAILABLE,
    )
    return spec, receipt, completion


def _delta_run_spec() -> I3ProductionRunSpec:
    spec = _run_spec()
    parent_output = _output_set()
    native = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=date(2026, 7, 9),
        release_available_session=AVAILABLE,
        native_v2_migration_id=spec.native_v2_migration_id,
        identity_policy_bundle_id=spec.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=spec.transform_semantics_digest,
        resolved_state_digest=parent_output.resolved_state_digest,
        output_artifacts=tuple(
            replace(item.manifest_output, session_date=date(2026, 7, 9))
            for item in parent_output.table_outputs
        ),
    )
    return replace(
        spec,
        run_kind=I3ProductionRunKind.DELTA,
        i2_base_frontier=None,
        i2_receipts=(_latest_i2_receipt(),),
        parent_release=NativeV2ParentReleasePin.from_manifest(
            native,
            path="manifests/silver/identity/s7-5-native-v2-staging/parent/native.json",
        ),
        parent_checkpoint_artifact=_pin(
            "manifests/silver/identity/s7-5-native-v2-staging/parent/checkpoint.json"
        ),
        parent_gate_a_manifest=ManifestPin(
            release_id=_digest("gate-a-parent"),
            manifest_path="manifests/silver/incremental/i3/parent/release.json",
            manifest_sha256=_digest("gate-a-parent-bytes"),
            manifest_bytes=10,
            release_available_session=AVAILABLE,
        ),
        parent_shadow_completion_artifact=_pin(
            "manifests/silver/identity/s7-5-native-v2-staging/parent/completion.json"
        ),
        parent_deep_attestation_artifact=_pin(
            "manifests/silver/identity/s7-5-native-v2-staging/parent/"
            "deep-verification-attestation.json"
        ),
        parent_authority=I3ProductionParentAuthority.MIGRATION_SHADOW,
    )


def test_run_spec_is_canonical_deterministic_and_exactly_loadable() -> None:
    spec = _run_spec()
    assert spec == I3ProductionRunSpec.from_dict(spec.to_dict())
    assert spec.run_spec_id == I3ProductionRunSpec.from_dict(spec.to_dict()).run_spec_id
    pin = spec.exact_pin(path="controls/run-spec.json")
    assert (
        load_i3_production_run_spec_exact(
            pin, lambda path: spec.canonical_bytes() if path == pin.path else b""
        )
        == spec
    )
    assert tuple(item.table_name for item in spec.v2_contracts) == I3_V2_TABLE_ORDER


def test_run_spec_rejects_fixture_i2_and_incomplete_or_reordered_contracts() -> None:
    spec = _run_spec()
    assert spec.i2_base_frontier is not None
    with pytest.raises(I3ProductionContractError, match="fixture I2"):
        replace(spec.i2_base_frontier, artifact=_pin("fixtures/i2/frontier.json"))
    with pytest.raises(I3ProductionContractError, match="exact v2 schema"):
        replace(spec, v2_contracts=tuple(reversed(spec.v2_contracts)))
    with pytest.raises(I3ProductionContractError, match="exact I2 base frontier"):
        replace(spec, i2_base_frontier=None)
    with pytest.raises(I3ProductionContractError, match="not canonical"):
        replace(
            spec.i2_base_frontier,
            artifact=replace(
                spec.i2_base_frontier.artifact,
                path="manifests/silver/incremental/s4/assets/base-frontiers/frontier.json",
            ),
        )


@pytest.mark.parametrize(
    "target",
    ("i0_oracle", "s4_v1_source", "identity_policy", "calendar", "i2_frontier"),
)
def test_run_spec_from_dict_rejects_temporary_base_authority_pins(target: str) -> None:
    value = json.loads(json.dumps(_run_spec().to_dict()))
    if target in {"i0_oracle", "s4_v1_source"}:
        value[target]["artifact"]["path"] = f"tmp/{target}.json"
    elif target == "identity_policy":
        value["identity_policy_bundle_artifact"]["path"] = "tmp/policy.json"
    elif target == "calendar":
        value["calendar"]["artifact"]["path"] = "tmp/calendar.json"
    else:
        value["i2_base_frontier"]["artifact"]["path"] = "tmp/frontier.json"
    with pytest.raises(I3ProductionContractError, match="temporary"):
        I3ProductionRunSpec.from_dict(value)


def test_run_spec_rejects_temporary_registry_release_pin() -> None:
    spec = _run_spec()
    first, *rest = spec.identity_policy_bundle.registry_releases
    policy = IdentityPolicyBundle(
        (replace(first, artifact=replace(first.artifact, path="tmp/registry.json")), *rest),
        bundle_available_session=spec.identity_policy_bundle.bundle_available_session,
    )
    with pytest.raises(I3ProductionContractError, match="temporary control"):
        replace(
            spec,
            identity_policy_bundle=policy,
            identity_policy_bundle_artifact=policy.exact_pin(
                path="manifests/silver/identity/s7-5-native-v2/policy-tamper.json"
            ),
        )


@pytest.mark.parametrize(
    ("target", "path_field"),
    (
        ("i2_receipts", "artifact"),
        ("parent_release", "manifest"),
        ("parent_checkpoint_artifact", None),
        ("parent_gate_a_manifest", None),
        ("parent_shadow_completion_artifact", None),
        ("parent_deep_attestation_artifact", None),
    ),
)
def test_delta_run_spec_from_dict_rejects_temporary_parent_authority_pins(
    target: str,
    path_field: str | None,
) -> None:
    value = json.loads(json.dumps(_delta_run_spec().to_dict()))
    if target == "i2_receipts":
        value[target][0][path_field]["path"] = "tmp/latest-i2.json"
    elif target == "parent_release":
        value[target][path_field]["path"] = "tmp/native.json"
    elif target == "parent_gate_a_manifest":
        value[target]["manifest_path"] = "tmp/gate-a.json"
    else:
        value[target]["path"] = f"tmp/{target}.json"
    with pytest.raises(I3ProductionContractError, match="temporary"):
        I3ProductionRunSpec.from_dict(value)


def test_published_delta_from_dict_rejects_temporary_pointer_event() -> None:
    published = replace(
        _delta_run_spec(),
        parent_authority=I3ProductionParentAuthority.PUBLISHED_DAILY,
        parent_pointer_event_artifact=_pin(
            "manifests/silver/incremental/i6/shadow-pointer/event.json"
        ),
    )
    value = json.loads(json.dumps(published.to_dict()))
    value["parent_pointer_event_artifact"]["path"] = "tmp/pointer-event.json"
    with pytest.raises(I3ProductionContractError, match="temporary parent"):
        I3ProductionRunSpec.from_dict(value)


def test_run_spec_rejects_operator_declared_transform_and_migration_semantics() -> None:
    spec = _run_spec()
    with pytest.raises(I3ProductionContractError, match="module-owned rule bundle"):
        replace(spec, transform_semantics_digest=_digest("operator-transform"))
    with pytest.raises(I3ProductionContractError, match="does not reproduce"):
        replace(spec, native_v2_migration_id=_digest("operator-migration"))


def test_fixture_controls_and_receipts_cannot_acquire_production_authority() -> None:
    spec = _run_spec()
    policy = spec.identity_policy_bundle
    with pytest.raises(I3ProductionContractError, match="fixture control artifact"):
        replace(
            spec,
            identity_policy_bundle_artifact=policy.exact_pin(path="fixtures/i3/policy.json"),
        )
    with pytest.raises(I3ProductionContractError, match="fixture RunSpec"):
        I3ProductionRunReceipt(
            run_spec_id=spec.run_spec_id,
            run_spec_artifact=spec.exact_pin(path="fixtures/i3/run-spec.json"),
            state=I3ProductionRunState.FAILED,
            receipt_available_session=AVAILABLE,
            resource_observation=I3ProductionResourceObservation(1, 1, 50 * 1024**3, 0),
            failure_code="source_mismatch",
            failure_detail_digest=_digest("failure"),
        )


def test_receipt_and_completion_from_dict_reject_rebuilt_temporary_authority_pins() -> None:
    spec, receipt, completion = _success_controls()
    receipt_value = receipt.to_dict()
    receipt_value["run_spec_artifact"] = spec.exact_pin(path="tmp/run-spec.json").to_dict()
    receipt_value["receipt_id"] = _digest("attacker-rebuilt-receipt")
    with pytest.raises(I3ProductionContractError, match="temporary RunSpec"):
        I3ProductionRunReceipt.from_dict(receipt_value)

    completion_value = completion.to_dict()
    completion_value["receipt_artifact"] = receipt.exact_pin(
        path="tmp/production-run-receipt.json"
    ).to_dict()
    completion_value["completion_id"] = _digest("attacker-rebuilt-completion")
    with pytest.raises(I3ProductionContractError, match="temporary receipt"):
        I3ProductionCompletion.from_dict(completion_value)


def test_dependency_pin_rejects_wrong_i0_or_s4_bytes() -> None:
    spec = _run_spec()
    with pytest.raises(I3ProductionContractError, match="frozen production dependency"):
        replace(
            spec.i0_oracle,
            artifact=replace(spec.i0_oracle.artifact, sha256=_digest("forged-i0")),
        )
    with pytest.raises(I3ProductionContractError, match="frozen production dependency"):
        replace(
            spec.s4_v1_source,
            artifact=replace(spec.s4_v1_source.artifact, bytes=S4_V1_RELEASE_SET_BYTES + 1),
        )


def test_dataset_index_is_canonical_and_binds_all_partitions() -> None:
    output = _output_set().table_outputs[-1]
    assert output.dataset_index is not None
    index = output.dataset_index
    assert I3ProductionDatasetIndex.from_dict(index.to_dict()) == index
    assert index.row_count == 6
    assert output.manifest_output.artifact == index.exact_pin(
        path=output.manifest_output.artifact.path
    )
    tampered = index.to_dict()
    tampered["row_count"] = 7
    with pytest.raises(I3ProductionContractError, match="row count"):
        I3ProductionDatasetIndex.from_dict(tampered)


def test_dataset_index_rejects_gap_order_and_duplicate_paths() -> None:
    first = _partition(date(2026, 7, 9))
    second = _partition(TERMINAL)
    with pytest.raises(I3ProductionContractError, match="sorted, unique"):
        I3ProductionDatasetIndex(
            table_name="universe_daily",
            terminal_session=TERMINAL,
            partitions=(second, first),
        )
    with pytest.raises(I3ProductionContractError, match="paths must be unique"):
        I3ProductionDatasetIndex(
            table_name="universe_daily",
            terminal_session=TERMINAL,
            partitions=(first, replace(second, artifact=first.artifact)),
        )


def test_storage_roles_are_closed_and_universe_cannot_be_single_parquet() -> None:
    output = _output_set().table_outputs[-1]
    with pytest.raises(I3ProductionContractError, match="must use a dataset index"):
        I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.PARQUET,
            manifest_output=output.manifest_output,
        )
    asset = _output_set().table_outputs[0]
    with pytest.raises(I3ProductionContractError, match="Parquet or rowset"):
        replace(asset, storage=I3ProductionOutputStorage.DATASET_INDEX)


def test_small_tables_support_append_only_rowset_indexes() -> None:
    contract = I3_V2_CONTRACTS["asset_master"]
    segments = tuple(
        I3ProductionSegmentPin(
            table_name="asset_master",
            segment_id=_digest(f"asset-segment-{index}"),
            artifact=_pin(
                f"silver/identity/s7-5-native-v2-staging/asset_master/segment-{index}.parquet"
            ),
            row_count=index + 1,
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            availability_session=AVAILABLE,
        )
        for index in range(2)
    )
    rowset = I3ProductionRowsetIndex(
        table_name="asset_master", terminal_session=TERMINAL, segments=segments
    )
    output = I3ProductionTableOutput(
        storage=I3ProductionOutputStorage.ROWSET_INDEX,
        manifest_output=NativeV2OutputArtifact(
            table_name="asset_master",
            session_date=TERMINAL,
            row_count=rowset.row_count,
            contract_id=contract.contract_id,
            schema_digest=contract.schema_digest,
            artifact=rowset.exact_pin(
                path="silver/identity/s7-5-native-v2-staging/asset_master/index.json"
            ),
        ),
        rowset_index=rowset,
    )
    assert I3ProductionTableOutput.from_dict(output.to_dict()) == output
    assert output.rowset_index is not None
    assert output.rowset_index.segments == segments


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("direct_parquet", "exactly one rowset segment"),
        ("wrong_segment_id", "module-owned identity"),
        ("two_segments", "exactly one rowset segment"),
    ),
)
def test_compact_base_initial_rowsets_are_module_owned(
    mutation: str,
    message: str,
) -> None:
    run_spec = _run_spec()
    outputs = _compact_base_outputs(run_spec)
    production.validate_production_compact_base_initial_rowsets(run_spec, outputs)
    with pytest.raises(I3ProductionContractError, match=message):
        production.validate_production_compact_base_initial_rowsets(
            run_spec,
            _malformed_compact_base_outputs(outputs, mutation),
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("direct_parquet", "exactly one rowset segment"),
        ("wrong_segment_id", "module-owned identity"),
        ("two_segments", "exactly one rowset segment"),
    ),
)
def test_shallow_parent_rejects_malformed_base_rowsets_without_parquet_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    parent_spec = _run_spec()
    assert parent_spec.i2_base_frontier is not None
    parent_spec = replace(
        parent_spec,
        terminal_session=date(2026, 7, 9),
        i2_base_frontier=replace(
            parent_spec.i2_base_frontier,
            terminal_session=date(2026, 7, 9),
        ),
    )
    parent_outputs = _malformed_compact_base_outputs(_compact_base_outputs(parent_spec), mutation)
    output_set = replace(_output_set(), table_outputs=parent_outputs)
    parent_spec_path = (
        f"manifests/silver/identity/s7-5-native-v2-staging/shallow-parent-{mutation}/run-spec.json"
    )
    parent_spec_pin = parent_spec.exact_pin(path=parent_spec_path)
    _, receipt_template, completion_template = _success_controls()
    receipt = replace(
        receipt_template,
        run_spec_id=parent_spec.run_spec_id,
        run_spec_artifact=parent_spec_pin,
        output_set=output_set,
    )
    receipt_path = (
        "manifests/silver/identity/s7-5-native-v2-staging/"
        f"shallow-parent-{mutation}/run-receipt.json"
    )
    receipt_pin = receipt.exact_pin(path=receipt_path)
    completion = replace(
        completion_template,
        run_spec_id=parent_spec.run_spec_id,
        receipt_id=receipt.receipt_id,
        receipt_artifact=receipt_pin,
        output_set_id=output_set.output_set_id,
    )
    completion_path = (
        "manifests/silver/identity/s7-5-native-v2-staging/"
        f"shallow-parent-{mutation}/completion.json"
    )
    completion_pin = completion.exact_pin(path=completion_path)
    child_spec = replace(
        _delta_run_spec(),
        parent_shadow_completion_artifact=completion_pin,
    )
    controls = {
        completion_pin.path: completion.canonical_bytes(),
        receipt_pin.path: receipt.canonical_bytes(),
        parent_spec_pin.path: parent_spec.canonical_bytes(),
    }
    read_paths: list[str] = []

    def artifact_reader(_root: Path, relative: str) -> bytes:
        read_paths.append(relative)
        if relative.endswith(".parquet"):
            raise AssertionError("shallow parent loader read Parquet before shape rejection")
        return controls[relative]

    monkeypatch.setattr(production, "_read_root_bytes", artifact_reader)
    with pytest.raises(I3ProductionContractError, match=message):
        production.load_i3_production_parent_shallow_exact(tmp_path, child_spec)
    assert read_paths == [completion_pin.path, receipt_pin.path, parent_spec_pin.path]
    assert not any(path.endswith(".parquet") for path in read_paths)


def test_output_set_roundtrip_totals_and_fixture_denial() -> None:
    output = _output_set()
    assert I3ProductionOutputSet.from_dict(output.to_dict()) == output
    assert output.total_rows == 12
    assert output.total_output_bytes > 0
    with pytest.raises(I3ProductionContractError, match="fixture artifact"):
        replace(
            output,
            checkpoint_artifact=_pin("fixtures/i3/production-checkpoint.json"),
        )
    extension = _pin("silver/identity/s7-5-native-v2-staging/extension.parquet")
    extended = replace(output, control_extension_artifacts=(extension,))
    assert extended.total_output_bytes == output.total_output_bytes + extension.bytes
    with pytest.raises(I3ProductionContractError, match="temporary artifact"):
        replace(output, checkpoint_artifact=_pin("tmp/production-checkpoint.json"))


def test_success_receipt_and_awaiting_review_completion_roundtrip() -> None:
    spec, receipt, completion = _success_controls()
    assert I3ProductionRunReceipt.from_dict(receipt.to_dict()) == receipt
    assert I3ProductionCompletion.from_dict(completion.to_dict()) == completion
    receipt.resource_observation.validate_caps(spec.resource_caps)
    assert completion.to_dict()["publish_authorized"] is False
    assert completion.to_dict()["cutover_authorized"] is False
    assert completion.to_dict()["state"] == "awaiting_review"


def test_deep_attestation_roundtrip_binds_gate_a_and_physical_indexes() -> None:
    spec, receipt, completion = _success_controls()
    output = receipt.output_set
    assert output is not None
    native = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=TERMINAL,
        release_available_session=AVAILABLE,
        native_v2_migration_id=spec.native_v2_migration_id,
        identity_policy_bundle_id=_policy().identity_policy_bundle_id,
        transform_semantics_digest=spec.transform_semantics_digest,
        resolved_state_digest=output.resolved_state_digest,
        output_artifacts=tuple(item.manifest_output for item in output.table_outputs),
    )
    completion_pin = completion.exact_pin(path="controls/completion.json")
    attestation = I3ProductionDeepVerificationAttestation(
        completion_id=completion.completion_id,
        completion_artifact=completion_pin,
        gate_a_manifest_pin=output.gate_a_manifest_pin,
        native_v2_release=NativeV2ParentReleasePin.from_manifest(
            native, path=output.release_manifest_artifact.path
        ),
        checkpoint_id=output.checkpoint_id,
        checkpoint_artifact=output.checkpoint_artifact,
        output_set_id=output.output_set_id,
        row_semantic_attestation_digest=_digest("row-attestation"),
        terminal_state_digest=_digest("terminal-state"),
        physical_index_digest=production_physical_index_digest(output),
        parent_frontier_attestation_digest=None,
        attestation_available_session=AVAILABLE,
        verification_resource_observation=receipt.resource_observation,
    )
    pin = attestation.exact_pin(path="controls/deep-attestation.json")
    assert (
        load_i3_production_deep_attestation_exact(
            pin, lambda path: attestation.canonical_bytes() if path == pin.path else b""
        )
        == attestation
    )
    assert attestation.to_dict()["publish_authorized"] is False


@pytest.mark.parametrize(
    "target",
    (
        "completion_artifact",
        "checkpoint_artifact",
        "gate_a_manifest_pin",
        "native_v2_release",
    ),
)
def test_deep_attestation_from_dict_rejects_rebuilt_temporary_authority_pins(
    target: str,
) -> None:
    spec, receipt, completion = _success_controls()
    output = receipt.output_set
    assert output is not None
    native = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=TERMINAL,
        release_available_session=AVAILABLE,
        native_v2_migration_id=spec.native_v2_migration_id,
        identity_policy_bundle_id=spec.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=spec.transform_semantics_digest,
        resolved_state_digest=output.resolved_state_digest,
        output_artifacts=tuple(item.manifest_output for item in output.table_outputs),
    )
    attestation = I3ProductionDeepVerificationAttestation(
        completion_id=completion.completion_id,
        completion_artifact=completion.exact_pin(path="controls/completion.json"),
        gate_a_manifest_pin=output.gate_a_manifest_pin,
        native_v2_release=NativeV2ParentReleasePin.from_manifest(
            native, path=output.release_manifest_artifact.path
        ),
        checkpoint_id=output.checkpoint_id,
        checkpoint_artifact=output.checkpoint_artifact,
        output_set_id=output.output_set_id,
        row_semantic_attestation_digest=_digest("row-attestation"),
        terminal_state_digest=_digest("terminal-state"),
        physical_index_digest=production_physical_index_digest(output),
        parent_frontier_attestation_digest=None,
        attestation_available_session=AVAILABLE,
        verification_resource_observation=receipt.resource_observation,
    )
    value = attestation.to_dict()
    if target == "gate_a_manifest_pin":
        value[target]["manifest_path"] = "tmp/gate-a-release.json"
    elif target == "native_v2_release":
        value[target]["manifest"]["path"] = "tmp/native-v2-release.json"
    else:
        value[target]["path"] = f"tmp/{target}.json"
    value["deep_attestation_id"] = _digest(f"attacker-rebuilt-{target}")
    with pytest.raises(I3ProductionContractError, match="temporary artifact"):
        I3ProductionDeepVerificationAttestation.from_dict(value)


def test_failed_receipt_cannot_expose_outputs_or_completion() -> None:
    spec = _run_spec()
    failed = I3ProductionRunReceipt(
        run_spec_id=spec.run_spec_id,
        run_spec_artifact=spec.exact_pin(path="controls/run-spec.json"),
        state=I3ProductionRunState.FAILED,
        receipt_available_session=AVAILABLE,
        resource_observation=I3ProductionResourceObservation(
            peak_rss_bytes=1,
            elapsed_seconds=1,
            minimum_disk_free_bytes=50 * 1024**3,
            temporary_bytes=0,
        ),
        failure_code="source_mismatch",
        failure_detail_digest=_digest("failure"),
    )
    assert I3ProductionRunReceipt.from_dict(failed.to_dict()) == failed
    with pytest.raises(I3ProductionContractError, match="failed receipt cannot expose"):
        replace(failed, output_set=_output_set())
    with pytest.raises(I3ProductionContractError, match="successful receipt requires"):
        replace(
            failed,
            state=I3ProductionRunState.SUCCEEDED,
            failure_code=None,
            failure_detail_digest=None,
        )


def test_exact_loaders_reject_tampering_noncanonical_bytes_and_duplicate_keys() -> None:
    spec, receipt, completion = _success_controls()
    for value, loader, path in (
        (spec, load_i3_production_run_spec_exact, "controls/spec.json"),
        (receipt, load_i3_production_run_receipt_exact, "controls/receipt.json"),
        (completion, load_i3_production_completion_exact, "controls/completion.json"),
    ):
        pin = value.exact_pin(path=path)
        content = value.canonical_bytes()
        with pytest.raises(I3ProductionContractError, match="exact pin"):
            loader(pin, lambda _path, raw=content: raw + b" ")

        pretty = json.dumps(value.to_dict(), indent=2, sort_keys=True).encode() + b"\n"
        pretty_pin = ArtifactPin(
            path=path,
            sha256=hashlib.sha256(pretty).hexdigest(),
            bytes=len(pretty),
        )
        with pytest.raises(I3ProductionContractError, match="canonical JSON"):
            loader(pretty_pin, lambda _path, raw=pretty: raw)

    duplicate = b'{"completion_id":"a","completion_id":"b"}\n'
    pin = ArtifactPin(
        path="controls/duplicate.json",
        sha256=hashlib.sha256(duplicate).hexdigest(),
        bytes=len(duplicate),
    )
    with pytest.raises(I3ProductionContractError, match="duplicate JSON key"):
        load_i3_production_completion_exact(pin, lambda _path: duplicate)


def test_resource_observation_is_fail_closed() -> None:
    caps = I3ProductionResourceCaps()
    observation = I3ProductionResourceObservation(
        peak_rss_bytes=caps.rss_bytes_hard_cap + 1,
        elapsed_seconds=1,
        minimum_disk_free_bytes=caps.disk_free_bytes_hard_floor,
        temporary_bytes=0,
    )
    with pytest.raises(I3ProductionContractError, match="exceeds"):
        observation.validate_caps(caps)


def test_wall_clock_is_observation_only_and_never_a_hard_cutoff() -> None:
    caps = I3ProductionResourceCaps()
    assert "wall_clock_seconds_hard_cap" not in caps.to_dict()
    I3ProductionResourceObservation(
        peak_rss_bytes=caps.rss_bytes_hard_cap,
        elapsed_seconds=10 * 365 * 24 * 3600,
        minimum_disk_free_bytes=caps.disk_free_bytes_hard_floor,
        temporary_bytes=caps.temporary_bytes_hard_cap,
    ).validate_caps(caps)


def test_delta_requires_gate_a_parent_shadow_authority_and_one_latest_i2() -> None:
    delta = _delta_run_spec()
    assert I3ProductionRunSpec.from_dict(delta.to_dict()) == delta
    with pytest.raises(I3ProductionContractError, match="Gate-A parent"):
        replace(delta, parent_gate_a_manifest=None)
    earlier = replace(
        delta.i2_receipts[0],
        session_date=date(2026, 7, 9),
        receipt_id=_digest("earlier-i2"),
        artifact=_pin("manifests/silver/assets-incremental/earlier/manifest.json"),
    )
    with pytest.raises(I3ProductionContractError, match="only the latest"):
        replace(delta, i2_receipts=(earlier, delta.i2_receipts[0]))


def test_physical_parquet_verifier_checks_exact_bytes_schema_rows_and_session(
    tmp_path: Path,
) -> None:
    contract = I3_V2_CONTRACTS["universe_daily"]
    path = tmp_path / "silver/universe/session_date=2026-07-10/part.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([], schema=contract.arrow_schema), path)
    content = path.read_bytes()
    pin = ArtifactPin(
        path=str(path.relative_to(tmp_path)),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    verified = production._verify_parquet_exact(
        tmp_path,
        pin,
        table_name="universe_daily",
        row_count=0,
        session_date=TERMINAL,
    )
    assert verified.schema.equals(contract.arrow_schema)
    with pytest.raises(I3ProductionContractError, match="schema or row count"):
        production._verify_parquet_exact(
            tmp_path,
            pin,
            table_name="universe_daily",
            row_count=1,
            session_date=TERMINAL,
        )
    path.write_bytes(content + b"tamper")
    with pytest.raises(I3ProductionContractError, match="differs from pin"):
        production._verify_parquet_exact(
            tmp_path,
            pin,
            table_name="universe_daily",
            row_count=0,
            session_date=TERMINAL,
        )


def test_terminal_leaf_derivation_and_checkpoint_reconciliation_are_exact() -> None:
    root_id = _digest("asset-root")
    successor_id = _digest("asset-successor")
    asset_id = _digest("asset")
    table = pa.Table.from_pylist(
        [
            {
                "asset_id": asset_id,
                "asset_master_version_id": root_id,
                "predecessor_asset_master_version_id": None,
                "version_available_session": date(2026, 8, 3),
                "payload": "old",
            },
            {
                "asset_id": asset_id,
                "asset_master_version_id": successor_id,
                "predecessor_asset_master_version_id": root_id,
                "version_available_session": AVAILABLE,
                "payload": "new",
            },
        ]
    )
    leaves = production._terminal_leaf_records("asset_master", table)
    assert len(leaves) == 1
    assert leaves[0]["row_version_id"] == successor_id
    checkpoint = SimpleNamespace(
        terminal_row_versions=(
            SimpleNamespace(
                table_name="asset_master",
                stable_row_key=asset_id,
                row_version_id=successor_id,
                predecessor_row_version_id=root_id,
                row_payload_digest=leaves[0]["row_payload_digest"],
                availability_session=AVAILABLE,
            ),
        )
    )
    production._reconcile_terminal_rows(checkpoint, leaves)
    checkpoint.terminal_row_versions[0].row_payload_digest = _digest("forged-payload")
    with pytest.raises(I3ProductionContractError, match="terminal leaves"):
        production._reconcile_terminal_rows(checkpoint, leaves)


def test_terminal_leaf_derivation_rejects_external_predecessor_and_forks() -> None:
    asset_id = _digest("asset")
    with pytest.raises(I3ProductionContractError, match="external predecessor"):
        production._terminal_leaf_records(
            "asset_master",
            pa.Table.from_pylist(
                [
                    {
                        "asset_id": asset_id,
                        "asset_master_version_id": _digest("version"),
                        "predecessor_asset_master_version_id": _digest("absent"),
                        "version_available_session": AVAILABLE,
                    }
                ]
            ),
        )
    root_id = _digest("root")
    with pytest.raises(I3ProductionContractError, match="multiple terminal leaves"):
        production._terminal_leaf_records(
            "asset_master",
            pa.Table.from_pylist(
                [
                    {
                        "asset_id": asset_id,
                        "asset_master_version_id": root_id,
                        "predecessor_asset_master_version_id": None,
                        "version_available_session": AVAILABLE,
                    },
                    {
                        "asset_id": asset_id,
                        "asset_master_version_id": _digest("independent-root"),
                        "predecessor_asset_master_version_id": None,
                        "version_available_session": AVAILABLE,
                    },
                ]
            ),
        )
