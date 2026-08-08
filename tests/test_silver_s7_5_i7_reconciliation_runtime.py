from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import identity_materialization_streaming as legacy_full
from ame_stocks_api.silver import incremental_i7_reconciliation_runtime as i7_runtime
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    ReconciliationCadence,
    ResourceGatePolicy,
)
from ame_stocks_api.silver.incremental_i7_reconciliation_runtime import (
    I7_FIXTURE_AUTHORITY,
    I7_PRODUCTION_AUTHORITY,
    CrossProducerProjectionKind,
    I7IncrementalTopSeamError,
    I7ReconciliationConfig,
    I7ReconciliationRuntimeError,
    ReconciliationPartition,
    ReconciliationTriggerKind,
    VerifiedIncrementalTopSnapshot,
    VerifiedLegacyFullSnapshot,
    _canonical_json_bytes,
    _prepare_i7_full_reconciliation_fixture,
    _stage_i7_full_reconciliation_fixture,
    _verify_i7_full_reconciliation_fixture,
    cross_producer_identity_policy_digest,
    cross_producer_projection_contract,
    cross_producer_transform_semantics_digest,
    load_official_legacy_full_snapshot,
    prepare_i7_full_reconciliation,
    stage_i7_full_reconciliation,
    verify_i7_full_reconciliation,
)

CUTOFF = date(2026, 7, 10)
AVAILABLE = date(2026, 7, 13)
DIGESTS = {
    name: stable_digest({"fixture": name})
    for name in (
        "bronze",
        "s4",
        "schema",
        "transform",
        "policy",
        "calendar",
        "incremental",
        "full",
    )
}
NATIVE_V2_RELEASE_ID = stable_digest({"fixture": "native-v2"})


def _pin_bytes(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _json_pin(root: Path, relative: str, value: object) -> ArtifactPin:
    return _pin_bytes(root, relative, _canonical_json_bytes(value))


def _field_value(field: pa.Field, *, seed: str, session: date) -> object:
    kind = field.type
    if pa.types.is_string(kind):
        if field.name == "ticker":
            return seed.upper()
        if field.name.endswith("_id") or field.name.endswith("_sha256"):
            return stable_digest({"field": field.name, "seed": seed})
        return f"{field.name}-{seed}"
    if pa.types.is_boolean(kind):
        return True
    if pa.types.is_integer(kind):
        return session.year if field.name == "session_year" else 1
    if pa.types.is_date32(kind):
        return session
    if pa.types.is_list(kind):
        return []
    raise AssertionError(f"unsupported fixture type {kind}")


def _row(table_name: str, *, seed: str = "one", session: date = CUTOFF) -> dict[str, object]:
    schema = S7_DERIVED_CONTRACTS[table_name].arrow_schema
    return {field.name: _field_value(field, seed=seed, session=session) for field in schema}


def _parquet_pin(
    root: Path,
    relative: str,
    table_name: str,
    rows: list[dict[str, object]],
) -> ArtifactPin:
    schema = S7_DERIVED_CONTRACTS[table_name].arrow_schema
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return _pin_bytes(root, relative, sink.getvalue().to_pybytes())


def _partition(
    root: Path,
    *,
    side: str,
    table_name: str,
    rows: list[dict[str, object]],
    partition_key: str | None = None,
) -> ReconciliationPartition:
    key = partition_key or (CUTOFF.isoformat() if table_name == "universe_daily" else "__table__")
    artifact = _parquet_pin(
        root,
        f"data/i7/{side}/{table_name}/{stable_digest({'key': key})}.parquet",
        table_name,
        rows,
    )
    contract = S7_DERIVED_CONTRACTS[table_name]
    return ReconciliationPartition(
        table_name=table_name,
        partition_key=key,
        artifact=artifact,
        row_count=len(rows),
        schema_digest=contract.schema_digest,
        physical_digest=stable_digest(
            {
                "artifact": artifact.to_dict(),
                "partition_key": key,
                "row_count": len(rows),
                "schema_digest": contract.schema_digest,
            }
        ),
        projection_kind=(
            CrossProducerProjectionKind.LEGACY_NATIVE_V1
            if side == "full"
            else CrossProducerProjectionKind.FIXTURE_CANONICAL_V1
        ),
        projection_contract_digest=cross_producer_projection_contract(table_name).contract_digest,
        native_release_id=(DIGESTS["full"] if side == "full" else NATIVE_V2_RELEASE_ID),
        lineage_artifacts=(artifact,),
    )


def _partitions(
    root: Path,
    side: str,
    *,
    changed_table: str | None = None,
) -> tuple[ReconciliationPartition, ...]:
    result = []
    for table_name in ("asset_master", "ticker_alias", "issuer_master", "universe_daily"):
        rows = [_row(table_name)]
        if changed_table == table_name:
            rows.append(_row(table_name, seed="two"))
        result.append(_partition(root, side=side, table_name=table_name, rows=rows))
    return tuple(result)


def _policy() -> ResourceGatePolicy:
    return ResourceGatePolicy(
        max_wall_clock_seconds=300,
        max_peak_rss_bytes=16 * 1024**3,
        min_free_disk_bytes=1,
        max_read_bytes=1024**3,
        max_write_bytes=16 * 1024**2,
        max_chain_resolution_milliseconds=60_000,
    )


def _fixture(
    root: Path,
    *,
    full_changed_table: str | None = None,
    before_changed_table: str | None = None,
    omit_full_table: str | None = None,
    cutoff: date = CUTOFF,
    full_source: str | None = None,
    cadence: ReconciliationCadence = ReconciliationCadence.MONTHLY,
) -> tuple[
    I7ReconciliationConfig,
    VerifiedIncrementalTopSnapshot,
    VerifiedLegacyFullSnapshot,
]:
    top = _json_pin(root, "controls/i7/research-top.json", {"release": DIGESTS["incremental"]})
    gate_c = _json_pin(root, "controls/i7/gate-c.json", {"target": DIGESTS["incremental"]})
    full_completion = _json_pin(
        root, "controls/i7/full-completion.json", {"release": DIGESTS["full"]}
    )
    incremental_verification = _json_pin(
        root, "controls/i7/incremental-verification.json", {"verified": True}
    )
    proof = _json_pin(root, "controls/i7/checkpoint-proof.json", {"verified": True})
    compaction_completion = _json_pin(
        root,
        "controls/i7/checkpoint-compaction-completion.json",
        {"completed": True},
    )
    full_verification = _json_pin(
        root, "controls/i7/full-verification.json", {"official_all_tree": True}
    )
    resolved = _partitions(root, "resolved")
    before = (
        _partitions(root, "before", changed_table=before_changed_table)
        if before_changed_table
        else resolved
    )
    full_parts = list(_partitions(root, "full", changed_table=full_changed_table))
    if omit_full_table:
        full_parts = [item for item in full_parts if item.table_name != omit_full_table]
    incremental = VerifiedIncrementalTopSnapshot(
        authority=I7_FIXTURE_AUTHORITY,
        release_id=DIGESTS["incremental"],
        native_v2_release_id=NATIVE_V2_RELEASE_ID,
        checkpoint_id=stable_digest({"fixture": "checkpoint"}),
        checkpoint_base_native_v2_release_id=NATIVE_V2_RELEASE_ID,
        checkpoint_base_checkpoint_id=stable_digest({"fixture": "checkpoint-base"}),
        resolved_state_digest=stable_digest({"fixture": "resolved-state"}),
        resolved_content_digest=stable_digest({"fixture": "resolved-content"}),
        physical_index_digest=stable_digest({"fixture": "physical-index"}),
        row_semantic_attestation_digest=stable_digest({"fixture": "row-semantic"}),
        cutoff_session=cutoff,
        producer_available_session=AVAILABLE,
        producer_replay_declared_bytes=8 * 1024**2,
        bronze_source_binding_digest=DIGESTS["bronze"],
        s4_source_binding_digest=DIGESTS["s4"],
        schema_bundle_digest=DIGESTS["schema"],
        transform_semantics_digest=DIGESTS["transform"],
        native_identity_policy_lineage_id=stable_digest({"fixture": "incremental-native-policy"}),
        identity_policy_bundle_id=DIGESTS["policy"],
        calendar_digest=DIGESTS["calendar"],
        top_pointer_artifact=top,
        gate_c_approval_artifact=gate_c,
        producer_verification_artifact=incremental_verification,
        release_completion_artifact=incremental_verification,
        checkpoint_base_compaction_completion_artifact=compaction_completion,
        checkpoint_base_compaction_proof_artifact=proof,
        resolved_partitions=resolved,
        checkpoint_before_partitions=before,
    )
    full = VerifiedLegacyFullSnapshot(
        authority=I7_FIXTURE_AUTHORITY,
        release_id=DIGESTS["full"],
        cutoff_session=cutoff,
        producer_available_session=AVAILABLE,
        producer_replay_declared_bytes=8 * 1024**2,
        bronze_source_binding_digest=full_source or DIGESTS["bronze"],
        s4_source_binding_digest=DIGESTS["s4"],
        schema_bundle_digest=DIGESTS["schema"],
        native_transform_lineage_id=stable_digest({"fixture": "full-native-transform"}),
        native_identity_policy_lineage_id=stable_digest({"fixture": "full-native-policy"}),
        identity_policy_bundle_id=DIGESTS["policy"],
        calendar_digest=DIGESTS["calendar"],
        completion_artifact=full_completion,
        producer_verification_artifact=full_verification,
        partitions=tuple(full_parts),
    )
    config = I7ReconciliationConfig(
        incremental_top_pointer_artifact=top,
        gate_c_approval_artifact=gate_c,
        checkpoint_compaction_completion_artifact=compaction_completion,
        independent_full_completion_artifact=full_completion,
        cutoff_session=CUTOFF,
        receipt_available_session=AVAILABLE,
        cadence=cadence,
        trigger_kind=ReconciliationTriggerKind.SCHEDULED,
        trigger_reason="periodic-independent-full-reconciliation",
        resource_policy=_policy(),
    )
    return config, incremental, full


def _loaders(
    incremental: VerifiedIncrementalTopSnapshot,
    full: VerifiedLegacyFullSnapshot,
):
    def load_incremental(
        _root: Path,
        *,
        top_pointer_artifact: ArtifactPin,
        gate_c_approval_artifact: ArtifactPin,
        checkpoint_compaction_completion_artifact: ArtifactPin,
        cutoff_session: date,
    ) -> VerifiedIncrementalTopSnapshot:
        assert top_pointer_artifact == incremental.top_pointer_artifact
        assert gate_c_approval_artifact == incremental.gate_c_approval_artifact
        assert (
            checkpoint_compaction_completion_artifact
            == incremental.checkpoint_base_compaction_completion_artifact
        )
        assert cutoff_session == incremental.cutoff_session
        return incremental

    def load_full(
        _root: Path,
        *,
        completion_artifact: ArtifactPin,
        cutoff_session: date,
    ) -> VerifiedLegacyFullSnapshot:
        assert completion_artifact == full.completion_artifact
        assert cutoff_session == full.cutoff_session
        return full

    return load_incremental, load_full


@pytest.mark.parametrize(
    "cadence", [ReconciliationCadence.MONTHLY, ReconciliationCadence.QUARTERLY]
)
def test_fixture_prepare_stage_verify_is_immutable_awaiting_review(
    tmp_path: Path, cadence: ReconciliationCadence
) -> None:
    config, incremental, full = _fixture(tmp_path, cadence=cadence)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    assert result.completion.to_dict()["state"] == "awaiting_review"
    assert result.completion.to_dict()["publish_authorized"] is False
    assert result.completion.to_dict()["automatic_publish_authorized"] is False
    assert result.completion.to_dict()["s7_5_complete"] is False
    assert result.completion.receipt.unexpected_difference_count == 0
    assert result.completion.receipt.compared_partition_count == 4
    replay = _verify_i7_full_reconciliation_fixture(
        tmp_path,
        completion_artifact=result.completion_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    assert replay.idempotent is True
    staged_again = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    assert staged_again.idempotent is True
    trigger = json.loads((tmp_path / prepared.trigger_artifact.path).read_bytes())
    alert = json.loads((tmp_path / result.completion.alert_artifact.path).read_bytes())
    assert trigger["cadence"] == cadence.value
    assert alert["outcome"] == "passed"
    assert alert["requires_review"] is True


def test_missing_partition_fails_prepare(tmp_path: Path) -> None:
    with pytest.raises(I7ReconciliationRuntimeError, match="exact four tables"):
        _fixture(tmp_path, omit_full_table="issuer_master")


def test_extra_row_writes_critical_alert_but_no_completion(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path, full_changed_table="asset_master")
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="found unexpected"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )
    alert_path = next((tmp_path / "manifests/fixtures/i7/alerts").rglob("manifest.json"))
    alert = json.loads(alert_path.read_bytes())
    assert alert["severity"] == "critical"
    assert alert["unexpected_difference_count"] > 0
    assert not (tmp_path / "manifests/fixtures/i7/completions").exists()


def test_checkpoint_logical_drift_is_independently_detected(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path, before_changed_table="issuer_master")
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="checkpoint-rebase"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


def test_wrong_cutoff_and_source_binding_fail_prepare(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path / "cutoff", cutoff=date(2026, 7, 9))
    incremental_loader, full_loader = _loaders(incremental, full)
    with pytest.raises((AssertionError, I7ReconciliationRuntimeError)):
        _prepare_i7_full_reconciliation_fixture(
            tmp_path / "cutoff",
            config=config,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )
    config, incremental, full = _fixture(
        tmp_path / "source", full_source=stable_digest({"wrong": "source"})
    )
    incremental_loader, full_loader = _loaders(incremental, full)
    with pytest.raises(I7ReconciliationRuntimeError, match="Bronze source binding differs"):
        _prepare_i7_full_reconciliation_fixture(
            tmp_path / "source",
            config=config,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


@pytest.mark.parametrize(
    "path",
    (
        "manifests/latest/top.json",
        "tmp/top.json",
        "manifests/fixtures/top.json",
        "manifests/i7/../top.json",
    ),
)
def test_noncanonical_latest_tmp_or_fixture_authority_path_is_rejected(path: str) -> None:
    with pytest.raises((I7ReconciliationRuntimeError, SilverContractError)):
        pin = ArtifactPin(path=path, sha256="a" * 64, bytes=1)
        I7ReconciliationConfig(
            incremental_top_pointer_artifact=pin,
            gate_c_approval_artifact=ArtifactPin(
                path="controls/i7/gate-c.json", sha256="b" * 64, bytes=1
            ),
            checkpoint_compaction_completion_artifact=ArtifactPin(
                path="controls/i7/compaction.json", sha256="d" * 64, bytes=1
            ),
            independent_full_completion_artifact=ArtifactPin(
                path="controls/i7/full.json", sha256="c" * 64, bytes=1
            ),
            cutoff_session=CUTOFF,
            receipt_available_session=AVAILABLE,
            cadence=ReconciliationCadence.MONTHLY,
            trigger_kind=ReconciliationTriggerKind.INCIDENT,
            trigger_reason="attack-regression",
            resource_policy=_policy(),
        )


def test_legacy_full_locator_is_rejected_before_first_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reads = 0

    def forbidden_read(*_args: object, **_kwargs: object) -> bytes:
        nonlocal reads
        reads += 1
        raise AssertionError("copied legacy completion was opened")

    monkeypatch.setattr(i7_runtime, "_read_exact_pin", forbidden_read)
    copied = ArtifactPin(
        path="manifests/silver/identity/copied-full-completion/manifest.json",
        sha256="a" * 64,
        bytes=1,
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="locator is noncanonical"):
        load_official_legacy_full_snapshot(
            tmp_path,
            completion_artifact=copied,
            cutoff_session=CUTOFF,
        )
    assert reads == 0


def test_immutable_run_spec_no_clobber(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    (tmp_path / prepared.run_spec_artifact.path).write_bytes(b"{}\n")
    with pytest.raises(I7ReconciliationRuntimeError, match="immutable I7 RunSpec bytes differ"):
        _prepare_i7_full_reconciliation_fixture(
            tmp_path,
            config=config,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


def test_joint_resign_cannot_enter_production_without_gate_c_loader(tmp_path: Path) -> None:
    config, _, _ = _fixture(tmp_path)
    assert set(inspect.signature(prepare_i7_full_reconciliation).parameters) == {
        "data_root",
        "config",
    }
    assert set(inspect.signature(stage_i7_full_reconciliation).parameters) == {
        "data_root",
        "run_spec_artifact",
    }
    assert set(inspect.signature(verify_i7_full_reconciliation).parameters) == {
        "data_root",
        "completion_artifact",
    }
    with pytest.raises(I7IncrementalTopSeamError, match="P0 I6 seam"):
        prepare_i7_full_reconciliation(tmp_path, config=config)
    assert not (tmp_path / "manifests/silver/incremental/i7").exists()


def test_verify_detects_tampered_exact_parquet(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    target = tmp_path / incremental.resolved_partitions[0].artifact.path
    target.write_bytes(target.read_bytes() + b"tamper")
    with pytest.raises(I7ReconciliationRuntimeError, match="exact pin differs"):
        _verify_i7_full_reconciliation_fixture(
            tmp_path,
            completion_artifact=result.completion_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


def test_joint_resigned_alert_and_completion_still_fail_semantic_replay(
    tmp_path: Path,
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    alert_path = tmp_path / result.completion.alert_artifact.path
    alert = json.loads(alert_path.read_bytes())
    alert["severity"] = "critical"
    alert_payload = dict(alert)
    alert_payload.pop("alert_id")
    alert["alert_id"] = stable_digest(alert_payload)
    alert_bytes = _canonical_json_bytes(alert)
    alert_path.write_bytes(alert_bytes)
    alert_pin = ArtifactPin(
        path=result.completion.alert_artifact.path,
        sha256=hashlib.sha256(alert_bytes).hexdigest(),
        bytes=len(alert_bytes),
    )

    completion_path = tmp_path / result.completion_artifact.path
    completion = json.loads(completion_path.read_bytes())
    completion["alert_artifact"] = alert_pin.to_dict()
    completion_payload = dict(completion)
    completion_payload.pop("completion_id")
    completion["completion_id"] = stable_digest(completion_payload)
    completion_bytes = _canonical_json_bytes(completion)
    completion_path.write_bytes(completion_bytes)
    resigned_completion = ArtifactPin(
        path=result.completion_artifact.path,
        sha256=hashlib.sha256(completion_bytes).hexdigest(),
        bytes=len(completion_bytes),
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="I7 alert does not replay"):
        _verify_i7_full_reconciliation_fixture(
            tmp_path,
            completion_artifact=resigned_completion,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


@pytest.mark.parametrize(
    ("approved_at", "chronology_fails"),
    (
        (datetime(2026, 7, 10, 19, tzinfo=UTC), False),
        (datetime(2026, 7, 10, 21, tzinfo=UTC), True),
    ),
)
def test_official_legacy_loader_calls_all_tree_verifier_and_checks_chronology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    approved_at: datetime,
    chronology_fails: bool,
) -> None:
    plan_id = "1" * 64
    approval_id = "2" * 64
    candidate_id = "3" * 64
    completion_id = "4" * 64
    candidate_root = legacy_full._candidate_path(candidate_id)
    outputs: dict[str, object] = {}
    for table_name in ("asset_master", "ticker_alias", "issuer_master", "universe_daily"):
        relative = (
            f"data/universe_daily/session_date={CUTOFF.isoformat()}/part-00000.parquet"
            if table_name == "universe_daily"
            else f"data/{table_name}.parquet"
        )
        pin = _parquet_pin(
            tmp_path,
            f"{candidate_root}/{relative}",
            table_name,
            [_row(table_name)],
        )
        receipt = {
            "bytes": pin.bytes,
            "path": relative,
            "row_count": 1,
            "schema_digest": S7_DERIVED_CONTRACTS[table_name].schema_digest,
            "sha256": pin.sha256,
        }
        outputs[table_name] = [receipt] if table_name == "universe_daily" else receipt
    outputs["qa"] = {"bytes": 1, "path": "qa/qa.json", "sha256": "f" * 64}
    candidate = {"outputs": outputs}
    candidate_pin = _json_pin(tmp_path, f"{candidate_root}/manifest.json", candidate)
    completion = {
        "approval_id": approval_id,
        "candidate_id": candidate_id,
        "candidate_manifest": candidate_pin.to_dict(),
        "completed_at_utc": "2026-07-10T20:00:00+00:00",
        "completion_id": completion_id,
        "plan_id": plan_id,
    }
    completion_pin = _json_pin(
        tmp_path, legacy_full._completion_path(plan_id, approval_id), completion
    )
    registry_pins = (
        SimpleNamespace(registry_name="identity_adjudication", release_id="5" * 64),
        SimpleNamespace(registry_name="identity_cross_market_adjudication", release_id="6" * 64),
        SimpleNamespace(registry_name="provider_composite_override", release_id="7" * 64),
        SimpleNamespace(registry_name="share_class_adjudication", release_id="8" * 64),
        SimpleNamespace(registry_name="asset_transition", release_id="9" * 64),
    )
    binding = SimpleNamespace(
        mode="production",
        cutoff_session=CUTOFF,
        source_release_pins={"source": {"release_id": "a" * 64}},
        six_release_binding_id="b" * 64,
        s4_release_set_id="c" * 64,
        s4_release_set_manifest=ArtifactPin(
            path="manifests/s4/release.json", sha256="d" * 64, bytes=1
        ),
        registry_pins=registry_pins,
        calendar_artifact_id="e" * 64,
        calendar_artifact_sha256="f" * 64,
        declared_source_bytes=1,
        contract_approvals=(SimpleNamespace(bytes=2), SimpleNamespace(bytes=3)),
        runtime_binding={"runtime_file_set_digest": "0" * 64},
        source_binding_id="1" * 64,
    )
    approval = SimpleNamespace(
        approval_id=approval_id,
        approved_at_utc=approved_at,
        approval_availability={"source_available_session": AVAILABLE.isoformat()},
    )
    source_binding_receipt = {
        "bytes": 1,
        "path": "manifests/source-binding.json",
        "sha256": "0" * 64,
    }
    plan = {
        "bounded_profile_evidence": None,
        "contract_pins": {"fixture": "1" * 64},
        "policy_version": "fixture-policy-v1",
        "resource_caps": {},
        "source_binding": source_binding_receipt,
    }
    control_receipt = SimpleNamespace(bytes=1)
    called = {"tree": 0, "sources": 0}

    monkeypatch.setattr(
        legacy_full,
        "_load_execution_controls",
        lambda *_args, **_kwargs: {
            "plan": plan,
            "approval": approval,
            "binding": binding,
            "plan_receipt": control_receipt,
            "request_receipt": control_receipt,
            "approval_receipt": control_receipt,
        },
    )
    monkeypatch.setattr(
        legacy_full.StreamingResourceCaps,
        "from_dict",
        classmethod(lambda cls, value: SimpleNamespace()),
    )

    def verify(*_args, **kwargs):
        called["tree"] += 1
        assert kwargs["expected_candidate_id"] == candidate_id
        return SimpleNamespace(
            candidate_id=candidate_id,
            completion_id=completion_id,
            state=legacy_full.STREAMING_STATE,
        )

    def sources(*_args, **_kwargs):
        called["sources"] += 1

    monkeypatch.setattr(legacy_full, "_verify_completion_and_candidate", verify)
    monkeypatch.setattr(legacy_full, "_load_verified_execution_sources", sources)
    monkeypatch.setattr(
        legacy_full,
        "_calendar_availability",
        lambda *_args, **_kwargs: {"source_available_session": AVAILABLE.isoformat()},
    )
    if chronology_fails:
        with pytest.raises(I7ReconciliationRuntimeError, match="predates its exact approval"):
            load_official_legacy_full_snapshot(
                tmp_path,
                completion_artifact=completion_pin,
                cutoff_session=CUTOFF,
            )
        assert called == {"tree": 0, "sources": 0}
        return
    loaded = load_official_legacy_full_snapshot(
        tmp_path,
        completion_artifact=completion_pin,
        cutoff_session=CUTOFF,
    )
    assert loaded.release_id == candidate_id
    assert len(loaded.partitions) == 4
    expected_output_bytes = 1 + sum(item.artifact.bytes for item in loaded.partitions)
    expected_control_and_source_bytes = 3 + 1 + 1 + 2 + 3
    assert loaded.producer_replay_declared_bytes == (
        completion_pin.bytes
        + candidate_pin.bytes
        + expected_output_bytes
        + expected_control_and_source_bytes
    )
    assert called == {"tree": 1, "sources": 1}


def test_fixture_run_spec_rejects_joint_resigned_loader_result(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    resigned = replace(incremental, release_id=stable_digest({"resigned": True}))
    resigned_loader, _ = _loaders(resigned, full)
    with pytest.raises((AssertionError, I7ReconciliationRuntimeError)):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=resigned_loader,
            full_loader=full_loader,
        )


def test_receipt_availability_cannot_predate_either_producer(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    future = date(2026, 7, 14)
    incremental = replace(incremental, producer_available_session=future)
    incremental_loader, full_loader = _loaders(incremental, full)
    with pytest.raises(I7ReconciliationRuntimeError, match="predates a producer"):
        _prepare_i7_full_reconciliation_fixture(
            tmp_path,
            config=config,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )
    assert not (tmp_path / "manifests/fixtures/i7").exists()


def test_named_projection_contract_rejects_native_projection_confusion(
    tmp_path: Path,
) -> None:
    partition = _partition(
        tmp_path,
        side="resolved",
        table_name="asset_master",
        rows=[_row("asset_master")],
    )
    contract = cross_producer_projection_contract("asset_master")
    assert contract.incremental_native_schema_digest != contract.canonical_schema_digest
    assert contract.legacy_native_schema_digest == contract.canonical_schema_digest
    with pytest.raises(I7ReconciliationRuntimeError, match="schema/projection kind differ"):
        replace(
            partition,
            projection_kind=CrossProducerProjectionKind.INCREMENTAL_NATIVE_V2,
        )
    with pytest.raises(I7ReconciliationRuntimeError, match="projection contract differs"):
        replace(
            partition,
            projection_contract_digest=stable_digest({"attacker": "native-digest"}),
        )


def test_native_policy_lineages_are_preserved_beside_named_common_projection(
    tmp_path: Path,
) -> None:
    _, incremental, full = _fixture(tmp_path)
    assert incremental.native_identity_policy_lineage_id != full.native_identity_policy_lineage_id
    assert incremental.identity_policy_bundle_id == full.identity_policy_bundle_id
    left = cross_producer_identity_policy_digest({"b": "2" * 64, "a": "1" * 64})
    right = cross_producer_identity_policy_digest({"a": "1" * 64, "b": "2" * 64})
    assert left == right
    assert incremental.to_dict()["native_identity_policy_lineage_id"] == (
        incremental.native_identity_policy_lineage_id
    )
    bridge = cross_producer_transform_semantics_digest(
        incremental.transform_semantics_digest,
        full.native_transform_lineage_id,
    )
    assert bridge not in {
        incremental.transform_semantics_digest,
        full.native_transform_lineage_id,
    }
    assert full.to_dict()["native_transform_lineage_id"] == full.native_transform_lineage_id


def test_declared_overcap_stops_before_stage_producer_replay(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    low_policy = replace(prepared.run_spec.resource_policy, max_read_bytes=1)
    low_spec = replace(prepared.run_spec, resource_policy=low_policy)
    low_spec_pin = _pin_bytes(
        tmp_path,
        (
            "manifests/fixtures/i7/full-reconciliation-run-specs/"
            f"run_spec_id={low_spec.run_spec_id}/manifest.json"
        ),
        low_spec.canonical_bytes(),
    )
    calls = {"incremental": 0, "full": 0}

    def reject_incremental(*_args: object, **_kwargs: object) -> VerifiedIncrementalTopSnapshot:
        calls["incremental"] += 1
        raise AssertionError("incremental producer replay crossed declared preflight")

    def reject_full(*_args: object, **_kwargs: object) -> VerifiedLegacyFullSnapshot:
        calls["full"] += 1
        raise AssertionError("Full producer replay crossed declared preflight")

    with pytest.raises(I7ReconciliationRuntimeError, match="declared-read preflight"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=low_spec_pin,
            incremental_loader=reject_incremental,
            full_loader=reject_full,
        )
    assert calls == {"incremental": 0, "full": 0}


def test_stage_process_io_gate_observes_whole_producer_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    values = iter(
        (
            (100, 100),
            (100, 100),
            (config.resource_policy.max_read_bytes + 101, 100),
        )
    )
    last = (config.resource_policy.max_read_bytes + 101, 100)
    monkeypatch.setattr(i7_runtime, "_proc_io_bytes", lambda: next(values, last))
    with pytest.raises(I7ReconciliationRuntimeError, match="read-byte resource gate"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


def test_stage_midrun_disk_floor_is_a_hard_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    calls = 0

    def disk_free(_root: Path) -> int:
        nonlocal calls
        calls += 1
        return 10 * 1024**3 if calls <= 3 else 0

    monkeypatch.setattr(i7_runtime, "_disk_free", disk_free)
    with pytest.raises(I7ReconciliationRuntimeError, match="disk-floor resource gate"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )


def test_completion_freezes_disk_rss_and_process_io_phase_evidence(tmp_path: Path) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    observation = result.completion.resource_observation
    disk_phases = dict(observation.phase_minimum_free_disk_bytes)
    rss_phases = dict(observation.phase_peak_rss_bytes)
    assert disk_phases.keys() == rss_phases.keys()
    assert {"entry", "preflight", "producer_replay", "before_completion"} <= disk_phases.keys()
    assert observation.minimum_free_disk_bytes == min(disk_phases.values())
    assert observation.peak_rss_bytes == max(rss_phases.values())
    assert observation.process_read_bytes >= 0
    assert observation.process_write_bytes >= 0
    expected_full_write = (
        result.completion.alert_artifact.bytes
        + result.completion.receipt.qa_artifact.bytes
        + result.completion.receipt.details_artifact.bytes
        + sum(
            partition.details_artifact.bytes
            for table in result.completion.receipt.table_evidence
            for partition in table.partitions
        )
        + result.completion_artifact.bytes
    )
    assert observation.observed_write_bytes == expected_full_write


def test_failed_staged_completion_gate_leaves_no_visible_success_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    original = i7_runtime._RuntimeResourceTracker.check

    def fail_staged_gate(self, phase: str, *, written_bytes: int | None = None) -> None:
        if phase == "completion_staged":
            raise I7ReconciliationRuntimeError("staged completion resource gate failed")
        original(self, phase, written_bytes=written_bytes)

    monkeypatch.setattr(i7_runtime._RuntimeResourceTracker, "check", fail_staged_gate)
    with pytest.raises(I7ReconciliationRuntimeError, match="staged completion resource gate"):
        _stage_i7_full_reconciliation_fixture(
            tmp_path,
            run_spec_artifact=prepared.run_spec_artifact,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )
    completion_root = tmp_path / "manifests/fixtures/i7/completions"
    assert not tuple(completion_root.rglob("manifest.json"))
    assert not tuple(tmp_path.rglob("*.staged-*"))


def test_production_checkpoint_rebase_requires_exact_compaction_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ame_stocks_api.silver import incremental_i6_pointer_runtime as i6_runtime

    top = _json_pin(tmp_path, "controls/i7/exact-top.json", {"top": True})
    gate_c = _json_pin(tmp_path, "controls/i7/exact-gate-c.json", {"gate": "c"})
    compaction_completion = _json_pin(
        tmp_path,
        "controls/i7/exact-compaction.json",
        {"completed": True},
    )

    class ExactTop:
        research_top_event_artifact = top
        gate_c_approval_artifact = gate_c
        terminal_session = CUTOFF
        source_cutoff_session = CUTOFF

    monkeypatch.setattr(i6_runtime, "ResearchTopSnapshot", ExactTop)
    monkeypatch.setattr(i6_runtime, "load_research_top_snapshot_exact", lambda _root: ExactTop())
    with pytest.raises(I7IncrementalTopSeamError, match="independent checkpoint BASE"):
        i7_runtime._load_production_incremental_top_snapshot(
            tmp_path,
            top_pointer_artifact=top,
            gate_c_approval_artifact=gate_c,
            checkpoint_compaction_completion_artifact=compaction_completion,
            cutoff_session=CUTOFF,
        )


def test_copied_production_run_spec_locator_fails_before_producer_replay(
    tmp_path: Path,
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    copied = replace(prepared.run_spec, authority=I7_PRODUCTION_AUTHORITY)
    wrong_directory_id = "f" * 64
    assert copied.run_spec_id != wrong_directory_id
    copied_pin = _pin_bytes(
        tmp_path,
        (
            "manifests/silver/incremental/i7/full-reconciliation-run-specs/"
            f"run_spec_id={wrong_directory_id}/manifest.json"
        ),
        copied.canonical_bytes(),
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="directory ID differs"):
        stage_i7_full_reconciliation(tmp_path, run_spec_artifact=copied_pin)


def test_copied_production_completion_locator_fails_before_deep_replay(
    tmp_path: Path,
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    copied = replace(result.completion, authority=I7_PRODUCTION_AUTHORITY)
    wrong_directory_id = "e" * 64
    assert copied.run_spec_id != wrong_directory_id
    copied_pin = _pin_bytes(
        tmp_path,
        (
            "manifests/silver/incremental/i7/completions/"
            f"run_spec_id={wrong_directory_id}/manifest.json"
        ),
        copied.canonical_bytes(),
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="directory ID differs"):
        verify_i7_full_reconciliation(tmp_path, completion_artifact=copied_pin)


def test_joint_resigned_understated_resource_evidence_fails_deep_replay(
    tmp_path: Path,
) -> None:
    config, incremental, full = _fixture(tmp_path)
    incremental_loader, full_loader = _loaders(incremental, full)
    prepared = _prepare_i7_full_reconciliation_fixture(
        tmp_path,
        config=config,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    result = _stage_i7_full_reconciliation_fixture(
        tmp_path,
        run_spec_artifact=prepared.run_spec_artifact,
        incremental_loader=incremental_loader,
        full_loader=full_loader,
    )
    completion_path = tmp_path / result.completion_artifact.path
    completion = json.loads(completion_path.read_bytes())
    completion["resource_observation"]["metered_read_bytes"] = 0
    payload = dict(completion)
    payload.pop("completion_id")
    completion["completion_id"] = stable_digest(payload)
    content = _canonical_json_bytes(completion)
    completion_path.write_bytes(content)
    resigned = ArtifactPin(
        path=result.completion_artifact.path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    with pytest.raises(I7ReconciliationRuntimeError, match="metered-read evidence is understated"):
        _verify_i7_full_reconciliation_fixture(
            tmp_path,
            completion_artifact=resigned,
            incremental_loader=incremental_loader,
            full_loader=full_loader,
        )
