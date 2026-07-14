from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.artifacts import sha256_file
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    SEPARATE_FULL_RUN_PLAN_POLICY,
    ApprovalDecision,
    ApprovalReceipt,
    ApprovalStage,
    ArrowType,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    ColumnSpec,
    FullRunPlan,
    PreviewMetadata,
    QACheckResult,
    QAMetric,
    QAOperator,
    QARule,
    QASeverity,
    QAStatus,
    QuarantineRecord,
    QuarantineReviewStatus,
    RowFunnel,
    SilverContractError,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    TableContract,
    UpstreamManifestRef,
    arrow_schema_digest,
)
from ame_stocks_api.silver.reader import PublishedSilverReader
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState

T0 = "2026-07-12T00:00:00+00:00"
T1 = "2026-07-12T00:01:00+00:00"
T2 = "2026-07-12T00:02:00+00:00"
T3 = "2026-07-12T00:03:00+00:00"
T4 = "2026-07-12T00:04:00+00:00"
T5 = "2026-07-12T00:05:00+00:00"
T6 = "2026-07-12T00:06:00+00:00"
T7 = "2026-07-12T00:07:00+00:00"
T8 = "2026-07-12T00:08:00+00:00"


def _contract(
    *,
    description: str = "Synthetic reviewed table",
    qa_severity: QASeverity = QASeverity.CRITICAL,
    qa_failure_status: QAStatus = QAStatus.FAILED,
) -> TableContract:
    return TableContract(
        domain="reference",
        table="synthetic_dim",
        schema_version=1,
        description=description,
        grain="One row per session and asset",
        columns=(
            ColumnSpec("session_date", ArrowType.DATE32, False, "Trading session"),
            ColumnSpec("asset_id", ArrowType.STRING, False, "Permanent security ID"),
            ColumnSpec("value", ArrowType.FLOAT64, True, "Synthetic value"),
        ),
        primary_key=("session_date", "asset_id"),
        partition_by=("session_date",),
        sort_by=("session_date", "asset_id"),
        source_datasets=("synthetic_source",),
        qa_rules=tuple(
            QARule(
                check_id=check_id,
                severity=qa_severity,
                metric=QAMetric.NUMERATOR,
                operator=QAOperator.EQUAL,
                limit=0.0,
                failure_status=qa_failure_status,
                description=f"{check_id} must have zero violations.",
            )
            for check_id in ("schema_exact", "primary_key_unique")
        ),
    )


def _source(root: Path, name: str = "source.json") -> ArtifactRef:
    path = root / "fixtures" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"fixture":true}\n')
    checksum = sha256_file(path)
    manifest_path = root / "manifests" / "fixtures" / f"{name}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "dataset": "synthetic_source",
                "outputs": [
                    {
                        "path": str(path.relative_to(root)),
                        "row_count": 2,
                        "sha256": checksum,
                    }
                ],
                "status": "complete",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    inventory = SourceInventory(
        source_dataset="synthetic_source",
        source_layer=SourceLayer.SYNTHETIC_FIXTURE,
        git_commit="a" * 40,
        upstream_manifests=(
            UpstreamManifestRef(
                path=str(manifest_path.relative_to(root)),
                sha256=sha256_file(manifest_path),
            ),
        ),
        artifacts=(
            SourceInventoryItem(
                path=str(path.relative_to(root)),
                sha256=checksum,
                bytes=path.stat().st_size,
                row_count=2,
                media_type="application/json",
            ),
        ),
    )
    inventory_document = SilverStore(root).register_source_inventory(inventory)
    return ArtifactRef(
        path=str(path.relative_to(root)),
        sha256=checksum,
        bytes=path.stat().st_size,
        row_count=2,
        media_type="application/json",
        role=ArtifactRole.SOURCE,
        source_dataset="synthetic_source",
        source_layer=SourceLayer.SYNTHETIC_FIXTURE,
        lineage_manifest_path=inventory_document.path,
        lineage_manifest_sha256=inventory_document.sha256,
    )


def _control_inventory(root: Path, relative_path: str) -> SourceInventory:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("BBG000000001\n", encoding="utf-8")
    upstream_path = root / "manifests" / "silver" / "source-coverage" / "fixture.json"
    upstream_path.parent.mkdir(parents=True, exist_ok=True)
    upstream_path.write_text(
        json.dumps(
            {
                "formal_identifier_receipt": {
                    "bytes": path.stat().st_size,
                    "path": relative_path,
                    "row_count": 1,
                    "sha256": sha256_file(path),
                },
                "status": "passed_with_warnings",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return SourceInventory(
        source_dataset="ticker_events",
        source_layer=SourceLayer.CONTROL_MANIFEST,
        git_commit="a" * 40,
        upstream_manifests=(
            UpstreamManifestRef(
                path=str(upstream_path.relative_to(root)),
                sha256=sha256_file(upstream_path),
            ),
        ),
        artifacts=(
            SourceInventoryItem(
                path=relative_path,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
                row_count=1,
                media_type="text/plain",
            ),
        ),
    )


def test_control_manifest_inventory_is_limited_to_its_dataset_plan_namespace(
    tmp_path: Path,
) -> None:
    valid = _control_inventory(
        tmp_path,
        "manifests/plans/ticker_events/identifiers.txt",
    )
    stored = SilverStore(tmp_path).register_source_inventory(valid)
    assert stored.path.startswith("manifests/silver/source-inventories/ticker_events/")

    cross_dataset = _control_inventory(
        tmp_path,
        "manifests/plans/other_dataset/identifiers.txt",
    )
    with pytest.raises(SilverStoreError, match="outside the declared control_manifest layer"):
        SilverStore(tmp_path).register_source_inventory(cross_dataset)


@pytest.mark.parametrize("media_types", [("application/json",), ("text/plain", "text/plain")])
def test_control_manifest_inventory_requires_one_plain_text_item(
    media_types: tuple[str, ...],
) -> None:
    with pytest.raises(SilverContractError, match="exactly one text/plain"):
        SourceInventory(
            source_dataset="ticker_events",
            source_layer=SourceLayer.CONTROL_MANIFEST,
            git_commit="a" * 40,
            upstream_manifests=(
                UpstreamManifestRef(path="manifests/fixture.json", sha256="b" * 64),
            ),
            artifacts=tuple(
                SourceInventoryItem(
                    path=f"manifests/plans/ticker_events/identifiers-{index}.txt",
                    sha256=f"{index + 1:064x}",
                    bytes=13,
                    row_count=1,
                    media_type=media_type,
                )
                for index, media_type in enumerate(media_types)
            ),
        )


def _qa_checks(
    *,
    status: QAStatus = QAStatus.PASSED,
    severity: QASeverity = QASeverity.CRITICAL,
) -> tuple[QACheckResult, ...]:
    numerator = 0 if status is QAStatus.PASSED else 1
    return tuple(
        QACheckResult(
            table="synthetic_dim",
            partition_key="all",
            check_id=check_id,
            severity=severity,
            status=status,
            numerator=numerator,
            denominator=2,
            rate=numerator / 2,
            threshold="numerator eq 0",
        )
        for check_id in ("schema_exact", "primary_key_unique")
    )


def _build(
    root: Path,
    contract: TableContract,
    workflow_id: str,
    *,
    kind: BuildKind,
    approved_preview_build_id: str | None = None,
    qa_checks: tuple[QACheckResult, ...] | None = None,
    source_name: str = "source.json",
    quarantine_severities: tuple[QASeverity, ...] = (),
    attempt: int = 1,
    retry_of_build_id: str | None = None,
    git_commit: str = "a" * 40,
    transform_version: str = "s0-fixture-v1",
    exchange_calendar_version: str = "fixture-calendar-v1",
    fixture_window: str = "2026-07-10",
    full_run_scope_policy: str | None = None,
    approved_full_run_plan_id: str | None = None,
) -> BuildManifest:
    if len(quarantine_severities) > 1:
        raise ValueError("the S0 fixture supports at most one quarantined source row")
    parameters: dict[str, object] = {"fixture_window": fixture_window}
    if full_run_scope_policy is not None:
        parameters["full_run_scope_policy"] = full_run_scope_policy
    if approved_preview_build_id is not None:
        parameters["approved_preview_build_id"] = approved_preview_build_id
    if approved_full_run_plan_id is not None:
        parameters["approved_full_run_plan_id"] = approved_full_run_plan_id
    intent = BuildIntent(
        workflow_id=workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=kind,
        attempt=attempt,
        retry_of_build_id=retry_of_build_id,
        transform_version=transform_version,
        git_commit=git_commit,
        exchange_calendar_version=exchange_calendar_version,
        inputs=(_source(root, source_name),),
        parameters=parameters,
    )
    relative_path = f"{SilverStore.build_output_prefix(intent)}/data.parquet"
    output_path = root / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data_rows = [
        {"session_date": date(2026, 7, 10), "asset_id": "asset-a", "value": 1.0},
        {"session_date": date(2026, 7, 10), "asset_id": "asset-b", "value": None},
    ]
    if quarantine_severities:
        data_rows = data_rows[:1]
    table = pa.Table.from_pylist(data_rows, schema=contract.arrow_schema)
    pq.write_table(table, output_path, compression="zstd")
    output_path.chmod(0o444)
    output = ArtifactRef(
        path=relative_path,
        sha256=sha256_file(output_path),
        bytes=output_path.stat().st_size,
        row_count=table.num_rows,
        media_type="application/vnd.apache.parquet",
        role=ArtifactRole.DATA,
        table=contract.table,
        schema_digest=arrow_schema_digest(table.schema),
    )
    checks = _qa_checks() if qa_checks is None else qa_checks
    qa_relative_path = f"{SilverStore.build_output_prefix(intent)}/qa.parquet"
    qa_output_path = root / qa_relative_path
    qa_table = pa.Table.from_pylist(
        [check.to_output_dict(intent.build_id) for check in checks],
        schema=QA_RESULT_ARROW_SCHEMA,
    )
    pq.write_table(qa_table, qa_output_path, compression="zstd")
    qa_output_path.chmod(0o444)
    qa_output = ArtifactRef(
        path=qa_relative_path,
        sha256=sha256_file(qa_output_path),
        bytes=qa_output_path.stat().st_size,
        row_count=qa_table.num_rows,
        media_type="application/vnd.apache.parquet",
        role=ArtifactRole.QA,
        table="qa_check_result",
        schema_digest=arrow_schema_digest(qa_table.schema),
    )
    outputs = [output, qa_output]
    quarantine_records = tuple(
        QuarantineRecord(
            source_record_id=f"source-row-{index}",
            table_name=contract.table,
            issue_code="fixture.quarantine",
            severity=severity,
            detected_build_id=intent.build_id,
            source_pointer=f"fixtures/source.json#row={index}",
            field_name="value",
            observed_value="invalid",
            expected_rule="Fixture value must pass the reviewed rule.",
            review_status=QuarantineReviewStatus.PENDING,
        )
        for index, severity in enumerate(quarantine_severities, start=1)
    )
    if quarantine_records:
        quarantine_path = root / SilverStore.build_output_prefix(intent) / "quarantine.parquet"
        quarantine_table = pa.Table.from_pylist(
            [record.to_dict() for record in quarantine_records],
            schema=QUARANTINE_ARROW_SCHEMA,
        )
        pq.write_table(quarantine_table, quarantine_path, compression="zstd")
        quarantine_path.chmod(0o444)
        outputs.append(
            ArtifactRef(
                path=str(quarantine_path.relative_to(root)),
                sha256=sha256_file(quarantine_path),
                bytes=quarantine_path.stat().st_size,
                row_count=quarantine_table.num_rows,
                media_type="application/vnd.apache.parquet",
                role=ArtifactRole.QUARANTINE,
                table="quarantine_record",
                schema_digest=arrow_schema_digest(quarantine_table.schema),
            )
        )
    preview = None
    if kind is BuildKind.PREVIEW:
        sample_refs: list[ArtifactRef] = []
        for sample_name, sample_rows in (
            ("input-sample.json", [{"row": 1}, {"row": 2}]),
            ("output-sample.json", [{"row": index} for index in range(table.num_rows)]),
        ):
            sample_path = root / SilverStore.build_output_prefix(intent) / sample_name
            sample_path.write_text(
                json.dumps(sample_rows, sort_keys=True),
                encoding="utf-8",
            )
            sample_path.chmod(0o444)
            sample_refs.append(
                ArtifactRef(
                    path=str(sample_path.relative_to(root)),
                    sha256=sha256_file(sample_path),
                    bytes=sample_path.stat().st_size,
                    row_count=len(sample_rows),
                    media_type="application/json",
                    role=ArtifactRole.SAMPLE,
                )
            )
        outputs.extend(sample_refs)
        full_run_projection: dict[str, object] = {
            "estimated_bytes": 2_048,
            "estimated_seconds": 2,
        }
        if full_run_scope_policy is not None:
            full_run_projection["scope_binding_mode"] = full_run_scope_policy
        preview = PreviewMetadata(
            fixed_case_ids=("normal_session",),
            fixed_case_qa_result_ids={"normal_session": (checks[0].result_id,)},
            input_sample_path=sample_refs[0].path,
            input_sample_rows=2,
            output_sample_path=sample_refs[1].path,
            output_sample_rows=table.num_rows,
            examples_truncated=False,
            full_run_inputs=intent.inputs,
            resource_usage={"elapsed_ms": 1, "peak_bytes": 1_024},
            full_run_projection=full_run_projection,
        )
    return BuildManifest(
        intent=intent,
        outputs=tuple(outputs),
        row_funnel=RowFunnel(
            input_rows=2,
            accepted_source_rows=2 - len(quarantine_records),
            exact_duplicate_excess=0,
            quarantined_source_rows=len(quarantine_records),
            unmapped_source_rows=0,
            version_preserved_rows=0,
            output_rows_by_table={contract.table: table.num_rows},
        ),
        qa_checks=checks,
        quarantine_issue_rows=len(quarantine_records),
        quarantine_unique_source_rows=len(quarantine_records),
        quarantine_issue_ids_by_severity={
            severity.value: tuple(
                record.issue_id for record in quarantine_records if record.severity is severity
            )
            for severity in QASeverity
        },
        started_at=T2 if kind is BuildKind.PREVIEW else T5,
        completed_at=T3 if kind is BuildKind.PREVIEW else T6,
        preview=preview,
    )


def _advance_to_code_ready(store: SilverStore, contract: TableContract) -> tuple[str, str]:
    snapshot = store.create_workflow(contract, actor="author", created_at=T0)
    workflow_id = snapshot.workflow_id
    snapshot = store.submit_schema_review(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T1,
    )
    snapshot = store.approve_schema(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="reviewer",
        decided_at=T2,
    )
    assert snapshot.state is WorkflowState.CODE_READY
    return workflow_id, snapshot.event_sha256


def _advance_to_preview_review(
    root: Path,
    *,
    qa_checks: tuple[QACheckResult, ...] | None = None,
    qa_severity: QASeverity = QASeverity.CRITICAL,
    qa_failure_status: QAStatus = QAStatus.FAILED,
    quarantine_severities: tuple[QASeverity, ...] = (),
    full_run_scope_policy: str | None = None,
) -> tuple[SilverStore, TableContract, BuildManifest, str]:
    store = SilverStore(root)
    contract = _contract(
        qa_severity=qa_severity,
        qa_failure_status=qa_failure_status,
    )
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    preview = _build(
        root,
        contract,
        workflow_id,
        kind=BuildKind.PREVIEW,
        qa_checks=qa_checks,
        quarantine_severities=quarantine_severities,
        full_run_scope_policy=full_run_scope_policy,
    )
    snapshot = store.record_preview_build(
        preview,
        expected_event_sha256=event_sha,
        actor="runner",
        recorded_at=T3,
    )
    snapshot = store.request_preview_review(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T4,
    )
    return store, contract, preview, snapshot.event_sha256


def _full_run_plan(
    root: Path,
    store: SilverStore,
    contract: TableContract,
    preview: BuildManifest,
    reviewed_preview_event_sha256: str,
    *,
    source_name: str = "full-source.json",
    git_commit: str = "b" * 40,
    fixture_window: str = "2016-07-11/2026-07-09",
) -> FullRunPlan:
    _, preview_stored = store.load_build(contract.table, preview.build_id)
    return FullRunPlan(
        workflow_id=preview.intent.workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        reviewed_preview_build_id=preview.build_id,
        reviewed_preview_manifest_sha256=preview_stored.sha256,
        reviewed_preview_event_sha256=reviewed_preview_event_sha256,
        transform_version="s0-fixture-full-v2",
        git_commit=git_commit,
        exchange_calendar_version="fixture-calendar-v2",
        inputs=(_source(root, source_name),),
        parameters={
            "fixture_window": fixture_window,
            "full_run_scope_policy": SEPARATE_FULL_RUN_PLAN_POLICY,
        },
        resource_projection={
            "estimated_bytes": 8_192,
            "estimated_seconds": 10,
            "minimum_free_bytes": 4_096,
        },
    )


def test_full_workflow_publishes_only_release_bound_data(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(tmp_path)
    workflow_id = preview.intent.workflow_id
    snapshot = store.approve_full_run(
        workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
    )
    full = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
    )
    snapshot = store.record_full_build(
        full,
        expected_event_sha256=snapshot.event_sha256,
        actor="runner",
        recorded_at=T6,
    )
    snapshot = store.request_publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T7,
    )
    snapshot, release = store.publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="publisher",
        decided_at=T8,
    )

    assert snapshot.state is WorkflowState.PUBLISHED
    assert len(store.workflow_events(workflow_id)) == 9
    published = PublishedSilverReader(tmp_path).inspect(release.release_id)
    assert published.release == release
    assert published.contract == contract
    assert published.build == full
    assert published.data_paths == (tmp_path / release.outputs[0].path,)
    assert not (tmp_path / "bronze").exists()


def test_full_run_plan_is_deterministic_and_strict(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)

    assert FullRunPlan.from_dict(plan.to_dict()) == plan
    assert plan.input_artifact_count == 1
    assert plan.input_rows == 2
    assert plan.input_bytes == plan.inputs[0].bytes

    tampered = plan.to_dict()
    tampered["input_bytes"] = plan.input_bytes + 1
    with pytest.raises(SilverContractError, match="input_bytes mismatch"):
        FullRunPlan.from_dict(tampered)
    with pytest.raises(SilverContractError, match="reserved keys"):
        replace(plan, parameters={"approved_preview_build_id": preview.build_id})


def test_unknown_preview_scope_policy_fails_before_registration(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    preview = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.PREVIEW,
        full_run_scope_policy="separate_approved_plan_vl",
    )
    with pytest.raises(SilverStoreError, match=r"scope policy.*unsupported"):
        store.record_preview_build(
            preview,
            expected_event_sha256=event_sha,
            actor="runner",
            recorded_at=T3,
        )
    assert store.status(workflow_id).state is WorkflowState.CODE_READY


def test_deferred_preview_rejects_a_forged_legacy_full_run_approval(
    tmp_path: Path,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    workflow_id = preview.intent.workflow_id
    _, preview_stored = store.load_build(contract.table, preview.build_id)
    receipt = ApprovalReceipt(
        workflow_id=workflow_id,
        stage=ApprovalStage.FULL_RUN,
        decision=ApprovalDecision.APPROVED,
        subject_id=preview.build_id,
        subject_manifest_sha256=preview_stored.sha256,
        expected_event_sha256=event_sha,
        approver="legacy-runner",
        decided_at=T5,
        note="forged old-format approval",
    )
    approval_stored = store._store_approval(receipt)
    forged = store._append_event(
        store.status(workflow_id),
        next_state=WorkflowState.APPROVED_FULL_RUN,
        actor="legacy-runner",
        created_at=T5,
        evidence={
            "approval_id": receipt.approval_id,
            "approval_path": approval_stored.path,
            "approval_sha256": approval_stored.sha256,
            "approved_preview_build_id": preview.build_id,
            "approved_preview_manifest_sha256": preview_stored.sha256,
        },
        note="forged old-format approval",
    )
    with pytest.raises(SilverStoreError, match="deferred preview"):
        store.status(workflow_id)

    full = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    with pytest.raises(SilverStoreError, match="deferred preview"):
        store.record_full_build(
            full,
            expected_event_sha256=forged.event_sha256,
            actor="runner",
            recorded_at=T6,
        )


def test_separate_full_run_plan_allows_later_scope_and_git_commit(
    tmp_path: Path,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    workflow_id = preview.intent.workflow_id
    with pytest.raises(SilverStoreError, match="separately reviewed full-run plan"):
        store.approve_full_run(
            workflow_id,
            expected_event_sha256=event_sha,
            approver="reviewer",
            decided_at=T5,
        )

    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    assert snapshot.state is WorkflowState.FULL_RUN_PLAN_REVIEW
    assert store.load_full_run_plan(contract.table, plan.plan_id)[0] == plan
    plan_path = tmp_path / str(snapshot.evidence["full_run_plan_path"])
    shadow_path = (
        tmp_path
        / "manifests/silver/full-run-plans/wrong_table"
        / f"plan_id={plan.plan_id}/manifest.json"
    )
    shadow_path.parent.mkdir(parents=True)
    shadow_path.write_bytes(plan_path.read_bytes())
    assert store.load_full_run_plan(contract.table, plan.plan_id)[0] == plan
    with pytest.raises(SilverStoreError, match="path identity"):
        store.load_full_run_plan("wrong_table", plan.plan_id)
    snapshot = store.approve_full_run_plan(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        expected_plan_id=plan.plan_id,
        expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
        approver="reviewer",
        decided_at=T5,
    )

    full = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
        approved_full_run_plan_id=plan.plan_id,
        source_name="full-source.json",
        git_commit=plan.git_commit,
        transform_version=plan.transform_version,
        exchange_calendar_version=plan.exchange_calendar_version,
        fixture_window="2016-07-11/2026-07-09",
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    snapshot = store.record_full_build(
        full,
        expected_event_sha256=snapshot.event_sha256,
        actor="runner",
        recorded_at=T6,
    )
    snapshot = store.request_publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T7,
    )
    snapshot, release = store.publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="publisher",
        decided_at=T8,
    )

    assert snapshot.state is WorkflowState.PUBLISHED
    assert len(store.workflow_events(workflow_id)) == 10
    assert full.intent.git_commit != preview.intent.git_commit
    assert full.intent.source_digest != preview.intent.source_digest
    assert PublishedSilverReader(tmp_path).inspect(release.release_id).build == full


@pytest.mark.parametrize(
    "mutation",
    (
        "git",
        "transform",
        "calendar",
        "source",
        "parameters",
        "missing_plan_id",
        "wrong_plan_id",
        "wrong_preview_id",
    ),
)
def test_full_build_must_match_the_separately_approved_plan(
    tmp_path: Path,
    mutation: str,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    snapshot = store.approve_full_run_plan(
        preview.intent.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        expected_plan_id=plan.plan_id,
        expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
        approver="reviewer",
        decided_at=T5,
    )
    build_arguments: dict[str, object] = {
        "approved_preview_build_id": preview.build_id,
        "approved_full_run_plan_id": plan.plan_id,
        "source_name": "full-source.json",
        "git_commit": plan.git_commit,
        "transform_version": plan.transform_version,
        "exchange_calendar_version": plan.exchange_calendar_version,
        "fixture_window": "2016-07-11/2026-07-09",
        "full_run_scope_policy": SEPARATE_FULL_RUN_PLAN_POLICY,
    }
    if mutation == "git":
        build_arguments["git_commit"] = "c" * 40
    elif mutation == "transform":
        build_arguments["transform_version"] = "s0-fixture-full-v3"
    elif mutation == "calendar":
        build_arguments["exchange_calendar_version"] = "fixture-calendar-v3"
    elif mutation == "source":
        build_arguments["source_name"] = "different-plan-source.json"
    elif mutation == "parameters":
        build_arguments["fixture_window"] = "2020-01-01/2026-07-09"
    elif mutation == "missing_plan_id":
        build_arguments["approved_full_run_plan_id"] = None
    elif mutation == "wrong_plan_id":
        build_arguments["approved_full_run_plan_id"] = "f" * 64
    elif mutation == "wrong_preview_id":
        build_arguments["approved_preview_build_id"] = "f" * 64
    wrong_code = _build(
        tmp_path,
        contract,
        preview.intent.workflow_id,
        kind=BuildKind.FULL,
        **build_arguments,
    )
    with pytest.raises(SilverStoreError, match=r"approved (?:full-run plan|preview)"):
        store.record_full_build(
            wrong_code,
            expected_event_sha256=snapshot.event_sha256,
            actor="runner",
            recorded_at=T6,
        )


def test_full_run_plan_document_is_part_of_the_trust_chain(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    plan_path = tmp_path / str(snapshot.evidence["full_run_plan_path"])
    plan_path.chmod(0o644)
    plan_path.write_bytes(plan_path.read_bytes() + b"\n")

    with pytest.raises(SilverStoreError, match=r"full-run plan|checksum"):
        store.status(preview.intent.workflow_id)


@pytest.mark.parametrize("mismatch", ("plan_id", "plan_sha256"))
def test_full_run_plan_approval_requires_explicit_exact_plan_identity(
    tmp_path: Path,
    mismatch: str,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    expected_plan_id = plan.plan_id
    expected_plan_sha256 = str(snapshot.evidence["full_run_plan_sha256"])
    if mismatch == "plan_id":
        expected_plan_id = "f" * 64
    else:
        expected_plan_sha256 = "f" * 64

    with pytest.raises(SilverStoreError, match="explicit full-run plan ID/SHA"):
        store.approve_full_run_plan(
            preview.intent.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            expected_plan_id=expected_plan_id,
            expected_plan_sha256=expected_plan_sha256,
            approver="reviewer",
            decided_at=T5,
        )

    assert store.status(preview.intent.workflow_id) == snapshot


def test_full_run_plan_approval_reuses_the_exact_preview_qa_gate(tmp_path: Path) -> None:
    checks = _qa_checks(status=QAStatus.WARNING, severity=QASeverity.MEDIUM)
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        qa_checks=checks,
        qa_severity=QASeverity.MEDIUM,
        qa_failure_status=QAStatus.WARNING,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    with pytest.raises(SilverStoreError, match="must exactly match"):
        store.approve_full_run_plan(
            preview.intent.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            expected_plan_id=plan.plan_id,
            expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
            approver="reviewer",
            decided_at=T5,
        )
    approved = store.approve_full_run_plan(
        preview.intent.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        expected_plan_id=plan.plan_id,
        expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
        approver="reviewer",
        decided_at=T5,
        waived_qa_result_ids=tuple(check.result_id for check in checks),
    )
    assert approved.state is WorkflowState.APPROVED_FULL_RUN


def test_full_run_plan_preserves_exact_retry_lineage(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    snapshot = store.approve_full_run_plan(
        preview.intent.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        expected_plan_id=plan.plan_id,
        expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
        approver="reviewer",
        decided_at=T5,
    )
    common: dict[str, object] = {
        "approved_preview_build_id": preview.build_id,
        "approved_full_run_plan_id": plan.plan_id,
        "source_name": "full-source.json",
        "git_commit": plan.git_commit,
        "transform_version": plan.transform_version,
        "exchange_calendar_version": plan.exchange_calendar_version,
        "fixture_window": "2016-07-11/2026-07-09",
        "full_run_scope_policy": SEPARATE_FULL_RUN_PLAN_POLICY,
    }
    first = _build(
        tmp_path,
        contract,
        preview.intent.workflow_id,
        kind=BuildKind.FULL,
        **common,
    )
    store.verify_build(first, contract)
    store._store_build(first)
    retry = _build(
        tmp_path,
        contract,
        preview.intent.workflow_id,
        kind=BuildKind.FULL,
        attempt=2,
        retry_of_build_id=first.build_id,
        **common,
    )
    snapshot = store.record_full_build(
        retry,
        expected_event_sha256=snapshot.event_sha256,
        actor="runner",
        recorded_at=T6,
    )
    assert snapshot.state is WorkflowState.FULL_READY


def test_full_run_plan_approval_requires_exact_high_quarantine_acceptance(
    tmp_path: Path,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        quarantine_severities=(QASeverity.HIGH,),
        full_run_scope_policy=SEPARATE_FULL_RUN_PLAN_POLICY,
    )
    plan = _full_run_plan(tmp_path, store, contract, preview, event_sha)
    snapshot = store.record_full_run_plan(
        plan,
        expected_event_sha256=event_sha,
        actor="author",
        recorded_at=T4,
    )
    with pytest.raises(SilverStoreError, match="must exactly match"):
        store.approve_full_run_plan(
            preview.intent.workflow_id,
            expected_event_sha256=snapshot.event_sha256,
            expected_plan_id=plan.plan_id,
            expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
            approver="reviewer",
            decided_at=T5,
        )
    accepted = preview.quarantine_issue_ids_by_severity[QASeverity.HIGH.value]
    approved = store.approve_full_run_plan(
        preview.intent.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        expected_plan_id=plan.plan_id,
        expected_plan_sha256=str(snapshot.evidence["full_run_plan_sha256"]),
        approver="reviewer",
        decided_at=T5,
        accepted_quarantine_issue_ids=accepted,
    )
    assert approved.state is WorkflowState.APPROVED_FULL_RUN


def test_published_reader_rechecks_output_integrity(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(tmp_path)
    workflow_id = preview.intent.workflow_id
    snapshot = store.approve_full_run(
        workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
    )
    full = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
    )
    snapshot = store.record_full_build(
        full,
        expected_event_sha256=snapshot.event_sha256,
        actor="runner",
        recorded_at=T6,
    )
    snapshot = store.request_publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T7,
    )
    _, release = store.publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="publisher",
        decided_at=T8,
    )
    reader = PublishedSilverReader(tmp_path)
    assert reader.data_files(release.release_id)

    tampered_path = tmp_path / release.outputs[0].path
    tampered_path.chmod(0o644)
    tampered_path.write_bytes(b"tampered")
    with pytest.raises(
        SilverStoreError,
        match=r"remains writable|byte count mismatch|checksum mismatch",
    ):
        reader.inspect(release.release_id)


def test_published_trust_chain_requires_intermediate_approval_receipts(
    tmp_path: Path,
) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(tmp_path)
    workflow_id = preview.intent.workflow_id
    snapshot = store.approve_full_run(
        workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
    )
    full = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
    )
    snapshot = store.record_full_build(
        full,
        expected_event_sha256=snapshot.event_sha256,
        actor="runner",
        recorded_at=T6,
    )
    snapshot = store.request_publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="author",
        created_at=T7,
    )
    _, release = store.publish(
        workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="publisher",
        decided_at=T8,
    )
    full_run_event = next(
        record
        for record in store.workflow_events(workflow_id)
        if record.event.to_state is WorkflowState.APPROVED_FULL_RUN
    )
    approval_path = tmp_path / str(full_run_event.event.evidence["approval_path"])
    original_approval = approval_path.read_bytes()
    approval_path.chmod(0o644)
    approval_path.write_bytes(original_approval + b"\n")

    with pytest.raises(SilverStoreError, match="approval evidence does not match"):
        store.status(workflow_id)
    with pytest.raises(SilverStoreError, match="approval evidence does not match"):
        PublishedSilverReader(tmp_path).inspect(release.release_id)


def test_stale_updates_and_state_skips_fail_closed(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    planned = store.create_workflow(contract, actor="author", created_at=T0)
    preview = _build(
        tmp_path,
        contract,
        planned.workflow_id,
        kind=BuildKind.PREVIEW,
    )
    with pytest.raises(SilverStoreError, match="must be code_ready"):
        store.record_preview_build(
            preview,
            expected_event_sha256=planned.event_sha256,
            actor="runner",
            recorded_at=T1,
        )

    review = store.submit_schema_review(
        planned.workflow_id,
        expected_event_sha256=planned.event_sha256,
        actor="author",
        created_at=T1,
    )
    with pytest.raises(SilverStoreError, match="stale workflow update"):
        store.submit_schema_review(
            planned.workflow_id,
            expected_event_sha256=planned.event_sha256,
            actor="author",
            created_at=T2,
        )
    failed = store.fail(
        planned.workflow_id,
        expected_event_sha256=review.event_sha256,
        actor="runner",
        created_at=T2,
        failure_code="fixture_failure",
        note="Preserve the failed evidence.",
    )
    assert failed.state is WorkflowState.FAILED
    with pytest.raises(SilverStoreError, match="terminal"):
        store.reject(
            planned.workflow_id,
            expected_event_sha256=failed.event_sha256,
            actor="reviewer",
            created_at=T3,
            reason_code="too_late",
            note="Terminal states cannot be changed.",
        )


def test_blocking_qa_prevents_full_run_approval(tmp_path: Path) -> None:
    checks = _qa_checks(status=QAStatus.FAILED, severity=QASeverity.HIGH)
    store, _, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        qa_checks=checks,
        qa_severity=QASeverity.HIGH,
        qa_failure_status=QAStatus.FAILED,
    )
    with pytest.raises(SilverStoreError, match="cannot be waived"):
        store.approve_full_run(
            preview.intent.workflow_id,
            expected_event_sha256=event_sha,
            approver="reviewer",
            decided_at=T5,
            waived_qa_result_ids=tuple(check.result_id for check in checks),
        )


def test_warning_requires_exact_result_level_waiver(tmp_path: Path) -> None:
    checks = _qa_checks(status=QAStatus.WARNING, severity=QASeverity.MEDIUM)
    store, _, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        qa_checks=checks,
        qa_severity=QASeverity.MEDIUM,
        qa_failure_status=QAStatus.WARNING,
    )
    with pytest.raises(SilverStoreError, match="must exactly match"):
        store.approve_full_run(
            preview.intent.workflow_id,
            expected_event_sha256=event_sha,
            approver="reviewer",
            decided_at=T5,
        )
    approved = store.approve_full_run(
        preview.intent.workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
        waived_qa_result_ids=tuple(check.result_id for check in checks),
    )
    assert approved.state is WorkflowState.APPROVED_FULL_RUN


def test_high_quarantine_requires_exact_receipt_level_acceptance(tmp_path: Path) -> None:
    store, _, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        quarantine_severities=(QASeverity.HIGH,),
    )
    issue_ids = preview.quarantine_issue_ids_by_severity["high"]
    with pytest.raises(SilverStoreError, match="must exactly match"):
        store.approve_full_run(
            preview.intent.workflow_id,
            expected_event_sha256=event_sha,
            approver="reviewer",
            decided_at=T5,
        )
    approved = store.approve_full_run(
        preview.intent.workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
        accepted_quarantine_issue_ids=issue_ids,
    )
    assert approved.state is WorkflowState.APPROVED_FULL_RUN


def test_critical_quarantine_cannot_be_accepted(tmp_path: Path) -> None:
    store, _, preview, event_sha = _advance_to_preview_review(
        tmp_path,
        quarantine_severities=(QASeverity.CRITICAL,),
    )
    issue_ids = preview.quarantine_issue_ids_by_severity["critical"]
    with pytest.raises(SilverStoreError, match="cannot be accepted"):
        store.approve_full_run(
            preview.intent.workflow_id,
            expected_event_sha256=event_sha,
            approver="reviewer",
            decided_at=T5,
            accepted_quarantine_issue_ids=issue_ids,
        )


def test_full_build_must_bind_the_approved_preview(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(tmp_path)
    workflow_id = preview.intent.workflow_id
    approved = store.approve_full_run(
        workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
    )
    unbound = _build(tmp_path, contract, workflow_id, kind=BuildKind.FULL)
    with pytest.raises(SilverStoreError, match="approved preview_build_id"):
        store.record_full_build(
            unbound,
            expected_event_sha256=approved.event_sha256,
            actor="runner",
            recorded_at=T6,
        )


def test_full_build_sources_must_match_the_approved_plan(tmp_path: Path) -> None:
    store, contract, preview, event_sha = _advance_to_preview_review(tmp_path)
    workflow_id = preview.intent.workflow_id
    approved = store.approve_full_run(
        workflow_id,
        expected_event_sha256=event_sha,
        approver="reviewer",
        decided_at=T5,
    )
    different_sources = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
        source_name="different-source.json",
    )
    with pytest.raises(SilverStoreError, match="full-run source inventory"):
        store.record_full_build(
            different_sources,
            expected_event_sha256=approved.event_sha256,
            actor="runner",
            recorded_at=T6,
        )


def test_qa_status_and_severity_are_evaluated_from_contract_policy(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    dishonest = _qa_checks(status=QAStatus.PASSED, severity=QASeverity.LOW)
    preview = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.PREVIEW,
        qa_checks=dishonest,
    )
    with pytest.raises(SilverStoreError, match="severity differs from policy"):
        store.record_preview_build(
            preview,
            expected_event_sha256=event_sha,
            actor="runner",
            recorded_at=T3,
        )


def test_build_registration_rejects_undeclared_output(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    preview = _build(tmp_path, contract, workflow_id, kind=BuildKind.PREVIEW)
    rogue = tmp_path / SilverStore.build_output_prefix(preview.intent) / "rogue.txt"
    rogue.write_text("not declared", encoding="utf-8")

    with pytest.raises(SilverStoreError, match="file set differs"):
        store.record_preview_build(
            preview,
            expected_event_sha256=event_sha,
            actor="runner",
            recorded_at=T3,
        )


def test_orphaned_manifest_is_recovered_by_reusing_exact_evidence(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    preview = _build(tmp_path, contract, workflow_id, kind=BuildKind.PREVIEW)
    store.verify_build(preview, contract)
    store._store_build(preview)  # Intentional crash-window fixture.

    recovered, _ = store.load_build(contract.table, preview.build_id)
    snapshot = store.record_preview_build(
        recovered,
        expected_event_sha256=event_sha,
        actor="runner",
        recorded_at=T3,
    )
    assert snapshot.state is WorkflowState.PREVIEW_READY


def test_explicit_retry_must_continue_exact_prior_attempt(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    contract = _contract()
    workflow_id, event_sha = _advance_to_code_ready(store, contract)
    first = _build(tmp_path, contract, workflow_id, kind=BuildKind.PREVIEW)
    store.verify_build(first, contract)
    store._store_build(first)  # Simulate a preserved failed attempt.
    retry = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.PREVIEW,
        attempt=2,
        retry_of_build_id=first.build_id,
    )
    snapshot = store.record_preview_build(
        retry,
        expected_event_sha256=event_sha,
        actor="runner",
        recorded_at=T3,
    )
    assert snapshot.state is WorkflowState.PREVIEW_READY

    unrelated = _build(
        tmp_path,
        contract,
        workflow_id,
        kind=BuildKind.PREVIEW,
        source_name="other.json",
        attempt=2,
        retry_of_build_id=first.build_id,
    )
    with pytest.raises(SilverStoreError, match="exact prior logical attempt"):
        store.validate_build_manifest(unrelated, contract, workflow_id=workflow_id)


def test_contract_schema_version_cannot_be_silently_replaced(tmp_path: Path) -> None:
    store = SilverStore(tmp_path)
    store.register_contract(_contract())
    with pytest.raises(SilverStoreError, match="different immutable contract"):
        store.register_contract(_contract(description="Different contract at the same version"))
