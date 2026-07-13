from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from ame_stocks_api.cli.silver_condition_codes_lifecycle import build_parser
from ame_stocks_api.silver import condition_code_lifecycle as lifecycle
from ame_stocks_api.silver.condition_code_lifecycle import (
    CURRENT_CONDITION_CODE_AUTHORIZATION,
    CURRENT_CONDITION_CODES_ARTIFACT_PATH,
    CURRENT_CONDITION_CODES_MANIFEST_PATH,
    S1_EXCHANGE_RELEASE_ID,
    S1_EXCHANGE_RELEASE_MANIFEST_SHA256,
    S3_COMPLETION_AUTHORIZATION,
    ConditionCodeAuthorization,
    _require_bridge_parent_coverage,
)
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    PreviewMetadata,
    QACheckResult,
    QASeverity,
    RowFunnel,
    SourceInventory,
    SourceInventoryItem,
    SourceLayer,
    UpstreamManifestRef,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState


def _tables() -> tuple[pa.Table, pa.Table]:
    dim_rows = [
        {
            "capture_date": date(2026, 7, 11),
            "asset_class": "stocks",
            "condition_type": "sale_condition",
            "condition_id": item,
            "is_legacy": False,
        }
        for item in range(94)
    ]
    bridge_rows = [dict(row, data_type="trade") for row in dim_rows]
    bridge_rows.extend(dict(dim_rows[item], data_type="bbo") for item in range(29))
    return pa.Table.from_pylist(dim_rows), pa.Table.from_pylist(bridge_rows)


def test_production_authorization_is_exact_and_not_cli_overridable() -> None:
    authorization = CURRENT_CONDITION_CODE_AUTHORIZATION
    assert authorization.manifest_path == CURRENT_CONDITION_CODES_MANIFEST_PATH
    assert authorization.artifact_path == CURRENT_CONDITION_CODES_ARTIFACT_PATH
    assert authorization.expected_source_rows == 94
    assert authorization.expected_dim_rows == 94
    assert authorization.expected_bridge_rows == 123
    assert authorization.sample_limit == 94
    assert authorization.exchange_release_id == S1_EXCHANGE_RELEASE_ID
    assert authorization.exchange_release_manifest_sha256 == S1_EXCHANGE_RELEASE_MANIFEST_SHA256
    assert S3_COMPLETION_AUTHORIZATION == "你直接把S3推进到完成吧"
    assert {action.dest for action in build_parser()._actions} == {
        "help",
        "data_root",
        "repo_root",
        "git_commit",
    }


def test_authorization_refuses_alternate_production_source() -> None:
    with pytest.raises(ValueError, match="manifest path"):
        ConditionCodeAuthorization(manifest_path="manifests/massive/condition_codes/other.json")


def test_bridge_parent_coverage_accepts_exact_94_to_123_relation() -> None:
    dim, bridge = _tables()
    _require_bridge_parent_coverage(dim, bridge)


@pytest.mark.parametrize("mutation", ["orphan", "missing", "duplicate"])
def test_bridge_parent_coverage_fails_closed(mutation: str) -> None:
    dim, bridge = _tables()
    rows = bridge.to_pylist()
    if mutation == "orphan":
        rows[-1]["condition_id"] = 999
    elif mutation == "missing":
        rows = [item for item in rows if item["condition_id"] != 93]
        rows.append(dict(rows[0], data_type="nbbo"))
    else:
        rows[-1] = dict(rows[0])
    damaged = pa.Table.from_pylist(rows)
    with pytest.raises(SilverStoreError, match=r"coverage|duplicated"):
        _require_bridge_parent_coverage(dim, damaged)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _fixture_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    for relative in lifecycle._LOGIC_CLOSURE:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"tracked fixture for {relative}\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "Tests")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(
        lifecycle,
        "__file__",
        str(repo / "backend/ame_stocks_api/silver/condition_code_lifecycle.py"),
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _write_condition_bronze(root: Path) -> tuple[str, str, bytes, bytes]:
    results = [
        {
            "asset_class": "stocks",
            "data_types": ["trade", "bbo"] if item <= 29 else ["trade"],
            "id": item,
            "name": f"Synthetic condition {item}",
            "sip_mapping": {"CTA": chr(65 + item % 26)},
            "type": "sale_condition",
        }
        for item in range(1, 95)
    ]
    raw = json.dumps(
        {
            "count": len(results),
            "request_id": "synthetic-condition-provider-request",
            "results": results,
            "status": "OK",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact = root / CURRENT_CONDITION_CODES_ARTIFACT_PATH
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(compressed)
    manifest = root / CURRENT_CONDITION_CODES_MANIFEST_PATH
    manifest.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "path": CURRENT_CONDITION_CODES_ARTIFACT_PATH,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": 94,
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "completed_at": "2026-07-11T18:54:40.265369+00:00",
        "created_at": "2026-07-11T18:54:39+00:00",
        "dataset": "condition_codes",
        "provider": "massive",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "condition_codes",
            "end": "2026-07-09",
            "parameters": {},
            "start": "2026-07-09",
        },
        "request_id": lifecycle.CURRENT_CONDITION_CODES_REQUEST_ID,
        "status": "complete",
    }
    manifest.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    return _sha(manifest), _sha(artifact), manifest.read_bytes(), artifact.read_bytes()


def _exchange_source(root: Path, git_commit: str) -> ArtifactRef:
    source = root / "bronze/exchanges/fixture.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        json.dumps([{"exchange_id": item} for item in range(1, 28)]), encoding="utf-8"
    )
    manifest = root / "manifests/fixtures/exchanges.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps(
            {
                "outputs": [
                    {
                        "bytes": source.stat().st_size,
                        "path": str(source.relative_to(root)),
                        "row_count": 27,
                        "sha256": _sha(source),
                    }
                ],
                "status": "complete",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    inventory = SourceInventory(
        source_dataset="exchanges",
        source_layer=SourceLayer.BRONZE,
        git_commit=git_commit,
        upstream_manifests=(
            UpstreamManifestRef(path=str(manifest.relative_to(root)), sha256=_sha(manifest)),
        ),
        artifacts=(
            SourceInventoryItem(
                path=str(source.relative_to(root)),
                sha256=_sha(source),
                bytes=source.stat().st_size,
                row_count=27,
                media_type="application/json",
            ),
        ),
    )
    stored = SilverStore(root).register_source_inventory(inventory)
    return ArtifactRef(
        path=str(source.relative_to(root)),
        sha256=_sha(source),
        bytes=source.stat().st_size,
        row_count=27,
        media_type="application/json",
        role=ArtifactRole.SOURCE,
        source_dataset="exchanges",
        source_layer=SourceLayer.BRONZE,
        lineage_manifest_path=stored.path,
        lineage_manifest_sha256=stored.sha256,
    )


def _exchange_table() -> pa.Table:
    captured = datetime(2026, 7, 11, 18, 54, 40, tzinfo=UTC)
    available = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    rows = [
        {
            "acronym": None,
            "asset_class": "stocks",
            "availability_rule": "first_xnys_open_after_source_capture_v1",
            "available_at_utc": available,
            "available_session": date(2026, 7, 13),
            "capture_date": date(2026, 7, 11),
            "exchange_id": item,
            "exchange_type": "exchange",
            "locale": "us",
            "mic": None,
            "name": f"Synthetic exchange {item}",
            "operating_mic": None,
            "participant_id": None,
            "snapshot_scope": "current_reference_snapshot",
            "source_artifact_sha256": f"{item + 1:064x}",
            "source_capture_at_utc": captured,
            "source_page_sequence": 0,
            "source_provider_request_id": "synthetic-exchange-provider-request",
            "source_record_id": f"{item + 101:064x}",
            "source_request_id": "1" * 64,
            "source_row_hash": f"{item + 201:064x}",
            "source_row_ordinal": item,
            "url": None,
        }
        for item in range(1, 28)
    ]
    return pa.Table.from_pylist(rows, schema=EXCHANGE_DIM_CONTRACT.arrow_schema)


def _exchange_checks() -> tuple[QACheckResult, ...]:
    return tuple(
        QACheckResult(
            table=EXCHANGE_DIM_CONTRACT.table,
            partition_key="all",
            check_id=rule.check_id,
            severity=rule.severity,
            status=rule.expected_status(numerator=0, rate=0.0),
            numerator=0,
            denominator=27,
            rate=0.0,
            threshold=rule.threshold_expression,
        )
        for rule in EXCHANGE_DIM_CONTRACT.qa_rules
    )


def _exchange_build(
    root: Path,
    *,
    workflow_id: str,
    git_commit: str,
    source: ArtifactRef,
    kind: BuildKind,
    approved_preview_build_id: str | None = None,
) -> BuildManifest:
    parameters: dict[str, object] = {"fixture": "published-s1"}
    if approved_preview_build_id is not None:
        parameters["approved_preview_build_id"] = approved_preview_build_id
    intent = BuildIntent(
        workflow_id=workflow_id,
        domain=EXCHANGE_DIM_CONTRACT.domain,
        table=EXCHANGE_DIM_CONTRACT.table,
        schema_version=EXCHANGE_DIM_CONTRACT.schema_version,
        contract_id=EXCHANGE_DIM_CONTRACT.contract_id,
        kind=kind,
        attempt=1,
        retry_of_build_id=None,
        transform_version="synthetic-exchange-v1",
        git_commit=git_commit,
        exchange_calendar_version="synthetic-calendar-v1",
        inputs=(source,),
        parameters=parameters,
    )
    prefix = SilverStore.build_output_prefix(intent)
    table = _exchange_table()
    checks = _exchange_checks()
    data = lifecycle._write_parquet(
        root,
        f"{prefix}/data/capture_date=2026-07-11/part-00000.parquet",
        table,
        ArtifactRole.DATA,
        EXCHANGE_DIM_CONTRACT.table,
    )
    qa = lifecycle._write_parquet(
        root,
        f"{prefix}/qa/qa-check-result.parquet",
        pa.Table.from_pylist(
            [item.to_output_dict(intent.build_id) for item in checks],
            QA_RESULT_ARROW_SCHEMA,
        ),
        ArtifactRole.QA,
        "qa_check_result",
    )
    quarantine = lifecycle._write_parquet(
        root,
        f"{prefix}/quarantine/quarantine-record.parquet",
        pa.Table.from_pylist([], QUARANTINE_ARROW_SCHEMA),
        ArtifactRole.QUARANTINE,
        "quarantine_record",
    )
    outputs: tuple[ArtifactRef, ...] = (data, qa, quarantine)
    preview = None
    if kind is BuildKind.PREVIEW:
        input_sample = lifecycle._write_sample(
            root, f"{prefix}/samples/input.json", [{"exchange_id": 0}]
        )
        output_sample = lifecycle._write_sample(
            root, f"{prefix}/samples/output.json", [{"exchange_id": 0}]
        )
        outputs = (*outputs, input_sample, output_sample)
        preview = PreviewMetadata(
            fixed_case_ids=("current_reference_snapshot",),
            fixed_case_qa_result_ids={"current_reference_snapshot": (checks[0].result_id,)},
            input_sample_path=input_sample.path,
            input_sample_rows=1,
            output_sample_path=output_sample.path,
            output_sample_rows=1,
            examples_truncated=True,
            full_run_inputs=(source,),
            resource_usage={"fixture_bytes": 1},
            full_run_projection={"projection_multiplier": 1.0},
        )
    return BuildManifest(
        intent=intent,
        outputs=outputs,
        row_funnel=RowFunnel(
            input_rows=27,
            accepted_source_rows=27,
            exact_duplicate_excess=0,
            quarantined_source_rows=0,
            unmapped_source_rows=0,
            version_preserved_rows=0,
            output_rows_by_table={EXCHANGE_DIM_CONTRACT.table: 27},
        ),
        qa_checks=checks,
        quarantine_issue_rows=0,
        quarantine_unique_source_rows=0,
        quarantine_issue_ids_by_severity={item.value: () for item in QASeverity},
        started_at=(
            "2026-07-13T04:02:00+00:00"
            if kind is BuildKind.PREVIEW
            else "2026-07-13T04:05:00+00:00"
        ),
        completed_at=(
            "2026-07-13T04:03:00+00:00"
            if kind is BuildKind.PREVIEW
            else "2026-07-13T04:06:00+00:00"
        ),
        preview=preview,
    )


def _publish_exchange(root: Path, git_commit: str) -> tuple[str, str]:
    store = SilverStore(root)
    source = _exchange_source(root, git_commit)
    snapshot = store.create_workflow(
        EXCHANGE_DIM_CONTRACT, actor="synthetic-s1", created_at="2026-07-13T04:00:00+00:00"
    )
    snapshot = store.submit_schema_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="synthetic-s1",
        created_at="2026-07-13T04:01:00+00:00",
    )
    snapshot = store.approve_schema(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="synthetic-reviewer",
        decided_at="2026-07-13T04:02:00+00:00",
    )
    preview = _exchange_build(
        root,
        workflow_id=snapshot.workflow_id,
        git_commit=git_commit,
        source=source,
        kind=BuildKind.PREVIEW,
    )
    snapshot = store.record_preview_build(
        preview,
        expected_event_sha256=snapshot.event_sha256,
        actor="synthetic-s1",
        recorded_at="2026-07-13T04:03:00+00:00",
    )
    snapshot = store.request_preview_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="synthetic-s1",
        created_at="2026-07-13T04:04:00+00:00",
    )
    snapshot = store.approve_full_run(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="synthetic-reviewer",
        decided_at="2026-07-13T04:05:00+00:00",
    )
    full = _exchange_build(
        root,
        workflow_id=snapshot.workflow_id,
        git_commit=git_commit,
        source=source,
        kind=BuildKind.FULL,
        approved_preview_build_id=preview.build_id,
    )
    snapshot = store.record_full_build(
        full,
        expected_event_sha256=snapshot.event_sha256,
        actor="synthetic-s1",
        recorded_at="2026-07-13T04:06:00+00:00",
    )
    snapshot = store.request_publish(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="synthetic-s1",
        created_at="2026-07-13T04:07:00+00:00",
    )
    _, release = store.publish(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="synthetic-reviewer",
        decided_at="2026-07-13T04:08:00+00:00",
    )
    return release.release_id, store.load_release(release.release_id)[1].sha256


def test_paired_lifecycle_publishes_and_replays_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, git_commit = _fixture_repo(tmp_path, monkeypatch)
    root = tmp_path / "data"
    root.mkdir()
    exchange_release_id, exchange_release_sha = _publish_exchange(root, git_commit)
    manifest_sha, artifact_sha, manifest_before, artifact_before = _write_condition_bronze(root)
    monkeypatch.setattr(
        lifecycle,
        "CURRENT_CONDITION_CODE_AUTHORIZATION",
        ConditionCodeAuthorization(
            manifest_sha256=manifest_sha,
            artifact_sha256=artifact_sha,
            exchange_release_id=exchange_release_id,
            exchange_release_manifest_sha256=exchange_release_sha,
        ),
    )

    first = lifecycle.complete_condition_code_lifecycle(root, repo_root=repo, git_commit=git_commit)
    second = lifecycle.complete_condition_code_lifecycle(
        root, repo_root=repo, git_commit=git_commit
    )
    store = SilverStore(root)

    assert first.dim.workflow.state is first.bridge.workflow.state is WorkflowState.PUBLISHED
    assert first.dim.workflow.sequence == first.bridge.workflow.sequence == 9
    assert first.dim.release.release_id == second.dim.release.release_id
    assert first.bridge.release.release_id == second.bridge.release.release_id
    assert len(first.inventory.upstream_manifests) == 2
    assert {item.path for item in first.inventory.upstream_manifests} == {
        CURRENT_CONDITION_CODES_MANIFEST_PATH,
        store.load_release(exchange_release_id)[1].path,
    }
    for run in (first.dim, first.bridge):
        store.verify_source_artifacts(run.full.intent.inputs, run.contract)
        parameters = dict(run.preview.intent.parameters)
        assert parameters["pandas_version"]
        assert parameters["python_version"]
        assert parameters["timezone_key"] == "America/New_York"
        assert len(parameters["timezone_probe_digest"]) == 64
    assert first.dim.full.row_funnel.output_rows_by_table == {"condition_code_dim": 94}
    assert first.bridge.full.row_funnel.output_rows_by_table == {
        "condition_code_data_type_bridge": 123
    }
    assert len(SilverStore(root).workflow_events(first.dim.workflow.workflow_id)) == 9
    assert len(SilverStore(root).workflow_events(first.bridge.workflow.workflow_id)) == 9
    assert all(not item.blocks_publish for item in first.dim.full.qa_checks)
    assert all(not item.blocks_publish for item in first.bridge.full.qa_checks)
    for run in (first.dim, first.bridge):
        for event in store.workflow_events(run.workflow.workflow_id):
            approval_id = event.event.evidence.get("approval_id")
            if event.event.to_state not in {
                WorkflowState.APPROVED_FULL_RUN,
                WorkflowState.PUBLISHED,
            }:
                continue
            assert isinstance(approval_id, str)
            approval, _ = store.load_approval(approval_id)
            assert approval.waived_qa_result_ids == ()
            assert approval.accepted_quarantine_issue_ids == ()
    assert (root / CURRENT_CONDITION_CODES_MANIFEST_PATH).read_bytes() == manifest_before
    assert (root / CURRENT_CONDITION_CODES_ARTIFACT_PATH).read_bytes() == artifact_before
