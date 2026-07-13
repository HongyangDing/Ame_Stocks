from __future__ import annotations

import gzip
import hashlib
import json
import stat
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ame_stocks_api.silver import ticker_type_preview as preview_module
from ame_stocks_api.silver.contracts import ArtifactRole, BuildKind, QAStatus
from ame_stocks_api.silver.store import SilverStore, SilverStoreError, WorkflowState
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT

CAPTURE_AT = datetime(2026, 7, 11, 15, 37, 40, 425142, tzinfo=UTC)
TICKER_TYPES = (
    ("CS", "Common Stock"),
    ("PFD", "Preferred Stock"),
    ("WARRANT", "Warrant"),
    ("RIGHT", "Rights"),
    ("BOND", "Corporate Bond"),
    ("ETF", "Exchange Traded Fund"),
    ("ETN", "Exchange Traded Note"),
    ("ETV", "Exchange Traded Vehicle"),
    ("SP", "Structured Product"),
    ("ADRC", "American Depository Receipt Common"),
    ("ADRP", "American Depository Receipt Preferred"),
    ("ADRW", "American Depository Receipt Warrants"),
    ("ADRR", "American Depository Receipt Rights"),
    ("FUND", "Fund"),
    ("BASKET", "Basket"),
    ("UNIT", "Unit"),
    ("LT", "Liquidating Trust"),
    ("OS", "Ordinary Shares"),
    ("GDR", "Global Depository Receipts"),
    ("OTHER", "Other Security Type"),
    ("NYRS", "New York Registry Shares"),
    ("AGEN", "Agency Bond"),
    ("EQLK", "Equity Linked Bond"),
    ("ETS", "Single-security ETF"),
)


def test_production_preview_authorization_is_exactly_pinned() -> None:
    assert preview_module._AUTHORIZED_WORKFLOW_ID == (
        "40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97"
    )
    assert preview_module._AUTHORIZED_CODE_READY_EVENT_SHA256 == (
        "da1b31856cde3d459762c1f563cca8256396a1aa53522f40d45ddbf7ceedc3ad"
    )
    assert TICKER_TYPE_DIM_CONTRACT.contract_id == preview_module._AUTHORIZED_CONTRACT_ID
    assert preview_module.DEFAULT_SAMPLE_LIMIT == 100
    authorization = preview_module.CURRENT_TICKER_TYPES_PREVIEW_AUTHORIZATION
    assert authorization.expected_rows == 24
    assert authorization.manifest_path == (
        "manifests/massive/ticker_types/"
        "b1e581dac57b064039555580a56d6179b8ecf3a3d00dce7e2ade8cf8abc6dea6.json"
    )
    assert authorization.manifest_sha256 == (
        "14e997a8ffd89ee5061bdf6d8c63db1974a9e257b2bb8c3b42d2f08bb3952825"
    )
    assert authorization.artifact_sha256 == (
        "b074aea89befa8bc6795bbd10c34d86448e32b7dec39708a2d4a9983b26e6af6"
    )


@dataclass(frozen=True, slots=True)
class _PreviewFixture:
    authorization: preview_module.TickerTypePreviewAuthorization
    swap_path: Path


def _write_preview_fixture(root: Path, *, row_count: int = 24) -> _PreviewFixture:
    request_id = "3" * 64
    rows = [
        {
            "asset_class": "stocks",
            "locale": "us",
            "code": code,
            "description": description,
        }
        for code, description in TICKER_TYPES[:row_count]
    ]
    if row_count > len(rows):
        rows.extend(
            {
                "asset_class": "stocks",
                "locale": "us",
                "code": f"SYN{index:02d}",
                "description": f"Synthetic type {index:02d}",
            }
            for index in range(len(rows), row_count)
        )
    response = {
        "count": len(rows),
        "request_id": "provider-preview-request",
        "results": rows,
        "status": "OK",
    }
    raw = json.dumps(response, separators=(",", ":"), sort_keys=True).encode()
    compressed = gzip.compress(raw, mtime=0)
    artifact_relative = f"bronze/massive/ticker_types/request_id={request_id}/page-00000.json.gz"
    artifact_path = root / artifact_relative
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(compressed)
    swap_path = artifact_path.with_name(".page-00000.json.gz.swp")
    swap_path.write_bytes(b"not-authoritative")
    manifest = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": artifact_relative,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.isoformat(),
        "dataset": "ticker_types",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "ticker_types",
            "end": "2026-07-09",
            "parameters": {},
            "start": "2026-07-09",
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.isoformat(),
    }
    manifest_relative = f"manifests/massive/ticker_types/{request_id}.json"
    manifest_content = json.dumps(manifest, sort_keys=True).encode()
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(manifest_content)
    authorization = preview_module.TickerTypePreviewAuthorization(
        manifest_path=manifest_relative,
        manifest_sha256=hashlib.sha256(manifest_content).hexdigest(),
        request_id=request_id,
        artifact_path=artifact_relative,
        artifact_sha256=hashlib.sha256(compressed).hexdigest(),
        expected_rows=24,
    )
    return _PreviewFixture(authorization=authorization, swap_path=swap_path)


def _init_preview_git_checkout(root: Path) -> tuple[Path, Path, str]:
    repo = root / "repo"
    module_path = repo / "backend/ame_stocks_api/silver/ticker_type_preview.py"
    module_path.parent.mkdir(parents=True)
    module_path.write_text("# synthetic provenance marker\n", encoding="utf-8")
    commands = (
        ("git", "init", "-q", str(repo)),
        ("git", "-C", str(repo), "config", "user.email", "preview@example.test"),
        ("git", "-C", str(repo), "config", "user.name", "Preview Test"),
        ("git", "-C", str(repo), "add", "."),
        ("git", "-C", str(repo), "commit", "-q", "-m", "preview fixture"),
    )
    for command in commands:
        subprocess.run(command, check=True, capture_output=True, text=True)
    head = subprocess.run(
        ("git", "-C", str(repo), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return repo, module_path, head


def _ticker_type_code_ready(root: Path) -> tuple[SilverStore, str, str]:
    store = SilverStore(root)
    snapshot = store.create_workflow(
        TICKER_TYPE_DIM_CONTRACT,
        actor="preview-test-author",
        created_at="2026-07-13T01:00:00+00:00",
    )
    snapshot = store.submit_schema_review(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        actor="preview-test-author",
        created_at="2026-07-13T01:01:00+00:00",
    )
    snapshot = store.approve_schema(
        snapshot.workflow_id,
        expected_event_sha256=snapshot.event_sha256,
        approver="preview-test-reviewer",
        decided_at="2026-07-13T01:02:00+00:00",
    )
    assert snapshot.state is WorkflowState.CODE_READY
    return store, snapshot.workflow_id, snapshot.event_sha256


def _run_fixture_preview(
    data_root: Path,
    *,
    workflow_id: str,
    event_sha256: str,
    repo_root: Path,
    git_commit: str,
    fixture: _PreviewFixture,
    sample_limit: int = 100,
):
    authorization = fixture.authorization
    return preview_module._run_ticker_type_preview_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=event_sha256,
        manifest_paths=(authorization.manifest_path,),
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        git_commit=git_commit,
        repo_root=repo_root,
        actor="preview-test-runner",
        calendar_name="XNYS",
        sample_limit=sample_limit,
        authorization=authorization,
    )


def _authorize_fixture_workflow(
    monkeypatch: pytest.MonkeyPatch,
    *,
    workflow_id: str,
    event_sha256: str,
) -> None:
    monkeypatch.setattr(preview_module, "_AUTHORIZED_WORKFLOW_ID", workflow_id)
    monkeypatch.setattr(
        preview_module,
        "_AUTHORIZED_CODE_READY_EVENT_SHA256",
        event_sha256,
    )


def test_bounded_ticker_type_preview_writes_reviewable_24_row_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(preview_module, "__file__", str(module_path))
    store, workflow_id, event_sha256 = _ticker_type_code_ready(data_root)
    _authorize_fixture_workflow(
        monkeypatch,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
    )

    run = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert run.workflow.state is WorkflowState.AWAITING_REVIEW
    assert run.workflow.sequence == 5
    assert run.build.intent.kind is BuildKind.PREVIEW
    assert run.build.intent.git_commit == git_commit
    assert (
        run.inventory.inventory_id == run.build.preview.full_run_projection["source_inventory_id"]
    )
    assert run.build.row_funnel.to_dict() == {
        "accepted_source_rows": 24,
        "exact_duplicate_excess": 0,
        "input_rows": 24,
        "output_rows_by_table": {"ticker_type_dim": 24},
        "quarantined_source_rows": 0,
        "unmapped_source_rows": 0,
        "version_preserved_rows": 0,
    }
    assert len(run.build.qa_checks) == 20
    assert all(check.status is QAStatus.PASSED for check in run.build.qa_checks)
    assert all(check.numerator == 0 for check in run.build.qa_checks)
    assert run.build.quarantine_issue_rows == run.build.quarantine_unique_source_rows == 0
    assert len(run.build.outputs) == 6

    outputs_by_role: dict[ArtifactRole, list] = {}
    for output in run.build.outputs:
        outputs_by_role.setdefault(output.role, []).append(output)
        path = data_root / output.path
        details = path.stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1
        assert details.st_size == output.bytes
        assert hashlib.sha256(path.read_bytes()).hexdigest() == output.sha256
    assert {role: len(items) for role, items in outputs_by_role.items()} == {
        ArtifactRole.DATA: 1,
        ArtifactRole.QA: 1,
        ArtifactRole.QUARANTINE: 1,
        ArtifactRole.SAMPLE: 3,
    }
    registered_documents = (
        run.build_document,
        run.inventory_document,
    )
    for document in registered_documents:
        path = data_root / document.path
        details = path.stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1
        assert details.st_size == document.bytes
        assert hashlib.sha256(path.read_bytes()).hexdigest() == document.sha256
    workflow_event_path = data_root / run.workflow.event_path
    assert stat.S_IMODE(workflow_event_path.stat().st_mode) == 0o444
    assert workflow_event_path.stat().st_nlink == 1
    assert hashlib.sha256(workflow_event_path.read_bytes()).hexdigest() == (
        run.workflow.event_sha256
    )

    data_output = outputs_by_role[ArtifactRole.DATA][0]
    data_table = pq.ParquetFile(data_root / data_output.path).read()
    assert data_table.schema == TICKER_TYPE_DIM_CONTRACT.arrow_schema
    assert data_table.num_rows == 24
    assert len(data_table.column_names) == 17
    assert data_table.column("type_code").to_pylist() == sorted(code for code, _ in TICKER_TYPES)
    qa_table = pq.ParquetFile(data_root / outputs_by_role[ArtifactRole.QA][0].path).read()
    assert qa_table.num_rows == 20
    assert set(qa_table.column("status").to_pylist()) == {"passed"}
    quarantine_table = pq.ParquetFile(
        data_root / outputs_by_role[ArtifactRole.QUARANTINE][0].path
    ).read()
    assert quarantine_table.num_rows == 0

    preview = run.build.preview
    assert preview is not None
    assert preview.input_sample_rows == preview.output_sample_rows == 24
    assert preview.examples_truncated is False
    input_rows = json.loads((data_root / preview.input_sample_path).read_text())
    output_rows = json.loads((data_root / preview.output_sample_path).read_text())
    assert len(input_rows) == len(output_rows) == 24
    fixed_output = next(
        item for item in outputs_by_role[ArtifactRole.SAMPLE] if "current-reference" in item.path
    )
    fixed_rows = json.loads((data_root / fixed_output.path).read_text())
    assert [item["assertion_id"] for item in fixed_rows] == [
        "capture_date_comes_from_capture_instant",
        "availability_strictly_after_capture",
        "later_capture_appends_without_backfill",
        "lineage_and_snapshot_qa_recomputable",
        "no_false_temporal_drift_for_identical_adjacent_snapshots",
    ]
    assert all(item["passed"] is True for item in fixed_rows)
    fixed_checks = {
        check.check_id: check
        for check in run.build.qa_checks
        if check.check_id in preview_module._FIXED_CASE_QA_CHECKS
    }
    assert set(fixed_checks) == set(preview_module._FIXED_CASE_QA_CHECKS)
    assert len(fixed_checks) == 7
    assert {check.bounded_examples_path for check in fixed_checks.values()} == {fixed_output.path}
    assert set(preview.fixed_case_qa_result_ids["current_reference_snapshot"]) == {
        check.result_id for check in fixed_checks.values()
    }

    store.verify_build(run.build, TICKER_TYPE_DIM_CONTRACT)
    assert fixture.swap_path.read_bytes() == b"not-authoritative"
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/releases").exists()
    assert not (data_root / "manifests/silver/approvals/full_run").exists()


def test_ticker_type_preview_is_exactly_idempotent_at_awaiting_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(preview_module, "__file__", str(module_path))
    store, workflow_id, event_sha256 = _ticker_type_code_ready(data_root)
    _authorize_fixture_workflow(
        monkeypatch,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
    )
    first = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )
    file_state = {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in first.build.outputs
    }
    event_count = len(store.workflow_events(workflow_id))

    repeated = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=first.workflow.event_sha256,
        repo_root=repo_root,
        git_commit=git_commit,
        fixture=fixture,
    )

    assert repeated.build.build_id == first.build.build_id
    assert repeated.workflow.event_sha256 == first.workflow.event_sha256
    assert len(store.workflow_events(workflow_id)) == event_count
    assert file_state == {
        item.path: (item.sha256, (data_root / item.path).stat().st_mtime_ns)
        for item in repeated.build.outputs
    }
    with pytest.raises(SilverStoreError, match="stale"):
        _run_fixture_preview(
            data_root,
            workflow_id=workflow_id,
            event_sha256=event_sha256,
            repo_root=repo_root,
            git_commit=git_commit,
            fixture=fixture,
        )
    assert len(store.workflow_events(workflow_id)) == event_count


def test_ticker_type_preview_rejects_scope_row_calendar_and_git_drift_before_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root, row_count=23)
    repo_root, module_path, git_commit = _init_preview_git_checkout(tmp_path)
    monkeypatch.setattr(preview_module, "__file__", str(module_path))
    _, workflow_id, event_sha256 = _ticker_type_code_ready(data_root)
    _authorize_fixture_workflow(
        monkeypatch,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
    )
    authorization = fixture.authorization
    common = {
        "workflow_id": workflow_id,
        "expected_event_sha256": event_sha256,
        "expected_manifest_sha256": authorization.manifest_sha256,
        "expected_input_rows": authorization.expected_rows,
        "git_commit": git_commit,
        "repo_root": repo_root,
        "actor": "preview-test-runner",
        "sample_limit": 100,
        "authorization": authorization,
    }

    wrong_workflow = dict(common)
    wrong_workflow["workflow_id"] = "f" * 64
    with pytest.raises(SilverStoreError, match="workflow ID is not authorized"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_workflow,
        )
    wrong_event = dict(common)
    wrong_event["expected_event_sha256"] = "f" * 64
    with pytest.raises(SilverStoreError, match="stale"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_event,
        )
    with pytest.raises(SilverStoreError, match="sample_limit is pinned to 100"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **(common | {"sample_limit": 23}),
        )
    with monkeypatch.context() as wrong_contract:
        wrong_contract.setattr(preview_module, "_AUTHORIZED_CONTRACT_ID", "f" * 64)
        with pytest.raises(SilverStoreError, match="contract identity is not authorized"):
            preview_module._run_ticker_type_preview_authorized(
                data_root,
                manifest_paths=(authorization.manifest_path,),
                calendar_name="XNYS",
                **common,
            )
    with pytest.raises(SilverStoreError, match="one exact manifest"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path, authorization.manifest_path),
            calendar_name="XNYS",
            **common,
        )
    wrong_manifest_sha = dict(common)
    wrong_manifest_sha["expected_manifest_sha256"] = "f" * 64
    with pytest.raises(SilverStoreError, match="manifest SHA is not authorized"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_manifest_sha,
        )
    wrong_expected_rows = dict(common)
    wrong_expected_rows["expected_input_rows"] = 23
    with pytest.raises(SilverStoreError, match="row count is not authorized"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_expected_rows,
        )
    wrong_artifact_sha = dict(common)
    wrong_artifact_sha["authorization"] = replace(
        authorization,
        artifact_sha256="f" * 64,
    )
    with pytest.raises(SilverStoreError, match="source page is not the authorized object"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_artifact_sha,
        )
    with pytest.raises(SilverStoreError, match="pinned to XNYS"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XHKG",
            **common,
        )
    with pytest.raises(SilverStoreError, match="source page is not the authorized object"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **common,
        )
    assert not (data_root / "staging").exists()
    assert not (data_root / "manifests/silver/source-inventories").exists()

    wrong_head = dict(common)
    wrong_head["git_commit"] = "f" * 40
    with pytest.raises(SilverStoreError, match="HEAD differs"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **wrong_head,
        )
    (repo_root / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SilverStoreError, match="not clean"):
        preview_module._run_ticker_type_preview_authorized(
            data_root,
            manifest_paths=(authorization.manifest_path,),
            calendar_name="XNYS",
            **common,
        )
    assert not (data_root / "staging").exists()


def test_ticker_type_preview_cli_requires_all_fail_closed_guards() -> None:
    from ame_stocks_api.cli.silver_ticker_types_preview import build_parser

    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--workflow-id",
                "a" * 64,
                "--expected-event-sha256",
                "b" * 64,
                "--manifest",
                "manifests/massive/ticker_types/example.json",
                "--git-commit",
                "c" * 40,
            ]
        )
