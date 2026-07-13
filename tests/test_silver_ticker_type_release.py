from __future__ import annotations

import gzip
import hashlib
import json
import stat
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ame_stocks_api.silver import ticker_type_preview as preview_module
from ame_stocks_api.silver import ticker_type_release as release_module
from ame_stocks_api.silver.contracts import ArtifactRole, BuildKind, QAStatus
from ame_stocks_api.silver.reader import PublishedSilverReader
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


@dataclass(frozen=True, slots=True)
class _PreviewFixture:
    authorization: preview_module.TickerTypePreviewAuthorization
    swap_path: Path


def _write_preview_fixture(root: Path) -> _PreviewFixture:
    request_id = "3" * 64
    rows = [
        {
            "asset_class": "stocks",
            "locale": "us",
            "code": code,
            "description": description,
        }
        for code, description in TICKER_TYPES
    ]
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
    return _PreviewFixture(
        authorization=preview_module.TickerTypePreviewAuthorization(
            manifest_path=manifest_relative,
            manifest_sha256=hashlib.sha256(manifest_content).hexdigest(),
            request_id=request_id,
            artifact_path=artifact_relative,
            artifact_sha256=hashlib.sha256(compressed).hexdigest(),
            expected_rows=24,
        ),
        swap_path=swap_path,
    )


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
    return store, snapshot.workflow_id, snapshot.event_sha256


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


def _run_fixture_preview(
    data_root: Path,
    *,
    workflow_id: str,
    event_sha256: str,
    repo_root: Path,
    git_commit: str,
    fixture: _PreviewFixture,
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
        sample_limit=100,
        authorization=authorization,
    )


def test_production_ticker_type_release_authorization_is_exactly_pinned() -> None:
    assert release_module.S2_COMPLETION_AUTHORIZATION == "继续吧，把S2推进结束"  # noqa: RUF001
    assert release_module._AUTHORIZED_WORKFLOW_ID == (
        "40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97"
    )
    assert release_module._AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256 == (
        "b40d81b23cebfe729186638f3f1e209305e067d44581349d16d5ba9df58b2ecb"
    )
    assert release_module._AUTHORIZED_PREVIEW_BUILD_ID == (
        "38998bc76c2ed04f3d9064e3a019cc953e6f1ed5d6594d9485a4978862f0b90d"
    )
    assert release_module._AUTHORIZED_PREVIEW_MANIFEST_SHA256 == (
        "d7ce6dde58914bebe2afcb064599925c75e8c2333821100608ce01d9ee387f66"
    )
    assert {
        WorkflowState.AWAITING_REVIEW,
        WorkflowState.APPROVED_FULL_RUN,
        WorkflowState.FULL_READY,
        WorkflowState.AWAITING_PUBLISH,
        WorkflowState.PUBLISHED,
    } == release_module._ALLOWED_STATES
    assert "backend/ame_stocks_api/silver/ticker_type_release.py" not in (
        release_module._REVIEWED_LOGIC_CLOSURE
    )
    assert not any("condition_code" in path for path in release_module._REVIEWED_LOGIC_CLOSURE)


@dataclass(frozen=True, slots=True)
class _ReviewedPreview:
    data_root: Path
    store: SilverStore
    fixture: object
    preview: object
    repo_root: Path
    preview_commit: str
    runner_commit: str


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _init_release_git_checkout(root: Path) -> tuple[Path, Path, str]:
    repo = root / "repo"
    for relative in release_module._REVIEWED_LOGIC_CLOSURE:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"# reviewed fixture: {relative}\n", encoding="utf-8")
    source_pyproject = Path(__file__).parents[1] / "pyproject.toml"
    (repo / "pyproject.toml").write_text(
        source_pyproject.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    subprocess.run(("git", "init", "-q", str(repo)), check=True, capture_output=True, text=True)
    _git(repo, "config", "user.email", "release@example.test")
    _git(repo, "config", "user.name", "Release Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "reviewed ticker-type logic")
    preview_module_path = repo / "backend/ame_stocks_api/silver/ticker_type_preview.py"
    return repo, preview_module_path, _git(repo, "rev-parse", "HEAD")


def _advance_release_git_checkout(
    repo: Path,
    *,
    drift_logic: bool = False,
) -> tuple[Path, str]:
    release_path = repo / "backend/ame_stocks_api/silver/ticker_type_release.py"
    release_path.parent.mkdir(parents=True, exist_ok=True)
    release_path.write_text("# orchestration adapter fixture\n", encoding="utf-8")
    if drift_logic:
        changed = repo / "backend/ame_stocks_api/silver/ticker_types.py"
        changed.write_text("# forbidden reviewed ticker-type logic drift\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "add ticker-type release adapter")
    return release_path, _git(repo, "rev-parse", "HEAD")


def _authorize_release_fixture(monkeypatch: pytest.MonkeyPatch, preview) -> None:
    monkeypatch.setattr(release_module, "_AUTHORIZED_WORKFLOW_ID", preview.workflow.workflow_id)
    monkeypatch.setattr(
        release_module,
        "_AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256",
        preview.workflow.event_sha256,
    )
    monkeypatch.setattr(
        release_module,
        "_AUTHORIZED_PREVIEW_BUILD_ID",
        preview.build.build_id,
    )
    monkeypatch.setattr(
        release_module,
        "_AUTHORIZED_PREVIEW_MANIFEST_SHA256",
        preview.build_document.sha256,
    )


def _prepare_reviewed_preview(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    drift_logic: bool = False,
) -> _ReviewedPreview:
    data_root = tmp_path / "data"
    fixture = _write_preview_fixture(data_root)
    repo_root, preview_module_path, preview_commit = _init_release_git_checkout(tmp_path)
    monkeypatch.setattr(preview_module, "__file__", str(preview_module_path))
    store, workflow_id, event_sha256 = _ticker_type_code_ready(data_root)
    _authorize_fixture_workflow(
        monkeypatch,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
    )
    preview = _run_fixture_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=preview_commit,
        fixture=fixture,
    )
    assert preview.workflow.state is WorkflowState.AWAITING_REVIEW
    assert preview.workflow.sequence == 5
    release_path, runner_commit = _advance_release_git_checkout(
        repo_root,
        drift_logic=drift_logic,
    )
    monkeypatch.setattr(release_module, "__file__", str(release_path))
    _authorize_release_fixture(monkeypatch, preview)
    return _ReviewedPreview(
        data_root=data_root,
        store=store,
        fixture=fixture,
        preview=preview,
        repo_root=repo_root,
        preview_commit=preview_commit,
        runner_commit=runner_commit,
    )


def _source_state(data_root: Path) -> dict[str, tuple[str, int, int, int, int]]:
    state: dict[str, tuple[str, int, int, int, int]] = {}
    for relative_root in (
        Path("bronze/massive/ticker_types"),
        Path("manifests/massive/ticker_types"),
    ):
        root = data_root / relative_root
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            details = path.stat()
            relative = path.relative_to(data_root).as_posix()
            state[relative] = (
                hashlib.sha256(path.read_bytes()).hexdigest(),
                details.st_size,
                details.st_mtime_ns,
                stat.S_IMODE(details.st_mode),
                details.st_nlink,
            )
    return state


def _release_arguments(reviewed: _ReviewedPreview) -> dict[str, object]:
    preview = reviewed.preview
    return {
        "workflow_id": preview.workflow.workflow_id,
        "reviewed_preview_build_id": preview.build.build_id,
        "reviewed_preview_manifest_sha256": preview.build_document.sha256,
        "repo_root": reviewed.repo_root,
        "runner_git_commit": reviewed.runner_commit,
        "actor": "s2-ticker-types-release-test-runner",
        "approver": "user-approved-s2-completion",
        "authorization": reviewed.fixture.authorization,
    }


def test_reviewed_ticker_type_preview_publishes_exact_24_row_release_without_s3_or_source_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reviewed = _prepare_reviewed_preview(tmp_path, monkeypatch)
    source_before = _source_state(reviewed.data_root)
    arguments = _release_arguments(reviewed)

    completed = release_module._complete_ticker_type_release_authorized(
        reviewed.data_root,
        expected_event_sha256=reviewed.preview.workflow.event_sha256,
        **arguments,
    )

    assert completed.workflow.state is WorkflowState.PUBLISHED
    assert completed.workflow.sequence == 9
    events = reviewed.store.workflow_events(completed.workflow.workflow_id)
    assert len(events) == 9
    assert [item.event.to_state for item in events] == [
        WorkflowState.PLANNED,
        WorkflowState.SCHEMA_REVIEW,
        WorkflowState.CODE_READY,
        WorkflowState.PREVIEW_READY,
        WorkflowState.AWAITING_REVIEW,
        WorkflowState.APPROVED_FULL_RUN,
        WorkflowState.FULL_READY,
        WorkflowState.AWAITING_PUBLISH,
        WorkflowState.PUBLISHED,
    ]
    assert events[4].event_sha256 == reviewed.preview.workflow.event_sha256

    assert completed.full.intent.kind is BuildKind.FULL
    assert completed.full.intent.git_commit == reviewed.preview_commit
    assert completed.full.intent.parameters["approved_preview_build_id"] == (
        reviewed.preview.build.build_id
    )
    assert completed.full.preview is None
    assert completed.full.row_funnel == reviewed.preview.build.row_funnel
    assert completed.full.row_funnel.input_rows == 24
    assert completed.full.row_funnel.output_rows_by_table == {"ticker_type_dim": 24}
    assert len(completed.full.qa_checks) == 20
    assert all(check.status is QAStatus.PASSED for check in completed.full.qa_checks)
    assert all(check.numerator == 0 for check in completed.full.qa_checks)
    assert completed.full.quarantine_issue_rows == 0
    assert completed.full.quarantine_unique_source_rows == 0
    assert all(
        not issue_ids for issue_ids in completed.full.quarantine_issue_ids_by_severity.values()
    )
    assert len(completed.full.outputs) == 7

    preview_data = next(
        output for output in reviewed.preview.build.outputs if output.role is ArtifactRole.DATA
    )
    full_data = next(
        output for output in completed.full.outputs if output.role is ArtifactRole.DATA
    )
    assert full_data.sha256 == preview_data.sha256
    assert full_data.row_count == preview_data.row_count == 24
    assert full_data.path.startswith("silver/schema=v1/reference/ticker_type_dim/")
    full_table = pq.ParquetFile(reviewed.data_root / full_data.path).read()
    assert full_table.schema == TICKER_TYPE_DIM_CONTRACT.arrow_schema
    assert full_table.num_rows == 24
    assert completed.release.outputs == (full_data,)
    assert completed.published.data_paths == (reviewed.data_root / full_data.path,)
    assert completed.published.release == completed.release
    assert completed.published.contract == TICKER_TYPE_DIM_CONTRACT
    assert completed.published.build == completed.full
    assert PublishedSilverReader(reviewed.data_root).inspect(completed.release.release_id) == (
        completed.published
    )
    assert not (reviewed.data_root / "silver/schema=v1/reference/ticker_type_dim/current").exists()

    provenance_output = next(
        output
        for output in completed.full.outputs
        if output.path.endswith("runtime-provenance.json")
    )
    provenance = json.loads((reviewed.data_root / provenance_output.path).read_text())[0]
    assert provenance["preview_transform_git_commit"] == reviewed.preview_commit
    assert provenance["runner_git_commit"] == reviewed.runner_commit
    assert provenance["full_build_id"] == completed.full.build_id
    assert set(provenance["logic_closure_paths"]) == set(release_module._REVIEWED_LOGIC_CLOSURE)
    assert provenance["completion_authorization"] == release_module.S2_COMPLETION_AUTHORIZATION

    for output in completed.full.outputs:
        details = (reviewed.data_root / output.path).stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1
        assert hashlib.sha256((reviewed.data_root / output.path).read_bytes()).hexdigest() == (
            output.sha256
        )
    for document in (completed.full_document, completed.release_document):
        details = (reviewed.data_root / document.path).stat()
        assert stat.S_IMODE(details.st_mode) == 0o444
        assert details.st_nlink == 1
        assert hashlib.sha256((reviewed.data_root / document.path).read_bytes()).hexdigest() == (
            document.sha256
        )

    approval_events = [
        event
        for event in events
        if event.event.to_state in {WorkflowState.APPROVED_FULL_RUN, WorkflowState.PUBLISHED}
    ]
    assert len(approval_events) == 2
    for event in approval_events:
        approval, _ = reviewed.store.load_approval(str(event.event.evidence["approval_id"]))
        assert approval.approver == "user-approved-s2-completion"
        assert release_module.S2_COMPLETION_AUTHORIZATION in approval.note
        assert approval.waived_qa_result_ids == ()
        assert approval.accepted_quarantine_issue_ids == ()

    assert _source_state(reviewed.data_root) == source_before
    assert reviewed.fixture.swap_path.read_bytes() == b"not-authoritative"
    assert not any(
        "condition_code" in path.relative_to(reviewed.data_root).as_posix()
        for path in reviewed.data_root.rglob("*")
    )

    immutable_state = {
        output.path: (
            output.sha256,
            (reviewed.data_root / output.path).stat().st_mtime_ns,
        )
        for output in completed.full.outputs
    }
    repeated = release_module._complete_ticker_type_release_authorized(
        reviewed.data_root,
        expected_event_sha256=completed.workflow.event_sha256,
        **arguments,
    )
    assert repeated.release.release_id == completed.release.release_id
    assert repeated.workflow.event_sha256 == completed.workflow.event_sha256
    assert len(reviewed.store.workflow_events(completed.workflow.workflow_id)) == 9
    assert immutable_state == {
        output.path: (
            output.sha256,
            (reviewed.data_root / output.path).stat().st_mtime_ns,
        )
        for output in repeated.full.outputs
    }
    assert _source_state(reviewed.data_root) == source_before


def test_ticker_type_release_refuses_reviewed_logic_drift_before_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reviewed = _prepare_reviewed_preview(tmp_path, monkeypatch, drift_logic=True)
    source_before = _source_state(reviewed.data_root)

    with pytest.raises(SilverStoreError, match="logic closure changed"):
        release_module._complete_ticker_type_release_authorized(
            reviewed.data_root,
            expected_event_sha256=reviewed.preview.workflow.event_sha256,
            **_release_arguments(reviewed),
        )

    assert reviewed.store.status(reviewed.preview.workflow.workflow_id).state is (
        WorkflowState.AWAITING_REVIEW
    )
    assert not (reviewed.data_root / "silver").exists()
    assert _source_state(reviewed.data_root) == source_before


def test_ticker_type_release_refuses_source_mutation_before_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reviewed = _prepare_reviewed_preview(tmp_path, monkeypatch)
    artifact = reviewed.data_root / reviewed.fixture.authorization.artifact_path
    artifact.chmod(0o644)
    artifact.write_bytes(artifact.read_bytes() + b"tampered")

    with pytest.raises(SilverStoreError, match=r"(byte count|checksum|integrity|verify)"):
        release_module._complete_ticker_type_release_authorized(
            reviewed.data_root,
            expected_event_sha256=reviewed.preview.workflow.event_sha256,
            **_release_arguments(reviewed),
        )

    assert reviewed.store.status(reviewed.preview.workflow.workflow_id).state is (
        WorkflowState.AWAITING_REVIEW
    )
    assert not (reviewed.data_root / "silver").exists()


@pytest.mark.parametrize(
    ("failing_method", "expected_state"),
    (
        ("record_full_build", WorkflowState.APPROVED_FULL_RUN),
        ("request_publish", WorkflowState.FULL_READY),
        ("publish", WorkflowState.AWAITING_PUBLISH),
    ),
)
def test_ticker_type_release_resumes_from_each_post_review_hard_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_method: str,
    expected_state: WorkflowState,
) -> None:
    reviewed = _prepare_reviewed_preview(tmp_path, monkeypatch)
    arguments = _release_arguments(reviewed)

    def simulated_interruption(*_args, **_kwargs):
        raise RuntimeError("simulated S2 release interruption")

    with monkeypatch.context() as interruption:
        interruption.setattr(SilverStore, failing_method, simulated_interruption)
        with pytest.raises(RuntimeError, match="simulated S2 release interruption"):
            release_module._complete_ticker_type_release_authorized(
                reviewed.data_root,
                expected_event_sha256=reviewed.preview.workflow.event_sha256,
                **arguments,
            )

    stopped = reviewed.store.status(reviewed.preview.workflow.workflow_id)
    assert stopped.state is expected_state
    completed = release_module._complete_ticker_type_release_authorized(
        reviewed.data_root,
        expected_event_sha256=stopped.event_sha256,
        **arguments,
    )
    assert completed.workflow.state is WorkflowState.PUBLISHED
    assert completed.workflow.sequence == 9


def test_ticker_type_release_refuses_pin_or_review_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reviewed = _prepare_reviewed_preview(tmp_path, monkeypatch)
    arguments = _release_arguments(reviewed)
    mutations = (
        ("_AUTHORIZED_WORKFLOW_ID", "f" * 64),
        ("_AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256", "f" * 64),
        ("_AUTHORIZED_PREVIEW_BUILD_ID", "f" * 64),
        ("_AUTHORIZED_PREVIEW_MANIFEST_SHA256", "f" * 64),
    )
    for name, value in mutations:
        with monkeypatch.context() as drift:
            drift.setattr(release_module, name, value)
            with pytest.raises(SilverStoreError):
                release_module._complete_ticker_type_release_authorized(
                    reviewed.data_root,
                    expected_event_sha256=reviewed.preview.workflow.event_sha256,
                    **arguments,
                )
        assert reviewed.store.status(reviewed.preview.workflow.workflow_id).state is (
            WorkflowState.AWAITING_REVIEW
        )
        assert not (reviewed.data_root / "silver").exists()

    wrong_arguments = dict(arguments)
    wrong_arguments["reviewed_preview_manifest_sha256"] = "e" * 64
    with pytest.raises(SilverStoreError, match="preview identity"):
        release_module._complete_ticker_type_release_authorized(
            reviewed.data_root,
            expected_event_sha256=reviewed.preview.workflow.event_sha256,
            **wrong_arguments,
        )
    with pytest.raises(SilverStoreError, match="stale"):
        release_module._complete_ticker_type_release_authorized(
            reviewed.data_root,
            expected_event_sha256="e" * 64,
            **arguments,
        )
    assert reviewed.store.status(reviewed.preview.workflow.workflow_id).state is (
        WorkflowState.AWAITING_REVIEW
    )


def test_ticker_type_release_cli_requires_review_and_provenance_guards() -> None:
    from ame_stocks_api.cli.silver_ticker_types_release import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "--data-root",
                "/tmp/data",
                "--repo-root",
                "/tmp/repo",
                "--workflow-id",
                "a" * 64,
                "--expected-event-sha256",
                "b" * 64,
                "--reviewed-preview-build-id",
                "c" * 64,
                "--runner-git-commit",
                "d" * 40,
                "--actor",
                "runner",
                "--approver",
                "reviewer",
            ]
        )
