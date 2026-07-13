from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from dataclasses import replace
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_ticker_types_schema_approval as cli
from ame_stocks_api.silver.contracts import ApprovalDecision, ApprovalStage, TableContract
from ame_stocks_api.silver.store import SilverStore, WorkflowState


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    module = repo / "backend/ame_stocks_api/cli/silver_ticker_types_schema_approval.py"
    candidate = repo / cli.CANDIDATE_PATH
    module.parent.mkdir(parents=True)
    candidate.parent.mkdir(parents=True)
    module.write_text("# tracked schema-approval adapter\n", encoding="utf-8")
    source_candidate = Path(__file__).parents[1] / cli.CANDIDATE_PATH
    candidate.write_bytes(source_candidate.read_bytes())
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "Tests")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(cli, "__file__", str(module))
    return repo, _git(repo, "rev-parse", "HEAD")


def _schema_review(
    tmp_path: Path,
    repo: Path,
) -> tuple[Path, cli.TickerTypeSchemaApprovalAuthorization]:
    contract = TableContract.from_dict(
        json.loads((repo / cli.CANDIDATE_PATH).read_text(encoding="utf-8"))
    )
    data_root = tmp_path / "data"
    data_root.mkdir()
    store = SilverStore(data_root)
    planned = store.create_workflow(
        contract,
        actor="fixture-s2-schema-review",
        created_at="2026-07-13T12:00:00+00:00",
    )
    review = store.submit_schema_review(
        planned.workflow_id,
        expected_event_sha256=planned.event_sha256,
        actor="fixture-s2-schema-review",
        created_at="2026-07-13T12:01:00+00:00",
    )
    _, registered = store.load_workflow_contract(review.workflow_id)
    authorization = replace(
        cli.CURRENT_AUTHORIZATION,
        workflow_id=review.workflow_id,
        schema_review_event_sha256=review.event_sha256,
        registered_contract_sha256=registered.sha256,
    )
    return data_root, authorization


def _run(data_root: Path, repo: Path, head: str) -> int:
    return cli.main(
        [
            "--data-root",
            str(data_root),
            "--repo-root",
            str(repo),
            "--git-commit",
            head,
            "--decided-at",
            "2026-07-13T12:02:00+00:00",
            "--approval-text",
            cli.APPROVAL_TEXT,
        ]
    )


def test_records_exact_schema_approval_and_stops_at_code_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorization = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATION", authorization)

    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)

    assert output == {
        "approval_id": output["approval_id"],
        "approval_path": output["approval_path"],
        "approval_sha256": output["approval_sha256"],
        "approval_text": cli.APPROVAL_TEXT,
        "approver": cli.APPROVER,
        "candidate_path": cli.CANDIDATE_PATH,
        "candidate_sha256": authorization.candidate_sha256,
        "contract_id": authorization.contract_id,
        "git_commit": head,
        "mode": "schema_approval_only",
        "registered_contract_path": output["registered_contract_path"],
        "registered_contract_sha256": authorization.registered_contract_sha256,
        "schema_digest": authorization.schema_digest,
        "sequence": 3,
        "state": "code_ready",
        "workflow_event_path": output["workflow_event_path"],
        "workflow_event_sha256": output["workflow_event_sha256"],
        "workflow_id": authorization.workflow_id,
    }
    store = SilverStore(data_root)
    assert store.status(authorization.workflow_id).state is WorkflowState.CODE_READY
    assert len(store.workflow_events(authorization.workflow_id)) == 3
    approval, _ = store.load_approval(output["approval_id"])
    assert approval.stage is ApprovalStage.SCHEMA
    assert approval.decision is ApprovalDecision.APPROVED
    assert approval.subject_id == authorization.contract_id
    assert approval.subject_manifest_sha256 == authorization.registered_contract_sha256
    assert approval.expected_event_sha256 == authorization.schema_review_event_sha256
    assert approval.note == cli.APPROVAL_TEXT
    assert approval.waived_qa_result_ids == ()
    assert approval.accepted_quarantine_issue_ids == ()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/builds").exists()
    assert not (data_root / "manifests/silver/releases").exists()


def test_exact_rerun_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorization = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATION", authorization)

    assert _run(data_root, repo, head) == 0
    first = json.loads(capsys.readouterr().out)
    assert _run(data_root, repo, head) == 0
    second = json.loads(capsys.readouterr().out)

    assert second == first
    assert len(SilverStore(data_root).workflow_events(authorization.workflow_id)) == 3


def test_refuses_wrong_approval_wording_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root, authorization = _schema_review(tmp_path, repo)

    with pytest.raises(cli.SilverStoreError, match="exact user authorization"):
        cli.record_ticker_type_schema_approval(
            data_root,
            repo_root=repo,
            approval_text="批准另一个 contract",
            decided_at="2026-07-13T12:02:00+00:00",
            authorization=authorization,
        )

    assert (
        SilverStore(data_root).status(authorization.workflow_id).state
        is WorkflowState.SCHEMA_REVIEW
    )
    assert not (data_root / "manifests/silver/approvals").exists()
    assert capsys.readouterr().out == ""


def test_refuses_unpinned_review_event_and_non_review_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root, authorization = _schema_review(tmp_path, repo)

    with pytest.raises(cli.SilverStoreError, match="event identity is not authorized"):
        cli.record_ticker_type_schema_approval(
            data_root,
            repo_root=repo,
            approval_text=cli.APPROVAL_TEXT,
            decided_at="2026-07-13T12:02:00+00:00",
            authorization=replace(authorization, schema_review_event_sha256="0" * 64),
        )
    store = SilverStore(data_root)
    store.reject(
        authorization.workflow_id,
        expected_event_sha256=store.status(authorization.workflow_id).event_sha256,
        actor="fixture-reviewer",
        created_at="2026-07-13T12:02:00+00:00",
        reason_code="fixture-rejection",
        note="fixture terminal state",
    )
    with pytest.raises(cli.SilverStoreError, match="refuses workflow state rejected"):
        cli.record_ticker_type_schema_approval(
            data_root,
            repo_root=repo,
            approval_text=cli.APPROVAL_TEXT,
            decided_at="2026-07-13T12:03:00+00:00",
            authorization=authorization,
        )


def test_refuses_dirty_or_stale_git_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorization = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATION", authorization)

    with pytest.raises(SystemExit) as stale:
        _run(data_root, repo, "0" * 40)
    assert stale.value.code == 2
    assert "HEAD differs" in capsys.readouterr().err

    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SystemExit) as dirty:
        _run(data_root, repo, head)
    assert dirty.value.code == 2
    assert "not clean" in capsys.readouterr().err
    assert (
        SilverStore(data_root).status(authorization.workflow_id).state
        is WorkflowState.SCHEMA_REVIEW
    )


def test_cli_entry_point_and_fixed_authorization_are_pinned(tmp_path: Path) -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["scripts"]["ame-silver-ticker-types-schema-approval"] == (
        "ame_stocks_api.cli.silver_ticker_types_schema_approval:main"
    )
    assert (
        cli.TickerTypeSchemaApprovalAuthorization(
            workflow_id="40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97",
            schema_review_event_sha256=(
                "72411cbb8714609eb91b516dc66771e8a9a1019edddf4db5c0f164c00e96d209"
            ),
            contract_id="b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d",
            schema_digest="b402318f8b67120fd0bf71fe1b67f56acba31b2ec70915d9b7e57acba84b1957",
            candidate_sha256=("cd11385be2649e00a7f99938754fe7d58e1fa12f6535786cadcce62c281adbd2"),
            registered_contract_sha256=(
                "e7d45dc2f0fba278fe059e374447a33a3aa7dbe7dcc97a073cb509a46ba4476b"
            ),
        )
        == cli.CURRENT_AUTHORIZATION
    )
    assert hashlib.sha256(Path(cli.CANDIDATE_PATH).read_bytes()).hexdigest() == (
        cli.CURRENT_AUTHORIZATION.candidate_sha256
    )
    contract = TableContract.from_dict(
        json.loads(Path(cli.CANDIDATE_PATH).read_text(encoding="utf-8"))
    )
    data_root = tmp_path / "data"
    data_root.mkdir()
    assert SilverStore(data_root).register_contract(contract).sha256 == (
        cli.CURRENT_AUTHORIZATION.registered_contract_sha256
    )
