from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from dataclasses import replace
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_assets_schema_approval as cli
from ame_stocks_api.silver.contracts import ApprovalDecision, ApprovalStage, TableContract
from ame_stocks_api.silver.store import SilverStore, WorkflowState

_ROOT = Path(__file__).resolve().parents[1]


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    module_path = "backend/ame_stocks_api/cli/silver_assets_schema_approval.py"
    paths = {
        module_path,
        *cli._TRACKED_RUNTIME_PATHS,
        *(item.candidate_path for item in cli.CURRENT_AUTHORIZATIONS),
        *(item.resource_path for item in cli.CURRENT_AUTHORIZATIONS),
    }
    for relative in paths:
        destination = repo / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((_ROOT / relative).read_bytes())
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "Tests")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(cli, "__file__", str(repo / module_path))
    return repo, _git(repo, "rev-parse", "HEAD")


def _schema_review(
    tmp_path: Path,
    repo: Path,
) -> tuple[Path, tuple[cli.AssetSchemaApprovalAuthorization, ...]]:
    data_root = tmp_path / "data"
    data_root.mkdir()
    store = SilverStore(data_root)
    authorizations: list[cli.AssetSchemaApprovalAuthorization] = []
    for index, authorization in enumerate(cli.CURRENT_AUTHORIZATIONS):
        contract = TableContract.from_dict(
            json.loads((repo / authorization.candidate_path).read_text(encoding="utf-8"))
        )
        actor = f"fixture-s4-schema-review-{index}"
        planned = store.create_workflow(
            contract,
            actor=actor,
            created_at="2026-07-13T08:30:00+00:00",
        )
        reviewed = store.submit_schema_review(
            planned.workflow_id,
            expected_event_sha256=planned.event_sha256,
            actor=actor,
            created_at="2026-07-13T08:30:00+00:00",
        )
        _, registered = store.load_workflow_contract(reviewed.workflow_id)
        authorizations.append(
            replace(
                authorization,
                workflow_id=reviewed.workflow_id,
                schema_review_event_sha256=reviewed.event_sha256,
                registered_contract_sha256=registered.sha256,
            )
        )
    return data_root, tuple(authorizations)


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
            cli.S4_SCHEMA_DECIDED_AT,
            "--approval-text-sha256",
            cli.APPROVAL_TEXT_SHA256,
        ]
    )


def _file_inventory(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_records_three_exact_approvals_and_stops_at_code_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATIONS", authorizations)

    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["approval_text"] == cli.APPROVAL_TEXT
    assert output["approval_text_sha256"] == cli.APPROVAL_TEXT_SHA256
    assert output["approver"] == cli.APPROVER
    assert output["decided_at"] == cli.S4_SCHEMA_DECIDED_AT
    assert output["git_commit"] == head
    assert output["mode"] == "schema_approval_only"
    assert output["state"] == "code_ready"
    assert set(output["workflows"]) == {item.table for item in authorizations}

    store = SilverStore(data_root)
    for authorization in authorizations:
        item = output["workflows"][authorization.table]
        assert item["workflow_id"] == authorization.workflow_id
        assert item["contract_id"] == authorization.contract_id
        assert item["schema_digest"] == authorization.schema_digest
        assert item["candidate_sha256"] == authorization.candidate_sha256
        assert item["registered_contract_sha256"] == (authorization.registered_contract_sha256)
        assert item["schema_review_event_sha256"] == (authorization.schema_review_event_sha256)
        assert item["state"] == "code_ready"
        assert item["sequence"] == 3
        snapshot = store.verify_workflow_trust_chain(
            authorization.workflow_id,
            verify_artifacts=True,
        )
        assert snapshot.state is WorkflowState.CODE_READY
        assert len(store.workflow_events(authorization.workflow_id)) == 3
        approval, _ = store.load_approval(item["approval_id"])
        assert approval.stage is ApprovalStage.SCHEMA
        assert approval.decision is ApprovalDecision.APPROVED
        assert approval.subject_id == authorization.contract_id
        assert approval.subject_manifest_sha256 == authorization.registered_contract_sha256
        assert approval.expected_event_sha256 == authorization.schema_review_event_sha256
        assert approval.approver == cli.APPROVER
        assert approval.decided_at == cli.S4_SCHEMA_DECIDED_AT
        assert approval.note == cli.APPROVAL_TEXT
        assert approval.waived_qa_result_ids == ()
        assert approval.accepted_quarantine_issue_ids == ()

    assert not (data_root / "manifests/silver/source-inventories").exists()
    assert not (data_root / "manifests/silver/builds").exists()
    assert not (data_root / "manifests/silver/releases").exists()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()


def test_exact_rerun_is_byte_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATIONS", authorizations)

    assert _run(data_root, repo, head) == 0
    first = json.loads(capsys.readouterr().out)
    inventory = _file_inventory(data_root)
    assert _run(data_root, repo, head) == 0
    second = json.loads(capsys.readouterr().out)

    assert second == first
    assert _file_inventory(data_root) == inventory
    assert all(
        len(SilverStore(data_root).workflow_events(item.workflow_id)) == 3
        for item in authorizations
    )


def test_lockstep_preflight_refuses_bad_pin_before_any_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    drifted = (
        authorizations[0],
        replace(authorizations[1], schema_review_event_sha256="0" * 64),
        authorizations[2],
    )

    with pytest.raises(cli.SilverStoreError, match="event is not authorized"):
        cli.record_asset_schema_approvals(
            data_root,
            repo_root=repo,
            approval_text_sha256=cli.APPROVAL_TEXT_SHA256,
            decided_at=cli.S4_SCHEMA_DECIDED_AT,
            authorizations=drifted,
        )

    store = SilverStore(data_root)
    assert all(
        store.status(item.workflow_id).state is WorkflowState.SCHEMA_REVIEW
        for item in authorizations
    )
    assert not (data_root / "manifests/silver/approvals").exists()


@pytest.mark.parametrize(
    ("approval_text_sha256", "decided_at", "message"),
    [
        ("0" * 64, cli.S4_SCHEMA_DECIDED_AT, "approval text digest"),
        (cli.APPROVAL_TEXT_SHA256, "2026-07-13T08:31:01+00:00", "decided_at"),
    ],
)
def test_refuses_wrong_approval_identity_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    approval_text_sha256: str,
    decided_at: str,
    message: str,
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    before = _file_inventory(data_root)

    with pytest.raises(cli.SilverStoreError, match=message):
        cli.record_asset_schema_approvals(
            data_root,
            repo_root=repo,
            approval_text_sha256=approval_text_sha256,
            decided_at=decided_at,
            authorizations=authorizations,
        )

    assert _file_inventory(data_root) == before
    assert not (data_root / "manifests/silver/approvals").exists()


def test_resumes_after_one_exact_member_was_already_approved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    first = authorizations[0]
    store = SilverStore(data_root)
    store.approve_schema(
        first.workflow_id,
        expected_event_sha256=first.schema_review_event_sha256,
        approver=cli.APPROVER,
        decided_at=cli.S4_SCHEMA_DECIDED_AT,
        note=cli.APPROVAL_TEXT,
    )

    result = cli.record_asset_schema_approvals(
        data_root,
        repo_root=repo,
        approval_text_sha256=cli.APPROVAL_TEXT_SHA256,
        decided_at=cli.S4_SCHEMA_DECIDED_AT,
        authorizations=authorizations,
    )

    assert len(result.items) == 3
    assert all(item.workflow.state is WorkflowState.CODE_READY for item in result.items)
    assert all(item.workflow.sequence == 3 for item in result.items)


def test_refuses_stale_or_dirty_git_checkout_before_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root, authorizations = _schema_review(tmp_path, repo)
    monkeypatch.setattr(cli, "CURRENT_AUTHORIZATIONS", authorizations)

    with pytest.raises(SystemExit) as stale:
        _run(data_root, repo, "0" * 40)
    assert stale.value.code == 2
    assert "HEAD differs" in capsys.readouterr().err

    (repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SystemExit) as dirty:
        _run(data_root, repo, head)
    assert dirty.value.code == 2
    assert "not clean" in capsys.readouterr().err
    assert all(
        SilverStore(data_root).status(item.workflow_id).state is WorkflowState.SCHEMA_REVIEW
        for item in authorizations
    )


def test_entry_point_parser_and_fixed_remote_authorizations_are_pinned() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["scripts"]["ame-silver-assets-schema-approval"] == (
        "ame_stocks_api.cli.silver_assets_schema_approval:main"
    )
    assert hashlib.sha256(cli.APPROVAL_TEXT.encode("utf-8")).hexdigest() == (
        cli.APPROVAL_TEXT_SHA256
    )
    assert not cli.APPROVAL_TEXT.endswith("\n")
    assert [item.workflow_id for item in cli.CURRENT_AUTHORIZATIONS] == [
        "c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec",
        "989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da",
        "918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755",
    ]
    assert [item.schema_review_event_sha256 for item in cli.CURRENT_AUTHORIZATIONS] == [
        "84749ab1a7a1cac80b636dbb4be9fb58af8ce22e2b34656044d7f34ed848d5cd",
        "c3ff6ef36cc5533bf6838912ee25aac0d9fa30ffc0bda3fbc0b387e90e027911",
        "57f357d158dd9856d0fda46262dee70308d7b9b30f0ce864954fc62c83703dbb",
    ]
    destinations = {action.dest for action in cli.build_parser()._actions}
    assert destinations == {
        "approval_text_sha256",
        "data_root",
        "decided_at",
        "git_commit",
        "help",
        "repo_root",
    }
