from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_ticker_types_schema_review as cli
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
    module = repo / "backend/ame_stocks_api/cli/silver_ticker_types_schema_review.py"
    candidate = repo / cli.CANDIDATE_PATH
    module.parent.mkdir(parents=True)
    candidate.parent.mkdir(parents=True)
    module.write_text("# tracked schema-review adapter\n", encoding="utf-8")
    source_candidate = (
        Path(__file__).parents[1]
        / "docs/silver/contracts/reference/ticker_type_dim.schema-v1.candidate.json"
    )
    candidate.write_bytes(source_candidate.read_bytes())
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "tests@example.invalid")
    _git(repo, "config", "user.name", "Tests")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(cli, "__file__", str(module))
    return repo, _git(repo, "rev-parse", "HEAD")


def _run(data_root: Path, repo: Path, head: str) -> int:
    return cli.main(
        [
            "--data-root",
            str(data_root),
            "--repo-root",
            str(repo),
            "--git-commit",
            head,
            "--created-at",
            "2026-07-13T12:00:00+00:00",
        ]
    )


def test_registers_exact_candidate_and_stops_at_schema_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()

    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)

    assert output == {
        "candidate_path": cli.CANDIDATE_PATH,
        "candidate_sha256": cli.CANDIDATE_SHA256,
        "contract_id": cli.CONTRACT_ID,
        "domain": "reference",
        "event_path": output["event_path"],
        "event_sha256": output["event_sha256"],
        "git_commit": head,
        "mode": "schema_review_only",
        "schema_digest": cli.SCHEMA_DIGEST,
        "schema_version": 1,
        "sequence": 2,
        "source_datasets": ["ticker_types"],
        "state": "schema_review",
        "table": "ticker_type_dim",
        "workflow_id": output["workflow_id"],
    }
    store = SilverStore(data_root)
    snapshot = store.status(output["workflow_id"])
    contract, _ = store.load_workflow_contract(output["workflow_id"])
    assert snapshot.state is WorkflowState.SCHEMA_REVIEW
    assert len(store.workflow_events(output["workflow_id"])) == 2
    assert contract.contract_id == cli.CONTRACT_ID
    assert contract.source_datasets == ("ticker_types",)
    assert not (data_root / "bronze").exists()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/approvals").exists()
    assert not (data_root / "manifests/silver/builds").exists()
    assert not (data_root / "manifests/silver/releases").exists()


def test_exact_rerun_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()

    assert _run(data_root, repo, head) == 0
    first = json.loads(capsys.readouterr().out)
    assert _run(data_root, repo, head) == 0
    second = json.loads(capsys.readouterr().out)

    assert second == first
    assert len(SilverStore(data_root).workflow_events(first["workflow_id"])) == 2


def test_refuses_stale_or_dirty_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()

    with pytest.raises(SystemExit) as stale:
        _run(data_root, repo, "0" * 40)
    assert stale.value.code == 2
    assert "HEAD differs" in capsys.readouterr().err

    (repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SystemExit) as dirty:
        _run(data_root, repo, head)
    assert dirty.value.code == 2
    assert "not clean" in capsys.readouterr().err
    assert not any(data_root.iterdir())


def test_refuses_clean_commit_with_wrong_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    candidate = repo / cli.CANDIDATE_PATH
    document = json.loads(candidate.read_text(encoding="utf-8"))
    document["description"] = "unauthorized replacement"
    candidate.write_text(json.dumps(document, sort_keys=True) + "\n", encoding="utf-8")
    _git(repo, "add", cli.CANDIDATE_PATH)
    _git(repo, "commit", "-q", "-m", "replace candidate")
    head = _git(repo, "rev-parse", "HEAD")
    data_root = tmp_path / "data"

    with pytest.raises(SystemExit) as wrong:
        _run(data_root, repo, head)

    assert wrong.value.code == 2
    assert "candidate SHA-256 is not authorized" in capsys.readouterr().err
    assert not data_root.exists()


def test_refuses_existing_workflow_that_advanced_beyond_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)
    store = SilverStore(data_root)
    snapshot = store.status(output["workflow_id"])
    store.approve_schema(
        output["workflow_id"],
        expected_event_sha256=snapshot.event_sha256,
        approver="test-reviewer",
        decided_at="2026-07-13T12:01:00+00:00",
    )

    with pytest.raises(SystemExit) as advanced:
        _run(data_root, repo, head)

    assert advanced.value.code == 2
    assert "already advanced beyond schema_review" in capsys.readouterr().err
    assert store.status(output["workflow_id"]).state is WorkflowState.CODE_READY
