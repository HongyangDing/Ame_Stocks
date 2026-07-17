from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_identity_market_inventory_run as cli
from ame_stocks_api.silver.identity_market_inventory_runner import (
    IdentityMarketInventoryRunnerError,
    S7CompositeInventoryExecutionCompletion,
)

DIGEST = "a" * 64


def _completion() -> S7CompositeInventoryExecutionCompletion:
    candidate_id = "b" * 64
    directory = (
        f"manifests/silver/identity/composite-inventory-candidates/candidate_id={candidate_id}"
    )
    return S7CompositeInventoryExecutionCompletion(
        plan_id=DIGEST,
        plan_sha256="c" * 64,
        approval_id="d" * 64,
        approval_sha256="e" * 64,
        request_event_id="f" * 64,
        request_event_sha256="1" * 64,
        input_binding_digest="2" * 64,
        candidate_id=candidate_id,
        candidate_path=f"{directory}/manifest.json",
        candidate_sha256="3" * 64,
        candidate_bytes=10,
        data_path=f"{directory}/data/part-00000.parquet",
        data_sha256="4" * 64,
        data_bytes=20,
        qa_path=f"{directory}/qa/qa.json",
        qa_sha256="5" * 64,
        qa_bytes=30,
        bounded_examples_path=f"{directory}/examples/invalid-figi.json",
        bounded_examples_sha256="6" * 64,
        bounded_examples_bytes=40,
        source_artifact_set_digest="7" * 64,
        source_artifact_count=2,
        source_row_count=11,
        source_bytes=50,
        authority_row_count=6,
        reconciliation_row_count=5,
        inventory_row_count=1,
        session_count=1,
        completed_at_utc=datetime(2026, 7, 17, tzinfo=UTC),
        wall_clock_seconds=1.5,
        peak_rss_bytes=100,
        minimum_disk_free_bytes=200,
        maximum_tmp_bytes=300,
        disk_free_warning_triggered=True,
        output_bytes=400,
    )


def test_main_passes_only_exact_control_ids_and_prints_review_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    expected = _completion()
    seen: dict[str, object] = {}

    def fake_run(data_root: Path, **kwargs: object) -> S7CompositeInventoryExecutionCompletion:
        seen.update({"data_root": data_root, **kwargs})
        return expected

    monkeypatch.setattr(cli, "_bootstrap_exact_checkout", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "_load_runner",
        lambda: (fake_run, IdentityMarketInventoryRunnerError),
    )
    assert (
        cli.main(
            [
                "--data-root",
                str(tmp_path),
                "--plan-id",
                DIGEST,
                "--plan-sha256",
                "c" * 64,
                "--approval-id",
                "d" * 64,
                "--approval-sha256",
                "e" * 64,
            ]
        )
        == 0
    )

    assert seen == {
        "data_root": tmp_path,
        "plan_id": DIGEST,
        "expected_plan_sha256": "c" * 64,
        "approval_id": "d" * 64,
        "expected_approval_sha256": "e" * 64,
    }
    output = json.loads(capsys.readouterr().out)
    assert output["candidate"]["state"] == "awaiting_review"
    assert output["completion"]["completion_id"] == expected.completion_id
    assert output["mode"] == "exact_inventory_execution_to_awaiting_review_only"


def test_main_fails_closed_on_runner_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(*args: object, **kwargs: object) -> S7CompositeInventoryExecutionCompletion:
        raise IdentityMarketInventoryRunnerError("approval differs")

    monkeypatch.setattr(cli, "_bootstrap_exact_checkout", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "_load_runner",
        lambda: (fail, IdentityMarketInventoryRunnerError),
    )
    with pytest.raises(SystemExit) as raised:
        cli.main(
            [
                "--data-root",
                str(tmp_path),
                "--plan-id",
                DIGEST,
                "--plan-sha256",
                "c" * 64,
                "--approval-id",
                "d" * 64,
                "--approval-sha256",
                "e" * 64,
            ]
        )
    assert raised.value.code == 2
    assert "approval differs" in capsys.readouterr().err


def test_stdlib_bootstrap_verifies_plan_tree_and_every_pin_before_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = tmp_path / "repo"
    data_root = tmp_path / "data"
    repository.mkdir()
    data_root.mkdir()
    run_cli = "backend/ame_stocks_api/cli/silver_identity_market_inventory_run.py"
    verification_path = "tests/test_inventory_bootstrap.py"
    files = {
        run_cli: b"bootstrap bytes\n",
        verification_path: b"verification bytes\n",
    }
    pins: dict[str, dict[str, object]] = {}
    for index, (relative, content) in enumerate(sorted(files.items()), start=1):
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        pins[relative] = {
            "bytes": len(content),
            "git_blob": f"{index:x}" * 40,
            "path": relative,
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    runtime = [pins[run_cli]]
    verification = [pins[verification_path]]
    plan_id = "1" * 64
    commit = "a" * 40
    tree = "b" * 40
    plan = {
        "artifact_type": "s7_composite_inventory_execution_plan_v2",
        "execution_data_root": str(data_root),
        "git_binding": {
            "execution_git_commit": commit,
            "execution_git_tree": tree,
            "runtime_file_set_digest": cli._stable_digest(runtime),
            "runtime_files": runtime,
        },
        "plan_id": plan_id,
        "plan_state": "awaiting_exact_execution_approval",
        "verification_binding": {
            "verification_file_set_digest": cli._stable_digest(verification),
            "verification_files": verification,
        },
    }
    content = cli._canonical_bytes(plan)
    plan_path = (
        data_root
        / "manifests/silver/identity/composite-inventory-execution-plans-v2"
        / f"plan_id={plan_id}/manifest.json"
    )
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(content)

    def fake_git(root: Path, *arguments: str) -> str:
        assert root == repository
        if arguments == ("rev-parse", "HEAD"):
            return commit
        if arguments == ("rev-parse", "HEAD^{tree}"):
            return tree
        if arguments == ("status", "--porcelain=v1", "--untracked-files=all"):
            return ""
        if arguments[:3] == ("ls-tree", commit, "--"):
            relative = arguments[3]
            return f"100644 blob {pins[relative]['git_blob']}\t{relative}"
        raise AssertionError(arguments)

    monkeypatch.setattr(cli, "_repository_root", lambda: repository)
    monkeypatch.setattr(cli, "_git", fake_git)
    cli._bootstrap_exact_checkout(
        data_root,
        plan_id=plan_id,
        expected_plan_sha256=hashlib.sha256(content).hexdigest(),
    )

    (repository / run_cli).write_bytes(b"tampered\n")
    with pytest.raises(cli.InventoryRunBootstrapError, match="pinned execution bytes differ"):
        cli._bootstrap_exact_checkout(
            data_root,
            plan_id=plan_id,
            expected_plan_sha256=hashlib.sha256(content).hexdigest(),
        )
