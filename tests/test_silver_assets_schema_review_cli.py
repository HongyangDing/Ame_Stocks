from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from dataclasses import replace
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_assets_schema_review as cli
from ame_stocks_api.silver.store import SilverStore, WorkflowState

_ROOT = Path(__file__).resolve().parents[1]
_CREATED_AT = cli.S4_SCHEMA_CREATED_AT


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    paths = {
        "backend/ame_stocks_api/cli/silver_assets_schema_review.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        *(spec.candidate_path for spec in cli.ASSET_SCHEMA_REVIEW_SPECS),
        *(spec.resource_path for spec in cli.ASSET_SCHEMA_REVIEW_SPECS),
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
    monkeypatch.setattr(
        cli,
        "__file__",
        str(repo / "backend/ame_stocks_api/cli/silver_assets_schema_review.py"),
    )
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
            _CREATED_AT,
        ]
    )


def _file_inventory(root: Path) -> dict[str, str]:
    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_registers_three_exact_contracts_and_stops_at_schema_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()

    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["created_at"] == _CREATED_AT
    assert output["git_commit"] == head
    assert output["mode"] == "schema_review_only"
    assert output["state"] == "schema_review"
    assert set(output["workflows"]) == {
        "asset_observation_daily",
        "asset_observation_version",
        "universe_source_daily",
    }

    store = SilverStore(data_root)
    contracts = cli._load_fixed_contracts(repo)
    for spec, contract in zip(cli.ASSET_SCHEMA_REVIEW_SPECS, contracts, strict=True):
        item = output["workflows"][spec.table]
        assert item == {
            "candidate_path": spec.candidate_path,
            "candidate_sha256": spec.candidate_sha256,
            "contract_id": spec.contract_id,
            "domain": spec.domain,
            "event_path": item["event_path"],
            "event_sha256": item["event_sha256"],
            "resource_path": spec.resource_path,
            "schema_digest": spec.schema_digest,
            "schema_version": 1,
            "sequence": 2,
            "source_datasets": ["assets"],
            "state": "schema_review",
            "workflow_id": item["workflow_id"],
        }
        snapshot = store.verify_workflow_trust_chain(
            item["workflow_id"],
            verify_artifacts=True,
        )
        events = store.workflow_events(item["workflow_id"])
        registered, _ = store.load_workflow_contract(item["workflow_id"])
        assert snapshot.state is WorkflowState.SCHEMA_REVIEW
        assert snapshot.sequence == 2
        assert registered == contract
        assert tuple(event.event.to_state for event in events) == (
            WorkflowState.PLANNED,
            WorkflowState.SCHEMA_REVIEW,
        )

    assert not (data_root / "bronze").exists()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()
    assert not (data_root / "manifests/silver/approvals").exists()
    assert not (data_root / "manifests/silver/source-inventories").exists()
    assert not (data_root / "manifests/silver/builds").exists()
    assert not (data_root / "manifests/silver/releases").exists()


def test_exact_rerun_is_idempotent_for_all_three_workflows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()

    assert _run(data_root, repo, head) == 0
    first = json.loads(capsys.readouterr().out)
    inventory = _file_inventory(data_root)
    assert _run(data_root, repo, head) == 0
    second = json.loads(capsys.readouterr().out)

    assert second == first
    assert _file_inventory(data_root) == inventory
    for item in second["workflows"].values():
        assert len(SilverStore(data_root).workflow_events(item["workflow_id"])) == 2


def test_recovers_a_preexisting_planned_member_without_advancing_past_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    contracts = cli._load_fixed_contracts(repo)
    first_spec = cli.ASSET_SCHEMA_REVIEW_SPECS[0]
    planned = SilverStore(data_root).create_workflow(
        contracts[0],
        actor=first_spec.actor,
        created_at=_CREATED_AT,
        note=cli.NOTE,
    )
    assert planned.state is WorkflowState.PLANNED

    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["workflows"][first_spec.table]["workflow_id"] == planned.workflow_id
    assert all(item["state"] == "schema_review" for item in output["workflows"].values())
    assert all(item["sequence"] == 2 for item in output["workflows"].values())


def test_preflight_refuses_an_advanced_member_before_any_new_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    assert _run(data_root, repo, head) == 0
    output = json.loads(capsys.readouterr().out)
    first = output["workflows"]["asset_observation_daily"]
    store = SilverStore(data_root)
    store.approve_schema(
        first["workflow_id"],
        expected_event_sha256=first["event_sha256"],
        approver="fixture-reviewer",
        decided_at="2026-07-13T13:01:00+00:00",
        note="fixture-only approval",
    )
    before = _file_inventory(data_root)

    with pytest.raises(SystemExit) as stopped:
        _run(data_root, repo, head)

    assert stopped.value.code == 2
    assert "already advanced beyond schema_review" in capsys.readouterr().err
    assert _file_inventory(data_root) == before


def test_preflight_refuses_a_conflicting_contract_before_creating_workflows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"
    data_root.mkdir()
    contracts = cli._load_fixed_contracts(repo)
    conflicting = replace(
        contracts[-1],
        description=f"{contracts[-1].description} Conflicting fixture.",
    )
    SilverStore(data_root).register_contract(conflicting)

    with pytest.raises(cli.SilverStoreError, match="different contract"):
        cli.register_asset_schema_reviews(
            data_root,
            contracts=contracts,
            created_at=_CREATED_AT,
        )

    assert not (data_root / "manifests/silver/workflows").exists()
    assert not (data_root / "staging").exists()
    assert not (data_root / "silver").exists()


@pytest.mark.parametrize("target", ["candidate", "resource"])
def test_refuses_candidate_or_packaged_resource_drift_without_data_root_mutation(
    target: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, _ = _repo(tmp_path, monkeypatch)
    spec = cli.ASSET_SCHEMA_REVIEW_SPECS[0]
    path = repo / (spec.candidate_path if target == "candidate" else spec.resource_path)
    path.write_bytes(path.read_bytes() + b" \n")
    _git(repo, "add", path.relative_to(repo).as_posix())
    _git(repo, "commit", "-q", "-m", f"drift {target}")
    head = _git(repo, "rev-parse", "HEAD")
    data_root = tmp_path / "data"

    with pytest.raises(SystemExit) as stopped:
        _run(data_root, repo, head)

    assert stopped.value.code == 2
    error = capsys.readouterr().err
    if target == "candidate":
        assert "candidate SHA-256 is not authorized" in error
    else:
        assert "resource differs from its candidate" in error
    assert not data_root.exists()


def test_refuses_stale_or_dirty_git_checkout_before_registry_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"

    with pytest.raises(SystemExit) as stale:
        _run(data_root, repo, "0" * 40)
    assert stale.value.code == 2
    assert "HEAD differs" in capsys.readouterr().err

    (repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SystemExit) as dirty:
        _run(data_root, repo, head)
    assert dirty.value.code == 2
    assert "not clean" in capsys.readouterr().err
    assert not data_root.exists()


def test_refuses_a_noncanonical_created_at_without_registry_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo, head = _repo(tmp_path, monkeypatch)
    data_root = tmp_path / "data"

    with pytest.raises(SystemExit) as stopped:
        cli.main(
            [
                "--data-root",
                str(data_root),
                "--repo-root",
                str(repo),
                "--git-commit",
                head,
                "--created-at",
                "2026-07-13T08:30:01+00:00",
            ]
        )

    assert stopped.value.code == 2
    assert "created_at is not authorized" in capsys.readouterr().err
    assert not data_root.exists()


def test_entry_point_and_all_fixed_candidate_identities_are_pinned() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["scripts"]["ame-silver-assets-schema-review"] == (
        "ame_stocks_api.cli.silver_assets_schema_review:main"
    )
    assert {
        spec.table: (
            spec.candidate_sha256,
            spec.contract_id,
            spec.schema_digest,
            spec.domain,
        )
        for spec in cli.ASSET_SCHEMA_REVIEW_SPECS
    } == {
        "asset_observation_daily": (
            "dbe656df1cd0e007498b2f7c3a79c6654a52d8ffa7f4099a1b8f32546ab3eced",
            "dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045",
            "402d0ea624dc26e43ea63974572ede5a46ae20e0741e97a3d01d07075a71bc1e",
            "identity",
        ),
        "asset_observation_version": (
            "c3249b8684347e5b491cbe31d44c19f6ce0ddec4568a61c831baebafe3433751",
            "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc",
            "4c797ca373d697078b2061b9a76696dc036a1d2db0a5f8e1fe3ce2dac4b6bb4b",
            "identity",
        ),
        "universe_source_daily": (
            "49fb584c6109eee6088aaf291773089caa171d02a31d3c159aa474885abd6d2a",
            "9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58",
            "78b799cd5a2621b5a78e4ed8c23c090f6aea686fcd786366e5c258e81ad278a5",
            "reference",
        ),
    }


def test_parser_exposes_no_approval_or_data_execution_option() -> None:
    destinations = {action.dest for action in cli.build_parser()._actions}
    assert destinations == {
        "created_at",
        "data_root",
        "git_commit",
        "help",
        "repo_root",
    }
