from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_identity_directional_raw_preview_request as cli
from ame_stocks_api.silver import identity_directional_raw_preview_request as module
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
)

GIT_COMMIT = "a" * 40
GIT_TREE = "b" * 40
RECORDED_AT = "2026-07-17T00:00:00+00:00"
SCOPE_ACTOR = "s7-directional-scope-author"
PLAN_ACTOR = "s7-directional-plan-author"
REQUEST_ACTOR = "s7-directional-request-author"


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, str, str]:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.email", "test@example.invalid")
    _git(repository, "config", "user.name", "test")
    tracked = repository / "runtime.py"
    tracked.write_text("VALUE = 1\n", encoding="utf-8")
    _git(repository, "add", "runtime.py")
    _git(repository, "commit", "-q", "-m", "fixture")
    return (
        repository,
        _git(repository, "rev-parse", "HEAD"),
        _git(repository, "rev-parse", "HEAD^{tree}"),
    )


def _patch_control_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    repository = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(
        module,
        "verify_exact_clean_checkout",
        lambda repo_root, expected_commit: (repo_root.resolve(), GIT_TREE),
    )

    def fake_pins(repo_root: Path, relative_paths: object) -> tuple[module.RepositoryFilePin, ...]:
        del repo_root
        return tuple(
            module.RepositoryFilePin(
                path=relative,
                git_blob="c" * 40,
                sha256=hashlib.sha256(relative.encode("utf-8")).hexdigest(),
                bytes=max(1, len(relative.encode("utf-8"))),
            )
            for relative in sorted(relative_paths)
        )

    monkeypatch.setattr(module, "pin_tracked_files", fake_pins)
    return repository


def _create(
    control_root: Path,
    repository: Path,
) -> module.S7DirectionalRawPreviewRequestRun:
    return module.create_s7_directional_raw_preview_request(
        control_root,
        repo_root=repository,
        git_commit=GIT_COMMIT,
        recorded_at=RECORDED_AT,
        scope_created_by=SCOPE_ACTOR,
        plan_created_by=PLAN_ACTOR,
        request_created_by=REQUEST_ACTOR,
    )


def _files(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())


def test_contract_candidate_and_resource_replay_exactly() -> None:
    repository = Path(__file__).resolve().parents[1]
    pin = module.verify_directional_raw_preview_contract_bytes(repository)

    assert pin.contract_id == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
    assert pin.schema_digest == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST
    assert pin.candidate_sha256 == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256
    assert pin.resource_sha256 == IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256


def test_git_and_runtime_preflight_pin_exact_clean_head(tmp_path: Path) -> None:
    repository, commit, tree = _repository(tmp_path)

    verified, verified_tree = module.verify_exact_clean_checkout(repository, commit)
    pins = module.pin_tracked_files(repository, ("runtime.py",))

    assert verified == repository.resolve()
    assert verified_tree == tree
    assert len(pins) == 1
    assert pins[0].path == "runtime.py"
    assert pins[0].git_blob == _git(repository, "rev-parse", "HEAD:runtime.py")
    assert pins[0].sha256 == hashlib.sha256(b"VALUE = 1\n").hexdigest()
    assert pins[0].bytes == len(b"VALUE = 1\n")

    (repository / "untracked").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="dirty, displaced, or at the wrong commit",
    ):
        module.verify_exact_clean_checkout(repository, commit)


def test_runtime_pin_rejects_worktree_mutation_and_untracked_path(tmp_path: Path) -> None:
    repository, _, _ = _repository(tmp_path)
    (repository / "runtime.py").write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="differs from HEAD",
    ):
        module.pin_tracked_files(repository, ("runtime.py",))

    (repository / "untracked.py").write_text("VALUE = 3\n", encoding="utf-8")
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="not tracked at HEAD",
    ):
        module.pin_tracked_files(repository, ("untracked.py",))

    for unsafe in ("../runtime.py", "/runtime.py", "nested/../runtime.py"):
        with pytest.raises(
            module.IdentityDirectionalRawPreviewRequestError,
            match="tracked path is unsafe",
        ):
            module.pin_tracked_files(repository, (unsafe,))


def test_git_preflight_rejects_repo_root_symlink_before_resolution(tmp_path: Path) -> None:
    repository, commit, _ = _repository(tmp_path)
    linked = tmp_path / "repo-link"
    linked.symlink_to(repository, target_is_directory=True)

    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="repo_root is unsafe",
    ):
        module.verify_exact_clean_checkout(linked, commit)


def test_destination_preflight_is_idempotent_and_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "controls"
    root.mkdir()
    relative = "scope/id/manifest.json"
    content = b"{}\n"
    checksum = hashlib.sha256(content).hexdigest()

    assert module.preflight_destination(root, relative, checksum) is False
    target = root / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(content)
    assert module.preflight_destination(root, relative, checksum) is True

    target.write_bytes(b"tampered\n")
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="control conflicts",
    ):
        module.preflight_destination(root, relative, checksum)

    target.unlink()
    target.symlink_to(root / "elsewhere")
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="control conflicts",
    ):
        module.preflight_destination(root, relative, checksum)


def test_orchestration_writes_exactly_three_immutable_json_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _patch_control_environment(monkeypatch)
    control_root = tmp_path / "controls"
    control_root.mkdir()

    first = _create(control_root, repository)
    first_mtimes = {
        path.relative_to(control_root).as_posix(): path.stat().st_mtime_ns
        for path in control_root.rglob("*")
        if path.is_file()
    }
    second = _create(control_root, repository)
    second_mtimes = {
        path.relative_to(control_root).as_posix(): path.stat().st_mtime_ns
        for path in control_root.rglob("*")
        if path.is_file()
    }

    assert first.all_documents_preexisting is False
    assert second.all_documents_preexisting is True
    assert first.scope == second.scope
    assert first.scope_document == second.scope_document
    assert first.plan == second.plan
    assert first.plan_document == second.plan_document
    assert first.request == second.request
    assert first.request_document == second.request_document
    assert first_mtimes == second_mtimes
    assert _files(control_root) == sorted(
        (
            first.scope_document.path,
            first.plan_document.path,
            first.request_document.path,
        )
    )
    assert first.scope.created_by == SCOPE_ACTOR
    assert first.plan.created_by == PLAN_ACTOR
    assert first.request.created_by == REQUEST_ACTOR
    assert first.git_tree == GIT_TREE
    assert first.plan.plan_state == "awaiting_non_execution_control_review"
    assert first.request.request_state == "awaiting_literal_human_approval"
    assert set(first.request.logical_payload()["authorization_flags"].values()) == {False}
    assert first.request.canonical_approval_literal.startswith("{")
    assert '"preview_execution":false' in first.request.content.decode("utf-8")
    literal = json.loads(first.request.canonical_approval_literal)
    assert literal["preparation_design_digest"] == first.plan.preparation_design_digest
    assert literal["registry_semantics_digest"] == first.request.registry_semantics_digest
    assert literal["request_event_sha256"] == first.request.sha256


def test_all_destination_preflight_finishes_before_first_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _patch_control_environment(monkeypatch)
    control_root = tmp_path / "controls"
    control_root.mkdir()
    calls = 0

    def fail_on_request(root: Path, relative: str, expected_sha256: str) -> bool:
        nonlocal calls
        del root, relative, expected_sha256
        calls += 1
        if calls == 3:
            raise module.IdentityDirectionalRawPreviewRequestError("request conflict")
        return False

    monkeypatch.setattr(module, "preflight_destination", fail_on_request)
    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="request conflict",
    ):
        _create(control_root, repository)

    assert calls == 3
    assert _files(control_root) == []


def test_actor_reuse_and_symlink_control_root_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _patch_control_environment(monkeypatch)
    control_root = tmp_path / "controls"
    control_root.mkdir()

    with pytest.raises(
        module.IdentityDirectionalRawPreviewRequestError,
        match="actors must be distinct",
    ):
        module.create_s7_directional_raw_preview_request(
            control_root,
            repo_root=repository,
            git_commit=GIT_COMMIT,
            recorded_at=RECORDED_AT,
            scope_created_by=SCOPE_ACTOR,
            plan_created_by=SCOPE_ACTOR,
            request_created_by=REQUEST_ACTOR,
        )
    assert _files(control_root) == []

    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)
    with pytest.raises(Exception, match="control root cannot be a symlink"):
        _create(linked, repository)
    assert _files(actual) == []


def test_request_and_cli_ast_have_no_data_network_runner_or_approval_imports() -> None:
    forbidden_roots = {
        "boto3",
        "httpx",
        "pandas",
        "polars",
        "pyarrow",
        "requests",
        "socket",
    }
    forbidden_module_fragments = (
        "approval",
        "identity_preview_runner",
        "materialization",
        "provider_client",
        "registry_release",
    )
    for path in (Path(module.__file__), Path(cli.__file__)):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.append(node.module)
        assert {name.split(".", 1)[0] for name in imported_modules}.isdisjoint(forbidden_roots)
        assert not any(
            fragment in imported
            for imported in imported_modules
            for fragment in forbidden_module_fragments
        )
        assert "read_parquet" not in source
        assert "write_parquet" not in source
        assert "store_approval(" not in source


def test_cli_parser_exposes_only_control_plane_inputs() -> None:
    parser = cli.build_parser()
    destinations = {action.dest for action in parser._actions}

    assert destinations == {
        "control_root",
        "git_commit",
        "help",
        "plan_created_by",
        "recorded_at",
        "repo_root",
        "request_created_by",
        "scope_created_by",
    }
    assert not destinations.intersection(
        {
            "approval_id",
            "execute",
            "registry",
            "source_data_root",
        }
    )
