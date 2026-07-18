from __future__ import annotations

import ast
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.cli import silver_identity_directional_raw_preview_manifest_request as cli
from ame_stocks_api.silver import identity_directional_raw_preview_manifest_request as module
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    MANIFEST_AUTHORIZED_ACTION,
    MANIFEST_LITERAL_VERSION,
    REQUIRED_MANIFEST_RUNTIME_PATHS,
    REQUIRED_MANIFEST_VERIFICATION_PATHS,
)

RECORDED_AT = datetime(2026, 7, 18, 6, 0, tzinfo=UTC).isoformat()


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "repo"
    for relative in REQUIRED_MANIFEST_RUNTIME_PATHS | REQUIRED_MANIFEST_VERIFICATION_PATHS:
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"fixture {relative}\n", encoding="utf-8")
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "tests@example.invalid")
    _git(root, "config", "user.name", "Tests")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    return root, _git(root, "rev-parse", "HEAD")


def _create(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    repository, head = _repository(tmp_path)
    control_root = tmp_path / "controls"
    control_root.mkdir()
    monkeypatch.setattr(module, "_load_and_verify_exact_preparation_controls", lambda root: None)
    result = module.create_s7_directional_raw_preview_manifest_request(
        control_root,
        repo_root=repository,
        git_commit=head,
        recorded_at=RECORDED_AT,
        preparation_authorization_recorded_by="preparation-receipt-recorder",
        plan_created_by="manifest-plan-author",
        request_created_by="manifest-request-author",
        future_manifest_reader_actor="future-manifest-reader",
        future_execution_plan_actor="future-execution-plan-author",
        future_execution_request_actor="future-execution-request-author",
    )
    return control_root, repository, head, result


def test_request_literal_is_narrow_and_binds_all_control_digests() -> None:
    plan_module = __import__(
        "ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan",
        fromlist=["S7DirectionalRawPreviewManifestPreflightPlan"],
    )
    authorization = plan_module.S7DirectionalRawPreviewPreparationAuthorizationReceipt(
        "receipt-recorder", datetime.fromisoformat(RECORDED_AT)
    )
    receipt = plan_module.StoredDirectionalRawPreviewManifestControl(
        authorization.relative_path, authorization.sha256, len(authorization.content)
    )
    pin = lambda path: plan_module.S7DirectionalRawPreviewManifestFilePin(  # noqa: E731
        path, "a" * 40, "b" * 64, 1
    )
    plan = plan_module.S7DirectionalRawPreviewManifestPreflightPlan.create(
        created_by="plan-author",
        created_at_utc=datetime.fromisoformat(RECORDED_AT),
        future_manifest_reader_actor="future-reader",
        future_execution_plan_actor="future-plan-author",
        future_execution_request_actor="future-request-author",
        git_commit="c" * 40,
        git_tree="d" * 40,
        execution_data_root=plan_module.MANIFEST_EXECUTION_DATA_ROOT,
        runtime_files=tuple(pin(path) for path in REQUIRED_MANIFEST_RUNTIME_PATHS),
        verification_files=tuple(pin(path) for path in REQUIRED_MANIFEST_VERIFICATION_PATHS),
        preparation_authorization=authorization,
        preparation_authorization_receipt=receipt,
    )
    request = module.S7DirectionalRawPreviewManifestPreflightRequest.create(
        plan,
        plan_module.StoredDirectionalRawPreviewManifestControl(
            plan.relative_path, plan.sha256, len(plan.content)
        ),
        created_by="request-author",
        created_at_utc=datetime.fromisoformat(RECORDED_AT),
    )
    literal = json.loads(request.canonical_approval_literal)
    assert literal["authorized_action"] == MANIFEST_AUTHORIZED_ACTION
    assert literal["literal_version"] == MANIFEST_LITERAL_VERSION
    assert literal["expected_source_artifact_count"] == 22
    assert literal["future_output_json_count"] == 5
    assert literal["plan_id"] == plan.plan_id
    assert literal["request_event_sha256"] == request.sha256
    assert set(request.document["authorization_boundary"].values()) == {False, True}
    assert request.document["authorization_boundary"]["parquet_content_read"] is False
    assert (
        module.S7DirectionalRawPreviewManifestPreflightRequest.from_dict(
            json.loads(request.content)
        )
        == request
    )


def test_orchestration_writes_exactly_three_json_controls_idempotently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_root, repository, head, first = _create(tmp_path, monkeypatch)
    first_mtimes = {
        path.relative_to(control_root).as_posix(): path.stat().st_mtime_ns
        for path in control_root.rglob("*")
        if path.is_file()
    }
    second = module.create_s7_directional_raw_preview_manifest_request(
        control_root,
        repo_root=repository,
        git_commit=head,
        recorded_at=RECORDED_AT,
        preparation_authorization_recorded_by="preparation-receipt-recorder",
        plan_created_by="manifest-plan-author",
        request_created_by="manifest-request-author",
        future_manifest_reader_actor="future-manifest-reader",
        future_execution_plan_actor="future-execution-plan-author",
        future_execution_request_actor="future-execution-request-author",
    )
    files = [path for path in control_root.rglob("*") if path.is_file()]
    assert len(files) == 3
    assert first.all_documents_preexisting is False
    assert second.all_documents_preexisting is True
    assert first.request.canonical_approval_literal == second.request.canonical_approval_literal
    assert first_mtimes == {
        path.relative_to(control_root).as_posix(): path.stat().st_mtime_ns for path in files
    }


def test_dirty_checkout_actor_reuse_and_symlink_root_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, head = _repository(tmp_path)
    control_root = tmp_path / "controls"
    control_root.mkdir()
    monkeypatch.setattr(module, "_load_and_verify_exact_preparation_controls", lambda root: None)
    (repository / "dirty.txt").write_text("dirty\n")
    with pytest.raises(module.IdentityDirectionalRawPreviewManifestRequestError, match="dirty"):
        module.create_s7_directional_raw_preview_manifest_request(
            control_root,
            repo_root=repository,
            git_commit=head,
            recorded_at=RECORDED_AT,
            preparation_authorization_recorded_by="same",
            plan_created_by="plan",
            request_created_by="request",
            future_manifest_reader_actor="reader",
            future_execution_plan_actor="future-plan",
            future_execution_request_actor="future-request",
        )
    (repository / "dirty.txt").unlink()
    with pytest.raises(module.IdentityDirectionalRawPreviewManifestRequestError, match="distinct"):
        module.create_s7_directional_raw_preview_manifest_request(
            control_root,
            repo_root=repository,
            git_commit=head,
            recorded_at=RECORDED_AT,
            preparation_authorization_recorded_by="same",
            plan_created_by="same",
            request_created_by="request",
            future_manifest_reader_actor="reader",
            future_execution_plan_actor="future-plan",
            future_execution_request_actor="future-request",
        )
    link = tmp_path / "link"
    link.symlink_to(control_root, target_is_directory=True)
    with pytest.raises(module.IdentityDirectionalRawPreviewManifestRequestError, match="symlink"):
        module.create_s7_directional_raw_preview_manifest_request(
            link,
            repo_root=repository,
            git_commit=head,
            recorded_at=RECORDED_AT,
            preparation_authorization_recorded_by="receipt",
            plan_created_by="plan",
            request_created_by="request",
            future_manifest_reader_actor="reader",
            future_execution_plan_actor="future-plan",
            future_execution_request_actor="future-request",
        )


def test_modules_have_no_data_reader_network_or_preview_execution_imports() -> None:
    paths = (
        Path(module.__file__),
        Path(cli.__file__),
    )
    forbidden = {
        "pyarrow",
        "pandas",
        "requests",
        "socket",
        "ame_stocks_api.silver.identity_source",
        "ame_stocks_api.silver.reader",
        "ame_stocks_api.silver.identity_directional_raw_preview_runner",
    }
    for path in paths:
        tree = ast.parse(path.read_text())
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        assert not imports & forbidden


def test_cli_exposes_only_control_inputs() -> None:
    parser = cli.build_parser()
    destinations = {action.dest for action in parser._actions}
    assert destinations == {
        "help",
        "control_root",
        "repo_root",
        "git_commit",
        "recorded_at",
        "preparation_authorization_recorded_by",
        "plan_created_by",
        "request_created_by",
        "future_manifest_reader_actor",
        "future_execution_plan_actor",
        "future_execution_request_actor",
    }
