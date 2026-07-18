from __future__ import annotations

import ast
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.cli import (
    silver_identity_directional_raw_preview_manifest_approval as approval_cli,
)
from ame_stocks_api.cli import silver_identity_directional_raw_preview_manifest_run as run_cli


def test_manifest_clis_expose_no_ticker_date_or_parquet_path_override() -> None:
    approval = {action.dest for action in approval_cli.build_parser()._actions}
    run = {action.dest for action in run_cli.build_parser()._actions}
    assert not {"ticker", "session", "start", "end", "parquet_path"} & approval
    assert not {"ticker", "session", "start", "end", "parquet_path"} & run
    assert {"plan_id", "request_event_id", "approval_literal"} <= approval
    assert {"plan_id", "request_event_id", "approval_id", "data_root"} <= run


def test_completion_cli_summary_includes_exact_receipt_bytes() -> None:
    run = SimpleNamespace(
        completion=SimpleNamespace(completion_id="1" * 64),
        completion_document=SimpleNamespace(
            path="controls/completion.json",
            sha256="2" * 64,
            bytes=321,
        ),
    )
    assert run_cli._completion_summary(run) == {
        "bytes": 321,
        "completion_id": "1" * 64,
        "path": "controls/completion.json",
        "sha256": "2" * 64,
        "state": "awaiting_review",
    }


def _pin(path: str) -> dict[str, object]:
    content = path.encode()
    return {
        "bytes": len(content),
        "git_blob": hashlib.sha1(
            b"blob " + str(len(content)).encode() + b"\0" + content
        ).hexdigest(),
        "path": path,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def test_run_cli_import_boundary_is_stdlib_only_until_bootstrap() -> None:
    tree = ast.parse(Path(run_cli.__file__).read_text(encoding="utf-8"))
    top_level_imports = {
        alias.name
        for node in tree.body
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(name.startswith("ame_stocks_api") for name in top_level_imports)
    loader = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_load_runner"
    )
    assert any(
        isinstance(node, ast.ImportFrom)
        and "identity_directional_raw_preview_manifest_runner" in (node.module or "")
        for node in ast.walk(loader)
    )


def test_bootstrap_file_sets_require_every_transitive_pin_and_exact_digests() -> None:
    runtime = [_pin(path) for path in sorted(run_cli._BOOTSTRAP_REQUIRED_RUNTIME_PATHS)]
    verification = [
        _pin(path) for path in sorted(run_cli._BOOTSTRAP_REQUIRED_VERIFICATION_PATHS)
    ]
    git_binding = {
        "runtime_files": runtime,
        "runtime_file_set_digest": run_cli._stable_digest(runtime),
    }
    verification_binding = {
        "verification_files": verification,
        "verification_file_set_digest": run_cli._stable_digest(verification),
    }
    assert run_cli._validate_bootstrap_file_sets(
        git_binding, verification_binding
    ) == (runtime, verification)

    with pytest.raises(run_cli.DirectionalRawPreviewManifestRunBootstrapError):
        run_cli._validate_bootstrap_file_sets(
            {
                **git_binding,
                "runtime_files": runtime[:-1],
                "runtime_file_set_digest": run_cli._stable_digest(runtime[:-1]),
            },
            verification_binding,
        )
    with pytest.raises(run_cli.DirectionalRawPreviewManifestRunBootstrapError):
        run_cli._validate_bootstrap_file_sets(
            {**git_binding, "runtime_file_set_digest": "0" * 64},
            verification_binding,
        )


def test_exact_control_recomputes_canonical_identity_and_rejects_tamper(tmp_path) -> None:
    logical = {"artifact_type": "fixture", "state": "frozen"}
    identity = run_cli._stable_digest(logical)
    document = {**logical, "plan_id": identity}
    content = run_cli._canonical_bytes(document)
    relative = f"controls/plan_id={identity}/manifest.json"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    checksum = hashlib.sha256(content).hexdigest()
    assert run_cli._read_exact_control(
        tmp_path,
        relative,
        checksum,
        identity_field="plan_id",
        expected_identity=identity,
    ) == document

    tampered = {**document, "state": "changed"}
    path.write_bytes(
        json.dumps(tampered, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    with pytest.raises(run_cli.DirectionalRawPreviewManifestRunBootstrapError):
        run_cli._read_exact_control(
            tmp_path,
            relative,
            hashlib.sha256(path.read_bytes()).hexdigest(),
            identity_field="plan_id",
            expected_identity=identity,
        )


def test_cli_root_rejects_symlink_before_resolve(tmp_path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(run_cli.DirectionalRawPreviewManifestRunBootstrapError):
        run_cli._resolve_cli_root(link, "repo_root")


def test_bootstrap_rejects_wrong_request_or_approval_before_runner_import(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_id, plan_sha = "1" * 64, "2" * 64
    request_id, request_sha = "3" * 64, "4" * 64
    approval_id, approval_sha = "5" * 64, "6" * 64
    created = datetime.now(UTC) - timedelta(minutes=3)
    resource_caps = {"output_json_count": 5}
    resource_digest = run_cli._stable_digest(resource_caps)
    plan = {
        "authorized_action": "manifest-only",
        "created_at_utc": created.isoformat(),
        "created_by": "plan-author",
        "execution_data_root": str(tmp_path),
        "future_execution_plan_actor": "execution-planner",
        "future_execution_request_actor": "execution-requester",
        "future_manifest_reader_actor": "manifest-reader",
        "git_binding": {
            "git_commit": "a" * 40,
            "git_tree": "b" * 40,
            "runtime_file_set_digest": "7" * 64,
        },
        "input_binding_digest": "8" * 64,
        "preparation_authorization": {
            "authorization_id": "9" * 64,
            "sha256": "a" * 64,
        },
        "resource_caps": resource_caps,
        "selection_semantics": {"digest": "b" * 64},
        "verification_binding": {"verification_file_set_digest": "c" * 64},
    }
    preparation_lineage = {
        "approved_literal_sha256": "d" * 64,
        "plan_id": "e" * 64,
        "plan_sha256": "f" * 64,
        "request_event_id": "0" * 64,
        "request_event_sha256": "1" * 64,
        "scope_set_id": "2" * 64,
        "scope_set_sha256": "3" * 64,
    }
    request = {
        "authorized_action": plan["authorized_action"],
        "created_at_utc": (created + timedelta(minutes=1)).isoformat(),
        "created_by": "request-author",
        "execution_data_root": str(tmp_path),
        "future_execution_plan_actor": plan["future_execution_plan_actor"],
        "future_execution_request_actor": plan["future_execution_request_actor"],
        "future_manifest_reader_actor": plan["future_manifest_reader_actor"],
        "input_binding_digest": plan["input_binding_digest"],
        "plan_id": plan_id,
        "plan_path": (
            "manifests/silver/identity/directional-raw-preview-manifest-preflight-plans/"
            f"plan_id={plan_id}/manifest.json"
        ),
        "plan_sha256": plan_sha,
        "preparation_authorization_id": "9" * 64,
        "preparation_authorization_sha256": "a" * 64,
        "preparation_control_lineage": preparation_lineage,
        "resource_caps_digest": resource_digest,
        "runtime_file_set_digest": "7" * 64,
        "selection_semantics_digest": "b" * 64,
        "verification_file_set_digest": "c" * 64,
    }
    literal_document = {
        "authorized_action": request["authorized_action"],
        "execution_data_root": str(tmp_path),
        "expected_source_artifact_count": 22,
        "future_execution_plan_actor": request["future_execution_plan_actor"],
        "future_execution_request_actor": request["future_execution_request_actor"],
        "future_manifest_reader_actor": request["future_manifest_reader_actor"],
        "future_output_json_count": 5,
        "input_binding_digest": request["input_binding_digest"],
        "literal_version": run_cli._MANIFEST_LITERAL_VERSION,
        "plan_id": plan_id,
        "plan_sha256": plan_sha,
        "preparation_authorization_id": request["preparation_authorization_id"],
        "preparation_authorization_sha256": request[
            "preparation_authorization_sha256"
        ],
        "preparation_literal_sha256": preparation_lineage[
            "approved_literal_sha256"
        ],
        "preparation_plan_id": preparation_lineage["plan_id"],
        "preparation_plan_sha256": preparation_lineage["plan_sha256"],
        "preparation_request_event_id": preparation_lineage["request_event_id"],
        "preparation_request_event_sha256": preparation_lineage[
            "request_event_sha256"
        ],
        "request_event_id": request_id,
        "request_event_sha256": request_sha,
        "resource_caps_digest": resource_digest,
        "runtime_file_set_digest": request["runtime_file_set_digest"],
        "scope_set_id": preparation_lineage["scope_set_id"],
        "scope_set_sha256": preparation_lineage["scope_set_sha256"],
        "selection_semantics_digest": request["selection_semantics_digest"],
        "verification_file_set_digest": request["verification_file_set_digest"],
    }
    literal = json.dumps(literal_document, separators=(",", ":"), sort_keys=True)
    approval = {
        "approval_literal": literal,
        "approval_literal_sha256": hashlib.sha256(literal.encode()).hexdigest(),
        "approved_at_utc": (created + timedelta(minutes=2)).isoformat(),
        "approved_by": "approval-author",
        "authorized_action": plan["authorized_action"],
        "input_binding_digest": plan["input_binding_digest"],
        "plan_id": plan_id,
        "plan_sha256": plan_sha,
        "request_event_id": request_id,
        "request_event_sha256": request_sha,
        "resource_caps_digest": resource_digest,
        "runtime_file_set_digest": request["runtime_file_set_digest"],
        "selection_semantics_digest": request["selection_semantics_digest"],
        "verification_file_set_digest": request["verification_file_set_digest"],
    }
    documents = {"plan_id": plan, "request_event_id": request, "approval_id": approval}

    monkeypatch.setattr(
        run_cli,
        "_read_exact_control",
        lambda root, relative, checksum, *, identity_field, expected_identity: documents[
            identity_field
        ],
    )
    monkeypatch.setattr(
        run_cli, "_validate_bootstrap_file_sets", lambda *args: ([], [])
    )
    monkeypatch.setattr(run_cli, "_verify_pin", lambda *args: None)

    def fake_git(root, *args):
        return {
            ("rev-parse", "--show-toplevel"): str(tmp_path),
            ("rev-parse", "HEAD"): "a" * 40,
            ("rev-parse", "HEAD^{tree}"): "b" * 40,
            ("status", "--porcelain=v1", "--untracked-files=all"): "",
        }[args]

    monkeypatch.setattr(run_cli, "_git", fake_git)
    kwargs = dict(
        plan_id=plan_id,
        expected_plan_sha256=plan_sha,
        request_event_id=request_id,
        expected_request_sha256=request_sha,
        approval_id=approval_id,
        expected_approval_sha256=approval_sha,
    )
    run_cli._bootstrap_exact_checkout(tmp_path, tmp_path, **kwargs)

    request["plan_id"] = "f" * 64
    with pytest.raises(
        run_cli.DirectionalRawPreviewManifestRunBootstrapError,
        match="Request",
    ):
        run_cli._bootstrap_exact_checkout(tmp_path, tmp_path, **kwargs)
    request["plan_id"] = plan_id
    approval["request_event_id"] = "f" * 64
    with pytest.raises(
        run_cli.DirectionalRawPreviewManifestRunBootstrapError,
        match="Approval",
    ):
        run_cli._bootstrap_exact_checkout(tmp_path, tmp_path, **kwargs)
