"""Bootstrap and run the exact approved S7 manifest-only preflight."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_MANIFEST_LITERAL_VERSION = "s7_directional_manifest_preflight_approval_literal_v1"
_BOOTSTRAP_REQUIRED_RUNTIME_PATHS = frozenset(
    {
        "pyproject.toml",
        "backend/ame_stocks_api/__init__.py",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/cli/__init__.py",
        "backend/ame_stocks_api/providers/massive.py",
        "backend/ame_stocks_api/silver/__init__.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        "backend/ame_stocks_api/silver/asset_full_run_plan.py",
        "backend/ame_stocks_api/silver/asset_publish_plan.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/asset_source.py",
        "backend/ame_stocks_api/silver/assets.py",
        "backend/ame_stocks_api/silver/calendar_artifact.py",
        "backend/ame_stocks_api/silver/exchange_contract.py",
        "backend/ame_stocks_api/silver/fixed_cases.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/identity_source.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
        "backend/ame_stocks_api/silver/identity_provider_evidence.py",
        "backend/ame_stocks_api/silver/identity_bounce.py",
        "backend/ame_stocks_api/silver/identity_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_streaming_preview.py",
        "backend/ame_stocks_api/silver/availability.py",
        "backend/ame_stocks_api/silver/ticker_event_contract.py",
        "backend/ame_stocks_api/silver/ticker_type_contract.py",
        "backend/ame_stocks_api/silver/ticker_overview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_runner.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_runner.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_run.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_request.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_manifest_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_run.py",
        "backend/ame_stocks_api/silver/schema_resources/"
        "identity_directional_raw_preview_slot.schema-v1.json",
        "docs/silver/contracts/identity/"
        "identity_directional_raw_preview_slot.schema-v1.candidate.json",
        "docs/silver-s7-directional-raw-preview-design.md",
    }
)
_BOOTSTRAP_REQUIRED_VERIFICATION_PATHS = frozenset(
    {
        "tests/test_silver_identity_directional_raw_preview_contract.py",
        "tests/test_silver_identity_directional_raw_preview_plan.py",
        "tests/test_silver_identity_directional_raw_preview_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_plan.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_request.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_approval.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_runner.py",
        "tests/test_silver_identity_directional_raw_preview_manifest_run.py",
        "tests/test_silver_identity_directional_raw_preview_execution_plan.py",
        "tests/test_silver_identity_directional_raw_preview_approval.py",
        "tests/test_silver_identity_directional_raw_preview_runner.py",
        "tests/test_silver_identity_directional_raw_preview_run.py",
        "tests/test_silver_identity_provider_evidence.py",
        "tests/test_silver_identity_source.py",
        "tests/test_silver_identity_market_inventory_engine.py",
        "tests/test_silver_identity_bounce.py",
        "tests/test_silver_identity_preview_plan.py",
        "tests/test_silver_identity_streaming_preview.py",
        "tests/test_silver_asset_contracts.py",
        "tests/test_silver_asset_full_run_plan.py",
        "tests/test_silver_asset_publish_plan.py",
        "tests/test_silver_asset_release_set.py",
        "tests/test_silver_asset_source.py",
        "tests/test_silver_assets.py",
        "tests/test_silver_calendar_artifact.py",
        "tests/test_silver_exchange_contract.py",
        "tests/test_silver_ticker_event_contracts.py",
        "tests/test_silver_ticker_type_contract.py",
        "tests/test_silver_ticker_overview_contract.py",
        "tests/test_massive_provider.py",
        "tests/test_silver_lazy_imports.py",
    }
)


class DirectionalRawPreviewManifestRunBootstrapError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--request-event-id", required=True)
    parser.add_argument("--request-event-sha256", required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--approval-sha256", required=True)
    parser.add_argument("--source-binding-created-at", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner_error: type[Exception] = RuntimeError
    try:
        for label, value in (
            ("plan ID", args.plan_id),
            ("plan SHA", args.plan_sha256),
            ("request ID", args.request_event_id),
            ("request SHA", args.request_event_sha256),
            ("approval ID", args.approval_id),
            ("approval SHA", args.approval_sha256),
        ):
            _require_digest(value, label)
        root = _resolve_cli_root(args.control_root, "control_root")
        data_root = _resolve_cli_root(args.data_root, "data_root")
        repository = _resolve_cli_root(args.repo_root, "repo_root")
        if root != data_root:
            raise DirectionalRawPreviewManifestRunBootstrapError(
                "control_root and data_root must be the same durable root"
            )
        _bootstrap_exact_checkout(
            root,
            repository,
            plan_id=args.plan_id,
            expected_plan_sha256=args.plan_sha256,
            request_event_id=args.request_event_id,
            expected_request_sha256=args.request_event_sha256,
            approval_id=args.approval_id,
            expected_approval_sha256=args.approval_sha256,
        )
        runner, runner_error = _load_runner()
        run = runner(
            control_root=root,
            data_root=data_root,
            repo_root=repository,
            plan_id=args.plan_id,
            plan_sha256=args.plan_sha256,
            request_event_id=args.request_event_id,
            request_event_sha256=args.request_event_sha256,
            approval_id=args.approval_id,
            approval_sha256=args.approval_sha256,
            source_binding_created_at_utc=datetime.fromisoformat(
                args.source_binding_created_at
            ),
        )
    except (DirectionalRawPreviewManifestRunBootstrapError, OSError, ValueError) as exc:
        raise SystemExit(f"manifest run bootstrap: {exc}") from exc
    except runner_error as exc:
        raise SystemExit(f"manifest run: {exc}") from exc
    print(
        json.dumps(
            {
                "all_documents_preexisting": run.all_documents_preexisting,
                "completion": _completion_summary(run),
                "execution_plan": {
                    "path": run.execution_plan_document.path,
                    "plan_id": run.execution_plan.plan_id,
                    "sha256": run.execution_plan_document.sha256,
                },
                "execution_request": {
                    "canonical_approval_literal": run.execution_request.canonical_approval_literal,
                    "path": run.execution_request_document.path,
                    "request_event_id": run.execution_request.request_event_id,
                    "sha256": run.execution_request_document.sha256,
                },
                "manifest_json_read_count": run.json_manifest_reads,
                "parquet_content_read_bytes": 0,
                "parquet_lstat_count": run.parquet_lstats,
                "run_intent": {
                    "intent_id": run.run_intent.intent_id,
                    "path": run.run_intent_document.path,
                    "sha256": run.run_intent_document.sha256,
                },
                "source_binding": {
                    "path": run.source_binding_document.path,
                    "sha256": run.source_binding_document.sha256,
                    "source_binding_id": run.source_binding.source_binding_id,
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _completion_summary(run: Any) -> dict[str, object]:
    return {
        "bytes": run.completion_document.bytes,
        "completion_id": run.completion.completion_id,
        "path": run.completion_document.path,
        "sha256": run.completion_document.sha256,
        "state": "awaiting_review",
    }


def _load_runner() -> tuple[Any, type[Exception]]:
    try:
        from ame_stocks_api.silver.identity_directional_raw_preview_manifest_runner import (
            IdentityDirectionalRawPreviewManifestRunnerError,
            run_s7_directional_raw_preview_manifest_preflight,
        )
    except ImportError as exc:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "verified manifest runner package cannot be imported"
        ) from exc
    return (
        run_s7_directional_raw_preview_manifest_preflight,
        IdentityDirectionalRawPreviewManifestRunnerError,
    )


def _resolve_cli_root(value: Path, label: str) -> Path:
    expanded = value.expanduser()
    if expanded.is_symlink() or not expanded.is_dir():
        raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} is unsafe")
    return expanded.resolve()


def _bootstrap_exact_checkout(
    data_root: Path,
    repository: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    request_event_id: str,
    expected_request_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> None:
    if not data_root.is_dir() or data_root.is_symlink():
        raise DirectionalRawPreviewManifestRunBootstrapError("data_root is unsafe")
    if not repository.is_dir() or repository.is_symlink():
        raise DirectionalRawPreviewManifestRunBootstrapError("repo_root is unsafe")
    plan_relative = (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-plans/"
        f"plan_id={plan_id}/manifest.json"
    )
    document = _read_exact_control(
        data_root,
        plan_relative,
        expected_plan_sha256,
        identity_field="plan_id",
        expected_identity=plan_id,
    )
    if document.get("execution_data_root") != str(data_root):
        raise DirectionalRawPreviewManifestRunBootstrapError("manifest Plan binding differs")
    git_binding = _mapping(document.get("git_binding"), "git binding")
    verification = _mapping(document.get("verification_binding"), "verification binding")
    preparation = _mapping(
        document.get("preparation_authorization"), "preparation authorization"
    )
    selection = _mapping(document.get("selection_semantics"), "selection semantics")
    runtime_pins, verification_pins = _validate_bootstrap_file_sets(
        git_binding, verification
    )
    commit = _require_git_object(git_binding.get("git_commit"), "Git commit")
    tree = _require_git_object(git_binding.get("git_tree"), "Git tree")
    if Path(_git(repository, "rev-parse", "--show-toplevel")).resolve() != repository:
        raise DirectionalRawPreviewManifestRunBootstrapError("repo_root is displaced")
    if _git(repository, "rev-parse", "HEAD") != commit or _git(
        repository, "rev-parse", "HEAD^{tree}"
    ) != tree:
        raise DirectionalRawPreviewManifestRunBootstrapError("Git commit/tree differs")
    if _git(repository, "status", "--porcelain=v1", "--untracked-files=all"):
        raise DirectionalRawPreviewManifestRunBootstrapError("Git checkout is not clean")
    for pin in (*runtime_pins, *verification_pins):
        _verify_pin(repository, commit, pin)

    request_relative = (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )
    request = _read_exact_control(
        data_root,
        request_relative,
        expected_request_sha256,
        identity_field="request_event_id",
        expected_identity=request_event_id,
    )
    plan_path = plan_relative
    projected = {
        "authorized_action": document.get("authorized_action"),
        "execution_data_root": document.get("execution_data_root"),
        "future_execution_plan_actor": document.get("future_execution_plan_actor"),
        "future_execution_request_actor": document.get("future_execution_request_actor"),
        "future_manifest_reader_actor": document.get("future_manifest_reader_actor"),
        "input_binding_digest": document.get("input_binding_digest"),
        "plan_id": plan_id,
        "plan_path": plan_path,
        "plan_sha256": expected_plan_sha256,
        "preparation_authorization_id": preparation.get("authorization_id"),
        "preparation_authorization_sha256": preparation.get("sha256"),
        "resource_caps_digest": _stable_digest(document.get("resource_caps")),
        "runtime_file_set_digest": git_binding.get("runtime_file_set_digest"),
        "selection_semantics_digest": selection.get("digest"),
        "verification_file_set_digest": verification.get(
            "verification_file_set_digest"
        ),
    }
    if any(request.get(key) != value for key, value in projected.items()):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Request does not project the exact Plan"
        )
    plan_actor = document.get("created_by")
    request_actor = request.get("created_by")
    future_actors = {
        document.get("future_manifest_reader_actor"),
        document.get("future_execution_plan_actor"),
        document.get("future_execution_request_actor"),
    }
    if (
        not isinstance(plan_actor, str)
        or not isinstance(request_actor, str)
        or len(future_actors) != 3
        or plan_actor in future_actors
        or request_actor in future_actors | {plan_actor}
        or _parse_bootstrap_utc(request.get("created_at_utc"), "request created_at")
        < _parse_bootstrap_utc(document.get("created_at_utc"), "plan created_at")
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Plan/Request actor or time chain differs"
        )

    approval_relative = (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )
    approval = _read_exact_control(
        data_root,
        approval_relative,
        expected_approval_sha256,
        identity_field="approval_id",
        expected_identity=approval_id,
    )
    approval_projection = {
        "authorized_action": document.get("authorized_action"),
        "input_binding_digest": document.get("input_binding_digest"),
        "plan_id": plan_id,
        "plan_sha256": expected_plan_sha256,
        "request_event_id": request_event_id,
        "request_event_sha256": expected_request_sha256,
        "resource_caps_digest": projected["resource_caps_digest"],
        "runtime_file_set_digest": projected["runtime_file_set_digest"],
        "selection_semantics_digest": projected["selection_semantics_digest"],
        "verification_file_set_digest": projected["verification_file_set_digest"],
    }
    literal = approval.get("approval_literal")
    literal_sha = approval.get("approval_literal_sha256")
    if (
        any(approval.get(key) != value for key, value in approval_projection.items())
        or not isinstance(literal, str)
        or not isinstance(literal_sha, str)
        or hashlib.sha256(literal.encode()).hexdigest() != literal_sha
        or approval.get("approved_by")
        in future_actors | {plan_actor, request_actor}
        or _parse_bootstrap_utc(approval.get("approved_at_utc"), "approval time")
        <= _parse_bootstrap_utc(request.get("created_at_utc"), "request created_at")
        or _parse_bootstrap_utc(approval.get("approved_at_utc"), "approval time")
        > datetime.now(UTC)
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Approval does not project the exact Request and Plan"
        )
    try:
        literal_document = json.loads(literal, object_pairs_hook=_reject_duplicates)
    except json.JSONDecodeError as exc:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Approval literal is not JSON"
        ) from exc
    preparation_lineage = _mapping(
        request.get("preparation_control_lineage"), "preparation control lineage"
    )
    expected_literal = {
        "authorized_action": request.get("authorized_action"),
        "execution_data_root": str(data_root),
        "expected_source_artifact_count": 22,
        "future_execution_plan_actor": document.get("future_execution_plan_actor"),
        "future_execution_request_actor": document.get("future_execution_request_actor"),
        "future_manifest_reader_actor": document.get("future_manifest_reader_actor"),
        "future_output_json_count": 5,
        "input_binding_digest": document.get("input_binding_digest"),
        "literal_version": _MANIFEST_LITERAL_VERSION,
        "plan_id": plan_id,
        "plan_sha256": expected_plan_sha256,
        "preparation_authorization_id": projected["preparation_authorization_id"],
        "preparation_authorization_sha256": projected[
            "preparation_authorization_sha256"
        ],
        "preparation_literal_sha256": preparation_lineage.get(
            "approved_literal_sha256"
        ),
        "preparation_plan_id": preparation_lineage.get("plan_id"),
        "preparation_plan_sha256": preparation_lineage.get("plan_sha256"),
        "preparation_request_event_id": preparation_lineage.get("request_event_id"),
        "preparation_request_event_sha256": preparation_lineage.get(
            "request_event_sha256"
        ),
        "request_event_id": request_event_id,
        "request_event_sha256": expected_request_sha256,
        "resource_caps_digest": projected["resource_caps_digest"],
        "runtime_file_set_digest": projected["runtime_file_set_digest"],
        "scope_set_id": preparation_lineage.get("scope_set_id"),
        "scope_set_sha256": preparation_lineage.get("scope_set_sha256"),
        "selection_semantics_digest": projected["selection_semantics_digest"],
        "verification_file_set_digest": projected["verification_file_set_digest"],
    }
    if (
        not isinstance(literal_document, dict)
        or json.dumps(
            literal_document,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        != literal
        or literal_document != expected_literal
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Approval literal chain differs"
        )


def _validate_bootstrap_file_sets(
    git_binding: dict[str, object], verification: dict[str, object]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    runtime_pins = _pin_array(git_binding.get("runtime_files"), "runtime files")
    verification_pins = _pin_array(
        verification.get("verification_files"), "verification files"
    )
    runtime_paths = {str(item["path"]) for item in runtime_pins}
    verification_paths = {str(item["path"]) for item in verification_pins}
    if (
        len(runtime_paths) != len(runtime_pins)
        or len(verification_paths) != len(verification_pins)
        or runtime_paths & verification_paths
        or not _BOOTSTRAP_REQUIRED_RUNTIME_PATHS.issubset(runtime_paths)
        or not _BOOTSTRAP_REQUIRED_VERIFICATION_PATHS.issubset(verification_paths)
        or git_binding.get("runtime_file_set_digest") != _stable_digest(runtime_pins)
        or verification.get("verification_file_set_digest")
        != _stable_digest(verification_pins)
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            "manifest Plan file-set binding is incomplete"
        )
    return runtime_pins, verification_pins


def _verify_pin(repository: Path, commit: str, pin: dict[str, object]) -> None:
    relative = pin["path"]
    if not isinstance(relative, str):
        raise DirectionalRawPreviewManifestRunBootstrapError("pin path is invalid")
    target = _safe_relative_no_symlink(repository, relative, "pin")
    if (
        not target.is_file()
        or target.is_symlink()
        or target.stat().st_size != pin["bytes"]
        or _sha256_file(target) != pin["sha256"]
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(f"pinned bytes differ: {relative}")
    output = _git(repository, "ls-tree", commit, "--", relative).split()
    if len(output) < 4 or output[1] != "blob" or output[2] != pin["git_blob"]:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"pinned Git blob differs: {relative}"
        )


def _read_exact_control(
    root: Path,
    relative: str,
    expected_sha256: str,
    *,
    identity_field: str,
    expected_identity: str,
) -> dict[str, object]:
    _require_digest(expected_sha256, "control SHA")
    _require_digest(expected_identity, "control identity")
    path = _safe_relative_no_symlink(root, relative, "control")
    if not path.is_file() or _sha256_file(path) != expected_sha256:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"exact {identity_field} control is missing"
        )
    content = path.read_bytes()
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{identity_field} control is not JSON"
        ) from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{identity_field} control is not canonical"
        )
    logical = dict(document)
    identity = logical.pop(identity_field, None)
    if identity != expected_identity or _stable_digest(logical) != expected_identity:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{identity_field} control identity does not reproduce"
        )
    return document


def _safe_relative_no_symlink(root: Path, relative: str, label: str) -> Path:
    candidate = Path(relative)
    if (
        candidate.is_absolute()
        or candidate.as_posix() != relative
        or ".." in candidate.parts
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{label} path is not canonical"
        )
    current = root
    for part in candidate.parts:
        current /= part
        if current.is_symlink():
            raise DirectionalRawPreviewManifestRunBootstrapError(
                f"{label} path traverses a symlink"
            )
    return current


def _stable_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def _parse_bootstrap_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} is not text")
    try:
        instant = datetime.fromisoformat(value)
    except ValueError as exc:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{label} is not ISO datetime"
        ) from exc
    if (
        instant.tzinfo is None
        or instant.utcoffset() is None
        or instant.utcoffset().total_seconds() != 0
        or instant.astimezone(UTC).isoformat() != value
    ):
        raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} is not UTC")
    return instant.astimezone(UTC)


def _pin_array(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} must be an array")
    result = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"bytes", "git_blob", "path", "sha256"}:
            raise DirectionalRawPreviewManifestRunBootstrapError(
                f"{label} pin schema differs"
            )
        if type(item["bytes"]) is not int or item["bytes"] <= 0:
            raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} pin bytes invalid")
        _require_digest(item["sha256"], f"{label} SHA")
        _require_git_object(item["git_blob"], f"{label} blob")
        result.append(item)
    return result


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise DirectionalRawPreviewManifestRunBootstrapError(f"{label} must be an object")
    return value


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{label} must be lowercase 64-hex"
        )
    return value


def _require_git_object(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT.fullmatch(value) is None:
        raise DirectionalRawPreviewManifestRunBootstrapError(
            f"{label} must be lowercase 40-hex"
        )
    return value


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise DirectionalRawPreviewManifestRunBootstrapError("Git verification failed")
    return result.stdout.strip()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise DirectionalRawPreviewManifestRunBootstrapError("duplicate Plan JSON key")
        output[key] = value
    return output


if __name__ == "__main__":
    raise SystemExit(main())
