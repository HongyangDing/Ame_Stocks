"""Run one exact, separately approved S7 directional raw preview."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_AUTHORIZED_ACTION = "execute_exact_s7_directional_raw_preview_once_to_awaiting_review"
_EXECUTION_SCOPE = (
    "exact_11_pair_22_artifact_directional_raw_preview_candidate_once_to_"
    "awaiting_review_no_registry_no_adjudication_no_full_no_publish"
)
_APPROVAL_RULE_VERSION = "s7_directional_raw_preview_execution_approval_v1"
_REQUIRED_RUNTIME_PATHS = frozenset(
    {
        "backend/ame_stocks_api/cli/silver_identity_directional_raw_preview_run.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_runner.py",
        "backend/ame_stocks_api/silver/identity_provider_evidence.py",
        "backend/ame_stocks_api/silver/identity_source.py",
    }
)
_REQUIRED_VERIFICATION_PATHS = frozenset(
    {
        "tests/test_silver_identity_directional_raw_preview_run.py",
        "tests/test_silver_identity_directional_raw_preview_runner.py",
    }
)


class DirectionalRawPreviewRunBootstrapError(RuntimeError):
    """Raised before importing the executable package when its Git bytes drift."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute one exact S7 directional raw preview to awaiting_review."
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--approval-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner_error: type[Exception] | None = None
    try:
        for label, value in (
            ("plan ID", args.plan_id),
            ("plan SHA", args.plan_sha256),
            ("approval ID", args.approval_id),
            ("approval SHA", args.approval_sha256),
        ):
            _require_digest(value, label)
        root = args.data_root.expanduser().resolve()
        _bootstrap_exact_checkout(
            root,
            plan_id=args.plan_id,
            expected_plan_sha256=args.plan_sha256,
            approval_id=args.approval_id,
            expected_approval_sha256=args.approval_sha256,
        )
        runner, runner_error = _load_runner()
        completion = runner(
            root,
            plan_id=args.plan_id,
            expected_plan_sha256=args.plan_sha256,
            approval_id=args.approval_id,
            expected_approval_sha256=args.approval_sha256,
        )
    except (DirectionalRawPreviewRunBootstrapError, ImportError, OSError) as exc:
        raise SystemExit(f"directional raw-preview bootstrap failed: {exc}") from exc
    except Exception as exc:
        if runner_error is not None and isinstance(exc, runner_error):
            raise SystemExit(f"directional raw-preview execution failed: {exc}") from exc
        raise
    print(
        json.dumps(
            {
                "candidate_id": completion.candidate_id,
                "completion_id": completion.completion_id,
                "completion_path": (
                    "manifests/silver/identity/"
                    "directional-raw-preview-execution-completions/"
                    f"plan_id={completion.plan_id}/approval_id={completion.approval_id}/"
                    "manifest.json"
                ),
                "state": completion.completion_state,
            },
            sort_keys=True,
        )
    )
    return 0


def _load_runner() -> tuple[Any, type[Exception]]:
    from ame_stocks_api.silver.identity_directional_raw_preview_runner import (
        IdentityDirectionalRawPreviewRunnerError,
        run_exact_s7_directional_raw_preview,
    )

    return run_exact_s7_directional_raw_preview, IdentityDirectionalRawPreviewRunnerError


def _bootstrap_exact_checkout(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> None:
    if not data_root.is_dir() or data_root.is_symlink():
        raise DirectionalRawPreviewRunBootstrapError("data_root is unsafe")
    plan_path = data_root / (
        "manifests/silver/identity/directional-raw-preview-execution-plans/"
        f"plan_id={plan_id}/manifest.json"
    )
    if (
        not plan_path.is_file()
        or plan_path.is_symlink()
        or _sha256_file(plan_path) != expected_plan_sha256
    ):
        raise DirectionalRawPreviewRunBootstrapError("exact execution Plan is missing")
    try:
        content = plan_path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectionalRawPreviewRunBootstrapError("execution Plan is not JSON") from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise DirectionalRawPreviewRunBootstrapError("execution Plan is not canonical")
    logical_plan = dict(document)
    embedded_plan_id = logical_plan.pop("plan_id", None)
    if (
        embedded_plan_id != plan_id
        or _stable_digest(logical_plan) != plan_id
        or document.get("execution_data_root") != str(data_root)
        or document.get("artifact_type")
        != "s7_directional_raw_preview_execution_plan"
        or document.get("plan_state") != "awaiting_exact_execution_approval"
        or document.get("execution_scope") != _EXECUTION_SCOPE
    ):
        raise DirectionalRawPreviewRunBootstrapError("execution Plan binding differs")
    plan_capabilities = _mapping(document.get("capabilities"), "Plan capabilities")
    source_binding = _mapping(document.get("source_binding"), "source binding")
    scope_binding = _mapping(document.get("scope_binding"), "scope binding")
    if (
        not plan_capabilities
        or any(value is not False for value in plan_capabilities.values())
        or source_binding.get("artifact_count") != 22
        or scope_binding.get("pair_count") != 11
    ):
        raise DirectionalRawPreviewRunBootstrapError(
            "execution Plan capability or exact scope differs"
        )
    git_binding = _mapping(document.get("git_binding"), "git binding")
    verification = _mapping(document.get("verification_binding"), "verification binding")
    commit = _require_git_object(git_binding.get("execution_git_commit"), "Git commit")
    tree = _require_git_object(git_binding.get("execution_git_tree"), "Git tree")
    repo = _repository_root()
    if _git(repo, "rev-parse", "HEAD") != commit or _git(
        repo, "rev-parse", "HEAD^{tree}"
    ) != tree:
        raise DirectionalRawPreviewRunBootstrapError("Git commit/tree differs")
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise DirectionalRawPreviewRunBootstrapError("Git checkout is not clean")
    runtime_pins = _pin_array(git_binding.get("runtime_files"), "runtime files")
    verification_pins = _pin_array(
        verification.get("verification_files"), "verification files"
    )
    _verify_pin_set_bindings(
        runtime_pins,
        verification_pins,
        runtime_digest=git_binding.get("runtime_file_set_digest"),
        verification_digest=verification.get("verification_file_set_digest"),
    )
    pins = runtime_pins + verification_pins
    if not pins or len({item["path"] for item in pins}) != len(pins):
        raise DirectionalRawPreviewRunBootstrapError("file pins are incomplete")
    for pin in pins:
        _verify_pin(repo, commit, pin)
    approval_path = data_root / (
        "manifests/silver/identity/directional-raw-preview-execution-approvals/"
        f"approval_id={approval_id}/manifest.json"
    )
    approval = _read_canonical_document(
        approval_path,
        expected_approval_sha256,
        "execution Approval",
    )
    _verify_execution_approval(
        approval,
        approval_id=approval_id,
        plan=document,
        plan_id=plan_id,
        plan_sha256=expected_plan_sha256,
        data_root=data_root,
    )


def _verify_execution_approval(
    approval: dict[str, object],
    *,
    approval_id: str,
    plan: dict[str, object],
    plan_id: str,
    plan_sha256: str,
    data_root: Path,
) -> None:
    request_id = _require_digest(
        approval.get("request_event_id"), "approval request event ID"
    )
    expected_request_path = (
        "manifests/silver/identity/directional-raw-preview-execution-requests/"
        f"request_event_id={request_id}/manifest.json"
    )
    expected_plan_path = (
        "manifests/silver/identity/directional-raw-preview-execution-plans/"
        f"plan_id={plan_id}/manifest.json"
    )
    calculated_approval_id = _stable_digest(
        {
            "approval_literal_sha256": approval.get("approval_literal_sha256"),
            "approval_rule_version": _APPROVAL_RULE_VERSION,
            "authorized_action": approval.get("authorized_action"),
            "execution_scope": approval.get("execution_scope"),
            "plan_id": approval.get("plan_id"),
            "plan_sha256": approval.get("plan_sha256"),
            "request_event_id": request_id,
            "request_event_sha256": approval.get("request_event_sha256"),
        }
    )
    literal = approval.get("approval_literal")
    if not isinstance(literal, str):
        raise DirectionalRawPreviewRunBootstrapError("approval literal is invalid")
    source = _mapping(plan.get("source_binding"), "Plan source binding")
    inventory = _mapping(plan.get("inventory_binding"), "Plan inventory binding")
    scope = _mapping(plan.get("scope_binding"), "Plan scope binding")
    contract = _mapping(plan.get("contract_binding"), "Plan contract binding")
    algorithm = _mapping(plan.get("algorithm"), "Plan algorithm")
    qa = _mapping(plan.get("qa"), "Plan QA")
    git = _mapping(plan.get("git_binding"), "Plan Git binding")
    verification = _mapping(plan.get("verification_binding"), "Plan verification binding")
    expected_bindings = {
        "algorithm_digest": algorithm.get("digest"),
        "contract_candidate_sha256": contract.get("candidate_sha256"),
        "contract_id": contract.get("contract_id"),
        "contract_schema_digest": contract.get("schema_digest"),
        "execution_data_root": str(data_root),
        "input_binding_digest": plan.get("input_binding_digest"),
        "inventory_completion_id": inventory.get("completion_id"),
        "inventory_completion_sha256": inventory.get("completion_manifest_sha256"),
        "manifest_preflight_intent_id": source.get("manifest_preflight_intent_id"),
        "manifest_preflight_intent_path": source.get("manifest_preflight_intent_path"),
        "manifest_preflight_intent_sha256": source.get("manifest_preflight_intent_sha256"),
        "plan_id": plan_id,
        "plan_path": expected_plan_path,
        "plan_sha256": plan_sha256,
        "qa_semantics_digest": qa.get("semantics_digest"),
        "registry_semantics_digest": plan.get("registry_semantics_digest"),
        "resource_caps_digest": _stable_digest(plan.get("resource_caps")),
        "runtime_file_set_digest": git.get("runtime_file_set_digest"),
        "scope_set_id": scope.get("scope_set_id"),
        "scope_set_sha256": scope.get("scope_set_sha256"),
        "source_artifact_set_digest": source.get("source_artifact_set_digest"),
        "source_binding_manifest_id": source.get("manifest_id"),
        "source_binding_manifest_sha256": source.get("manifest_sha256"),
        "verification_file_set_digest": verification.get(
            "verification_file_set_digest"
        ),
    }
    try:
        literal_document = json.loads(literal)
    except json.JSONDecodeError as exc:
        raise DirectionalRawPreviewRunBootstrapError(
            "approval literal is not JSON"
        ) from exc
    expected_literal = {
        "algorithm_digest": expected_bindings["algorithm_digest"],
        "authorized_action": _AUTHORIZED_ACTION,
        "contract_candidate_sha256": expected_bindings[
            "contract_candidate_sha256"
        ],
        "contract_id": expected_bindings["contract_id"],
        "contract_schema_digest": expected_bindings["contract_schema_digest"],
        "execution_data_root": str(data_root),
        "input_binding_digest": expected_bindings["input_binding_digest"],
        "inventory_completion_id": expected_bindings["inventory_completion_id"],
        "inventory_completion_sha256": expected_bindings[
            "inventory_completion_sha256"
        ],
        "literal_version": "s7_directional_raw_preview_execution_approval_literal_v1",
        "manifest_preflight_intent_id": expected_bindings[
            "manifest_preflight_intent_id"
        ],
        "manifest_preflight_intent_path": expected_bindings[
            "manifest_preflight_intent_path"
        ],
        "manifest_preflight_intent_sha256": expected_bindings[
            "manifest_preflight_intent_sha256"
        ],
        "plan_id": plan_id,
        "plan_sha256": plan_sha256,
        "preparation_approval_literal_sha256": approval.get(
            "preparation_approval_literal_sha256"
        ),
        "qa_semantics_digest": expected_bindings["qa_semantics_digest"],
        "registry_semantics_digest": expected_bindings[
            "registry_semantics_digest"
        ],
        "request_event_id": request_id,
        "request_event_sha256": approval.get("request_event_sha256"),
        "resource_caps_digest": expected_bindings["resource_caps_digest"],
        "runtime_file_set_digest": expected_bindings["runtime_file_set_digest"],
        "scope_set_id": expected_bindings["scope_set_id"],
        "scope_set_sha256": expected_bindings["scope_set_sha256"],
        "source_artifact_set_digest": expected_bindings[
            "source_artifact_set_digest"
        ],
        "source_binding_manifest_id": expected_bindings[
            "source_binding_manifest_id"
        ],
        "source_binding_manifest_sha256": expected_bindings[
            "source_binding_manifest_sha256"
        ],
        "verification_file_set_digest": expected_bindings[
            "verification_file_set_digest"
        ],
    }
    required_true = {
        "data_read_authorized",
        "once_to_awaiting_review",
        "parquet_read_authorized",
        "preview_execution_authorized",
    }
    required_false = {
        "adjudication_authorized",
        "caller_scope_override_authorized",
        "exact_group_history_read_authorized",
        "external_evidence_capture_authorized",
        "forced_liquidation_authorized",
        "full_run_authorized",
        "network_access_authorized",
        "publication_authorized",
        "registry_evaluation_authorized",
        "research_table_materialization_authorized",
        "source_discovery_authorized",
    }
    if (
        approval.get("approval_id") != approval_id
        or calculated_approval_id != approval_id
        or approval.get("artifact_type")
        != "s7_directional_raw_preview_execution_approval"
        or approval.get("approval_rule_version") != _APPROVAL_RULE_VERSION
        or approval.get("approval_stage")
        != "s7_directional_raw_preview_exact_bounded_preview"
        or approval.get("decision") != "approved"
        or approval.get("authorized_action") != _AUTHORIZED_ACTION
        or approval.get("execution_scope") != _EXECUTION_SCOPE
        or approval.get("request_event_path") != expected_request_path
        or literal_document != expected_literal
        or json.dumps(
            literal_document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        != literal
        or any(approval.get(key) != value for key, value in expected_bindings.items())
        or any(approval.get(key) is not True for key in required_true)
        or any(approval.get(key) is not False for key in required_false)
        or hashlib.sha256(literal.encode("utf-8")).hexdigest()
        != approval.get("approval_literal_sha256")
    ):
        raise DirectionalRawPreviewRunBootstrapError(
            "execution Approval binding or capability differs"
        )


def _verify_pin_set_bindings(
    runtime_pins: list[dict[str, object]],
    verification_pins: list[dict[str, object]],
    *,
    runtime_digest: object,
    verification_digest: object,
) -> None:
    if (
        _stable_digest(runtime_pins) != runtime_digest
        or _stable_digest(verification_pins) != verification_digest
        or not _REQUIRED_RUNTIME_PATHS.issubset(
            {str(item["path"]) for item in runtime_pins}
        )
        or not _REQUIRED_VERIFICATION_PATHS.issubset(
            {str(item["path"]) for item in verification_pins}
        )
    ):
        raise DirectionalRawPreviewRunBootstrapError(
            "pinned file-set digest or required paths differ"
        )


def _read_canonical_document(
    path: Path, expected_sha256: str, label: str
) -> dict[str, object]:
    if (
        not path.is_file()
        or path.is_symlink()
        or _sha256_file(path) != expected_sha256
    ):
        raise DirectionalRawPreviewRunBootstrapError(f"exact {label} is missing")
    try:
        content = path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectionalRawPreviewRunBootstrapError(f"{label} is not JSON") from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise DirectionalRawPreviewRunBootstrapError(f"{label} is not canonical")
    return document


def _verify_pin(repository: Path, commit: str, pin: dict[str, object]) -> None:
    relative = pin["path"]
    if not isinstance(relative, str):
        raise DirectionalRawPreviewRunBootstrapError("pin path is invalid")
    target = (repository / relative).resolve()
    try:
        target.relative_to(repository)
    except ValueError as exc:
        raise DirectionalRawPreviewRunBootstrapError("pin escapes repository") from exc
    if (
        not target.is_file()
        or target.is_symlink()
        or target.stat().st_size != pin["bytes"]
        or _sha256_file(target) != pin["sha256"]
    ):
        raise DirectionalRawPreviewRunBootstrapError(f"pinned bytes differ: {relative}")
    output = _git(repository, "ls-tree", commit, "--", relative).split()
    if len(output) < 4 or output[1] != "blob" or output[2] != pin["git_blob"]:
        raise DirectionalRawPreviewRunBootstrapError(f"pinned Git blob differs: {relative}")


def _pin_array(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise DirectionalRawPreviewRunBootstrapError(f"{label} must be an array")
    result = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"bytes", "git_blob", "path", "sha256"}:
            raise DirectionalRawPreviewRunBootstrapError(f"{label} pin schema differs")
        if type(item["bytes"]) is not int or item["bytes"] <= 0:
            raise DirectionalRawPreviewRunBootstrapError(f"{label} pin bytes invalid")
        _require_digest(item["sha256"], f"{label} SHA")
        _require_git_object(item["git_blob"], f"{label} blob")
        result.append(item)
    return result


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise DirectionalRawPreviewRunBootstrapError(f"{label} must be an object")
    return value


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        .encode("utf-8")
        + b"\n"
    )


def _stable_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise DirectionalRawPreviewRunBootstrapError(f"{label} must be lowercase 64-hex")
    return value


def _require_git_object(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT.fullmatch(value) is None:
        raise DirectionalRawPreviewRunBootstrapError(f"{label} must be lowercase 40-hex")
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
        raise DirectionalRawPreviewRunBootstrapError("Git verification failed")
    return result.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
