"""Bootstrap and run the one exact approved S7 full-history review."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_ACTION = "execute_exact_s7_three_group_full_s4_history_once_to_awaiting_review"
_SOURCE_COUNT = 5_026
_SESSION_COUNT = 2_513
_SOURCE_ROWS = 138_757_511
_SOURCE_BYTES = 15_910_278_169
_MANIFEST_DOCUMENT_COUNT = 7
_MANIFEST_DOCUMENT_KINDS = (
    "asset_release_manifest",
    "directional_candidate_manifest",
    "directional_completion_manifest",
    "inventory_candidate_manifest",
    "inventory_completion_manifest",
    "universe_release_manifest",
    "xnys_calendar_manifest",
)
_APPROVAL_RULE_VERSION = "s7_exact_group_history_execution_approval_v1"
_APPROVAL_STAGE = "s7_exact_group_history_full_s4_bounded_review"
_APPROVAL_SCOPE = (
    "exact_three_group_5026_artifact_full_s4_observed_history_once_to_"
    "awaiting_review_no_registry_no_adjudication_no_full_no_publish"
)
_PLAN_CAPABILITY_KEYS = {
    "adjudication",
    "canonical_identity_output",
    "exact_group_history_read",
    "full_run",
    "network_access",
    "parquet_content_read",
    "publication",
    "registry_evaluation",
}
_SOURCE_CAPABILITY_KEYS = _PLAN_CAPABILITY_KEYS - {"exact_group_history_read"}
_REQUEST_KEYS = {
    "artifact_type",
    "authorized_action",
    "created_at_utc",
    "created_by",
    "input_binding_digest",
    "inventory_projection_set_digest",
    "literal_version",
    "manifest_approval_id",
    "manifest_approval_sha256",
    "manifest_plan_id",
    "manifest_plan_sha256",
    "normalized_source_artifact_set_digest",
    "plan_id",
    "plan_sha256",
    "raw_source_artifact_set_digest",
    "request_event_id",
    "request_state",
    "resource_caps_digest",
    "schema_version",
    "source_artifact_count",
    "source_binding_id",
    "source_binding_sha256",
}
_SOURCE_BINDING_KEYS = {
    "artifact_type",
    "capabilities",
    "created_at_utc",
    "created_by",
    "execution_source_pins",
    "inventory_projection_set_digest",
    "manifest_controls",
    "manifest_document_set_digest",
    "manifest_documents",
    "normalized_source_artifact_set_digest",
    "raw_source_artifact_set_digest",
    "schema_version",
    "source_artifact_count",
    "source_artifacts",
    "source_binding_id",
    "source_bytes",
    "source_row_count",
    "state",
}
_PLAN_KEYS = {
    "artifact_type",
    "authorized_action",
    "capabilities_before_exact_literal",
    "contract",
    "created_at_utc",
    "created_by",
    "execution_semantics",
    "git_binding",
    "input_binding_digest",
    "manifest_controls",
    "plan_id",
    "plan_state",
    "resource_caps",
    "resource_caps_digest",
    "schema_version",
    "scope_binding",
    "source_artifact_count",
    "source_artifacts",
    "source_binding",
}
_APPROVAL_KEYS = {
    "adjudication_authorized",
    "approval_id",
    "approval_literal",
    "approval_literal_sha256",
    "approval_note",
    "approval_rule_version",
    "approval_stage",
    "approved_at_utc",
    "approved_by",
    "artifact_type",
    "authorized_action",
    "caller_scope_override_authorized",
    "decision",
    "exact_group_history_execution_authorized",
    "execution_data_root",
    "execution_scope",
    "external_evidence_capture_authorized",
    "forced_liquidation_authorized",
    "full_run_authorized",
    "membership_mutation_authorized",
    "network_access_authorized",
    "normalized_source_artifact_set_digest",
    "once_to_awaiting_review",
    "override_generation_authorized",
    "parquet_read_authorized",
    "plan_id",
    "plan_path",
    "plan_sha256",
    "publication_authorized",
    "registry_evaluation_authorized",
    "request_event_id",
    "request_event_path",
    "request_event_sha256",
    "schema_version",
    "share_class_filter_authorized",
    "source_artifact_set_digest",
    "source_binding_id",
    "source_binding_sha256",
    "source_discovery_authorized",
    "source_read_authorized",
    "table_materialization_authorized",
}


class ExactGroupHistoryRunBootstrapError(RuntimeError):
    """Raised before importing executable code when frozen bytes drift."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute the exact approved three-group S7 history review."
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
        root = args.data_root.expanduser().resolve()
        _bootstrap_exact_checkout(
            root,
            plan_id=_require_digest(args.plan_id, "Plan ID"),
            plan_sha256=_require_digest(args.plan_sha256, "Plan SHA"),
            approval_id=_require_digest(args.approval_id, "Approval ID"),
            approval_sha256=_require_digest(args.approval_sha256, "Approval SHA"),
        )
        runner, runner_error = _load_runner()
        completion = runner(
            root,
            plan_id=args.plan_id,
            expected_plan_sha256=args.plan_sha256,
            approval_id=args.approval_id,
            expected_approval_sha256=args.approval_sha256,
        )
    except (ExactGroupHistoryRunBootstrapError, ImportError, OSError) as exc:
        raise SystemExit(f"exact-group history bootstrap failed: {exc}") from exc
    except Exception as exc:
        if runner_error is not None and isinstance(exc, runner_error):
            raise SystemExit(f"exact-group history execution failed: {exc}") from exc
        raise
    print(
        json.dumps(
            {
                "candidate_id": completion.candidate_id,
                "completion_id": completion.completion_id,
                "state": completion.completion_state,
            },
            sort_keys=True,
        )
    )
    return 0


def _load_runner() -> tuple[Any, type[Exception]]:
    from ame_stocks_api.silver.identity_exact_group_history_runner import (
        IdentityExactGroupHistoryRunnerError,
        run_exact_s7_exact_group_history_review,
    )

    return run_exact_s7_exact_group_history_review, IdentityExactGroupHistoryRunnerError


def _bootstrap_exact_checkout(
    root: Path,
    *,
    plan_id: str,
    plan_sha256: str,
    approval_id: str,
    approval_sha256: str,
) -> None:
    if not root.is_dir() or root.is_symlink():
        raise ExactGroupHistoryRunBootstrapError("data_root is unsafe")
    plan = _read_control(
        root
        / "manifests/silver/identity/exact-group-history-execution-plans"
        / f"plan_id={plan_id}"
        / "manifest.json",
        plan_sha256,
        "Plan",
    )
    plan_logical = dict(plan)
    if (
        set(plan) != _PLAN_KEYS
        or plan_logical.pop("plan_id", None) != plan_id
        or _stable_digest(plan_logical) != plan_id
        or plan.get("artifact_type") != "s7_exact_group_history_execution_plan"
        or plan.get("authorized_action") != _ACTION
        or plan.get("plan_state") != "awaiting_exact_execution_approval"
        or plan.get("source_artifact_count") != _SOURCE_COUNT
    ):
        raise ExactGroupHistoryRunBootstrapError("Plan identity or scope differs")
    _verify_plan_projections(plan)
    semantics = _mapping(plan.get("execution_semantics"), "execution semantics")
    plan_capabilities = _mapping(plan.get("capabilities_before_exact_literal"), "Plan capabilities")
    if (
        semantics.get("share_class_is_not_a_filter") is not True
        or semantics.get("physical_artifact_count") != _SOURCE_COUNT
        or set(plan_capabilities) != _PLAN_CAPABILITY_KEYS
        or any(value is not False for value in plan_capabilities.values())
    ):
        raise ExactGroupHistoryRunBootstrapError(
            "Plan filter semantics or pre-literal capabilities differ"
        )
    git = _mapping(plan.get("git_binding"), "Git binding")
    commit = _require_git_object(git.get("git_commit"), "Git commit")
    tree = _require_git_object(git.get("git_tree"), "Git tree")
    controls = _mapping(plan.get("manifest_controls"), "manifest controls")
    manifest_id = _require_digest(controls.get("plan_id"), "manifest Plan ID")
    manifest_sha = _require_digest(controls.get("plan_sha256"), "manifest Plan SHA")
    manifest = _read_control(
        root
        / "manifests/silver/identity/exact-group-history-manifest-preflight-plans"
        / f"plan_id={manifest_id}"
        / "manifest.json",
        manifest_sha,
        "manifest Plan",
    )
    manifest_logical = dict(manifest)
    if (
        manifest_logical.pop("plan_id", None) != manifest_id
        or _stable_digest(manifest_logical) != manifest_id
    ):
        raise ExactGroupHistoryRunBootstrapError("manifest Plan identity differs")
    manifest_git = _mapping(manifest.get("git_binding"), "manifest Git binding")
    verification = _mapping(manifest.get("verification_binding"), "verification binding")
    if (
        manifest_git.get("git_commit") != commit
        or manifest_git.get("git_tree") != tree
        or manifest_git.get("runtime_file_set_digest") != git.get("runtime_file_set_digest")
        or verification.get("verification_file_set_digest")
        != git.get("verification_file_set_digest")
    ):
        raise ExactGroupHistoryRunBootstrapError("Git pin projections differ")
    repository = Path(__file__).resolve().parents[3]
    if (
        _git(repository, "rev-parse", "HEAD") != commit
        or _git(repository, "rev-parse", "HEAD^{tree}") != tree
        or _git(repository, "status", "--porcelain", "--untracked-files=all")
    ):
        raise ExactGroupHistoryRunBootstrapError("Git checkout differs")
    runtime_pins = _pin_array(manifest_git.get("runtime_files"), "runtime files")
    verification_pins = _pin_array(verification.get("verification_files"), "verification files")
    if _stable_digest(runtime_pins) != git.get("runtime_file_set_digest") or _stable_digest(
        verification_pins
    ) != git.get("verification_file_set_digest"):
        raise ExactGroupHistoryRunBootstrapError("file-set digest differs")
    for pin in (*runtime_pins, *verification_pins):
        _verify_file_pin(repository, commit, pin)
    approval = _read_control(
        root
        / "manifests/silver/identity/exact-group-history-execution-approvals"
        / f"approval_id={approval_id}"
        / "manifest.json",
        approval_sha256,
        "Approval",
    )
    plan_source = _mapping(plan.get("source_binding"), "Plan source binding")
    expected_plan_path = (
        "manifests/silver/identity/exact-group-history-execution-plans/"
        f"plan_id={plan_id}/manifest.json"
    )
    if set(approval) != _APPROVAL_KEYS:
        raise ExactGroupHistoryRunBootstrapError("Approval schema differs")
    if (
        approval.get("approval_id") != approval_id
        or _approval_slot_id(approval) != approval_id
        or approval.get("artifact_type") != "s7_exact_group_history_execution_approval"
        or approval.get("schema_version") != 1
        or approval.get("decision") != "approved"
        or approval.get("approval_stage") != _APPROVAL_STAGE
        or approval.get("plan_id") != plan_id
        or approval.get("plan_path") != expected_plan_path
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("execution_data_root") != str(root)
        or approval.get("source_binding_id") != plan_source.get("source_binding_id")
        or approval.get("source_binding_sha256") != plan_source.get("sha256")
        or approval.get("source_artifact_set_digest")
        != plan_source.get("raw_source_artifact_set_digest")
        or approval.get("normalized_source_artifact_set_digest")
        != plan_source.get("normalized_source_artifact_set_digest")
        or approval.get("authorized_action") != _ACTION
        or approval.get("approval_rule_version") != _APPROVAL_RULE_VERSION
        or approval.get("execution_scope") != _APPROVAL_SCOPE
        or approval.get("exact_group_history_execution_authorized") is not True
        or approval.get("source_read_authorized") is not True
        or approval.get("parquet_read_authorized") is not True
        or approval.get("once_to_awaiting_review") is not True
    ):
        raise ExactGroupHistoryRunBootstrapError("Approval binding differs")
    forbidden = (
        "source_discovery_authorized",
        "caller_scope_override_authorized",
        "share_class_filter_authorized",
        "network_access_authorized",
        "external_evidence_capture_authorized",
        "registry_evaluation_authorized",
        "adjudication_authorized",
        "override_generation_authorized",
        "table_materialization_authorized",
        "full_run_authorized",
        "publication_authorized",
        "membership_mutation_authorized",
        "forced_liquidation_authorized",
    )
    if any(approval.get(name) is not False for name in forbidden):
        raise ExactGroupHistoryRunBootstrapError("Approval is too broad")
    request = _verify_execution_request(root, plan=plan, approval=approval)
    _verify_source_binding(root, plan=plan, request=request)


def _verify_plan_projections(plan: Mapping[str, object]) -> None:
    contract = _mapping(plan.get("contract"), "Plan contract")
    scope = _mapping(plan.get("scope_binding"), "Plan scope")
    source = _mapping(plan.get("source_binding"), "Plan source binding")
    controls = _mapping(plan.get("manifest_controls"), "Plan manifest controls")
    caps = _mapping(plan.get("resource_caps"), "Plan resource caps")
    if (
        set(contract) != {"candidate_sha256", "contract_id", "schema_digest"}
        or set(scope) != {"scope_set_id", "scope_set_sha256"}
        or set(source)
        != {
            "created_by",
            "inventory_projection_set_digest",
            "normalized_source_artifact_set_digest",
            "path",
            "raw_source_artifact_set_digest",
            "sha256",
            "source_binding_id",
        }
        or set(controls) != {"approval_id", "approval_sha256", "plan_id", "plan_sha256"}
        or set(caps)
        != {
            "batch_row_count",
            "disk_free_bytes_hard_floor",
            "output_bytes_hard_cap",
            "physical_source_artifact_count",
            "physical_source_bytes",
            "physical_source_row_count",
            "review_group_count",
            "rss_bytes_hard_cap",
            "selected_row_hard_cap",
            "tmp_bytes_hard_cap",
            "wall_clock_seconds_hard_cap",
            "xnys_session_count",
        }
    ):
        raise ExactGroupHistoryRunBootstrapError("Plan projection schema differs")
    for label, value in (
        *contract.items(),
        *scope.items(),
        *((key, value) for key, value in source.items() if key != "created_by" and key != "path"),
        *controls.items(),
    ):
        _require_digest(value, label)
    _safe_relative(source["path"], "Plan source path")
    if any(type(value) is not int or value <= 0 for value in caps.values()) or (
        caps["physical_source_artifact_count"] != _SOURCE_COUNT
        or caps["physical_source_bytes"] != _SOURCE_BYTES
        or caps["physical_source_row_count"] != _SOURCE_ROWS
        or caps["xnys_session_count"] != _SESSION_COUNT
        or caps["review_group_count"] != 3
        or caps["rss_bytes_hard_cap"] != 2 * 1024**3
        or caps["disk_free_bytes_hard_floor"] != 40 * 1024**3
    ):
        raise ExactGroupHistoryRunBootstrapError("Plan resource caps differ")
    caps_digest = _stable_digest(caps)
    expected_input = _stable_digest(
        {
            "contract": [
                contract["contract_id"],
                contract["schema_digest"],
                contract["candidate_sha256"],
            ],
            "resource_caps_digest": caps_digest,
            "scope": [scope["scope_set_id"], scope["scope_set_sha256"]],
            "source": [
                source["source_binding_id"],
                source["sha256"],
                source["raw_source_artifact_set_digest"],
                source["inventory_projection_set_digest"],
                source["normalized_source_artifact_set_digest"],
            ],
        }
    )
    if (
        plan.get("schema_version") != 1
        or plan.get("resource_caps_digest") != caps_digest
        or plan.get("input_binding_digest") != expected_input
        or not isinstance(plan.get("source_artifacts"), list)
        or len(plan["source_artifacts"]) != _SOURCE_COUNT
    ):
        raise ExactGroupHistoryRunBootstrapError("Plan projection digest differs")


def _verify_execution_request(
    root: Path,
    *,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    request_id = _require_digest(approval.get("request_event_id"), "Request ID")
    request_sha = _require_digest(approval.get("request_event_sha256"), "Request SHA")
    expected_relative = (
        "manifests/silver/identity/exact-group-history-execution-requests/"
        f"request_event_id={request_id}.json"
    )
    request_relative = _safe_relative(approval.get("request_event_path"), "Request path")
    if request_relative != expected_relative:
        raise ExactGroupHistoryRunBootstrapError("Request path differs")
    request = _read_control(root / request_relative, request_sha, "Request")
    logical = dict(request)
    if (
        set(request) != _REQUEST_KEYS
        or logical.pop("request_event_id", None) != request_id
        or _stable_digest(logical) != request_id
        or request.get("artifact_type") != "s7_exact_group_history_execution_request"
        or request.get("authorized_action") != _ACTION
        or request.get("literal_version") != "s7_exact_group_history_execution_literal_v1"
        or request.get("request_state") != "awaiting_literal_human_approval"
        or request.get("schema_version") != 1
        or request.get("source_artifact_count") != _SOURCE_COUNT
    ):
        raise ExactGroupHistoryRunBootstrapError("Request identity or scope differs")
    source = _mapping(plan.get("source_binding"), "Plan source binding")
    controls = _mapping(plan.get("manifest_controls"), "Plan manifest controls")
    projections = {
        "input_binding_digest": plan.get("input_binding_digest"),
        "inventory_projection_set_digest": source.get("inventory_projection_set_digest"),
        "manifest_approval_id": controls.get("approval_id"),
        "manifest_approval_sha256": controls.get("approval_sha256"),
        "manifest_plan_id": controls.get("plan_id"),
        "manifest_plan_sha256": controls.get("plan_sha256"),
        "normalized_source_artifact_set_digest": source.get(
            "normalized_source_artifact_set_digest"
        ),
        "plan_id": plan.get("plan_id"),
        "plan_sha256": hashlib.sha256(_canonical_bytes(plan)).hexdigest(),
        "raw_source_artifact_set_digest": source.get("raw_source_artifact_set_digest"),
        "resource_caps_digest": plan.get("resource_caps_digest"),
        "source_binding_id": source.get("source_binding_id"),
        "source_binding_sha256": source.get("sha256"),
    }
    if any(request.get(key) != value for key, value in projections.items()):
        raise ExactGroupHistoryRunBootstrapError("Request-to-Plan projection differs")
    excluded = {
        "artifact_type",
        "created_at_utc",
        "created_by",
        "request_state",
        "schema_version",
    }
    literal_payload = {key: value for key, value in request.items() if key not in excluded}
    literal_payload["request_event_sha256"] = request_sha
    literal = json.dumps(literal_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    if (
        approval.get("approval_literal") != literal
        or approval.get("approval_literal_sha256") != hashlib.sha256(literal.encode()).hexdigest()
    ):
        raise ExactGroupHistoryRunBootstrapError("Request literal differs from Approval")
    return request


def _verify_source_binding(
    root: Path,
    *,
    plan: Mapping[str, object],
    request: Mapping[str, object],
) -> None:
    plan_source = _mapping(plan.get("source_binding"), "Plan source binding")
    source_id = _require_digest(plan_source.get("source_binding_id"), "source binding ID")
    source_sha = _require_digest(plan_source.get("sha256"), "source binding SHA")
    relative = _safe_relative(plan_source.get("path"), "source binding path")
    source = _read_control(root / relative, source_sha, "SourceBinding")
    logical = dict(source)
    source_capabilities = _mapping(source.get("capabilities"), "SourceBinding capabilities")
    if (
        set(source) != _SOURCE_BINDING_KEYS
        or set(source_capabilities) != _SOURCE_CAPABILITY_KEYS
        or any(value is not False for value in source_capabilities.values())
        or logical.pop("source_binding_id", None) != source_id
        or _stable_digest(logical) != source_id
        or source.get("artifact_type") != "s7_exact_group_history_source_binding"
        or source.get("state") != "awaiting_exact_execution_approval"
        or source.get("source_artifact_count") != _SOURCE_COUNT
        or source.get("source_row_count") != _SOURCE_ROWS
        or source.get("source_bytes") != _SOURCE_BYTES
    ):
        raise ExactGroupHistoryRunBootstrapError("SourceBinding identity or totals differ")
    raw = _source_array(source.get("source_artifacts"), raw=True)
    normalized = _source_array(source.get("execution_source_pins"), raw=False)
    if (
        len(raw) != _SOURCE_COUNT
        or len(normalized) != _SOURCE_COUNT
        or raw != sorted(raw, key=_raw_source_key)
        or normalized != sorted(normalized, key=_execution_source_key)
        or len({(item["table"], item["session_date"]) for item in raw}) != _SOURCE_COUNT
        or sum(item["table"] == "asset_observation_daily" for item in raw) != _SESSION_COUNT
        or sum(item["table"] == "universe_source_daily" for item in raw) != _SESSION_COUNT
        or sum(int(item["row_count"]) for item in raw) != _SOURCE_ROWS
        or sum(int(item["bytes"]) for item in raw) != _SOURCE_BYTES
    ):
        raise ExactGroupHistoryRunBootstrapError("SourceBinding source scope differs")
    projected_inventory = [
        {
            key: item[key]
            for key in (
                "bytes",
                "path",
                "release_id",
                "release_manifest_sha256",
                "row_count",
                "session_date",
                "sha256",
                "table",
            )
        }
        for item in raw
    ]
    projected_execution = sorted(
        [
            {
                "bytes": item["bytes"],
                "path": item["path"],
                "release_id": item["release_id"],
                "release_manifest_sha256": item["release_manifest_sha256"],
                "row_count": item["row_count"],
                "schema_digest": item["source_schema_digest"],
                "session_date": item["session_date"],
                "sha256": item["sha256"],
                "source_contract_id": item["source_contract_id"],
                "table": item["table"],
            }
            for item in raw
        ],
        key=_execution_source_key,
    )
    raw_digest = _stable_digest(raw)
    inventory_digest = _stable_digest(projected_inventory)
    normalized_digest = _stable_digest(normalized)
    if (
        projected_execution != normalized
        or source.get("raw_source_artifact_set_digest") != raw_digest
        or source.get("inventory_projection_set_digest") != inventory_digest
        or source.get("normalized_source_artifact_set_digest") != normalized_digest
        or plan_source.get("raw_source_artifact_set_digest") != raw_digest
        or plan_source.get("inventory_projection_set_digest") != inventory_digest
        or plan_source.get("normalized_source_artifact_set_digest") != normalized_digest
        or plan.get("source_artifacts") != normalized
        or request.get("raw_source_artifact_set_digest") != raw_digest
        or request.get("inventory_projection_set_digest") != inventory_digest
        or request.get("normalized_source_artifact_set_digest") != normalized_digest
    ):
        raise ExactGroupHistoryRunBootstrapError(
            "SourceBinding raw/inventory/normalized projection differs"
        )
    source_controls = _mapping(source.get("manifest_controls"), "source controls")
    plan_controls = _mapping(plan.get("manifest_controls"), "Plan controls")
    expected_control_keys = {
        "approval_id",
        "approval_sha256",
        "literal_sha256",
        "plan_id",
        "plan_sha256",
        "request_event_id",
        "request_event_sha256",
        "run_intent_id",
        "run_intent_path",
        "run_intent_sha256",
    }
    documents = source.get("manifest_documents")
    if (
        not isinstance(documents, list)
        or any(
            not isinstance(item, dict)
            or set(item) != {"bytes", "kind", "logical_id", "path", "sha256"}
            for item in documents
        )
        or len(documents) != _MANIFEST_DOCUMENT_COUNT
        or tuple(item["kind"] for item in documents) != _MANIFEST_DOCUMENT_KINDS
        or documents
        != sorted(
            documents,
            key=lambda item: (
                item["kind"],
                item["logical_id"],
                item["path"],
                item["sha256"],
                item["bytes"],
            ),
        )
    ):
        raise ExactGroupHistoryRunBootstrapError("SourceBinding manifest documents differ")
    for item in documents:
        if (
            type(item["bytes"]) is not int
            or int(item["bytes"]) <= 0
            or not isinstance(item["kind"], str)
        ):
            raise ExactGroupHistoryRunBootstrapError("SourceBinding manifest documents differ")
        _require_digest(item["logical_id"], "manifest document ID")
        _require_digest(item["sha256"], "manifest document SHA")
        _safe_relative(item["path"], "manifest document path")
    for name in expected_control_keys - {"run_intent_path"}:
        _require_digest(source_controls.get(name), name)
    expected_relative = (
        "manifests/silver/identity/exact-group-history-source-bindings/"
        f"run_intent_id={_require_digest(source_controls.get('run_intent_id'), 'run intent ID')}/"
        "manifest.json"
    )
    expected_intent_path = (
        "manifests/silver/identity/"
        "exact-group-history-manifest-preflight-run-intents/"
        f"plan_id={source_controls['plan_id']}/"
        f"approval_id={source_controls['approval_id']}/manifest.json"
    )
    if (
        set(source_controls) != expected_control_keys
        or source.get("manifest_document_set_digest") != _stable_digest(documents)
        or relative != expected_relative
        or source_controls.get("run_intent_path") != expected_intent_path
        or any(
            source_controls.get(source_key) != plan_controls.get(plan_key)
            for source_key, plan_key in (
                ("plan_id", "plan_id"),
                ("plan_sha256", "plan_sha256"),
                ("approval_id", "approval_id"),
                ("approval_sha256", "approval_sha256"),
            )
        )
    ):
        raise ExactGroupHistoryRunBootstrapError("SourceBinding manifest lineage differs")


def _source_array(value: object, *, raw: bool) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ExactGroupHistoryRunBootstrapError("source array is not a list")
    raw_keys = {
        "bytes",
        "content_opened",
        "disk_is_regular_file",
        "disk_is_symlink",
        "disk_size_bytes",
        "media_type",
        "path",
        "release_id",
        "release_manifest_sha256",
        "role",
        "row_count",
        "session_date",
        "sha256",
        "source_contract_id",
        "source_schema_digest",
        "table",
    }
    execution_keys = {
        "bytes",
        "path",
        "release_id",
        "release_manifest_sha256",
        "row_count",
        "schema_digest",
        "session_date",
        "sha256",
        "source_contract_id",
        "table",
    }
    result: list[dict[str, object]] = []
    for value_item in value:
        item = _mapping(value_item, "source item")
        if set(item) != (raw_keys if raw else execution_keys):
            raise ExactGroupHistoryRunBootstrapError("source item schema differs")
        if item.get("table") not in {
            "asset_observation_daily",
            "universe_source_daily",
        }:
            raise ExactGroupHistoryRunBootstrapError("source table differs")
        for name in (
            "release_id",
            "release_manifest_sha256",
            "sha256",
            "source_contract_id",
            "source_schema_digest" if raw else "schema_digest",
        ):
            _require_digest(item.get(name), name)
        if (
            type(item.get("bytes")) is not int
            or int(item["bytes"]) <= 0
            or type(item.get("row_count")) is not int
            or int(item["row_count"]) < 0
        ):
            raise ExactGroupHistoryRunBootstrapError("source counts differ")
        _safe_relative(item.get("path"), "source path")
        if raw and (
            item.get("disk_size_bytes") != item.get("bytes")
            or item.get("role") != "data"
            or item.get("media_type") != "application/vnd.apache.parquet"
            or item.get("disk_is_regular_file") is not True
            or item.get("disk_is_symlink") is not False
            or item.get("content_opened") is not False
        ):
            raise ExactGroupHistoryRunBootstrapError("raw source boundary differs")
        result.append(item)
    return result


def _raw_source_key(item: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(
        item[name]
        for name in (
            "table",
            "session_date",
            "release_id",
            "release_manifest_sha256",
            "source_contract_id",
            "source_schema_digest",
            "path",
            "sha256",
            "bytes",
            "row_count",
            "disk_size_bytes",
            "role",
            "media_type",
            "disk_is_regular_file",
            "disk_is_symlink",
            "content_opened",
        )
    )


def _execution_source_key(item: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(
        item[name]
        for name in (
            "table",
            "session_date",
            "release_id",
            "release_manifest_sha256",
            "path",
            "sha256",
            "bytes",
            "row_count",
            "source_contract_id",
            "schema_digest",
        )
    )


def _approval_slot_id(approval: Mapping[str, object]) -> str:
    literal_sha = _require_digest(approval.get("approval_literal_sha256"), "approval literal SHA")
    return _stable_digest(
        {
            "approval_literal_sha256": literal_sha,
            "approval_rule_version": _APPROVAL_RULE_VERSION,
            "authorized_action": _ACTION,
            "execution_scope": _APPROVAL_SCOPE,
            "namespace": "ame_stocks.s7.exact_group_history.execution_approval_slot.v1",
            "plan_id": _require_digest(approval.get("plan_id"), "Plan ID"),
            "plan_sha256": _require_digest(approval.get("plan_sha256"), "Plan SHA"),
            "request_event_id": _require_digest(approval.get("request_event_id"), "Request ID"),
            "request_event_sha256": _require_digest(
                approval.get("request_event_sha256"), "Request SHA"
            ),
        }
    )


def _safe_relative(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ExactGroupHistoryRunBootstrapError(f"{label} is not text")
    path = Path(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise ExactGroupHistoryRunBootstrapError(f"{label} is unsafe")
    return value


def _read_control(path: Path, expected_sha: str, label: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        raise ExactGroupHistoryRunBootstrapError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected_sha:
        raise ExactGroupHistoryRunBootstrapError(f"{label} SHA differs")
    try:
        value = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExactGroupHistoryRunBootstrapError(f"{label} is not JSON") from exc
    if not isinstance(value, dict) or _canonical_bytes(value) != content:
        raise ExactGroupHistoryRunBootstrapError(f"{label} is not canonical")
    return value


def _pin_array(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value:
        raise ExactGroupHistoryRunBootstrapError(f"{label} is empty")
    result = []
    for raw in value:
        item = _mapping(raw, label)
        if set(item) != {"bytes", "git_blob", "path", "sha256"}:
            raise ExactGroupHistoryRunBootstrapError(f"{label} schema differs")
        result.append(item)
    return result


def _verify_file_pin(repository: Path, commit: str, pin: Mapping[str, object]) -> None:
    relative = pin.get("path")
    if (
        not isinstance(relative, str)
        or Path(relative).is_absolute()
        or ".." in Path(relative).parts
    ):
        raise ExactGroupHistoryRunBootstrapError("file pin path is unsafe")
    path = repository / relative
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.get("bytes")
        or _sha256_file(path) != pin.get("sha256")
        or _git(repository, "rev-parse", f"{commit}:{relative}") != pin.get("git_blob")
    ):
        raise ExactGroupHistoryRunBootstrapError(f"file pin differs: {relative}")


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ExactGroupHistoryRunBootstrapError(f"{label} is not an object")
    return dict(value)


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ExactGroupHistoryRunBootstrapError(f"{label} must be lowercase 64-hex")
    return value


def _require_git_object(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT.fullmatch(value) is None:
        raise ExactGroupHistoryRunBootstrapError(f"{label} must be lowercase 40-hex")
    return value


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _stable_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise ExactGroupHistoryRunBootstrapError("Git verification failed")
    return result.stdout.strip()


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExactGroupHistoryRunBootstrapError("duplicate JSON key")
        result[key] = value
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
