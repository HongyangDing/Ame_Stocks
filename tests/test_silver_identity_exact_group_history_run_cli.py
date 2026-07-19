from __future__ import annotations

import hashlib
import importlib
import json
import sys

import pytest


def test_run_cli_import_does_not_import_runner() -> None:
    runner_name = "ame_stocks_api.silver.identity_exact_group_history_runner"
    sys.modules.pop(runner_name, None)
    module = importlib.import_module("ame_stocks_api.cli.run_s7_exact_group_history_review")
    assert module is not None
    assert runner_name not in sys.modules


def test_bootstrap_failure_happens_before_runner_import(monkeypatch, tmp_path) -> None:
    module = importlib.import_module("ame_stocks_api.cli.run_s7_exact_group_history_review")
    called = False

    def fail_bootstrap(*args, **kwargs):
        del args, kwargs
        raise module.ExactGroupHistoryRunBootstrapError("tampered pin")

    def load_runner():
        nonlocal called
        called = True
        raise AssertionError("runner imported before bootstrap")

    monkeypatch.setattr(module, "_bootstrap_exact_checkout", fail_bootstrap)
    monkeypatch.setattr(module, "_load_runner", load_runner)
    with pytest.raises(SystemExit, match="bootstrap failed"):
        module.main(
            [
                "--data-root",
                str(tmp_path),
                "--plan-id",
                "1" * 64,
                "--plan-sha256",
                "2" * 64,
                "--approval-id",
                "3" * 64,
                "--approval-sha256",
                "4" * 64,
            ]
        )
    assert called is False


def _execution_documents(module, root, tamper):
    commit = "a" * 40
    tree = "b" * 40
    runtime = [{"bytes": 1, "git_blob": "c" * 40, "path": "runtime.py", "sha256": "1" * 64}]
    verification = [{"bytes": 1, "git_blob": "d" * 40, "path": "test.py", "sha256": "2" * 64}]
    manifest_logical = {
        "artifact_type": "manifest-plan",
        "git_binding": {
            "git_commit": commit,
            "git_tree": tree,
            "runtime_file_set_digest": module._stable_digest(runtime),
            "runtime_files": runtime,
        },
        "verification_binding": {
            "verification_file_set_digest": module._stable_digest(verification),
            "verification_files": verification,
        },
    }
    manifest_id = module._stable_digest(manifest_logical)
    manifest = {**manifest_logical, "plan_id": manifest_id}
    manifest_sha = hashlib.sha256(module._canonical_bytes(manifest)).hexdigest()
    manifest_approval_id = "3" * 64
    manifest_approval_sha = "4" * 64
    run_intent_id = "5" * 64
    source_path = (
        "manifests/silver/identity/exact-group-history-source-bindings/"
        f"run_intent_id={run_intent_id}/manifest.json"
    )
    rows = [1, module._SOURCE_ROWS - 1]
    sizes = [1, module._SOURCE_BYTES - 1]
    raw = []
    for index, table in enumerate(("asset_observation_daily", "universe_source_daily")):
        raw.append(
            {
                "bytes": sizes[index],
                "content_opened": False,
                "disk_is_regular_file": True,
                "disk_is_symlink": False,
                "disk_size_bytes": sizes[index],
                "media_type": "application/vnd.apache.parquet",
                "path": f"silver/{table}/session_date=2026-01-02/part.parquet",
                "release_id": f"{6 + index:x}" * 64,
                "release_manifest_sha256": f"{8 + index:x}" * 64,
                "role": "data",
                "row_count": rows[index],
                "session_date": "2026-01-02",
                "sha256": f"{10 + index:x}" * 64,
                "source_contract_id": f"{12 + index:x}" * 64,
                "source_schema_digest": f"{14 + index:x}" * 64,
                "table": table,
            }
        )
    normalized = [
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
    ]
    if tamper == "source_normalize":
        raw[0]["sha256"] = "f" * 64
    inventory = [
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
    raw_digest = module._stable_digest(raw)
    inventory_digest = module._stable_digest(inventory)
    normalized_digest = module._stable_digest(normalized)
    manifest_documents = [
        {
            "bytes": 1,
            "kind": kind,
            "logical_id": f"{index + 1:x}" * 64,
            "path": f"manifests/input-{index}.json",
            "sha256": f"{index + 8:x}" * 64,
        }
        for index, kind in enumerate(module._MANIFEST_DOCUMENT_KINDS)
    ]
    if tamper == "manifest_kind":
        manifest_documents[0]["kind"] = "unexpected_manifest"
    source_logical = {
        "artifact_type": "s7_exact_group_history_source_binding",
        "capabilities": {
            "adjudication": False,
            "canonical_identity_output": False,
            "full_run": False,
            "network_access": False,
            "parquet_content_read": False,
            "publication": False,
            "registry_evaluation": False,
        },
        "created_at_utc": "2026-07-19T00:00:00+00:00",
        "created_by": "source-reader",
        "execution_source_pins": normalized,
        "inventory_projection_set_digest": inventory_digest,
        "manifest_controls": {
            "approval_id": manifest_approval_id,
            "approval_sha256": manifest_approval_sha,
            "literal_sha256": "6" * 64,
            "plan_id": manifest_id,
            "plan_sha256": manifest_sha,
            "request_event_id": "7" * 64,
            "request_event_sha256": "8" * 64,
            "run_intent_id": run_intent_id,
            "run_intent_path": (
                "manifests/silver/identity/"
                "exact-group-history-manifest-preflight-run-intents/"
                f"plan_id={manifest_id}/approval_id={manifest_approval_id}/manifest.json"
            ),
            "run_intent_sha256": "9" * 64,
        },
        "manifest_document_set_digest": module._stable_digest(manifest_documents),
        "manifest_documents": manifest_documents,
        "normalized_source_artifact_set_digest": normalized_digest,
        "raw_source_artifact_set_digest": raw_digest,
        "schema_version": 1,
        "source_artifact_count": 2,
        "source_artifacts": raw,
        "source_bytes": module._SOURCE_BYTES,
        "source_row_count": module._SOURCE_ROWS,
        "state": "awaiting_exact_execution_approval",
    }
    if tamper == "source_capability":
        source_logical["capabilities"]["network_access"] = True
    elif tamper == "source_schema":
        source_logical["unexpected_control"] = False
    source_id = module._stable_digest(source_logical)
    source = {**source_logical, "source_binding_id": source_id}
    source_sha = hashlib.sha256(module._canonical_bytes(source)).hexdigest()
    contract = {
        "candidate_sha256": "a" * 64,
        "contract_id": "b" * 64,
        "schema_digest": "c" * 64,
    }
    scope = {"scope_set_id": "d" * 64, "scope_set_sha256": "e" * 64}
    resource_caps = {
        "batch_row_count": 65_536,
        "disk_free_bytes_hard_floor": 40 * 1024**3,
        "output_bytes_hard_cap": 512 * 1024**2,
        "physical_source_artifact_count": 2,
        "physical_source_bytes": module._SOURCE_BYTES,
        "physical_source_row_count": module._SOURCE_ROWS,
        "review_group_count": 3,
        "rss_bytes_hard_cap": 2 * 1024**3,
        "selected_row_hard_cap": 1_000_000,
        "tmp_bytes_hard_cap": 2 * 1024**3,
        "wall_clock_seconds_hard_cap": 43_200,
        "xnys_session_count": 1,
    }
    resource_caps_digest = module._stable_digest(resource_caps)
    plan_source = {
        "created_by": "source-reader",
        "inventory_projection_set_digest": inventory_digest,
        "normalized_source_artifact_set_digest": normalized_digest,
        "path": source_path,
        "raw_source_artifact_set_digest": raw_digest,
        "sha256": source_sha,
        "source_binding_id": source_id,
    }
    input_binding_digest = module._stable_digest(
        {
            "contract": [
                contract["contract_id"],
                contract["schema_digest"],
                contract["candidate_sha256"],
            ],
            "resource_caps_digest": resource_caps_digest,
            "scope": [scope["scope_set_id"], scope["scope_set_sha256"]],
            "source": [
                source_id,
                source_sha,
                raw_digest,
                inventory_digest,
                normalized_digest,
            ],
        }
    )
    plan_logical = {
        "artifact_type": "s7_exact_group_history_execution_plan",
        "authorized_action": module._ACTION,
        "capabilities_before_exact_literal": {
            "adjudication": False,
            "canonical_identity_output": False,
            "exact_group_history_read": False,
            "full_run": False,
            "network_access": False,
            "parquet_content_read": False,
            "publication": False,
            "registry_evaluation": False,
        },
        "contract": contract,
        "created_at_utc": "2026-07-19T00:00:00+00:00",
        "created_by": "plan-actor",
        "execution_semantics": {
            "physical_artifact_count": 2,
            "share_class_is_not_a_filter": True,
        },
        "git_binding": {
            "git_commit": commit,
            "git_tree": tree,
            "runtime_file_set_digest": module._stable_digest(runtime),
            "verification_file_set_digest": module._stable_digest(verification),
        },
        "input_binding_digest": input_binding_digest,
        "manifest_controls": {
            "approval_id": manifest_approval_id,
            "approval_sha256": manifest_approval_sha,
            "plan_id": manifest_id,
            "plan_sha256": manifest_sha,
        },
        "plan_state": "awaiting_exact_execution_approval",
        "resource_caps": resource_caps,
        "resource_caps_digest": resource_caps_digest,
        "schema_version": 1,
        "scope_binding": scope,
        "source_artifact_count": 2,
        "source_artifacts": normalized,
        "source_binding": plan_source,
    }
    if tamper == "plan_capability":
        plan_logical["capabilities_before_exact_literal"]["network_access"] = True
    elif tamper == "plan_projection":
        plan_logical["input_binding_digest"] = "0" * 64
    plan_id = module._stable_digest(plan_logical)
    plan = {**plan_logical, "plan_id": plan_id}
    plan_sha = hashlib.sha256(module._canonical_bytes(plan)).hexdigest()
    request_logical = {
        "artifact_type": "s7_exact_group_history_execution_request",
        "authorized_action": module._ACTION,
        "created_at_utc": "2026-07-19T00:00:00+00:00",
        "created_by": "request-actor",
        "input_binding_digest": plan["input_binding_digest"],
        "inventory_projection_set_digest": inventory_digest,
        "literal_version": "s7_exact_group_history_execution_literal_v1",
        "manifest_approval_id": manifest_approval_id,
        "manifest_approval_sha256": manifest_approval_sha,
        "manifest_plan_id": manifest_id,
        "manifest_plan_sha256": manifest_sha,
        "normalized_source_artifact_set_digest": normalized_digest,
        "plan_id": plan_id,
        "plan_sha256": plan_sha,
        "raw_source_artifact_set_digest": raw_digest,
        "request_state": "awaiting_literal_human_approval",
        "resource_caps_digest": plan["resource_caps_digest"],
        "schema_version": 1,
        "source_artifact_count": 2,
        "source_binding_id": source_id,
        "source_binding_sha256": source_sha,
    }
    if tamper == "request_projection":
        request_logical["source_binding_id"] = "0" * 64
    elif tamper == "request_schema":
        request_logical["unexpected_control"] = False
    elif tamper == "request_literal_version":
        request_logical["literal_version"] = "wrong"
    request_id = module._stable_digest(request_logical)
    request = {**request_logical, "request_event_id": request_id}
    request_sha = hashlib.sha256(module._canonical_bytes(request)).hexdigest()
    excluded = {
        "artifact_type",
        "created_at_utc",
        "created_by",
        "request_state",
        "schema_version",
    }
    literal_payload = {key: value for key, value in request.items() if key not in excluded}
    literal_payload["request_event_sha256"] = request_sha
    literal = json.dumps(literal_payload, separators=(",", ":"), sort_keys=True)
    if tamper == "literal":
        literal = literal[:-1] + ',"tampered":true}'
    approval = {
        "approval_note": "",
        "approval_literal": literal,
        "approval_literal_sha256": hashlib.sha256(literal.encode()).hexdigest(),
        "approval_rule_version": module._APPROVAL_RULE_VERSION,
        "approval_stage": module._APPROVAL_STAGE,
        "approved_at_utc": "2026-07-19T00:01:00+00:00",
        "approved_by": "independent-reviewer",
        "artifact_type": "s7_exact_group_history_execution_approval",
        "authorized_action": module._ACTION,
        "decision": "approved",
        "execution_data_root": str(root),
        "execution_scope": module._APPROVAL_SCOPE,
        "exact_group_history_execution_authorized": True,
        "normalized_source_artifact_set_digest": normalized_digest,
        "once_to_awaiting_review": True,
        "parquet_read_authorized": True,
        "plan_id": plan_id,
        "plan_path": (
            "manifests/silver/identity/exact-group-history-execution-plans/"
            f"plan_id={plan_id}/manifest.json"
        ),
        "plan_sha256": plan_sha,
        "request_event_id": request_id,
        "request_event_path": (
            "manifests/silver/identity/exact-group-history-execution-requests/"
            f"request_event_id={request_id}.json"
        ),
        "request_event_sha256": request_sha,
        "schema_version": 1,
        "source_artifact_set_digest": raw_digest,
        "source_binding_id": source_id,
        "source_binding_sha256": source_sha,
        "source_read_authorized": True,
    }
    for name in (
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
    ):
        approval[name] = False
    if tamper == "forbidden":
        approval["external_evidence_capture_authorized"] = True
    elif tamper == "approval_schema":
        approval["future_authorized"] = True
    approval_id = module._approval_slot_id(approval)
    approval["approval_id"] = approval_id
    approval_sha = hashlib.sha256(module._canonical_bytes(approval)).hexdigest()
    return (
        {
            "Plan": plan,
            "manifest Plan": manifest,
            "Approval": approval,
            "Request": request,
            "SourceBinding": source,
        },
        (plan_id, plan_sha, approval_id, approval_sha),
        (commit, tree),
    )


@pytest.mark.parametrize(
    ("tamper", "expected"),
    [
        ("request_projection", "Request-to-Plan projection differs"),
        ("source_normalize", "raw/inventory/normalized projection differs"),
        ("literal", "Request literal differs"),
        ("forbidden", "Approval is too broad"),
        ("plan_capability", "pre-literal capabilities differ"),
        ("plan_projection", "Plan projection digest differs"),
        ("source_capability", "SourceBinding identity or totals differ"),
        ("source_schema", "SourceBinding identity or totals differ"),
        ("manifest_kind", "SourceBinding manifest documents differ"),
        ("request_schema", "Request identity or scope differs"),
        ("request_literal_version", "Request identity or scope differs"),
        ("approval_schema", "Approval schema differs"),
    ],
)
def test_stdlib_gate_rejects_control_tamper_before_runner_import(
    monkeypatch, tmp_path, tamper, expected
) -> None:
    module = importlib.import_module("ame_stocks_api.cli.run_s7_exact_group_history_review")
    monkeypatch.setattr(module, "_SOURCE_COUNT", 2)
    monkeypatch.setattr(module, "_SESSION_COUNT", 1)
    documents, identifiers, git_objects = _execution_documents(module, tmp_path, tamper)
    plan_id, plan_sha, approval_id, approval_sha = identifiers
    commit, tree = git_objects
    monkeypatch.setattr(
        module,
        "_read_control",
        lambda path, expected, label: documents[label],
    )
    monkeypatch.setattr(module, "_verify_file_pin", lambda *args: None)

    def fake_git(repository, *args):
        del repository
        if args == ("rev-parse", "HEAD"):
            return commit
        if args == ("rev-parse", "HEAD^{tree}"):
            return tree
        if args == ("status", "--porcelain", "--untracked-files=all"):
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(module, "_git", fake_git)
    loaded = False

    def load_runner():
        nonlocal loaded
        loaded = True
        raise AssertionError("runner imported after a tampered bootstrap")

    monkeypatch.setattr(module, "_load_runner", load_runner)
    with pytest.raises(SystemExit, match=expected):
        module.main(
            [
                "--data-root",
                str(tmp_path),
                "--plan-id",
                plan_id,
                "--plan-sha256",
                plan_sha,
                "--approval-id",
                approval_id,
                "--approval-sha256",
                approval_sha,
            ]
        )
    assert loaded is False
