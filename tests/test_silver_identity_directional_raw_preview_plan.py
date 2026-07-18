from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    AUTHORIZED_ACTION,
    INVENTORY_CANDIDATE_DATA_SHA256,
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_CONTRACT_ID,
    INVENTORY_INPUT_BINDING_DIGEST,
    INVENTORY_SCHEMA_DIGEST,
    INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    REQUIRED_PREPARATION_RUNTIME_PATHS,
    REQUIRED_PREPARATION_VERIFICATION_PATHS,
    S4_SOURCE_PINS,
    IdentityDirectionalRawPreviewPlanError,
    IdentityDirectionalRawPreviewPlanStore,
    S7DirectionalRawPreviewControlFilePin,
    S7DirectionalRawPreviewPreparationCaps,
    S7DirectionalRawPreviewPreparationPlan,
    S7DirectionalRawPreviewScopeSet,
    StoredDirectionalRawPreviewControl,
)

CREATED = datetime(2026, 7, 18, 4, 0, tzinfo=UTC)


def _pin(path: str) -> S7DirectionalRawPreviewControlFilePin:
    content = path.encode("utf-8")
    return S7DirectionalRawPreviewControlFilePin(
        path=path,
        git_blob=hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _scope() -> S7DirectionalRawPreviewScopeSet:
    return S7DirectionalRawPreviewScopeSet.create(
        created_by="s7-directional-scope-author",
        created_at_utc=CREATED,
    )


def _plan(scope: S7DirectionalRawPreviewScopeSet | None = None):
    scope = scope or _scope()
    receipt = StoredDirectionalRawPreviewControl(
        scope.relative_path,
        scope.sha256,
        len(scope.content),
    )
    plan = S7DirectionalRawPreviewPreparationPlan.create(
        created_by="s7-directional-plan-author",
        created_at_utc=CREATED,
        git_commit="a" * 40,
        git_tree="b" * 40,
        runtime_files=tuple(_pin(path) for path in REQUIRED_PREPARATION_RUNTIME_PATHS),
        verification_files=tuple(_pin(path) for path in REQUIRED_PREPARATION_VERIFICATION_PATHS),
        scope=scope,
        stored_scope=receipt,
    )
    return scope, receipt, plan


def test_scope_is_exact_eleven_pairs_not_range_or_cartesian_product() -> None:
    scope = _scope()
    document = scope.document
    cases = document["cases"]
    pairs = [(case["ticker"], session) for case in cases for session in case["sessions"]]

    assert document["pair_count"] == 11
    assert document["unique_session_count"] == 11
    assert document["expected_physical_artifact_count"] == 22
    assert document["fixed_contract_scope_digest"] == (DIRECTIONAL_RAW_PREVIEW_FIXED_SCOPE_DIGEST)
    assert pairs == [
        ("SOR", "2024-12-31"),
        ("SOR", "2025-01-02"),
        ("SOR", "2025-01-03"),
        ("XZO", "2025-11-04"),
        ("XZO", "2025-11-05"),
        ("XZO", "2025-11-06"),
        ("XZO", "2025-11-07"),
        ("ANABV", "2026-04-06"),
        ("ANABV", "2026-04-07"),
        ("ANABV", "2026-04-17"),
        ("ANABV", "2026-04-20"),
    ]
    assert len(pairs) == len(set(pairs)) == 11
    assert "no_range_no_cartesian_product" in document["selection_rule"]
    assert S7DirectionalRawPreviewScopeSet.from_dict(json.loads(scope.content)) == scope

    changed = json.loads(scope.content)
    changed["cases"][0]["ticker"] = "sor"
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="exact eleven pairs"):
        S7DirectionalRawPreviewScopeSet.from_dict(changed)
    changed = json.loads(scope.content)
    changed["cases"][0]["sessions"].append("2025-01-06")
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="exact eleven pairs"):
        S7DirectionalRawPreviewScopeSet.from_dict(changed)


def test_plan_binds_contract_registry_s4_inventory_git_files_and_caps() -> None:
    _, _, plan = _plan()
    document = plan.document
    lineage = document["inventory_lineage"]

    assert document["authorized_action"] == AUTHORIZED_ACTION
    assert "prepare_and_freeze" in AUTHORIZED_ACTION
    assert document["preparation_scope"].endswith("no_runner_no_execution")
    assert set(document["capabilities"].values()) == {False}
    assert document["future_executable_package"] == {
        "approval_recorder_bound": False,
        "completion_manifest_sha256_bound": False,
        "exact_daily_artifact_refs_bound": False,
        "new_exact_execution_plan_and_literal_required": True,
        "run_cli_bound": False,
        "runner_bound": False,
        "this_plan_is_executable": False,
    }
    assert document["contract_binding"]["contract_id"] == (
        IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID
    )
    assert (
        document["preparation_design"]["semantics"]["registry_exclusivity_semantics_digest"]
        == DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
    )
    assert (
        document["preparation_design"]["semantics"]["registry_evaluation_state"] == "not_evaluated"
    )
    assert document["source_binding"]["expected_daily_artifact_count"] == 22
    assert document["source_binding"]["exact_daily_artifact_refs_state"].startswith(
        "pending_manifest_only"
    )
    assert [item["table"] for item in document["source_binding"]["source_pins"]] == [
        "asset_observation_daily",
        "universe_source_daily",
    ]
    assert lineage == {
        "candidate_data_sha256": INVENTORY_CANDIDATE_DATA_SHA256,
        "candidate_id": INVENTORY_CANDIDATE_ID,
        "candidate_manifest_sha256": INVENTORY_CANDIDATE_MANIFEST_SHA256,
        "completion_id": INVENTORY_COMPLETION_ID,
        "completion_manifest_sha256_state": ("pending_manifest_only_future_executable_preflight"),
        "input_binding_digest": INVENTORY_INPUT_BINDING_DIGEST,
        "inventory_contract_id": INVENTORY_CONTRACT_ID,
        "inventory_data_read_authorized": False,
        "inventory_reexecuted": False,
        "inventory_schema_digest": INVENTORY_SCHEMA_DIGEST,
        "inventory_v2_plan_id": (
            "57dcfe2cd7431105e0b664163a75e76a42a023e777055bad935b548f41935eb5"
        ),
        "inventory_v2_plan_sha256": (
            "b0d0a7987e75ed3ca366f4305d5d1260fc7b2b3b3ec6414b31ae1bcab29e4dc0"
        ),
        "lineage_role": "audit_origin_only_not_selection_or_execution_authority",
        "source_artifact_set_digest": INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    }
    assert plan.resource_caps == S7DirectionalRawPreviewPreparationCaps()
    assert plan.resource_caps.selected_asset_row_cap == 128
    assert plan.resource_caps.output_slot_row_cap == 11
    assert plan.plan_id == json.loads(plan.content)["plan_id"]
    assert plan.sha256 == hashlib.sha256(plan.content).hexdigest()
    assert S7DirectionalRawPreviewPreparationPlan.from_dict(json.loads(plan.content)) == plan


def test_plan_fails_closed_on_missing_files_scope_receipt_or_cap_drift() -> None:
    scope, receipt, plan = _plan()
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="runtime file set"):
        replace(plan, runtime_files=plan.runtime_files[1:])
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="verification file set"):
        replace(plan, verification_files=plan.verification_files[1:])
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="stored scope"):
        S7DirectionalRawPreviewPreparationPlan.create(
            created_by="planner",
            created_at_utc=CREATED,
            git_commit="a" * 40,
            git_tree="b" * 40,
            runtime_files=plan.runtime_files,
            verification_files=plan.verification_files,
            scope=scope,
            stored_scope=replace(receipt, sha256="0" * 64),
        )
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="caps changed"):
        replace(plan.resource_caps, selected_asset_row_cap=129)


def test_store_is_immutable_idempotent_and_rejects_tamper(tmp_path: Path) -> None:
    scope, _, plan = _plan()
    store = IdentityDirectionalRawPreviewPlanStore(tmp_path)
    first_scope = store.store_scope(scope)
    first_plan = store.store_plan(plan)
    mtimes = {
        path.relative_to(tmp_path).as_posix(): path.stat().st_mtime_ns
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert store.store_scope(scope) == first_scope
    assert store.store_plan(plan) == first_plan
    assert store.load_scope(scope.scope_set_id, expected_sha256=scope.sha256)[0] == scope
    assert store.load_plan(plan.plan_id, expected_sha256=plan.sha256)[0] == plan
    assert mtimes == {
        path.relative_to(tmp_path).as_posix(): path.stat().st_mtime_ns
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    plan_path = tmp_path / plan.relative_path
    plan_path.chmod(0o600)
    plan_path.write_text("tampered\n", encoding="utf-8")
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="missing or altered"):
        store.load_plan(plan.plan_id, expected_sha256=plan.sha256)


def test_parsers_reject_extra_keys_and_noncanonical_json() -> None:
    scope, _, plan = _plan()
    changed_scope = json.loads(scope.content)
    changed_scope["unexpected"] = True
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="exact eleven pairs"):
        S7DirectionalRawPreviewScopeSet.from_dict(changed_scope)

    changed_plan = json.loads(plan.content)
    changed_plan["capabilities"]["data_read"] = True
    with pytest.raises(IdentityDirectionalRawPreviewPlanError, match="canonical bytes"):
        S7DirectionalRawPreviewPreparationPlan.from_dict(changed_plan)


def test_plan_module_has_no_data_network_runner_or_approval_imports() -> None:
    import ast
    import inspect

    import ame_stocks_api.silver.identity_directional_raw_preview_plan as module

    source = inspect.getsource(module)
    imported: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert {item.split(".", 1)[0] for item in imported}.isdisjoint(
        {"boto3", "httpx", "pandas", "pyarrow", "requests", "socket"}
    )
    assert not any(
        fragment in item
        for item in imported
        for fragment in ("approval", "runner", "materialization", "registry_release")
    )
    assert "read_parquet" not in source
    assert "write_parquet" not in source


def test_frozen_s4_source_pins_cannot_be_mutated() -> None:
    with pytest.raises(TypeError):
        S4_SOURCE_PINS[0]["release_id"] = "0" * 64  # type: ignore[index]


def test_lightweight_s4_pins_equal_authoritative_identity_source_constants() -> None:
    from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
        S4_RELEASE_SET_ID,
        S4_RELEASE_SET_MANIFEST_SHA256,
        SIX_RELEASE_BINDING_ID,
    )
    from ame_stocks_api.silver.identity_source import (
        S7_S4_RELEASE_SET_ID,
        S7_S4_RELEASE_SET_MANIFEST_SHA256,
        S7_SIX_RELEASE_BINDING_ID,
        S7_SOURCE_PINS,
    )

    assert S4_RELEASE_SET_ID == S7_S4_RELEASE_SET_ID
    assert S4_RELEASE_SET_MANIFEST_SHA256 == S7_S4_RELEASE_SET_MANIFEST_SHA256
    assert SIX_RELEASE_BINDING_ID == S7_SIX_RELEASE_BINDING_ID
    for frozen in S4_SOURCE_PINS:
        authoritative = S7_SOURCE_PINS[str(frozen["table"])]
        assert dict(frozen) == {
            "artifact_count": authoritative.artifact_count,
            "build_id": authoritative.build_id,
            "evidence_only_s4": authoritative.evidence_only_s4,
            "release_id": authoritative.release_id,
            "release_manifest_sha256": authoritative.release_manifest_sha256,
            "row_count": authoritative.row_count,
            "table": authoritative.table,
        }
