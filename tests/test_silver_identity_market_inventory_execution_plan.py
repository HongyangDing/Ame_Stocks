from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import ame_stocks_api.silver.identity_market_inventory_execution_plan as module
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    BLOCKED_EVENT_STATE,
    EXECUTION_AUTHORIZED_ACTION,
    INVENTORY_ALGORITHM_DIGEST,
    INVENTORY_ALGORITHM_RULE_VERSION,
    INVENTORY_CONTRACT_CANDIDATE_PATH,
    INVENTORY_CONTRACT_RESOURCE_PATH,
    INVENTORY_QA_SEMANTICS_DIGEST,
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
    V1_BLOCKED_REASONS,
    V1_BOUND_GIT_COMMIT,
    V1_BOUND_GIT_TREE,
    V1_LITERAL,
    V1_LITERAL_SHA256,
    V1_PLAN_ID,
    V1_PLAN_SHA256,
    V1_REQUEST_EVENT_ID,
    V1_REQUEST_EVENT_SHA256,
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
    S7InventoryCandidateContractPin,
    S7InventoryExecutionResourceCaps,
    S7InventoryRuntimeFilePin,
    S7V1InventoryControlLineage,
    S7V1InventoryExecutionBlockedEvent,
    StoredInventoryExecutionDocument,
    canonical_execution_paths,
    inventory_algorithm_spec,
    inventory_qa_semantics,
)

RECORDED_AT = datetime(2026, 7, 17, 5, 0, tzinfo=UTC)
PLANNED_AT = RECORDED_AT + timedelta(minutes=1)
REQUESTED_AT = RECORDED_AT + timedelta(minutes=2)
EXECUTION_COMMIT = "a" * 40
EXECUTION_TREE = "b" * 40


def _pin(path: str) -> S7InventoryRuntimeFilePin:
    content = path.encode("utf-8")
    return S7InventoryRuntimeFilePin(
        path=path,
        git_blob=hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _contract() -> S7InventoryCandidateContractPin:
    return S7InventoryCandidateContractPin(
        contract_id=COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
        schema_digest=COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
        candidate_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
        resource_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    )


def _blocked() -> S7V1InventoryExecutionBlockedEvent:
    return S7V1InventoryExecutionBlockedEvent(
        recorded_by="s7-v1-execution-block-recorder",
        recorded_at_utc=RECORDED_AT,
    )


def _runtime() -> tuple[S7InventoryRuntimeFilePin, ...]:
    paths = set(REQUIRED_EXECUTION_RUNTIME_PATHS) | {
        INVENTORY_CONTRACT_CANDIDATE_PATH,
        INVENTORY_CONTRACT_RESOURCE_PATH,
    }
    return tuple(_pin(path) for path in sorted(paths))


def _verification() -> tuple[S7InventoryRuntimeFilePin, ...]:
    return tuple(_pin(path) for path in sorted(REQUIRED_EXECUTION_VERIFICATION_PATHS))


def _plan(
    execution_data_root: str = "/mnt/HC_Volume_106309665/american_stocks",
) -> tuple[
    S7V1InventoryExecutionBlockedEvent,
    StoredInventoryExecutionDocument,
    S7CompositeInventoryExecutionPlanV2,
]:
    blocked = _blocked()
    receipt = StoredInventoryExecutionDocument(
        blocked.relative_path,
        blocked.sha256,
        len(blocked.content),
    )
    plan = S7CompositeInventoryExecutionPlanV2.create(
        created_by="s7-composite-inventory-v2-planner",
        created_at_utc=PLANNED_AT,
        execution_git_commit=EXECUTION_COMMIT,
        execution_git_tree=EXECUTION_TREE,
        execution_data_root=execution_data_root,
        runtime_files=_runtime(),
        verification_files=_verification(),
        inventory_contract=_contract(),
        blocked_event=blocked,
        blocked_event_receipt=receipt,
    )
    return blocked, receipt, plan


def _request(
    execution_data_root: str = "/mnt/HC_Volume_106309665/american_stocks",
) -> tuple[
    S7V1InventoryExecutionBlockedEvent,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
]:
    blocked, _, plan = _plan(execution_data_root)
    plan_receipt = StoredInventoryExecutionDocument(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7CompositeInventoryExecutionRequestV2.create(
        plan,
        plan_receipt,
        created_by="s7-composite-inventory-v2-requester",
        created_at_utc=REQUESTED_AT,
    )
    return blocked, plan, request


def test_v1_literal_is_preserved_but_blocked_event_has_no_execution_authority() -> None:
    lineage = S7V1InventoryControlLineage()
    event = _blocked()
    document = event.document

    assert lineage.plan_id == V1_PLAN_ID
    assert lineage.plan_sha256 == V1_PLAN_SHA256
    assert lineage.request_event_id == V1_REQUEST_EVENT_ID
    assert lineage.request_event_sha256 == V1_REQUEST_EVENT_SHA256
    assert lineage.bound_git_commit == V1_BOUND_GIT_COMMIT
    assert lineage.bound_git_tree == V1_BOUND_GIT_TREE
    assert lineage.to_dict()["literal"] == V1_LITERAL
    assert hashlib.sha256(V1_LITERAL.encode()).hexdigest() == V1_LITERAL_SHA256
    assert document["state"] == BLOCKED_EVENT_STATE
    assert tuple(document["blocking_reasons"]) == V1_BLOCKED_REASONS
    assert document["supersession_policy"] == {
        "new_exact_literal_required": True,
        "v1_controls_remain_immutable": True,
        "v1_literal_received_but_not_converted_to_approval": True,
    }
    assert set(document["capabilities"].values()) == {False}
    assert set(document["execution_facts"].values()) == {False}
    assert document["execution_facts"]["approval_receipt_created"] is False
    assert document["execution_facts"]["data_run_started"] is False
    assert S7V1InventoryExecutionBlockedEvent.from_dict(json.loads(event.content)) == event


def test_v2_plan_binds_executable_git_contract_algorithm_qa_paths_and_v1() -> None:
    _, _, plan = _plan()
    document = plan.document

    assert document["git_binding"]["execution_git_commit"] == EXECUTION_COMMIT
    assert document["git_binding"]["execution_git_tree"] == EXECUTION_TREE
    assert set(REQUIRED_EXECUTION_RUNTIME_PATHS).issubset(
        {item["path"] for item in document["git_binding"]["runtime_files"]}
    )
    assert set(REQUIRED_EXECUTION_VERIFICATION_PATHS).issubset(
        {item["path"] for item in document["verification_binding"]["verification_files"]}
    )
    assert document["candidate_contract"] == _contract().to_dict()
    assert document["algorithm"]["digest"] == INVENTORY_ALGORITHM_DIGEST
    assert document["algorithm"]["rule_version"] == INVENTORY_ALGORITHM_RULE_VERSION
    assert document["qa"]["semantics_digest"] == INVENTORY_QA_SEMANTICS_DIGEST
    assert document["canonical_paths"] == canonical_execution_paths()
    assert document["output_contract"]["contains_market_classification"] is False
    assert document["output_contract"]["contains_canonical_identity"] is False
    assert document["output_contract"]["contains_backtest_eligibility"] is False
    assert document["v1_lineage"]["v1_controls"]["literal_received"] is True
    assert set(document["capabilities"].values()) == {False}
    assert document["single_use_policy"] == {
        "existing_candidate_without_completion": "fail_closed",
        "existing_completion": "read_and_revalidate_without_rescan",
        "immutable_candidate_and_completion": True,
        "one_logical_run_per_plan_and_approval": True,
        "parallel_runner": "exclusive_nonblocking_plan_approval_lock",
        "stale_staging": "fail_closed_no_implicit_delete",
    }
    assert S7CompositeInventoryExecutionPlanV2.from_dict(json.loads(plan.content)) == plan


def test_algorithm_semantics_are_exact_asset_only_and_reproducible() -> None:
    spec = inventory_algorithm_spec()
    lineage = spec["lineage_digest"]

    assert spec["authority_table"] == "asset_observation_daily"
    assert spec["reconciliation_only_table"] == "universe_source_daily"
    assert spec["universe_rows_are_inventory_observations"] is False
    assert spec["parent_table_count"] == ("constant_one_asset_observation_daily_authority_only")
    assert spec["source_release_count"] == ("constant_one_asset_observation_daily_release_only")
    assert lineage["rule_version"] == "s7_composite_inventory_source_record_lineage_v1"
    assert lineage["record_order"] == "artifact_path_asc,row_group_asc,row_index_asc"
    assert lineage["update_per_authority_occurrence"] == (
        "hash_update_bytes_fromhex_source_record_id"
    )
    seed = json.dumps(
        lineage["seed"]["json"],
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert seed == (
        b'{"parent_table":"asset_observation_daily","release_id":"26819530e50cb92cbe0ec833d4b731b959c8bd2463ee2197255c02994241d44c",'
        b'"rule_version":"s7_composite_inventory_source_record_lineage_v1",'
        b'"scan_order":"artifact_path_asc,row_group_asc,row_index_asc"}'
    )
    reasons = spec["figi_reason_partition"]
    assert reasons["precedence"] == [
        "null",
        "empty",
        "whitespace_only",
        "surrounding_whitespace",
        "length_not_12",
        "non_upper_ascii_alnum",
        "prefix_not_BBG",
    ]
    assert spec["candidate_serialization"] == {
        "compression": "zstd",
        "compression_level": 9,
        "data_file_count": 1,
        "data_page_version": "2.0",
        "format": "parquet",
        "parquet_version": "2.6",
        "pyarrow_version": "25.0.0",
        "row_group_size": 100_000,
        "store_schema": True,
        "use_dictionary": False,
        "use_threads": False,
        "write_statistics": True,
    }
    assert stable_digest_for_test(spec) == INVENTORY_ALGORITHM_DIGEST


def stable_digest_for_test(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


def test_qa_semantics_are_the_exact_contract_rules_with_review_severities() -> None:
    qa = inventory_qa_semantics()
    by_id = {item["check_id"]: item for item in qa}

    assert len(qa) == 22
    assert by_id["schema_exact"]["severity"] == "critical"
    assert by_id["malformed_composite_rows"]["severity"] == "high"
    assert by_id["malformed_share_class_rows"]["severity"] == "high"
    assert by_id["share_class_conflict_groups"]["severity"] == "high"
    assert by_id["composite_figi_null_rows"]["severity"] == "medium"
    assert by_id["valid_composite_missing_share_class_rows"]["severity"] == "medium"
    assert stable_digest_for_test(list(qa)) == INVENTORY_QA_SEMANTICS_DIGEST


def test_v2_request_literal_binds_all_execution_digests_and_stops_for_approval() -> None:
    _, plan, request = _request()
    literal = json.loads(request.canonical_approval_literal)

    assert request.request_state == "awaiting_literal_human_approval"
    assert literal["authorized_action"] == EXECUTION_AUTHORIZED_ACTION
    assert literal["plan_id"] == plan.plan_id
    assert literal["plan_sha256"] == plan.sha256
    assert literal["request_event_id"] == request.request_event_id
    assert literal["request_event_sha256"] == request.sha256
    assert literal["runtime_file_set_digest"] == plan.runtime_file_set_digest
    assert literal["verification_file_set_digest"] == plan.verification_file_set_digest
    assert literal["inventory_contract_id"] == plan.inventory_contract.contract_id
    assert literal["inventory_schema_digest"] == plan.inventory_contract.schema_digest
    assert literal["algorithm_digest"] == INVENTORY_ALGORITHM_DIGEST
    assert literal["qa_semantics_digest"] == INVENTORY_QA_SEMANTICS_DIGEST
    assert S7CompositeInventoryExecutionRequestV2.from_dict(json.loads(request.content)) == request


def test_missing_runtime_verification_or_exact_contract_fails_closed() -> None:
    blocked, receipt, plan = _plan()
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="runtime file set misses"):
        replace(plan, runtime_files=plan.runtime_files[1:])
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="verification file set"):
        replace(plan, verification_files=plan.verification_files[1:])
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="contract identity"):
        S7InventoryCandidateContractPin(
            contract_id="0" * 64,
            schema_digest=COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
            candidate_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
            resource_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
        )
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="stored receipt differs"):
        S7CompositeInventoryExecutionPlanV2.create(
            created_by="planner",
            created_at_utc=PLANNED_AT,
            execution_git_commit=EXECUTION_COMMIT,
            execution_git_tree=EXECUTION_TREE,
            execution_data_root="/mnt/HC_Volume_106309665/american_stocks",
            runtime_files=_runtime(),
            verification_files=_verification(),
            inventory_contract=_contract(),
            blocked_event=blocked,
            blocked_event_receipt=replace(receipt, sha256="0" * 64),
        )
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="resource caps"):
        replace(S7InventoryExecutionResourceCaps(), worker_count=2)


def test_canonical_plan_and_request_tampering_is_rejected() -> None:
    _, plan, request = _request()
    plan_document = json.loads(plan.content)
    plan_document["algorithm"]["digest"] = "0" * 64
    with pytest.raises(
        IdentityMarketInventoryExecutionPlanError,
        match="does not reproduce canonical bytes",
    ):
        S7CompositeInventoryExecutionPlanV2.from_dict(plan_document)

    request_document = json.loads(request.content)
    request_document["runtime_file_set_digest"] = "0" * 64
    with pytest.raises(
        IdentityMarketInventoryExecutionPlanError,
        match="does not reproduce canonical bytes",
    ):
        S7CompositeInventoryExecutionRequestV2.from_dict(request_document)


def test_store_is_content_addressed_idempotent_and_has_no_approval_or_runner_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked, _, plan = _plan(str(tmp_path))
    _, _, request = _request(str(tmp_path))
    store = IdentityMarketInventoryExecutionPlanStore(tmp_path)
    monkeypatch.setattr(
        store,
        "load_exact_v1_controls",
        lambda: (
            object(),
            SimpleNamespace(created_at_utc=RECORDED_AT - timedelta(minutes=1)),
        ),
    )

    blocked_receipt = store.store_blocked_event(blocked)
    plan_receipt = store.store_execution_plan_v2(plan)
    request_receipt = store.store_execution_request_v2(request)

    assert store.store_blocked_event(blocked) == blocked_receipt
    assert store.store_execution_plan_v2(plan) == plan_receipt
    assert store.store_execution_request_v2(request) == request_receipt
    assert store.load_blocked_event(blocked.event_id, expected_sha256=blocked.sha256) == (
        blocked,
        blocked_receipt,
    )
    assert store.load_execution_plan_v2(plan.plan_id, expected_sha256=plan.sha256) == (
        plan,
        plan_receipt,
    )
    assert store.load_execution_request_v2(
        request.request_event_id, expected_sha256=request.sha256
    ) == (request, request_receipt)
    assert not hasattr(store, "store_approval")
    assert not hasattr(store, "run")
    assert not any("latest" in path.parts for path in tmp_path.rglob("*"))


def test_plan_cannot_be_replayed_under_another_data_root(tmp_path: Path) -> None:
    _, _, plan = _plan("/mnt/HC_Volume_106309665/american_stocks")
    store = IdentityMarketInventoryExecutionPlanStore(tmp_path)
    with pytest.raises(
        IdentityMarketInventoryExecutionPlanError,
        match="execution_data_root differs",
    ):
        store.store_execution_plan_v2(plan)


def test_load_exact_v1_controls_uses_only_exact_id_sha_and_literal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = SimpleNamespace(
        git_commit=V1_BOUND_GIT_COMMIT,
        input_binding_digest=module.V1_INPUT_BINDING_DIGEST,
        resource_caps=SimpleNamespace(digest=module.V1_RESOURCE_CAPS_DIGEST),
        plan_id=V1_PLAN_ID,
        sha256=V1_PLAN_SHA256,
    )
    request = SimpleNamespace(
        plan_id=V1_PLAN_ID,
        plan_sha256=V1_PLAN_SHA256,
        canonical_approval_literal=V1_LITERAL,
    )
    lineage = S7V1InventoryControlLineage()

    class FakeV1Store:
        def __init__(self, root: Path) -> None:
            assert root == tmp_path

        def load_plan(self, plan_id: str, *, expected_sha256: str):
            assert (plan_id, expected_sha256) == (V1_PLAN_ID, V1_PLAN_SHA256)
            return plan, SimpleNamespace(path=lineage.plan_path)

        def load_approval_request(self, request_id: str, *, expected_sha256: str):
            assert (request_id, expected_sha256) == (
                V1_REQUEST_EVENT_ID,
                V1_REQUEST_EVENT_SHA256,
            )
            return request, SimpleNamespace(path=lineage.request_path)

    monkeypatch.setattr(module, "IdentityMarketInventoryPlanStore", FakeV1Store)
    store = IdentityMarketInventoryExecutionPlanStore(tmp_path)
    assert store.load_exact_v1_controls() == (plan, request)

    request.canonical_approval_literal = V1_LITERAL + " "
    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="differ from literal"):
        store.load_exact_v1_controls()


def test_module_has_no_parquet_network_approval_or_runner_capability() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(
        {"boto3", "httpx", "pandas", "polars", "pyarrow", "requests", "socket"}
    )
    assert not hasattr(module, "S7CompositeInventoryExecutionApproval")
    assert not hasattr(module, "run_s7_composite_inventory")
    assert "write_table" not in source
    assert "ParquetFile" not in source
