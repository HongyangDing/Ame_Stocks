from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import ame_stocks_api.silver.identity_market_inventory_approval as module
from ame_stocks_api.silver.identity_market_inventory_approval import (
    IdentityMarketInventoryExecutionApprovalError,
    IdentityMarketInventoryExecutionApprovalStore,
    S7CompositeInventoryExecutionApprovalV2,
    record_s7_composite_inventory_execution_approval,
)
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    INVENTORY_CONTRACT_CANDIDATE_PATH,
    INVENTORY_CONTRACT_RESOURCE_PATH,
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
    S7InventoryCandidateContractPin,
    S7InventoryRuntimeFilePin,
    S7V1InventoryExecutionBlockedEvent,
)

RECORDED_AT = datetime(2026, 7, 17, 5, 0, tzinfo=UTC)
PLANNED_AT = RECORDED_AT + timedelta(minutes=1)
REQUESTED_AT = RECORDED_AT + timedelta(minutes=2)
APPROVED_AT = RECORDED_AT + timedelta(minutes=3)


def _pin(path: str) -> S7InventoryRuntimeFilePin:
    content = path.encode()
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


def _controls(
    execution_data_root: str = "/tmp/ame-stocks-inventory-test",
) -> tuple[
    S7V1InventoryExecutionBlockedEvent,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
]:
    blocked = S7V1InventoryExecutionBlockedEvent(
        recorded_by="fixture-v1-block-recorder",
        recorded_at_utc=RECORDED_AT,
    )
    blocked_receipt = module.StoredInventoryExecutionDocument(
        blocked.relative_path,
        blocked.sha256,
        len(blocked.content),
    )
    runtime_paths = set(REQUIRED_EXECUTION_RUNTIME_PATHS) | {
        INVENTORY_CONTRACT_CANDIDATE_PATH,
        INVENTORY_CONTRACT_RESOURCE_PATH,
    }
    plan = S7CompositeInventoryExecutionPlanV2.create(
        created_by="fixture-v2-planner",
        created_at_utc=PLANNED_AT,
        execution_git_commit="a" * 40,
        execution_git_tree="b" * 40,
        execution_data_root=execution_data_root,
        runtime_files=tuple(_pin(path) for path in sorted(runtime_paths)),
        verification_files=tuple(
            _pin(path) for path in sorted(REQUIRED_EXECUTION_VERIFICATION_PATHS)
        ),
        inventory_contract=_contract(),
        blocked_event=blocked,
        blocked_event_receipt=blocked_receipt,
    )
    plan_receipt = module.StoredInventoryExecutionDocument(
        plan.relative_path,
        plan.sha256,
        len(plan.content),
    )
    request = S7CompositeInventoryExecutionRequestV2.create(
        plan,
        plan_receipt,
        created_by="fixture-v2-requester",
        created_at_utc=REQUESTED_AT,
    )
    return blocked, plan, request


def _approval() -> tuple[
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
    S7CompositeInventoryExecutionApprovalV2,
]:
    _, plan, request = _controls()
    request_receipt = module.StoredInventoryExecutionDocument(
        request.relative_path,
        request.sha256,
        len(request.content),
    )
    approval = S7CompositeInventoryExecutionApprovalV2.create(
        request,
        request_receipt,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-human-reviewer",
        approved_at_utc=APPROVED_AT,
        approval_note="exact v2 test approval",
    )
    return plan, request, approval


def _prepare_store(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    IdentityMarketInventoryExecutionApprovalStore,
    S7CompositeInventoryExecutionPlanV2,
    S7CompositeInventoryExecutionRequestV2,
]:
    blocked, plan, request = _controls(str(root))
    monkeypatch.setattr(
        IdentityMarketInventoryExecutionPlanStore,
        "load_exact_v1_controls",
        lambda self: (
            object(),
            SimpleNamespace(created_at_utc=RECORDED_AT - timedelta(minutes=1)),
        ),
    )
    store = IdentityMarketInventoryExecutionApprovalStore(root)
    store.plan_store.store_blocked_event(blocked)
    store.plan_store.store_execution_plan_v2(plan)
    store.plan_store.store_execution_request_v2(request)
    return store, plan, request


def test_exact_literal_approval_round_trip_is_execution_only() -> None:
    plan, request, approval = _approval()

    assert approval.plan_id == plan.plan_id
    assert approval.request_event_id == request.request_event_id
    assert approval.approval_literal == request.canonical_approval_literal
    assert approval.execution_authorized is True
    assert approval.market_classification_authorized is False
    assert approval.adjudication_authorized is False
    assert approval.table_materialization_authorized is False
    assert approval.publication_authorized is False
    assert approval.network_access_authorized is False
    assert (
        S7CompositeInventoryExecutionApprovalV2.from_dict(json.loads(approval.content)) == approval
    )


def test_literal_time_and_binding_tampering_fail_closed() -> None:
    _, request, approval = _approval()
    receipt = module.StoredInventoryExecutionDocument(
        request.relative_path,
        request.sha256,
        len(request.content),
    )
    with pytest.raises(
        IdentityMarketInventoryExecutionApprovalError,
        match="literal differs",
    ):
        S7CompositeInventoryExecutionApprovalV2.create(
            request,
            receipt,
            approval_literal=request.canonical_approval_literal + " ",
            approved_by="reviewer",
            approved_at_utc=APPROVED_AT,
        )
    with pytest.raises(
        IdentityMarketInventoryExecutionApprovalError,
        match="strictly predate",
    ):
        S7CompositeInventoryExecutionApprovalV2.create(
            request,
            receipt,
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=request.created_at_utc,
        )
    with pytest.raises(
        IdentityMarketInventoryExecutionApprovalError,
        match="literal is not the exact",
    ):
        replace(approval, input_binding_digest="0" * 64)
    with pytest.raises(
        IdentityMarketInventoryExecutionApprovalError,
        match="cannot be in the future",
    ):
        S7CompositeInventoryExecutionApprovalV2.create(
            request,
            receipt,
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=datetime.now(UTC) + timedelta(days=1),
        )


def test_approval_store_is_immutable_idempotent_and_parent_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _, request = _prepare_store(tmp_path, monkeypatch)
    request_receipt = module.StoredInventoryExecutionDocument(
        request.relative_path,
        request.sha256,
        len(request.content),
    )
    approval = S7CompositeInventoryExecutionApprovalV2.create(
        request,
        request_receipt,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-reviewer",
        approved_at_utc=APPROVED_AT,
    )

    first = store.store_approval(approval)
    second = store.store_approval(approval)

    assert first == second
    assert store.load_approval(
        approval.approval_id,
        expected_sha256=approval.sha256,
    ) == (approval, first)
    assert not (tmp_path / "silver").exists()
    assert not any("candidate" in path.name for path in tmp_path.rglob("*.parquet"))


def test_one_literal_has_one_approval_slot_even_if_metadata_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _, request = _prepare_store(tmp_path, monkeypatch)
    receipt = module.StoredInventoryExecutionDocument(
        request.relative_path,
        request.sha256,
        len(request.content),
    )
    first = S7CompositeInventoryExecutionApprovalV2.create(
        request,
        receipt,
        approval_literal=request.canonical_approval_literal,
        approved_by="first-reviewer",
        approved_at_utc=APPROVED_AT,
    )
    replay = S7CompositeInventoryExecutionApprovalV2.create(
        request,
        receipt,
        approval_literal=request.canonical_approval_literal,
        approved_by="second-reviewer",
        approved_at_utc=APPROVED_AT + timedelta(minutes=1),
        approval_note="changed metadata must not mint a new grant",
    )

    assert replay.approval_id == first.approval_id
    assert replay.sha256 != first.sha256
    store.store_approval(first)
    with pytest.raises(
        IdentityMarketInventoryExecutionApprovalError,
        match="refusing to overwrite immutable artifact",
    ):
        store.store_approval(replay)


def test_record_function_needs_exact_ids_and_does_not_execute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, plan, request = _prepare_store(tmp_path, monkeypatch)

    result = record_s7_composite_inventory_execution_approval(
        tmp_path,
        plan_id=plan.plan_id,
        expected_plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        expected_request_event_sha256=request.sha256,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-reviewer",
        approved_at=APPROVED_AT.isoformat(),
    )

    assert result.approval.execution_authorized is True
    assert result.approval_document_preexisting is False
    assert result.approval_document.path.endswith("/manifest.json")
    assert not list(tmp_path.rglob("*.parquet"))
    with pytest.raises(IdentityMarketInventoryExecutionPlanError):
        record_s7_composite_inventory_execution_approval(
            tmp_path,
            plan_id=plan.plan_id,
            expected_plan_sha256="0" * 64,
            request_event_id=request.request_event_id,
            expected_request_event_sha256=request.sha256,
            approval_literal=request.canonical_approval_literal,
            approved_by="fixture-reviewer",
            approved_at=APPROVED_AT.isoformat(),
        )


def test_approval_module_has_no_parquet_network_or_execution_capability() -> None:
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
    assert "write_table" not in source
    assert "open_identity_source_bundle" not in source
    assert "run_s7_composite_inventory" not in source
