from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import identity_exact_group_history_approval as approval_module
from ame_stocks_api.silver.identity_exact_group_history_approval import (
    EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION,
    ExactGroupHistoryExecutionApprovalStore,
    IdentityExactGroupHistoryApprovalError,
    S7ExactGroupHistoryExecutionApproval,
)


def _controls() -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    now = datetime.now(UTC) - timedelta(minutes=2)
    source = "1" * 64
    normalized = "2" * 64
    caps = SimpleNamespace(digest="3" * 64)
    plan = SimpleNamespace(
        plan_id="4" * 64,
        sha256="5" * 64,
        relative_path="plans/plan.json",
        execution_data_root="/tmp/data",
        source_binding_id="6" * 64,
        source_binding_sha256="7" * 64,
        source_artifact_set_digest=source,
        raw_source_artifact_set_digest=source,
        inventory_projection_set_digest="8" * 64,
        normalized_source_artifact_set_digest=normalized,
        input_binding_digest="9" * 64,
        manifest_plan_id="a" * 64,
        manifest_plan_sha256="b" * 64,
        manifest_approval_id="c" * 64,
        manifest_approval_sha256="d" * 64,
        execution_resource_caps=caps,
        created_by="plan-actor",
        source_binding_created_by="source-actor",
        document={"authorized_action": EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION},
    )
    request_payload = {
        "authorized_action": EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION,
        "plan_id": plan.plan_id,
    }
    literal = json.dumps(request_payload, separators=(",", ":"), sort_keys=True)
    request = SimpleNamespace(
        request_event_id="e" * 64,
        sha256="f" * 64,
        relative_path="requests/request.json",
        content=b"request\n",
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        input_binding_digest=plan.input_binding_digest,
        manifest_plan_id=plan.manifest_plan_id,
        manifest_plan_sha256=plan.manifest_plan_sha256,
        manifest_approval_id=plan.manifest_approval_id,
        manifest_approval_sha256=plan.manifest_approval_sha256,
        source_binding_id=plan.source_binding_id,
        source_binding_sha256=plan.source_binding_sha256,
        raw_source_artifact_set_digest=plan.raw_source_artifact_set_digest,
        inventory_projection_set_digest=plan.inventory_projection_set_digest,
        normalized_source_artifact_set_digest=(plan.normalized_source_artifact_set_digest),
        resource_caps_digest=caps.digest,
        canonical_approval_literal=literal,
        created_at_utc=now,
        created_by="request-actor",
        document={"authorized_action": EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION},
    )
    receipt = SimpleNamespace(
        path=request.relative_path,
        sha256=request.sha256,
        bytes=len(request.content),
    )
    return plan, request, receipt


def test_execution_approval_round_trips_exact_literal() -> None:
    plan, request, receipt = _controls()
    approval = S7ExactGroupHistoryExecutionApproval.create(
        request,
        receipt,
        plan=plan,
        approval_literal=request.canonical_approval_literal,
        approved_by="human-reviewer",
        approved_at_utc=request.created_at_utc + timedelta(minutes=1),
    )
    assert approval.authorized_action == EXACT_GROUP_HISTORY_EXECUTION_AUTHORIZED_ACTION
    assert approval.share_class_filter_authorized is False
    assert approval.full_run_authorized is False
    assert S7ExactGroupHistoryExecutionApproval.from_dict(json.loads(approval.content)) == approval
    assert hashlib.sha256(approval.content).hexdigest() == approval.sha256
    assert stable_digest(approval.approval_slot_payload()) == approval.approval_id


def test_execution_approval_rejects_cross_source_request_projection() -> None:
    plan, request, receipt = _controls()
    request.source_binding_id = "0" * 64
    with pytest.raises(IdentityExactGroupHistoryApprovalError, match="crosses"):
        S7ExactGroupHistoryExecutionApproval.create(
            request,
            receipt,
            plan=plan,
            approval_literal=request.canonical_approval_literal,
            approved_by="human-reviewer",
            approved_at_utc=request.created_at_utc + timedelta(minutes=1),
        )


def test_execution_approval_rejects_resource_caps_projection_tamper() -> None:
    plan, request, receipt = _controls()
    request.resource_caps_digest = "0" * 64
    with pytest.raises(IdentityExactGroupHistoryApprovalError, match="crosses"):
        S7ExactGroupHistoryExecutionApproval.create(
            request,
            receipt,
            plan=plan,
            approval_literal=request.canonical_approval_literal,
            approved_by="human-reviewer",
            approved_at_utc=request.created_at_utc + timedelta(minutes=1),
        )


def test_same_request_has_one_fixed_approval_slot(monkeypatch, tmp_path) -> None:
    plan, request, receipt = _controls()
    first = S7ExactGroupHistoryExecutionApproval.create(
        request,
        receipt,
        plan=plan,
        approval_literal=request.canonical_approval_literal,
        approved_by="human-reviewer",
        approved_at_utc=request.created_at_utc + timedelta(seconds=30),
        approval_note="first receipt",
    )
    second = S7ExactGroupHistoryExecutionApproval.create(
        request,
        receipt,
        plan=plan,
        approval_literal=request.canonical_approval_literal,
        approved_by="second-human-reviewer",
        approved_at_utc=request.created_at_utc + timedelta(seconds=60),
        approval_note="different metadata cannot create another lane",
    )
    assert first.approval_id == second.approval_id
    assert first.sha256 != second.sha256
    monkeypatch.setattr(
        approval_module,
        "_load_manifest_controls",
        lambda *args, **kwargs: (plan, request, receipt),
    )
    monkeypatch.setattr(approval_module, "_verify_bindings", lambda *args: None)
    store = ExactGroupHistoryExecutionApprovalStore(tmp_path)
    first_receipt = store.store_approval(first)
    assert store.store_approval(first) == first_receipt
    with pytest.raises(IdentityExactGroupHistoryApprovalError, match="immutable"):
        store.store_approval(second)
