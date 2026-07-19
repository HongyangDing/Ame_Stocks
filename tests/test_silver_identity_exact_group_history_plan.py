from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_exact_group_history_plan import (
    DEFAULT_RUNTIME_PATHS,
    DEFAULT_VERIFICATION_PATHS,
    FIXED_GROUPS,
    ExactGroupHistoryFilePin,
    ExactGroupHistoryPlanStore,
    IdentityExactGroupHistoryPlanError,
    S7ExactGroupHistoryExecutionCaps,
    S7ExactGroupHistoryPreparationPlan,
    S7ExactGroupHistoryPreparationRequest,
    S7ExactGroupHistoryScope,
    S7ExactGroupHistoryScopeSet,
    exact_group_lineage,
    fixed_scopes,
)

T0 = datetime(2026, 7, 19, 0, 0, tzinfo=UTC)
D = "a" * 64
G = "b" * 40


def _pin(path: str) -> ExactGroupHistoryFilePin:
    return ExactGroupHistoryFilePin(path, G, D, 1)


def _plan(scope: S7ExactGroupHistoryScopeSet) -> S7ExactGroupHistoryPreparationPlan:
    return S7ExactGroupHistoryPreparationPlan(
        created_by="plan_actor",
        created_at_utc=T0 + timedelta(seconds=1),
        git_commit=G,
        git_tree=G,
        runtime_files=tuple(_pin(path) for path in sorted(DEFAULT_RUNTIME_PATHS)),
        verification_files=tuple(_pin(path) for path in sorted(DEFAULT_VERIFICATION_PATHS)),
        scope_set_id=scope.scope_set_id,
        scope_set_sha256=scope.sha256,
        contract_id=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
        contract_candidate_sha256=(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256),
    )


def test_fixed_scope_is_exact_and_share_class_is_not_a_filter() -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    assert scope.groups == fixed_scopes()
    assert {item.ticker for item in scope.groups} == {"SOR", "XZO", "ANABV"}
    assert scope.document["share_class_is_not_a_filter"] is True
    assert all(not value for value in scope.document["capabilities"].values())
    assert scope.scope_set_id == stable_digest(scope.logical_payload())
    assert json.loads(scope.content)["scope_set_id"] == scope.scope_set_id


def test_scope_rejects_composite_or_provider_drift() -> None:
    item = dict(FIXED_GROUPS[0])
    item["provider"] = "other"
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="outside fixed scope"):
        S7ExactGroupHistoryScope.from_dict(item)


def test_execution_caps_are_fully_frozen() -> None:
    caps = S7ExactGroupHistoryExecutionCaps()
    assert caps.digest == stable_digest(caps.to_dict())
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="not exact"):
        S7ExactGroupHistoryExecutionCaps(tmp_bytes_hard_cap=caps.tmp_bytes_hard_cap + 1)
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="not exact"):
        S7ExactGroupHistoryExecutionCaps(wall_clock_seconds_hard_cap=1)


def test_preparation_plan_binds_lineage_contract_scope_files_and_caps() -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    plan = _plan(scope)
    assert plan.document["lineage"] == exact_group_lineage()
    assert plan.document["resource_caps_digest"] == plan.execution_resource_caps.digest
    assert plan.input_binding_digest == stable_digest(
        {
            "contract": {
                "candidate_sha256": (IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256),
                "contract_id": IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
                "fixed_scope_digest": EXACT_GROUP_HISTORY_FIXED_SCOPE_DIGEST,
                "observed_run_semantics_digest": (
                    EXACT_GROUP_HISTORY_OBSERVED_RUN_SEMANTICS_DIGEST
                ),
                "qa_semantics_digest": (
                    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_QA_SEMANTICS_DIGEST
                ),
                "schema_digest": IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
            },
            "lineage": exact_group_lineage(),
            "resource_caps_digest": plan.execution_resource_caps.digest,
            "scope_set_id": scope.scope_set_id,
            "scope_set_sha256": scope.sha256,
        }
    )
    assert S7ExactGroupHistoryPreparationPlan.from_dict(plan.document) == plan


@pytest.mark.parametrize(
    ("binding", "required_path"),
    [("runtime", path) for path in sorted(DEFAULT_RUNTIME_PATHS)]
    + [("verification", path) for path in sorted(DEFAULT_VERIFICATION_PATHS)],
)
def test_preparation_plan_rejects_each_missing_required_pin(
    binding: str, required_path: str
) -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    document = json.loads(_plan(scope).content)
    container = (
        document["git_binding"] if binding == "runtime" else document["verification_binding"]
    )
    key = "runtime_files" if binding == "runtime" else "verification_files"
    container[key] = [item for item in container[key] if item["path"] != required_path]
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="required file pins"):
        S7ExactGroupHistoryPreparationPlan.from_dict(document)


def test_request_requires_separate_later_actor_and_binds_caps() -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    plan = _plan(scope)
    request = S7ExactGroupHistoryPreparationRequest.create(
        plan,
        created_by="request_actor",
        created_at_utc=T0 + timedelta(seconds=2),
    )
    assert request.resource_caps_digest == plan.execution_resource_caps.digest
    assert json.loads(request.canonical_approval_literal)["request_event_sha256"] == request.sha256
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="actor/time"):
        S7ExactGroupHistoryPreparationRequest.create(
            plan,
            created_by="plan_actor",
            created_at_utc=T0 + timedelta(seconds=2),
        )


def test_store_is_content_addressed_idempotent_and_rejects_tamper(tmp_path) -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    store = ExactGroupHistoryPlanStore(tmp_path)
    receipt = store.store_scope(scope)
    assert store.store_scope(scope) == receipt
    assert store.load_scope(scope.scope_set_id, scope.sha256) == scope
    path = tmp_path / scope.relative_path
    path.chmod(0o600)
    path.write_bytes(b"{}\n")
    with pytest.raises(IdentityExactGroupHistoryPlanError, match="differs"):
        store.load_scope(scope.scope_set_id, scope.sha256)


def test_store_rejects_symlink_target(tmp_path) -> None:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    path = tmp_path / scope.relative_path
    path.parent.mkdir(parents=True)
    target = tmp_path / "target.json"
    target.write_bytes(scope.content)
    path.symlink_to(target)
    with pytest.raises(IdentityExactGroupHistoryPlanError):
        ExactGroupHistoryPlanStore(tmp_path).store_scope(scope)
