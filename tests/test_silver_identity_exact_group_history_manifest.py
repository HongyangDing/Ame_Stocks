from __future__ import annotations

import json
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import identity_exact_group_history_manifest as m
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_exact_group_history_plan import (
    DEFAULT_RUNTIME_PATHS,
    DEFAULT_VERIFICATION_PATHS,
    DIRECTIONAL_CANDIDATE_ID,
    DIRECTIONAL_COMPLETION_ID,
    INVENTORY_COMPLETION_ID,
    ExactGroupHistoryFilePin,
    S7ExactGroupHistoryExecutionCaps,
    S7ExactGroupHistoryPreparationPlan,
    S7ExactGroupHistoryPreparationRequest,
    S7ExactGroupHistoryScopeSet,
)

T0 = datetime(2026, 7, 19, 0, 0, tzinfo=UTC)
D = "a" * 64
G = "b" * 40


def _pin(path: str) -> ExactGroupHistoryFilePin:
    return ExactGroupHistoryFilePin(path, G, D, 1)


def _manifest_plan() -> m.S7ExactGroupHistoryManifestPlan:
    return m.S7ExactGroupHistoryManifestPlan(
        created_by="manifest_plan_actor",
        created_at_utc=T0,
        execution_data_root=m.EXECUTION_DATA_ROOT,
        git_commit=G,
        git_tree=G,
        runtime_files=tuple(_pin(path) for path in sorted(DEFAULT_RUNTIME_PATHS)),
        verification_files=tuple(_pin(path) for path in sorted(DEFAULT_VERIFICATION_PATHS)),
        scope_set_id=D,
        scope_set_sha256=D,
        contract_id=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
        contract_candidate_sha256=(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256),
        execution_resource_caps_digest=S7ExactGroupHistoryExecutionCaps().digest,
        preparation_authorization_id=D,
        preparation_authorization_sha256=D,
        preparation_authorization_path=(
            "manifests/silver/identity/exact-group-history-preparation-authorizations/"
            f"authorization_id={D}/manifest.json"
        ),
        preparation_plan_id=D,
        preparation_plan_sha256=D,
        preparation_request_event_id=D,
        preparation_request_event_sha256=D,
        manifest_inputs=m.canonical_manifest_inputs(
            inventory_completion_path=(
                "manifests/silver/identity/composite-inventory-execution-completions/"
                f"plan_id={D}/approval_id={D}/manifest.json"
            ),
            directional_completion_path=(
                "manifests/silver/identity/directional-raw-preview-execution-completions/"
                f"plan_id={D}/approval_id={D}/manifest.json"
            ),
        ),
        future_manifest_reader_actor="reader_actor",
        future_execution_plan_actor="execution_plan_actor",
        future_execution_request_actor="execution_request_actor",
    )


def _request_approval(
    plan: m.S7ExactGroupHistoryManifestPlan,
) -> tuple[m.S7ExactGroupHistoryManifestRequest, m.S7ExactGroupHistoryManifestApproval]:
    request = m.S7ExactGroupHistoryManifestRequest.create(
        plan, created_by="manifest_request_actor", created_at_utc=T0 + timedelta(seconds=1)
    )
    approval = m.S7ExactGroupHistoryManifestApproval(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        request_event_sha256=request.sha256,
        approval_literal=request.canonical_approval_literal,
        approved_by="manifest_approver",
        approved_at_utc=T0 + timedelta(seconds=2),
    )
    return request, approval


def _exact_control_chain(
    *,
    future_manifest_reader_actor: str = "reader_actor",
    manifest_plan_created_at_utc: datetime = T0 + timedelta(seconds=4),
) -> tuple[
    S7ExactGroupHistoryScopeSet,
    S7ExactGroupHistoryPreparationPlan,
    S7ExactGroupHistoryPreparationRequest,
    m.S7ExactGroupHistoryPreparationAuthorization,
    m.S7ExactGroupHistoryManifestPlan,
    m.S7ExactGroupHistoryManifestRequest,
    m.S7ExactGroupHistoryManifestApproval,
]:
    scope = S7ExactGroupHistoryScopeSet("scope_actor", T0)
    preparation_plan = S7ExactGroupHistoryPreparationPlan(
        created_by="preparation_plan_actor",
        created_at_utc=T0 + timedelta(seconds=1),
        git_commit=G,
        git_tree=G,
        runtime_files=tuple(_pin(path) for path in sorted(DEFAULT_RUNTIME_PATHS)),
        verification_files=tuple(_pin(path) for path in sorted(DEFAULT_VERIFICATION_PATHS)),
        scope_set_id=scope.scope_set_id,
        scope_set_sha256=scope.sha256,
        contract_id=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
        contract_schema_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
        contract_candidate_sha256=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    )
    preparation_request = S7ExactGroupHistoryPreparationRequest.create(
        preparation_plan,
        created_by="preparation_request_actor",
        created_at_utc=T0 + timedelta(seconds=2),
    )
    authorization = m.S7ExactGroupHistoryPreparationAuthorization(
        plan_id=preparation_plan.plan_id,
        plan_sha256=preparation_plan.sha256,
        request_event_id=preparation_request.request_event_id,
        request_event_sha256=preparation_request.sha256,
        approval_literal=preparation_request.canonical_approval_literal,
        approved_by="preparation_approver",
        approved_at_utc=T0 + timedelta(seconds=3),
    )
    manifest_plan = m.S7ExactGroupHistoryManifestPlan(
        created_by="manifest_plan_actor",
        created_at_utc=manifest_plan_created_at_utc,
        execution_data_root=m.EXECUTION_DATA_ROOT,
        git_commit=preparation_plan.git_commit,
        git_tree=preparation_plan.git_tree,
        runtime_files=preparation_plan.runtime_files,
        verification_files=preparation_plan.verification_files,
        scope_set_id=scope.scope_set_id,
        scope_set_sha256=scope.sha256,
        contract_id=preparation_plan.contract_id,
        contract_schema_digest=preparation_plan.contract_schema_digest,
        contract_candidate_sha256=preparation_plan.contract_candidate_sha256,
        execution_resource_caps_digest=preparation_plan.execution_resource_caps.digest,
        preparation_authorization_id=authorization.authorization_id,
        preparation_authorization_sha256=authorization.sha256,
        preparation_authorization_path=authorization.relative_path,
        preparation_plan_id=preparation_plan.plan_id,
        preparation_plan_sha256=preparation_plan.sha256,
        preparation_request_event_id=preparation_request.request_event_id,
        preparation_request_event_sha256=preparation_request.sha256,
        manifest_inputs=m.canonical_manifest_inputs(
            inventory_completion_path=(
                "manifests/silver/identity/composite-inventory-execution-completions/"
                f"plan_id={D}/approval_id={D}/manifest.json"
            ),
            directional_completion_path=(
                "manifests/silver/identity/directional-raw-preview-execution-completions/"
                f"plan_id={D}/approval_id={D}/manifest.json"
            ),
        ),
        future_manifest_reader_actor=future_manifest_reader_actor,
        future_execution_plan_actor="execution_plan_actor",
        future_execution_request_actor="execution_request_actor",
    )
    manifest_request = m.S7ExactGroupHistoryManifestRequest.create(
        manifest_plan,
        created_by="manifest_request_actor",
        created_at_utc=T0 + timedelta(seconds=5),
    )
    manifest_approval = m.S7ExactGroupHistoryManifestApproval(
        plan_id=manifest_plan.plan_id,
        plan_sha256=manifest_plan.sha256,
        request_event_id=manifest_request.request_event_id,
        request_event_sha256=manifest_request.sha256,
        approval_literal=manifest_request.canonical_approval_literal,
        approved_by="manifest_approver",
        approved_at_utc=T0 + timedelta(seconds=6),
    )
    return (
        scope,
        preparation_plan,
        preparation_request,
        authorization,
        manifest_plan,
        manifest_request,
        manifest_approval,
    )


@pytest.fixture(scope="module")
def synthetic_sources() -> tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...]:
    sessions = tuple(m.START_SESSION + timedelta(days=index) for index in range(m.SESSION_COUNT))
    rows_base, rows_extra = divmod(m.SOURCE_ROW_COUNT, m.SOURCE_ARTIFACT_COUNT)
    bytes_base, bytes_extra = divmod(m.SOURCE_BYTES, m.SOURCE_ARTIFACT_COUNT)
    values = []
    index = 0
    for table, contract, release_id, release_sha in (
        (
            "asset_observation_daily",
            ASSET_OBSERVATION_DAILY_CONTRACT,
            m.ASSET_RELEASE_ID,
            m.ASSET_RELEASE_SHA256,
        ),
        (
            "universe_source_daily",
            UNIVERSE_SOURCE_DAILY_CONTRACT,
            m.UNIVERSE_RELEASE_ID,
            m.UNIVERSE_RELEASE_SHA256,
        ),
    ):
        for session in sessions:
            row_count = rows_base + (index < rows_extra)
            byte_count = bytes_base + (index < bytes_extra)
            values.append(
                m.S7ExactGroupHistoryRawSourceArtifactRef(
                    table=table,
                    session_date=session,
                    release_id=release_id,
                    release_manifest_sha256=release_sha,
                    source_contract_id=contract.contract_id,
                    source_schema_digest=contract.schema_digest,
                    path=f"silver/{table}/session_date={session.isoformat()}/part.parquet",
                    sha256=f"{index:064x}",
                    bytes=byte_count,
                    row_count=row_count,
                    disk_size_bytes=byte_count,
                )
            )
            index += 1
    return tuple(sorted(values))


def _manifest_refs(
    plan: m.S7ExactGroupHistoryManifestPlan,
) -> tuple[m.S7ExactGroupHistoryManifestDocumentRef, ...]:
    return tuple(
        m.S7ExactGroupHistoryManifestDocumentRef(
            item.kind, item.logical_id, item.path, item.sha256, 10
        )
        for item in plan.manifest_inputs
    )


def _empty_operation_counts() -> dict[str, int]:
    return {
        "json_reads": 0,
        "lstats": 0,
        "parquet_bytes": 0,
        "json_bytes": 0,
        "source_lstat_bytes": 0,
        "source_rows": 0,
    }


def _binding(
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
    plan: m.S7ExactGroupHistoryManifestPlan,
    request: m.S7ExactGroupHistoryManifestRequest,
    approval: m.S7ExactGroupHistoryManifestApproval,
) -> tuple[m.S7ExactGroupHistoryManifestRunIntent, m.S7ExactGroupHistorySourceBinding]:
    inventory_digest = stable_digest([item.project_inventory_8() for item in synthetic_sources])
    monkeypatch.setattr(m, "INVENTORY_SOURCE_ARTIFACT_SET_DIGEST", inventory_digest)
    intent = m.S7ExactGroupHistoryManifestRunIntent(
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_request_event_id=request.request_event_id,
        manifest_request_event_sha256=request.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        approval_literal_sha256=approval.approval_literal_sha256,
        input_binding_digest=plan.input_binding_digest,
        execution_data_root=plan.execution_data_root,
        source_binding_created_by=plan.future_manifest_reader_actor,
        source_binding_created_at_utc=T0 + timedelta(seconds=3),
        execution_plan_created_by=plan.future_execution_plan_actor,
        execution_request_created_by=plan.future_execution_request_actor,
    )
    binding = m.S7ExactGroupHistorySourceBinding(
        created_by=intent.source_binding_created_by,
        created_at_utc=intent.source_binding_created_at_utc,
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_request_event_id=request.request_event_id,
        manifest_request_event_sha256=request.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        manifest_literal_sha256=approval.approval_literal_sha256,
        run_intent_id=intent.intent_id,
        run_intent_path=intent.relative_path,
        run_intent_sha256=intent.sha256,
        source_artifacts=synthetic_sources,
        execution_source_pins=m.normalize_raw_sources(synthetic_sources),
        manifest_documents=_manifest_refs(plan),
    )
    return intent, binding


def _materialize_synthetic_completion(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> tuple[
    m.S7ExactGroupHistoryManifestPlan,
    m.S7ExactGroupHistoryManifestRequest,
    m.S7ExactGroupHistoryManifestApproval,
    m.ExactGroupHistoryManifestStore,
    m.S7ExactGroupHistoryManifestRun,
]:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    inventory_digest = stable_digest([item.project_inventory_8() for item in synthetic_sources])
    monkeypatch.setattr(m, "INVENTORY_SOURCE_ARTIFACT_SET_DIGEST", inventory_digest)

    def fake_read(_root, _plan, counts, _started):
        counts.update(
            {
                "json_reads": m.EXPECTED_MANIFEST_INPUT_COUNT,
                "lstats": m.SOURCE_ARTIFACT_COUNT,
                "parquet_bytes": 0,
                "json_bytes": 1,
                "source_lstat_bytes": m.SOURCE_BYTES,
                "source_rows": m.SOURCE_ROW_COUNT,
            }
        )
        return _manifest_refs(plan), synthetic_sources

    monkeypatch.setattr(m, "_read_and_bind_sources", fake_read)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    result = m._run_manifest_preflight_locked(
        root=tmp_path,
        store=store,
        plan=plan,
        request=request,
        approval=approval,
        source_time=T0 + timedelta(seconds=3),
    )
    return plan, request, approval, store, result


def test_manifest_caps_are_fully_frozen() -> None:
    caps = m.S7ExactGroupHistoryManifestCaps()
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="not exact"):
        m.S7ExactGroupHistoryManifestCaps(json_manifest_bytes_hard_cap=1)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="not exact"):
        m.S7ExactGroupHistoryManifestCaps(wall_clock_seconds_hard_cap=1)
    assert caps.lstat_hard_cap == m.SOURCE_ARTIFACT_COUNT


def test_source_json_fstat_caps_fail_before_any_read(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"{}\n"
    path = tmp_path / "source.json"
    path.write_bytes(content)
    pin = m.S7ExactGroupHistoryManifestInputPin(
        "asset_release_manifest",
        D,
        "source.json",
        m.hashlib.sha256(content).hexdigest(),
    )
    plan = _manifest_plan()
    real_info = path.stat()
    oversized = SimpleNamespace(
        st_mode=real_info.st_mode,
        st_size=plan.resource_caps.json_manifest_bytes_hard_cap + 1,
        st_dev=real_info.st_dev,
        st_ino=real_info.st_ino,
        st_mtime_ns=real_info.st_mtime_ns,
    )
    monkeypatch.setattr(m.os, "fstat", lambda _descriptor: oversized)
    read_calls = 0

    def forbidden_read(*_args):
        nonlocal read_calls
        read_calls += 1
        raise AssertionError("read must not happen")

    monkeypatch.setattr(m.os, "read", forbidden_read)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="before read"):
        m._read_exact_json(tmp_path, pin, _empty_operation_counts(), plan, time.monotonic())
    assert read_calls == 0


def test_source_json_cumulative_cap_fails_before_read(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"{}\n"
    (tmp_path / "source.json").write_bytes(content)
    pin = m.S7ExactGroupHistoryManifestInputPin(
        "asset_release_manifest",
        D,
        "source.json",
        m.hashlib.sha256(content).hexdigest(),
    )
    plan = _manifest_plan()
    counts = _empty_operation_counts()
    counts["json_bytes"] = plan.resource_caps.json_manifest_bytes_hard_cap - 1
    monkeypatch.setattr(
        m.os,
        "read",
        lambda *_args: pytest.fail("read attempted after cumulative cap"),
    )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="before read"):
        m._read_exact_json(tmp_path, pin, counts, plan, time.monotonic())


def test_json_and_lstat_paths_continuously_check_live_caps(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"{}\n"
    (tmp_path / "source.json").write_bytes(content)
    pin = m.S7ExactGroupHistoryManifestInputPin(
        "asset_release_manifest",
        D,
        "source.json",
        m.hashlib.sha256(content).hexdigest(),
    )
    plan = _manifest_plan()
    counts = _empty_operation_counts()
    calls = 0
    original = m._check_live_caps

    def observed(*args):
        nonlocal calls
        calls += 1
        original(*args)

    monkeypatch.setattr(m, "_check_live_caps", observed)
    parsed_content, document = m._read_exact_json(tmp_path, pin, counts, plan, time.monotonic())
    assert parsed_content == content
    assert document == {}
    assert calls >= 5

    lstat_calls = 0

    def forbidden_lstat(*_args):
        nonlocal lstat_calls
        lstat_calls += 1
        raise AssertionError("expired work must fail before lstat")

    monkeypatch.setattr(m, "_safe_lstat", forbidden_lstat)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="resource cap"):
        m._checked_source_lstat(
            tmp_path,
            "never.parquet",
            expected_row_count=1,
            plan=plan,
            counts=_empty_operation_counts(),
            started=time.monotonic() - plan.resource_caps.wall_clock_seconds_hard_cap - 1,
        )
    assert lstat_calls == 0


def test_manifest_request_literal_and_approval_are_exact() -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    literal = json.loads(request.canonical_approval_literal)
    assert literal["request_event_sha256"] == request.sha256
    assert literal["parquet_content_read"] is False
    assert (
        approval.approval_literal_sha256
        == m.hashlib.sha256(request.canonical_approval_literal.encode()).hexdigest()
    )
    assert m.S7ExactGroupHistoryManifestPlan.from_dict(plan.document) == plan
    assert m.S7ExactGroupHistoryManifestRequest.from_dict(request.document) == request
    assert m.S7ExactGroupHistoryManifestApproval.from_dict(approval.document) == approval


def test_fixed_approval_slots_reject_multi_lane_receipts(tmp_path) -> None:
    chain = _exact_control_chain()
    authorization = chain[3]
    alternate_authorization = replace(
        authorization,
        approved_by="alternate_preparation_reviewer",
        approved_at_utc=authorization.approved_at_utc + timedelta(microseconds=1),
        review_note="alternate preparation note",
    )
    assert alternate_authorization.authorization_id == authorization.authorization_id
    assert {"approved_by", "approved_at_utc", "review_note"}.isdisjoint(
        authorization.slot_payload()
    )
    assert alternate_authorization.relative_path == authorization.relative_path
    assert alternate_authorization.content != authorization.content
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    authorization_receipt = store.store_preparation_authorization(authorization)
    assert store.store_preparation_authorization(authorization) == authorization_receipt
    with pytest.raises(m.IdentityExactGroupHistoryManifestError):
        store.store_preparation_authorization(alternate_authorization)

    approval = chain[6]
    alternate_approval = replace(
        approval,
        approved_by="alternate_manifest_reviewer",
        approved_at_utc=approval.approved_at_utc + timedelta(microseconds=1),
        review_note="alternate manifest note",
    )
    assert alternate_approval.approval_id == approval.approval_id
    assert {"approved_by", "approved_at_utc", "review_note"}.isdisjoint(approval.slot_payload())
    assert alternate_approval.relative_path == approval.relative_path
    assert alternate_approval.content != approval.content
    approval_receipt = store.store_manifest_approval(approval)
    assert store.store_manifest_approval(approval) == approval_receipt
    with pytest.raises(m.IdentityExactGroupHistoryManifestError):
        store.store_manifest_approval(alternate_approval)


@pytest.mark.parametrize(
    ("binding", "required_path"),
    [("runtime", path) for path in sorted(DEFAULT_RUNTIME_PATHS)]
    + [("verification", path) for path in sorted(DEFAULT_VERIFICATION_PATHS)],
)
def test_manifest_plan_rejects_each_missing_required_pin(binding: str, required_path: str) -> None:
    document = json.loads(_manifest_plan().content)
    container = (
        document["git_binding"] if binding == "runtime" else document["verification_binding"]
    )
    key = "runtime_files" if binding == "runtime" else "verification_files"
    container[key] = [item for item in container[key] if item["path"] != required_path]
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="required manifest"):
        m.S7ExactGroupHistoryManifestPlan.from_dict(document)


def test_central_cross_binding_verifier_covers_all_projections() -> None:
    chain = _exact_control_chain()
    m.verify_exact_group_history_cross_bindings(
        scope=chain[0],
        preparation_plan=chain[1],
        preparation_request=chain[2],
        preparation_authorization=chain[3],
        manifest_plan=chain[4],
        manifest_request=chain[5],
        manifest_approval=chain[6],
    )
    bad_preparation_request = replace(chain[2], input_binding_digest=D)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="cross-binding"):
        m.verify_exact_group_history_cross_bindings(
            scope=chain[0],
            preparation_plan=chain[1],
            preparation_request=bad_preparation_request,
            preparation_authorization=chain[3],
            manifest_plan=chain[4],
            manifest_request=chain[5],
            manifest_approval=chain[6],
        )
    bad_manifest_request = replace(chain[5], future_execution_plan_actor="drift_actor")
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="cross-binding"):
        m.verify_exact_group_history_cross_bindings(
            scope=chain[0],
            preparation_plan=chain[1],
            preparation_request=chain[2],
            preparation_authorization=chain[3],
            manifest_plan=chain[4],
            manifest_request=bad_manifest_request,
            manifest_approval=chain[6],
        )


def test_central_cross_binding_verifier_requires_global_actor_and_time_separation() -> None:
    actor_chain = _exact_control_chain(future_manifest_reader_actor="scope_actor")
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="globally unique"):
        m.verify_exact_group_history_cross_bindings(
            scope=actor_chain[0],
            preparation_plan=actor_chain[1],
            preparation_request=actor_chain[2],
            preparation_authorization=actor_chain[3],
            manifest_plan=actor_chain[4],
            manifest_request=actor_chain[5],
            manifest_approval=actor_chain[6],
        )
    time_chain = _exact_control_chain(manifest_plan_created_at_utc=T0 + timedelta(seconds=3))
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="strictly increase"):
        m.verify_exact_group_history_cross_bindings(
            scope=time_chain[0],
            preparation_plan=time_chain[1],
            preparation_request=time_chain[2],
            preparation_authorization=time_chain[3],
            manifest_plan=time_chain[4],
            manifest_request=time_chain[5],
            manifest_approval=time_chain[6],
        )


def test_raw16_normalized10_and_projection_domains_are_independent(
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    _intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    assert binding.inventory_projection_set_digest == m.INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
    assert binding.execution_source_pins == m.normalize_raw_sources(binding.source_artifacts)
    assert binding.raw_source_artifact_set_digest != (binding.normalized_source_artifact_set_digest)
    assert len(binding.source_artifacts[0].to_dict()) == 16
    assert len(binding.execution_source_pins[0].to_dict()) == 10


def test_normalized_execution_pin_tamper_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    pins = list(binding.execution_source_pins)
    changed = pins[0].to_dict()
    changed["row_count"] = int(changed["row_count"]) + 1
    pins[0] = m.S7ExactGroupHistoryExecutionSourcePin.from_dict(changed)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match=r"normalize\(raw\)"):
        m.S7ExactGroupHistorySourceBinding(
            created_by=binding.created_by,
            created_at_utc=binding.created_at_utc,
            manifest_plan_id=plan.plan_id,
            manifest_plan_sha256=plan.sha256,
            manifest_request_event_id=request.request_event_id,
            manifest_request_event_sha256=request.sha256,
            manifest_approval_id=approval.approval_id,
            manifest_approval_sha256=approval.sha256,
            manifest_literal_sha256=approval.approval_literal_sha256,
            run_intent_id=intent.intent_id,
            run_intent_path=intent.relative_path,
            run_intent_sha256=intent.sha256,
            source_artifacts=synthetic_sources,
            execution_source_pins=tuple(pins),
            manifest_documents=_manifest_refs(plan),
        )


def test_raw_lstat_or_inventory_projection_tamper_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    raw = synthetic_sources[0].to_dict()
    raw["disk_size_bytes"] = int(raw["disk_size_bytes"]) + 1
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="lstat size"):
        m.S7ExactGroupHistoryRawSourceArtifactRef.from_dict(raw)
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    monkeypatch.setattr(m, "INVENTORY_SOURCE_ARTIFACT_SET_DIGEST", D)
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="inventory projection"):
        m.S7ExactGroupHistorySourceBinding(
            created_by=binding.created_by,
            created_at_utc=binding.created_at_utc,
            manifest_plan_id=plan.plan_id,
            manifest_plan_sha256=plan.sha256,
            manifest_request_event_id=request.request_event_id,
            manifest_request_event_sha256=request.sha256,
            manifest_approval_id=approval.approval_id,
            manifest_approval_sha256=approval.sha256,
            manifest_literal_sha256=approval.approval_literal_sha256,
            run_intent_id=intent.intent_id,
            run_intent_path=intent.relative_path,
            run_intent_sha256=intent.sha256,
            source_artifacts=synthetic_sources,
            execution_source_pins=binding.execution_source_pins,
            manifest_documents=binding.manifest_documents,
        )


def test_future_execution_controls_bind_source_and_remain_unapproved(
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    _intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    execution_plan, execution_request = m.build_execution_controls(
        binding,
        plan,
        created_at_utc=T0 + timedelta(seconds=4),
        plan_actor=plan.future_execution_plan_actor,
        request_actor=plan.future_execution_request_actor,
    )
    assert execution_plan.source_artifact_set_digest == binding.raw_source_artifact_set_digest
    assert execution_plan.execution_data_root == m.EXECUTION_DATA_ROOT
    assert execution_plan.inventory_completion_id == INVENTORY_COMPLETION_ID
    assert execution_plan.directional_preview_candidate_id == DIRECTIONAL_CANDIDATE_ID
    assert execution_plan.directional_preview_completion_id == DIRECTIONAL_COMPLETION_ID
    assert execution_plan.document["plan_state"] == "awaiting_exact_execution_approval"
    assert not any(execution_plan.document["capabilities_before_exact_literal"].values())
    assert execution_request.resource_caps_digest == execution_plan.execution_resource_caps.digest
    assert json.loads(execution_request.canonical_approval_literal)["request_event_sha256"] == (
        execution_request.sha256
    )


def test_manifest_preflight_is_at_most_once_and_retry_is_zero_read(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    inventory_digest = stable_digest([item.project_inventory_8() for item in synthetic_sources])
    monkeypatch.setattr(m, "INVENTORY_SOURCE_ARTIFACT_SET_DIGEST", inventory_digest)

    def fake_read(_root, _plan, counts, _started):
        counts.update(
            {
                "json_reads": m.EXPECTED_MANIFEST_INPUT_COUNT,
                "lstats": m.SOURCE_ARTIFACT_COUNT,
                "parquet_bytes": 0,
                "json_bytes": 1,
                "source_lstat_bytes": m.SOURCE_BYTES,
                "source_rows": m.SOURCE_ROW_COUNT,
            }
        )
        return _manifest_refs(plan), synthetic_sources

    monkeypatch.setattr(m, "_read_and_bind_sources", fake_read)
    monkeypatch.setattr(m, "_check_caps", lambda *_args: None)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    first = m._run_manifest_preflight_locked(
        root=tmp_path,
        store=store,
        plan=plan,
        request=request,
        approval=approval,
        source_time=T0 + timedelta(seconds=3),
    )
    assert first.attempt_source_json_reads == m.EXPECTED_MANIFEST_INPUT_COUNT
    assert first.attempt_parquet_lstats == m.SOURCE_ARTIFACT_COUNT
    assert first.attempt_parquet_content_bytes_read == 0
    assert first.recovered is False
    second = m._run_manifest_preflight_locked(
        root=tmp_path,
        store=store,
        plan=plan,
        request=request,
        approval=approval,
        source_time=T0 + timedelta(seconds=30),
    )
    assert second.completion.completion_id == first.completion.completion_id
    assert second.attempt_source_json_reads == 0
    assert second.attempt_parquet_lstats == 0
    assert second.attempt_parquet_content_bytes_read == 0
    assert second.recovered is True


def test_completion_retry_rejects_noncanonical_receipt_path_source_free(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan, request, approval, store, first = _materialize_synthetic_completion(
        tmp_path, monkeypatch, synthetic_sources
    )
    execution_plan, _receipt_value = store.load_execution_plan(
        first.completion.execution_plan.logical_id,
        first.completion.execution_plan.sha256,
    )
    duplicate_relative = "duplicate/execution-plan.json"
    duplicate = tmp_path / duplicate_relative
    duplicate.parent.mkdir(parents=True)
    duplicate.write_bytes(execution_plan.content)
    bad_completion = replace(
        first.completion,
        execution_plan=replace(
            first.completion.execution_plan,
            path=duplicate_relative,
        ),
    )
    completion_path = tmp_path / first.completion.relative_path
    completion_path.chmod(0o600)
    completion_path.write_bytes(bad_completion.content)
    monkeypatch.setattr(
        m,
        "_read_and_bind_sources",
        lambda *_args: pytest.fail("source read attempted during completion retry"),
    )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="completion receipt"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=30),
        )


def test_completion_retry_rejects_nondeterministic_execution_controls_source_free(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan, request, approval, store, first = _materialize_synthetic_completion(
        tmp_path, monkeypatch, synthetic_sources
    )
    execution_plan, _receipt_value = store.load_execution_plan(
        first.completion.execution_plan.logical_id,
        first.completion.execution_plan.sha256,
    )
    drifted_plan = replace(
        execution_plan,
        created_at_utc=execution_plan.created_at_utc + timedelta(microseconds=9),
    )
    drifted_receipt = store.store_execution_plan(drifted_plan)
    bad_completion = replace(first.completion, execution_plan=drifted_receipt)
    completion_path = tmp_path / first.completion.relative_path
    completion_path.chmod(0o600)
    completion_path.write_bytes(bad_completion.content)
    monkeypatch.setattr(
        m,
        "_read_and_bind_sources",
        lambda *_args: pytest.fail("source read attempted during completion retry"),
    )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="completion outputs"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=30),
        )


def test_binding_recovery_deeply_verifies_intent_and_binding_source_free(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    bad_intent = replace(intent, input_binding_digest="f" * 64)
    store.store_run_intent(bad_intent)
    store.store_source_binding(binding)
    monkeypatch.setattr(
        m,
        "_read_and_bind_sources",
        lambda *_args: pytest.fail("source read attempted during binding retry"),
    )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="run intent"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=30),
        )


def test_binding_recovery_rejects_cross_binding_tamper_source_free(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    bad_binding = replace(binding, manifest_request_event_id="f" * 64)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    store.store_run_intent(intent)
    store.store_source_binding(bad_binding)
    monkeypatch.setattr(
        m,
        "_read_and_bind_sources",
        lambda *_args: pytest.fail("source read attempted during binding retry"),
    )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="source binding"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=30),
        )


def test_intent_without_complete_binding_fails_closed_on_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    calls = 0

    def interrupted(*_args):
        nonlocal calls
        calls += 1
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(m, "_read_and_bind_sources", interrupted)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=3),
        )
    with pytest.raises(m.IdentityExactGroupHistoryManifestError, match="fail closed"):
        m._run_manifest_preflight_locked(
            root=tmp_path,
            store=store,
            plan=plan,
            request=request,
            approval=approval,
            source_time=T0 + timedelta(seconds=30),
        )
    assert calls == 1


def test_complete_binding_recovers_partial_outputs_without_source_reads(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    synthetic_sources: tuple[m.S7ExactGroupHistoryRawSourceArtifactRef, ...],
) -> None:
    plan = _manifest_plan()
    request, approval = _request_approval(plan)
    intent, binding = _binding(monkeypatch, synthetic_sources, plan, request, approval)
    store = m.ExactGroupHistoryManifestStore(tmp_path)
    store.store_run_intent(intent)
    store.store_source_binding(binding)
    monkeypatch.setattr(
        m,
        "_read_and_bind_sources",
        lambda *_args: pytest.fail("source read attempted during binding recovery"),
    )
    result = m._run_manifest_preflight_locked(
        root=tmp_path,
        store=store,
        plan=plan,
        request=request,
        approval=approval,
        source_time=T0 + timedelta(seconds=30),
    )
    assert result.recovered is True
    assert result.attempt_source_json_reads == 0
    assert result.attempt_parquet_lstats == 0
    assert result.attempt_parquet_content_bytes_read == 0


def test_manifest_prepare_cli_preserves_exact_arguments_and_literal(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    from ame_stocks_api.cli import silver_identity_exact_group_history_prepare as cli

    receipt = m.StoredExactGroupHistoryManifestDocument(D, "control.json", D, 1)
    captured = {}

    def fake_build(**kwargs):
        captured.update(kwargs)
        return receipt, receipt, receipt, SimpleNamespace(canonical_approval_literal="literal")

    monkeypatch.setattr(cli, "build_exact_group_history_manifest_controls", fake_build)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare",
            "manifest",
            "--control-root",
            "/tmp/control",
            "--preparation-plan-id",
            D,
            "--preparation-plan-sha256",
            D,
            "--preparation-request-event-id",
            D,
            "--preparation-request-event-sha256",
            D,
            "--approved-preparation-literal",
            "literal",
            "--preparation-approved-by",
            "prep_approver",
            "--preparation-approved-at-utc",
            T0.isoformat(),
            "--manifest-plan-created-by",
            "manifest_plan_actor",
            "--manifest-plan-created-at-utc",
            (T0 + timedelta(seconds=1)).isoformat(),
            "--manifest-request-created-by",
            "manifest_request_actor",
            "--manifest-request-created-at-utc",
            (T0 + timedelta(seconds=2)).isoformat(),
            "--inventory-completion-path",
            "inventory.json",
            "--directional-completion-path",
            "directional.json",
            "--future-manifest-reader-actor",
            "reader_actor",
            "--future-execution-plan-actor",
            "execution_plan_actor",
            "--future-execution-request-actor",
            "execution_request_actor",
        ],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["approval_literal"] == "literal"
    assert captured["future_manifest_reader_actor"] == "reader_actor"
    assert captured["future_execution_plan_actor"] == "execution_plan_actor"
    assert captured["future_execution_request_actor"] == "execution_request_actor"


def test_manifest_approval_and_run_clis_preserve_receipts_and_zero_parquet(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    from ame_stocks_api.cli import (
        silver_identity_exact_group_history_manifest_approval as approval_cli,
    )
    from ame_stocks_api.cli import (
        silver_identity_exact_group_history_manifest_run as run_cli,
    )

    receipt = m.StoredExactGroupHistoryManifestDocument(D, "control.json", D, 1)
    monkeypatch.setattr(
        approval_cli,
        "record_exact_group_history_manifest_approval",
        lambda **_kwargs: receipt,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "approval",
            "--control-root",
            "/tmp/control",
            "--manifest-plan-id",
            D,
            "--manifest-plan-sha256",
            D,
            "--manifest-request-event-id",
            D,
            "--manifest-request-event-sha256",
            D,
            "--approval-literal",
            "literal",
            "--approved-by",
            "approver",
            "--approved-at-utc",
            T0.isoformat(),
        ],
    )
    assert approval_cli.main() == 0
    assert json.loads(capsys.readouterr().out)["path"] == "control.json"

    completion = SimpleNamespace(
        completion_id=D,
        sha256=D,
        execution_plan=SimpleNamespace(logical_id=D),
        execution_request=SimpleNamespace(logical_id=D),
    )
    result = SimpleNamespace(
        attempt_parquet_content_bytes_read=0,
        attempt_parquet_lstats=0,
        attempt_source_json_reads=0,
        completion=completion,
        completion_receipt=receipt,
        recovered=True,
    )
    monkeypatch.setattr(
        run_cli,
        "run_exact_group_history_manifest_preflight",
        lambda **_kwargs: result,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run",
            "--data-root",
            m.EXECUTION_DATA_ROOT,
            "--repository-root",
            "/opt/american_stocks",
            "--manifest-plan-id",
            D,
            "--manifest-plan-sha256",
            D,
            "--manifest-approval-id",
            D,
            "--manifest-approval-sha256",
            D,
            "--source-binding-created-at-utc",
            T0.isoformat(),
        ],
    )
    assert run_cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["attempt_parquet_content_bytes_read"] == 0
    assert payload["recovered"] is True
    assert payload["state"] == "awaiting_review"
