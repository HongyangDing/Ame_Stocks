from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.cli.silver_identity_directional_raw_preview_approval import build_parser
from ame_stocks_api.silver import identity_directional_raw_preview_manifest_approval
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_directional_raw_preview_approval import (
    DirectionalRawPreviewExecutionApprovalStore,
    IdentityDirectionalRawPreviewExecutionApprovalError,
    S7DirectionalRawPreviewExecutionApproval,
    record_s7_directional_raw_preview_execution_approval,
)
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
)
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
    DirectionalRawPreviewExecutionPlanStore,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
    StoredDirectionalRawPreviewExecutionDocument,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    IdentityDirectionalRawPreviewManifestStore,
    S7DirectionalRawPreviewManifestDocumentRef,
    S7DirectionalRawPreviewManifestRunIntent,
    S7DirectionalRawPreviewSourceArtifactRef,
    S7DirectionalRawPreviewSourceBinding,
    StoredDirectionalRawPreviewManifestControl,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_EXECUTION_PLAN_ID,
    S4_SOURCE_PINS,
    S7DirectionalRawPreviewControlFilePin,
)

SOURCE_AT = datetime(2026, 7, 18, 2, 0, tzinfo=UTC)
PLAN_AT = SOURCE_AT
REQUEST_AT = SOURCE_AT
APPROVAL_AT = SOURCE_AT + timedelta(minutes=3)


def _pin(path: str) -> S7DirectionalRawPreviewControlFilePin:
    content = path.encode()
    return S7DirectionalRawPreviewControlFilePin(
        path=path,
        git_blob=hashlib.sha1(
            b"blob " + str(len(content)).encode() + b"\0" + content
        ).hexdigest(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _binding() -> S7DirectionalRawPreviewSourceBinding:
    contracts = {
        "asset_observation_daily": ASSET_OBSERVATION_DAILY_CONTRACT,
        "universe_source_daily": UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
    release_pins = {str(item["table"]): item for item in S4_SOURCE_PINS}
    sessions = sorted(
        {
            session
            for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in case_sessions
        }
    )
    artifacts = []
    for table in contracts:
        for session in sessions:
            artifacts.append(
                S7DirectionalRawPreviewSourceArtifactRef(
                    table=table,
                    session_date=session,
                    release_id=str(release_pins[table]["release_id"]),
                    release_manifest_sha256=str(
                        release_pins[table]["release_manifest_sha256"]
                    ),
                    source_contract_id=contracts[table].contract_id,
                    source_schema_digest=contracts[table].schema_digest,
                    path=f"silver/{table}/session_date={session}/part.parquet",
                    sha256=hashlib.sha256(f"{table}:{session}".encode()).hexdigest(),
                    bytes=1_000,
                    row_count=10,
                    disk_size_bytes=1_000,
                )
            )
    manifests = (
        S7DirectionalRawPreviewManifestDocumentRef(
            "asset_release_manifest",
            str(release_pins["asset_observation_daily"]["release_id"]),
            (
                "manifests/silver/releases/"
                f"release_id={release_pins['asset_observation_daily']['release_id']}.json"
            ),
            str(release_pins["asset_observation_daily"]["release_manifest_sha256"]),
            100,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "universe_release_manifest",
            str(release_pins["universe_source_daily"]["release_id"]),
            (
                "manifests/silver/releases/"
                f"release_id={release_pins['universe_source_daily']['release_id']}.json"
            ),
            str(release_pins["universe_source_daily"]["release_manifest_sha256"]),
            100,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_completion_manifest",
            INVENTORY_COMPLETION_ID,
            (
                "manifests/silver/identity/composite-inventory-execution-completions/"
                f"plan_id={INVENTORY_EXECUTION_PLAN_ID}/approval_id={'d' * 64}/"
                "manifest.json"
            ),
            "c" * 64,
            100,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_candidate_manifest",
            INVENTORY_CANDIDATE_ID,
            (
                "manifests/silver/identity/composite-inventory-candidates/"
                f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
            ),
            INVENTORY_CANDIDATE_MANIFEST_SHA256,
            100,
        ),
    )
    intent = _run_intent()
    return S7DirectionalRawPreviewSourceBinding(
        created_by="fixture-manifest-reader",
        created_at_utc=SOURCE_AT,
        manifest_plan_id="1" * 64,
        manifest_plan_sha256="2" * 64,
        manifest_request_event_id="3" * 64,
        manifest_request_event_sha256="4" * 64,
        manifest_literal_sha256="5" * 64,
        manifest_approval_id="6" * 64,
        manifest_approval_sha256="7" * 64,
        manifest_run_intent_id=intent.intent_id,
        manifest_run_intent_path=intent.relative_path,
        manifest_run_intent_sha256=intent.sha256,
        manifest_run_intent_execution_plan_created_by=intent.execution_plan_created_by,
        manifest_run_intent_execution_request_created_by=(
            intent.execution_request_created_by
        ),
        source_artifacts=tuple(artifacts),
        manifest_documents=manifests,
    )


def _run_intent() -> S7DirectionalRawPreviewManifestRunIntent:
    return S7DirectionalRawPreviewManifestRunIntent(
        manifest_plan_id="1" * 64,
        manifest_plan_sha256="2" * 64,
        manifest_request_event_id="3" * 64,
        manifest_request_event_sha256="4" * 64,
        manifest_approval_id="6" * 64,
        manifest_approval_sha256="7" * 64,
        approval_literal_sha256="5" * 64,
        input_binding_digest="8" * 64,
        execution_data_root="/mnt/HC_Volume_106309665/american_stocks",
        source_binding_created_by="fixture-manifest-reader",
        source_binding_created_at_utc=SOURCE_AT,
        execution_plan_created_by="fixture-execution-planner",
        execution_request_created_by="fixture-execution-requester",
    )


def _controls(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    S7DirectionalRawPreviewSourceBinding,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
]:
    source = _binding()
    intent = _run_intent()
    intent_path = root / intent.relative_path
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_bytes(intent.content)
    source_path = root / source.relative_path
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source.content)
    runtime_files = tuple(_pin(path) for path in REQUIRED_EXECUTION_RUNTIME_PATHS)
    verification_files = tuple(
        _pin(path) for path in REQUIRED_EXECUTION_VERIFICATION_PATHS
    )
    manifest_plan = SimpleNamespace(
        git_commit="a" * 40,
        git_tree="b" * 40,
        execution_data_root=str(root),
        runtime_files=runtime_files,
        verification_files=verification_files,
    )
    manifest_approval = SimpleNamespace(
        plan_id=source.manifest_plan_id,
        plan_sha256=source.manifest_plan_sha256,
        request_event_id=source.manifest_request_event_id,
        request_event_sha256=source.manifest_request_event_sha256,
        approval_literal_sha256=source.manifest_literal_sha256,
        approval_id=source.manifest_approval_id,
        sha256=source.manifest_approval_sha256,
        approved_at_utc=SOURCE_AT - timedelta(minutes=1),
    )
    monkeypatch.setattr(
        IdentityDirectionalRawPreviewManifestStore,
        "load_plan",
        lambda self, plan_id, *, expected_sha256: (manifest_plan, SimpleNamespace()),
    )
    monkeypatch.setattr(
        IdentityDirectionalRawPreviewManifestStore,
        "load_run_intent",
        lambda self, plan_id, approval_id, *, expected_sha256: (
            intent,
            StoredDirectionalRawPreviewManifestControl(
                intent.relative_path, intent.sha256, len(intent.content)
            ),
        ),
    )
    monkeypatch.setattr(
        identity_directional_raw_preview_manifest_approval.DirectionalRawPreviewManifestApprovalStore,
        "load_approval",
        lambda self, approval_id, *, expected_sha256: (
            manifest_approval,
            SimpleNamespace(),
        ),
    )
    plan = S7DirectionalRawPreviewExecutionPlan.create(
        created_by="fixture-execution-planner",
        created_at_utc=PLAN_AT,
        execution_git_commit="a" * 40,
        execution_git_tree="b" * 40,
        execution_data_root=str(root),
        runtime_files=runtime_files,
        verification_files=verification_files,
        source_binding=source,
        source_binding_receipt=StoredDirectionalRawPreviewManifestControl(
            source.relative_path, source.sha256, len(source.content)
        ),
    )
    request = S7DirectionalRawPreviewExecutionRequest.create(
        plan,
        StoredDirectionalRawPreviewExecutionDocument(
            plan.relative_path, plan.sha256, len(plan.content)
        ),
        created_by="fixture-execution-requester",
        created_at_utc=REQUEST_AT,
    )
    return source, plan, request


def _approval(
    root: Path, monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
    S7DirectionalRawPreviewExecutionApproval,
]:
    _, plan, request = _controls(root, monkeypatch)
    approval = S7DirectionalRawPreviewExecutionApproval.create(
        request,
        StoredDirectionalRawPreviewExecutionDocument(
            request.relative_path, request.sha256, len(request.content)
        ),
        plan=plan,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-human-reviewer",
        approved_at_utc=APPROVAL_AT,
        approval_note="exact bounded preview only",
    )
    return plan, request, approval


def _store_controls(
    root: Path, monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
]:
    _, plan, request = _controls(root, monkeypatch)
    store = DirectionalRawPreviewExecutionPlanStore(root)
    store.store_execution_plan(plan)
    store.store_execution_request(request)
    return plan, request


def test_exact_literal_approval_authorizes_only_one_bounded_preview(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, request, approval = _approval(tmp_path, monkeypatch)

    assert approval.plan_id == plan.plan_id
    assert approval.request_event_id == request.request_event_id
    assert approval.approval_literal == request.canonical_approval_literal
    assert approval.preview_execution_authorized is True
    assert approval.data_read_authorized is True
    assert approval.parquet_read_authorized is True
    assert approval.once_to_awaiting_review is True
    assert approval.caller_scope_override_authorized is False
    assert approval.source_discovery_authorized is False
    assert approval.exact_group_history_read_authorized is False
    assert approval.registry_evaluation_authorized is False
    assert approval.adjudication_authorized is False
    assert approval.full_run_authorized is False
    assert approval.publication_authorized is False
    assert S7DirectionalRawPreviewExecutionApproval.from_dict(
        json.loads(approval.content)
    ) == approval


def test_literal_actor_time_and_binding_tampering_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, request, approval = _approval(tmp_path, monkeypatch)
    receipt = StoredDirectionalRawPreviewExecutionDocument(
        request.relative_path, request.sha256, len(request.content)
    )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionApprovalError, match="literal differs"
    ):
        S7DirectionalRawPreviewExecutionApproval.create(
            request,
            receipt,
            plan=plan,
            approval_literal=request.canonical_approval_literal + " ",
            approved_by="reviewer",
            approved_at_utc=APPROVAL_AT,
        )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionApprovalError, match="separate"
    ):
        S7DirectionalRawPreviewExecutionApproval.create(
            request,
            receipt,
            plan=plan,
            approval_literal=request.canonical_approval_literal,
            approved_by=request.created_by,
            approved_at_utc=APPROVAL_AT,
        )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionApprovalError, match="strictly predate"
    ):
        S7DirectionalRawPreviewExecutionApproval.create(
            request,
            receipt,
            plan=plan,
            approval_literal=request.canonical_approval_literal,
            approved_by="reviewer",
            approved_at_utc=request.created_at_utc,
        )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionApprovalError, match="not the exact"
    ):
        replace(approval, source_artifact_set_digest="0" * 64)


def test_store_and_record_function_are_immutable_and_do_not_execute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, request = _store_controls(tmp_path, monkeypatch)
    first = record_s7_directional_raw_preview_execution_approval(
        tmp_path,
        plan_id=plan.plan_id,
        expected_plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        expected_request_event_sha256=request.sha256,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-human-reviewer",
        approved_at=APPROVAL_AT.isoformat(),
        approval_note="bounded preview",
    )
    second = record_s7_directional_raw_preview_execution_approval(
        tmp_path,
        plan_id=plan.plan_id,
        expected_plan_sha256=plan.sha256,
        request_event_id=request.request_event_id,
        expected_request_event_sha256=request.sha256,
        approval_literal=request.canonical_approval_literal,
        approved_by="fixture-human-reviewer",
        approved_at=APPROVAL_AT.isoformat(),
        approval_note="bounded preview",
    )

    assert first.approval_document_preexisting is False
    assert second.approval_document_preexisting is True
    assert first.approval == second.approval
    assert not (tmp_path / "gold").exists()
    loaded = DirectionalRawPreviewExecutionApprovalStore(tmp_path).load_approval(
        first.approval.approval_id,
        expected_sha256=first.approval.sha256,
    )
    assert loaded[0] == first.approval


def test_cli_has_no_ticker_date_or_execution_overrides() -> None:
    parser = build_parser()
    options = {action.dest for action in parser._actions}

    assert {
        "data_root",
        "plan_id",
        "plan_sha256",
        "request_event_id",
        "request_event_sha256",
        "approval_literal",
        "approved_by",
        "approved_at",
        "approval_note",
    } <= options
    assert not ({"ticker", "date", "session", "start", "end", "path"} & options)


def test_approval_module_has_no_runner_parquet_or_network_capability() -> None:
    path = Path(
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py"
    )
    tree = ast.parse(path.read_text())
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any("runner" in item for item in imports)
    assert not any(item in {"polars", "pyarrow", "requests", "httpx"} for item in imports)
