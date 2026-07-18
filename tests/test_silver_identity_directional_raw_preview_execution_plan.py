from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.silver import identity_directional_raw_preview_manifest_approval
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
)
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    DIRECTIONAL_RAW_PREVIEW_ALGORITHM_DIGEST,
    DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST,
    PREPARATION_APPROVAL_LITERAL_SHA256,
    PREPARATION_REQUEST_EVENT_ID,
    PREPARATION_REQUEST_EVENT_SHA256,
    REQUIRED_EXECUTION_RUNTIME_PATHS,
    REQUIRED_EXECUTION_VERIFICATION_PATHS,
    DirectionalRawPreviewExecutionPlanStore,
    IdentityDirectionalRawPreviewExecutionPlanError,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
    StoredDirectionalRawPreviewExecutionDocument,
    directional_raw_preview_algorithm_spec,
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

SOURCE_AT = datetime(2026, 7, 18, 1, 0, tzinfo=UTC)
PLAN_AT = SOURCE_AT
REQUEST_AT = SOURCE_AT


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


def _source_binding() -> S7DirectionalRawPreviewSourceBinding:
    contracts = {
        "asset_observation_daily": ASSET_OBSERVATION_DAILY_CONTRACT,
        "universe_source_daily": UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
    pins = {str(item["table"]): item for item in S4_SOURCE_PINS}
    sessions = sorted(
        {
            session
            for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in case_sessions
        }
    )
    sources = []
    for table in ("asset_observation_daily", "universe_source_daily"):
        for session in sessions:
            content = f"{table}:{session.isoformat()}".encode()
            sources.append(
                S7DirectionalRawPreviewSourceArtifactRef(
                    table=table,
                    session_date=session,
                    release_id=str(pins[table]["release_id"]),
                    release_manifest_sha256=str(
                        pins[table]["release_manifest_sha256"]
                    ),
                    source_contract_id=contracts[table].contract_id,
                    source_schema_digest=contracts[table].schema_digest,
                    path=(
                        f"silver/{table}/session_date={session.isoformat()}/"
                        "part-00000.parquet"
                    ),
                    sha256=hashlib.sha256(content).hexdigest(),
                    bytes=1_000,
                    row_count=10,
                    disk_size_bytes=1_000,
                )
            )
    documents = (
        S7DirectionalRawPreviewManifestDocumentRef(
            "asset_release_manifest",
            str(pins["asset_observation_daily"]["release_id"]),
            (
                "manifests/silver/releases/"
                f"release_id={pins['asset_observation_daily']['release_id']}.json"
            ),
            str(pins["asset_observation_daily"]["release_manifest_sha256"]),
            100,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "universe_release_manifest",
            str(pins["universe_source_daily"]["release_id"]),
            (
                "manifests/silver/releases/"
                f"release_id={pins['universe_source_daily']['release_id']}.json"
            ),
            str(pins["universe_source_daily"]["release_manifest_sha256"]),
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
        source_artifacts=tuple(sources),
        manifest_documents=documents,
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


def _plan(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[S7DirectionalRawPreviewSourceBinding, S7DirectionalRawPreviewExecutionPlan]:
    source = _source_binding()
    intent = _run_intent()
    intent_path = root / intent.relative_path
    intent_path.parent.mkdir(parents=True, exist_ok=True)
    intent_path.write_bytes(intent.content)
    source_path = root / source.relative_path
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source.content)
    source_receipt = StoredDirectionalRawPreviewManifestControl(
        source.relative_path, source.sha256, len(source.content)
    )
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
    monkeypatch.setattr(
        IdentityDirectionalRawPreviewManifestStore,
        "load_plan",
        lambda self, plan_id, *, expected_sha256: (
            manifest_plan,
            SimpleNamespace(),
        ),
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
    plan = S7DirectionalRawPreviewExecutionPlan.create(
        created_by="fixture-execution-planner",
        created_at_utc=PLAN_AT,
        execution_git_commit="a" * 40,
        execution_git_tree="b" * 40,
        execution_data_root=str(root),
        runtime_files=runtime_files,
        verification_files=verification_files,
        source_binding=source,
        source_binding_receipt=source_receipt,
    )
    return source, plan


def _request(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[
    S7DirectionalRawPreviewSourceBinding,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
]:
    source, plan = _plan(root, monkeypatch)
    request = S7DirectionalRawPreviewExecutionRequest.create(
        plan,
        StoredDirectionalRawPreviewExecutionDocument(
            plan.relative_path, plan.sha256, len(plan.content)
        ),
        created_by="fixture-execution-requester",
        created_at_utc=REQUEST_AT,
    )
    return source, plan, request


def test_plan_requires_exact_source_binding_and_embeds_all_twenty_two_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, plan = _plan(tmp_path, monkeypatch)
    document = plan.document

    assert len(plan.source_artifacts) == 22
    assert document["source_binding"]["artifact_count"] == 22
    assert document["source_binding"]["manifest_id"] == source.source_binding_id
    assert document["source_binding"]["manifest_sha256"] == source.sha256
    assert document["source_binding"]["source_artifact_set_digest"] == (
        plan.source_artifact_set_digest
    )
    assert all(
        {
            "table",
            "session_date",
            "path",
            "sha256",
            "bytes",
            "row_count",
            "source_contract_id",
            "schema_digest",
        }
        <= set(item)
        for item in document["source_binding"]["source_artifacts"]
    )
    assert document["inventory_binding"]["completion_manifest_sha256"] == "c" * 64
    assert document["preparation_authorization"]["approved_literal_sha256"] == (
        PREPARATION_APPROVAL_LITERAL_SHA256
    )
    assert set(document["capabilities"].values()) == {False}
    assert S7DirectionalRawPreviewExecutionPlan.from_dict(json.loads(plan.content)) == plan


def test_request_literal_is_execution_specific_and_has_no_scope_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, plan, request = _request(tmp_path, monkeypatch)
    literal = json.loads(request.canonical_approval_literal)

    assert request.plan_id == plan.plan_id
    assert request.preparation_request_event_id == PREPARATION_REQUEST_EVENT_ID
    assert request.preparation_request_event_sha256 == PREPARATION_REQUEST_EVENT_SHA256
    assert literal["source_artifact_set_digest"] == plan.source_artifact_set_digest
    assert literal["preparation_approval_literal_sha256"] == (
        PREPARATION_APPROVAL_LITERAL_SHA256
    )
    assert not {
        "ticker",
        "date",
        "session",
        "start_date",
        "end_date",
        "artifact_path",
    }.intersection(literal)
    assert S7DirectionalRawPreviewExecutionRequest.from_dict(
        json.loads(request.content)
    ) == request


def test_algorithm_and_qa_are_frozen_to_review_only_semantics() -> None:
    algorithm = directional_raw_preview_algorithm_spec()

    assert algorithm["source_artifact_discovery"] == (
        "forbidden_use_plan_embedded_refs_only"
    )
    assert algorithm["registry_evaluation"] == "forbidden_not_evaluated"
    assert hashlib.sha256(
        json.dumps(algorithm, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest() == DIRECTIONAL_RAW_PREVIEW_ALGORITHM_DIGEST
    assert len(DIRECTIONAL_RAW_PREVIEW_QA_SEMANTICS_DIGEST) == 64


def test_execution_plan_requires_exact_manifest_plan_git_and_file_pins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _ = _plan(tmp_path, monkeypatch)
    runtime = tuple(_pin(path) for path in REQUIRED_EXECUTION_RUNTIME_PATHS)
    verification = tuple(_pin(path) for path in REQUIRED_EXECUTION_VERIFICATION_PATHS)
    altered = (replace(runtime[0], sha256="f" * 64), *runtime[1:])
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError,
        match="approved manifest plan",
    ):
        S7DirectionalRawPreviewExecutionPlan.create(
            created_by="fixture-execution-planner",
            created_at_utc=PLAN_AT,
            execution_git_commit="a" * 40,
            execution_git_tree="b" * 40,
            execution_data_root=str(tmp_path),
            runtime_files=altered,
            verification_files=verification,
            source_binding=source,
            source_binding_receipt=StoredDirectionalRawPreviewManifestControl(
                source.relative_path, source.sha256, len(source.content)
            ),
        )


def test_forged_or_incomplete_source_binding_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source_binding()
    source_path = tmp_path / source.relative_path
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(source.content)
    receipt = StoredDirectionalRawPreviewManifestControl(
        source.relative_path, source.sha256, len(source.content)
    )
    common = dict(
        created_by="planner",
        created_at_utc=PLAN_AT,
        execution_git_commit="a" * 40,
        execution_git_tree="b" * 40,
        execution_data_root=str(tmp_path),
        runtime_files=tuple(_pin(path) for path in REQUIRED_EXECUTION_RUNTIME_PATHS),
        verification_files=tuple(
            _pin(path) for path in REQUIRED_EXECUTION_VERIFICATION_PATHS
        ),
        source_binding_receipt=receipt,
    )
    forged = SimpleNamespace(**{name: getattr(source, name) for name in (
        "source_binding_id",
        "relative_path",
        "sha256",
        "content",
    )})
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError, match="exact manifest"
    ):
        S7DirectionalRawPreviewExecutionPlan.create(
            **common,
            source_binding=forged,
        )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError, match="twenty-two"
    ):
        plan = _plan(tmp_path, monkeypatch)[1]
        replace(plan, source_artifacts=plan.source_artifacts[:-1])


def test_store_parses_and_revalidates_the_source_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, plan = _plan(tmp_path, monkeypatch)
    source_path = tmp_path / source.relative_path
    approval = SimpleNamespace(
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
        identity_directional_raw_preview_manifest_approval.DirectionalRawPreviewManifestApprovalStore,
        "load_approval",
        lambda self, approval_id, *, expected_sha256: (approval, SimpleNamespace()),
    )
    store = DirectionalRawPreviewExecutionPlanStore(tmp_path)
    receipt = store.store_execution_plan(plan)
    request = S7DirectionalRawPreviewExecutionRequest.create(
        plan,
        receipt,
        created_by="fixture-execution-requester",
        created_at_utc=REQUEST_AT,
    )
    request_receipt = store.store_execution_request(request)

    assert store.load_execution_plan(plan.plan_id, expected_sha256=plan.sha256)[0] == plan
    assert store.load_execution_request(
        request.request_event_id, expected_sha256=request.sha256
    )[1] == request_receipt
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError,
        match="crosses plan bindings",
    ):
        store.store_execution_request(
            replace(request, created_by="forged-execution-requester")
        )
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError,
        match="crosses plan bindings",
    ):
        store.store_execution_request(
            replace(request, created_at_utc=request.created_at_utc + timedelta(seconds=1))
        )
    source_path.write_bytes(source.content + b" ")
    with pytest.raises(
        IdentityDirectionalRawPreviewExecutionPlanError, match="missing or altered"
    ):
        store.load_execution_plan(plan.plan_id, expected_sha256=plan.sha256)


def test_execution_plan_module_has_no_runner_parquet_or_approval_import() -> None:
    path = Path(
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py"
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
    assert not any("runner" in item or "approval" in item for item in imports)
    assert not any(item in {"polars", "pyarrow", "requests", "httpx"} for item in imports)
