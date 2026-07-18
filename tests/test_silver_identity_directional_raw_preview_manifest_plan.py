from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    MANIFEST_AUTHORIZED_ACTION,
    MANIFEST_EXECUTION_DATA_ROOT,
    MANIFEST_SELECTION_SEMANTICS_DIGEST,
    PREPARATION_LITERAL,
    PREPARATION_LITERAL_SHA256,
    REQUIRED_MANIFEST_RUNTIME_PATHS,
    REQUIRED_MANIFEST_VERIFICATION_PATHS,
    IdentityDirectionalRawPreviewManifestPlanError,
    IdentityDirectionalRawPreviewManifestStore,
    S7DirectionalRawPreviewManifestDocumentRef,
    S7DirectionalRawPreviewManifestFilePin,
    S7DirectionalRawPreviewManifestPreflightPlan,
    S7DirectionalRawPreviewManifestRunIntent,
    S7DirectionalRawPreviewPreparationAuthorizationReceipt,
    S7DirectionalRawPreviewSourceArtifactRef,
    S7DirectionalRawPreviewSourceBinding,
    StoredDirectionalRawPreviewManifestControl,
    exact_source_selection_semantics,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_EXECUTION_PLAN_ID,
    S4_SOURCE_PINS,
)

NOW = datetime(2026, 7, 18, 6, 0, tzinfo=UTC)


def _pin(path: str) -> S7DirectionalRawPreviewManifestFilePin:
    content = path.encode()
    return S7DirectionalRawPreviewManifestFilePin(
        path=path,
        git_blob=hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _authorization() -> S7DirectionalRawPreviewPreparationAuthorizationReceipt:
    return S7DirectionalRawPreviewPreparationAuthorizationReceipt(
        recorded_by="preparation-receipt-recorder",
        recorded_at_utc=NOW,
    )


def _plan() -> S7DirectionalRawPreviewManifestPreflightPlan:
    authorization = _authorization()
    receipt = StoredDirectionalRawPreviewManifestControl(
        authorization.relative_path,
        authorization.sha256,
        len(authorization.content),
    )
    return S7DirectionalRawPreviewManifestPreflightPlan.create(
        created_by="manifest-plan-author",
        created_at_utc=NOW,
        future_manifest_reader_actor="future-manifest-reader",
        future_execution_plan_actor="future-execution-plan-author",
        future_execution_request_actor="future-execution-request-author",
        git_commit="a" * 40,
        git_tree="b" * 40,
        execution_data_root=MANIFEST_EXECUTION_DATA_ROOT,
        runtime_files=tuple(_pin(path) for path in REQUIRED_MANIFEST_RUNTIME_PATHS),
        verification_files=tuple(_pin(path) for path in REQUIRED_MANIFEST_VERIFICATION_PATHS),
        preparation_authorization=authorization,
        preparation_authorization_receipt=receipt,
    )


def _source_artifacts() -> tuple[S7DirectionalRawPreviewSourceArtifactRef, ...]:
    sessions = sorted(
        {
            session
            for _, case_sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in case_sessions
        }
    )
    output = []
    for table, contract in (
        ("asset_observation_daily", ASSET_OBSERVATION_DAILY_CONTRACT),
        ("universe_source_daily", UNIVERSE_SOURCE_DAILY_CONTRACT),
    ):
        pin = next(item for item in S4_SOURCE_PINS if item["table"] == table)
        for ordinal, session in enumerate(sessions, start=1):
            output.append(
                S7DirectionalRawPreviewSourceArtifactRef(
                    table=table,
                    session_date=session,
                    release_id=str(pin["release_id"]),
                    release_manifest_sha256=str(pin["release_manifest_sha256"]),
                    source_contract_id=contract.contract_id,
                    source_schema_digest=contract.schema_digest,
                    path=f"silver/{table}/session_date={session}/part-00000.parquet",
                    sha256=f"{ordinal + (100 if table.startswith('universe') else 0):064x}",
                    bytes=1_000 + ordinal,
                    row_count=100 + ordinal,
                    disk_size_bytes=1_000 + ordinal,
                )
            )
    return tuple(output)


def _manifest_documents() -> tuple[S7DirectionalRawPreviewManifestDocumentRef, ...]:
    asset = next(item for item in S4_SOURCE_PINS if item["table"] == "asset_observation_daily")
    universe = next(item for item in S4_SOURCE_PINS if item["table"] == "universe_source_daily")
    return (
        S7DirectionalRawPreviewManifestDocumentRef(
            "asset_release_manifest",
            str(asset["release_id"]),
            f"manifests/silver/releases/release_id={asset['release_id']}.json",
            str(asset["release_manifest_sha256"]),
            1_000,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "universe_release_manifest",
            str(universe["release_id"]),
            f"manifests/silver/releases/release_id={universe['release_id']}.json",
            str(universe["release_manifest_sha256"]),
            1_001,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_completion_manifest",
            INVENTORY_COMPLETION_ID,
            "manifests/silver/identity/composite-inventory-execution-completions/"
            f"plan_id={INVENTORY_EXECUTION_PLAN_ID}/approval_id={'2' * 64}/manifest.json",
            "3" * 64,
            2_000,
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_candidate_manifest",
            INVENTORY_CANDIDATE_ID,
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json",
            INVENTORY_CANDIDATE_MANIFEST_SHA256,
            2_001,
        ),
    )


def _binding() -> S7DirectionalRawPreviewSourceBinding:
    intent = _intent()
    return S7DirectionalRawPreviewSourceBinding(
        created_by="future-manifest-reader",
        created_at_utc=NOW,
        manifest_plan_id="4" * 64,
        manifest_plan_sha256="5" * 64,
        manifest_request_event_id="6" * 64,
        manifest_request_event_sha256="7" * 64,
        manifest_literal_sha256="8" * 64,
        manifest_approval_id="9" * 64,
        manifest_approval_sha256="a" * 64,
        manifest_run_intent_id=intent.intent_id,
        manifest_run_intent_path=intent.relative_path,
        manifest_run_intent_sha256=intent.sha256,
        manifest_run_intent_execution_plan_created_by=intent.execution_plan_created_by,
        manifest_run_intent_execution_request_created_by=(
            intent.execution_request_created_by
        ),
        source_artifacts=_source_artifacts(),
        manifest_documents=_manifest_documents(),
    )


def _intent() -> S7DirectionalRawPreviewManifestRunIntent:
    return S7DirectionalRawPreviewManifestRunIntent(
        manifest_plan_id="4" * 64,
        manifest_plan_sha256="5" * 64,
        manifest_request_event_id="6" * 64,
        manifest_request_event_sha256="7" * 64,
        manifest_approval_id="9" * 64,
        manifest_approval_sha256="a" * 64,
        approval_literal_sha256="8" * 64,
        input_binding_digest="b" * 64,
        execution_data_root=MANIFEST_EXECUTION_DATA_ROOT,
        source_binding_created_by="future-manifest-reader",
        source_binding_created_at_utc=NOW,
        execution_plan_created_by="future-execution-plan-author",
        execution_request_created_by="future-execution-request-author",
    )


def test_approved_preparation_literal_and_manifest_scope_are_exact() -> None:
    assert hashlib.sha256(PREPARATION_LITERAL.encode()).hexdigest() == (PREPARATION_LITERAL_SHA256)
    semantics = exact_source_selection_semantics()
    assert len(semantics["fixed_sessions"]) == 11
    assert semantics["expected_artifact_count"] == 22
    assert semantics["artifact_content_hashing"].startswith("forbidden")
    assert semantics["inventory_candidate_data_read"] is False
    assert (
        hashlib.sha256(
            json.dumps(
                semantics,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()
        == MANIFEST_SELECTION_SEMANTICS_DIGEST
    )


def test_authorization_and_plan_are_canonical_and_nonexecuting() -> None:
    authorization = _authorization()
    assert set(authorization.document["authorization_boundary"].values()) == {
        False,
        True,
    }
    assert authorization.document["authorization_boundary"]["manifest_read_authorized"] is False
    assert (
        S7DirectionalRawPreviewPreparationAuthorizationReceipt.from_dict(
            json.loads(authorization.content)
        )
        == authorization
    )

    plan = _plan()
    assert plan.document["authorized_action"] == MANIFEST_AUTHORIZED_ACTION
    assert set(plan.document["capabilities_before_exact_literal"].values()) == {False}
    assert plan.document["future_outputs_after_exact_literal"]["artifact_count"] == 5
    assert plan.document["resource_caps"]["parquet_content_read_bytes"] == 0
    assert S7DirectionalRawPreviewManifestPreflightPlan.from_dict(json.loads(plan.content)) == plan


def test_source_binding_is_exact_twenty_two_and_exposes_typed_inventory_refs() -> None:
    binding = _binding()
    assert len(binding.source_artifacts) == 22
    assert len(binding.release_manifest_refs) == 2
    assert binding.inventory_completion_ref.logical_id == INVENTORY_COMPLETION_ID
    assert binding.inventory_candidate_ref.sha256 == INVENTORY_CANDIDATE_MANIFEST_SHA256
    assert binding.source_caps.parquet_content_read_bytes == 0
    assert set(binding.document["capabilities"].values()) == {False}
    assert S7DirectionalRawPreviewSourceBinding.from_dict(json.loads(binding.content)) == binding

    with pytest.raises(
        IdentityDirectionalRawPreviewManifestPlanError,
        match="exact twenty-two",
    ):
        S7DirectionalRawPreviewSourceBinding(
            created_by=binding.created_by,
            created_at_utc=binding.created_at_utc,
            manifest_plan_id=binding.manifest_plan_id,
            manifest_plan_sha256=binding.manifest_plan_sha256,
            manifest_request_event_id=binding.manifest_request_event_id,
            manifest_request_event_sha256=binding.manifest_request_event_sha256,
            manifest_literal_sha256=binding.manifest_literal_sha256,
            manifest_approval_id=binding.manifest_approval_id,
            manifest_approval_sha256=binding.manifest_approval_sha256,
            manifest_run_intent_id=binding.manifest_run_intent_id,
            manifest_run_intent_path=binding.manifest_run_intent_path,
            manifest_run_intent_sha256=binding.manifest_run_intent_sha256,
            manifest_run_intent_execution_plan_created_by=(
                binding.manifest_run_intent_execution_plan_created_by
            ),
            manifest_run_intent_execution_request_created_by=(
                binding.manifest_run_intent_execution_request_created_by
            ),
            source_artifacts=binding.source_artifacts[:-1],
            manifest_documents=binding.manifest_documents,
        )


def test_manifest_store_is_immutable_idempotent_and_rejects_tamper(tmp_path) -> None:
    store = IdentityDirectionalRawPreviewManifestStore(tmp_path)
    binding = _binding()
    intent = _intent()
    intent_path = tmp_path / intent.relative_path
    intent_path.parent.mkdir(parents=True)
    intent_path.write_bytes(intent.content)
    first = store.store_source_binding(binding)
    second = store.store_source_binding(binding)
    assert first == second
    loaded, _ = store.load_source_binding(
        binding.manifest_run_intent_id,
        expected_source_binding_id=binding.source_binding_id,
        expected_sha256=binding.sha256,
    )
    assert loaded == binding

    target = tmp_path / binding.relative_path
    target.chmod(0o644)
    target.write_bytes(b"{}\n")
    with pytest.raises(IdentityDirectionalRawPreviewManifestPlanError, match="missing or unsafe"):
        store.load_source_binding(
            binding.manifest_run_intent_id,
            expected_source_binding_id=binding.source_binding_id,
            expected_sha256=binding.sha256,
        )


def test_run_intent_is_a_single_plan_approval_scoped_immutable_claim(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = IdentityDirectionalRawPreviewManifestStore(tmp_path)
    monkeypatch.setattr(
        IdentityDirectionalRawPreviewManifestStore,
        "_verify_run_intent_controls",
        lambda self, value: None,
    )
    intent = _intent()
    first = store.store_run_intent(intent)
    second = store.store_run_intent(intent)
    assert first == second
    assert "plan_id=" in first.path and "approval_id=" in first.path

    conflicting = replace(
        intent,
        execution_plan_created_by="different-execution-plan-author",
    )
    assert conflicting.relative_path == intent.relative_path
    with pytest.raises(IdentityDirectionalRawPreviewManifestPlanError):
        store.store_run_intent(conflicting)


def test_source_binding_path_is_discoverable_from_the_run_intent() -> None:
    binding = _binding()
    assert f"run_intent_id={binding.manifest_run_intent_id}" in binding.relative_path
    assert binding.source_binding_id not in binding.relative_path
