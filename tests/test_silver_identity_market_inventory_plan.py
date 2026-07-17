from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import ame_stocks_api.silver.identity_market_inventory_plan as inventory_plan_module
from ame_stocks_api.artifacts import sha256_file
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_market_inventory_plan import (
    APPROVAL_TEXT,
    APPROVAL_TEXT_SHA256,
    APPROVED_CONTRACT_PINS,
    DAILY_SOURCE_ARTIFACT_COUNT,
    DAILY_SOURCE_BYTES,
    DAILY_SOURCE_ROW_COUNT,
    EXACT_SOURCE_PINS,
    EXTERNAL_EVIDENCE_MANIFEST_ID,
    EXTERNAL_EVIDENCE_MANIFEST_PATH,
    EXTERNAL_EVIDENCE_MANIFEST_SHA256,
    INVENTORY_CALENDAR_ARTIFACT_ID,
    INVENTORY_CALENDAR_ARTIFACT_SHA256,
    INVENTORY_END_SESSION,
    INVENTORY_SESSION_COUNT,
    INVENTORY_START_SESSION,
    MARKET_INVENTORY_AUTHORIZED_ACTION,
    IdentityMarketInventoryPlanError,
    IdentityMarketInventoryPlanStore,
    S7CompositeInventoryApprovalRequest,
    S7CompositeInventoryPlan,
    S7MarketInventoryResourceCaps,
    S7SchemaEvidenceApprovalBundle,
    StoredIdentityMarketInventoryDocument,
    composite_inventory_approval_request_path,
    composite_inventory_plan_path,
    schema_evidence_approval_path,
)
from ame_stocks_api.silver.identity_source import S7_SOURCE_PINS

REPO_ROOT = Path(__file__).resolve().parents[1]
RECORDED_AT = datetime(2026, 7, 17, 1, 0, tzinfo=UTC)
PLANNED_AT = datetime(2026, 7, 17, 2, 0, tzinfo=UTC)
REQUESTED_AT = datetime(2026, 7, 17, 3, 0, tzinfo=UTC)
GIT_COMMIT = "a" * 40


@pytest.fixture(scope="module")
def exact_calendar():
    calendar = build_xnys_calendar_artifact(date(2016, 7, 11), date(2026, 12, 31))
    assert calendar.calendar_artifact_id == INVENTORY_CALENDAR_ARTIFACT_ID
    assert calendar.sha256 == INVENTORY_CALENDAR_ARTIFACT_SHA256
    assert len(calendar.sessions) == 2_635
    return calendar


def _bundle() -> S7SchemaEvidenceApprovalBundle:
    return S7SchemaEvidenceApprovalBundle.create(
        recorded_by="s7-schema-evidence-recorder",
        recorded_at_utc=RECORDED_AT,
        exact_approval_text=APPROVAL_TEXT,
    )


def _prepare(root: Path, exact_calendar):
    write_xnys_calendar_artifact(root, exact_calendar)
    store = IdentityMarketInventoryPlanStore(root)
    bundle = _bundle()
    bundle_receipt = store.store_schema_evidence_bundle(bundle)
    plan = S7CompositeInventoryPlan.create(
        created_by="s7-composite-inventory-planner",
        created_at_utc=PLANNED_AT,
        git_commit=GIT_COMMIT,
        approval=bundle,
        approval_receipt=bundle_receipt,
        calendar_artifact_id=exact_calendar.calendar_artifact_id,
        calendar_artifact_sha256=exact_calendar.sha256,
    )
    plan_receipt = store.store_plan(plan)
    request = S7CompositeInventoryApprovalRequest.create(
        plan,
        plan_receipt,
        created_by="s7-composite-inventory-requester",
        created_at_utc=REQUESTED_AT,
    )
    request_receipt = store.store_approval_request(request)
    return store, bundle, bundle_receipt, plan, plan_receipt, request, request_receipt


def test_exact_approval_text_contracts_and_evidence_are_atomic() -> None:
    bundle = _bundle()

    assert len(APPROVAL_TEXT.encode("utf-8")) == 1_534
    assert APPROVAL_TEXT_SHA256 == (
        "ceb0160c00aef8a69f09570266a55648cfec3ff044acf451334c26ee374c00b9"
    )
    assert hashlib.sha256(APPROVAL_TEXT.encode("utf-8")).hexdigest() == APPROVAL_TEXT_SHA256
    assert bundle.approval_text_sha256 == APPROVAL_TEXT_SHA256
    assert len(bundle.contract_pins) == 6
    assert tuple(bundle.contract_pins) == APPROVED_CONTRACT_PINS
    assert bundle.evidence_manifest_id == EXTERNAL_EVIDENCE_MANIFEST_ID
    assert bundle.evidence_manifest_path == EXTERNAL_EVIDENCE_MANIFEST_PATH
    assert bundle.evidence_manifest_sha256 == EXTERNAL_EVIDENCE_MANIFEST_SHA256
    assert bundle.subject_git_commit.startswith("04540a6")
    assert bundle.document["subject_git_provenance"]["commit_short"] == "04540a6"

    for pin in bundle.contract_pins:
        candidate_path = REPO_ROOT / pin.candidate_path
        candidate = json.loads(candidate_path.read_bytes())
        assert sha256_file(candidate_path) == pin.candidate_sha256
        assert candidate["contract_id"] == pin.contract_id
        assert candidate["table"] == pin.table
        assert candidate["domain"] == pin.domain

    evidence_path = REPO_ROOT / bundle.evidence_manifest_path
    evidence = json.loads(evidence_path.read_bytes())
    assert sha256_file(evidence_path) == bundle.evidence_manifest_sha256
    assert evidence["manifest_id"] == bundle.evidence_manifest_id


def test_schema_evidence_receipt_does_not_authorize_gate_a_or_later_actions() -> None:
    scope = dict(_bundle().document["approval_scope"])

    assert scope["schema_contracts_approved"] is True
    assert scope["external_evidence_admitted_for_schema_review"] is True
    for key in (
        "adjudication_plan_authorized",
        "composite_inventory_execution_authorized",
        "full_run_authorized",
        "identity_market_consistency_scan_authorized",
        "publish_authorized",
        "registry_release_authorized",
        "table_materialization_authorized",
    ):
        assert scope[key] is False
    assert "plan_and_request_generation_only" not in scope


def test_source_caps_calendar_and_prior_preview_lineage_are_exact(
    tmp_path: Path, exact_calendar
) -> None:
    _, _, _, plan, _, _, _ = _prepare(tmp_path, exact_calendar)
    document = plan.document
    source = document["source_binding"]
    selection = document["selection"]
    lineage = document["preview_lineage"]

    assert tuple(plan.source_pins) == EXACT_SOURCE_PINS
    assert {item.table for item in plan.source_pins} == set(S7_SOURCE_PINS)
    assert source["inventory_authority_table"] == "asset_observation_daily"
    assert source["reconciliation_only_table"] == "universe_source_daily"
    assert source["daily_source_totals"] == {
        "artifact_count": DAILY_SOURCE_ARTIFACT_COUNT,
        "row_count": DAILY_SOURCE_ROW_COUNT,
        "stored_bytes": DAILY_SOURCE_BYTES,
    }
    assert selection == {
        "caller_date_filter_allowed": False,
        "caller_ticker_filter_allowed": False,
        "end_session": INVENTORY_END_SESSION.isoformat(),
        "locale": "us",
        "market": "stocks",
        "session_count": INVENTORY_SESSION_COUNT,
        "start_session": INVENTORY_START_SESSION.isoformat(),
        "ticker_scope": "all_provider_tickers_active_and_inactive",
    }
    assert lineage["preview_plan_id"] == (
        "b0cccdd8303b25a1af9a7f145dd3f95356d16d5e05fa527c8fd5cb22f7fd4fa8"
    )
    assert lineage["preview_approval_id"] == (
        "b941f839bdd524fc901f7db26c1a4fd1dfe523efa97f09ab14c3986586cdd306"
    )
    assert lineage["completion_id"] == (
        "7a1e2386e18428aecf50a9ce322eaaf6b3035307b4a704939584288f131c6b9d"
    )
    assert lineage["completion_sha256"] == (
        "2d57dffb3602f8ae77f0f733ac11dbd88dc610fcc233b773d5eb3a3ce5a081bf"
    )
    assert lineage["case_count"] == 19
    assert lineage["case_evidence_set_digest"] == (
        "d19f8a1abbf83a4aacf50844792d9bb2eaca741fbe8e2d010381eb3b7619b907"
    )


def test_plan_is_inventory_only_and_has_no_classification_or_override_capability(
    tmp_path: Path, exact_calendar
) -> None:
    _, _, _, plan, _, _, _ = _prepare(tmp_path, exact_calendar)
    capabilities = plan.document["capabilities"]
    output = plan.document["output_contract"]

    assert all(value is False for value in capabilities.values())
    assert output["status_after_success"] == "awaiting_review"
    assert output["contains_market_classification"] is False
    assert output["contains_canonical_identity"] is False
    assert output["contains_backtest_eligibility"] is False
    assert output["inventory_row_hard_cap"] == 100_000
    assert output["denominator_role"] == ("valid_distinct_provider_observed_composite_figi_domain")
    assert output["reconciliation_rows_are_not_inventory_observations"] is True
    assert "market_class" not in output["inventory_columns"]
    assert "canonical_composite_figi" not in output["inventory_columns"]


def test_store_round_trip_is_content_addressed_exact_and_idempotent(
    tmp_path: Path, exact_calendar
) -> None:
    store, bundle, bundle_receipt, plan, plan_receipt, request, request_receipt = _prepare(
        tmp_path, exact_calendar
    )

    assert store.store_schema_evidence_bundle(bundle) == bundle_receipt
    assert store.store_plan(plan) == plan_receipt
    assert store.store_approval_request(request) == request_receipt
    assert store.load_schema_evidence_bundle(bundle.approval_id, expected_sha256=bundle.sha256) == (
        bundle,
        bundle_receipt,
    )
    assert store.load_plan(plan.plan_id, expected_sha256=plan.sha256) == (
        plan,
        plan_receipt,
    )
    assert store.load_approval_request(
        request.request_event_id, expected_sha256=request.sha256
    ) == (request, request_receipt)
    assert bundle.content.endswith(b"\n")
    assert plan.content.endswith(b"\n")
    assert request.content.endswith(b"\n")
    assert bundle.relative_path == schema_evidence_approval_path(bundle.approval_id)
    assert plan.relative_path == composite_inventory_plan_path(plan.plan_id)
    assert request.relative_path == composite_inventory_approval_request_path(
        request.request_event_id
    )
    assert not any("latest" in path.parts for path in tmp_path.rglob("*"))
    assert not any("plan-approvals" in path.as_posix() for path in tmp_path.rglob("*"))


def test_frozen_plan_request_ids_and_canonical_literal(tmp_path: Path, exact_calendar) -> None:
    _, bundle, _, plan, _, request, _ = _prepare(tmp_path, exact_calendar)

    # Frozen vectors detect any change to the approved package, exact inputs, or capabilities.
    assert bundle.approval_id == (
        "9308fcb15b3bc1245dfd59d81133348018f8b167e07e9308c9406a293e4b9541"
    )
    assert bundle.sha256 == ("8162013c278944ad0c0fe5bd3eef6d9447d3eff12c4a7a47d218bbc5691aa6a7")
    assert plan.plan_id == ("a10177af6834e8dcf14e7f4ae70a8999ab35e465690e4815c44e4130f20f5d17")
    assert plan.sha256 == ("2f4183d63ccdb8b141633fb903951d97d115f6e69c53673f8d0529f6286da48e")
    assert request.request_event_id == (
        "d7a3af73a7a9895796729f6743b179e5577a552306db70ebaa9f71530883855d"
    )
    assert request.sha256 == ("8ceb9f5c546463c507fc3a06aedc63f78c2789649e6bf5c6776ea742fc6fe49b")

    literal = json.loads(request.canonical_approval_literal)
    assert literal == {
        "authorized_action": MARKET_INVENTORY_AUTHORIZED_ACTION,
        "input_binding_digest": plan.input_binding_digest,
        "literal_version": "s7_composite_inventory_approval_literal_v1",
        "plan_id": plan.plan_id,
        "plan_sha256": plan.sha256,
        "request_event_id": request.request_event_id,
        "request_event_sha256": request.sha256,
        "resource_caps_digest": plan.resource_caps.digest,
    }


def test_tampered_bundle_caps_calendar_or_receipt_fails_closed(exact_calendar) -> None:
    bundle = _bundle()
    with pytest.raises(IdentityMarketInventoryPlanError, match="approval text"):
        S7SchemaEvidenceApprovalBundle.create(
            recorded_by="recorder",
            recorded_at_utc=RECORDED_AT,
            exact_approval_text=APPROVAL_TEXT + "\n",
        )
    with pytest.raises(IdentityMarketInventoryPlanError, match="resource caps"):
        replace(S7MarketInventoryResourceCaps(), worker_count=2)
    with pytest.raises(IdentityMarketInventoryPlanError, match="calendar binding"):
        S7CompositeInventoryPlan(
            created_by="planner",
            created_at_utc=PLANNED_AT,
            git_commit=GIT_COMMIT,
            schema_approval_id=bundle.approval_id,
            schema_approval_path=bundle.relative_path,
            schema_approval_sha256=bundle.sha256,
            schema_package_digest=bundle.package_digest,
            calendar_artifact_id="0" * 64,
            calendar_artifact_sha256=exact_calendar.sha256,
        )
    with pytest.raises(IdentityMarketInventoryPlanError, match="receipt differs"):
        S7CompositeInventoryPlan.create(
            created_by="planner",
            created_at_utc=PLANNED_AT,
            git_commit=GIT_COMMIT,
            approval=bundle,
            approval_receipt=StoredIdentityMarketInventoryDocument(
                bundle.relative_path, "0" * 64, len(bundle.content)
            ),
            calendar_artifact_id=exact_calendar.calendar_artifact_id,
            calendar_artifact_sha256=exact_calendar.sha256,
        )


def test_store_rejects_missing_dependencies_and_wrong_exact_sha(
    tmp_path: Path, exact_calendar
) -> None:
    store, bundle, _, plan, _, request, _ = _prepare(tmp_path, exact_calendar)

    with pytest.raises(IdentityMarketInventoryPlanError, match="SHA-256 differs"):
        store.load_plan(plan.plan_id, expected_sha256="0" * 64)

    missing_root = tmp_path / "missing"
    missing_root.mkdir()
    write_xnys_calendar_artifact(missing_root, exact_calendar)
    missing_store = IdentityMarketInventoryPlanStore(missing_root)
    with pytest.raises(IdentityMarketInventoryPlanError, match="missing"):
        missing_store.store_plan(plan)

    missing_calendar_root = tmp_path / "missing-calendar"
    missing_calendar_root.mkdir()
    missing_calendar_store = IdentityMarketInventoryPlanStore(missing_calendar_root)
    missing_calendar_store.store_schema_evidence_bundle(bundle)
    with pytest.raises(IdentityMarketInventoryPlanError, match="calendar binding"):
        missing_calendar_store.store_plan(plan)

    with pytest.raises(IdentityMarketInventoryPlanError, match="wrong type"):
        store.store_approval_request(plan)  # type: ignore[arg-type]
    assert request.plan_id == plan.plan_id


def test_store_rejects_noncanonical_duplicate_and_symlink_documents(
    tmp_path: Path, exact_calendar
) -> None:
    store, bundle, bundle_receipt, _, _, _, _ = _prepare(tmp_path, exact_calendar)
    path = tmp_path / bundle_receipt.path
    path.chmod(0o644)

    noncanonical = bundle.content + b"\n"
    path.write_bytes(noncanonical)
    with pytest.raises(IdentityMarketInventoryPlanError, match="canonical JSON"):
        store.load_schema_evidence_bundle(
            bundle.approval_id,
            expected_sha256=hashlib.sha256(noncanonical).hexdigest(),
        )

    duplicate = f'{{"approval_id":"{bundle.approval_id}",'.encode() + bundle.content[1:]
    path.write_bytes(duplicate)
    with pytest.raises(IdentityMarketInventoryPlanError, match="duplicate JSON keys"):
        store.load_schema_evidence_bundle(
            bundle.approval_id,
            expected_sha256=hashlib.sha256(duplicate).hexdigest(),
        )

    path.unlink()
    target = tmp_path / "outside.json"
    target.write_bytes(bundle.content)
    path.symlink_to(target)
    with pytest.raises(IdentityMarketInventoryPlanError, match="symlink"):
        store.load_schema_evidence_bundle(
            bundle.approval_id,
            expected_sha256=bundle.sha256,
        )


def test_store_rejects_symlink_data_root(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(IdentityMarketInventoryPlanError, match="symlink"):
        IdentityMarketInventoryPlanStore(linked)


def test_module_has_no_execution_approval_runner_or_network_capability() -> None:
    assert not hasattr(inventory_plan_module, "S7CompositeInventoryPlanApproval")
    assert not hasattr(inventory_plan_module, "S7CompositeInventoryRunner")
    assert not hasattr(IdentityMarketInventoryPlanStore, "store_approval")
    assert not hasattr(IdentityMarketInventoryPlanStore, "load_approval")

    tree = ast.parse(Path(inventory_plan_module.__file__).read_text(encoding="utf-8"))
    imported_roots = {
        node.names[0].name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import)
    }
    imported_roots.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert imported_roots.isdisjoint(
        {"httpx", "requests", "urllib", "socket", "subprocess", "pyarrow", "polars"}
    )
