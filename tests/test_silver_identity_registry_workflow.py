from __future__ import annotations

import hashlib
import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import ame_stocks_api.silver.identity_registry_exact_group_scopes as exact_group_scopes
import ame_stocks_api.silver.identity_registry_workflow as registry_workflow
from ame_stocks_api.artifacts import stable_digest, write_bytes_immutable
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArrowType
from ame_stocks_api.silver.identity_registry_workflow import (
    APPROVAL_ACTION,
    FIXED_DECISION_SCOPE_SPECS,
    REGISTRY_ORDER,
    RUNTIME_BINDING_PATHS,
    STANDING_AUTHORIZATION_CAPABILITIES,
    STANDING_AUTHORIZATION_LITERAL,
    STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES,
    STANDING_REAFFIRMATION_LITERAL,
    ExactArtifactBinding,
    ExactSourceRow,
    ExactSourceScope,
    LoadedRegistryReleaseSet,
    RegistryCandidateManifest,
    RegistryDecisionCandidate,
    RegistryName,
    RegistryReleasePin,
    RegistryRuntimeBinding,
    RegistryStandingApprovalReceipt,
    RegistryWorkflowError,
    RuntimeFilePin,
    build_approved_registry_rows,
    build_registry_authorization_document,
    capture_registry_runtime_binding,
    create_approval_request,
    create_decision_plan,
    create_registry_decision_candidate,
    current_registry_contract_pin,
    load_registry_release,
    publish_release,
    publish_release_under_standing_authority,
    record_exact_approval,
    record_production_prerequisite_authorization,
    record_standing_approval,
    record_standing_candidate_authorization,
    store_approval_request,
    store_candidate,
    store_decision_plan,
    validate_fixed_decision_candidate,
)
from ame_stocks_api.silver.identity_relation_registries import (
    AssetTransitionDecision,
    AssetTransitionDisposition,
    AssetTransitionType,
    ProviderCompositeOverrideDecision,
    ProviderCompositeOverrideDisposition,
    ShareClassAdjudicationDecision,
    ShareClassAdjudicationDisposition,
)
from ame_stocks_api.silver.identity_resolution_contract import S7_ADJUDICATION_CONTRACTS

S4_RELEASE = "1" * 64
SOURCE_ID = "2" * 64
SOURCE_COMPLETION_ID = "a" * 64
EVIDENCE_ID = "3" * 64
DUMMY_PLAN = "4" * 64
DUMMY_PLAN_SHA = "5" * 64
DUMMY_REQUEST = "6" * 64
DUMMY_REQUEST_SHA = "7" * 64
DUMMY_RECEIPT = "8" * 64
DUMMY_RECEIPT_SHA = "9" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _runtime_binding(*, marker: str = "a") -> RegistryRuntimeBinding:
    return RegistryRuntimeBinding(
        git_commit=marker * 40,
        git_tree=("b" if marker != "b" else "c") * 40,
        files=tuple(
            RuntimeFilePin(
                path=path,
                git_mode="100644",
                git_blob_id=marker * 40,
                sha256=marker * 64,
                bytes=1,
            )
            for path in sorted(RUNTIME_BINDING_PATHS)
        ),
        python_implementation="CPython",
        python_version="3.13.5",
        pyarrow_version="20.0.0",
    )


def _artifact(
    root: Path,
    *,
    role: str,
    artifact_id: str,
    path: str,
    available: date,
) -> ExactArtifactBinding:
    content = _canonical({"manifest_id": artifact_id, "role": role})
    receipt = write_bytes_immutable(root, root / path, content)
    return ExactArtifactBinding(
        role=role,
        artifact_id=artifact_id,
        path=str(receipt["path"]),
        sha256=str(receipt["sha256"]),
        bytes=int(receipt["bytes"]),
        available_session=available,
        embedded_id_field="manifest_id",
    )


def _authorization(
    root: Path,
    *,
    role: str,
    registry_name: str,
    targets: tuple[tuple[str, str], ...],
) -> ExactArtifactBinding:
    document = build_registry_authorization_document(
        authorization_type=role,
        registry_name=registry_name,
        target_refs=targets,
        approved_by="fixture-authorization-reviewer",
        approved_at_utc=datetime(2026, 7, 17, 14, tzinfo=UTC),
        approval_available_session=date(2026, 7, 20),
    )
    content = _canonical(document)
    path = f"inputs/{role}.json"
    receipt = write_bytes_immutable(root, root / path, content)
    return ExactArtifactBinding(
        role=role,
        artifact_id=str(document["authorization_id"]),
        path=str(receipt["path"]),
        sha256=str(receipt["sha256"]),
        bytes=int(receipt["bytes"]),
        available_session=date(2026, 7, 20),
        embedded_id_field="authorization_id",
    )


def _source_row(
    ticker: str,
    session: date,
    composite: str,
    *,
    share_class: str | None = "BBG001S87NT0",
) -> ExactSourceRow:
    return ExactSourceRow(
        session_date=session,
        source_record_id=stable_digest(
            {"composite": composite, "session": session.isoformat(), "ticker": ticker}
        ),
        source_dataset="asset_observation_daily",
        source_s4_release_set_id=S4_RELEASE,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=ticker,
        observed_composite_figi=composite,
        observed_share_class_figi=share_class,
        primary_exchange_mic="XNYS",
    )


@dataclass(frozen=True)
class _FixtureExactGroupScopes:
    scopes: dict[str, ExactSourceScope]

    def require_scope(self, case_key: str) -> ExactSourceScope:
        return self.scopes[case_key]


def _install_exact_group_scope_fixture(
    monkeypatch: pytest.MonkeyPatch,
    *,
    case_key: str,
    scope: ExactSourceScope,
) -> None:
    monkeypatch.setattr(
        exact_group_scopes,
        "load_identity_registry_exact_group_scopes",
        lambda *_args, **_kwargs: _FixtureExactGroupScopes({case_key: scope}),
    )


def _dummy_common(*, source_id: str, evidence_id: str) -> dict[str, object]:
    return {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "source_s4_release_set_id": S4_RELEASE,
        "source_exact_group_candidate_manifest_id": source_id,
        "source_exact_group_candidate_manifest_sha256": "a" * 64,
        "candidate_available_session": date(2026, 7, 20),
        "source_external_evidence_manifest_id": evidence_id,
        "source_external_evidence_manifest_sha256": "b" * 64,
        "external_evidence_available_session": date(2026, 7, 20),
        "source_decision_plan_id": DUMMY_PLAN,
        "source_decision_plan_path": "controls/dummy-plan.json",
        "source_decision_plan_sha256": DUMMY_PLAN_SHA,
        "approval_request_event_id": DUMMY_REQUEST,
        "approval_request_event_sha256": DUMMY_REQUEST_SHA,
        "approval_receipt_id": DUMMY_RECEIPT,
        "approval_receipt_sha256": DUMMY_RECEIPT_SHA,
        "approved_by": "fixture-reviewer",
        "approved_at_utc": datetime(2026, 7, 20, 14, tzinfo=UTC),
        "approval_available_session": date(2026, 7, 21),
        "availability_calendar_id": "c" * 64,
        "availability_calendar_sha256": "d" * 64,
    }


def _sor_transition(scope: ExactSourceScope) -> AssetTransitionDecision:
    return AssetTransitionDecision(
        **_dummy_common(source_id=SOURCE_ID, evidence_id=EVIDENCE_ID),
        observed_ticker="SOR",
        transition_type=AssetTransitionType.CORPORATE_REORGANIZATION_SUCCESSOR_SECURITY,
        legal_effective_date=date(2025, 1, 1),
        predecessor_last_session=date(2024, 12, 31),
        successor_first_session=date(2025, 1, 2),
        predecessor_composite_figi="BBG000KMY6N2",
        successor_composite_figi="BBG01RK6N4M5",
        boundary_source_record_ids=scope.source_record_ids,
        disposition=AssetTransitionDisposition.CONFIRMED_GENUINE_TRANSITION,
        decision_version=1,
        supersedes_asset_transition_id=None,
        reason_code="source_capital_reorganization",
        reason_detail="Official evidence establishes the successor security boundary.",
    )


def _sor_override(
    scope: ExactSourceScope,
    transition: AssetTransitionDecision,
    *,
    controls: dict[str, object] | None = None,
) -> ProviderCompositeOverrideDecision:
    common = _dummy_common(source_id=SOURCE_ID, evidence_id=EVIDENCE_ID)
    if controls:
        common.update(controls)
    return ProviderCompositeOverrideDecision(
        **common,
        observed_ticker="SOR",
        observed_composite_figi="BBG000KMY6N2",
        canonical_composite_figi="BBG01RK6N4M5",
        observed_composite_market_code="US",
        canonical_composite_market_code="US",
        valid_from_session=date(2025, 1, 2),
        valid_through_session=date(2026, 7, 9),
        scoped_source_record_ids=scope.source_record_ids,
        asset_transition_series_id=transition.asset_transition_series_id,
        asset_transition_id=transition.asset_transition_id,
        asset_transition_available_session=date(2026, 7, 20),
        disposition=ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION,
        decision_version=1,
        supersedes_provider_composite_override_id=None,
        reason_code="same_market_stale_after_transition",
        reason_detail="Provider retained the predecessor Composite after the successor began.",
    )


def _share_class_decision(
    scope: ExactSourceScope,
    *,
    ticker: str,
    composite: str,
    observed: str,
    canonical: str,
) -> ShareClassAdjudicationDecision:
    return ShareClassAdjudicationDecision(
        **_dummy_common(source_id=SOURCE_ID, evidence_id=EVIDENCE_ID),
        observed_ticker=ticker,
        observed_composite_figi=composite,
        required_unique_canonical_composite_figi=composite,
        observed_share_class_figi=observed,
        canonical_share_class_figi=canonical,
        valid_from_session=scope.rows[0].session_date,
        valid_through_session=scope.rows[-1].session_date,
        scoped_source_record_ids=scope.source_record_ids,
        disposition=ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION,
        decision_version=1,
        supersedes_share_class_adjudication_id=None,
        reason_code="frozen_share_class_correction",
        reason_detail="Exact external hierarchy evidence supports this correction.",
    )


def test_fixed_sor_xzo_and_anabv_candidates_preserve_registry_responsibilities() -> None:
    transition_scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2024, 12, 31), "BBG000KMY6N2"),
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
        )
    )
    transition = _sor_transition(transition_scope)
    transition_candidate = create_registry_decision_candidate(
        registry_name=RegistryName.ASSET_TRANSITION.value,
        case_key="asset_transition:SOR",
        proposed_contract_row=transition.to_registry_row(),
        source_scope=transition_scope,
    )
    validate_fixed_decision_candidate(transition_candidate)

    override_sessions = tuple(
        item.session_date
        for item in build_xnys_calendar_artifact(date(2025, 1, 2), date(2026, 7, 9)).sessions
    )
    assert len(override_sessions) == 379
    override_scope = ExactSourceScope(
        rows=tuple(_source_row("SOR", session, "BBG000KMY6N2") for session in override_sessions)
    )
    override_candidate = create_registry_decision_candidate(
        registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        case_key="provider_composite_override:SOR",
        proposed_contract_row=_sor_override(override_scope, transition).to_registry_row(),
        source_scope=override_scope,
    )
    validate_fixed_decision_candidate(override_candidate)
    assert transition_candidate.decision_id != override_candidate.decision_id

    for ticker, composite, observed, canonical, sessions in (
        (
            "XZO",
            "BBG01XL8FHT0",
            "BBG01XL8FJS7",
            "BBG01227MF17",
            (date(2025, 11, 4), date(2025, 11, 5)),
        ),
        (
            "ANABV",
            "BBG021DMXXT2",
            "BBG0026ZDHT8",
            "BBG021GNPBR6",
            (date(2026, 4, 6),),
        ),
    ):
        scope = ExactSourceScope(
            rows=tuple(
                _source_row(ticker, session, composite, share_class=observed)
                for session in sessions
            )
        )
        decision = _share_class_decision(
            scope,
            ticker=ticker,
            composite=composite,
            observed=observed,
            canonical=canonical,
        )
        candidate = create_registry_decision_candidate(
            registry_name=RegistryName.SHARE_CLASS_ADJUDICATION.value,
            case_key=f"share_class_adjudication:{ticker}",
            proposed_contract_row=decision.to_registry_row(),
            source_scope=scope,
        )
        validate_fixed_decision_candidate(candidate)
        assert candidate.frozen_row_claims["asset_id_effect"] == "none"


def test_relation_candidates_recompute_all_row_local_derived_ids() -> None:
    transition_scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2024, 12, 31), "BBG000KMY6N2"),
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
        )
    )
    transition = _sor_transition(transition_scope)
    transition_candidate = create_registry_decision_candidate(
        registry_name=RegistryName.ASSET_TRANSITION.value,
        case_key="asset_transition:SOR",
        proposed_contract_row=transition.to_registry_row(),
        source_scope=transition_scope,
    )
    override_scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
            _source_row("SOR", date(2026, 7, 9), "BBG000KMY6N2"),
        )
    )
    override_candidate = create_registry_decision_candidate(
        registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        case_key="provider_composite_override:SOR",
        proposed_contract_row=_sor_override(override_scope, transition).to_registry_row(),
        source_scope=override_scope,
    )
    share_scope = ExactSourceScope(
        rows=(
            _source_row(
                "ANABV",
                date(2026, 4, 6),
                "BBG021DMXXT2",
                share_class="BBG0026ZDHT8",
            ),
        )
    )
    share_candidate = create_registry_decision_candidate(
        registry_name=RegistryName.SHARE_CLASS_ADJUDICATION.value,
        case_key="share_class_adjudication:ANABV",
        proposed_contract_row=_share_class_decision(
            share_scope,
            ticker="ANABV",
            composite="BBG021DMXXT2",
            observed="BBG0026ZDHT8",
            canonical="BBG021GNPBR6",
        ).to_registry_row(),
        source_scope=share_scope,
    )
    cases = (
        (
            transition_candidate,
            (
                "asset_transition_id",
                "asset_transition_series_id",
                "asset_transition_subject_id",
                "boundary_source_record_set_digest",
                "predecessor_asset_id",
                "successor_asset_id",
            ),
        ),
        (
            override_candidate,
            (
                "canonical_asset_id",
                "provider_composite_override_id",
                "provider_composite_override_series_id",
                "provider_composite_override_subject_id",
                "scoped_source_record_set_digest",
            ),
        ),
        (
            share_candidate,
            (
                "canonical_share_class_id",
                "scoped_source_record_set_digest",
                "share_class_adjudication_id",
                "share_class_adjudication_series_id",
                "share_class_adjudication_subject_id",
            ),
        ),
    )
    for candidate, fields in cases:
        id_column = {
            RegistryName.ASSET_TRANSITION.value: "asset_transition_id",
            RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value: ("provider_composite_override_id"),
            RegistryName.SHARE_CLASS_ADJUDICATION.value: "share_class_adjudication_id",
        }[candidate.registry_name]
        for field in fields:
            claims = {**candidate.frozen_row_claims, field: "f" * 64}
            with pytest.raises(RegistryWorkflowError):
                RegistryDecisionCandidate(
                    registry_name=candidate.registry_name,
                    case_key=candidate.case_key,
                    decision_id=("f" * 64 if field == id_column else candidate.decision_id),
                    decision_version=candidate.decision_version,
                    supersedes_decision_id=candidate.supersedes_decision_id,
                    frozen_row_claims=claims,
                    source_scope=candidate.source_scope,
                )


def _default_value(arrow_type: ArrowType, nullable: bool) -> object:
    if nullable:
        return None
    if arrow_type in {ArrowType.STRING, ArrowType.JSON_STRING}:
        return "fixture"
    if arrow_type is ArrowType.BOOLEAN:
        return False
    if arrow_type is ArrowType.INT64:
        return 1
    if arrow_type is ArrowType.FLOAT64:
        return 0.0
    if arrow_type is ArrowType.DATE32:
        return date(2026, 7, 20)
    if arrow_type is ArrowType.TIMESTAMP_NS_UTC:
        return datetime(2026, 7, 20, 14, tzinfo=UTC)
    if arrow_type is ArrowType.LIST_STRING:
        return []
    raise AssertionError(arrow_type)


def _cross_market_candidate(case_key: str):
    spec = FIXED_DECISION_SCOPE_SPECS[case_key]
    if spec.expected_source_row_count == 15:
        sessions = (
            date(2022, 2, 9),
            date(2022, 2, 10),
            date(2022, 2, 11),
            date(2022, 2, 14),
            date(2022, 2, 15),
            date(2022, 2, 16),
            date(2022, 2, 17),
            date(2022, 2, 18),
            date(2022, 2, 22),
            date(2022, 2, 23),
            date(2022, 2, 24),
            date(2022, 2, 25),
            date(2022, 2, 28),
            date(2022, 3, 1),
            date(2022, 3, 2),
        )
    else:
        sessions = (spec.valid_from_session,)
    scope = ExactSourceScope(
        rows=tuple(
            _source_row(
                spec.ticker,
                session,
                str(spec.observed_composite_figi),
                share_class=spec.observed_share_class_figi,
            )
            for session in sessions
        )
    )
    contract = S7_ADJUDICATION_CONTRACTS[RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value]
    row: dict[str, Any] = {
        column.name: _default_value(column.arrow_type, column.nullable)
        for column in contract.columns
    }
    decision_id = stable_digest({"fixed_case": case_key})
    case_count = 3 if len(scope.rows) == 15 else 1
    case_ids = sorted(
        stable_digest({"case": case_key, "index": index}) for index in range(case_count)
    )
    case_roles = {
        case_id: (
            "inverse_middle_is_canonical_us"
            if case_count == 3 and index == 2
            else "contaminated_middle_episode"
        )
        for index, case_id in enumerate(case_ids)
    }
    source_ids_json = json.dumps(
        list(scope.source_record_ids), separators=(",", ":"), sort_keys=True
    )
    row.update(
        {
            "cross_market_adjudication_id": decision_id,
            "cross_market_series_id": stable_digest({"series": spec.ticker}),
            "decision_version": 1,
            "supersedes_cross_market_adjudication_id": None,
            "cross_market_subject_id": stable_digest({"subject": spec.ticker}),
            "cross_market_scope_id": stable_digest({"scope": spec.ticker}),
            "provider_id": "massive",
            "provider_market": "stocks",
            "provider_locale": "us",
            "observed_ticker": spec.ticker,
            "share_class_figi": spec.observed_share_class_figi,
            "observed_foreign_composite_figi": spec.observed_composite_figi,
            "observed_composite_market_code": spec.observed_market_code,
            "canonical_us_composite_figi": spec.canonical_composite_figi,
            "canonical_composite_market_code": "US",
            "canonical_asset_id": stable_digest({"asset": spec.canonical_composite_figi}),
            "valid_from_session": spec.valid_from_session,
            "valid_through_session": spec.valid_through_session,
            "scoped_source_record_count": len(scope.rows),
            "scoped_source_record_set_digest": scope.source_record_set_digest,
            "scoped_source_record_ids_json": source_ids_json,
            "related_identity_case_count": case_count,
            "related_identity_case_ids_json": json.dumps(
                case_ids, separators=(",", ":"), sort_keys=True
            ),
            "related_identity_case_roles_json": json.dumps(
                case_roles, separators=(",", ":"), sort_keys=True
            ),
            "identity_disposition": "confirmed_provider_contamination",
            "canonical_override": True,
            "identity_effect": "canonical_research_identity_only",
            "membership_effect": "none",
            "active_status_effect": "none",
            "identity_quality_liquidation_signal": False,
            "reason_code": "non_us_composite_in_us_locale",
            "reason_detail": "Frozen OpenFIGI evidence identifies a foreign Composite.",
            "source_s4_release_set_id": S4_RELEASE,
            "source_six_release_binding_id": "a" * 64,
            "source_identity_case_candidate_manifest_id": "b" * 64,
            "source_identity_case_candidate_manifest_sha256": "c" * 64,
            "source_identity_market_consistency_candidate_manifest_id": "d" * 64,
            "source_identity_market_consistency_candidate_manifest_sha256": "e" * 64,
            "candidate_available_session": date(2026, 7, 20),
            "source_external_evidence_manifest_id": "f" * 64,
            "source_external_evidence_manifest_sha256": "0" * 64,
            "external_evidence_available_session": date(2026, 7, 20),
            "evidence_claim_digest": stable_digest({"claim": spec.ticker}),
            "outcome_or_backtest_evidence_used": False,
            "availability_calendar_id": "1" * 64,
            "availability_calendar_sha256": "2" * 64,
        }
    )
    return create_registry_decision_candidate(
        registry_name=RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
        case_key=case_key,
        proposed_contract_row=row,
        source_scope=scope,
    )


def test_all_nine_cross_market_specs_require_exact_foreign_rows() -> None:
    keys = sorted(
        key
        for key in FIXED_DECISION_SCOPE_SPECS
        if key.startswith("identity_cross_market_adjudication:")
    )
    assert len(keys) == 9
    assert sum(FIXED_DECISION_SCOPE_SPECS[key].expected_source_row_count or 0 for key in keys) == 79
    assert (
        sum(
            3 if FIXED_DECISION_SCOPE_SPECS[key].expected_source_row_count == 15 else 1
            for key in keys
        )
        == 19
    )
    for key in keys:
        candidate = _cross_market_candidate(key)
        validate_fixed_decision_candidate(candidate)
        assert all(
            row.observed_composite_figi
            == candidate.frozen_row_claims["observed_foreign_composite_figi"]
            for row in candidate.source_scope.rows
        )


def test_full_literal_release_and_loader_replay_exact_row_and_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_binding = _runtime_binding()
    monkeypatch.setattr(
        registry_workflow,
        "capture_registry_runtime_binding",
        lambda: runtime_binding,
    )
    calendar = build_xnys_calendar_artifact(date(2026, 7, 1), date(2026, 8, 31))
    write_xnys_calendar_artifact(tmp_path, calendar)
    monkeypatch.setattr(
        registry_workflow,
        "_runtime_utc_now",
        lambda: datetime(2026, 7, 17, 14, tzinfo=UTC),
    )
    source = _artifact(
        tmp_path,
        role="source_exact_group_candidate_manifest",
        artifact_id=SOURCE_ID,
        path="inputs/source.json",
        available=date(2026, 7, 20),
    )
    completion = _artifact(
        tmp_path,
        role="source_exact_group_completion_manifest",
        artifact_id=SOURCE_COMPLETION_ID,
        path="inputs/source-completion.json",
        available=date(2026, 7, 20),
    )
    evidence = _artifact(
        tmp_path,
        role="external_evidence",
        artifact_id=EVIDENCE_ID,
        path="inputs/evidence.json",
        available=date(2026, 7, 20),
    )
    registry_name = RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value
    contract_pin = current_registry_contract_pin(registry_name)
    authorizations = tuple(
        record_standing_candidate_authorization(
            tmp_path,
            authorization_type=role,
            registry_name=registry_name,
            target_refs=targets,
            availability_calendar_id=calendar.calendar_artifact_id,
            availability_calendar_sha256=calendar.sha256,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )
        for role, targets in (
            (
                "external_evidence_approval",
                ((evidence.artifact_id, evidence.sha256),),
            ),
            (
                "schema_contract_approval",
                ((contract_pin.contract_id, contract_pin.resource_sha256),),
            ),
            (
                "source_candidate_approval",
                (
                    (source.artifact_id, source.sha256),
                    (completion.artifact_id, completion.sha256),
                ),
            ),
        )
    )
    first_authorization_document = json.loads(
        (tmp_path / authorizations[0].path).read_text(encoding="utf-8")
    )
    assert first_authorization_document["capabilities"] == dict(
        STANDING_CANDIDATE_AUTHORIZATION_CAPABILITIES
    )
    assert first_authorization_document["capabilities"]["network_access"] is False
    assert "remote_execution" not in first_authorization_document["capabilities"]
    transition_scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2024, 12, 31), "BBG000KMY6N2"),
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
        )
    )
    transition = _sor_transition(transition_scope)
    scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
            _source_row("SOR", date(2026, 7, 9), "BBG000KMY6N2"),
        )
    )
    draft = _sor_override(
        scope,
        transition,
        controls={
            "source_exact_group_candidate_manifest_sha256": source.sha256,
            "source_external_evidence_manifest_sha256": evidence.sha256,
            "availability_calendar_id": calendar.calendar_artifact_id,
            "availability_calendar_sha256": calendar.sha256,
        },
    )
    intent = create_registry_decision_candidate(
        registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        case_key="provider_composite_override:SOR",
        proposed_contract_row=draft.to_registry_row(),
        source_scope=scope,
    )
    _install_exact_group_scope_fixture(
        monkeypatch,
        case_key="provider_composite_override:SOR",
        scope=scope,
    )
    candidate = RegistryCandidateManifest(
        registry_name=registry_name,
        contract_pin=contract_pin,
        source_artifacts=(source, completion),
        evidence_artifacts=(evidence,),
        authorization_artifacts=authorizations,
        availability_calendar_id=calendar.calendar_artifact_id,
        availability_calendar_sha256=calendar.sha256,
        created_at_utc=datetime(2026, 7, 19, 12, tzinfo=UTC),
        candidate_available_session=date(2026, 7, 20),
        decisions=(intent,),
    )
    with monkeypatch.context() as production_root:
        production_root.setattr(
            registry_workflow,
            "CANONICAL_PRODUCTION_DATA_ROOT",
            tmp_path,
        )
        with pytest.raises(RegistryWorkflowError, match="lacks immutable ingress provenance"):
            store_candidate(tmp_path, candidate)
        with pytest.raises(RegistryWorkflowError, match="fixture/internal"):
            record_standing_candidate_authorization(
                tmp_path,
                authorization_type="schema_contract_approval",
                registry_name=registry_name,
                target_refs=((contract_pin.contract_id, contract_pin.resource_sha256),),
                availability_calendar_id=calendar.calendar_artifact_id,
                availability_calendar_sha256=calendar.sha256,
                standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
                reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
                approved_by="joe",
            )
    candidate_doc = store_candidate(tmp_path, candidate)
    retry_candidate = replace(
        candidate,
        created_at_utc=datetime(2026, 7, 19, 13, tzinfo=UTC),
    )
    assert retry_candidate.candidate_id != candidate.candidate_id
    assert retry_candidate.candidate_scope_slot_id == candidate.candidate_scope_slot_id
    assert store_candidate(tmp_path, retry_candidate) == candidate_doc
    plan = create_decision_plan(candidate, candidate_doc)
    plan_doc = store_decision_plan(tmp_path, plan)
    request = create_approval_request(plan, plan_doc)
    request_doc = store_approval_request(tmp_path, request)

    with pytest.raises(RegistryWorkflowError, match="approval literal differs"):
        record_exact_approval(
            tmp_path,
            request=request,
            request_document=request_doc,
            literal={**request.literal_payload(), "authorized_action": "wrong"},
            approved_by="joe",
            approved_at_utc=datetime(2026, 7, 20, 14, tzinfo=UTC),
            approval_available_session=date(2026, 7, 21),
        )
    receipt, receipt_doc = record_exact_approval(
        tmp_path,
        request=request,
        request_document=request_doc,
        literal=request.literal_payload(),
        approved_by="joe",
        approved_at_utc=datetime(2026, 7, 20, 14, tzinfo=UTC),
        approval_available_session=date(2026, 7, 21),
    )
    assert request.literal_payload()["authorized_action"] == APPROVAL_ACTION

    final = _sor_override(
        scope,
        transition,
        controls={
            "source_exact_group_candidate_manifest_sha256": source.sha256,
            "source_external_evidence_manifest_sha256": evidence.sha256,
            "source_decision_plan_id": plan.plan_id,
            "source_decision_plan_path": plan_doc.path,
            "source_decision_plan_sha256": plan_doc.sha256,
            "approval_request_event_id": request.request_event_id,
            "approval_request_event_sha256": hashlib.sha256(request.literal_bytes()).hexdigest(),
            "approval_receipt_id": receipt.receipt_id,
            "approval_receipt_sha256": receipt_doc.sha256,
            "approved_by": "joe",
            "approved_at_utc": datetime(2026, 7, 20, 14, tzinfo=UTC),
            "approval_available_session": date(2026, 7, 21),
            "availability_calendar_id": calendar.calendar_artifact_id,
            "availability_calendar_sha256": calendar.sha256,
        },
    )
    with monkeypatch.context() as production_root:
        production_root.setattr(
            registry_workflow,
            "CANONICAL_PRODUCTION_DATA_ROOT",
            tmp_path,
        )
        with pytest.raises(RegistryWorkflowError, match="fixture/internal"):
            publish_release(
                tmp_path,
                plan=plan,
                plan_document=plan_doc,
                request=request,
                request_document=request_doc,
                approval_receipt=receipt,
                approval_receipt_document=receipt_doc,
                decision_rows=(final.to_registry_row(),),
                published_at_utc=datetime(2000, 1, 3, tzinfo=UTC),
                release_available_session=date(2026, 7, 22),
            )
    pin = publish_release(
        tmp_path,
        plan=plan,
        plan_document=plan_doc,
        request=request,
        request_document=request_doc,
        approval_receipt=receipt,
        approval_receipt_document=receipt_doc,
        decision_rows=(final.to_registry_row(),),
        published_at_utc=datetime(2026, 7, 21, 14, tzinfo=UTC),
        release_available_session=date(2026, 7, 22),
    )
    loaded = load_registry_release(tmp_path, pin)
    assert loaded.registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value
    with monkeypatch.context() as production_root:
        production_root.setattr(
            registry_workflow,
            "CANONICAL_PRODUCTION_DATA_ROOT",
            tmp_path,
        )
        with pytest.raises(RegistryWorkflowError, match="provenance/root binding"):
            load_registry_release(tmp_path, pin)
    replay = loaded.require_exact_source_row(
        final.provider_composite_override_id,
        scope.rows[0],
        cutoff_session=date(2026, 7, 22),
    )
    assert replay["canonical_composite_figi"] == "BBG01RK6N4M5"
    with pytest.raises(RegistryWorkflowError, match="exact released source-row scope"):
        loaded.require_exact_source_row(
            final.provider_composite_override_id,
            _source_row("SOR", date(2025, 1, 3), "BBG000KMY6N2"),
            cutoff_session=date(2026, 7, 22),
        )
    with pytest.raises(RegistryWorkflowError, match="unavailable"):
        loaded.require_decision(
            final.provider_composite_override_id,
            cutoff_session=date(2026, 7, 20),
        )


def test_standing_authority_binds_runtime_review_and_one_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_binding = _runtime_binding()
    monkeypatch.setattr(
        registry_workflow,
        "capture_registry_runtime_binding",
        lambda: runtime_binding,
    )
    calendar = build_xnys_calendar_artifact(date(2026, 7, 1), date(2026, 8, 31))
    write_xnys_calendar_artifact(tmp_path, calendar)
    monkeypatch.setattr(
        registry_workflow,
        "_runtime_utc_now",
        lambda: datetime(2026, 7, 17, 14, tzinfo=UTC),
    )
    source = _artifact(
        tmp_path,
        role="source_exact_group_candidate_manifest",
        artifact_id=SOURCE_ID,
        path="inputs/standing-source.json",
        available=date(2026, 7, 20),
    )
    completion = _artifact(
        tmp_path,
        role="source_exact_group_completion_manifest",
        artifact_id=SOURCE_COMPLETION_ID,
        path="inputs/standing-source-completion.json",
        available=date(2026, 7, 20),
    )
    evidence = _artifact(
        tmp_path,
        role="external_evidence",
        artifact_id=EVIDENCE_ID,
        path="inputs/standing-evidence.json",
        available=date(2026, 7, 20),
    )
    registry_name = RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value
    contract_pin = current_registry_contract_pin(registry_name)
    authorizations = tuple(
        record_standing_candidate_authorization(
            tmp_path,
            authorization_type=role,
            registry_name=registry_name,
            target_refs=targets,
            availability_calendar_id=calendar.calendar_artifact_id,
            availability_calendar_sha256=calendar.sha256,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )
        for role, targets in (
            (
                "external_evidence_approval",
                ((evidence.artifact_id, evidence.sha256),),
            ),
            (
                "schema_contract_approval",
                ((contract_pin.contract_id, contract_pin.resource_sha256),),
            ),
            (
                "source_candidate_approval",
                (
                    (source.artifact_id, source.sha256),
                    (completion.artifact_id, completion.sha256),
                ),
            ),
        )
    )
    transition_scope = ExactSourceScope(
        rows=(
            _source_row("SOR", date(2024, 12, 31), "BBG000KMY6N2"),
            _source_row("SOR", date(2025, 1, 2), "BBG000KMY6N2"),
        )
    )
    transition = _sor_transition(transition_scope)
    sessions = tuple(
        item.session_date
        for item in build_xnys_calendar_artifact(date(2025, 1, 2), date(2026, 7, 9)).sessions
    )
    assert len(sessions) == 379
    scope = ExactSourceScope(
        rows=tuple(_source_row("SOR", session, "BBG000KMY6N2") for session in sessions)
    )
    draft = _sor_override(
        scope,
        transition,
        controls={
            "source_exact_group_candidate_manifest_sha256": source.sha256,
            "source_external_evidence_manifest_sha256": evidence.sha256,
            "availability_calendar_id": calendar.calendar_artifact_id,
            "availability_calendar_sha256": calendar.sha256,
        },
    )
    intent = create_registry_decision_candidate(
        registry_name=registry_name,
        case_key="provider_composite_override:SOR",
        proposed_contract_row=draft.to_registry_row(),
        source_scope=scope,
    )
    _install_exact_group_scope_fixture(
        monkeypatch,
        case_key="provider_composite_override:SOR",
        scope=scope,
    )
    candidate = RegistryCandidateManifest(
        registry_name=registry_name,
        contract_pin=contract_pin,
        source_artifacts=(source, completion),
        evidence_artifacts=(evidence,),
        authorization_artifacts=authorizations,
        availability_calendar_id=calendar.calendar_artifact_id,
        availability_calendar_sha256=calendar.sha256,
        created_at_utc=datetime(2026, 7, 19, 12, tzinfo=UTC),
        candidate_available_session=date(2026, 7, 20),
        decisions=(intent,),
    )
    candidate_doc = store_candidate(tmp_path, candidate)
    plan = create_decision_plan(candidate, candidate_doc)
    plan_doc = store_decision_plan(tmp_path, plan)
    request = create_approval_request(plan, plan_doc)
    request_doc = store_approval_request(tmp_path, request)

    runtime = datetime(2026, 7, 20, 14, 0, 0, 123456, tzinfo=UTC)
    monkeypatch.setattr(registry_workflow, "_runtime_utc_now", lambda: runtime)
    with pytest.raises(RegistryWorkflowError, match="literal bytes changed"):
        publish_release_under_standing_authority(
            tmp_path,
            request_document=request_doc,
            standing_authorization_literal=(STANDING_AUTHORIZATION_LITERAL + "\n").encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )

    clock_lock = threading.Lock()
    clock_count = 0

    def racing_clock() -> datetime:
        nonlocal clock_count
        with clock_lock:
            value = runtime + timedelta(seconds=clock_count)
            clock_count += 1
            return value

    monkeypatch.setattr(registry_workflow, "_runtime_utc_now", racing_clock)
    original_store = registry_workflow._store_control
    barrier = threading.Barrier(2)

    def racing_store(*args: Any, **kwargs: Any):
        relative_path = args[2]
        if "standing-approval-slots" in relative_path:
            barrier.wait(timeout=5)
        return original_store(*args, **kwargs)

    monkeypatch.setattr(registry_workflow, "_store_control", racing_store)

    def approve_once():
        return record_standing_approval(
            tmp_path,
            request_document=request_doc,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        concurrent_results = tuple(executor.map(lambda _: approve_once(), range(2)))
    monkeypatch.setattr(registry_workflow, "_store_control", original_store)
    receipt, receipt_doc = concurrent_results[0]
    assert concurrent_results[1] == (receipt, receipt_doc)
    assert receipt.approved_at_utc in {runtime, runtime + timedelta(seconds=1)}

    rows = build_approved_registry_rows(
        tmp_path,
        request_document=request_doc,
        approval_receipt_document=receipt_doc,
    )
    drifted_runtime = _runtime_binding(marker="c")
    monkeypatch.setattr(
        registry_workflow,
        "capture_registry_runtime_binding",
        lambda: drifted_runtime,
    )
    with pytest.raises(RegistryWorkflowError, match="runtime binding drifted"):
        publish_release(
            tmp_path,
            plan=plan,
            plan_document=plan_doc,
            request=request,
            request_document=request_doc,
            approval_receipt=receipt,
            approval_receipt_document=receipt_doc,
            decision_rows=rows,
            published_at_utc=datetime(2026, 7, 20, 15, tzinfo=UTC),
            release_available_session=date(2026, 7, 21),
        )
    assert not (
        tmp_path
        / "manifests/silver/identity/registry-workflow"
        / f"registry={registry_name}"
        / "publish-intents"
    ).exists()

    monkeypatch.setattr(
        registry_workflow,
        "capture_registry_runtime_binding",
        lambda: runtime_binding,
    )
    monkeypatch.setattr(
        registry_workflow,
        "_runtime_utc_now",
        lambda: datetime(2026, 7, 20, 15, tzinfo=UTC),
    )
    original_write_release_member = registry_workflow._write_release_member
    release_member_write_count = 0

    def crash_after_first_release_member(*args: Any, **kwargs: Any) -> None:
        nonlocal release_member_write_count
        release_member_write_count += 1
        if release_member_write_count == 2:
            raise RuntimeError("fixture crash after first release member")
        original_write_release_member(*args, **kwargs)

    monkeypatch.setattr(
        registry_workflow,
        "_write_release_member",
        crash_after_first_release_member,
    )
    with pytest.raises(RuntimeError, match="fixture crash"):
        publish_release_under_standing_authority(
            tmp_path,
            request_document=request_doc,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )
    release_root = tmp_path / "manifests/silver/identity/registry-releases"
    assert not list(release_root.rglob("manifest.json"))
    monkeypatch.setattr(
        registry_workflow,
        "_write_release_member",
        original_write_release_member,
    )

    def resume_publish_once():
        return publish_release_under_standing_authority(
            tmp_path,
            request_document=request_doc,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
            approved_by="joe",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        resumed_publications = tuple(executor.map(lambda _: resume_publish_once(), range(2)))
    resumed_receipt, resumed_doc, pin = resumed_publications[0]
    assert resumed_publications[1] == resumed_publications[0]
    assert (resumed_receipt, resumed_doc) == (receipt, receipt_doc)
    assert receipt.approval_available_session == date(2026, 7, 21)
    assert receipt.exact_request_literal == request.literal_payload()
    assert receipt.source_artifacts == (source, completion)
    assert receipt.evidence_artifacts == (evidence,)
    assert receipt.capabilities == STANDING_AUTHORIZATION_CAPABILITIES
    assert receipt.capabilities["single_exact_registry_release"] is True
    assert receipt.capabilities["materialization"] is False
    assert receipt.qa_review["open_critical_qa_count"] == 0
    assert receipt.qa_review["factual_contradiction_count"] == 0
    loaded = load_registry_release(tmp_path, pin)
    assert loaded.approval_receipt == receipt
    assert loaded.release_available_session == date(2026, 7, 21)

    monkeypatch.setattr(
        registry_workflow,
        "_runtime_utc_now",
        lambda: datetime(2026, 7, 20, 16, tzinfo=UTC),
    )
    retry_receipt, retry_doc, retry_pin = publish_release_under_standing_authority(
        tmp_path,
        request_document=request_doc,
        standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode(),
        reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode(),
        approved_by="joe",
    )
    assert (retry_receipt, retry_doc, retry_pin) == (receipt, receipt_doc, pin)

    tampered = receipt.to_dict()
    tampered["capabilities"] = {**dict(receipt.capabilities), "materialization": True}
    with pytest.raises(RegistryWorkflowError, match="capabilities broadened"):
        RegistryStandingApprovalReceipt.from_dict(tampered)
    tampered = receipt.to_dict()
    review = dict(receipt.qa_review)
    review["factual_contradiction_count"] = 1
    tampered["qa_review"] = review
    with pytest.raises(RegistryWorkflowError, match="review is not clean"):
        RegistryStandingApprovalReceipt.from_dict(tampered)
    assert receipt_doc.object_id == receipt.receipt_id


@dataclass
class _FakeRelease:
    registry_name: str
    release_id: str
    source_scopes: dict[str, ExactSourceScope]
    decision_rows: dict[str, dict[str, object]]

    def decision_ids_for_exact_source_row(
        self, source_row: ExactSourceRow, *, cutoff_session: date
    ) -> tuple[str, ...]:
        del cutoff_session
        return tuple(
            sorted(
                decision_id
                for decision_id, scope in self.source_scopes.items()
                if source_row in scope.rows
            )
        )


def test_release_set_fails_closed_on_multi_composite_registry_scope() -> None:
    source_row = _source_row("AZPN", date(2022, 2, 9), "BBG000KRLLH9")
    scope = ExactSourceScope(rows=(source_row,))
    releases = []
    for index, name in enumerate(REGISTRY_ORDER):
        decision_id = stable_digest({"decision": name})
        scoped = (
            {decision_id: scope}
            if name
            in {
                RegistryName.IDENTITY_ADJUDICATION.value,
                RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
            }
            else {}
        )
        releases.append(
            _FakeRelease(
                registry_name=name,
                release_id=f"{index + 1:x}" * 64,
                source_scopes=scoped,
                decision_rows={},
            )
        )
    release_set = LoadedRegistryReleaseSet(tuple(releases))  # type: ignore[arg-type]
    with pytest.raises(RegistryWorkflowError, match="multiple Composite"):
        release_set.require_unique_composite_match(
            source_row,
            cutoff_session=date(2026, 7, 20),
        )
    with pytest.raises(RegistryWorkflowError, match="source-row collisions"):
        release_set.validate_all_composite_scopes_are_exclusive()


def test_release_pin_cannot_substitute_registry_or_availability() -> None:
    with pytest.raises(RegistryWorkflowError, match="unsupported"):
        RegistryReleasePin(
            registry_name="unknown",
            release_id="a" * 64,
            manifest_path="release.json",
            manifest_sha256="b" * 64,
            manifest_bytes=1,
            release_available_session=date(2026, 7, 20),
        )


def test_runtime_binding_rejects_dirty_checkout_and_detects_committed_source_drift(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "runtime-repo"
    repo.mkdir()
    for index, relative in enumerate(sorted(RUNTIME_BINDING_PATHS), start=1):
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"runtime fixture {index}\n", encoding="utf-8")

    def git(*args: str) -> None:
        subprocess.run(
            ("git", "-C", str(repo), *args),
            check=True,
            capture_output=True,
            text=True,
        )

    git("init", "-q")
    git("add", "--", *RUNTIME_BINDING_PATHS)
    git(
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-q",
        "-m",
        "runtime fixture",
    )
    original = capture_registry_runtime_binding(repo)
    workflow_path = repo / "backend/ame_stocks_api/silver/identity_registry_workflow.py"
    workflow_path.write_text("runtime drift\n", encoding="utf-8")
    with pytest.raises(RegistryWorkflowError, match="not clean"):
        capture_registry_runtime_binding(repo)
    git("add", "--", "backend/ame_stocks_api/silver/identity_registry_workflow.py")
    git(
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-q",
        "-m",
        "runtime drift",
    )
    changed = capture_registry_runtime_binding(repo)
    assert changed != original
    assert changed.git_commit != original.git_commit
    assert changed.git_tree != original.git_tree


def test_production_prerequisite_authorization_is_root_runtime_and_target_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_binding = _runtime_binding()
    calendar = build_xnys_calendar_artifact(date(2026, 7, 1), date(2026, 8, 31))
    write_xnys_calendar_artifact(tmp_path, calendar)
    monkeypatch.setattr(
        registry_workflow,
        "CANONICAL_PRODUCTION_DATA_ROOT",
        tmp_path,
    )
    monkeypatch.setattr(
        registry_workflow,
        "capture_registry_runtime_binding",
        lambda: runtime_binding,
    )
    monkeypatch.setattr(
        registry_workflow,
        "_runtime_utc_now",
        lambda: datetime(2026, 7, 17, 14, tzinfo=UTC),
    )
    contract = current_registry_contract_pin(RegistryName.ASSET_TRANSITION.value)
    kwargs = {
        "authorization_type": "schema_contract_approval",
        "registry_name": RegistryName.ASSET_TRANSITION.value,
        "target_refs": ((contract.contract_id, contract.resource_sha256),),
        "availability_calendar_id": calendar.calendar_artifact_id,
        "availability_calendar_sha256": calendar.sha256,
        "standing_authorization_literal": STANDING_AUTHORIZATION_LITERAL.encode(),
        "reaffirmation_literal": STANDING_REAFFIRMATION_LITERAL.encode(),
        "approved_by": "joe",
    }
    binding = record_production_prerequisite_authorization(tmp_path, **kwargs)
    document = json.loads((tmp_path / binding.path).read_bytes())
    assert document["artifact_type"] == "s7_registry_production_prerequisite_authorization"
    assert document["production_data_root"] == tmp_path.as_posix()
    assert document["runtime_binding"] == runtime_binding.to_dict()
    assert document["target_refs"] == [
        {"artifact_id": contract.contract_id, "sha256": contract.resource_sha256}
    ]
    assert record_production_prerequisite_authorization(tmp_path, **kwargs) == binding
