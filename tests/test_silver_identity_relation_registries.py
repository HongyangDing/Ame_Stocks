from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_relation_registries import (
    AssetTransitionDecision,
    AssetTransitionDisposition,
    AssetTransitionType,
    CompositeRegistryMatch,
    IdentityRelationRegistryError,
    IdentityRelationRegistryRelease,
    ProviderCompositeOverrideDecision,
    ProviderCompositeOverrideDisposition,
    RegistryDecisionArtifactRef,
    RelationRegistryKind,
    ShareClassAdjudicationDecision,
    ShareClassAdjudicationDisposition,
    evaluate_composite_registry_collisions,
    select_effective_terminal_decisions,
)
from ame_stocks_api.silver.identity_relation_registry_contract import (
    RELATION_REGISTRY_CONTRACTS,
)

S4_RELEASE = "1" * 64
CANDIDATE_ID = "2" * 64
CANDIDATE_SHA = "3" * 64
EVIDENCE_ID = "4" * 64
EVIDENCE_SHA = "5" * 64
PLAN_ID = "6" * 64
PLAN_SHA = "7" * 64
REQUEST_ID = "8" * 64
REQUEST_SHA = "9" * 64
RECEIPT_ID = "a" * 64
RECEIPT_SHA = "b" * 64
CALENDAR_ID = "c" * 64
CALENDAR_SHA = "d" * 64


def _source(label: str) -> str:
    return stable_digest({"source": label})


def _common() -> dict[str, object]:
    return {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "source_s4_release_set_id": S4_RELEASE,
        "source_exact_group_candidate_manifest_id": CANDIDATE_ID,
        "source_exact_group_candidate_manifest_sha256": CANDIDATE_SHA,
        "candidate_available_session": date(2026, 7, 20),
        "source_external_evidence_manifest_id": EVIDENCE_ID,
        "source_external_evidence_manifest_sha256": EVIDENCE_SHA,
        "external_evidence_available_session": date(2026, 7, 20),
        "source_decision_plan_id": PLAN_ID,
        "source_decision_plan_path": "manifests/silver/identity/decision-plans/plan.json",
        "source_decision_plan_sha256": PLAN_SHA,
        "approval_request_event_id": REQUEST_ID,
        "approval_request_event_sha256": REQUEST_SHA,
        "approval_receipt_id": RECEIPT_ID,
        "approval_receipt_sha256": RECEIPT_SHA,
        "approved_by": "joe_s7_identity_reviewer",
        "approved_at_utc": datetime(2026, 7, 20, 8, tzinfo=UTC),
        "approval_available_session": date(2026, 7, 21),
        "availability_calendar_id": CALENDAR_ID,
        "availability_calendar_sha256": CALENDAR_SHA,
    }


def _sor_transition() -> AssetTransitionDecision:
    return AssetTransitionDecision(
        **_common(),
        observed_ticker="SOR",
        transition_type=AssetTransitionType.CORPORATE_REORGANIZATION_SUCCESSOR_SECURITY,
        legal_effective_date=date(2025, 1, 1),
        predecessor_last_session=date(2024, 12, 31),
        successor_first_session=date(2025, 1, 2),
        predecessor_composite_figi="BBG000KMY6N2",
        successor_composite_figi="BBG01RK6N4M5",
        boundary_source_record_ids=tuple(
            sorted((_source("sor-2024-12-31"), _source("sor-2025-01-02")))
        ),
        disposition=AssetTransitionDisposition.CONFIRMED_GENUINE_TRANSITION,
        decision_version=1,
        supersedes_asset_transition_id=None,
        reason_code="source_capital_corporation_to_trust_reorganization",
        reason_detail=(
            "Official filings establish a genuine reorganization effective before the "
            "2025-01-02 successor trading session."
        ),
    )


def _sor_override(transition: AssetTransitionDecision) -> ProviderCompositeOverrideDecision:
    return ProviderCompositeOverrideDecision(
        **_common(),
        observed_ticker="SOR",
        observed_composite_figi="BBG000KMY6N2",
        canonical_composite_figi="BBG01RK6N4M5",
        observed_composite_market_code="US",
        canonical_composite_market_code="US",
        valid_from_session=date(2025, 1, 2),
        valid_through_session=date(2026, 7, 9),
        scoped_source_record_ids=tuple(
            sorted((_source("sor-2025-01-02"), _source("sor-2026-07-09")))
        ),
        asset_transition_series_id=transition.asset_transition_series_id,
        asset_transition_id=transition.asset_transition_id,
        asset_transition_available_session=transition.transition_available_session,
        disposition=(ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION),
        decision_version=1,
        supersedes_provider_composite_override_id=None,
        reason_code="same_market_provider_composite_stale_after_transition",
        reason_detail=(
            "Massive retained the predecessor Composite after the approved SOR successor "
            "security became effective."
        ),
    )


def _share_class(
    *,
    ticker: str,
    composite: str,
    observed: str,
    canonical: str,
    start: date,
    end: date,
    labels: tuple[str, ...],
    reason_code: str,
) -> ShareClassAdjudicationDecision:
    return ShareClassAdjudicationDecision(
        **_common(),
        observed_ticker=ticker,
        observed_composite_figi=composite,
        required_unique_canonical_composite_figi=composite,
        observed_share_class_figi=observed,
        canonical_share_class_figi=canonical,
        valid_from_session=start,
        valid_through_session=end,
        scoped_source_record_ids=tuple(sorted(_source(label) for label in labels)),
        disposition=ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION,
        decision_version=1,
        supersedes_share_class_adjudication_id=None,
        reason_code=reason_code,
        reason_detail=(
            "Immutable identifier and issuer evidence supports this exact hierarchy correction."
        ),
    )


def test_sor_transition_and_provider_override_have_separate_effects_and_exact_scope() -> None:
    transition = _sor_transition()
    override = _sor_override(transition)

    assert transition.predecessor_asset_id != transition.successor_asset_id
    assert transition.transition_available_session == date(2026, 7, 21)
    assert transition.transition_available_session != transition.legal_effective_date
    assert override.asset_transition_id == transition.asset_transition_id
    assert override.override_available_session == date(2026, 7, 21)
    assert override.canonical_asset_id == transition.successor_asset_id
    assert set(transition.to_registry_row()) == {
        item.name for item in RELATION_REGISTRY_CONTRACTS["asset_transition"].columns
    }
    assert set(override.to_registry_row()) == {
        item.name for item in RELATION_REGISTRY_CONTRACTS["provider_composite_override"].columns
    }
    assert transition.to_registry_row()["identity_override_effect"] == "none"
    assert transition.to_registry_row()["return_stitching_effect"] == (
        "none_requires_future_entitlement_accounting"
    )
    assert override.matches(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="SOR",
        observed_composite_figi="BBG000KMY6N2",
        session_date=date(2025, 1, 2),
        source_record_id=_source("sor-2025-01-02"),
        source_s4_release_set_id=S4_RELEASE,
    )
    assert not override.matches(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="SOR",
        observed_composite_figi="BBG000KMY6N2",
        session_date=date(2024, 12, 31),
        source_record_id=_source("sor-2024-12-31"),
        source_s4_release_set_id=S4_RELEASE,
    )


def test_xzo_share_class_correction_requires_unique_composite() -> None:
    decision = _share_class(
        ticker="XZO",
        composite="BBG01XL8FHT0",
        observed="BBG01XL8FJS7",
        canonical="BBG01227MF17",
        start=date(2025, 11, 4),
        end=date(2025, 11, 5),
        labels=("xzo-2025-11-04", "xzo-2025-11-05"),
        reason_code="transient_duplicate_share_class_during_ipo_onboarding",
    )

    kwargs = {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "ticker": "XZO",
        "observed_composite_figi": "BBG01XL8FHT0",
        "observed_share_class_figi": "BBG01XL8FJS7",
        "session_date": date(2025, 11, 4),
        "source_record_id": _source("xzo-2025-11-04"),
        "source_s4_release_set_id": S4_RELEASE,
    }
    assert decision.matches(**kwargs, unique_canonical_composite_figi="BBG01XL8FHT0")
    assert not decision.matches(**kwargs, unique_canonical_composite_figi=None)
    assert not decision.matches(**kwargs, unique_canonical_composite_figi="BBG000DFMXT3")
    assert not hasattr(decision, "canonical_asset_id")
    assert decision.adjudication_available_session == date(2026, 7, 21)
    assert set(decision.to_registry_row()) == {
        item.name for item in RELATION_REGISTRY_CONTRACTS["share_class_adjudication"].columns
    }
    assert decision.to_registry_row()["asset_id_effect"] == "none"


def test_anabv_only_corrects_first_share_class_row_and_never_merges_anab() -> None:
    decision = _share_class(
        ticker="ANABV",
        composite="BBG021DMXXT2",
        observed="BBG0026ZDHT8",
        canonical="BBG021GNPBR6",
        start=date(2026, 4, 6),
        end=date(2026, 4, 6),
        labels=("anabv-2026-04-06",),
        reason_code="temporary_ex_distribution_security_provider_share_class_lag",
    )

    assert decision.required_unique_canonical_composite_figi == "BBG021DMXXT2"
    assert decision.canonical_share_class_figi == "BBG021GNPBR6"
    assert decision.observed_share_class_figi == "BBG0026ZDHT8"
    assert not decision.matches(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="ANABV",
        observed_composite_figi="BBG021DMXXT2",
        unique_canonical_composite_figi="BBG021DMXXT2",
        observed_share_class_figi="BBG021GNPBR6",
        session_date=date(2026, 4, 7),
        source_record_id=_source("anabv-2026-04-07"),
        source_s4_release_set_id=S4_RELEASE,
    )
    with pytest.raises(IdentityRelationRegistryError, match="distinct assets"):
        replace(
            _sor_transition(),
            observed_ticker="ANABV",
            predecessor_composite_figi="BBG021DMXXT2",
            successor_composite_figi="BBG021DMXXT2",
        )


def test_registry_rows_materialize_against_exact_arrow_contracts() -> None:
    transition = _sor_transition()
    rows = {
        "asset_transition": transition.to_registry_row(),
        "provider_composite_override": _sor_override(transition).to_registry_row(),
        "share_class_adjudication": _share_class(
            ticker="XZO",
            composite="BBG01XL8FHT0",
            observed="BBG01XL8FJS7",
            canonical="BBG01227MF17",
            start=date(2025, 11, 4),
            end=date(2025, 11, 5),
            labels=("xzo-2025-11-04", "xzo-2025-11-05"),
            reason_code="transient_duplicate_share_class_during_ipo_onboarding",
        ).to_registry_row(),
    }
    for table, row in rows.items():
        contract = RELATION_REGISTRY_CONTRACTS[table]
        materialized = pa.Table.from_pylist([row], schema=contract.arrow_schema)
        assert materialized.schema == contract.arrow_schema
        assert materialized.num_rows == 1


def test_composite_registry_collision_is_reported_and_never_prioritized() -> None:
    source_id = _source("collision")
    first = CompositeRegistryMatch(
        registry_name="identity_adjudication",
        decision_id="1" * 64,
        source_record_id=source_id,
        observed_composite_figi="BBG000KMY6N2",
        canonical_composite_figi="BBG01RK6N4M5",
    )
    second = CompositeRegistryMatch(
        registry_name="provider_composite_override",
        decision_id="2" * 64,
        source_record_id=source_id,
        observed_composite_figi="BBG000KMY6N2",
        canonical_composite_figi="BBG01RK6N4M5",
    )

    unique = evaluate_composite_registry_collisions((first,))
    assert unique.unique_decision_id == first.decision_id
    assert unique.backtest_identity_eligible
    assert unique.identity_resolved
    assert unique.alias_allowed

    direct = evaluate_composite_registry_collisions(())
    assert direct.raw_match_count == 0
    assert direct.unique_decision_id is None
    assert direct.backtest_identity_eligible
    assert direct.identity_resolved
    assert direct.alias_allowed

    collision = evaluate_composite_registry_collisions((first, second))
    assert collision.raw_match_count == 2
    assert collision.collision
    assert collision.unique_decision_id is None
    assert not collision.backtest_identity_eligible
    assert not collision.identity_resolved
    assert not collision.alias_allowed

    with pytest.raises(IdentityRelationRegistryError, match="only Composite"):
        CompositeRegistryMatch(
            registry_name="share_class_adjudication",
            decision_id="3" * 64,
            source_record_id=source_id,
            observed_composite_figi="BBG000KMY6N2",
            canonical_composite_figi="BBG01RK6N4M5",
        )


def test_append_only_withdrawal_selection_and_release_availability_are_fail_closed() -> None:
    first = _sor_override(_sor_transition())
    second = replace(
        first,
        canonical_composite_figi=None,
        canonical_composite_market_code=None,
        disposition=(ProviderCompositeOverrideDisposition.ADJUDICATED_UNRESOLVED),
        decision_version=2,
        supersedes_provider_composite_override_id=first.provider_composite_override_id,
        approval_receipt_id="e" * 64,
        approval_receipt_sha256="f" * 64,
        approval_available_session=date(2026, 7, 22),
    )

    select = lambda cutoff: select_effective_terminal_decisions(  # noqa: E731
        (first, second),
        cutoff_session=cutoff,
        series_id=lambda item: item.provider_composite_override_series_id,
        decision_id=lambda item: item.provider_composite_override_id,
        decision_version=lambda item: item.decision_version,
        predecessor_id=lambda item: item.supersedes_provider_composite_override_id,
        available_session=lambda item: item.override_available_session,
    )
    assert select(date(2026, 7, 21)) == (first,)
    assert select(date(2026, 7, 22)) == (second,)

    ref = RegistryDecisionArtifactRef(
        decision_id=first.provider_composite_override_id,
        decision_path="manifests/silver/identity/provider-overrides/approved.json",
        decision_sha256="0" * 64,
        decision_available_session=first.override_available_session,
    )
    release = IdentityRelationRegistryRelease(
        registry_kind=RelationRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
        source_candidate_manifest_id=CANDIDATE_ID,
        source_candidate_manifest_sha256=CANDIDATE_SHA,
        source_external_evidence_manifest_id=EVIDENCE_ID,
        source_external_evidence_manifest_sha256=EVIDENCE_SHA,
        availability_calendar_id=CALENDAR_ID,
        availability_calendar_sha256=CALENDAR_SHA,
        published_at_utc=datetime(2026, 7, 21, 15, tzinfo=UTC),
        release_available_session=date(2026, 7, 22),
        decisions=(ref,),
    )
    assert len(release.release_id) == 64

    with pytest.raises(IdentityRelationRegistryError, match="precedes a decision"):
        replace(
            release,
            published_at_utc=datetime(2026, 7, 20, 15, tzinfo=UTC),
            release_available_session=date(2026, 7, 20),
        )
