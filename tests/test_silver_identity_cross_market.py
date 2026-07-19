from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import TableContract
from ame_stocks_api.silver.identity_cross_market import (
    ApprovedCrossMarketAdjudication,
    CompositeMarketClass,
    CompositeMarketReference,
    CrossMarketAdjudicationDisposition,
    CrossMarketIdentityObservation,
    IdentityCaseResolutionRole,
    LinkedIdentityCase,
    resolve_cross_market_identity,
)

US_FIGI = "BBG000DFMXT3"
FOREIGN_FIGI = "BBG000KRLLH9"
SHARE_CLASS_FIGI = "BBG001S87NT0"
S4_RELEASE_SET_ID = "1" * 64
SIX_RELEASE_BINDING_ID = "2" * 64
EVIDENCE_ID = "3" * 64
EVIDENCE_SHA = "4" * 64
CANDIDATE_ID = "5" * 64
CANDIDATE_SHA = "6" * 64
APPROVAL_ID = "7" * 64
APPROVAL_SHA = "8" * 64
ROOT = Path(__file__).resolve().parents[1]


def _source_id(label: str) -> str:
    return stable_digest({"source": label})


def _case_id(label: str) -> str:
    return stable_digest({"case": label})


def _reference(
    composite_figi: str,
    *,
    market_code: str,
    market_class: CompositeMarketClass,
) -> CompositeMarketReference:
    return CompositeMarketReference(
        composite_figi=composite_figi,
        share_class_figi=SHARE_CLASS_FIGI,
        composite_market_code=market_code,
        market_class=market_class,
        external_evidence_manifest_id=EVIDENCE_ID,
        external_evidence_manifest_sha256=EVIDENCE_SHA,
        evidence_available_session=date(2026, 7, 17),
    )


def _observation(
    label: str,
    session: date,
    composite_figi: str,
    *,
    locale: str = "us",
    primary_exchange_mic: str | None = "XNAS",
    case_id: str | None = None,
    case_role: IdentityCaseResolutionRole | None = None,
) -> CrossMarketIdentityObservation:
    return CrossMarketIdentityObservation(
        session_date=session,
        provider_id="massive",
        provider_market="stocks",
        provider_locale=locale,
        ticker="AZPN",
        active_on_date=True,
        primary_exchange_mic=primary_exchange_mic,
        observed_composite_figi=composite_figi,
        observed_share_class_figi=SHARE_CLASS_FIGI,
        source_record_id=_source_id(label),
        source_s4_release_set_id=S4_RELEASE_SET_ID,
        identity_case_id=case_id,
        identity_case_resolution_role=case_role,
    )


def _approved_override(
    rows: tuple[CrossMarketIdentityObservation, ...],
    *,
    valid_from: date,
    valid_through: date,
    linked_cases: tuple[LinkedIdentityCase, ...] = (),
) -> ApprovedCrossMarketAdjudication:
    return ApprovedCrossMarketAdjudication(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker="AZPN",
        share_class_figi=SHARE_CLASS_FIGI,
        observed_foreign_composite_figi=FOREIGN_FIGI,
        disposition=(CrossMarketAdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION),
        canonical_us_composite_figi=US_FIGI,
        observed_composite_market_code="GR",
        canonical_composite_market_code="US",
        valid_from_session=valid_from,
        valid_through_session=valid_through,
        scoped_source_record_ids=tuple(row.source_record_id for row in rows),
        source_s4_release_set_id=S4_RELEASE_SET_ID,
        source_six_release_binding_id=SIX_RELEASE_BINDING_ID,
        source_market_consistency_candidate_manifest_id=CANDIDATE_ID,
        source_market_consistency_candidate_manifest_sha256=CANDIDATE_SHA,
        candidate_available_session=date(2026, 7, 16),
        source_external_evidence_manifest_id=EVIDENCE_ID,
        source_external_evidence_manifest_sha256=EVIDENCE_SHA,
        external_evidence_available_session=date(2026, 7, 17),
        approval_receipt_id=APPROVAL_ID,
        approval_receipt_sha256=APPROVAL_SHA,
        approved_by="literal_human_approval",
        approved_at_utc=datetime(2026, 7, 17, 15, tzinfo=UTC),
        approval_available_session=date(2026, 7, 20),
        decision_version=1,
        supersedes_cross_market_adjudication_id=None,
        linked_identity_cases=linked_cases,
        reason_code="same_share_class_non_us_composite_in_us_locale",
        reason_detail=(
            "Pinned Massive US-locale lineage and immutable identifier-authority "
            "evidence establish a non-US Composite observation for the same share class."
        ),
    )


def _references() -> tuple[CompositeMarketReference, ...]:
    return (
        _reference(
            US_FIGI,
            market_code="US",
            market_class=CompositeMarketClass.US,
        ),
        _reference(
            FOREIGN_FIGI,
            market_code="GR",
            market_class=CompositeMarketClass.NON_US,
        ),
    )


def test_cross_market_scope_series_and_decision_ids_are_fixed_vectors() -> None:
    row = _observation("vector", date(2022, 2, 9), FOREIGN_FIGI)
    override = _approved_override(
        (row,), valid_from=row.session_date, valid_through=row.session_date
    )

    assert override.scoped_source_record_set_digest == (
        "a83366498e3cbb7d39e3c3c4133ae581ba963ec05961e6152ba657e0433e0587"
    )
    assert override.cross_market_subject_id == (
        "6d20ac67c01036559af45f8c412d39eaae05660851cccfcd4b089e192e6878aa"
    )
    assert override.cross_market_scope_id == (
        "ad0ce53936d3b3d5660541a176d2f9af2124e89e729f8ec755d0391f637028b5"
    )
    assert override.cross_market_series_id == (
        "62c0b66f12520613288a15f8104c493232496bf08f9ecfe962fda3127817a0ef"
    )
    assert override.cross_market_adjudication_id == (
        "670a8a5a23d58c90dd6fd3d72d249eeecff2b55db12de3f40e0248a9489fc661"
    )


def test_append_only_withdrawal_keeps_subject_series_and_changes_effective_scope() -> None:
    first = _observation("withdraw-first", date(2022, 2, 9), FOREIGN_FIGI)
    second = _observation("withdraw-second", date(2022, 2, 10), FOREIGN_FIGI)
    approved = _approved_override(
        (first,), valid_from=first.session_date, valid_through=first.session_date
    )
    withdrawn = replace(
        approved,
        disposition=CrossMarketAdjudicationDisposition.ADJUDICATED_UNRESOLVED,
        canonical_us_composite_figi=None,
        canonical_composite_market_code=None,
        valid_through_session=second.session_date,
        scoped_source_record_ids=(first.source_record_id, second.source_record_id),
        approval_receipt_id="9" * 64,
        approval_receipt_sha256="a" * 64,
        approved_at_utc=datetime(2026, 7, 20, 15, tzinfo=UTC),
        approval_available_session=date(2026, 7, 21),
        decision_version=2,
        supersedes_cross_market_adjudication_id=(approved.cross_market_adjudication_id),
        reason_code="withdraw_cross_market_mapping",
        reason_detail=(
            "Later reviewed evidence withdraws the canonical mapping while preserving "
            "the provider observations and subject history."
        ),
    )

    assert withdrawn.cross_market_subject_id == approved.cross_market_subject_id
    assert withdrawn.cross_market_series_id == approved.cross_market_series_id
    assert withdrawn.cross_market_scope_id != approved.cross_market_scope_id

    before_withdrawal = resolve_cross_market_identity(
        (first, second),
        _references(),
        (approved, withdrawn),
        cutoff_session=date(2026, 7, 20),
    )
    assert before_withdrawal.decisions[0].canonical_composite_figi == US_FIGI
    assert before_withdrawal.decisions[0].backtest_identity_eligible is True
    assert before_withdrawal.decisions[1].identity_disposition == ("pending_cross_market_review")

    after_withdrawal = resolve_cross_market_identity(
        (first, second),
        _references(),
        (approved, withdrawn),
        cutoff_session=date(2026, 7, 21),
    )
    assert all(
        item.identity_disposition == "cross_market_adjudicated_unresolved"
        for item in after_withdrawal.decisions
    )
    assert all(item.canonical_composite_figi is None for item in after_withdrawal.decisions)
    assert all(item.backtest_identity_eligible is False for item in after_withdrawal.decisions)


def test_us_foreign_us_preserves_observed_lineage_and_overrides_only_foreign() -> None:
    rows = (
        _observation("us-left", date(2022, 2, 8), US_FIGI),
        _observation("foreign-middle", date(2022, 2, 9), FOREIGN_FIGI),
        _observation("us-right", date(2022, 2, 10), US_FIGI),
    )
    override = _approved_override(
        (rows[1],), valid_from=rows[1].session_date, valid_through=rows[1].session_date
    )

    result = resolve_cross_market_identity(
        rows, _references(), (override,), cutoff_session=date(2026, 7, 20)
    )
    middle = result.decisions[1]

    assert middle.observed_composite_figi == FOREIGN_FIGI
    assert middle.canonical_composite_figi == US_FIGI
    assert middle.canonical_override is True
    assert middle.backtest_identity_eligible is True
    assert middle.identity_disposition == "confirmed_provider_contamination"
    assert all(item.identity_quality_liquidation_signal is False for item in result.decisions)
    assert all(item.identity_quality_membership_mutated is False for item in result.decisions)
    assert result.audit.us_locale_non_us_composite_figi_rows == 1
    assert result.audit.unapproved_cross_market_composite_eligible_rows == 0


def test_foreign_us_foreign_inverse_keeps_us_middle_direct_and_not_genuine() -> None:
    inverse_case = _case_id("inverse")
    foreign_left = _observation(
        "foreign-left",
        date(2022, 2, 9),
        FOREIGN_FIGI,
        case_id=_case_id("direct-left"),
        case_role=IdentityCaseResolutionRole.CONTAMINATED_MIDDLE_EPISODE,
    )
    us_middle = _observation(
        "us-middle",
        date(2022, 2, 10),
        US_FIGI,
        case_id=inverse_case,
        case_role=IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US,
    )
    foreign_right = _observation(
        "foreign-right",
        date(2022, 2, 11),
        FOREIGN_FIGI,
        case_id=_case_id("direct-right"),
        case_role=IdentityCaseResolutionRole.CONTAMINATED_MIDDLE_EPISODE,
    )
    override = _approved_override(
        (foreign_left, foreign_right),
        valid_from=foreign_left.session_date,
        valid_through=foreign_right.session_date,
        linked_cases=(
            LinkedIdentityCase(
                identity_case_id=inverse_case,
                role=IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US,
            ),
        ),
    )

    result = resolve_cross_market_identity(
        (foreign_left, us_middle, foreign_right),
        _references(),
        (override,),
        cutoff_session=date(2026, 7, 20),
    )
    middle = result.decisions[1]

    assert middle.observed_composite_figi == US_FIGI
    assert middle.canonical_composite_figi == US_FIGI
    assert middle.canonical_override is False
    assert middle.identity_disposition == "observed_consistent"
    assert middle.identity_resolution_method == (
        "source_composite_figi_exact_with_inverse_bounce_resolution"
    )
    assert middle.backtest_identity_eligible is True
    assert middle.cross_market_scope_id == override.cross_market_scope_id
    assert result.audit.inverse_bounce_misclassified_as_genuine_transition_rows == 0
    assert result.audit.correct_us_observation_overridden_rows == 0


def test_long_lived_foreign_composite_is_found_without_a_bounce_and_fails_closed() -> None:
    rows = tuple(
        _observation(f"long-{index}", date(2022, 2, 8 + index), FOREIGN_FIGI) for index in range(3)
    )

    result = resolve_cross_market_identity(
        rows, _references(), (), cutoff_session=date(2026, 7, 20)
    )

    assert result.audit.us_locale_non_us_composite_figi_rows == 3
    assert result.audit.us_locale_non_us_reason_counts == {
        "known_non_us_composite_in_us_provider_locale_with_us_primary_exchange": 3
    }
    assert len(result.audit.us_locale_non_us_bounded_examples) == 3
    assert all(
        item.identity_disposition == "pending_cross_market_review" for item in result.decisions
    )
    assert all(item.backtest_identity_eligible is False for item in result.decisions)
    assert result.audit.unapproved_cross_market_composite_eligible_rows == 0


def test_unknown_reference_preserves_membership_but_blocks_identity_and_alias() -> None:
    row = _observation("unknown-reference", date(2022, 2, 9), US_FIGI)

    result = resolve_cross_market_identity((row,), (), (), cutoff_session=date(2026, 7, 20))
    decision = result.decisions[0]

    assert decision.active_on_date is True
    assert decision.observed_composite_figi == US_FIGI
    assert decision.canonical_composite_figi is None
    assert decision.cross_market_classification_status == "not_classified"
    assert decision.identity_resolution_status == "unresolved"
    assert decision.identity_resolution_method == ("cross_market_composite_pending_unresolved")
    assert decision.identity_disposition == "pending_cross_market_review"
    assert decision.backtest_identity_eligible is False
    assert decision.alias_emitted is False
    assert decision.identity_quality_membership_mutated is False
    assert decision.identity_quality_liquidation_signal is False
    assert result.audit.figi_market_classification_uncovered_rows == 1
    assert result.audit.identity_quality_membership_mutation_rows == 0
    assert result.audit.identity_quality_liquidation_signal_rows == 0


def test_same_foreign_composite_in_real_foreign_locale_is_never_globally_overridden() -> None:
    us_row = _observation("us-scoped-foreign", date(2022, 2, 9), FOREIGN_FIGI)
    foreign_row = _observation(
        "real-foreign-market",
        date(2022, 2, 9),
        FOREIGN_FIGI,
        locale="de",
        primary_exchange_mic="XETR",
    )
    override = _approved_override(
        (us_row,),
        valid_from=date(2022, 2, 9),
        valid_through=date(2022, 2, 9),
    )

    result = resolve_cross_market_identity(
        (us_row, foreign_row),
        _references(),
        (override,),
        cutoff_session=date(2026, 7, 20),
    )
    real_foreign = next(item for item in result.decisions if item.provider_locale == "de")

    assert real_foreign.observed_composite_figi == FOREIGN_FIGI
    assert real_foreign.canonical_composite_figi == FOREIGN_FIGI
    assert real_foreign.canonical_override is False
    assert real_foreign.cross_market_adjudication_id is None
    assert real_foreign.backtest_identity_eligible is True
    assert result.audit.cross_market_override_foreign_locale_leak_rows == 0


def test_fixture_methods_dispositions_and_lineage_are_legal_in_derived_contracts() -> None:
    universe = TableContract.from_dict(
        json.loads(
            (
                ROOT / "docs/silver/contracts/reference/"
                "universe_daily.schema-v1.registry-v4.candidate.json"
            ).read_text()
        )
    )
    alias = TableContract.from_dict(
        json.loads(
            (
                ROOT / "docs/silver/contracts/identity/"
                "ticker_alias.schema-v1.registry-v4.candidate.json"
            ).read_text()
        )
    )
    universe_columns = {item.name: item.description for item in universe.columns}
    alias_columns = {item.name: item.description for item in alias.columns}

    approved_row = _observation("contract-approved", date(2022, 2, 9), FOREIGN_FIGI)
    approved = _approved_override(
        (approved_row,),
        valid_from=approved_row.session_date,
        valid_through=approved_row.session_date,
    )
    approved_decision = resolve_cross_market_identity(
        (approved_row,),
        _references(),
        (approved,),
        cutoff_session=date(2026, 7, 20),
    ).decisions[0]

    inverse_case = _case_id("contract-inverse")
    inverse_row = _observation(
        "contract-inverse-us",
        date(2022, 2, 10),
        US_FIGI,
        case_id=inverse_case,
        case_role=IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US,
    )
    inverse_group = replace(
        approved,
        linked_identity_cases=(
            LinkedIdentityCase(
                identity_case_id=inverse_case,
                role=IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US,
            ),
        ),
    )
    inverse_decision = resolve_cross_market_identity(
        (inverse_row,),
        _references(),
        (inverse_group,),
        cutoff_session=date(2026, 7, 20),
    ).decisions[0]

    pending_row = _observation("contract-pending", date(2022, 2, 11), FOREIGN_FIGI)
    pending_decision = resolve_cross_market_identity(
        (pending_row,),
        _references(),
        (),
        cutoff_session=date(2026, 7, 20),
    ).decisions[0]

    withdrawn = replace(
        approved,
        disposition=CrossMarketAdjudicationDisposition.ADJUDICATED_UNRESOLVED,
        canonical_us_composite_figi=None,
        canonical_composite_market_code=None,
        approval_receipt_id="9" * 64,
        approval_receipt_sha256="a" * 64,
        approved_at_utc=datetime(2026, 7, 20, 15, tzinfo=UTC),
        approval_available_session=date(2026, 7, 21),
        decision_version=2,
        supersedes_cross_market_adjudication_id=(approved.cross_market_adjudication_id),
        reason_code="withdraw_cross_market_mapping",
        reason_detail="Withdraw the mapping without changing the provider observation.",
    )
    withdrawn_decision = resolve_cross_market_identity(
        (approved_row,),
        _references(),
        (approved, withdrawn),
        cutoff_session=date(2026, 7, 21),
    ).decisions[0]

    decisions = (
        approved_decision,
        inverse_decision,
        pending_decision,
        withdrawn_decision,
    )
    for item in decisions:
        assert item.identity_resolution_method in universe_columns["identity_resolution_method"]
        assert item.identity_disposition in universe_columns["identity_disposition"]
        assert (
            item.cross_market_classification_status
            in universe_columns["cross_market_classification_status"]
        )
        if item.alias_emitted:
            assert item.identity_resolution_method in alias_columns["alias_resolution_method"]
            assert item.identity_disposition in alias_columns["identity_disposition"]

    assert inverse_decision.identity_case_id == inverse_case
    assert inverse_decision.identity_case_resolution_role == ("inverse_middle_is_canonical_us")
    assert inverse_decision.canonical_override is False
    assert pending_decision.alias_emitted is False
    assert withdrawn_decision.alias_emitted is False
